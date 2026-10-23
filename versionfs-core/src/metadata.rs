//! Backend-neutral contract for durable Version FS control metadata.
//!
//! The authority stores opaque byte keys and values. Domain protocols choose
//! their own canonical record encodings and use point reads for every mutation
//! precondition. Prefix scans are read-only and remain bound to one snapshot.

use std::num::NonZeroUsize;

use thiserror::Error;

use crate::time::MonotonicTime;

/// A stable identity for one logical metadata mutation and its durable result.
///
/// The mutation must write its terminal result at `outcome_key` in the same
/// transaction as its state changes. This lets the owner reconcile an unknown
/// commit after recovery before admitting another mutation.
#[derive(Clone, Copy, Debug)]
pub struct MutationIdentity<'a> {
    /// Stable request identity, reused for retries of the same logical request.
    pub operation_id: &'a [u8],
    /// Point key holding the immutable terminal outcome for this request.
    pub outcome_key: &'a [u8],
}

/// A typed error from the Metadata Authority contract.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum MetadataError {
    /// A point precondition failed and the attempted mutation did not commit.
    #[error("metadata precondition failed")]
    Conflict,
    /// The body rejected the operation before the backend commit began.
    #[error("metadata operation rejected before commit: {0}")]
    Rejected(String),
    /// The backend may have committed; no later mutation is admitted until
    /// [`MetadataAuthority::reconcile_unknown`] resolves the operation.
    #[error("commit outcome is unknown for operation {operation_id:?}: {cause}")]
    UnknownOutcome {
        /// Stable ID of the unresolved mutation.
        operation_id: Vec<u8>,
        /// Backend error or injected lost-reply boundary.
        cause: String,
    },
    /// A prior unknown operation still blocks writes, or its reconciliation
    /// cannot proceed yet.
    #[error("authority requires reconciliation for operation {operation_id:?}")]
    ReconciliationRequired {
        /// Stable ID of the unresolved mutation that owns the gate.
        operation_id: Vec<u8>,
    },
    /// No unresolved operation matches the requested reconciliation identity.
    #[error("no matching unknown operation is awaiting reconciliation")]
    NoUnknownOutcome,
    /// The metadata backend returned an error.
    #[error("metadata backend error: {0}")]
    Backend(String),
    /// The supplied operation identity or page request is invalid.
    #[error("invalid metadata request: {0}")]
    InvalidRequest(&'static str),
}

/// Point-transaction surface exposed to one `transact` body.
pub trait MetadataTxn {
    /// Return a point read from the transaction snapshot plus its own buffered
    /// writes. Missing and deleted keys are tracked preconditions.
    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, MetadataError>;

    /// Buffer a value replacement under `key`.
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), MetadataError>;

    /// Buffer a key deletion. This does not delete any object-store object.
    fn delete(&mut self, key: &[u8]) -> Result<(), MetadataError>;

    /// Read the authority owner's monotonic clock.
    ///
    /// Lease-dependent mutations perform their final validation after reading
    /// all relevant metadata and immediately before the body returns. The
    /// authority gate then keeps that decision ordered through backend commit.
    fn now(&self) -> MonotonicTime;
}

/// Opaque, read-only point-in-time authority snapshot.
pub trait MetadataSnapshot: Send + Sync {
    /// Unique in-process scope used to reject cursors from another snapshot.
    fn cursor_scope(&self) -> u64;
}

/// Opaque cursor tied to one snapshot and one exact prefix scan.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetadataCursor {
    pub(crate) snapshot_scope: u64,
    pub(crate) prefix: Vec<u8>,
    pub(crate) last_key: Vec<u8>,
}

impl MetadataCursor {
    /// Construct a cursor for a key observed in `snapshot` under `prefix`.
    pub fn new(snapshot: &impl MetadataSnapshot, prefix: &[u8], last_key: Vec<u8>) -> Self {
        Self {
            snapshot_scope: snapshot.cursor_scope(),
            prefix: prefix.to_vec(),
            last_key,
        }
    }

    /// Return the exclusive key position represented by this cursor.
    pub fn last_key(&self) -> &[u8] {
        &self.last_key
    }

    /// Return the in-process snapshot scope this cursor belongs to.
    pub fn snapshot_scope(&self) -> u64 {
        self.snapshot_scope
    }

    /// Return the exact prefix this cursor continues.
    pub fn prefix(&self) -> &[u8] {
        &self.prefix
    }
}

/// One ordered page from an authority snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetadataPage {
    /// Bytewise-sorted key/value pairs in this page.
    pub entries: Vec<(Vec<u8>, Vec<u8>)>,
    /// Exclusive snapshot-scoped cursor for the next page, or `None` at end.
    pub next_cursor: Option<MetadataCursor>,
}

/// Strongly consistent, durable metadata service used by Version FS protocols.
///
/// Implementations may invoke a transaction body more than once only after a
/// definite no-commit conflict. Bodies must therefore be repeatable and must
/// not perform external I/O, allocate a new logical operation ID, or retain
/// data from an earlier attempt. Every mutation is identified by a stable
/// operation ID and outcome key.
pub trait MetadataAuthority: Send + Sync {
    /// The backend-owned type that pins one immutable read boundary.
    type Snapshot: MetadataSnapshot;

    /// Read the latest authoritative value for a point key.
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, MetadataError>;

    /// Run one atomic, serializable point transaction.
    ///
    /// The body may be rerun after a definite conflict. Any other body error
    /// means no commit was attempted. An [`MetadataError::UnknownOutcome`]
    /// blocks later mutations until the exact operation has been reconciled.
    fn transact<R, F>(&self, operation: MutationIdentity<'_>, body: F) -> Result<R, MetadataError>
    where
        F: FnMut(&mut dyn MetadataTxn) -> Result<R, MetadataError>;

    /// Resolve a prior unknown commit by reopening/recovering the authority as
    /// needed and reading its stable operation result key. The result is
    /// committed bytes when present, otherwise the operation did not commit.
    fn reconcile_unknown(
        &self,
        operation: MutationIdentity<'_>,
    ) -> Result<ReconciledOutcome, MetadataError>;

    /// Capture a stable, read-only snapshot for ordered scans.
    fn snapshot(&self) -> Result<Self::Snapshot, MetadataError>;

    /// Read one bytewise-ordered page from `snapshot` under `prefix`.
    ///
    /// `cursor` is exclusive and scoped to the supplied snapshot and prefix.
    /// Snapshot loss is an error; implementations must never silently continue
    /// a cursor against a newer snapshot.
    fn scan_prefix(
        &self,
        snapshot: &Self::Snapshot,
        prefix: &[u8],
        cursor: Option<&MetadataCursor>,
        limit: NonZeroUsize,
    ) -> Result<MetadataPage, MetadataError>;
}

/// Authoritative result of reconciling an operation after a lost commit reply.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReconciledOutcome {
    /// The operation result was committed with its associated state changes.
    Committed(Vec<u8>),
    /// Recovery established that no terminal result was committed.
    NotCommitted,
}
