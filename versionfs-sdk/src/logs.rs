//! Native ordered logs: stream identities, atomic conditional batch appends,
//! whole-batch-boundary live trim, protected historical reads, and live polling cursors.
//!
//! Implements RFC-002 Section 7 over `versionfs-core`'s persistent maps and
//! `PreparationKind::LogMutation` authority protocol without introducing a second
//! checkpoint-coordination protocol or recopying historical segments.

use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};
use thiserror::Error;
use versionfs_core::control::{
    BranchState, CommittedRootTransition, ControlError, GenerationSeed, LiveRoots,
    NativeRootTransitionMeta, OperationState, PreparationKind, PreparationRequest,
    WorkspaceControl, WriterGrant, decode_branch_operation_result,
};
use versionfs_core::format::{
    FormatError, LogMapEntry, LogSegment, MAX_LOG_RECORD_BYTES, MAX_LOG_RECORDS, ObjectRef,
    ObjectType, SegmentIndexEntry,
};
use versionfs_core::identity::{
    AttachmentId, BranchId, GenerationId, LogId, ObjectId, ObjectStoreScopeId, OperationId,
    ProtectionId,
};
use versionfs_core::metadata::{MetadataAuthority, MetadataError};
use versionfs_core::persistent_map::{DurableObjectStore, MapObjectStore};
use versionfs_core::time::MonotonicTime;
use versionfs_core::views::{
    LogMap, LogSegmentIndex, ViewError, decode_artifact_blob, decode_artifact_chunk,
};

use crate::barrier::{VolumeBarrierControl, same_grant_identity};

/// Default preparation renewal interval during long native-view uploads (3 minutes).
pub const DEFAULT_NATIVE_RENEWAL_INTERVAL: Duration = Duration::from_secs(180);

/// Lead time before a read protection deadline at which multi-object reads renew
/// protection (1/3 of `READER_PROTECTION_NANOS`).
pub(crate) const READER_RENEW_LEAD_NANOS: u64 =
    versionfs_core::control::READER_PROTECTION_NANOS / 3;

/// Errors returned by native ordered-log operations.
#[derive(Debug, Error)]
pub enum LogError {
    /// Native mutation admission is closed while a checkpoint capture drains/seals.
    #[error("native log mutation admission is closed during capture")]
    AdmissionClosed,
    /// Append batches must contain at least one record.
    #[error("log append batch must not be empty")]
    EmptyBatch,
    /// Batch record count exceeds the v0 limit (1000 records).
    #[error("log append batch has {actual} records, exceeding maximum {maximum}")]
    TooManyRecords {
        /// Number of records supplied.
        actual: usize,
        /// Maximum permitted records per batch.
        maximum: usize,
    },
    /// Encoded record bytes exceed the v0 limit (1 MiB).
    #[error("log append batch has {actual} encoded record bytes, exceeding maximum {maximum}")]
    BatchTooLarge {
        /// Encoded record bytes supplied.
        actual: usize,
        /// Maximum permitted encoded record bytes per batch.
        maximum: usize,
    },
    /// Appending this batch would overflow `u64` sequence numbers.
    #[error("log sequence number overflow")]
    SequenceOverflow,
    /// The caller's `match_seq_num` did not match the stream's current `next_seq`.
    #[error("conditional append mismatch: expected next_seq {expected}, actual {actual}")]
    MatchSeqMismatch {
        /// Sequence number expected by the caller.
        expected: u64,
        /// Authoritative next sequence number on the stream.
        actual: u64,
    },
    /// The branch's `stream_epoch` or the named log's `log_id` changed (e.g.
    /// rollback/reset/delete).
    #[error(
        "log stream was replaced (expected stream_epoch {expected_epoch}, actual {actual_epoch})"
    )]
    StreamReplaced {
        /// Stream epoch expected by the caller or cursor.
        expected_epoch: u64,
        /// Authoritative stream epoch currently on the branch.
        actual_epoch: u64,
    },
    /// A live cursor points before the stream's retained `first_seq` after a trim.
    #[error(
        "live log cursor at sequence {cursor_seq} expired; current retained start is {first_seq}"
    )]
    CursorExpired {
        /// Next sequence requested by the cursor.
        cursor_seq: u64,
        /// Current retained first sequence on the stream.
        first_seq: u64,
    },
    /// Live trim requested a sequence that is not a whole-batch boundary.
    #[error(
        "trim target {requested_seq} is not a whole-batch boundary in [{first_seq}, {next_seq}]"
    )]
    InvalidTrimBoundary {
        /// Requested new `first_seq`.
        requested_seq: u64,
        /// Current `first_seq`.
        first_seq: u64,
        /// Current `next_seq`.
        next_seq: u64,
    },
    /// The named log does not exist in the targeted log view.
    #[error("log '{0}' not found")]
    LogNotFound(String),
    /// Creating a log failed because a live log with that name already exists.
    #[error("log '{0}' already exists")]
    LogAlreadyExists(String),
    /// Log name was empty or invalid.
    #[error("log name must not be empty")]
    InvalidName,
    /// Attachment reference is not a valid immutable artifact payload object.
    #[error("log attachment must reference an ArtifactBlob or ArtifactChunk object")]
    InvalidAttachment,
    /// Canonical format validation failed.
    #[error(transparent)]
    Format(#[from] FormatError),
    /// Persistent-view traversal or sealing failed.
    #[error(transparent)]
    View(#[from] ViewError),
    /// Metadata Authority control error.
    #[error(transparent)]
    Control(#[from] ControlError),
    /// Durable object-store failure.
    #[error("object store error: {0}")]
    ObjectStore(String),
}

/// Writable stream identity bound to a branch, stream fencing epoch, and stable log ID.
///
/// Forking creates a distinct `branch_id`; rollback/adoption advances `stream_epoch`;
/// deleting and recreating a log allocates a new `log_id`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct StreamId {
    /// Branch containing this live stream.
    pub branch_id: BranchId,
    /// Monotone stream fencing epoch on the branch.
    pub stream_epoch: u64,
    /// Stable log identity within the log map.
    pub log_id: LogId,
}

/// Authoritative summary of one named log within a live or historical log root.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogView {
    /// UTF-8 log name within the log map.
    pub name: String,
    /// Stable log identity.
    pub log_id: LogId,
    /// Inclusive first retained sequence number.
    pub first_seq: u64,
    /// Exclusive next sequence number (visible range is `[first_seq, next_seq)`).
    pub next_seq: u64,
    /// Immutable segment-index root covering `[first_seq, next_seq)`.
    pub segment_index_root: ObjectRef,
}

impl LogView {
    /// Converts a [`LogMapEntry`] and name into a [`LogView`].
    pub fn from_entry(name: impl Into<String>, entry: LogMapEntry) -> Self {
        Self {
            name: name.into(),
            log_id: entry.log_id,
            first_seq: entry.first_seq,
            next_seq: entry.end_seq,
            segment_index_root: entry.segment_index_root,
        }
    }
}

/// Exact durable acknowledgement returned by an accepted log batch append.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AppendReceipt {
    /// Stream identity that accepted the batch.
    pub stream_id: StreamId,
    /// Stable idempotency operation ID for this append.
    pub append_id: OperationId,
    /// Inclusive first sequence number assigned to the batch.
    pub start_seq: u64,
    /// Exclusive next sequence number after the batch (`end_seq - start_seq == record_count`).
    pub end_seq: u64,
    /// Authoritative branch revision committed by this append.
    pub branch_revision: u64,
}

/// Polling cursor for following a live ordered log without silent stream switches.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LiveLogCursor {
    /// Exact stream identity this cursor follows.
    pub stream_id: StreamId,
    /// Next sequence number to read (inclusive).
    pub next_seq: u64,
}

/// One verified sealed batch read from a live or historical log.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogBatchView {
    /// Stable log identity.
    pub log_id: LogId,
    /// Inclusive first sequence number in this batch.
    pub first_seq: u64,
    /// Exclusive next sequence number after this batch.
    pub end_seq: u64,
    /// Record payloads in consecutive sequence order.
    pub records: Vec<Vec<u8>>,
    /// Explicit typed attachment object references bound to this batch.
    pub attachments: Vec<ObjectRef>,
}

/// Authority operations needed for native log and artifact mutations and protected reads.
pub trait NativeControl: VolumeBarrierControl {
    /// Opens a bounded protected read view of a branch's current live-log root.
    fn open_live_log(
        &self,
        operation_id: OperationId,
        branch_id: BranchId,
        expected_revision: u64,
        expected_root: ObjectRef,
        protection_id: ProtectionId,
    ) -> Result<MonotonicTime, ControlError>;

    /// Opens a historical Generation and creates a renewable read protection.
    fn open_generation(
        &self,
        operation_id: OperationId,
        generation_id: GenerationId,
        protection_id: ProtectionId,
    ) -> Result<MonotonicTime, ControlError>;

    /// Reads an authoritative Generation's descriptor and complete view roots.
    fn get_generation(&self, generation_id: GenerationId) -> Result<GenerationSeed, ControlError>;

    /// Renews a read protection before expiry.
    fn renew_protection(
        &self,
        operation_id: OperationId,
        protection_id: ProtectionId,
    ) -> Result<MonotonicTime, ControlError>;

    /// Closes an active read protection.
    fn close_protection(
        &self,
        operation_id: OperationId,
        protection_id: ProtectionId,
    ) -> Result<(), ControlError>;

    /// Returns the authority's current monotonic clock reading.
    fn now(&self) -> MonotonicTime;
}

impl<A: MetadataAuthority> NativeControl for WorkspaceControl<A> {
    fn open_live_log(
        &self,
        operation_id: OperationId,
        branch_id: BranchId,
        expected_revision: u64,
        expected_root: ObjectRef,
        protection_id: ProtectionId,
    ) -> Result<MonotonicTime, ControlError> {
        WorkspaceControl::open_live_log(
            self,
            operation_id,
            branch_id,
            expected_revision,
            expected_root,
            protection_id,
        )
    }

    fn open_generation(
        &self,
        operation_id: OperationId,
        generation_id: GenerationId,
        protection_id: ProtectionId,
    ) -> Result<MonotonicTime, ControlError> {
        WorkspaceControl::open_generation(self, operation_id, generation_id, protection_id)
    }

    fn get_generation(&self, generation_id: GenerationId) -> Result<GenerationSeed, ControlError> {
        WorkspaceControl::get_generation(self, generation_id)
    }

    fn renew_protection(
        &self,
        operation_id: OperationId,
        protection_id: ProtectionId,
    ) -> Result<MonotonicTime, ControlError> {
        WorkspaceControl::renew_protection(self, operation_id, protection_id)
    }

    fn close_protection(
        &self,
        operation_id: OperationId,
        protection_id: ProtectionId,
    ) -> Result<(), ControlError> {
        WorkspaceControl::close_protection(self, operation_id, protection_id)
    }

    fn now(&self) -> MonotonicTime {
        WorkspaceControl::now(self)
    }
}

pub(crate) fn ensure_store_scope<S, C>(store: &S, control: &C) -> Result<(), ControlError>
where
    S: MapObjectStore,
    C: VolumeBarrierControl,
{
    if store.proof_scope() != control.object_store_scope() {
        return Err(ControlError::InvalidRequest(
            "object store scope does not match workspace object_store_scope",
        ));
    }
    Ok(())
}

pub(crate) struct ReadProtectionGuard<'a, C, F> {
    control: &'a C,
    protection_id: ProtectionId,
    deadline: MonotonicTime,
    next_renewal_id: F,
}

impl<'a, C, F> ReadProtectionGuard<'a, C, F>
where
    C: NativeControl,
    F: FnMut() -> OperationId,
{
    pub(crate) fn new(
        control: &'a C,
        protection_id: ProtectionId,
        deadline: MonotonicTime,
        next_renewal_id: F,
    ) -> Self {
        Self {
            control,
            protection_id,
            deadline,
            next_renewal_id,
        }
    }

    /// Ensures the read protection has not expired and renews it before expiry
    /// when within [`READER_RENEW_LEAD_NANOS`] of the deadline.
    pub(crate) fn ensure_live(&mut self) -> Result<(), ControlError> {
        let now = self.control.now();
        let remaining = self.deadline.as_nanos().saturating_sub(now.as_nanos());
        if remaining == 0 {
            return Err(ControlError::Expired);
        }
        if remaining <= READER_RENEW_LEAD_NANOS {
            let renew_op = (self.next_renewal_id)();
            self.deadline = self
                .control
                .renew_protection(renew_op, self.protection_id)?;
        }
        Ok(())
    }
}

/// Shared admission and drain gate connecting native log and artifact producers
/// to P7's [`crate::capture::ProducerGate`] common-cut protocol.
#[derive(Clone, Debug, Default)]
pub struct NativeAdmissionGate {
    state: Arc<(Mutex<AdmissionState>, Condvar)>,
}

#[derive(Debug, Default)]
struct AdmissionState {
    closed: bool,
    in_flight: usize,
}

/// RAII guard tracking one admitted in-flight native mutation.
pub struct NativeAdmissionGuard {
    state: Arc<(Mutex<AdmissionState>, Condvar)>,
}

impl Drop for NativeAdmissionGuard {
    fn drop(&mut self) {
        let (lock, cvar) = &*self.state;
        let mut guard = lock.lock().unwrap_or_else(|e| e.into_inner());
        guard.in_flight = guard.in_flight.saturating_sub(1);
        if guard.in_flight == 0 {
            cvar.notify_all();
        }
    }
}

impl NativeAdmissionGate {
    /// Creates a new open native-mutation admission gate.
    pub fn new() -> Self {
        Self::default()
    }

    /// Attempts to enter an admitted native mutation, rejecting with `None` if
    /// admission is currently closed for checkpoint capture.
    pub fn try_enter(&self) -> Option<NativeAdmissionGuard> {
        let (lock, _) = &*self.state;
        let mut guard = lock.lock().unwrap_or_else(|e| e.into_inner());
        if guard.closed {
            return None;
        }
        guard.in_flight += 1;
        Some(NativeAdmissionGuard {
            state: Arc::clone(&self.state),
        })
    }

    /// Closes admission so new native mutations fail immediately with `AdmissionClosed`.
    pub fn close(&self) {
        let (lock, _) = &*self.state;
        let mut guard = lock.lock().unwrap_or_else(|e| e.into_inner());
        guard.closed = true;
    }

    /// Blocks until every previously admitted native mutation has finished.
    pub fn drain(&self) {
        let (lock, cvar) = &*self.state;
        let mut guard = lock.lock().unwrap_or_else(|e| e.into_inner());
        while guard.in_flight > 0 {
            guard = cvar.wait(guard).unwrap_or_else(|e| e.into_inner());
        }
    }

    /// Reopens admission idempotently (safe to call when already open).
    pub fn reopen(&self) {
        let (lock, _) = &*self.state;
        let mut guard = lock.lock().unwrap_or_else(|e| e.into_inner());
        guard.closed = false;
    }

    /// Returns whether admission is currently closed.
    pub fn is_closed(&self) -> bool {
        let (lock, _) = &*self.state;
        lock.lock().unwrap_or_else(|e| e.into_inner()).closed
    }
}

/// Caller-supplied writer/attachment context for one native branch mutation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NativeWriteContext {
    /// Branch being mutated.
    pub branch_id: BranchId,
    /// Current writer grant for the branch.
    pub grant: WriterGrant,
    /// Active writable attachment identity on the branch.
    pub attachment_id: AttachmentId,
    /// Active volume fencing epoch on the branch.
    pub volume_epoch: u64,
}

/// Staging object store that reads from an existing store and buffers newly
/// encoded immutable objects in topological order before durable upload.
pub(crate) struct StagingObjectStore<'a, S> {
    backing: &'a S,
    staged_map: HashMap<ObjectId, Vec<u8>>,
    staged_order: Vec<(ObjectRef, Vec<u8>)>,
}

impl<'a, S: MapObjectStore> StagingObjectStore<'a, S> {
    pub(crate) fn new(backing: &'a S) -> Self {
        Self {
            backing,
            staged_map: HashMap::new(),
            staged_order: Vec::new(),
        }
    }

    pub(crate) fn into_staged(self) -> Vec<(ObjectRef, Vec<u8>)> {
        self.staged_order
    }
}

impl<S: MapObjectStore> MapObjectStore for StagingObjectStore<'_, S> {
    fn proof_scope(&self) -> ObjectStoreScopeId {
        self.backing.proof_scope()
    }

    fn get(&self, id: ObjectId) -> Result<Option<Vec<u8>>, String> {
        if let Some(bytes) = self.staged_map.get(&id) {
            return Ok(Some(bytes.clone()));
        }
        self.backing.get(id)
    }

    fn get_ref(&self, reference: ObjectRef) -> Result<Option<Vec<u8>>, String> {
        if let Some(bytes) = self.staged_map.get(&reference.object_id) {
            return Ok(Some(bytes.clone()));
        }
        self.backing.get_ref(reference)
    }

    fn put(&mut self, id: ObjectId, bytes: Vec<u8>) -> Result<(), String> {
        let reference = ObjectRef::from_encoded(&bytes).map_err(|e| e.to_string())?;
        if reference.object_id != id {
            return Err("staged object ID does not match encoded digest".to_owned());
        }
        if let std::collections::hash_map::Entry::Vacant(entry) = self.staged_map.entry(id) {
            entry.insert(bytes.clone());
            self.staged_order.push((reference, bytes));
        }
        Ok(())
    }
}

/// Error returned by [`commit_native_root_mutation`], preserving exact
/// authority vs. object-store error distinction.
pub(crate) enum NativeCommitError {
    Control(ControlError),
    ObjectStore(String),
}

impl From<ControlError> for NativeCommitError {
    fn from(error: ControlError) -> Self {
        Self::Control(error)
    }
}

impl From<NativeCommitError> for LogError {
    fn from(error: NativeCommitError) -> Self {
        match error {
            NativeCommitError::Control(e) => Self::Control(e),
            NativeCommitError::ObjectStore(msg) => Self::ObjectStore(msg),
        }
    }
}

pub(crate) fn is_unknown_control_error(error: &ControlError) -> bool {
    matches!(
        error,
        ControlError::Metadata(
            MetadataError::UnknownOutcome { .. }
                | MetadataError::ReconciliationRequired { .. }
                | MetadataError::Backend(_)
        )
    )
}

/// Uploads staged objects durably under an active preparation, refreshing the
/// writer grant if a concurrent heartbeat extended it, renewing the preparation
/// if `renewal_interval` elapses during long uploads, and aborting the active
/// preparation on definite upload or renewal failure so the branch is never
/// wedged with `Busy`.
#[allow(clippy::too_many_arguments)]
pub(crate) fn commit_native_root_mutation<S, C, F>(
    store: &mut S,
    control: &C,
    ctx: NativeWriteContext,
    operation_id: OperationId,
    protection_id: ProtectionId,
    expected_revision: u64,
    kind: PreparationKind,
    next_roots: LiveRoots,
    dependencies: Vec<ObjectRef>,
    staged_objects: Vec<(ObjectRef, Vec<u8>)>,
    native_meta: Option<NativeRootTransitionMeta>,
    renewal_interval: Duration,
    mut next_renewal_id: F,
) -> Result<BranchState, NativeCommitError>
where
    S: DurableObjectStore,
    C: VolumeBarrierControl,
    F: FnMut() -> OperationId,
{
    ensure_store_scope(store, control)?;

    // Attempt preparation admission with the caller's grant first so an
    // idempotent replay of an already-admitted preparation (after a lost reply
    // and subsequent writer-lease heartbeat renewal) matches its original
    // preparation fingerprint instead of failing with `OperationMismatch`.
    let mut current_grant = ctx.grant;
    let begin_result = control.begin_preparation(PreparationRequest {
        operation_id,
        branch_id: ctx.branch_id,
        expected_revision,
        grant: current_grant,
        attachment_id: ctx.attachment_id,
        volume_epoch: ctx.volume_epoch,
        protection_id,
        kind,
        expected_roots: Some(next_roots),
        dependencies: dependencies.clone(),
    });
    let begin_state = match begin_result {
        Ok(state) => state,
        Err(
            err @ (ControlError::Fenced | ControlError::Expired | ControlError::OperationMismatch),
        ) => {
            let live = control.check_attachment_live(
                ctx.branch_id,
                ctx.attachment_id,
                ctx.volume_epoch,
                ctx.grant,
            )?;
            if same_grant_identity(current_grant, live)
                && live.expires_at > current_grant.expires_at
            {
                current_grant = live;
                control.begin_preparation(PreparationRequest {
                    operation_id,
                    branch_id: ctx.branch_id,
                    expected_revision,
                    grant: current_grant,
                    attachment_id: ctx.attachment_id,
                    volume_epoch: ctx.volume_epoch,
                    protection_id,
                    kind,
                    expected_roots: Some(next_roots),
                    dependencies,
                })?
            } else {
                return Err(err.into());
            }
        }
        Err(error) => return Err(error.into()),
    };

    match begin_state {
        OperationState::Committed(bytes) => return Ok(decode_branch_operation_result(&bytes)?),
        OperationState::Aborted => return Err(ControlError::Aborted.into()),
        OperationState::Preparing { .. } => {}
    }

    let mut last_renewal = Instant::now();
    for (reference, encoded) in staged_objects {
        if last_renewal.elapsed() >= renewal_interval {
            let attempt_started = Instant::now();
            let renewal_id = next_renewal_id();
            match control.renew_preparation(renewal_id, operation_id, current_grant) {
                Ok(_) => {
                    last_renewal = attempt_started;
                }
                Err(err @ (ControlError::Expired | ControlError::Fenced)) => {
                    match control.check_attachment_live(
                        ctx.branch_id,
                        ctx.attachment_id,
                        ctx.volume_epoch,
                        ctx.grant,
                    ) {
                        Ok(live)
                            if same_grant_identity(current_grant, live)
                                && live.expires_at > current_grant.expires_at =>
                        {
                            current_grant = live;
                            let retry_started = Instant::now();
                            let retry_id = next_renewal_id();
                            if let Err(retry_err) =
                                control.renew_preparation(retry_id, operation_id, current_grant)
                            {
                                if !is_unknown_control_error(&retry_err) {
                                    let _ = control.abort_operation(operation_id);
                                }
                                return Err(retry_err.into());
                            }
                            last_renewal = retry_started;
                        }
                        Ok(_) => {
                            let _ = control.abort_operation(operation_id);
                            return Err(err.into());
                        }
                        Err(check_err) => {
                            if !is_unknown_control_error(&check_err) {
                                let _ = control.abort_operation(operation_id);
                            }
                            return Err(check_err.into());
                        }
                    }
                }
                Err(error) => {
                    if !is_unknown_control_error(&error) {
                        let _ = control.abort_operation(operation_id);
                    }
                    return Err(error.into());
                }
            }
        }
        if let Err(msg) = store.put_durable(reference, &encoded) {
            let _ = control.abort_operation(operation_id);
            return Err(NativeCommitError::ObjectStore(msg));
        }
    }

    match control.commit_live_roots_with_meta(
        operation_id,
        expected_revision,
        current_grant,
        next_roots,
        None,
        native_meta.clone(),
    ) {
        Ok(branch) => Ok(branch),
        Err(err @ (ControlError::Fenced | ControlError::Expired)) => {
            match control.check_attachment_live(
                ctx.branch_id,
                ctx.attachment_id,
                ctx.volume_epoch,
                ctx.grant,
            ) {
                Ok(live)
                    if same_grant_identity(current_grant, live)
                        && live.expires_at > current_grant.expires_at =>
                {
                    match control.commit_live_roots_with_meta(
                        operation_id,
                        expected_revision,
                        live,
                        next_roots,
                        None,
                        native_meta,
                    ) {
                        Ok(branch) => Ok(branch),
                        Err(retry_err) => {
                            if !is_unknown_control_error(&retry_err) {
                                let _ = control.abort_operation(operation_id);
                            }
                            Err(retry_err.into())
                        }
                    }
                }
                Ok(_) => {
                    let _ = control.abort_operation(operation_id);
                    Err(err.into())
                }
                Err(check_err) => {
                    if !is_unknown_control_error(&check_err) {
                        let _ = control.abort_operation(operation_id);
                    }
                    Err(check_err.into())
                }
            }
        }
        Err(error) => {
            if !is_unknown_control_error(&error) {
                let _ = control.abort_operation(operation_id);
            }
            Err(error.into())
        }
    }
}

/// Client for native ordered-log mutations and protected live/historical reads.
pub struct LogClient {
    admission: Option<NativeAdmissionGate>,
    renewal_interval: Duration,
}

impl Default for LogClient {
    fn default() -> Self {
        Self::new()
    }
}

impl LogClient {
    /// Creates an ungated native log client with default preparation renewal interval.
    pub const fn new() -> Self {
        Self {
            admission: None,
            renewal_interval: DEFAULT_NATIVE_RENEWAL_INTERVAL,
        }
    }

    /// Creates a native log client connected to a shared [`NativeAdmissionGate`].
    pub fn with_admission(admission: NativeAdmissionGate) -> Self {
        Self {
            admission: Some(admission),
            renewal_interval: DEFAULT_NATIVE_RENEWAL_INTERVAL,
        }
    }

    /// Overrides the preparation renewal interval used during long object uploads.
    pub const fn with_renewal_interval(mut self, interval: Duration) -> Self {
        self.renewal_interval = interval;
        self
    }

    fn enter_admission(&self) -> Result<Option<NativeAdmissionGuard>, LogError> {
        match &self.admission {
            Some(gate) => gate.try_enter().map(Some).ok_or(LogError::AdmissionClosed),
            None => Ok(None),
        }
    }

    /// Creates a new named log with a fresh [`LogId`] and empty range `[0, 0)`.
    ///
    /// Retrying the same `operation_id` with the same name and `log_id` returns
    /// the original [`StreamId`] and [`BranchState`].
    #[allow(clippy::too_many_arguments)]
    pub fn create_log<S, C, F>(
        &self,
        store: &mut S,
        control: &C,
        ctx: NativeWriteContext,
        operation_id: OperationId,
        protection_id: ProtectionId,
        name: &str,
        log_id: LogId,
        next_renewal_id: F,
    ) -> Result<(StreamId, BranchState), LogError>
    where
        S: DurableObjectStore,
        C: VolumeBarrierControl,
        F: FnMut() -> OperationId,
    {
        if name.is_empty() {
            return Err(LogError::InvalidName);
        }
        ensure_store_scope(store, control)?;
        let req_fp = fingerprint_create_log(ctx.branch_id, name, log_id);

        // Check for an already-terminal replay of `operation_id` before
        // requiring mutation admission so retries after capture closes admission
        // still return the committed result.
        if let Some(transition) = control.get_committed_root_transition(operation_id)? {
            return verify_committed_create_log_replay(&transition, ctx.branch_id, log_id, req_fp);
        }

        let _guard = self.enter_admission()?;

        let branch = control.get_branch(ctx.branch_id)?;
        let (next_logs_root, staged) = {
            let mut staging = StagingObjectStore::new(store);
            let log_map = LogMap::open(&staging, branch.logs_root)?;
            if log_map.get(&staging, name)?.is_some() {
                if let Some(transition) = control.get_committed_root_transition(operation_id)? {
                    return verify_committed_create_log_replay(
                        &transition,
                        ctx.branch_id,
                        log_id,
                        req_fp,
                    );
                }
                return Err(LogError::LogAlreadyExists(name.to_owned()));
            }
            let empty_index = LogSegmentIndex::empty(&mut staging)?;
            let entry = LogMapEntry {
                log_id,
                first_seq: 0,
                end_seq: 0,
                segment_index_root: empty_index.root(),
            };
            let next_map = log_map.insert(&mut staging, name, entry)?;
            let root = next_map.seal(&mut staging)?;
            (root, staging.into_staged())
        };

        let next_roots = LiveRoots {
            volume: branch.volume_root,
            logs: next_logs_root,
            artifacts: branch.artifacts_root,
        };
        let committed = commit_native_root_mutation(
            store,
            control,
            ctx,
            operation_id,
            protection_id,
            branch.revision,
            PreparationKind::LogMutation,
            next_roots,
            vec![next_logs_root],
            staged,
            Some(NativeRootTransitionMeta {
                request_fingerprint: req_fp,
                receipt_bytes: Vec::new(),
                claim_log_id: Some(log_id),
            }),
            self.renewal_interval,
            next_renewal_id,
        )?;
        let stream_id = StreamId {
            branch_id: committed.branch_id,
            stream_epoch: committed.stream_epoch,
            log_id,
        };
        Ok((stream_id, committed))
    }

    /// Atomically appends one sealed batch of records and explicit attachment
    /// references to `name` under conditional `match_seq_num` and `stream_id`.
    ///
    /// Retrying the same `append_id` with identical payload, attachments, and
    /// expected position returns the exact original [`AppendReceipt`] even if
    /// the tail has since advanced; changing any of them fails with
    /// [`ControlError::OperationMismatch`].
    #[allow(clippy::too_many_arguments)]
    pub fn append<S, C, F>(
        &self,
        store: &mut S,
        control: &C,
        ctx: NativeWriteContext,
        name: &str,
        stream_id: StreamId,
        match_seq_num: u64,
        append_id: OperationId,
        protection_id: ProtectionId,
        records: Vec<Vec<u8>>,
        attachments: Vec<ObjectRef>,
        next_renewal_id: F,
    ) -> Result<AppendReceipt, LogError>
    where
        S: DurableObjectStore,
        C: VolumeBarrierControl,
        F: FnMut() -> OperationId,
    {
        if name.is_empty() {
            return Err(LogError::InvalidName);
        }
        ensure_store_scope(store, control)?;
        if ctx.branch_id != stream_id.branch_id {
            let actual_epoch = control
                .get_branch(ctx.branch_id)
                .map(|b| b.stream_epoch)
                .unwrap_or(0);
            return Err(LogError::StreamReplaced {
                expected_epoch: stream_id.stream_epoch,
                actual_epoch,
            });
        }

        // Check for an already-terminal replay of `append_id` BEFORE batch
        // validation or staging so invalid/changed inputs under a committed ID
        // return `OperationMismatch` and matching retries return the exact
        // original receipt even after the tail advances or GC runs.
        if let Some(transition) = control.get_committed_root_transition(append_id)? {
            let Ok(()) = validate_append_batch(&records, &attachments) else {
                return Err(LogError::Control(ControlError::OperationMismatch));
            };
            let Some(end_seq) = match_seq_num.checked_add(records.len() as u64) else {
                return Err(LogError::Control(ControlError::OperationMismatch));
            };
            let segment = LogSegment {
                log_id: stream_id.log_id,
                first_seq: match_seq_num,
                end_seq,
                records,
                attachments,
            };
            let Ok(encoded_segment) = segment.encode() else {
                return Err(LogError::Control(ControlError::OperationMismatch));
            };
            let Ok(expected_segment_ref) = ObjectRef::from_encoded(&encoded_segment) else {
                return Err(LogError::Control(ControlError::OperationMismatch));
            };
            let req_fp = fingerprint_append(
                ctx.branch_id,
                name,
                stream_id,
                match_seq_num,
                expected_segment_ref,
            );
            return verify_committed_append_replay(&transition, stream_id, append_id, req_fp);
        }

        validate_append_batch(&records, &attachments)?;
        let record_count = records.len() as u64;
        let end_seq = match_seq_num
            .checked_add(record_count)
            .ok_or(LogError::SequenceOverflow)?;

        let segment = LogSegment {
            log_id: stream_id.log_id,
            first_seq: match_seq_num,
            end_seq,
            records,
            attachments: attachments.clone(),
        };
        let encoded_segment = segment.encode()?;
        let expected_segment_ref = ObjectRef::from_encoded(&encoded_segment)?;
        let req_fp = fingerprint_append(
            ctx.branch_id,
            name,
            stream_id,
            match_seq_num,
            expected_segment_ref,
        );

        // Verify caller-supplied attachment objects exist and pass digest/format
        // checks in `store` before registering a preparation or committing roots.
        verify_attachment_objects_in_store(store, &attachments)?;

        let _guard = self.enter_admission()?;

        let branch = control.get_branch(ctx.branch_id)?;
        if branch.branch_id != stream_id.branch_id || branch.stream_epoch != stream_id.stream_epoch
        {
            return Err(LogError::StreamReplaced {
                expected_epoch: stream_id.stream_epoch,
                actual_epoch: branch.stream_epoch,
            });
        }

        let (next_logs_root, staged) = {
            let mut staging = StagingObjectStore::new(store);
            let log_map = LogMap::open(&staging, branch.logs_root)?;
            let Some(entry) = log_map.get(&staging, name)? else {
                return Err(LogError::LogNotFound(name.to_owned()));
            };
            if entry.log_id != stream_id.log_id {
                return Err(LogError::StreamReplaced {
                    expected_epoch: stream_id.stream_epoch,
                    actual_epoch: branch.stream_epoch,
                });
            }
            if entry.end_seq != match_seq_num {
                if let Some(transition) = control.get_committed_root_transition(append_id)? {
                    return verify_committed_append_replay(
                        &transition,
                        stream_id,
                        append_id,
                        req_fp,
                    );
                }
                return Err(LogError::MatchSeqMismatch {
                    expected: match_seq_num,
                    actual: entry.end_seq,
                });
            }
            staging
                .put(expected_segment_ref.object_id, encoded_segment)
                .map_err(LogError::ObjectStore)?;
            let seg_index = LogSegmentIndex::from_root(entry.segment_index_root)?;
            let next_index = seg_index.insert(
                &mut staging,
                match_seq_num,
                SegmentIndexEntry {
                    end_seq,
                    segment: expected_segment_ref,
                },
            )?;
            let next_entry = LogMapEntry {
                log_id: entry.log_id,
                first_seq: entry.first_seq,
                end_seq,
                segment_index_root: next_index.root(),
            };
            let next_map = log_map.insert(&mut staging, name, next_entry)?;
            let root = next_map.seal(&mut staging)?;
            (root, staging.into_staged())
        };

        let mut dependencies = Vec::with_capacity(attachments.len() + 2);
        dependencies.push(next_logs_root);
        dependencies.push(expected_segment_ref);
        dependencies.extend(attachments);

        let next_roots = LiveRoots {
            volume: branch.volume_root,
            logs: next_logs_root,
            artifacts: branch.artifacts_root,
        };
        let committed = commit_native_root_mutation(
            store,
            control,
            ctx,
            append_id,
            protection_id,
            branch.revision,
            PreparationKind::LogMutation,
            next_roots,
            dependencies,
            staged,
            Some(NativeRootTransitionMeta {
                request_fingerprint: req_fp,
                receipt_bytes: encode_append_receipt_meta(
                    stream_id,
                    match_seq_num,
                    end_seq,
                    expected_segment_ref,
                ),
                claim_log_id: None,
            }),
            self.renewal_interval,
            next_renewal_id,
        )?;
        Ok(AppendReceipt {
            stream_id,
            append_id,
            start_seq: match_seq_num,
            end_seq,
            branch_revision: committed.revision,
        })
    }

    /// Trims a live log forward to an exact whole-batch boundary `trim_to_seq`.
    ///
    /// Rejects cuts inside a batch with [`LogError::InvalidTrimBoundary`].
    /// Never modifies or deletes historical segments, so retained Generations,
    /// forks, and protected readers keep their older ranges reachable.
    #[allow(clippy::too_many_arguments)]
    pub fn trim_log<S, C, F>(
        &self,
        store: &mut S,
        control: &C,
        ctx: NativeWriteContext,
        name: &str,
        stream_id: StreamId,
        trim_to_seq: u64,
        operation_id: OperationId,
        protection_id: ProtectionId,
        next_renewal_id: F,
    ) -> Result<(LogView, BranchState), LogError>
    where
        S: DurableObjectStore,
        C: VolumeBarrierControl,
        F: FnMut() -> OperationId,
    {
        if name.is_empty() {
            return Err(LogError::InvalidName);
        }
        ensure_store_scope(store, control)?;
        if ctx.branch_id != stream_id.branch_id {
            let actual_epoch = control
                .get_branch(ctx.branch_id)
                .map(|b| b.stream_epoch)
                .unwrap_or(0);
            return Err(LogError::StreamReplaced {
                expected_epoch: stream_id.stream_epoch,
                actual_epoch,
            });
        }
        let req_fp = fingerprint_trim_log(ctx.branch_id, name, stream_id, trim_to_seq);

        if let Some(transition) = control.get_committed_root_transition(operation_id)? {
            return verify_committed_trim_log_replay(&transition, name, stream_id, req_fp);
        }

        let _guard = self.enter_admission()?;

        let branch = control.get_branch(ctx.branch_id)?;
        if branch.branch_id != stream_id.branch_id || branch.stream_epoch != stream_id.stream_epoch
        {
            return Err(LogError::StreamReplaced {
                expected_epoch: stream_id.stream_epoch,
                actual_epoch: branch.stream_epoch,
            });
        }

        let (next_entry, next_logs_root, staged) = {
            let mut staging = StagingObjectStore::new(store);
            let log_map = LogMap::open(&staging, branch.logs_root)?;
            let Some(entry) = log_map.get(&staging, name)? else {
                return Err(LogError::LogNotFound(name.to_owned()));
            };
            if entry.log_id != stream_id.log_id {
                return Err(LogError::StreamReplaced {
                    expected_epoch: stream_id.stream_epoch,
                    actual_epoch: branch.stream_epoch,
                });
            }
            if trim_to_seq < entry.first_seq || trim_to_seq > entry.end_seq {
                return Err(LogError::InvalidTrimBoundary {
                    requested_seq: trim_to_seq,
                    first_seq: entry.first_seq,
                    next_seq: entry.end_seq,
                });
            }
            let seg_index = LogSegmentIndex::from_root(entry.segment_index_root)?;
            let segments = seg_index.entries(&staging)?;
            let is_boundary = trim_to_seq == entry.first_seq
                || trim_to_seq == entry.end_seq
                || segments.iter().any(|(first, _)| *first == trim_to_seq);
            if !is_boundary {
                return Err(LogError::InvalidTrimBoundary {
                    requested_seq: trim_to_seq,
                    first_seq: entry.first_seq,
                    next_seq: entry.end_seq,
                });
            }
            let mut next_index = seg_index;
            for (seg_first, _) in &segments {
                if *seg_first < trim_to_seq {
                    next_index = next_index.remove(&mut staging, *seg_first)?;
                }
            }
            let next_entry = LogMapEntry {
                log_id: entry.log_id,
                first_seq: trim_to_seq,
                end_seq: entry.end_seq,
                segment_index_root: next_index.root(),
            };
            let next_map = log_map.insert(&mut staging, name, next_entry)?;
            let root = next_map.seal(&mut staging)?;
            (next_entry, root, staging.into_staged())
        };

        let next_roots = LiveRoots {
            volume: branch.volume_root,
            logs: next_logs_root,
            artifacts: branch.artifacts_root,
        };
        let committed = commit_native_root_mutation(
            store,
            control,
            ctx,
            operation_id,
            protection_id,
            branch.revision,
            PreparationKind::LogMutation,
            next_roots,
            vec![next_logs_root],
            staged,
            Some(NativeRootTransitionMeta {
                request_fingerprint: req_fp,
                receipt_bytes: next_entry.encode()?,
                claim_log_id: None,
            }),
            self.renewal_interval,
            next_renewal_id,
        )?;
        Ok((LogView::from_entry(name, next_entry), committed))
    }

    /// Removes a live named log from the branch's log map without erasing
    /// historical segments retained by Generations or other branches.
    #[allow(clippy::too_many_arguments)]
    pub fn delete_log<S, C, F>(
        &self,
        store: &mut S,
        control: &C,
        ctx: NativeWriteContext,
        name: &str,
        stream_id: StreamId,
        operation_id: OperationId,
        protection_id: ProtectionId,
        next_renewal_id: F,
    ) -> Result<BranchState, LogError>
    where
        S: DurableObjectStore,
        C: VolumeBarrierControl,
        F: FnMut() -> OperationId,
    {
        if name.is_empty() {
            return Err(LogError::InvalidName);
        }
        ensure_store_scope(store, control)?;
        if ctx.branch_id != stream_id.branch_id {
            let actual_epoch = control
                .get_branch(ctx.branch_id)
                .map(|b| b.stream_epoch)
                .unwrap_or(0);
            return Err(LogError::StreamReplaced {
                expected_epoch: stream_id.stream_epoch,
                actual_epoch,
            });
        }
        let req_fp = fingerprint_delete_log(ctx.branch_id, name, stream_id);

        if let Some(transition) = control.get_committed_root_transition(operation_id)? {
            return verify_committed_delete_log_replay(&transition, stream_id, req_fp);
        }

        let _guard = self.enter_admission()?;

        let branch = control.get_branch(ctx.branch_id)?;
        if branch.branch_id != stream_id.branch_id || branch.stream_epoch != stream_id.stream_epoch
        {
            return Err(LogError::StreamReplaced {
                expected_epoch: stream_id.stream_epoch,
                actual_epoch: branch.stream_epoch,
            });
        }

        let (next_logs_root, staged) = {
            let mut staging = StagingObjectStore::new(store);
            let log_map = LogMap::open(&staging, branch.logs_root)?;
            let Some(entry) = log_map.get(&staging, name)? else {
                if let Some(transition) = control.get_committed_root_transition(operation_id)? {
                    return verify_committed_delete_log_replay(&transition, stream_id, req_fp);
                }
                return Err(LogError::LogNotFound(name.to_owned()));
            };
            if entry.log_id != stream_id.log_id {
                return Err(LogError::StreamReplaced {
                    expected_epoch: stream_id.stream_epoch,
                    actual_epoch: branch.stream_epoch,
                });
            }
            let next_map = log_map.remove(&mut staging, name)?;
            let root = next_map.seal(&mut staging)?;
            (root, staging.into_staged())
        };

        let next_roots = LiveRoots {
            volume: branch.volume_root,
            logs: next_logs_root,
            artifacts: branch.artifacts_root,
        };
        let committed = commit_native_root_mutation(
            store,
            control,
            ctx,
            operation_id,
            protection_id,
            branch.revision,
            PreparationKind::LogMutation,
            next_roots,
            vec![next_logs_root],
            staged,
            Some(NativeRootTransitionMeta {
                request_fingerprint: req_fp,
                receipt_bytes: Vec::new(),
                claim_log_id: None,
            }),
            self.renewal_interval,
            next_renewal_id,
        )?;
        Ok(committed)
    }

    /// Reads verified batches from a historical Generation's log view under a
    /// bounded Generation read protection, without mounting the volume.
    #[allow(clippy::too_many_arguments)]
    pub fn read_historical_log<S, C, F>(
        &self,
        store: &S,
        control: &C,
        generation_id: GenerationId,
        name: &str,
        from_seq: u64,
        max_batches: usize,
        open_op_id: OperationId,
        protection_id: ProtectionId,
        close_op_id: OperationId,
        next_renewal_id: F,
    ) -> Result<(LogView, Vec<LogBatchView>), LogError>
    where
        S: MapObjectStore,
        C: NativeControl,
        F: FnMut() -> OperationId,
    {
        if name.is_empty() {
            return Err(LogError::InvalidName);
        }
        ensure_store_scope(store, control)?;
        let deadline = control.open_generation(open_op_id, generation_id, protection_id)?;
        let mut guard = ReadProtectionGuard::new(control, protection_id, deadline, next_renewal_id);
        let read_result = (|| {
            guard.ensure_live()?;
            let seed = control.get_generation(generation_id)?;
            read_log_from_root_with_guard(
                store,
                seed.roots.logs,
                name,
                from_seq,
                max_batches,
                &mut || guard.ensure_live(),
            )
        })();
        let _ = control.close_protection(close_op_id, protection_id);
        read_result
    }

    /// Polls a live log from `cursor` under a bounded `open_live_log` protection.
    ///
    /// Returns [`LogError::StreamReplaced`] if the branch's `stream_epoch` or
    /// the named log's `log_id` changed, or [`LogError::CursorExpired`] if the
    /// cursor points before the current retained `first_seq`.
    #[allow(clippy::too_many_arguments)]
    pub fn poll_live_log<S, C, F>(
        &self,
        store: &S,
        control: &C,
        name: &str,
        cursor: LiveLogCursor,
        max_batches: usize,
        open_op_id: OperationId,
        protection_id: ProtectionId,
        close_op_id: OperationId,
        next_renewal_id: F,
    ) -> Result<(Vec<LogBatchView>, LiveLogCursor), LogError>
    where
        S: MapObjectStore,
        C: NativeControl,
        F: FnMut() -> OperationId,
    {
        if name.is_empty() {
            return Err(LogError::InvalidName);
        }
        ensure_store_scope(store, control)?;
        let branch = control.get_branch(cursor.stream_id.branch_id)?;
        if branch.stream_epoch != cursor.stream_id.stream_epoch {
            return Err(LogError::StreamReplaced {
                expected_epoch: cursor.stream_id.stream_epoch,
                actual_epoch: branch.stream_epoch,
            });
        }
        let deadline = control.open_live_log(
            open_op_id,
            branch.branch_id,
            branch.revision,
            branch.logs_root,
            protection_id,
        )?;
        let mut guard = ReadProtectionGuard::new(control, protection_id, deadline, next_renewal_id);
        let read_result = (|| {
            guard.ensure_live()?;
            let log_map = LogMap::open(store, branch.logs_root)?;
            let Some(entry) = log_map.get(store, name)? else {
                return Err(LogError::StreamReplaced {
                    expected_epoch: cursor.stream_id.stream_epoch,
                    actual_epoch: branch.stream_epoch,
                });
            };
            if entry.log_id != cursor.stream_id.log_id {
                return Err(LogError::StreamReplaced {
                    expected_epoch: cursor.stream_id.stream_epoch,
                    actual_epoch: branch.stream_epoch,
                });
            }
            if cursor.next_seq < entry.first_seq {
                return Err(LogError::CursorExpired {
                    cursor_seq: cursor.next_seq,
                    first_seq: entry.first_seq,
                });
            }
            let batches = read_batches_from_entry_with_guard(
                store,
                entry,
                cursor.next_seq,
                max_batches,
                &mut || guard.ensure_live(),
            )?;
            let next_seq = batches
                .last()
                .map_or(cursor.next_seq, |batch| batch.end_seq);
            Ok((
                batches,
                LiveLogCursor {
                    stream_id: cursor.stream_id,
                    next_seq,
                },
            ))
        })();
        let _ = control.close_protection(close_op_id, protection_id);
        read_result
    }
}

/// Reads verified batches from a specific immutable `logs_root` without mounting.
pub fn read_log_from_root<S: MapObjectStore>(
    store: &S,
    logs_root: ObjectRef,
    name: &str,
    from_seq: u64,
    max_batches: usize,
) -> Result<(LogView, Vec<LogBatchView>), LogError> {
    read_log_from_root_with_guard(
        store,
        logs_root,
        name,
        from_seq,
        max_batches,
        &mut || Ok(()),
    )
}

fn read_log_from_root_with_guard<S: MapObjectStore>(
    store: &S,
    logs_root: ObjectRef,
    name: &str,
    from_seq: u64,
    max_batches: usize,
    ensure_protected: &mut dyn FnMut() -> Result<(), ControlError>,
) -> Result<(LogView, Vec<LogBatchView>), LogError> {
    ensure_protected()?;
    let log_map = LogMap::open(store, logs_root)?;
    let Some(entry) = log_map.get(store, name)? else {
        return Err(LogError::LogNotFound(name.to_owned()));
    };
    if from_seq < entry.first_seq {
        return Err(LogError::CursorExpired {
            cursor_seq: from_seq,
            first_seq: entry.first_seq,
        });
    }
    let batches =
        read_batches_from_entry_with_guard(store, entry, from_seq, max_batches, ensure_protected)?;
    Ok((LogView::from_entry(name, entry), batches))
}

/// Lists all named logs in an immutable `logs_root` without mounting.
pub fn list_logs_from_root<S: MapObjectStore>(
    store: &S,
    logs_root: ObjectRef,
) -> Result<Vec<LogView>, LogError> {
    let log_map = LogMap::open(store, logs_root)?;
    Ok(log_map
        .entries(store)?
        .into_iter()
        .map(|(name, entry)| LogView::from_entry(name, entry))
        .collect())
}

fn read_batches_from_entry_with_guard<S: MapObjectStore>(
    store: &S,
    entry: LogMapEntry,
    from_seq: u64,
    max_batches: usize,
    ensure_protected: &mut dyn FnMut() -> Result<(), ControlError>,
) -> Result<Vec<LogBatchView>, LogError> {
    if max_batches == 0 || from_seq >= entry.end_seq {
        return Ok(Vec::new());
    }
    ensure_protected()?;
    let seg_index = LogSegmentIndex::from_root(entry.segment_index_root)?;
    let selected = seg_index.entries_from(store, from_seq, max_batches)?;
    let mut batches = Vec::with_capacity(selected.len());
    for (seg_first, seg_entry) in selected {
        ensure_protected()?;
        let bytes = store
            .get_ref(seg_entry.segment)
            .map_err(LogError::ObjectStore)?
            .ok_or(ViewError::MissingObject(seg_entry.segment.object_id))?;
        seg_entry.segment.verify(&bytes)?;
        let segment = LogSegment::decode(&bytes)?;
        if segment.log_id != entry.log_id
            || segment.first_seq != seg_first
            || segment.end_seq != seg_entry.end_seq
        {
            return Err(FormatError::InvalidLogSegment.into());
        }
        let (first_seq, records) = if from_seq > segment.first_seq {
            let skip = usize::try_from(from_seq - segment.first_seq)
                .map_err(|_| FormatError::InvalidLogSegment)?;
            if skip >= segment.records.len() {
                return Err(FormatError::InvalidLogSegment.into());
            }
            let mut records = segment.records;
            let suffix = records.split_off(skip);
            (from_seq, suffix)
        } else {
            (segment.first_seq, segment.records)
        };
        batches.push(LogBatchView {
            log_id: segment.log_id,
            first_seq,
            end_seq: segment.end_seq,
            records,
            attachments: segment.attachments,
        });
    }
    Ok(batches)
}

fn validate_append_batch(records: &[Vec<u8>], attachments: &[ObjectRef]) -> Result<(), LogError> {
    if records.is_empty() {
        return Err(LogError::EmptyBatch);
    }
    if records.len() > MAX_LOG_RECORDS {
        return Err(LogError::TooManyRecords {
            actual: records.len(),
            maximum: MAX_LOG_RECORDS,
        });
    }
    let encoded_record_bytes = records.iter().try_fold(0_usize, |total, record| {
        total
            .checked_add(std::mem::size_of::<u32>())
            .and_then(|total| total.checked_add(record.len()))
            .ok_or(LogError::BatchTooLarge {
                actual: usize::MAX,
                maximum: MAX_LOG_RECORD_BYTES,
            })
    })?;
    if encoded_record_bytes > MAX_LOG_RECORD_BYTES {
        return Err(LogError::BatchTooLarge {
            actual: encoded_record_bytes,
            maximum: MAX_LOG_RECORD_BYTES,
        });
    }
    for attachment in attachments {
        if !matches!(
            attachment.object_type,
            ObjectType::ArtifactBlob | ObjectType::ArtifactChunk
        ) {
            return Err(LogError::InvalidAttachment);
        }
    }
    Ok(())
}

fn verify_attachment_objects_in_store<S: MapObjectStore>(
    store: &S,
    attachments: &[ObjectRef],
) -> Result<(), LogError> {
    for attachment in attachments {
        let bytes = store
            .get_ref(*attachment)
            .map_err(LogError::ObjectStore)?
            .ok_or(ViewError::MissingObject(attachment.object_id))?;
        attachment.verify(&bytes)?;
        match attachment.object_type {
            ObjectType::ArtifactBlob => {
                decode_artifact_blob(&bytes)?;
            }
            ObjectType::ArtifactChunk => {
                decode_artifact_chunk(&bytes)?;
            }
            _ => return Err(LogError::InvalidAttachment),
        }
    }
    Ok(())
}

pub(crate) fn sha256_domain(domain: &[u8], payload: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(domain);
    hasher.update([0]);
    hasher.update(payload);
    hasher.finalize().into()
}

pub(crate) fn push_len_bytes(buf: &mut Vec<u8>, bytes: &[u8]) {
    buf.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
    buf.extend_from_slice(bytes);
}

pub(crate) fn push_object_ref(buf: &mut Vec<u8>, reference: ObjectRef) {
    buf.extend_from_slice(&(reference.object_type as u16).to_le_bytes());
    buf.extend_from_slice(reference.object_id.as_bytes());
    buf.extend_from_slice(&reference.encoded_len.to_le_bytes());
}

fn fingerprint_create_log(branch_id: BranchId, name: &str, log_id: LogId) -> [u8; 32] {
    let mut buf = Vec::new();
    buf.extend_from_slice(branch_id.as_bytes());
    push_len_bytes(&mut buf, name.as_bytes());
    buf.extend_from_slice(log_id.as_bytes());
    sha256_domain(b"versionfs.native.create_log.v1", &buf)
}

fn fingerprint_append(
    branch_id: BranchId,
    name: &str,
    stream_id: StreamId,
    match_seq_num: u64,
    segment_ref: ObjectRef,
) -> [u8; 32] {
    let mut buf = Vec::new();
    buf.extend_from_slice(branch_id.as_bytes());
    push_len_bytes(&mut buf, name.as_bytes());
    buf.extend_from_slice(stream_id.branch_id.as_bytes());
    buf.extend_from_slice(&stream_id.stream_epoch.to_le_bytes());
    buf.extend_from_slice(stream_id.log_id.as_bytes());
    buf.extend_from_slice(&match_seq_num.to_le_bytes());
    push_object_ref(&mut buf, segment_ref);
    sha256_domain(b"versionfs.native.append.v1", &buf)
}

fn fingerprint_trim_log(
    branch_id: BranchId,
    name: &str,
    stream_id: StreamId,
    trim_to_seq: u64,
) -> [u8; 32] {
    let mut buf = Vec::new();
    buf.extend_from_slice(branch_id.as_bytes());
    push_len_bytes(&mut buf, name.as_bytes());
    buf.extend_from_slice(stream_id.branch_id.as_bytes());
    buf.extend_from_slice(&stream_id.stream_epoch.to_le_bytes());
    buf.extend_from_slice(stream_id.log_id.as_bytes());
    buf.extend_from_slice(&trim_to_seq.to_le_bytes());
    sha256_domain(b"versionfs.native.trim_log.v1", &buf)
}

fn fingerprint_delete_log(branch_id: BranchId, name: &str, stream_id: StreamId) -> [u8; 32] {
    let mut buf = Vec::new();
    buf.extend_from_slice(branch_id.as_bytes());
    push_len_bytes(&mut buf, name.as_bytes());
    buf.extend_from_slice(stream_id.branch_id.as_bytes());
    buf.extend_from_slice(&stream_id.stream_epoch.to_le_bytes());
    buf.extend_from_slice(stream_id.log_id.as_bytes());
    sha256_domain(b"versionfs.native.delete_log.v1", &buf)
}

fn encode_append_receipt_meta(
    stream_id: StreamId,
    start_seq: u64,
    end_seq: u64,
    segment_ref: ObjectRef,
) -> Vec<u8> {
    let mut buf = Vec::with_capacity(16 + 8 + 16 + 8 + 8 + 2 + 32 + 8);
    buf.extend_from_slice(stream_id.branch_id.as_bytes());
    buf.extend_from_slice(&stream_id.stream_epoch.to_le_bytes());
    buf.extend_from_slice(stream_id.log_id.as_bytes());
    buf.extend_from_slice(&start_seq.to_le_bytes());
    buf.extend_from_slice(&end_seq.to_le_bytes());
    push_object_ref(&mut buf, segment_ref);
    buf
}

fn decode_append_receipt_meta(
    bytes: &[u8],
) -> Result<(StreamId, u64, u64, ObjectRef), ControlError> {
    const EXPECTED_LEN: usize = 16 + 8 + 16 + 8 + 8 + 2 + 32 + 8;
    if bytes.len() != EXPECTED_LEN {
        return Err(ControlError::CorruptRecord(
            "invalid append receipt metadata length".into(),
        ));
    }
    let branch_id = BranchId::from_bytes(bytes[0..16].try_into().unwrap());
    let stream_epoch = u64::from_le_bytes(bytes[16..24].try_into().unwrap());
    let log_id = LogId::from_bytes(bytes[24..40].try_into().unwrap());
    let start_seq = u64::from_le_bytes(bytes[40..48].try_into().unwrap());
    let end_seq = u64::from_le_bytes(bytes[48..56].try_into().unwrap());
    let raw_type = u16::from_le_bytes(bytes[56..58].try_into().unwrap());
    let object_type = ObjectType::try_from(raw_type)
        .map_err(|_| ControlError::CorruptRecord("invalid append receipt object type".into()))?;
    let object_id = ObjectId::from_bytes(bytes[58..90].try_into().unwrap());
    let encoded_len = u64::from_le_bytes(bytes[90..98].try_into().unwrap());
    let segment_ref = ObjectRef {
        object_id,
        object_type,
        encoded_len,
    };
    Ok((
        StreamId {
            branch_id,
            stream_epoch,
            log_id,
        },
        start_seq,
        end_seq,
        segment_ref,
    ))
}

fn verify_committed_create_log_replay(
    transition: &CommittedRootTransition,
    branch_id: BranchId,
    log_id: LogId,
    expected_fp: [u8; 32],
) -> Result<(StreamId, BranchState), LogError> {
    let Some(meta) = &transition.native_meta else {
        return Err(LogError::Control(ControlError::OperationMismatch));
    };
    if transition.kind != PreparationKind::LogMutation
        || transition.branch.branch_id != branch_id
        || meta.request_fingerprint != expected_fp
    {
        return Err(LogError::Control(ControlError::OperationMismatch));
    }
    Ok((
        StreamId {
            branch_id: transition.branch.branch_id,
            stream_epoch: transition.branch.stream_epoch,
            log_id,
        },
        transition.branch.clone(),
    ))
}

fn verify_committed_append_replay(
    transition: &CommittedRootTransition,
    stream_id: StreamId,
    append_id: OperationId,
    expected_fp: [u8; 32],
) -> Result<AppendReceipt, LogError> {
    let Some(meta) = &transition.native_meta else {
        return Err(LogError::Control(ControlError::OperationMismatch));
    };
    if transition.kind != PreparationKind::LogMutation
        || transition.branch.branch_id != stream_id.branch_id
        || transition.branch.stream_epoch != stream_id.stream_epoch
        || meta.request_fingerprint != expected_fp
    {
        return Err(LogError::Control(ControlError::OperationMismatch));
    }
    let (recorded_stream, start_seq, end_seq, _segment_ref) =
        decode_append_receipt_meta(&meta.receipt_bytes)?;
    if recorded_stream != stream_id {
        return Err(LogError::Control(ControlError::OperationMismatch));
    }
    Ok(AppendReceipt {
        stream_id: recorded_stream,
        append_id,
        start_seq,
        end_seq,
        branch_revision: transition.branch.revision,
    })
}

fn verify_committed_trim_log_replay(
    transition: &CommittedRootTransition,
    name: &str,
    stream_id: StreamId,
    expected_fp: [u8; 32],
) -> Result<(LogView, BranchState), LogError> {
    let Some(meta) = &transition.native_meta else {
        return Err(LogError::Control(ControlError::OperationMismatch));
    };
    if transition.kind != PreparationKind::LogMutation
        || transition.branch.branch_id != stream_id.branch_id
        || transition.branch.stream_epoch != stream_id.stream_epoch
        || meta.request_fingerprint != expected_fp
    {
        return Err(LogError::Control(ControlError::OperationMismatch));
    }
    let entry = LogMapEntry::decode(&meta.receipt_bytes)?;
    Ok((LogView::from_entry(name, entry), transition.branch.clone()))
}

fn verify_committed_delete_log_replay(
    transition: &CommittedRootTransition,
    stream_id: StreamId,
    expected_fp: [u8; 32],
) -> Result<BranchState, LogError> {
    let Some(meta) = &transition.native_meta else {
        return Err(LogError::Control(ControlError::OperationMismatch));
    };
    if transition.kind != PreparationKind::LogMutation
        || transition.branch.branch_id != stream_id.branch_id
        || transition.branch.stream_epoch != stream_id.stream_epoch
        || meta.request_fingerprint != expected_fp
    {
        return Err(LogError::Control(ControlError::OperationMismatch));
    }
    Ok(transition.branch.clone())
}
