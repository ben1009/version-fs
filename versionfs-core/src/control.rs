//! Backend-neutral control records and branch authority protocols.
//!
//! This module performs metadata-only state transitions. Object uploads,
//! filesystem/device operations, and external effects remain outside authority
//! transactions. Callers supply stable IDs before issuing any mutation.

use std::sync::Arc;

use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{
    format::{
        CanonicalDecoder, CanonicalEncoder, FormatError, MetadataRecordType, ObjectRef, ObjectType,
        decode_metadata_record, encode_metadata_record,
    },
    identity::{
        AttachmentId, BranchId, GenerationId, OperationId, PinId, PrincipalId, ProtectionId,
        WorkspaceId,
    },
    metadata::{
        MetadataAuthority, MetadataError, MetadataTxn, MutationIdentity, ReconciledOutcome,
    },
    time::MonotonicTime,
};

/// Initial renewable writer lease (30 seconds).
pub const WRITER_LEASE_NANOS: u64 = 30_000_000_000;
/// Attachment-base protection lifetime (five minutes).
pub const ATTACHMENT_PROTECTION_NANOS: u64 = 300_000_000_000;
/// Historical/live reader protection lifetime (five minutes).
pub const READER_PROTECTION_NANOS: u64 = 300_000_000_000;
/// Protected preparation inactivity lifetime (ten minutes).
pub const PREPARATION_LEASE_NANOS: u64 = 600_000_000_000;

const KEY_PREFIX: &[u8] = b"\x01versionfs\x00\x01";
const REJECT_PREFIX: &str = "versionfs-control:";
const OP_OWNER_START: u16 = 1;
const OP_CREATE_WORKSPACE: u16 = 2;
const OP_REGISTER_GENERATION: u16 = 3;
const OP_CREATE_BRANCH: u16 = 4;
const OP_ACQUIRE_WRITER: u16 = 5;
const OP_RENEW_WRITER: u16 = 6;
const OP_RELEASE_WRITER: u16 = 7;
const OP_ATTACH: u16 = 8;
const OP_RENEW_ATTACHMENT: u16 = 9;
const OP_DETACH: u16 = 10;
const OP_PIN: u16 = 11;
const OP_UNPIN: u16 = 12;
const OP_OPEN_GENERATION: u16 = 13;
const OP_OPEN_LIVE_LOG: u16 = 14;
const OP_RENEW_PROTECTION: u16 = 15;
const OP_CLOSE_PROTECTION: u16 = 16;
const OP_BEGIN_PREPARATION: u16 = 17;
const OP_RENEW_PREPARATION: u16 = 18;
const OP_AUTHORIZE_CAPTURE_CHILD: u16 = 19;
const OP_COMMIT_LIVE_ROOTS: u16 = 20;
const OP_SEAL_CAPTURE: u16 = 21;
const OP_MARK_UNRETAINED: u16 = 23;
const OP_EXPIRE_PROTECTION: u16 = 24;
const TRANSITION_BEGIN: u16 = 1;
const TRANSITION_COMMIT_LIVE_ROOTS: u16 = 2;
const TRANSITION_ABORT: u16 = 3;
const TRANSITION_EXPIRE_PREPARATION: u16 = 4;
const OP_TOMBSTONE: u16 = u16::MAX;

/// A durable authority-owner incarnation. Every owner restart advances it.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct OwnerIncarnation(u64);

impl OwnerIncarnation {
    /// Reconstructs an incarnation from its durable fixed-width value.
    pub const fn from_u64(value: u64) -> Self {
        Self(value)
    }

    /// Returns the monotone value persisted by start_authority_owner.
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Errors returned by the backend-neutral control protocol.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum ControlError {
    /// A point precondition or exact revision/epoch did not match.
    #[error("control precondition conflict")]
    Conflict,
    /// The operation ID was reused with a different request fingerprint.
    #[error("operation ID was reused with a different request")]
    OperationMismatch,
    /// The operation was durably aborted or tombstoned.
    #[error("operation was aborted")]
    Aborted,
    /// The operation is still in its protected preparation phase.
    #[error("operation is still in progress")]
    InProgress,
    /// The requested record does not exist or is no longer visible.
    #[error("control record not found")]
    NotFound,
    /// The grant belongs to an older owner or writer epoch.
    #[error("writer or attachment is fenced")]
    Fenced,
    /// The exact writer lease or protection has expired.
    #[error("writer lease or protection expired")]
    Expired,
    /// Another writer, attachment, capture, or child operation owns admission.
    #[error("branch is busy")]
    Busy,
    /// A Generation is no longer eligible for new protection.
    #[error("Generation is not retained")]
    GenerationExpired,
    /// The requested state transition is not valid from the stored phase.
    #[error("invalid control-state transition")]
    InvalidTransition,
    /// A required ID, name, or fixed-format field is invalid.
    #[error("invalid control request: {0}")]
    InvalidRequest(&'static str),
    /// A durable record failed canonical decoding or consistency checks.
    #[error("invalid durable control record: {0}")]
    CorruptRecord(String),
    /// A caller supplied a field that cannot be represented by the canonical format.
    #[error(transparent)]
    Format(#[from] FormatError),
    /// The metadata backend returned an error.
    #[error(transparent)]
    Metadata(#[from] MetadataError),
}

/// The complete state visible for one stable branch identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BranchState {
    /// Workspace containing this branch.
    pub workspace_id: WorkspaceId,
    /// Stable branch identity. Names may later be reused by another ID.
    pub branch_id: BranchId,
    /// Current mutable name.
    pub name: String,
    /// Published Generation head.
    pub head: GenerationId,
    /// Revision incremented by head and live-root changes.
    pub revision: u64,
    /// Number of admitted non-capture mutations still in preparation.
    /// Checkpoint capture waits for this count to reach zero.
    pub pending_mutation_preparations: u64,
    /// Writer fencing epoch.
    pub writer_epoch: u64,
    /// Block-volume fencing epoch.
    pub volume_epoch: u64,
    /// Native-stream fencing epoch.
    pub stream_epoch: u64,
    /// Current authoritative live block-volume root.
    pub volume_root: ObjectRef,
    /// Current authoritative live-log-map root.
    pub logs_root: ObjectRef,
    /// Current authoritative live-artifact-map root.
    pub artifacts_root: ObjectRef,
    /// Current writer lease, if any.
    pub writer: Option<WriterGrant>,
    /// Active writable attachment, if any.
    pub attachment: Option<AttachmentState>,
    /// Active capture token, if any.
    pub capture: Option<CaptureState>,
    /// Deleted branches remain as identity tombstones and are not addressable.
    pub deleted: bool,
}

/// Stable writer authority returned by acquire and renewed by exact grant.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WriterGrant {
    /// Branch this grant controls.
    pub branch_id: BranchId,
    /// Monotone writer fencing epoch.
    pub writer_epoch: u64,
    /// Authority incarnation that issued the grant.
    pub owner_incarnation: OwnerIncarnation,
    /// Acquisition operation ID; remains stable across renewals.
    pub grant_id: OperationId,
    /// Authority-clock lease deadline.
    pub expires_at: MonotonicTime,
}

/// Writable attachment and its retained Generation base.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AttachmentState {
    /// Stable device attachment identity.
    pub attachment_id: AttachmentId,
    /// Stable protection identity for the selected base roots.
    pub protection_id: ProtectionId,
    /// Generation from which this attachment was opened.
    pub base_generation: GenerationId,
    /// Writer epoch under which it was attached.
    pub writer_epoch: u64,
    /// Block-volume epoch under which it was attached.
    pub volume_epoch: u64,
    /// Current attachment-base protection deadline.
    pub protection_expires_at: MonotonicTime,
}

/// Capture token state used to gate head and live-root mutations.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CaptureState {
    /// Checkpoint operation that owns this token.
    pub operation_id: OperationId,
    /// Authenticated caller scope that owns the capture and its children.
    pub principal_id: PrincipalId,
    /// Protection record that keeps this capture's dependencies reachable.
    pub protection_id: ProtectionId,
    /// Exact branch revision captured before producer drain.
    pub expected_revision: u64,
    /// Capture is open to identified drain children until sealed.
    pub sealed: bool,
    /// At most one admitted drain child is pending in the v0 branch stream.
    pub pending_child: Option<OperationId>,
    /// Revision/root tuple recorded when sealing completed.
    pub sealed_revision: Option<u64>,
    /// Live roots recorded at the sealed boundary.
    pub sealed_roots: Option<LiveRoots>,
}

/// Roots atomically advanced by a durable branch mutation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LiveRoots {
    /// Durable block-volume root.
    pub volume: ObjectRef,
    /// Durable native log-map root.
    pub logs: ObjectRef,
    /// Durable native artifact-map root.
    pub artifacts: ObjectRef,
}

/// A trusted Generation catalog entry supplied only after its graph is durable.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GenerationSeed {
    /// Immutable Generation identity.
    pub generation_id: GenerationId,
    /// Durable descriptor object.
    pub descriptor: ObjectRef,
    /// Complete roots captured by the descriptor.
    pub roots: LiveRoots,
}

/// Durable operation state returned by query and abort APIs.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OperationState {
    /// Operation is still protecting prepared roots or dependencies.
    Preparing {
        /// Stable operation ID.
        operation_id: OperationId,
        /// Current inactivity deadline.
        expires_at: MonotonicTime,
        /// Branch whose root is protected.
        branch_id: BranchId,
    },
    /// Operation committed with these stable result bytes.
    Committed(Vec<u8>),
    /// Operation cannot execute again.
    Aborted,
}

/// Type tag for a protected remote-I/O preparation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum PreparationKind {
    /// Volume FLUSH/FUA or another durable block-root replacement.
    VolumeBarrier = 1,
    /// Native log create, append, trim, or delete.
    LogMutation = 2,
    /// Artifact upload or checkpoint artifact capture.
    ArtifactMutation = 3,
    /// Common-cut Generation capture.
    CheckpointCapture = 4,
}

/// Inputs for registering a protected remote-I/O preparation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparationRequest {
    /// Stable operation ID for this preparation.
    pub operation_id: OperationId,
    /// Branch whose state will be changed or captured.
    pub branch_id: BranchId,
    /// Exact branch revision admitted for this operation.
    pub expected_revision: u64,
    /// Current writer fencing grant.
    pub grant: WriterGrant,
    /// Writable attachment participating in the preparation.
    pub attachment_id: AttachmentId,
    /// Exact block-volume epoch returned for this attachment.
    pub volume_epoch: u64,
    /// Stable identity for the protection created with the preparation.
    pub protection_id: ProtectionId,
    /// Kind of remote-I/O preparation.
    pub kind: PreparationKind,
    /// Resulting roots for mutations; checkpoint capture has no replacement roots.
    pub expected_roots: Option<LiveRoots>,
    /// Immutable objects that must remain reachable while the preparation is open.
    pub dependencies: Vec<ObjectRef>,
}

/// Advances the durable single-owner incarnation and returns this owner identity.
///
/// The operation ID must be recorded before sending the request and reused if
/// its reply is lost. Reopening with a new ID fences every previous owner grant.
pub fn start_authority_owner<A: MetadataAuthority>(
    authority: &A,
    operation_id: OperationId,
) -> Result<OwnerIncarnation, ControlError> {
    let key = owner_operation_key(operation_id);
    let request_fingerprint = fingerprint(OP_OWNER_START, &[]);
    if let Some(bytes) = reconcile_outcome(authority, &key)? {
        return decode_owner_start_result(&bytes, request_fingerprint);
    }
    let outcome = match authority.transact(
        MutationIdentity {
            operation_id: &key,
            outcome_key: &key,
        },
        |txn| {
            if let Some(value) = txn.get(&key)? {
                let record = decode_operation(&value).map_err(to_metadata_error)?;
                check_fingerprint(&record, OP_OWNER_START, request_fingerprint)
                    .map_err(to_metadata_error)?;
                return match record.status {
                    StoredOperationStatus::Committed(bytes) => Ok(bytes),
                    StoredOperationStatus::Aborted => Err(reject("aborted")),
                    StoredOperationStatus::Preparing { .. } => Err(reject("in_progress")),
                };
            }
            let current = txn
                .get(&owner_incarnation_key())?
                .map(|value| decode_owner(&value).map_err(to_metadata_error))
                .transpose()?
                .unwrap_or(0);
            let next = current.checked_add(1).ok_or_else(|| reject("overflow"))?;
            txn.put(
                &owner_incarnation_key(),
                &encode_owner(next).map_err(to_metadata_error)?,
            )?;
            let bytes = next.to_le_bytes().to_vec();
            let record =
                StoredOperation::committed(OP_OWNER_START, request_fingerprint, bytes.clone());
            txn.put(&key, &encode_operation(&record).map_err(to_metadata_error)?)?;
            Ok(bytes)
        },
    ) {
        Ok(bytes) => bytes,
        Err(MetadataError::Conflict) => {
            if let Some(bytes) = reconcile_outcome(authority, &key)? {
                return decode_owner_start_result(&bytes, request_fingerprint);
            }
            return Err(ControlError::Conflict);
        }
        Err(error) => return Err(map_metadata_error(error)),
    };
    let value: [u8; 8] = outcome
        .as_slice()
        .try_into()
        .map_err(|_| ControlError::CorruptRecord("invalid owner operation result".into()))?;
    Ok(OwnerIncarnation(u64::from_le_bytes(value)))
}

fn decode_owner_start_result(
    bytes: &[u8],
    request_fingerprint: [u8; 32],
) -> Result<OwnerIncarnation, ControlError> {
    let record = decode_operation(bytes)?;
    check_fingerprint(&record, OP_OWNER_START, request_fingerprint)?;
    let StoredOperationStatus::Committed(value) = record.status else {
        return Err(ControlError::CorruptRecord(
            "owner-start operation is not committed".into(),
        ));
    };
    let value: [u8; 8] = value
        .as_slice()
        .try_into()
        .map_err(|_| ControlError::CorruptRecord("invalid owner operation result".into()))?;
    Ok(OwnerIncarnation(u64::from_le_bytes(value)))
}

/// Workspace-scoped control protocol over any conforming Metadata Authority.
pub struct WorkspaceControl<A> {
    authority: Arc<A>,
    workspace_id: WorkspaceId,
    principal_id: PrincipalId,
    owner_incarnation: OwnerIncarnation,
}

impl<A: MetadataAuthority> WorkspaceControl<A> {
    /// Creates a client for one workspace, caller scope, and owner epoch.
    ///
    /// This handle enforces protocol fencing, not user authorization. Callers
    /// must authenticate and authorize the workspace and principal before
    /// constructing it; its IDs are scope inputs, not credentials.
    pub fn new(
        authority: Arc<A>,
        workspace_id: WorkspaceId,
        principal_id: PrincipalId,
        owner_incarnation: OwnerIncarnation,
    ) -> Self {
        Self {
            authority,
            workspace_id,
            principal_id,
            owner_incarnation,
        }
    }

    /// Creates a workspace and its first branch after the initial Generation is durable.
    pub fn create_workspace(
        &self,
        operation_id: OperationId,
        main_branch_id: BranchId,
        main_name: &str,
        retention_period_ms: u64,
        initial: GenerationSeed,
    ) -> Result<BranchState, ControlError> {
        validate_name(main_name)?;
        validate_generation_seed(initial)?;
        if retention_period_ms == 0 {
            return Err(ControlError::InvalidRequest(
                "retention period must be positive",
            ));
        }
        let mut payload = CanonicalEncoder::new();
        payload.fixed(self.workspace_id.as_bytes());
        payload.fixed(main_branch_id.as_bytes());
        payload.bytes(main_name.as_bytes())?;
        payload.u64_le(retention_period_ms);
        encode_generation_seed(&mut payload, initial);
        let result =
            self.run_atomic(operation_id, OP_CREATE_WORKSPACE, payload.finish(), |txn| {
                if txn.get(&workspace_key(self.workspace_id))?.is_some()
                    || txn
                        .get(&branch_key(self.workspace_id, main_branch_id))?
                        .is_some()
                    || txn
                        .get(&branch_name_key(self.workspace_id, main_name))?
                        .is_some()
                    || txn
                        .get(&generation_key(self.workspace_id, initial.generation_id))?
                        .is_some()
                {
                    return Err(MetadataError::Conflict);
                }
                txn.put(
                    &workspace_key(self.workspace_id),
                    &encode_workspace(self.workspace_id, retention_period_ms)
                        .map_err(to_metadata_error)?,
                )?;
                let mut generation = GenerationRecord::new(initial, true);
                generation.branch_refs = 1;
                txn.put(
                    &generation_key(self.workspace_id, initial.generation_id),
                    &encode_generation(&generation).map_err(to_metadata_error)?,
                )?;
                let branch = BranchState {
                    workspace_id: self.workspace_id,
                    branch_id: main_branch_id,
                    name: main_name.to_owned(),
                    head: initial.generation_id,
                    revision: 1,
                    pending_mutation_preparations: 0,
                    writer_epoch: 0,
                    volume_epoch: 1,
                    stream_epoch: 1,
                    volume_root: initial.roots.volume,
                    logs_root: initial.roots.logs,
                    artifacts_root: initial.roots.artifacts,
                    writer: None,
                    attachment: None,
                    capture: None,
                    deleted: false,
                };
                txn.put(
                    &branch_key(self.workspace_id, main_branch_id),
                    &encode_branch(&branch).map_err(to_metadata_error)?,
                )?;
                txn.put(
                    &branch_name_key(self.workspace_id, main_name),
                    &encode_name_binding(main_branch_id).map_err(to_metadata_error)?,
                )?;
                encode_branch(&branch).map_err(to_metadata_error)
            })?;
        decode_branch(&result)
    }

    /// Adds a durable, initially retained Generation catalog record.
    pub fn register_generation(
        &self,
        operation_id: OperationId,
        generation: GenerationSeed,
    ) -> Result<(), ControlError> {
        validate_generation_seed(generation)?;
        let mut payload = CanonicalEncoder::new();
        payload.fixed(self.workspace_id.as_bytes());
        encode_generation_seed(&mut payload, generation);
        self.run_atomic(
            operation_id,
            OP_REGISTER_GENERATION,
            payload.finish(),
            |txn| {
                if txn.get(&workspace_key(self.workspace_id))?.is_none() {
                    return Err(reject("not_found"));
                }
                let key = generation_key(self.workspace_id, generation.generation_id);
                if txn.get(&key)?.is_some() {
                    return Err(MetadataError::Conflict);
                }
                txn.put(
                    &key,
                    &encode_generation(&GenerationRecord::new(generation, true))
                        .map_err(to_metadata_error)?,
                )?;
                Ok(Vec::new())
            },
        )?;
        Ok(())
    }

    /// Creates a named branch from a retained Generation using an absent-key claim.
    pub fn create_branch(
        &self,
        operation_id: OperationId,
        branch_id: BranchId,
        name: &str,
        base: GenerationId,
    ) -> Result<BranchState, ControlError> {
        validate_name(name)?;
        let mut payload = CanonicalEncoder::new();
        payload.fixed(self.workspace_id.as_bytes());
        payload.fixed(branch_id.as_bytes());
        payload.bytes(name.as_bytes())?;
        payload.fixed(base.as_bytes());
        let result = self.run_atomic(operation_id, OP_CREATE_BRANCH, payload.finish(), |txn| {
            if txn.get(&workspace_key(self.workspace_id))?.is_none() {
                return Err(reject("not_found"));
            }
            let key = branch_key(self.workspace_id, branch_id);
            let name_key = branch_name_key(self.workspace_id, name);
            if txn.get(&key)?.is_some() || txn.get(&name_key)?.is_some() {
                return Err(MetadataError::Conflict);
            }
            let generation_key = generation_key(self.workspace_id, base);
            let mut generation = read_generation(txn, &generation_key)?;
            if !generation.eligible() {
                return Err(reject("generation_expired"));
            }
            generation.branch_refs = increment(generation.branch_refs)?;
            let branch = BranchState {
                workspace_id: self.workspace_id,
                branch_id,
                name: name.to_owned(),
                head: base,
                revision: 1,
                pending_mutation_preparations: 0,
                writer_epoch: 0,
                volume_epoch: 1,
                stream_epoch: 1,
                volume_root: generation.seed.roots.volume,
                logs_root: generation.seed.roots.logs,
                artifacts_root: generation.seed.roots.artifacts,
                writer: None,
                attachment: None,
                capture: None,
                deleted: false,
            };
            txn.put(
                &generation_key,
                &encode_generation(&generation).map_err(to_metadata_error)?,
            )?;
            txn.put(&key, &encode_branch(&branch).map_err(to_metadata_error)?)?;
            txn.put(
                &name_key,
                &encode_name_binding(branch_id).map_err(to_metadata_error)?,
            )?;
            encode_branch(&branch).map_err(to_metadata_error)
        })?;
        decode_branch(&result)
    }

    /// Reads a branch by stable ID.
    pub fn get_branch(&self, branch_id: BranchId) -> Result<BranchState, ControlError> {
        let bytes = self
            .authority
            .get(&branch_key(self.workspace_id, branch_id))?
            .ok_or(ControlError::NotFound)?;
        let branch = decode_branch(&bytes)?;
        if branch.deleted {
            return Err(ControlError::NotFound);
        }
        Ok(branch)
    }

    /// Resolves a branch name through its deterministic point-key index.
    pub fn branch_by_name(&self, name: &str) -> Result<BranchState, ControlError> {
        validate_name(name)?;
        let bytes = self
            .authority
            .get(&branch_name_key(self.workspace_id, name))?
            .ok_or(ControlError::NotFound)?;
        self.get_branch(decode_name_binding(&bytes)?)
    }

    /// Acquires a writer grant using exact revision and writer-epoch preconditions.
    pub fn acquire_writer(
        &self,
        operation_id: OperationId,
        branch_id: BranchId,
        expected_revision: u64,
        expected_writer_epoch: u64,
    ) -> Result<WriterGrant, ControlError> {
        let mut payload = CanonicalEncoder::new();
        payload.fixed(branch_id.as_bytes());
        payload.u64_le(expected_revision);
        payload.u64_le(expected_writer_epoch);
        let result = self.run_atomic(operation_id, OP_ACQUIRE_WRITER, payload.finish(), |txn| {
            self.require_current_owner(txn)?;
            let key = branch_key(self.workspace_id, branch_id);
            let mut branch = read_branch(txn, &key)?;
            ensure_live_branch(&branch)?;
            if branch.revision != expected_revision || branch.writer_epoch != expected_writer_epoch
            {
                return Err(MetadataError::Conflict);
            }
            if let Some(writer) = branch.writer
                && writer.owner_incarnation == self.owner_incarnation
                && writer.expires_at > txn.now()
            {
                return Err(reject("busy"));
            }
            if branch.capture.is_some() {
                return Err(reject("busy"));
            }
            fence_attachment(txn, self.workspace_id, &mut branch)?;
            branch.writer_epoch = increment(branch.writer_epoch)?;
            let grant = WriterGrant {
                branch_id,
                writer_epoch: branch.writer_epoch,
                owner_incarnation: self.owner_incarnation,
                grant_id: operation_id,
                expires_at: txn.now().saturating_add_nanos(WRITER_LEASE_NANOS),
            };
            branch.writer = Some(grant);
            txn.put(&key, &encode_branch(&branch).map_err(to_metadata_error)?)?;
            Ok(encode_writer_grant_result(grant))
        })?;
        decode_writer_grant_result(&result)
    }

    /// Renews a grant. Replaying the renewal operation returns its original deadline.
    pub fn renew_writer(
        &self,
        operation_id: OperationId,
        grant: WriterGrant,
    ) -> Result<WriterGrant, ControlError> {
        let mut payload = CanonicalEncoder::new();
        encode_writer_grant(&mut payload, grant);
        let result = self.run_atomic(operation_id, OP_RENEW_WRITER, payload.finish(), |txn| {
            self.require_current_owner(txn)?;
            let key = branch_key(self.workspace_id, grant.branch_id);
            let mut branch = read_branch(txn, &key)?;
            validate_writer(&branch, grant, self.owner_incarnation, txn.now())?;
            let renewed = WriterGrant {
                expires_at: txn.now().saturating_add_nanos(WRITER_LEASE_NANOS),
                ..grant
            };
            branch.writer = Some(renewed);
            txn.put(&key, &encode_branch(&branch).map_err(to_metadata_error)?)?;
            Ok(encode_writer_grant_result(renewed))
        })?;
        decode_writer_grant_result(&result)
    }

    /// Releases the exact current grant and fences its attachment.
    pub fn release_writer(
        &self,
        operation_id: OperationId,
        grant: WriterGrant,
    ) -> Result<(), ControlError> {
        let mut payload = CanonicalEncoder::new();
        encode_writer_grant(&mut payload, grant);
        self.run_atomic(operation_id, OP_RELEASE_WRITER, payload.finish(), |txn| {
            self.require_current_owner(txn)?;
            let key = branch_key(self.workspace_id, grant.branch_id);
            let mut branch = read_branch(txn, &key)?;
            validate_grant_identity(&branch, grant, self.owner_incarnation)?;
            if branch.capture.is_some() || branch.pending_mutation_preparations != 0 {
                return Err(reject("busy"));
            }
            fence_attachment(txn, self.workspace_id, &mut branch)?;
            branch.writer = None;
            branch.writer_epoch = increment(branch.writer_epoch)?;
            txn.put(&key, &encode_branch(&branch).map_err(to_metadata_error)?)?;
            Ok(Vec::new())
        })?;
        Ok(())
    }

    /// Opens one writable attachment and atomically protects its selected base roots.
    pub fn attach_volume(
        &self,
        operation_id: OperationId,
        branch_id: BranchId,
        expected_revision: u64,
        grant: WriterGrant,
        attachment_id: AttachmentId,
        protection_id: ProtectionId,
    ) -> Result<AttachmentState, ControlError> {
        let mut payload = CanonicalEncoder::new();
        payload.fixed(branch_id.as_bytes());
        payload.u64_le(expected_revision);
        encode_writer_grant(&mut payload, grant);
        payload.fixed(attachment_id.as_bytes());
        payload.fixed(protection_id.as_bytes());
        let result = self.run_atomic(operation_id, OP_ATTACH, payload.finish(), |txn| {
            self.require_current_owner(txn)?;
            let key = branch_key(self.workspace_id, branch_id);
            let mut branch = read_branch(txn, &key)?;
            validate_writer(&branch, grant, self.owner_incarnation, txn.now())?;
            if branch.revision != expected_revision {
                return Err(MetadataError::Conflict);
            }
            if branch.attachment.is_some()
                || branch.capture.is_some()
                || branch.pending_mutation_preparations != 0
            {
                return Err(reject("busy"));
            }
            let gen_key = generation_key(self.workspace_id, branch.head);
            let mut generation = read_generation(txn, &gen_key)?;
            if !generation.eligible() {
                return Err(reject("generation_expired"));
            }
            generation.attachment_refs = increment(generation.attachment_refs)?;
            let attachment = AttachmentState {
                attachment_id,
                protection_id,
                base_generation: branch.head,
                writer_epoch: branch.writer_epoch,
                volume_epoch: branch.volume_epoch,
                protection_expires_at: txn.now().saturating_add_nanos(ATTACHMENT_PROTECTION_NANOS),
            };
            let protection = ProtectionRecord::AttachmentBase {
                id: protection_id,
                branch_id,
                attachment_id,
                generation_id: branch.head,
                roots: LiveRoots {
                    volume: branch.volume_root,
                    logs: branch.logs_root,
                    artifacts: branch.artifacts_root,
                },
                owner_incarnation: self.owner_incarnation,
                expires_at: attachment.protection_expires_at,
            };
            if txn
                .get(&protection_key(self.workspace_id, protection_id))?
                .is_some()
            {
                return Err(MetadataError::Conflict);
            }
            branch.attachment = Some(attachment);
            txn.put(&key, &encode_branch(&branch).map_err(to_metadata_error)?)?;
            txn.put(
                &gen_key,
                &encode_generation(&generation).map_err(to_metadata_error)?,
            )?;
            txn.put(
                &protection_key(self.workspace_id, protection_id),
                &encode_protection(&protection).map_err(to_metadata_error)?,
            )?;
            Ok(encode_attachment_result(attachment))
        })?;
        decode_attachment_result(&result)
    }

    /// Renews the exact attachment base before expiry, without changing branch revision.
    pub fn renew_attachment_base(
        &self,
        operation_id: OperationId,
        branch_id: BranchId,
        attachment_id: AttachmentId,
        volume_epoch: u64,
        protection_id: ProtectionId,
        grant: WriterGrant,
    ) -> Result<AttachmentState, ControlError> {
        let mut payload = CanonicalEncoder::new();
        payload.fixed(branch_id.as_bytes());
        payload.fixed(attachment_id.as_bytes());
        payload.u64_le(volume_epoch);
        payload.fixed(protection_id.as_bytes());
        encode_writer_grant(&mut payload, grant);
        let result =
            self.run_atomic(operation_id, OP_RENEW_ATTACHMENT, payload.finish(), |txn| {
                self.require_current_owner(txn)?;
                let key = branch_key(self.workspace_id, branch_id);
                let mut branch = read_branch(txn, &key)?;
                validate_writer(&branch, grant, self.owner_incarnation, txn.now())?;
                let mut attachment = branch.attachment.ok_or_else(|| reject("fenced"))?;
                if attachment.attachment_id != attachment_id
                    || attachment.volume_epoch != volume_epoch
                    || attachment.protection_id != protection_id
                    || attachment.writer_epoch != grant.writer_epoch
                {
                    return Err(reject("fenced"));
                }
                let pkey = protection_key(self.workspace_id, attachment.protection_id);
                let mut protection = txn
                    .get(&pkey)?
                    .map(|v| decode_protection(&v).map_err(to_metadata_error))
                    .transpose()?
                    .ok_or_else(|| reject("expired"))?;
                match &mut protection {
                    ProtectionRecord::AttachmentBase {
                        branch_id: protected_branch,
                        attachment_id: protected_attachment,
                        generation_id,
                        owner_incarnation,
                        expires_at,
                        ..
                    } if *owner_incarnation == self.owner_incarnation
                        && *expires_at > txn.now() =>
                    {
                        if *protected_branch != branch_id
                            || *protected_attachment != attachment_id
                            || *generation_id != attachment.base_generation
                            || *expires_at != attachment.protection_expires_at
                        {
                            return Err(reject("corrupt_record"));
                        }
                        let generation = read_generation(
                            txn,
                            &generation_key(self.workspace_id, *generation_id),
                        )?;
                        if generation.attachment_refs == 0 {
                            return Err(reject("corrupt_record"));
                        }
                        *expires_at = txn.now().saturating_add_nanos(ATTACHMENT_PROTECTION_NANOS);
                        attachment.protection_expires_at = *expires_at;
                    }
                    _ => return Err(reject("expired")),
                }
                branch.attachment = Some(attachment);
                txn.put(&key, &encode_branch(&branch).map_err(to_metadata_error)?)?;
                txn.put(
                    &pkey,
                    &encode_protection(&protection).map_err(to_metadata_error)?,
                )?;
                Ok(encode_attachment_result(attachment))
            })?;
        decode_attachment_result(&result)
    }

    /// Detaches the exact clean, drained attachment and releases its base protection.
    pub fn detach_volume(
        &self,
        operation_id: OperationId,
        branch_id: BranchId,
        attachment_id: AttachmentId,
        volume_epoch: u64,
        protection_id: ProtectionId,
        grant: WriterGrant,
    ) -> Result<(), ControlError> {
        let mut payload = CanonicalEncoder::new();
        payload.fixed(branch_id.as_bytes());
        payload.fixed(attachment_id.as_bytes());
        payload.u64_le(volume_epoch);
        payload.fixed(protection_id.as_bytes());
        encode_writer_grant(&mut payload, grant);
        self.run_atomic(operation_id, OP_DETACH, payload.finish(), |txn| {
            self.require_current_owner(txn)?;
            let key = branch_key(self.workspace_id, branch_id);
            let mut branch = read_branch(txn, &key)?;
            validate_writer(&branch, grant, self.owner_incarnation, txn.now())?;
            if branch.attachment.is_none_or(|a| {
                a.attachment_id != attachment_id
                    || a.volume_epoch != volume_epoch
                    || a.protection_id != protection_id
            }) {
                return Err(reject("fenced"));
            }
            if branch.capture.is_some() || branch.pending_mutation_preparations != 0 {
                return Err(reject("busy"));
            }
            release_attachment(txn, self.workspace_id, &mut branch)?;
            txn.put(&key, &encode_branch(&branch).map_err(to_metadata_error)?)?;
            Ok(Vec::new())
        })?;
        Ok(())
    }

    /// Pins a retained Generation under a stable pin identity.
    pub fn pin_generation(
        &self,
        operation_id: OperationId,
        generation_id: GenerationId,
        pin_id: PinId,
    ) -> Result<(), ControlError> {
        let mut payload = CanonicalEncoder::new();
        payload.fixed(generation_id.as_bytes());
        payload.fixed(pin_id.as_bytes());
        self.run_atomic(operation_id, OP_PIN, payload.finish(), |txn| {
            let gkey = generation_key(self.workspace_id, generation_id);
            let mut generation = read_generation(txn, &gkey)?;
            if !generation.eligible() {
                return Err(reject("generation_expired"));
            }
            let pkey = pin_key(self.workspace_id, pin_id);
            if txn.get(&pkey)?.is_some() {
                return Err(MetadataError::Conflict);
            }
            generation.pin_refs = increment(generation.pin_refs)?;
            txn.put(
                &gkey,
                &encode_generation(&generation).map_err(to_metadata_error)?,
            )?;
            txn.put(
                &pkey,
                &encode_pin(PinRecord {
                    generation_id,
                    active: true,
                })
                .map_err(to_metadata_error)?,
            )?;
            Ok(Vec::new())
        })?;
        Ok(())
    }

    /// Releases one explicit Generation pin.
    pub fn unpin_generation(
        &self,
        operation_id: OperationId,
        pin_id: PinId,
    ) -> Result<(), ControlError> {
        let mut payload = CanonicalEncoder::new();
        payload.fixed(pin_id.as_bytes());
        self.run_atomic(operation_id, OP_UNPIN, payload.finish(), |txn| {
            let pkey = pin_key(self.workspace_id, pin_id);
            let mut pin = decode_pin(&txn.get(&pkey)?.ok_or_else(|| reject("not_found"))?)
                .map_err(to_metadata_error)?;
            if !pin.active {
                return Err(reject("invalid_transition"));
            }
            let gkey = generation_key(self.workspace_id, pin.generation_id);
            let mut generation = read_generation(txn, &gkey)?;
            generation.pin_refs = decrement(generation.pin_refs)?;
            txn.put(
                &gkey,
                &encode_generation(&generation).map_err(to_metadata_error)?,
            )?;
            // A pin identity is a permanent tombstone; it can never be reused.
            pin.active = false;
            txn.put(&pkey, &encode_pin(pin).map_err(to_metadata_error)?)?;
            Ok(Vec::new())
        })?;
        Ok(())
    }

    /// Opens a historical Generation and creates a renewable read protection.
    pub fn open_generation(
        &self,
        operation_id: OperationId,
        generation_id: GenerationId,
        protection_id: ProtectionId,
    ) -> Result<MonotonicTime, ControlError> {
        let mut payload = CanonicalEncoder::new();
        payload.fixed(generation_id.as_bytes());
        payload.fixed(protection_id.as_bytes());
        let result =
            self.run_atomic(operation_id, OP_OPEN_GENERATION, payload.finish(), |txn| {
                self.require_current_owner(txn)?;
                let gkey = generation_key(self.workspace_id, generation_id);
                let mut generation = read_generation(txn, &gkey)?;
                if !generation.eligible() {
                    return Err(reject("generation_expired"));
                }
                let pkey = protection_key(self.workspace_id, protection_id);
                if txn.get(&pkey)?.is_some() {
                    return Err(MetadataError::Conflict);
                }
                let expires = txn.now().saturating_add_nanos(READER_PROTECTION_NANOS);
                generation.reader_refs = increment(generation.reader_refs)?;
                let record = ProtectionRecord::GenerationRead {
                    id: protection_id,
                    generation_id,
                    owner_incarnation: self.owner_incarnation,
                    expires_at: expires,
                };
                txn.put(
                    &gkey,
                    &encode_generation(&generation).map_err(to_metadata_error)?,
                )?;
                txn.put(
                    &pkey,
                    &encode_protection(&record).map_err(to_metadata_error)?,
                )?;
                Ok(expires.as_nanos().to_le_bytes().to_vec())
            })?;
        decode_deadline(&result)
    }

    /// Opens a bounded protected view of the current live-log root.
    pub fn open_live_log(
        &self,
        operation_id: OperationId,
        branch_id: BranchId,
        expected_revision: u64,
        expected_root: ObjectRef,
        protection_id: ProtectionId,
    ) -> Result<MonotonicTime, ControlError> {
        let mut payload = CanonicalEncoder::new();
        payload.fixed(branch_id.as_bytes());
        payload.u64_le(expected_revision);
        payload.object_ref(expected_root);
        payload.fixed(protection_id.as_bytes());
        let result = self.run_atomic(operation_id, OP_OPEN_LIVE_LOG, payload.finish(), |txn| {
            self.require_current_owner(txn)?;
            let branch = read_branch(txn, &branch_key(self.workspace_id, branch_id))?;
            ensure_live_branch(&branch)?;
            if branch.revision != expected_revision || branch.logs_root != expected_root {
                return Err(MetadataError::Conflict);
            }
            let pkey = protection_key(self.workspace_id, protection_id);
            if txn.get(&pkey)?.is_some() {
                return Err(MetadataError::Conflict);
            }
            let expires = txn.now().saturating_add_nanos(READER_PROTECTION_NANOS);
            let record = ProtectionRecord::LiveLogRead {
                id: protection_id,
                branch_id,
                revision: expected_revision,
                root: expected_root,
                owner_incarnation: self.owner_incarnation,
                expires_at: expires,
            };
            txn.put(
                &pkey,
                &encode_protection(&record).map_err(to_metadata_error)?,
            )?;
            Ok(expires.as_nanos().to_le_bytes().to_vec())
        })?;
        decode_deadline(&result)
    }

    /// Renews a read handle before expiry. Replaying its ID returns the old deadline.
    pub fn renew_protection(
        &self,
        operation_id: OperationId,
        protection_id: ProtectionId,
    ) -> Result<MonotonicTime, ControlError> {
        let mut payload = CanonicalEncoder::new();
        payload.fixed(protection_id.as_bytes());
        let result =
            self.run_atomic(operation_id, OP_RENEW_PROTECTION, payload.finish(), |txn| {
                self.require_current_owner(txn)?;
                let key = protection_key(self.workspace_id, protection_id);
                let mut record = txn
                    .get(&key)?
                    .map(|v| decode_protection(&v).map_err(to_metadata_error))
                    .transpose()?
                    .ok_or_else(|| reject("not_found"))?;
                let now = txn.now();
                let deadline = match &mut record {
                    ProtectionRecord::GenerationRead {
                        owner_incarnation,
                        expires_at,
                        ..
                    }
                    | ProtectionRecord::LiveLogRead {
                        owner_incarnation,
                        expires_at,
                        ..
                    } if *owner_incarnation == self.owner_incarnation && *expires_at > now => {
                        *expires_at = now.saturating_add_nanos(READER_PROTECTION_NANOS);
                        *expires_at
                    }
                    ProtectionRecord::Retired { .. } => return Err(reject("not_found")),
                    _ => return Err(reject("expired")),
                };
                txn.put(
                    &key,
                    &encode_protection(&record).map_err(to_metadata_error)?,
                )?;
                Ok(deadline.as_nanos().to_le_bytes().to_vec())
            })?;
        decode_deadline(&result)
    }

    /// Closes a read protection and updates its retention guard atomically.
    pub fn close_protection(
        &self,
        operation_id: OperationId,
        protection_id: ProtectionId,
    ) -> Result<(), ControlError> {
        let mut payload = CanonicalEncoder::new();
        payload.fixed(protection_id.as_bytes());
        self.run_atomic(operation_id, OP_CLOSE_PROTECTION, payload.finish(), |txn| {
            close_protection(txn, self.workspace_id, protection_id)?;
            Ok(Vec::new())
        })?;
        Ok(())
    }

    /// Reclaims an expired or owner-fenced protection and its retention counter.
    /// An attachment with an active capture must have that preparation resolved first.
    pub fn expire_protection(
        &self,
        operation_id: OperationId,
        protection_id: ProtectionId,
    ) -> Result<(), ControlError> {
        let mut payload = CanonicalEncoder::new();
        payload.fixed(protection_id.as_bytes());
        self.run_atomic(
            operation_id,
            OP_EXPIRE_PROTECTION,
            payload.finish(),
            |txn| {
                self.require_current_owner(txn)?;
                let pkey = protection_key(self.workspace_id, protection_id);
                let bytes = txn.get(&pkey)?.ok_or_else(|| reject("not_found"))?;
                let record = decode_protection(&bytes).map_err(to_metadata_error)?;
                match record {
                    ProtectionRecord::GenerationRead {
                        generation_id,
                        owner_incarnation,
                        expires_at,
                        ..
                    } => {
                        require_expired(
                            owner_incarnation,
                            expires_at,
                            self.owner_incarnation,
                            txn.now(),
                        )?;
                        let gkey = generation_key(self.workspace_id, generation_id);
                        let mut generation = read_generation(txn, &gkey)?;
                        generation.reader_refs = decrement(generation.reader_refs)?;
                        txn.put(
                            &gkey,
                            &encode_generation(&generation).map_err(to_metadata_error)?,
                        )?;
                    }
                    ProtectionRecord::LiveLogRead {
                        owner_incarnation,
                        expires_at,
                        ..
                    } => {
                        require_expired(
                            owner_incarnation,
                            expires_at,
                            self.owner_incarnation,
                            txn.now(),
                        )?;
                    }
                    ProtectionRecord::AttachmentBase {
                        branch_id,
                        attachment_id,
                        generation_id,
                        owner_incarnation,
                        expires_at,
                        ..
                    } => {
                        require_expired(
                            owner_incarnation,
                            expires_at,
                            self.owner_incarnation,
                            txn.now(),
                        )?;
                        let bkey = branch_key(self.workspace_id, branch_id);
                        let mut branch = read_branch(txn, &bkey)?;
                        if branch.attachment.is_some_and(|attachment| {
                            attachment.attachment_id == attachment_id
                                && attachment.protection_id == protection_id
                                && attachment.base_generation == generation_id
                        }) {
                            if branch.capture.is_some() {
                                return Err(reject("busy"));
                            }
                            release_attachment(txn, self.workspace_id, &mut branch)?;
                            txn.put(&bkey, &encode_branch(&branch).map_err(to_metadata_error)?)?;
                            // release_attachment decrements the protected Generation.
                        } else {
                            let gkey = generation_key(self.workspace_id, generation_id);
                            let mut generation = read_generation(txn, &gkey)?;
                            generation.attachment_refs = decrement(generation.attachment_refs)?;
                            txn.put(
                                &gkey,
                                &encode_generation(&generation).map_err(to_metadata_error)?,
                            )?;
                            retire_protection(txn, self.workspace_id, protection_id)?;
                        }
                        return Ok(Vec::new());
                    }
                    ProtectionRecord::Preparation { .. } => {
                        return Err(reject("invalid_transition"));
                    }
                    ProtectionRecord::Retired { .. } => return Err(reject("not_found")),
                }
                retire_protection(txn, self.workspace_id, protection_id)?;
                Ok(Vec::new())
            },
        )?;
        Ok(())
    }

    /// Marks a Generation outside retention if no branch, pin, reader, or
    /// attachment protection still references it. No object bytes are deleted.
    pub fn mark_generation_unretained(
        &self,
        operation_id: OperationId,
        generation_id: GenerationId,
    ) -> Result<(), ControlError> {
        let mut payload = CanonicalEncoder::new();
        payload.fixed(generation_id.as_bytes());
        self.run_atomic(operation_id, OP_MARK_UNRETAINED, payload.finish(), |txn| {
            let key = generation_key(self.workspace_id, generation_id);
            let mut generation = read_generation(txn, &key)?;
            if generation.branch_refs != 0
                || generation.pin_refs != 0
                || generation.reader_refs != 0
                || generation.attachment_refs != 0
            {
                return Err(reject("busy"));
            }
            generation.retained = false;
            txn.put(
                &key,
                &encode_generation(&generation).map_err(to_metadata_error)?,
            )?;
            Ok(Vec::new())
        })?;
        Ok(())
    }

    /// Registers a protected remote-I/O preparation and, for checkpoint capture,
    /// closes branch head/live mutation admission.
    pub fn begin_preparation(
        &self,
        request: PreparationRequest,
    ) -> Result<OperationState, ControlError> {
        let PreparationRequest {
            operation_id,
            branch_id,
            expected_revision,
            grant,
            attachment_id,
            volume_epoch,
            protection_id,
            kind,
            expected_roots,
            dependencies,
        } = request;
        let capture = kind == PreparationKind::CheckpointCapture;
        if dependencies.len() > 4096 {
            return Err(ControlError::InvalidRequest(
                "too many protected dependencies",
            ));
        }
        let mut payload = CanonicalEncoder::new();
        payload.fixed(branch_id.as_bytes());
        payload.u64_le(expected_revision);
        encode_writer_grant(&mut payload, grant);
        payload.fixed(attachment_id.as_bytes());
        payload.u64_le(volume_epoch);
        payload.fixed(protection_id.as_bytes());
        payload.u8(kind as u8);
        match expected_roots {
            Some(roots) => {
                payload.u8(1);
                encode_live_roots(&mut payload, roots);
            }
            None => payload.u8(0),
        }
        payload.u8(u8::from(capture));
        if capture != expected_roots.is_none() {
            return Err(ControlError::InvalidRequest(
                "capture preparations have no live-root result; mutations bind one",
            ));
        }
        if let Some(roots) = expected_roots {
            validate_live_roots(roots)?;
        }
        for dependency in &dependencies {
            dependency.validate()?;
        }
        payload.u32_le(dependencies.len() as u32);
        for dependency in &dependencies {
            payload.object_ref(*dependency);
        }
        let request = payload.finish();
        let fp = fingerprint(OP_BEGIN_PREPARATION, &request);
        let key = self.operation_key(operation_id);
        let outcome_key = self.transition_key(operation_id, TRANSITION_BEGIN);
        let begin_outcome = reconcile_outcome(self.authority.as_ref(), &outcome_key)?;
        if let Some(bytes) = self.authority.get(&key)? {
            let record = decode_operation(&bytes)?;
            check_fingerprint(&record, OP_BEGIN_PREPARATION, fp)?;
            return Ok(record.public_state(operation_id));
        }
        if begin_outcome.is_some() {
            return Err(ControlError::CorruptRecord(
                "committed preparation transition has no current operation record".into(),
            ));
        }
        let committed = self.transact_with_outcome(&outcome_key, |txn| {
            if let Some(bytes) = txn.get(&key)? {
                let record = decode_operation(&bytes).map_err(to_metadata_error)?;
                check_fingerprint(&record, OP_BEGIN_PREPARATION, fp).map_err(to_metadata_error)?;
                return encode_operation(&record).map_err(to_metadata_error);
            }
            self.require_current_owner(txn)?;
            let bkey = branch_key(self.workspace_id, branch_id);
            let mut branch = read_branch(txn, &bkey)?;
            validate_writer(&branch, grant, self.owner_incarnation, txn.now())?;
            if branch.revision != expected_revision {
                return Err(MetadataError::Conflict);
            }
            validate_attachment_protection(
                txn,
                self.workspace_id,
                &branch,
                attachment_id,
                volume_epoch,
                grant,
                self.owner_incarnation,
            )?;
            let is_capture_child = branch.capture.is_some();
            if let Some(token) = &branch.capture {
                validate_capture_principal(token, self.principal_id)?;
            }
            match (&branch.capture, capture) {
                (None, true) => {
                    self.require_unreserved_operation(txn, operation_id)?;
                    if branch.pending_mutation_preparations != 0 {
                        return Err(reject("busy"));
                    }
                }
                (Some(token), false)
                    if !token.sealed
                        && token.pending_child == Some(operation_id)
                        && kind == PreparationKind::VolumeBarrier =>
                {
                    validate_capture_preparation(
                        txn,
                        self.workspace_id,
                        token.operation_id,
                        token.protection_id,
                        grant,
                        self.owner_incarnation,
                    )?;
                    let reservation_key = capture_child_reservation_key(
                        self.workspace_id,
                        self.principal_id,
                        operation_id,
                    );
                    let reservation_bytes = txn
                        .get(&reservation_key)?
                        .ok_or_else(|| reject("corrupt_record"))?;
                    let reservation = decode_capture_child_reservation(&reservation_bytes)
                        .map_err(to_metadata_error)?;
                    if reservation
                        != (CaptureChildReservation {
                            capture_id: token.operation_id,
                            branch_id,
                        })
                    {
                        return Err(reject("corrupt_record"));
                    }
                    txn.delete(&reservation_key)?;
                }
                (None, false) => {
                    self.require_unreserved_operation(txn, operation_id)?;
                    branch.pending_mutation_preparations =
                        increment(branch.pending_mutation_preparations)?;
                }
                _ => return Err(reject("busy")),
            }
            if let Some(roots) = expected_roots {
                validate_preparation_root_scope(
                    kind,
                    LiveRoots {
                        volume: branch.volume_root,
                        logs: branch.logs_root,
                        artifacts: branch.artifacts_root,
                    },
                    roots,
                )?;
            }
            let pkey = protection_key(self.workspace_id, protection_id);
            if txn.get(&pkey)?.is_some() {
                return Err(MetadataError::Conflict);
            }
            let expires = txn.now().saturating_add_nanos(PREPARATION_LEASE_NANOS);
            let protection = ProtectionRecord::Preparation {
                id: protection_id,
                operation_id,
                branch_id,
                attachment_id,
                volume_epoch,
                grant,
                owner_incarnation: self.owner_incarnation,
                expires_at: expires,
                dependencies: dependencies.clone(),
            };
            if capture {
                branch.capture = Some(CaptureState {
                    operation_id,
                    principal_id: self.principal_id,
                    protection_id,
                    expected_revision,
                    sealed: false,
                    pending_child: None,
                    sealed_revision: None,
                    sealed_roots: None,
                });
            }
            if capture || !is_capture_child {
                txn.put(&bkey, &encode_branch(&branch).map_err(to_metadata_error)?)?;
            }
            let record = StoredOperation {
                kind: OP_BEGIN_PREPARATION,
                fingerprint: fp,
                commit_fingerprint: None,
                status: StoredOperationStatus::Preparing {
                    branch_id,
                    protection_id,
                    grant,
                    kind,
                    expected_revision,
                    expected_roots: expected_roots.map(Box::new),
                    expires_at: expires,
                    capture,
                },
            };
            let encoded = encode_operation(&record).map_err(to_metadata_error)?;
            txn.put(
                &pkey,
                &encode_protection(&protection).map_err(to_metadata_error)?,
            )?;
            txn.put(&key, &encoded)?;
            Ok(encoded)
        });
        match committed {
            Ok(bytes) => Ok(decode_operation(&bytes)?.public_state(operation_id)),
            Err(ControlError::Conflict) => {
                let outcome = reconcile_outcome(self.authority.as_ref(), &outcome_key)?;
                if let Some(bytes) = self.authority.get(&key)? {
                    let record = decode_operation(&bytes)?;
                    check_fingerprint(&record, OP_BEGIN_PREPARATION, fp)?;
                    Ok(record.public_state(operation_id))
                } else if outcome.is_some() {
                    Err(ControlError::CorruptRecord(
                        "committed preparation transition has no current operation record".into(),
                    ))
                } else {
                    Err(ControlError::Conflict)
                }
            }
            Err(error) => Err(error),
        }
    }

    /// Extends an active preparation. Each renewal operation ID is independently
    /// replay-safe and returns its recorded deadline.
    pub fn renew_preparation(
        &self,
        renewal_id: OperationId,
        prepared_id: OperationId,
        grant: WriterGrant,
    ) -> Result<MonotonicTime, ControlError> {
        let mut payload = CanonicalEncoder::new();
        payload.fixed(prepared_id.as_bytes());
        encode_writer_grant(&mut payload, grant);
        let result =
            self.run_atomic(renewal_id, OP_RENEW_PREPARATION, payload.finish(), |txn| {
                self.require_current_owner(txn)?;
                let prep_key = self.operation_key(prepared_id);
                let mut prepared = txn
                    .get(&prep_key)?
                    .map(|v| decode_operation(&v).map_err(to_metadata_error))
                    .transpose()?
                    .ok_or_else(|| reject("not_found"))?;
                let (
                    branch_id,
                    protection_id,
                    prep_grant,
                    kind,
                    prep_revision,
                    expected_roots,
                    capture,
                ) = match prepared.status {
                    StoredOperationStatus::Preparing {
                        branch_id,
                        protection_id,
                        grant,
                        kind,
                        expected_revision,
                        expected_roots,
                        capture,
                        ..
                    } => (
                        branch_id,
                        protection_id,
                        grant,
                        kind,
                        expected_revision,
                        expected_roots,
                        capture,
                    ),
                    StoredOperationStatus::Committed(_) => {
                        return Err(reject("invalid_transition"));
                    }
                    StoredOperationStatus::Aborted => return Err(reject("aborted")),
                };
                if !same_grant_identity(prep_grant, grant) {
                    return Err(reject("fenced"));
                }
                let branch = read_branch(txn, &branch_key(self.workspace_id, branch_id))?;
                validate_writer(&branch, grant, self.owner_incarnation, txn.now())?;
                if capture {
                    let token = branch.capture.as_ref().ok_or_else(|| reject("fenced"))?;
                    validate_capture_principal(token, self.principal_id)?;
                    if token.operation_id != prepared_id || token.protection_id != protection_id {
                        return Err(reject("fenced"));
                    }
                    validate_capture_preparation(
                        txn,
                        self.workspace_id,
                        prepared_id,
                        protection_id,
                        grant,
                        self.owner_incarnation,
                    )?;
                }
                let pkey = protection_key(self.workspace_id, protection_id);
                let mut protection = txn
                    .get(&pkey)?
                    .map(|v| decode_protection(&v).map_err(to_metadata_error))
                    .transpose()?
                    .ok_or_else(|| reject("expired"))?;
                let expires = txn.now().saturating_add_nanos(PREPARATION_LEASE_NANOS);
                match &mut protection {
                    ProtectionRecord::Preparation {
                        operation_id,
                        attachment_id,
                        volume_epoch,
                        grant: old_grant,
                        owner_incarnation,
                        expires_at,
                        ..
                    } if *operation_id == prepared_id
                        && same_grant_identity(*old_grant, grant)
                        && *owner_incarnation == self.owner_incarnation
                        && *expires_at > txn.now() =>
                    {
                        validate_attachment_protection(
                            txn,
                            self.workspace_id,
                            &branch,
                            *attachment_id,
                            *volume_epoch,
                            grant,
                            self.owner_incarnation,
                        )?;
                        *old_grant = grant;
                        *expires_at = expires;
                    }
                    _ => return Err(reject("expired")),
                }
                prepared.status = StoredOperationStatus::Preparing {
                    branch_id,
                    protection_id,
                    grant,
                    kind,
                    expected_revision: prep_revision,
                    expected_roots,
                    expires_at: expires,
                    capture,
                };
                txn.put(
                    &prep_key,
                    &encode_operation(&prepared).map_err(to_metadata_error)?,
                )?;
                txn.put(
                    &pkey,
                    &encode_protection(&protection).map_err(to_metadata_error)?,
                )?;
                Ok(expires.as_nanos().to_le_bytes().to_vec())
            })?;
        decode_deadline(&result)
    }

    /// Authorizes one identified drain child while the capture token is open.
    pub fn authorize_capture_child(
        &self,
        operation_id: OperationId,
        capture_id: OperationId,
        child_id: OperationId,
        grant: WriterGrant,
    ) -> Result<(), ControlError> {
        if operation_id == capture_id || operation_id == child_id || capture_id == child_id {
            return Err(ControlError::InvalidRequest(
                "capture and child operation IDs must be distinct",
            ));
        }
        let mut payload = CanonicalEncoder::new();
        payload.fixed(capture_id.as_bytes());
        payload.fixed(child_id.as_bytes());
        encode_writer_grant(&mut payload, grant);
        self.run_atomic(
            operation_id,
            OP_AUTHORIZE_CAPTURE_CHILD,
            payload.finish(),
            |txn| {
                self.require_current_owner(txn)?;
                let key = branch_key(self.workspace_id, grant.branch_id);
                let mut branch = read_branch(txn, &key)?;
                validate_writer(&branch, grant, self.owner_incarnation, txn.now())?;
                let capture = branch.capture.as_mut().ok_or_else(|| reject("not_found"))?;
                validate_capture_principal(capture, self.principal_id)?;
                if capture.operation_id != capture_id || capture.sealed {
                    return Err(reject("invalid_transition"));
                }
                validate_capture_preparation(
                    txn,
                    self.workspace_id,
                    capture_id,
                    capture.protection_id,
                    grant,
                    self.owner_incarnation,
                )?;
                if capture.pending_child.is_some() {
                    return Err(reject("busy"));
                }
                if txn.get(&self.operation_key(child_id))?.is_some() {
                    return Err(MetadataError::Conflict);
                }
                let reservation_key =
                    capture_child_reservation_key(self.workspace_id, self.principal_id, child_id);
                if txn.get(&reservation_key)?.is_some() {
                    return Err(MetadataError::Conflict);
                }
                capture.pending_child = Some(child_id);
                txn.put(&key, &encode_branch(&branch).map_err(to_metadata_error)?)?;
                txn.put(
                    &reservation_key,
                    &encode_capture_child_reservation(CaptureChildReservation {
                        capture_id,
                        branch_id: grant.branch_id,
                    })
                    .map_err(to_metadata_error)?,
                )?;
                Ok(Vec::new())
            },
        )?;
        Ok(())
    }

    /// Advances live roots under exact revision/grant CAS. During capture only
    /// the currently identified child may mutate roots; commit consumes that child.
    pub fn commit_live_roots(
        &self,
        operation_id: OperationId,
        expected_revision: u64,
        grant: WriterGrant,
        roots: LiveRoots,
        capture_child: Option<(OperationId, OperationId)>,
    ) -> Result<BranchState, ControlError> {
        validate_live_roots(roots)?;
        let mut payload = CanonicalEncoder::new();
        payload.u64_le(expected_revision);
        encode_writer_grant(&mut payload, grant);
        encode_live_roots(&mut payload, roots);
        match capture_child {
            Some((capture_id, child_id)) => {
                payload.u8(1);
                payload.fixed(capture_id.as_bytes());
                payload.fixed(child_id.as_bytes());
            }
            None => payload.u8(0),
        }
        let request = payload.finish();
        let fp = fingerprint(OP_COMMIT_LIVE_ROOTS, &request);
        let key = self.operation_key(operation_id);
        let outcome_key = self.transition_key(operation_id, TRANSITION_COMMIT_LIVE_ROOTS);
        if let Some(bytes) = reconcile_outcome(self.authority.as_ref(), &outcome_key)? {
            return decode_committed_live_roots(&bytes, fp);
        }
        let outcome = self.transact_with_outcome(&outcome_key, |txn| {
            let prior = txn
                .get(&key)?
                .map(|bytes| decode_operation(&bytes).map_err(to_metadata_error))
                .transpose()?;
            let record = prior.as_ref().ok_or_else(|| reject("invalid_transition"))?;
            let (prepared_attachment_id, prepared_volume_epoch, prepared_kind) =
                match &record.status {
                    StoredOperationStatus::Committed(_) => {
                        if record.commit_fingerprint != Some(fp) {
                            return Err(reject("operation_mismatch"));
                        }
                        return encode_operation(record).map_err(to_metadata_error);
                    }
                    StoredOperationStatus::Aborted => return Err(reject("aborted")),
                    StoredOperationStatus::Preparing {
                        branch_id,
                        protection_id,
                        grant: prepared_grant,
                        kind: prepared_kind,
                        expected_revision: prepared_revision,
                        expected_roots,
                        capture,
                        ..
                    } => {
                        if record.kind != OP_BEGIN_PREPARATION
                            || *branch_id != grant.branch_id
                            || !same_grant_identity(*prepared_grant, grant)
                            || *prepared_revision != expected_revision
                            || *capture
                            || expected_roots.as_deref().copied() != Some(roots)
                        {
                            return Err(reject("operation_mismatch"));
                        }
                        let pkey = protection_key(self.workspace_id, *protection_id);
                        let protection = txn.get(&pkey)?.ok_or_else(|| reject("expired"))?;
                        match decode_protection(&protection).map_err(to_metadata_error)? {
                            ProtectionRecord::Preparation {
                                operation_id: prepared_id,
                                branch_id,
                                attachment_id,
                                volume_epoch,
                                grant: protection_grant,
                                owner_incarnation,
                                expires_at,
                                ..
                            } if prepared_id == operation_id
                                && branch_id == grant.branch_id
                                && protection_grant == *prepared_grant
                                && same_grant_identity(*prepared_grant, grant)
                                && owner_incarnation == self.owner_incarnation
                                && expires_at > txn.now() =>
                            {
                                (attachment_id, volume_epoch, *prepared_kind)
                            }
                            _ => return Err(reject("expired")),
                        }
                    }
                };
            self.require_current_owner(txn)?;
            let bkey = branch_key(self.workspace_id, grant.branch_id);
            let mut branch = read_branch(txn, &bkey)?;
            validate_writer(&branch, grant, self.owner_incarnation, txn.now())?;
            validate_attachment_protection(
                txn,
                self.workspace_id,
                &branch,
                prepared_attachment_id,
                prepared_volume_epoch,
                grant,
                self.owner_incarnation,
            )?;
            if branch.revision != expected_revision {
                return Err(MetadataError::Conflict);
            }
            validate_preparation_root_scope(
                prepared_kind,
                LiveRoots {
                    volume: branch.volume_root,
                    logs: branch.logs_root,
                    artifacts: branch.artifacts_root,
                },
                roots,
            )?;
            if let Some(capture) = &branch.capture {
                validate_capture_principal(capture, self.principal_id)?;
            }
            match (&mut branch.capture, capture_child) {
                (None, None) => {
                    branch.pending_mutation_preparations =
                        decrement(branch.pending_mutation_preparations)?;
                }
                (Some(capture), Some((capture_id, child_id)))
                    if capture.operation_id == capture_id
                        && !capture.sealed
                        && capture.pending_child == Some(child_id)
                        && operation_id == child_id
                        && prepared_kind == PreparationKind::VolumeBarrier =>
                {
                    validate_capture_preparation(
                        txn,
                        self.workspace_id,
                        capture_id,
                        capture.protection_id,
                        grant,
                        self.owner_incarnation,
                    )?;
                    capture.pending_child = None;
                }
                (Some(_), _) => return Err(reject("busy")),
                (None, Some(_)) => return Err(reject("invalid_transition")),
            }
            branch.volume_root = roots.volume;
            branch.logs_root = roots.logs;
            branch.artifacts_root = roots.artifacts;
            branch.revision = increment(branch.revision)?;
            let encoded = encode_branch(&branch).map_err(to_metadata_error)?;
            txn.put(&bkey, &encoded)?;
            let mut operation = prior.ok_or_else(|| reject("invalid_transition"))?;
            let protection_id = match &operation.status {
                StoredOperationStatus::Preparing { protection_id, .. } => Some(*protection_id),
                _ => None,
            };
            operation.commit_fingerprint = Some(fp);
            operation.status = StoredOperationStatus::Committed(encoded.clone());
            if let Some(protection_id) = protection_id {
                retire_protection(txn, self.workspace_id, protection_id)?;
            }
            let encoded = encode_operation(&operation).map_err(to_metadata_error)?;
            txn.put(&key, &encoded)?;
            Ok(encoded)
        });
        let bytes = match outcome {
            Ok(bytes) => bytes,
            Err(ControlError::Conflict) => {
                if let Some(bytes) = reconcile_outcome(self.authority.as_ref(), &outcome_key)? {
                    bytes
                } else {
                    return Err(ControlError::Conflict);
                }
            }
            Err(error) => return Err(error),
        };
        decode_committed_live_roots(&bytes, fp)
    }

    /// Seals a capture at the exact final revision after all admitted children drain.
    pub fn seal_capture(
        &self,
        operation_id: OperationId,
        capture_id: OperationId,
        expected_revision: u64,
        grant: WriterGrant,
    ) -> Result<CaptureState, ControlError> {
        let mut payload = CanonicalEncoder::new();
        payload.fixed(capture_id.as_bytes());
        payload.u64_le(expected_revision);
        encode_writer_grant(&mut payload, grant);
        let result = self.run_atomic(operation_id, OP_SEAL_CAPTURE, payload.finish(), |txn| {
            self.require_current_owner(txn)?;
            let key = branch_key(self.workspace_id, grant.branch_id);
            let mut branch = read_branch(txn, &key)?;
            validate_writer(&branch, grant, self.owner_incarnation, txn.now())?;
            if branch.revision != expected_revision {
                return Err(MetadataError::Conflict);
            }
            let state = branch.capture.as_mut().ok_or_else(|| reject("not_found"))?;
            validate_capture_principal(state, self.principal_id)?;
            if state.operation_id != capture_id || state.sealed {
                return Err(reject("invalid_transition"));
            }
            if state.pending_child.is_some() {
                return Err(reject("busy"));
            }
            validate_capture_preparation(
                txn,
                self.workspace_id,
                capture_id,
                state.protection_id,
                grant,
                self.owner_incarnation,
            )?;
            state.sealed = true;
            state.sealed_revision = Some(branch.revision);
            state.sealed_roots = Some(LiveRoots {
                volume: branch.volume_root,
                logs: branch.logs_root,
                artifacts: branch.artifacts_root,
            });
            let encoded = encode_branch(&branch).map_err(to_metadata_error)?;
            txn.put(&key, &encoded)?;
            Ok(encode_capture_result(
                branch
                    .capture
                    .as_ref()
                    .ok_or_else(|| reject("invalid_transition"))?,
            ))
        })?;
        decode_capture_result(&result)
    }

    /// Reads or reconciles an operation outcome after an uncertain response.
    pub fn get_operation(&self, operation_id: OperationId) -> Result<OperationState, ControlError> {
        let key = self.operation_key(operation_id);
        let bytes = match reconcile_outcome(self.authority.as_ref(), &key)? {
            Some(bytes) => bytes,
            None => self.authority.get(&key)?.ok_or(ControlError::NotFound)?,
        };
        Ok(decode_operation(&bytes)?.public_state(operation_id))
    }

    /// Aborts a preparation or tombstones an unseen ID. Committed operations are
    /// returned without undoing their effects.
    pub fn abort_operation(
        &self,
        operation_id: OperationId,
    ) -> Result<OperationState, ControlError> {
        let key = self.operation_key(operation_id);
        let outcome_key = self.transition_key(operation_id, TRANSITION_ABORT);
        if let Some(bytes) = reconcile_outcome(self.authority.as_ref(), &outcome_key)? {
            return Ok(decode_operation(&bytes)?.public_state(operation_id));
        }
        let outcome = self.transact_with_outcome(&outcome_key, |txn| {
            self.require_current_owner(txn)?;
            let Some(bytes) = txn.get(&key)? else {
                let reservation_key = capture_child_reservation_key(
                    self.workspace_id,
                    self.principal_id,
                    operation_id,
                );
                if let Some(reservation_bytes) = txn.get(&reservation_key)? {
                    let reservation = decode_capture_child_reservation(&reservation_bytes)
                        .map_err(to_metadata_error)?;
                    let bkey = branch_key(self.workspace_id, reservation.branch_id);
                    let mut branch = read_branch(txn, &bkey)?;
                    let capture = branch
                        .capture
                        .as_mut()
                        .ok_or_else(|| reject("corrupt_record"))?;
                    if capture.operation_id != reservation.capture_id
                        || capture.principal_id != self.principal_id
                        || capture.pending_child != Some(operation_id)
                    {
                        return Err(reject("corrupt_record"));
                    }
                    capture.pending_child = None;
                    txn.put(&bkey, &encode_branch(&branch).map_err(to_metadata_error)?)?;
                    txn.delete(&reservation_key)?;
                }
                let tombstone = StoredOperation::aborted(OP_TOMBSTONE, [0; 32]);
                let encoded = encode_operation(&tombstone).map_err(to_metadata_error)?;
                txn.put(&key, &encoded)?;
                return Ok(encoded);
            };
            let mut record = decode_operation(&bytes).map_err(to_metadata_error)?;
            match record.status {
                StoredOperationStatus::Committed(_) | StoredOperationStatus::Aborted => Ok(bytes),
                StoredOperationStatus::Preparing {
                    branch_id,
                    protection_id,
                    grant,
                    expires_at,
                    capture,
                    ..
                } => {
                    let pkey = protection_key(self.workspace_id, protection_id);
                    let protection = txn.get(&pkey)?.ok_or_else(|| reject("expired"))?;
                    match decode_protection(&protection).map_err(to_metadata_error)? {
                        ProtectionRecord::Preparation {
                            operation_id: prepared_id,
                            branch_id: protected_branch,
                            grant: protected_grant,
                            owner_incarnation,
                            expires_at: protected_deadline,
                            ..
                        } if prepared_id == operation_id
                            && protected_branch == branch_id
                            && protected_grant == grant
                            && owner_incarnation == grant.owner_incarnation
                            && protected_deadline == expires_at => {}
                        _ => return Err(reject("corrupt_record")),
                    }
                    let bkey = branch_key(self.workspace_id, branch_id);
                    let mut branch = read_branch(txn, &bkey)?;
                    if capture {
                        let token = branch
                            .capture
                            .as_ref()
                            .ok_or_else(|| reject("corrupt_record"))?;
                        if token.operation_id != operation_id
                            || token.principal_id != self.principal_id
                            || token.protection_id != protection_id
                        {
                            return Err(reject("corrupt_record"));
                        }
                        if let Some(child_id) = token.pending_child {
                            self.retire_unstarted_capture_child(
                                txn,
                                branch_id,
                                operation_id,
                                grant,
                                child_id,
                            )?;
                        }
                        branch.capture = None;
                    } else if let Some(token) = &mut branch.capture {
                        if token.principal_id == self.principal_id
                            && token.pending_child == Some(operation_id)
                        {
                            token.pending_child = None;
                        } else {
                            return Err(reject("corrupt_record"));
                        }
                    } else {
                        branch.pending_mutation_preparations =
                            decrement(branch.pending_mutation_preparations)?;
                    }
                    txn.put(&bkey, &encode_branch(&branch).map_err(to_metadata_error)?)?;
                    retire_protection(txn, self.workspace_id, protection_id)?;
                    record.status = StoredOperationStatus::Aborted;
                    let encoded = encode_operation(&record).map_err(to_metadata_error)?;
                    txn.put(&key, &encoded)?;
                    Ok(encoded)
                }
            }
        });
        match outcome {
            Ok(bytes) => Ok(decode_operation(&bytes)?.public_state(operation_id)),
            Err(ControlError::Conflict) => {
                if let Some(bytes) = reconcile_outcome(self.authority.as_ref(), &outcome_key)? {
                    Ok(decode_operation(&bytes)?.public_state(operation_id))
                } else {
                    Err(ControlError::Conflict)
                }
            }
            Err(error) => Err(error),
        }
    }

    /// Expires a preparation only after its lease deadline or owner incarnation
    /// is no longer current. Capture admission and protection are cleared together.
    pub fn expire_preparation(
        &self,
        operation_id: OperationId,
    ) -> Result<OperationState, ControlError> {
        let key = self.operation_key(operation_id);
        let outcome_key = self.transition_key(operation_id, TRANSITION_EXPIRE_PREPARATION);
        if let Some(bytes) = reconcile_outcome(self.authority.as_ref(), &outcome_key)? {
            return Ok(decode_operation(&bytes)?.public_state(operation_id));
        }
        let outcome = self.transact_with_outcome(&outcome_key, |txn| {
            self.require_current_owner(txn)?;
            let mut record = txn
                .get(&key)?
                .map(|v| decode_operation(&v).map_err(to_metadata_error))
                .transpose()?
                .ok_or_else(|| reject("not_found"))?;
            let (branch_id, protection_id, grant, deadline, capture) = match record.status {
                StoredOperationStatus::Preparing {
                    branch_id,
                    protection_id,
                    grant,
                    expires_at,
                    capture,
                    ..
                } => (branch_id, protection_id, grant, expires_at, capture),
                _ => return encode_operation(&record).map_err(to_metadata_error),
            };
            let pkey = protection_key(self.workspace_id, protection_id);
            let protection_bytes = txn.get(&pkey)?.ok_or_else(|| reject("expired"))?;
            let (attachment_id, volume_epoch) =
                match decode_protection(&protection_bytes).map_err(to_metadata_error)? {
                    ProtectionRecord::Preparation {
                        operation_id: prepared_id,
                        branch_id: protected_branch,
                        attachment_id,
                        volume_epoch,
                        grant: protected_grant,
                        owner_incarnation,
                        expires_at,
                        ..
                    } if prepared_id == operation_id
                        && protected_branch == branch_id
                        && same_grant_identity(protected_grant, grant)
                        && owner_incarnation == grant.owner_incarnation
                        && expires_at == deadline =>
                    {
                        (attachment_id, volume_epoch)
                    }
                    _ => return Err(reject("expired")),
                };
            let bkey = branch_key(self.workspace_id, branch_id);
            let mut branch = read_branch(txn, &bkey)?;
            let now = txn.now();
            let grant_current = grant.owner_incarnation == self.owner_incarnation
                && branch.writer.is_some_and(|current| {
                    same_grant_identity(current, grant) && current.expires_at > now
                })
                && branch.attachment.is_some_and(|attachment| {
                    attachment.attachment_id == attachment_id
                        && attachment.volume_epoch == volume_epoch
                        && attachment.writer_epoch == grant.writer_epoch
                        && attachment.protection_expires_at > now
                });
            if grant_current && deadline > now {
                return Err(reject("busy"));
            }
            if capture {
                let token = branch
                    .capture
                    .as_ref()
                    .ok_or_else(|| reject("corrupt_record"))?;
                if token.operation_id != operation_id
                    || token.principal_id != self.principal_id
                    || token.protection_id != protection_id
                {
                    return Err(reject("corrupt_record"));
                }
                if let Some(child_id) = token.pending_child {
                    self.retire_unstarted_capture_child(
                        txn,
                        branch_id,
                        operation_id,
                        grant,
                        child_id,
                    )?;
                }
                branch.capture = None;
            } else if let Some(token) = &mut branch.capture {
                if token.principal_id == self.principal_id
                    && token.pending_child == Some(operation_id)
                {
                    token.pending_child = None;
                } else {
                    return Err(reject("corrupt_record"));
                }
            } else {
                branch.pending_mutation_preparations =
                    decrement(branch.pending_mutation_preparations)?;
            }
            txn.put(&bkey, &encode_branch(&branch).map_err(to_metadata_error)?)?;
            retire_protection(txn, self.workspace_id, protection_id)?;
            record.status = StoredOperationStatus::Aborted;
            let encoded = encode_operation(&record).map_err(to_metadata_error)?;
            txn.put(&key, &encoded)?;
            Ok(encoded)
        });
        match outcome {
            Ok(bytes) => Ok(decode_operation(&bytes)?.public_state(operation_id)),
            Err(ControlError::Conflict) => {
                if let Some(bytes) = reconcile_outcome(self.authority.as_ref(), &outcome_key)? {
                    Ok(decode_operation(&bytes)?.public_state(operation_id))
                } else {
                    Err(ControlError::Conflict)
                }
            }
            Err(error) => Err(error),
        }
    }

    fn operation_key(&self, id: OperationId) -> Vec<u8> {
        scoped_operation_key(self.workspace_id, self.principal_id, id)
    }

    fn require_unreserved_operation(
        &self,
        txn: &mut dyn MetadataTxn,
        operation_id: OperationId,
    ) -> Result<(), MetadataError> {
        let key = capture_child_reservation_key(self.workspace_id, self.principal_id, operation_id);
        if txn.get(&key)?.is_some() {
            return Err(reject("operation_mismatch"));
        }
        Ok(())
    }

    /// Retires a capture child that was authorized but never began preparation.
    /// A tombstone reserves the ID so a delayed worker cannot start it after the
    /// parent capture has released admission.
    fn retire_unstarted_capture_child(
        &self,
        txn: &mut dyn MetadataTxn,
        branch_id: BranchId,
        capture_id: OperationId,
        grant: WriterGrant,
        child_id: OperationId,
    ) -> Result<(), MetadataError> {
        let key = self.operation_key(child_id);
        let Some(bytes) = txn.get(&key)? else {
            let reservation_key =
                capture_child_reservation_key(self.workspace_id, self.principal_id, child_id);
            let reservation_bytes = txn
                .get(&reservation_key)?
                .ok_or_else(|| reject("corrupt_record"))?;
            let reservation =
                decode_capture_child_reservation(&reservation_bytes).map_err(to_metadata_error)?;
            if reservation
                != (CaptureChildReservation {
                    capture_id,
                    branch_id,
                })
            {
                return Err(reject("corrupt_record"));
            }
            let tombstone = StoredOperation::aborted(OP_TOMBSTONE, [0; 32]);
            txn.put(
                &key,
                &encode_operation(&tombstone).map_err(to_metadata_error)?,
            )?;
            txn.delete(&reservation_key)?;
            return Ok(());
        };

        let record = decode_operation(&bytes).map_err(to_metadata_error)?;
        match record.status {
            StoredOperationStatus::Aborted => Ok(()),
            StoredOperationStatus::Preparing {
                branch_id: child_branch,
                grant: child_grant,
                capture,
                ..
            } if record.kind == OP_BEGIN_PREPARATION
                && child_branch == branch_id
                && same_grant_identity(child_grant, grant)
                && !capture =>
            {
                Err(reject("busy"))
            }
            StoredOperationStatus::Preparing { .. } | StoredOperationStatus::Committed(_) => {
                Err(reject("corrupt_record"))
            }
        }
    }

    fn transition_key(&self, id: OperationId, transition: u16) -> Vec<u8> {
        let mut key = self.operation_key(id);
        key.extend_from_slice(b"/transition/");
        key.extend_from_slice(&transition.to_be_bytes());
        key
    }

    fn transact_with_outcome<F>(
        &self,
        outcome_key: &[u8],
        mut body: F,
    ) -> Result<Vec<u8>, ControlError>
    where
        F: FnMut(&mut dyn MetadataTxn) -> Result<Vec<u8>, MetadataError>,
    {
        self.authority
            .transact(
                MutationIdentity {
                    operation_id: outcome_key,
                    outcome_key,
                },
                |txn| {
                    let outcome = body(txn)?;
                    txn.put(outcome_key, &outcome)?;
                    Ok(outcome)
                },
            )
            .map_err(map_metadata_error)
    }

    fn transact<R, F>(
        &self,
        _operation_id: OperationId,
        key: &[u8],
        body: F,
    ) -> Result<R, ControlError>
    where
        F: FnMut(&mut dyn MetadataTxn) -> Result<R, MetadataError>,
    {
        self.authority
            .transact(
                MutationIdentity {
                    operation_id: key,
                    outcome_key: key,
                },
                body,
            )
            .map_err(map_metadata_error)
    }

    fn run_atomic<F>(
        &self,
        operation_id: OperationId,
        kind: u16,
        payload: Vec<u8>,
        mut action: F,
    ) -> Result<Vec<u8>, ControlError>
    where
        F: FnMut(&mut dyn MetadataTxn) -> Result<Vec<u8>, MetadataError>,
    {
        let key = self.operation_key(operation_id);
        let fp = fingerprint(kind, &payload);
        if let Some(result) = self.replay_operation(&key, kind, fp)? {
            return Ok(result);
        }
        let op_key = key.clone();
        let outcome = self.transact(operation_id, &key, |txn| {
            if let Some(bytes) = txn.get(&op_key)? {
                let record = decode_operation(&bytes).map_err(to_metadata_error)?;
                check_fingerprint(&record, kind, fp).map_err(to_metadata_error)?;
                return match record.status {
                    StoredOperationStatus::Committed(result) => Ok(result),
                    StoredOperationStatus::Aborted => Err(reject("aborted")),
                    StoredOperationStatus::Preparing { .. } => Err(reject("in_progress")),
                };
            }
            self.require_current_owner(txn)?;
            self.require_unreserved_operation(txn, operation_id)?;
            let result = action(txn)?;
            let record = StoredOperation::committed(kind, fp, result.clone());
            txn.put(
                &op_key,
                &encode_operation(&record).map_err(to_metadata_error)?,
            )?;
            Ok(result)
        });
        match outcome {
            Ok(result) => Ok(result),
            Err(ControlError::Conflict) => {
                if let Some(result) = self.replay_operation(&key, kind, fp)? {
                    Ok(result)
                } else {
                    Err(ControlError::Conflict)
                }
            }
            Err(error) => Err(error),
        }
    }

    fn replay_operation(
        &self,
        key: &[u8],
        kind: u16,
        request_fingerprint: [u8; 32],
    ) -> Result<Option<Vec<u8>>, ControlError> {
        let bytes = match reconcile_outcome(self.authority.as_ref(), key)? {
            Some(bytes) => Some(bytes),
            None => self.authority.get(key)?,
        };
        let Some(bytes) = bytes else {
            return Ok(None);
        };
        let record = decode_operation(&bytes)?;
        check_fingerprint(&record, kind, request_fingerprint)?;
        match record.status {
            StoredOperationStatus::Committed(result) => Ok(Some(result)),
            StoredOperationStatus::Aborted => Err(ControlError::Aborted),
            StoredOperationStatus::Preparing { .. } => Err(ControlError::InProgress),
        }
    }

    fn require_current_owner(&self, txn: &mut dyn MetadataTxn) -> Result<(), MetadataError> {
        let current = txn
            .get(&owner_incarnation_key())?
            .map(|v| decode_owner(&v).map_err(to_metadata_error))
            .transpose()?
            .ok_or_else(|| reject("fenced"))?;
        if current != self.owner_incarnation.get() {
            return Err(reject("fenced"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct GenerationRecord {
    seed: GenerationSeed,
    retained: bool,
    branch_refs: u64,
    pin_refs: u64,
    reader_refs: u64,
    attachment_refs: u64,
}

impl GenerationRecord {
    fn new(seed: GenerationSeed, retained: bool) -> Self {
        Self {
            seed,
            retained,
            branch_refs: 0,
            pin_refs: 0,
            reader_refs: 0,
            attachment_refs: 0,
        }
    }

    fn eligible(&self) -> bool {
        self.retained
            || self.branch_refs != 0
            || self.pin_refs != 0
            || self.reader_refs != 0
            || self.attachment_refs != 0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum StoredOperationStatus {
    Preparing {
        branch_id: BranchId,
        protection_id: ProtectionId,
        grant: WriterGrant,
        kind: PreparationKind,
        expected_revision: u64,
        expected_roots: Option<Box<LiveRoots>>,
        expires_at: MonotonicTime,
        capture: bool,
    },
    Committed(Vec<u8>),
    Aborted,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct StoredOperation {
    kind: u16,
    fingerprint: [u8; 32],
    commit_fingerprint: Option<[u8; 32]>,
    status: StoredOperationStatus,
}

impl StoredOperation {
    fn committed(kind: u16, fingerprint: [u8; 32], result: Vec<u8>) -> Self {
        Self {
            kind,
            fingerprint,
            commit_fingerprint: None,
            status: StoredOperationStatus::Committed(result),
        }
    }

    fn aborted(kind: u16, fingerprint: [u8; 32]) -> Self {
        Self {
            kind,
            fingerprint,
            commit_fingerprint: None,
            status: StoredOperationStatus::Aborted,
        }
    }

    fn public_state(&self, operation_id: OperationId) -> OperationState {
        match &self.status {
            StoredOperationStatus::Preparing {
                branch_id,
                expires_at,
                ..
            } => OperationState::Preparing {
                operation_id,
                expires_at: *expires_at,
                branch_id: *branch_id,
            },
            StoredOperationStatus::Committed(bytes) => OperationState::Committed(bytes.clone()),
            StoredOperationStatus::Aborted => OperationState::Aborted,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ProtectionRecord {
    Retired {
        id: ProtectionId,
    },
    AttachmentBase {
        id: ProtectionId,
        branch_id: BranchId,
        attachment_id: AttachmentId,
        generation_id: GenerationId,
        roots: LiveRoots,
        owner_incarnation: OwnerIncarnation,
        expires_at: MonotonicTime,
    },
    GenerationRead {
        id: ProtectionId,
        generation_id: GenerationId,
        owner_incarnation: OwnerIncarnation,
        expires_at: MonotonicTime,
    },
    LiveLogRead {
        id: ProtectionId,
        branch_id: BranchId,
        revision: u64,
        root: ObjectRef,
        owner_incarnation: OwnerIncarnation,
        expires_at: MonotonicTime,
    },
    Preparation {
        id: ProtectionId,
        operation_id: OperationId,
        branch_id: BranchId,
        attachment_id: AttachmentId,
        volume_epoch: u64,
        grant: WriterGrant,
        owner_incarnation: OwnerIncarnation,
        expires_at: MonotonicTime,
        dependencies: Vec<ObjectRef>,
    },
}

fn fingerprint(kind: u16, payload: &[u8]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"versionfs-control-operation-v1\0");
    hash.update(kind.to_le_bytes());
    hash.update((payload.len() as u64).to_le_bytes());
    hash.update(payload);
    hash.finalize().into()
}

fn check_fingerprint(
    record: &StoredOperation,
    kind: u16,
    expected: [u8; 32],
) -> Result<(), ControlError> {
    if record.kind == OP_TOMBSTONE && record.status == StoredOperationStatus::Aborted {
        return Err(ControlError::Aborted);
    }
    if record.kind != kind || record.fingerprint != expected {
        return Err(ControlError::OperationMismatch);
    }
    Ok(())
}

fn encode_operation(record: &StoredOperation) -> Result<Vec<u8>, FormatError> {
    let mut e = CanonicalEncoder::new();
    e.u16_le(record.kind);
    e.fixed(&record.fingerprint);
    match record.commit_fingerprint {
        Some(value) => {
            e.u8(1);
            e.fixed(&value);
        }
        None => e.u8(0),
    }
    match &record.status {
        StoredOperationStatus::Preparing {
            branch_id,
            protection_id,
            grant,
            kind,
            expected_revision,
            expected_roots,
            expires_at,
            capture,
        } => {
            e.u8(1);
            e.fixed(branch_id.as_bytes());
            e.fixed(protection_id.as_bytes());
            encode_writer_grant(&mut e, *grant);
            e.u8(*kind as u8);
            e.u64_le(*expected_revision);
            match expected_roots {
                Some(roots) => {
                    e.u8(1);
                    encode_live_roots(&mut e, **roots);
                }
                None => e.u8(0),
            }
            e.u64_le(expires_at.as_nanos());
            e.u8(u8::from(*capture));
        }
        StoredOperationStatus::Committed(result) => {
            e.u8(2);
            e.bytes(result)?;
        }
        StoredOperationStatus::Aborted => {
            e.u8(3);
        }
    }
    encode_metadata_record(MetadataRecordType::OperationOutcome, &e.finish())
}

fn decode_operation(bytes: &[u8]) -> Result<StoredOperation, ControlError> {
    let (record_type, payload) = decode_metadata_record(bytes).map_err(corrupt)?;
    if record_type != MetadataRecordType::OperationOutcome {
        return Err(ControlError::CorruptRecord(
            "wrong operation record type".into(),
        ));
    }
    let mut d = CanonicalDecoder::new(payload);
    let kind = d.u16_le().map_err(corrupt)?;
    let fingerprint = d.fixed().map_err(corrupt)?;
    let commit_fingerprint = match d.u8().map_err(corrupt)? {
        0 => None,
        1 => Some(d.fixed().map_err(corrupt)?),
        _ => {
            return Err(ControlError::CorruptRecord(
                "invalid commit fingerprint tag".into(),
            ));
        }
    };
    let tag = d.u8().map_err(corrupt)?;
    let status = match tag {
        1 => {
            let branch_id = BranchId::from_bytes(d.fixed().map_err(corrupt)?);
            let protection_id = ProtectionId::from_bytes(d.fixed().map_err(corrupt)?);
            let grant = decode_writer_grant(&mut d).map_err(corrupt)?;
            let prep_kind = decode_preparation_kind(d.u8().map_err(corrupt)?).map_err(corrupt)?;
            let expected_revision = d.u64_le().map_err(corrupt)?;
            let expected_roots = match d.u8().map_err(corrupt)? {
                0 => None,
                1 => Some(Box::new(decode_live_roots(&mut d).map_err(corrupt)?)),
                _ => {
                    return Err(ControlError::CorruptRecord(
                        "invalid preparation roots tag".into(),
                    ));
                }
            };
            let deadline = MonotonicTime::from_nanos(d.u64_le().map_err(corrupt)?);
            let capture = decode_bool(d.u8().map_err(corrupt)?)?;
            if capture != expected_roots.is_none() {
                return Err(ControlError::CorruptRecord(
                    "inconsistent preparation roots".into(),
                ));
            }
            StoredOperationStatus::Preparing {
                branch_id,
                protection_id,
                grant,
                kind: prep_kind,
                expected_revision,
                expected_roots,
                expires_at: deadline,
                capture,
            }
        }
        2 => StoredOperationStatus::Committed(
            d.bytes(crate::format::MAX_OBJECT_PAYLOAD_BYTES)
                .map_err(corrupt)?
                .to_vec(),
        ),
        3 => StoredOperationStatus::Aborted,
        _ => {
            return Err(ControlError::CorruptRecord(
                "invalid operation status".into(),
            ));
        }
    };
    d.finish().map_err(corrupt)?;
    Ok(StoredOperation {
        kind,
        fingerprint,
        commit_fingerprint,
        status,
    })
}

fn encode_branch(branch: &BranchState) -> Result<Vec<u8>, FormatError> {
    let mut e = CanonicalEncoder::new();
    e.fixed(branch.workspace_id.as_bytes());
    e.fixed(branch.branch_id.as_bytes());
    e.bytes(branch.name.as_bytes())?;
    e.fixed(branch.head.as_bytes());
    e.u64_le(branch.revision);
    e.u64_le(branch.pending_mutation_preparations);
    e.u64_le(branch.writer_epoch);
    e.u64_le(branch.volume_epoch);
    e.u64_le(branch.stream_epoch);
    e.object_ref(branch.volume_root);
    e.object_ref(branch.logs_root);
    e.object_ref(branch.artifacts_root);
    match branch.writer {
        Some(grant) => {
            e.u8(1);
            encode_writer_grant(&mut e, grant);
        }
        None => e.u8(0),
    }
    match branch.attachment {
        Some(attachment) => {
            e.u8(1);
            encode_attachment(&mut e, attachment);
        }
        None => e.u8(0),
    }
    match &branch.capture {
        Some(capture) => {
            e.u8(1);
            encode_capture(&mut e, capture);
        }
        None => e.u8(0),
    }
    e.u8(u8::from(branch.deleted));
    encode_metadata_record(MetadataRecordType::Branch, &e.finish())
}

fn decode_branch(bytes: &[u8]) -> Result<BranchState, ControlError> {
    let (record_type, payload) = decode_metadata_record(bytes).map_err(corrupt)?;
    if record_type != MetadataRecordType::Branch {
        return Err(ControlError::CorruptRecord(
            "wrong branch record type".into(),
        ));
    }
    let mut d = CanonicalDecoder::new(payload);
    let workspace_id = WorkspaceId::from_bytes(d.fixed().map_err(corrupt)?);
    let branch_id = BranchId::from_bytes(d.fixed().map_err(corrupt)?);
    let name = std::str::from_utf8(d.bytes(255).map_err(corrupt)?)
        .map_err(|_| ControlError::CorruptRecord("branch name is not UTF-8".into()))?
        .to_owned();
    let head = GenerationId::from_bytes(d.fixed().map_err(corrupt)?);
    let revision = d.u64_le().map_err(corrupt)?;
    let pending_mutation_preparations = d.u64_le().map_err(corrupt)?;
    let writer_epoch = d.u64_le().map_err(corrupt)?;
    let volume_epoch = d.u64_le().map_err(corrupt)?;
    let stream_epoch = d.u64_le().map_err(corrupt)?;
    let volume_root = d.object_ref().map_err(corrupt)?;
    let logs_root = d.object_ref().map_err(corrupt)?;
    let artifacts_root = d.object_ref().map_err(corrupt)?;
    validate_live_roots(LiveRoots {
        volume: volume_root,
        logs: logs_root,
        artifacts: artifacts_root,
    })
    .map_err(corrupt)?;
    let writer = match d.u8().map_err(corrupt)? {
        0 => None,
        1 => Some(decode_writer_grant(&mut d).map_err(corrupt)?),
        _ => return Err(ControlError::CorruptRecord("invalid writer option".into())),
    };
    let attachment = match d.u8().map_err(corrupt)? {
        0 => None,
        1 => Some(decode_attachment_fields(&mut d).map_err(corrupt)?),
        _ => {
            return Err(ControlError::CorruptRecord(
                "invalid attachment option".into(),
            ));
        }
    };
    let capture = match d.u8().map_err(corrupt)? {
        0 => None,
        1 => Some(decode_capture_fields(&mut d).map_err(corrupt)?),
        _ => return Err(ControlError::CorruptRecord("invalid capture option".into())),
    };
    let deleted = decode_bool(d.u8().map_err(corrupt)?)?;
    d.finish().map_err(corrupt)?;
    Ok(BranchState {
        workspace_id,
        branch_id,
        name,
        head,
        revision,
        pending_mutation_preparations,
        writer_epoch,
        volume_epoch,
        stream_epoch,
        volume_root,
        logs_root,
        artifacts_root,
        writer,
        attachment,
        capture,
        deleted,
    })
}

fn encode_generation(record: &GenerationRecord) -> Result<Vec<u8>, FormatError> {
    let mut e = CanonicalEncoder::new();
    encode_generation_seed(&mut e, record.seed);
    e.u8(u8::from(record.retained));
    e.u64_le(record.branch_refs);
    e.u64_le(record.pin_refs);
    e.u64_le(record.reader_refs);
    e.u64_le(record.attachment_refs);
    encode_metadata_record(MetadataRecordType::Generation, &e.finish())
}

fn decode_generation(bytes: &[u8]) -> Result<GenerationRecord, ControlError> {
    let (record_type, payload) = decode_metadata_record(bytes).map_err(corrupt)?;
    if record_type != MetadataRecordType::Generation {
        return Err(ControlError::CorruptRecord(
            "wrong Generation record type".into(),
        ));
    }
    let mut d = CanonicalDecoder::new(payload);
    let seed = decode_generation_seed(&mut d).map_err(corrupt)?;
    let retained = decode_bool(d.u8().map_err(corrupt)?)?;
    let branch_refs = d.u64_le().map_err(corrupt)?;
    let pin_refs = d.u64_le().map_err(corrupt)?;
    let reader_refs = d.u64_le().map_err(corrupt)?;
    let attachment_refs = d.u64_le().map_err(corrupt)?;
    d.finish().map_err(corrupt)?;
    Ok(GenerationRecord {
        seed,
        retained,
        branch_refs,
        pin_refs,
        reader_refs,
        attachment_refs,
    })
}

fn encode_workspace(id: WorkspaceId, retention_ms: u64) -> Result<Vec<u8>, FormatError> {
    let mut e = CanonicalEncoder::new();
    e.fixed(id.as_bytes());
    e.u64_le(retention_ms);
    encode_metadata_record(MetadataRecordType::Workspace, &e.finish())
}

fn encode_owner(incarnation: u64) -> Result<Vec<u8>, FormatError> {
    let mut e = CanonicalEncoder::new();
    e.u64_le(incarnation);
    encode_metadata_record(MetadataRecordType::AuthorityOwner, &e.finish())
}

fn decode_owner(bytes: &[u8]) -> Result<u64, FormatError> {
    let (record_type, payload) = decode_metadata_record(bytes)?;
    if record_type != MetadataRecordType::AuthorityOwner {
        return Err(FormatError::InvalidEnvelope("wrong owner type"));
    }
    let mut d = CanonicalDecoder::new(payload);
    let owner = d.u64_le()?;
    d.finish()?;
    Ok(owner)
}

fn encode_protection(record: &ProtectionRecord) -> Result<Vec<u8>, FormatError> {
    let mut e = CanonicalEncoder::new();
    match record {
        ProtectionRecord::Retired { id } => {
            e.u8(5);
            e.fixed(id.as_bytes());
        }
        ProtectionRecord::AttachmentBase {
            id,
            branch_id,
            attachment_id,
            generation_id,
            roots,
            owner_incarnation,
            expires_at,
        } => {
            e.u8(1);
            e.fixed(id.as_bytes());
            e.fixed(branch_id.as_bytes());
            e.fixed(attachment_id.as_bytes());
            e.fixed(generation_id.as_bytes());
            encode_live_roots(&mut e, *roots);
            e.u64_le(owner_incarnation.get());
            e.u64_le(expires_at.as_nanos());
        }
        ProtectionRecord::GenerationRead {
            id,
            generation_id,
            owner_incarnation,
            expires_at,
        } => {
            e.u8(2);
            e.fixed(id.as_bytes());
            e.fixed(generation_id.as_bytes());
            e.u64_le(owner_incarnation.get());
            e.u64_le(expires_at.as_nanos());
        }
        ProtectionRecord::LiveLogRead {
            id,
            branch_id,
            revision,
            root,
            owner_incarnation,
            expires_at,
        } => {
            e.u8(3);
            e.fixed(id.as_bytes());
            e.fixed(branch_id.as_bytes());
            e.u64_le(*revision);
            e.object_ref(*root);
            e.u64_le(owner_incarnation.get());
            e.u64_le(expires_at.as_nanos());
        }
        ProtectionRecord::Preparation {
            id,
            operation_id,
            branch_id,
            attachment_id,
            volume_epoch,
            grant,
            owner_incarnation,
            expires_at,
            dependencies,
        } => {
            e.u8(4);
            e.fixed(id.as_bytes());
            e.fixed(operation_id.as_bytes());
            e.fixed(branch_id.as_bytes());
            e.fixed(attachment_id.as_bytes());
            e.u64_le(*volume_epoch);
            encode_writer_grant(&mut e, *grant);
            e.u64_le(owner_incarnation.get());
            e.u64_le(expires_at.as_nanos());
            e.u32_le(dependencies.len() as u32);
            for dep in dependencies {
                e.object_ref(*dep);
            }
        }
    }
    encode_metadata_record(MetadataRecordType::Protection, &e.finish())
}

fn decode_protection(bytes: &[u8]) -> Result<ProtectionRecord, ControlError> {
    let (record_type, payload) = decode_metadata_record(bytes).map_err(corrupt)?;
    if record_type != MetadataRecordType::Protection {
        return Err(ControlError::CorruptRecord("wrong protection type".into()));
    }
    let mut d = CanonicalDecoder::new(payload);
    let record = match d.u8().map_err(corrupt)? {
        5 => ProtectionRecord::Retired {
            id: ProtectionId::from_bytes(d.fixed().map_err(corrupt)?),
        },
        1 => ProtectionRecord::AttachmentBase {
            id: ProtectionId::from_bytes(d.fixed().map_err(corrupt)?),
            branch_id: BranchId::from_bytes(d.fixed().map_err(corrupt)?),
            attachment_id: AttachmentId::from_bytes(d.fixed().map_err(corrupt)?),
            generation_id: GenerationId::from_bytes(d.fixed().map_err(corrupt)?),
            roots: decode_live_roots(&mut d).map_err(corrupt)?,
            owner_incarnation: OwnerIncarnation(d.u64_le().map_err(corrupt)?),
            expires_at: MonotonicTime::from_nanos(d.u64_le().map_err(corrupt)?),
        },
        2 => ProtectionRecord::GenerationRead {
            id: ProtectionId::from_bytes(d.fixed().map_err(corrupt)?),
            generation_id: GenerationId::from_bytes(d.fixed().map_err(corrupt)?),
            owner_incarnation: OwnerIncarnation(d.u64_le().map_err(corrupt)?),
            expires_at: MonotonicTime::from_nanos(d.u64_le().map_err(corrupt)?),
        },
        3 => ProtectionRecord::LiveLogRead {
            id: ProtectionId::from_bytes(d.fixed().map_err(corrupt)?),
            branch_id: BranchId::from_bytes(d.fixed().map_err(corrupt)?),
            revision: d.u64_le().map_err(corrupt)?,
            root: d.object_ref().map_err(corrupt)?,
            owner_incarnation: OwnerIncarnation(d.u64_le().map_err(corrupt)?),
            expires_at: MonotonicTime::from_nanos(d.u64_le().map_err(corrupt)?),
        },
        4 => {
            let id = ProtectionId::from_bytes(d.fixed().map_err(corrupt)?);
            let operation_id = OperationId::from_bytes(d.fixed().map_err(corrupt)?);
            let branch_id = BranchId::from_bytes(d.fixed().map_err(corrupt)?);
            let attachment_id = AttachmentId::from_bytes(d.fixed().map_err(corrupt)?);
            let volume_epoch = d.u64_le().map_err(corrupt)?;
            let grant = decode_writer_grant(&mut d).map_err(corrupt)?;
            let owner_incarnation = OwnerIncarnation(d.u64_le().map_err(corrupt)?);
            let expires_at = MonotonicTime::from_nanos(d.u64_le().map_err(corrupt)?);
            let count = d.u32_le().map_err(corrupt)? as usize;
            if count > 4096 {
                return Err(ControlError::CorruptRecord(
                    "too many protected dependencies".into(),
                ));
            }
            let mut dependencies = Vec::with_capacity(count);
            for _ in 0..count {
                dependencies.push(d.object_ref().map_err(corrupt)?);
            }
            ProtectionRecord::Preparation {
                id,
                operation_id,
                branch_id,
                attachment_id,
                volume_epoch,
                grant,
                owner_incarnation,
                expires_at,
                dependencies,
            }
        }
        _ => {
            return Err(ControlError::CorruptRecord(
                "unknown protection kind".into(),
            ));
        }
    };
    d.finish().map_err(corrupt)?;
    Ok(record)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PinRecord {
    generation_id: GenerationId,
    active: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CaptureChildReservation {
    capture_id: OperationId,
    branch_id: BranchId,
}

fn encode_capture_child_reservation(
    reservation: CaptureChildReservation,
) -> Result<Vec<u8>, FormatError> {
    let mut e = CanonicalEncoder::new();
    e.fixed(reservation.capture_id.as_bytes());
    e.fixed(reservation.branch_id.as_bytes());
    encode_metadata_record(MetadataRecordType::CaptureChildReservation, &e.finish())
}

fn decode_capture_child_reservation(bytes: &[u8]) -> Result<CaptureChildReservation, FormatError> {
    let (record_type, payload) = decode_metadata_record(bytes)?;
    if record_type != MetadataRecordType::CaptureChildReservation {
        return Err(FormatError::InvalidEnvelope(
            "wrong capture child reservation type",
        ));
    }
    let mut d = CanonicalDecoder::new(payload);
    let reservation = CaptureChildReservation {
        capture_id: OperationId::from_bytes(d.fixed()?),
        branch_id: BranchId::from_bytes(d.fixed()?),
    };
    d.finish()?;
    Ok(reservation)
}

fn encode_pin(pin: PinRecord) -> Result<Vec<u8>, FormatError> {
    let mut e = CanonicalEncoder::new();
    e.fixed(pin.generation_id.as_bytes());
    e.u8(u8::from(pin.active));
    encode_metadata_record(MetadataRecordType::Pin, &e.finish())
}

fn decode_pin(bytes: &[u8]) -> Result<PinRecord, FormatError> {
    let (record_type, payload) = decode_metadata_record(bytes)?;
    if record_type != MetadataRecordType::Pin {
        return Err(FormatError::InvalidEnvelope("wrong pin type"));
    }
    let mut d = CanonicalDecoder::new(payload);
    let generation_id = GenerationId::from_bytes(d.fixed()?);
    let active = match d.u8()? {
        0 => false,
        1 => true,
        _ => return Err(FormatError::InvalidEnvelope("invalid pin state")),
    };
    d.finish()?;
    Ok(PinRecord {
        generation_id,
        active,
    })
}

fn encode_name_binding(branch: BranchId) -> Result<Vec<u8>, FormatError> {
    let mut e = CanonicalEncoder::new();
    e.fixed(branch.as_bytes());
    encode_metadata_record(MetadataRecordType::BranchNameIndex, &e.finish())
}

fn decode_name_binding(bytes: &[u8]) -> Result<BranchId, ControlError> {
    let (record_type, payload) = decode_metadata_record(bytes).map_err(corrupt)?;
    if record_type != MetadataRecordType::BranchNameIndex {
        return Err(ControlError::CorruptRecord(
            "wrong branch-name index type".into(),
        ));
    }
    let mut d = CanonicalDecoder::new(payload);
    let branch = BranchId::from_bytes(d.fixed().map_err(corrupt)?);
    d.finish().map_err(corrupt)?;
    Ok(branch)
}

fn encode_writer_grant(e: &mut CanonicalEncoder, grant: WriterGrant) {
    e.fixed(grant.branch_id.as_bytes());
    e.u64_le(grant.writer_epoch);
    e.u64_le(grant.owner_incarnation.get());
    e.fixed(grant.grant_id.as_bytes());
    e.u64_le(grant.expires_at.as_nanos());
}

fn decode_writer_grant(d: &mut CanonicalDecoder<'_>) -> Result<WriterGrant, FormatError> {
    Ok(WriterGrant {
        branch_id: BranchId::from_bytes(d.fixed()?),
        writer_epoch: d.u64_le()?,
        owner_incarnation: OwnerIncarnation(d.u64_le()?),
        grant_id: OperationId::from_bytes(d.fixed()?),
        expires_at: MonotonicTime::from_nanos(d.u64_le()?),
    })
}

fn encode_writer_grant_result(grant: WriterGrant) -> Vec<u8> {
    let mut e = CanonicalEncoder::new();
    encode_writer_grant(&mut e, grant);
    e.finish()
}

fn decode_writer_grant_result(bytes: &[u8]) -> Result<WriterGrant, ControlError> {
    decode_writer_grant_from_bytes(bytes)
}

fn decode_writer_grant_from_bytes(bytes: &[u8]) -> Result<WriterGrant, ControlError> {
    let mut d = CanonicalDecoder::new(bytes);
    let grant = decode_writer_grant(&mut d).map_err(corrupt)?;
    d.finish().map_err(corrupt)?;
    Ok(grant)
}

fn encode_attachment(e: &mut CanonicalEncoder, value: AttachmentState) {
    e.fixed(value.attachment_id.as_bytes());
    e.fixed(value.protection_id.as_bytes());
    e.fixed(value.base_generation.as_bytes());
    e.u64_le(value.writer_epoch);
    e.u64_le(value.volume_epoch);
    e.u64_le(value.protection_expires_at.as_nanos());
}

fn encode_attachment_result(value: AttachmentState) -> Vec<u8> {
    let mut e = CanonicalEncoder::new();
    encode_attachment(&mut e, value);
    e.finish()
}

fn decode_attachment_result(bytes: &[u8]) -> Result<AttachmentState, ControlError> {
    let mut d = CanonicalDecoder::new(bytes);
    let value = decode_attachment_fields(&mut d).map_err(corrupt)?;
    d.finish().map_err(corrupt)?;
    Ok(value)
}

fn decode_attachment_fields(d: &mut CanonicalDecoder<'_>) -> Result<AttachmentState, FormatError> {
    Ok(AttachmentState {
        attachment_id: AttachmentId::from_bytes(d.fixed()?),
        protection_id: ProtectionId::from_bytes(d.fixed()?),
        base_generation: GenerationId::from_bytes(d.fixed()?),
        writer_epoch: d.u64_le()?,
        volume_epoch: d.u64_le()?,
        protection_expires_at: MonotonicTime::from_nanos(d.u64_le()?),
    })
}

fn encode_capture(e: &mut CanonicalEncoder, value: &CaptureState) {
    e.fixed(value.operation_id.as_bytes());
    e.fixed(value.principal_id.as_bytes());
    e.fixed(value.protection_id.as_bytes());
    e.u64_le(value.expected_revision);
    e.u8(u8::from(value.sealed));
    match value.pending_child {
        Some(id) => {
            e.u8(1);
            e.fixed(id.as_bytes());
        }
        None => e.u8(0),
    }
    match value.sealed_revision {
        Some(revision) => {
            e.u8(1);
            e.u64_le(revision);
        }
        None => e.u8(0),
    }
    match value.sealed_roots {
        Some(roots) => {
            e.u8(1);
            encode_live_roots(e, roots);
        }
        None => e.u8(0),
    }
}

fn encode_capture_result(value: &CaptureState) -> Vec<u8> {
    let mut e = CanonicalEncoder::new();
    encode_capture(&mut e, value);
    e.finish()
}

fn decode_capture_result(bytes: &[u8]) -> Result<CaptureState, ControlError> {
    let mut d = CanonicalDecoder::new(bytes);
    let value = decode_capture_fields(&mut d).map_err(corrupt)?;
    d.finish().map_err(corrupt)?;
    Ok(value)
}

fn decode_capture_fields(d: &mut CanonicalDecoder<'_>) -> Result<CaptureState, FormatError> {
    let operation_id = OperationId::from_bytes(d.fixed()?);
    let principal_id = PrincipalId::from_bytes(d.fixed()?);
    let protection_id = ProtectionId::from_bytes(d.fixed()?);
    let expected_revision = d.u64_le()?;
    let sealed =
        decode_bool(d.u8()?).map_err(|_| FormatError::InvalidEnvelope("invalid capture flag"))?;
    let pending_child = match d.u8()? {
        0 => None,
        1 => Some(OperationId::from_bytes(d.fixed()?)),
        _ => return Err(FormatError::InvalidEnvelope("invalid child option")),
    };
    let sealed_revision = match d.u8()? {
        0 => None,
        1 => Some(d.u64_le()?),
        _ => {
            return Err(FormatError::InvalidEnvelope(
                "invalid sealed revision option",
            ));
        }
    };
    let sealed_roots = match d.u8()? {
        0 => None,
        1 => Some(decode_live_roots(d)?),
        _ => return Err(FormatError::InvalidEnvelope("invalid sealed roots option")),
    };
    if sealed != sealed_revision.is_some()
        || sealed != sealed_roots.is_some()
        || (sealed && pending_child.is_some())
    {
        return Err(FormatError::InvalidEnvelope("inconsistent capture state"));
    }
    Ok(CaptureState {
        operation_id,
        principal_id,
        protection_id,
        expected_revision,
        sealed,
        pending_child,
        sealed_revision,
        sealed_roots,
    })
}

fn encode_live_roots(e: &mut CanonicalEncoder, roots: LiveRoots) {
    e.object_ref(roots.volume);
    e.object_ref(roots.logs);
    e.object_ref(roots.artifacts);
}

fn decode_live_roots(d: &mut CanonicalDecoder<'_>) -> Result<LiveRoots, FormatError> {
    let roots = LiveRoots {
        volume: d.object_ref()?,
        logs: d.object_ref()?,
        artifacts: d.object_ref()?,
    };
    validate_live_roots(roots)?;
    Ok(roots)
}

fn encode_generation_seed(e: &mut CanonicalEncoder, seed: GenerationSeed) {
    e.fixed(seed.generation_id.as_bytes());
    e.object_ref(seed.descriptor);
    encode_live_roots(e, seed.roots);
}

fn decode_generation_seed(d: &mut CanonicalDecoder<'_>) -> Result<GenerationSeed, FormatError> {
    let seed = GenerationSeed {
        generation_id: GenerationId::from_bytes(d.fixed()?),
        descriptor: d.object_ref()?,
        roots: decode_live_roots(d)?,
    };
    validate_generation_seed(seed)?;
    Ok(seed)
}

fn validate_generation_seed(seed: GenerationSeed) -> Result<(), FormatError> {
    validate_object_ref_type(seed.descriptor, ObjectType::GenerationDescriptor)?;
    validate_live_roots(seed.roots)
}

fn validate_live_roots(roots: LiveRoots) -> Result<(), FormatError> {
    validate_object_ref_type(roots.volume, ObjectType::VolumeRoot)?;
    validate_object_ref_type(roots.logs, ObjectType::LogMapRoot)?;
    validate_object_ref_type(roots.artifacts, ObjectType::ArtifactMapRoot)
}

fn validate_object_ref_type(reference: ObjectRef, expected: ObjectType) -> Result<(), FormatError> {
    reference.validate()?;
    if reference.object_type != expected {
        return Err(FormatError::ObjectReferenceMismatch);
    }
    Ok(())
}

fn validate_preparation_root_scope(
    kind: PreparationKind,
    current: LiveRoots,
    next: LiveRoots,
) -> Result<(), MetadataError> {
    let valid = match kind {
        PreparationKind::VolumeBarrier => {
            next.logs == current.logs && next.artifacts == current.artifacts
        }
        PreparationKind::LogMutation => {
            next.volume == current.volume && next.artifacts == current.artifacts
        }
        PreparationKind::ArtifactMutation => {
            next.volume == current.volume && next.logs == current.logs
        }
        PreparationKind::CheckpointCapture => false,
    };
    if valid {
        Ok(())
    } else {
        Err(reject(
            "invalid_request: preparation kind may replace only its own live root",
        ))
    }
}

fn decode_preparation_kind(value: u8) -> Result<PreparationKind, FormatError> {
    match value {
        1 => Ok(PreparationKind::VolumeBarrier),
        2 => Ok(PreparationKind::LogMutation),
        3 => Ok(PreparationKind::ArtifactMutation),
        4 => Ok(PreparationKind::CheckpointCapture),
        _ => Err(FormatError::InvalidEnvelope("unknown preparation kind")),
    }
}

fn decode_deadline(bytes: &[u8]) -> Result<MonotonicTime, ControlError> {
    let raw: [u8; 8] = bytes
        .try_into()
        .map_err(|_| ControlError::CorruptRecord("invalid deadline result".into()))?;
    Ok(MonotonicTime::from_nanos(u64::from_le_bytes(raw)))
}

fn reconcile_outcome<A: MetadataAuthority>(
    authority: &A,
    key: &[u8],
) -> Result<Option<Vec<u8>>, ControlError> {
    let outcome = match authority.reconcile_unknown(MutationIdentity {
        operation_id: key,
        outcome_key: key,
    }) {
        Ok(outcome) => outcome,
        Err(MetadataError::ReconciliationRequired {
            operation_id: pending,
        }) => {
            let pending_outcome = authority
                .reconcile_unknown(MutationIdentity {
                    operation_id: &pending,
                    outcome_key: &pending,
                })
                .map_err(map_metadata_error)?;
            if pending.as_slice() == key {
                pending_outcome
            } else {
                authority
                    .reconcile_unknown(MutationIdentity {
                        operation_id: key,
                        outcome_key: key,
                    })
                    .map_err(map_metadata_error)?
            }
        }
        Err(error) => return Err(map_metadata_error(error)),
    };
    match outcome {
        ReconciledOutcome::Committed(bytes) => Ok(Some(bytes)),
        ReconciledOutcome::NotCommitted => Ok(None),
    }
}

fn decode_committed_live_roots(
    bytes: &[u8],
    expected_fingerprint: [u8; 32],
) -> Result<BranchState, ControlError> {
    let record = decode_operation(bytes)?;
    if record.kind != OP_BEGIN_PREPARATION
        || record.commit_fingerprint != Some(expected_fingerprint)
    {
        return Err(ControlError::OperationMismatch);
    }
    match record.status {
        StoredOperationStatus::Committed(branch) => decode_branch(&branch),
        StoredOperationStatus::Aborted => Err(ControlError::Aborted),
        StoredOperationStatus::Preparing { .. } => Err(ControlError::InProgress),
    }
}

fn decode_bool(value: u8) -> Result<bool, ControlError> {
    match value {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(ControlError::CorruptRecord("invalid boolean".into())),
    }
}

fn validate_name(name: &str) -> Result<(), ControlError> {
    if name.is_empty() || name.len() > 255 || name.as_bytes().contains(&0) {
        return Err(ControlError::InvalidRequest(
            "branch name must be 1..=255 bytes without NUL",
        ));
    }
    Ok(())
}

fn ensure_live_branch(branch: &BranchState) -> Result<(), MetadataError> {
    if branch.deleted {
        Err(reject("not_found"))
    } else {
        Ok(())
    }
}

fn validate_grant_identity(
    branch: &BranchState,
    grant: WriterGrant,
    owner: OwnerIncarnation,
) -> Result<(), MetadataError> {
    if grant.owner_incarnation != owner
        || grant.branch_id != branch.branch_id
        || branch.writer != Some(grant)
    {
        return Err(reject("fenced"));
    }
    Ok(())
}

fn same_grant_identity(left: WriterGrant, right: WriterGrant) -> bool {
    left.branch_id == right.branch_id
        && left.writer_epoch == right.writer_epoch
        && left.owner_incarnation == right.owner_incarnation
        && left.grant_id == right.grant_id
}

fn validate_capture_principal(
    capture: &CaptureState,
    principal_id: PrincipalId,
) -> Result<(), MetadataError> {
    if capture.principal_id != principal_id {
        return Err(reject("fenced"));
    }
    Ok(())
}

fn validate_attachment_protection(
    txn: &mut dyn MetadataTxn,
    workspace: WorkspaceId,
    branch: &BranchState,
    attachment_id: AttachmentId,
    volume_epoch: u64,
    grant: WriterGrant,
    owner: OwnerIncarnation,
) -> Result<(), MetadataError> {
    let attachment = branch.attachment.ok_or_else(|| reject("fenced"))?;
    if attachment.attachment_id != attachment_id
        || attachment.volume_epoch != volume_epoch
        || attachment.writer_epoch != grant.writer_epoch
    {
        return Err(reject("fenced"));
    }
    let pkey = protection_key(workspace, attachment.protection_id);
    let bytes = txn.get(&pkey)?.ok_or_else(|| reject("expired"))?;
    match decode_protection(&bytes).map_err(to_metadata_error)? {
        ProtectionRecord::AttachmentBase {
            branch_id,
            attachment_id: protected_attachment,
            generation_id,
            owner_incarnation,
            expires_at,
            ..
        } if branch_id == branch.branch_id
            && protected_attachment == attachment_id
            && generation_id == attachment.base_generation
            && owner_incarnation == owner
            && expires_at == attachment.protection_expires_at =>
        {
            if expires_at <= txn.now() {
                return Err(reject("expired"));
            }
            let generation =
                read_generation(txn, &generation_key(workspace, attachment.base_generation))?;
            if generation.attachment_refs == 0 {
                return Err(reject("corrupt_record"));
            }
            Ok(())
        }
        ProtectionRecord::AttachmentBase { .. } => Err(reject("corrupt_record")),
        _ => Err(reject("fenced")),
    }
}

fn validate_writer(
    branch: &BranchState,
    grant: WriterGrant,
    owner: OwnerIncarnation,
    now: MonotonicTime,
) -> Result<(), MetadataError> {
    ensure_live_branch(branch)?;
    validate_grant_identity(branch, grant, owner)?;
    if grant.expires_at <= now {
        return Err(reject("expired"));
    }
    Ok(())
}

fn read_branch(txn: &mut dyn MetadataTxn, key: &[u8]) -> Result<BranchState, MetadataError> {
    txn.get(key)?
        .map(|v| decode_branch(&v).map_err(to_metadata_error))
        .transpose()?
        .ok_or_else(|| reject("not_found"))
}

fn read_generation(
    txn: &mut dyn MetadataTxn,
    key: &[u8],
) -> Result<GenerationRecord, MetadataError> {
    txn.get(key)?
        .map(|v| decode_generation(&v).map_err(to_metadata_error))
        .transpose()?
        .ok_or_else(|| reject("not_found"))
}

fn fence_attachment(
    txn: &mut dyn MetadataTxn,
    workspace: WorkspaceId,
    branch: &mut BranchState,
) -> Result<(), MetadataError> {
    if branch.capture.is_some() {
        return Err(reject("busy"));
    }
    if let Some(attachment) = branch.attachment.take() {
        let gkey = generation_key(workspace, attachment.base_generation);
        let mut generation = read_generation(txn, &gkey)?;
        generation.attachment_refs = decrement(generation.attachment_refs)?;
        txn.put(
            &gkey,
            &encode_generation(&generation).map_err(to_metadata_error)?,
        )?;
        let pkey = protection_key(workspace, attachment.protection_id);
        let bytes = txn.get(&pkey)?.ok_or_else(|| reject("corrupt_record"))?;
        match decode_protection(&bytes).map_err(to_metadata_error)? {
            ProtectionRecord::AttachmentBase {
                attachment_id,
                generation_id,
                ..
            } if attachment_id == attachment.attachment_id
                && generation_id == attachment.base_generation => {}
            _ => return Err(reject("corrupt_record")),
        }
        retire_protection(txn, workspace, attachment.protection_id)?;
        branch.volume_epoch = increment(branch.volume_epoch)?;
        branch.stream_epoch = increment(branch.stream_epoch)?;
    }
    Ok(())
}

fn release_attachment(
    txn: &mut dyn MetadataTxn,
    workspace: WorkspaceId,
    branch: &mut BranchState,
) -> Result<(), MetadataError> {
    if let Some(attachment) = branch.attachment.take() {
        let gkey = generation_key(workspace, attachment.base_generation);
        let mut generation = read_generation(txn, &gkey)?;
        generation.attachment_refs = decrement(generation.attachment_refs)?;
        txn.put(
            &gkey,
            &encode_generation(&generation).map_err(to_metadata_error)?,
        )?;
        let pkey = protection_key(workspace, attachment.protection_id);
        let bytes = txn.get(&pkey)?.ok_or_else(|| reject("corrupt_record"))?;
        match decode_protection(&bytes).map_err(to_metadata_error)? {
            ProtectionRecord::AttachmentBase {
                attachment_id,
                generation_id,
                ..
            } if attachment_id == attachment.attachment_id
                && generation_id == attachment.base_generation => {}
            _ => return Err(reject("corrupt_record")),
        }
        retire_protection(txn, workspace, attachment.protection_id)?;
        branch.volume_epoch = increment(branch.volume_epoch)?;
        branch.stream_epoch = increment(branch.stream_epoch)?;
    }
    Ok(())
}

fn close_protection(
    txn: &mut dyn MetadataTxn,
    workspace: WorkspaceId,
    id: ProtectionId,
) -> Result<(), MetadataError> {
    let key = protection_key(workspace, id);
    let bytes = txn.get(&key)?.ok_or_else(|| reject("not_found"))?;
    match decode_protection(&bytes).map_err(to_metadata_error)? {
        ProtectionRecord::GenerationRead { generation_id, .. } => {
            let gkey = generation_key(workspace, generation_id);
            let mut generation = read_generation(txn, &gkey)?;
            generation.reader_refs = decrement(generation.reader_refs)?;
            txn.put(
                &gkey,
                &encode_generation(&generation).map_err(to_metadata_error)?,
            )?;
        }
        ProtectionRecord::AttachmentBase { .. } | ProtectionRecord::Preparation { .. } => {
            return Err(reject("invalid_transition"));
        }
        ProtectionRecord::LiveLogRead { .. } => {}
        ProtectionRecord::Retired { .. } => return Err(reject("not_found")),
    }
    retire_protection(txn, workspace, id)
}

fn retire_protection(
    txn: &mut dyn MetadataTxn,
    workspace: WorkspaceId,
    id: ProtectionId,
) -> Result<(), MetadataError> {
    txn.put(
        &protection_key(workspace, id),
        &encode_protection(&ProtectionRecord::Retired { id }).map_err(to_metadata_error)?,
    )
}

fn require_expired(
    owner: OwnerIncarnation,
    expires_at: MonotonicTime,
    current_owner: OwnerIncarnation,
    now: MonotonicTime,
) -> Result<(), MetadataError> {
    if owner == current_owner && expires_at > now {
        return Err(reject("busy"));
    }
    Ok(())
}

fn validate_capture_preparation(
    txn: &mut dyn MetadataTxn,
    workspace: WorkspaceId,
    capture_id: OperationId,
    protection_id: ProtectionId,
    grant: WriterGrant,
    owner: OwnerIncarnation,
) -> Result<(), MetadataError> {
    let pkey = protection_key(workspace, protection_id);
    let snapshot = txn.get(&pkey)?.ok_or_else(|| reject("expired"))?;
    match decode_protection(&snapshot).map_err(to_metadata_error)? {
        ProtectionRecord::Preparation {
            operation_id,
            branch_id,
            grant: prepared_grant,
            owner_incarnation,
            expires_at,
            ..
        } if operation_id == capture_id
            && branch_id == grant.branch_id
            && same_grant_identity(prepared_grant, grant)
            && owner_incarnation == owner
            && expires_at > txn.now() =>
        {
            Ok(())
        }
        _ => Err(reject("expired")),
    }
}

fn increment(value: u64) -> Result<u64, MetadataError> {
    value.checked_add(1).ok_or_else(|| reject("overflow"))
}

fn decrement(value: u64) -> Result<u64, MetadataError> {
    value
        .checked_sub(1)
        .ok_or_else(|| reject("corrupt_counter"))
}

fn reject(code: &str) -> MetadataError {
    MetadataError::Rejected(format!("{REJECT_PREFIX}{code}"))
}

fn to_metadata_error(error: impl Into<ControlError>) -> MetadataError {
    match error.into() {
        ControlError::Conflict => MetadataError::Conflict,
        ControlError::OperationMismatch => reject("operation_mismatch"),
        ControlError::Aborted => reject("aborted"),
        ControlError::InProgress => reject("in_progress"),
        ControlError::NotFound => reject("not_found"),
        ControlError::Fenced => reject("fenced"),
        ControlError::Expired => reject("expired"),
        ControlError::Busy => reject("busy"),
        ControlError::GenerationExpired => reject("generation_expired"),
        ControlError::InvalidTransition => reject("invalid_transition"),
        ControlError::InvalidRequest(_) => reject("invalid_request"),
        ControlError::CorruptRecord(message) => MetadataError::Backend(message),
        ControlError::Format(error) => MetadataError::Backend(error.to_string()),
        ControlError::Metadata(error) => error,
    }
}

fn map_metadata_error(error: MetadataError) -> ControlError {
    match error {
        MetadataError::Conflict => ControlError::Conflict,
        MetadataError::Rejected(message)
            if message == format!("{REJECT_PREFIX}operation_mismatch") =>
        {
            ControlError::OperationMismatch
        }
        MetadataError::Rejected(message) if message == format!("{REJECT_PREFIX}aborted") => {
            ControlError::Aborted
        }
        MetadataError::Rejected(message) if message == format!("{REJECT_PREFIX}in_progress") => {
            ControlError::InProgress
        }
        MetadataError::Rejected(message) if message == format!("{REJECT_PREFIX}not_found") => {
            ControlError::NotFound
        }
        MetadataError::Rejected(message) if message == format!("{REJECT_PREFIX}fenced") => {
            ControlError::Fenced
        }
        MetadataError::Rejected(message) if message == format!("{REJECT_PREFIX}expired") => {
            ControlError::Expired
        }
        MetadataError::Rejected(message) if message == format!("{REJECT_PREFIX}busy") => {
            ControlError::Busy
        }
        MetadataError::Rejected(message)
            if message == format!("{REJECT_PREFIX}generation_expired") =>
        {
            ControlError::GenerationExpired
        }
        MetadataError::Rejected(message)
            if message == format!("{REJECT_PREFIX}invalid_transition") =>
        {
            ControlError::InvalidTransition
        }
        MetadataError::Rejected(message)
            if message.starts_with(&format!("{REJECT_PREFIX}invalid_request")) =>
        {
            ControlError::InvalidRequest("request or monotone counter is invalid")
        }
        other => ControlError::Metadata(other),
    }
}

fn corrupt(error: FormatError) -> ControlError {
    ControlError::CorruptRecord(error.to_string())
}

fn owner_incarnation_key() -> Vec<u8> {
    [KEY_PREFIX, b"owner/incarnation"].concat()
}
fn owner_operation_key(id: OperationId) -> Vec<u8> {
    [KEY_PREFIX, b"owner/operation/", id.as_bytes()].concat()
}
fn workspace_key(workspace: WorkspaceId) -> Vec<u8> {
    [KEY_PREFIX, b"workspace/", workspace.as_bytes()].concat()
}
fn branch_key(workspace: WorkspaceId, branch: BranchId) -> Vec<u8> {
    [
        KEY_PREFIX,
        b"branch/",
        workspace.as_bytes(),
        branch.as_bytes(),
    ]
    .concat()
}
fn branch_name_key(workspace: WorkspaceId, name: &str) -> Vec<u8> {
    let mut key = [KEY_PREFIX, b"branch-name/", workspace.as_bytes()].concat();
    key.extend_from_slice(&(name.len() as u16).to_be_bytes());
    key.extend_from_slice(name.as_bytes());
    key
}
fn capture_child_reservation_key(
    workspace: WorkspaceId,
    principal: PrincipalId,
    child: OperationId,
) -> Vec<u8> {
    [
        KEY_PREFIX,
        b"capture-child/",
        workspace.as_bytes(),
        principal.as_bytes(),
        child.as_bytes(),
    ]
    .concat()
}
fn generation_key(workspace: WorkspaceId, generation: GenerationId) -> Vec<u8> {
    [
        KEY_PREFIX,
        b"generation/",
        workspace.as_bytes(),
        generation.as_bytes(),
    ]
    .concat()
}
fn pin_key(workspace: WorkspaceId, pin: PinId) -> Vec<u8> {
    [KEY_PREFIX, b"pin/", workspace.as_bytes(), pin.as_bytes()].concat()
}
fn protection_key(workspace: WorkspaceId, protection: ProtectionId) -> Vec<u8> {
    [
        KEY_PREFIX,
        b"protection/",
        workspace.as_bytes(),
        protection.as_bytes(),
    ]
    .concat()
}
fn scoped_operation_key(
    workspace: WorkspaceId,
    principal: PrincipalId,
    operation: OperationId,
) -> Vec<u8> {
    [
        KEY_PREFIX,
        b"operation/",
        workspace.as_bytes(),
        principal.as_bytes(),
        operation.as_bytes(),
    ]
    .concat()
}
