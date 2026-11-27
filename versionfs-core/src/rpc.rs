//! Backend-neutral typed, authorized, bounded streaming RPC protocol for
//! Version FS service/SDK communication (RFC-002 Section 12).
//!
//! All requests carry caller authentication (`RpcAuth`), enforce explicit payload
//! and streaming bounds (`MAX_RPC_FRAME_BYTES`, `MAX_STREAM_READ_BYTES`), and
//! never expose arbitrary digest lookup.

use crate::control::{
    AttachmentState, BranchState, CaptureState, CommittedRootTransition, ControlError,
    GenerationSeed, LiveRoots, NativeRootTransitionMeta, OperationState, PreparationKind,
    PreparationRequest, PublishedGeneration, WriterGrant,
};
use crate::format::{
    ArtifactDescriptor, CanonicalDecoder, CanonicalEncoder, FormatError,
    GenerationDescriptorFormat, LogMapEntry, MAX_ENCODED_OBJECT_BYTES, MAX_LOG_RECORDS,
    MAX_PROVENANCE_CONTEXT_BYTES, ObjectRef, ProvenanceBinding, ProvenanceContext,
    ProvenanceCoverage, ProvenanceStatus, VolumeGeometry,
};
use crate::gc::{GcCandidatePage, GcReportBudget, GcReportSummary};
use crate::graph::{ProvenanceInspectError, ProvenanceInspection};
use crate::identity::{
    AttachmentId, BranchId, GenerationId, LogId, OperationId, PinId, PrincipalId, ProtectionId,
    VolumeBoundaryId, WorkspaceId,
};
use crate::metadata::MetadataError;
use crate::time::MonotonicTime;
use crate::views::{ArtifactChange, ExtentRange, LogViewChange};
use std::io::{Read, Write};
use thiserror::Error;

/// Maximum wire bytes in one framed RPC request or streaming response frame.
pub const MAX_RPC_FRAME_BYTES: usize = MAX_ENCODED_OBJECT_BYTES + 64 * 1024;
/// Maximum logical bytes returned by a single bounded `read_blocks_at` or `read_artifact` call.
pub const MAX_STREAM_READ_BYTES: u64 = 4 * 1024 * 1024;
/// Maximum batches returned in one `read_log_at` / `tail_log` request.
pub const MAX_LOG_READ_BATCHES: usize = 256;
/// Maximum items per streamed diff chunk.
pub const MAX_DIFF_CHUNK_ITEMS: usize = 256;
/// Maximum authentication token length in bytes.
pub const MAX_AUTH_TOKEN_BYTES: usize = 1024;

/// Structured RFC-002 Section 12/13 error codes returned by the service.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u16)]
pub enum ServiceErrorCode {
    /// Optimistic revision/epoch/key conflict.
    Conflict = 1,
    /// Caller's writer grant, attachment, or owner incarnation is fenced.
    Fenced = 2,
    /// Writer, attachment, preparation, or read protection lease expired.
    LeaseExpired = 3,
    /// Operation ID was previously committed or prepared with different arguments.
    OperationMismatch = 4,
    /// Operation is currently in progress.
    InProgress = 5,
    /// Generation retention eligibility expired.
    GenerationExpired = 6,
    /// Log stream epoch or log ID changed.
    StreamReplaced = 7,
    /// Log cursor falls before retained `first_seq`.
    CursorExpired = 8,
    /// Object/descriptor/context/map integrity verification failed.
    IntegrityError = 9,
    /// Requested operation is unsupported (for example, P10 reports or writes on read-only).
    UnsupportedOperation = 10,
    /// Volume geometry or byte range alignment/bounds are invalid.
    InvalidGeometry = 11,
    /// Filesystem freeze or clean unmount failed.
    FreezeFailed = 12,
    /// Report details expired.
    ReportExpired = 13,
    /// Conflicting attachment, capture, or pending preparation is active.
    Busy = 14,
    /// Requested branch, generation, operation, protection, or object was not found.
    NotFound = 15,
    /// Caller authentication or authorization failed.
    Unauthorized = 16,
    /// Operation was explicitly aborted.
    Aborted = 17,
    /// Durable live state differs from head and `discard_live_state` was false.
    LiveStateDiscardRequired = 18,
    /// Request parameters failed validation.
    InvalidRequest = 19,
    /// Authority or object-store commit outcome is unknown and requires reconciliation.
    UnknownOutcome = 20,
    /// Request or response exceeded configured bounds.
    LimitExceeded = 21,
    /// Backend metadata or object-store failure.
    BackendError = 22,
}

impl TryFrom<u16> for ServiceErrorCode {
    type Error = FormatError;

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Conflict),
            2 => Ok(Self::Fenced),
            3 => Ok(Self::LeaseExpired),
            4 => Ok(Self::OperationMismatch),
            5 => Ok(Self::InProgress),
            6 => Ok(Self::GenerationExpired),
            7 => Ok(Self::StreamReplaced),
            8 => Ok(Self::CursorExpired),
            9 => Ok(Self::IntegrityError),
            10 => Ok(Self::UnsupportedOperation),
            11 => Ok(Self::InvalidGeometry),
            12 => Ok(Self::FreezeFailed),
            13 => Ok(Self::ReportExpired),
            14 => Ok(Self::Busy),
            15 => Ok(Self::NotFound),
            16 => Ok(Self::Unauthorized),
            17 => Ok(Self::Aborted),
            18 => Ok(Self::LiveStateDiscardRequired),
            19 => Ok(Self::InvalidRequest),
            20 => Ok(Self::UnknownOutcome),
            21 => Ok(Self::LimitExceeded),
            22 => Ok(Self::BackendError),
            _ => Err(FormatError::InvalidEnvelope("unknown service error code")),
        }
    }
}

/// Structured error returned over the service RPC boundary.
#[derive(Clone, Debug, Eq, PartialEq, Error)]
#[error("{code:?}: {message}")]
pub struct ServiceError {
    /// Machine-readable RFC-002 error code.
    pub code: ServiceErrorCode,
    /// Human-readable diagnostic message.
    pub message: String,
    /// Optional numeric metadata (for example `cursor_seq` / `first_seq` on `CursorExpired`).
    pub detail_u64_a: u64,
    /// Optional secondary numeric metadata.
    pub detail_u64_b: u64,
}

impl ServiceError {
    /// Creates a structured service error with no extra numeric details.
    pub fn new(code: ServiceErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            detail_u64_a: 0,
            detail_u64_b: 0,
        }
    }

    /// Maps this service error back to [`ControlError`] when invoked through control traits.
    pub fn to_control_error(&self) -> ControlError {
        match self.code {
            ServiceErrorCode::Conflict => ControlError::Conflict,
            ServiceErrorCode::Fenced => ControlError::Fenced,
            ServiceErrorCode::LeaseExpired => ControlError::Expired,
            ServiceErrorCode::OperationMismatch => ControlError::OperationMismatch,
            ServiceErrorCode::InProgress => ControlError::InProgress,
            ServiceErrorCode::GenerationExpired => ControlError::GenerationExpired,
            ServiceErrorCode::Busy => ControlError::Busy,
            ServiceErrorCode::NotFound => ControlError::NotFound,
            ServiceErrorCode::Aborted => ControlError::Aborted,
            ServiceErrorCode::LiveStateDiscardRequired => ControlError::LiveStateDiscardRequired,
            ServiceErrorCode::IntegrityError => ControlError::CorruptRecord(self.message.clone()),
            ServiceErrorCode::UnknownOutcome => {
                ControlError::Metadata(MetadataError::UnknownOutcome {
                    operation_id: Vec::new(),
                    cause: self.message.clone(),
                })
            }
            ServiceErrorCode::Unauthorized => ControlError::InvalidRequest("unauthorized"),
            ServiceErrorCode::InvalidRequest
            | ServiceErrorCode::InvalidGeometry
            | ServiceErrorCode::UnsupportedOperation
            | ServiceErrorCode::LimitExceeded => {
                ControlError::InvalidRequest("invalid service request")
            }
            ServiceErrorCode::StreamReplaced
            | ServiceErrorCode::CursorExpired
            | ServiceErrorCode::FreezeFailed
            | ServiceErrorCode::ReportExpired
            | ServiceErrorCode::BackendError => {
                ControlError::Metadata(MetadataError::Backend(self.message.clone()))
            }
        }
    }
}

impl From<ControlError> for ServiceError {
    fn from(error: ControlError) -> Self {
        let code = match &error {
            ControlError::Conflict => ServiceErrorCode::Conflict,
            ControlError::Fenced => ServiceErrorCode::Fenced,
            ControlError::Expired => ServiceErrorCode::LeaseExpired,
            ControlError::OperationMismatch => ServiceErrorCode::OperationMismatch,
            ControlError::InProgress => ServiceErrorCode::InProgress,
            ControlError::GenerationExpired => ServiceErrorCode::GenerationExpired,
            ControlError::Busy => ServiceErrorCode::Busy,
            ControlError::NotFound => ServiceErrorCode::NotFound,
            ControlError::Aborted => ServiceErrorCode::Aborted,
            ControlError::LiveStateDiscardRequired => ServiceErrorCode::LiveStateDiscardRequired,
            ControlError::InvalidTransition | ControlError::InvalidRequest(_) => {
                ServiceErrorCode::InvalidRequest
            }
            ControlError::Format(FormatError::InvalidGeometry(_)) => {
                ServiceErrorCode::InvalidGeometry
            }
            ControlError::Format(_) | ControlError::CorruptRecord(_) => {
                ServiceErrorCode::IntegrityError
            }
            ControlError::Metadata(MetadataError::UnknownOutcome { .. }) => {
                ServiceErrorCode::UnknownOutcome
            }
            ControlError::Metadata(MetadataError::Conflict) => ServiceErrorCode::Conflict,
            ControlError::Metadata(_) => ServiceErrorCode::BackendError,
            ControlError::ReportExpired => ServiceErrorCode::ReportExpired,
        };
        Self::new(code, error.to_string())
    }
}

impl From<ProvenanceInspectError> for ServiceError {
    fn from(error: ProvenanceInspectError) -> Self {
        match error {
            ProvenanceInspectError::Unauthorized => {
                Self::new(ServiceErrorCode::Unauthorized, error.to_string())
            }
            ProvenanceInspectError::Integrity(_) => {
                Self::new(ServiceErrorCode::IntegrityError, error.to_string())
            }
            ProvenanceInspectError::Store(_) => {
                Self::new(ServiceErrorCode::BackendError, error.to_string())
            }
        }
    }
}

impl From<FormatError> for ServiceError {
    fn from(error: FormatError) -> Self {
        match error {
            FormatError::InvalidGeometry(_) => {
                Self::new(ServiceErrorCode::InvalidGeometry, error.to_string())
            }
            FormatError::PayloadTooLarge { .. } => {
                Self::new(ServiceErrorCode::LimitExceeded, error.to_string())
            }
            _ => Self::new(ServiceErrorCode::IntegrityError, error.to_string()),
        }
    }
}

/// Caller credentials attached to every RPC request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RpcAuth {
    /// Target workspace scope.
    pub workspace_id: WorkspaceId,
    /// Authenticated caller principal.
    pub principal_id: PrincipalId,
    /// Shared service authentication token.
    pub auth_token: Vec<u8>,
}

/// Wire representation of one RPC request envelope.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RpcEnvelope {
    /// Authenticated caller scope.
    pub auth: RpcAuth,
    /// Typed operation payload.
    pub request: RpcRequest,
}

/// Typed requests corresponding to RFC-002 Section 12.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RpcRequest {
    /// Returns the workspace's configured `ObjectStoreScopeId`.
    GetScope,
    /// Returns the authority's current monotonic clock reading.
    Now,
    /// Creates an empty workspace, genesis Generation, writer grant, and attachment.
    CreateEmptyWorkspace {
        /// Idempotent workspace creation operation ID.
        create_op_id: OperationId,
        /// Genesis Generation ID.
        generation_id: GenerationId,
        /// Initial empty boundary ID.
        boundary_id: VolumeBoundaryId,
        /// Fixed volume geometry.
        geometry: VolumeGeometry,
        /// Informational creation timestamp.
        created_at_unix_ms: u64,
        /// Initial main branch ID.
        branch_id: BranchId,
        /// Initial main branch name.
        name: String,
        /// Default retention period in milliseconds.
        retention_period_ms: u64,
        /// Idempotent acquire-writer operation ID.
        acquire_op_id: OperationId,
        /// Idempotent attach-volume operation ID.
        attach_op_id: OperationId,
        /// Initial attachment ID.
        attachment_id: AttachmentId,
        /// Initial attachment-base protection ID.
        attachment_protection_id: ProtectionId,
    },
    /// Creates a branch from a retained Generation.
    CreateBranch {
        /// Idempotent operation ID.
        operation_id: OperationId,
        /// Stable new branch ID.
        branch_id: BranchId,
        /// Display name.
        name: String,
        /// Retained base Generation.
        base: GenerationId,
    },
    /// Reads branch state by stable ID.
    GetBranch {
        /// Branch ID.
        branch_id: BranchId,
    },
    /// Resolves a branch by display name.
    BranchByName {
        /// Branch name.
        name: String,
    },
    /// Reads an authoritative Generation seed.
    GetGeneration {
        /// Generation ID.
        generation_id: GenerationId,
    },
    /// Acquires a writer lease.
    AcquireWriter {
        /// Operation ID.
        operation_id: OperationId,
        /// Branch ID.
        branch_id: BranchId,
        /// Expected branch revision.
        expected_revision: u64,
        /// Expected writer epoch.
        expected_writer_epoch: u64,
    },
    /// Forces writer handover.
    ForceAcquireWriter {
        /// Operation ID.
        operation_id: OperationId,
        /// Branch ID.
        branch_id: BranchId,
        /// Expected branch revision.
        expected_revision: u64,
        /// Expected writer epoch.
        expected_writer_epoch: u64,
    },
    /// Renews a writer lease.
    RenewWriter {
        /// Renewal operation ID.
        operation_id: OperationId,
        /// Current writer grant.
        grant: WriterGrant,
    },
    /// Releases a writer lease.
    ReleaseWriter {
        /// Operation ID.
        operation_id: OperationId,
        /// Current writer grant.
        grant: WriterGrant,
    },
    /// Attaches a writable volume.
    AttachVolume {
        /// Operation ID.
        operation_id: OperationId,
        /// Branch ID.
        branch_id: BranchId,
        /// Expected branch revision.
        expected_revision: u64,
        /// Active writer grant.
        grant: WriterGrant,
        /// Attachment ID.
        attachment_id: AttachmentId,
        /// Attachment-base protection ID.
        protection_id: ProtectionId,
    },
    /// Renews an active attachment-base protection.
    RenewAttachmentBase {
        /// Operation ID.
        operation_id: OperationId,
        /// Branch ID.
        branch_id: BranchId,
        /// Attachment ID.
        attachment_id: AttachmentId,
        /// Volume epoch.
        volume_epoch: u64,
        /// Protection ID.
        protection_id: ProtectionId,
        /// Writer grant.
        grant: WriterGrant,
    },
    /// Detaches a clean, drained attachment.
    DetachVolume {
        /// Operation ID.
        operation_id: OperationId,
        /// Branch ID.
        branch_id: BranchId,
        /// Attachment ID.
        attachment_id: AttachmentId,
        /// Volume epoch.
        volume_epoch: u64,
        /// Protection ID.
        protection_id: ProtectionId,
        /// Writer grant.
        grant: WriterGrant,
    },
    /// Checks writer grant & attachment liveness.
    CheckAttachmentLive {
        /// Branch ID.
        branch_id: BranchId,
        /// Attachment ID.
        attachment_id: AttachmentId,
        /// Volume epoch.
        volume_epoch: u64,
        /// Writer grant.
        grant: WriterGrant,
    },
    /// Moves branch head to a retained Generation.
    MoveHead {
        /// Operation ID.
        operation_id: OperationId,
        /// Branch ID.
        branch_id: BranchId,
        /// Expected revision.
        expected_revision: u64,
        /// Target Generation ID.
        target_generation: GenerationId,
        /// Writer grant.
        grant: WriterGrant,
        /// Explicit live-state discard flag.
        discard_live_state: bool,
    },
    /// Promotes a source branch head onto a destination branch.
    Promote {
        /// Operation ID.
        operation_id: OperationId,
        /// Source branch ID.
        source_branch_id: BranchId,
        /// Expected source revision.
        expected_source_revision: u64,
        /// Destination branch ID.
        destination_branch_id: BranchId,
        /// Expected destination revision.
        expected_destination_revision: u64,
        /// Destination writer grant.
        destination_grant: WriterGrant,
        /// Explicit live-state discard flag.
        discard_live_state: bool,
    },
    /// Deletes a branch.
    DeleteBranch {
        /// Operation ID.
        operation_id: OperationId,
        /// Branch ID.
        branch_id: BranchId,
        /// Expected branch revision.
        expected_revision: u64,
        /// Writer grant.
        grant: WriterGrant,
        /// Explicit live-state discard flag.
        discard_live_state: bool,
    },
    /// Begins or replays a mutation or capture preparation.
    BeginPreparation {
        /// Preparation request.
        request: PreparationRequest,
        /// Explicit provenance binding (used for capture preparations).
        provenance: ProvenanceBinding,
    },
    /// Renews a preparation lease.
    RenewPreparation {
        /// Renewal operation ID.
        renewal_id: OperationId,
        /// Prepared operation ID.
        prepared_id: OperationId,
        /// Writer grant.
        grant: WriterGrant,
    },
    /// Authorizes one drain child under an open capture token.
    AuthorizeCaptureChild {
        /// Authorization operation ID.
        authorization_id: OperationId,
        /// Capture operation ID.
        capture_id: OperationId,
        /// Child operation ID.
        child_id: OperationId,
        /// Writer grant.
        grant: WriterGrant,
    },
    /// Atomically commits live roots for a prepared mutation.
    CommitLiveRoots {
        /// Operation ID.
        operation_id: OperationId,
        /// Expected branch revision.
        expected_revision: u64,
        /// Writer grant.
        grant: WriterGrant,
        /// Candidate live roots.
        roots: LiveRoots,
        /// Optional `(capture_id, child_id)` pair.
        capture_child: Option<(OperationId, OperationId)>,
        /// Optional native-view replay metadata.
        native_meta: Option<NativeRootTransitionMeta>,
    },
    /// Seals an open capture token.
    SealCapture {
        /// Seal operation ID.
        operation_id: OperationId,
        /// Capture operation ID.
        capture_id: OperationId,
        /// Expected final revision.
        expected_revision: u64,
        /// Writer grant.
        grant: WriterGrant,
    },
    /// Publishes a Generation from a sealed capture and uploaded descriptor.
    PublishCapture {
        /// Capture operation ID.
        capture_id: OperationId,
        /// Expected branch revision.
        expected_revision: u64,
        /// Expected parent Generation ID.
        expected_parent: GenerationId,
        /// Writer grant.
        grant: WriterGrant,
        /// Attachment ID.
        attachment_id: AttachmentId,
        /// Volume epoch.
        volume_epoch: u64,
        /// Uploaded descriptor reference (verified server-side before publication).
        descriptor_ref: ObjectRef,
        /// Whether to advance branch head.
        advance_head: bool,
    },
    /// Uploads one verified object for an active `Preparing` operation.
    UploadPreparedObject {
        /// Active preparing operation ID.
        operation_id: OperationId,
        /// Expected typed object reference.
        reference: ObjectRef,
        /// Complete canonical encoded object bytes.
        encoded_bytes: Vec<u8>,
    },
    /// Validates and uploads a bounded `ProvenanceContext` under a live writer grant.
    UploadProvenanceContext {
        /// Branch ID.
        branch_id: BranchId,
        /// Active writer grant.
        grant: WriterGrant,
        /// Canonical encoded `ProvenanceContext` bytes (`<= 64 KiB`).
        encoded_context: Vec<u8>,
    },
    /// Queries operation outcome state.
    GetOperation {
        /// Operation ID.
        operation_id: OperationId,
    },
    /// Queries committed root-transition metadata for native replay verification.
    GetCommittedRootTransition {
        /// Operation ID.
        operation_id: OperationId,
    },
    /// Aborts a preparation or tombstones an unseen ID.
    AbortOperation {
        /// Operation ID.
        operation_id: OperationId,
    },
    /// Expires an expired or fenced preparation.
    ExpirePreparation {
        /// Operation ID.
        operation_id: OperationId,
    },
    /// Pins a retained Generation.
    PinGeneration {
        /// Operation ID.
        operation_id: OperationId,
        /// Generation ID.
        generation_id: GenerationId,
        /// Pin ID.
        pin_id: PinId,
    },
    /// Unpins a Generation pin.
    UnpinGeneration {
        /// Operation ID.
        operation_id: OperationId,
        /// Pin ID.
        pin_id: PinId,
    },
    /// Opens a bounded Generation read protection.
    OpenGeneration {
        /// Operation ID.
        operation_id: OperationId,
        /// Generation ID.
        generation_id: GenerationId,
        /// Protection ID.
        protection_id: ProtectionId,
    },
    /// Opens a bounded live-log read protection.
    OpenLiveLog {
        /// Operation ID.
        operation_id: OperationId,
        /// Branch ID.
        branch_id: BranchId,
        /// Expected revision.
        expected_revision: u64,
        /// Expected `LogMapRoot`.
        expected_root: ObjectRef,
        /// Protection ID.
        protection_id: ProtectionId,
    },
    /// Renews a read protection handle.
    RenewProtection {
        /// Renewal operation ID.
        operation_id: OperationId,
        /// Protection ID.
        protection_id: ProtectionId,
    },
    /// Closes a read protection handle.
    CloseProtection {
        /// Operation ID.
        operation_id: OperationId,
        /// Protection ID.
        protection_id: ProtectionId,
    },
    /// Fetches one object proven reachable under an active protection handle.
    FetchProtectedObject {
        /// Active protection handle.
        protection_id: ProtectionId,
        /// Expected object reference.
        reference: ObjectRef,
    },
    /// Streams verified block bytes from a protected Generation (`read_blocks_at`).
    ReadBlocksAt {
        /// Active Generation read protection handle.
        protection_id: ProtectionId,
        /// Sector-aligned byte offset.
        offset: u64,
        /// Sector-aligned byte length (`<= MAX_STREAM_READ_BYTES`).
        length: u64,
    },
    /// Inspects a protected Generation's provenance binding and context.
    ReadProvenance {
        /// Active Generation read protection handle.
        protection_id: ProtectionId,
        /// Target Generation ID.
        generation_id: GenerationId,
    },
    /// Streams structural extent, log, and artifact diffs between two protected Generations.
    DiffGenerations {
        /// Left Generation protection handle.
        left_protection_id: ProtectionId,
        /// Left Generation ID.
        left_generation_id: GenerationId,
        /// Right Generation protection handle.
        right_protection_id: ProtectionId,
        /// Right Generation ID.
        right_generation_id: GenerationId,
    },
    /// Streams verified log batches from a protected Generation (`read_log_at`).
    ReadLogAt {
        /// Active Generation read protection handle.
        protection_id: ProtectionId,
        /// Log name.
        name: String,
        /// Starting sequence number.
        from_seq: u64,
        /// Maximum batches (`<= MAX_LOG_READ_BATCHES`).
        max_batches: usize,
    },
    /// Lists all named logs under an active Generation or LiveLog protection.
    ListLogs {
        /// Active protection handle.
        protection_id: ProtectionId,
    },
    /// Streams a named artifact payload under an active Generation protection.
    ReadArtifact {
        /// Active Generation read protection handle.
        protection_id: ProtectionId,
        /// Artifact name.
        name: String,
        /// Byte offset.
        offset: u64,
        /// Maximum bytes to return (`<= MAX_STREAM_READ_BYTES`).
        length: u64,
    },
    /// Lists all named artifacts under an active Generation protection.
    ListArtifacts {
        /// Active Generation read protection handle.
        protection_id: ProtectionId,
    },
    /// Collect a bounded mark-only retention/GC report (P10).
    CollectReport {
        /// Stable operation identity for idempotent report collection.
        operation_id: OperationId,
        /// Optional budget/TTL overrides (`None` uses [`GcReportBudget::default`]).
        budget: Option<GcReportBudget>,
    },
    /// Look up a persisted mark-only GC report summary and optional candidate page (P10).
    GetGcReport {
        /// Stable report operation identity.
        report_id: OperationId,
        /// Optional zero-based candidate page index to fetch alongside the summary.
        page_index: Option<u32>,
    },
}

/// One streamed chunk returned by [`RpcRequest::DiffGenerations`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DiffStreamChunk {
    /// Changed logical volume extent ranges.
    Extents(Vec<ExtentRange>),
    /// Changed native log entries.
    Logs(Vec<LogViewChange>),
    /// Changed native artifact descriptors.
    Artifacts(Vec<ArtifactChange>),
}

/// One typed response frame in a bounded RPC stream.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RpcResponseFrame {
    /// Opaque encoded payload chunk (single-item or streamed slice).
    Data(Vec<u8>),
    /// Normal end of response stream.
    End,
    /// Terminal structured service error.
    Error(ServiceError),
}

/// Writes one length-prefixed frame bounded by [`MAX_RPC_FRAME_BYTES`].
pub fn write_frame<W: Write>(writer: &mut W, payload: &[u8]) -> Result<(), ServiceError> {
    if payload.len() > MAX_RPC_FRAME_BYTES {
        return Err(ServiceError::new(
            ServiceErrorCode::LimitExceeded,
            format!(
                "RPC frame length {} exceeds maximum {}",
                payload.len(),
                MAX_RPC_FRAME_BYTES
            ),
        ));
    }
    let len = u32::try_from(payload.len())
        .map_err(|_| ServiceError::new(ServiceErrorCode::LimitExceeded, "RPC frame too large"))?;
    writer
        .write_all(&len.to_le_bytes())
        .map_err(|e| ServiceError::new(ServiceErrorCode::BackendError, e.to_string()))?;
    writer
        .write_all(payload)
        .map_err(|e| ServiceError::new(ServiceErrorCode::BackendError, e.to_string()))?;
    writer
        .flush()
        .map_err(|e| ServiceError::new(ServiceErrorCode::BackendError, e.to_string()))?;
    Ok(())
}

/// Reads one length-prefixed frame bounded by [`MAX_RPC_FRAME_BYTES`].
pub fn read_frame<R: Read>(reader: &mut R) -> Result<Vec<u8>, ServiceError> {
    let mut len_buf = [0_u8; 4];
    reader
        .read_exact(&mut len_buf)
        .map_err(|e| ServiceError::new(ServiceErrorCode::BackendError, e.to_string()))?;
    let len = usize::try_from(u32::from_le_bytes(len_buf))
        .map_err(|_| ServiceError::new(ServiceErrorCode::LimitExceeded, "frame length overflow"))?;
    if len > MAX_RPC_FRAME_BYTES {
        return Err(ServiceError::new(
            ServiceErrorCode::LimitExceeded,
            format!("RPC frame length {len} exceeds maximum {MAX_RPC_FRAME_BYTES}"),
        ));
    }
    let mut payload = vec![0_u8; len];
    reader
        .read_exact(&mut payload)
        .map_err(|e| ServiceError::new(ServiceErrorCode::BackendError, e.to_string()))?;
    Ok(payload)
}

impl RpcResponseFrame {
    /// Encodes a response frame for wire transport.
    pub fn encode(&self) -> Result<Vec<u8>, ServiceError> {
        let mut e = CanonicalEncoder::new();
        match self {
            Self::Data(bytes) => {
                e.u8(1);
                e.bytes(bytes)?;
            }
            Self::End => e.u8(2),
            Self::Error(err) => {
                e.u8(3);
                e.u16_le(err.code as u16);
                e.bytes(err.message.as_bytes())?;
                e.u64_le(err.detail_u64_a);
                e.u64_le(err.detail_u64_b);
            }
        }
        Ok(e.finish())
    }

    /// Decodes a response frame from wire bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, ServiceError> {
        let mut d = CanonicalDecoder::new(bytes);
        let tag = d.u8()?;
        let frame = match tag {
            1 => Self::Data(d.bytes(MAX_RPC_FRAME_BYTES)?.to_vec()),
            2 => Self::End,
            3 => {
                let code = ServiceErrorCode::try_from(d.u16_le()?)?;
                let msg_bytes = d.bytes(16 * 1024)?;
                let message = std::str::from_utf8(msg_bytes)
                    .map_err(|_| FormatError::InvalidEnvelope("error message must be UTF-8"))?
                    .to_owned();
                let detail_u64_a = d.u64_le()?;
                let detail_u64_b = d.u64_le()?;
                Self::Error(ServiceError {
                    code,
                    message,
                    detail_u64_a,
                    detail_u64_b,
                })
            }
            _ => return Err(FormatError::InvalidEnvelope("unknown RPC response frame tag").into()),
        };
        d.finish()?;
        Ok(frame)
    }
}

// ---------------------------------------------------------------------------
// Canonical encoding / decoding helpers for RpcEnvelope and response payloads
// ---------------------------------------------------------------------------

impl RpcEnvelope {
    /// Encodes the authenticated RPC request envelope.
    pub fn encode(&self) -> Result<Vec<u8>, ServiceError> {
        let mut e = CanonicalEncoder::new();
        e.fixed(self.auth.workspace_id.as_bytes());
        e.fixed(self.auth.principal_id.as_bytes());
        if self.auth.auth_token.len() > MAX_AUTH_TOKEN_BYTES {
            return Err(ServiceError::new(
                ServiceErrorCode::LimitExceeded,
                "auth token exceeds limit",
            ));
        }
        e.bytes(&self.auth.auth_token)?;
        encode_request(&mut e, &self.request)?;
        Ok(e.finish())
    }

    /// Decodes an authenticated RPC request envelope.
    pub fn decode(bytes: &[u8]) -> Result<Self, ServiceError> {
        let mut d = CanonicalDecoder::new(bytes);
        let workspace_id = WorkspaceId::from_bytes(d.fixed()?);
        let principal_id = PrincipalId::from_bytes(d.fixed()?);
        let auth_token = d.bytes(MAX_AUTH_TOKEN_BYTES)?.to_vec();
        let request = decode_request(&mut d)?;
        d.finish()?;
        Ok(Self {
            auth: RpcAuth {
                workspace_id,
                principal_id,
                auth_token,
            },
            request,
        })
    }
}

fn encode_str(e: &mut CanonicalEncoder, s: &str) -> Result<(), ServiceError> {
    e.bytes(s.as_bytes()).map_err(ServiceError::from)
}

fn decode_str(d: &mut CanonicalDecoder<'_>, max: usize) -> Result<String, ServiceError> {
    let raw = d.bytes(max)?;
    Ok(std::str::from_utf8(raw)
        .map_err(|_| FormatError::InvalidEnvelope("string field must be UTF-8"))?
        .to_owned())
}

fn encode_grant(e: &mut CanonicalEncoder, g: WriterGrant) {
    e.fixed(g.branch_id.as_bytes());
    e.u64_le(g.writer_epoch);
    e.u64_le(g.owner_incarnation.get());
    e.fixed(g.grant_id.as_bytes());
    e.u64_le(g.expires_at.as_nanos());
}

fn decode_grant(d: &mut CanonicalDecoder<'_>) -> Result<WriterGrant, ServiceError> {
    Ok(WriterGrant {
        branch_id: BranchId::from_bytes(d.fixed()?),
        writer_epoch: d.u64_le()?,
        owner_incarnation: crate::control::OwnerIncarnation::from_u64(d.u64_le()?),
        grant_id: OperationId::from_bytes(d.fixed()?),
        expires_at: MonotonicTime::from_nanos(d.u64_le()?),
    })
}

fn encode_roots(e: &mut CanonicalEncoder, r: LiveRoots) {
    e.object_ref(r.volume);
    e.object_ref(r.logs);
    e.object_ref(r.artifacts);
}

fn decode_roots(d: &mut CanonicalDecoder<'_>) -> Result<LiveRoots, ServiceError> {
    Ok(LiveRoots {
        volume: d.object_ref()?,
        logs: d.object_ref()?,
        artifacts: d.object_ref()?,
    })
}

fn encode_provenance_binding(e: &mut CanonicalEncoder, p: ProvenanceBinding) {
    match p {
        ProvenanceBinding::Unattributed => e.u8(0),
        ProvenanceBinding::Recorded { context_root } => {
            e.u8(1);
            e.object_ref(context_root);
        }
    }
}

fn decode_provenance_binding(
    d: &mut CanonicalDecoder<'_>,
) -> Result<ProvenanceBinding, ServiceError> {
    match d.u8()? {
        0 => Ok(ProvenanceBinding::Unattributed),
        1 => Ok(ProvenanceBinding::Recorded {
            context_root: d.object_ref()?,
        }),
        _ => Err(FormatError::InvalidProvenance("invalid provenance binding tag").into()),
    }
}

fn encode_request(e: &mut CanonicalEncoder, req: &RpcRequest) -> Result<(), ServiceError> {
    match req {
        RpcRequest::GetScope => e.u16_le(1),
        RpcRequest::Now => e.u16_le(2),
        RpcRequest::CreateEmptyWorkspace {
            create_op_id,
            generation_id,
            boundary_id,
            geometry,
            created_at_unix_ms,
            branch_id,
            name,
            retention_period_ms,
            acquire_op_id,
            attach_op_id,
            attachment_id,
            attachment_protection_id,
        } => {
            e.u16_le(3);
            e.fixed(create_op_id.as_bytes());
            e.fixed(generation_id.as_bytes());
            e.fixed(boundary_id.as_bytes());
            e.u64_le(geometry.capacity_bytes);
            e.u32_le(geometry.logical_sector_bytes);
            e.u32_le(geometry.extent_bytes);
            e.u64_le(*created_at_unix_ms);
            e.fixed(branch_id.as_bytes());
            encode_str(e, name)?;
            e.u64_le(*retention_period_ms);
            e.fixed(acquire_op_id.as_bytes());
            e.fixed(attach_op_id.as_bytes());
            e.fixed(attachment_id.as_bytes());
            e.fixed(attachment_protection_id.as_bytes());
        }
        RpcRequest::CreateBranch {
            operation_id,
            branch_id,
            name,
            base,
        } => {
            e.u16_le(4);
            e.fixed(operation_id.as_bytes());
            e.fixed(branch_id.as_bytes());
            encode_str(e, name)?;
            e.fixed(base.as_bytes());
        }
        RpcRequest::GetBranch { branch_id } => {
            e.u16_le(5);
            e.fixed(branch_id.as_bytes());
        }
        RpcRequest::BranchByName { name } => {
            e.u16_le(6);
            encode_str(e, name)?;
        }
        RpcRequest::GetGeneration { generation_id } => {
            e.u16_le(7);
            e.fixed(generation_id.as_bytes());
        }
        RpcRequest::AcquireWriter {
            operation_id,
            branch_id,
            expected_revision,
            expected_writer_epoch,
        } => {
            e.u16_le(8);
            e.fixed(operation_id.as_bytes());
            e.fixed(branch_id.as_bytes());
            e.u64_le(*expected_revision);
            e.u64_le(*expected_writer_epoch);
        }
        RpcRequest::ForceAcquireWriter {
            operation_id,
            branch_id,
            expected_revision,
            expected_writer_epoch,
        } => {
            e.u16_le(9);
            e.fixed(operation_id.as_bytes());
            e.fixed(branch_id.as_bytes());
            e.u64_le(*expected_revision);
            e.u64_le(*expected_writer_epoch);
        }
        RpcRequest::RenewWriter {
            operation_id,
            grant,
        } => {
            e.u16_le(10);
            e.fixed(operation_id.as_bytes());
            encode_grant(e, *grant);
        }
        RpcRequest::ReleaseWriter {
            operation_id,
            grant,
        } => {
            e.u16_le(11);
            e.fixed(operation_id.as_bytes());
            encode_grant(e, *grant);
        }
        RpcRequest::AttachVolume {
            operation_id,
            branch_id,
            expected_revision,
            grant,
            attachment_id,
            protection_id,
        } => {
            e.u16_le(12);
            e.fixed(operation_id.as_bytes());
            e.fixed(branch_id.as_bytes());
            e.u64_le(*expected_revision);
            encode_grant(e, *grant);
            e.fixed(attachment_id.as_bytes());
            e.fixed(protection_id.as_bytes());
        }
        RpcRequest::RenewAttachmentBase {
            operation_id,
            branch_id,
            attachment_id,
            volume_epoch,
            protection_id,
            grant,
        } => {
            e.u16_le(13);
            e.fixed(operation_id.as_bytes());
            e.fixed(branch_id.as_bytes());
            e.fixed(attachment_id.as_bytes());
            e.u64_le(*volume_epoch);
            e.fixed(protection_id.as_bytes());
            encode_grant(e, *grant);
        }
        RpcRequest::DetachVolume {
            operation_id,
            branch_id,
            attachment_id,
            volume_epoch,
            protection_id,
            grant,
        } => {
            e.u16_le(14);
            e.fixed(operation_id.as_bytes());
            e.fixed(branch_id.as_bytes());
            e.fixed(attachment_id.as_bytes());
            e.u64_le(*volume_epoch);
            e.fixed(protection_id.as_bytes());
            encode_grant(e, *grant);
        }
        RpcRequest::CheckAttachmentLive {
            branch_id,
            attachment_id,
            volume_epoch,
            grant,
        } => {
            e.u16_le(15);
            e.fixed(branch_id.as_bytes());
            e.fixed(attachment_id.as_bytes());
            e.u64_le(*volume_epoch);
            encode_grant(e, *grant);
        }
        RpcRequest::MoveHead {
            operation_id,
            branch_id,
            expected_revision,
            target_generation,
            grant,
            discard_live_state,
        } => {
            e.u16_le(16);
            e.fixed(operation_id.as_bytes());
            e.fixed(branch_id.as_bytes());
            e.u64_le(*expected_revision);
            e.fixed(target_generation.as_bytes());
            encode_grant(e, *grant);
            e.u8(u8::from(*discard_live_state));
        }
        RpcRequest::Promote {
            operation_id,
            source_branch_id,
            expected_source_revision,
            destination_branch_id,
            expected_destination_revision,
            destination_grant,
            discard_live_state,
        } => {
            e.u16_le(17);
            e.fixed(operation_id.as_bytes());
            e.fixed(source_branch_id.as_bytes());
            e.u64_le(*expected_source_revision);
            e.fixed(destination_branch_id.as_bytes());
            e.u64_le(*expected_destination_revision);
            encode_grant(e, *destination_grant);
            e.u8(u8::from(*discard_live_state));
        }
        RpcRequest::DeleteBranch {
            operation_id,
            branch_id,
            expected_revision,
            grant,
            discard_live_state,
        } => {
            e.u16_le(18);
            e.fixed(operation_id.as_bytes());
            e.fixed(branch_id.as_bytes());
            e.u64_le(*expected_revision);
            encode_grant(e, *grant);
            e.u8(u8::from(*discard_live_state));
        }
        RpcRequest::BeginPreparation {
            request,
            provenance,
        } => {
            e.u16_le(19);
            e.fixed(request.operation_id.as_bytes());
            e.fixed(request.branch_id.as_bytes());
            e.u64_le(request.expected_revision);
            encode_grant(e, request.grant);
            e.fixed(request.attachment_id.as_bytes());
            e.u64_le(request.volume_epoch);
            e.fixed(request.protection_id.as_bytes());
            e.u8(request.kind as u8);
            match request.expected_roots {
                Some(roots) => {
                    e.u8(1);
                    encode_roots(e, roots);
                }
                None => e.u8(0),
            }
            e.u32_le(request.dependencies.len() as u32);
            for dep in &request.dependencies {
                e.object_ref(*dep);
            }
            encode_provenance_binding(e, *provenance);
        }
        RpcRequest::RenewPreparation {
            renewal_id,
            prepared_id,
            grant,
        } => {
            e.u16_le(20);
            e.fixed(renewal_id.as_bytes());
            e.fixed(prepared_id.as_bytes());
            encode_grant(e, *grant);
        }
        RpcRequest::AuthorizeCaptureChild {
            authorization_id,
            capture_id,
            child_id,
            grant,
        } => {
            e.u16_le(21);
            e.fixed(authorization_id.as_bytes());
            e.fixed(capture_id.as_bytes());
            e.fixed(child_id.as_bytes());
            encode_grant(e, *grant);
        }
        RpcRequest::CommitLiveRoots {
            operation_id,
            expected_revision,
            grant,
            roots,
            capture_child,
            native_meta,
        } => {
            e.u16_le(22);
            e.fixed(operation_id.as_bytes());
            e.u64_le(*expected_revision);
            encode_grant(e, *grant);
            encode_roots(e, *roots);
            match capture_child {
                Some((cap, child)) => {
                    e.u8(1);
                    e.fixed(cap.as_bytes());
                    e.fixed(child.as_bytes());
                }
                None => e.u8(0),
            }
            match native_meta {
                Some(meta) => {
                    e.u8(1);
                    e.fixed(&meta.request_fingerprint);
                    e.bytes(&meta.receipt_bytes)?;
                    match meta.claim_log_id {
                        Some(log_id) => {
                            e.u8(1);
                            e.fixed(log_id.as_bytes());
                        }
                        None => e.u8(0),
                    }
                }
                None => e.u8(0),
            }
        }
        RpcRequest::SealCapture {
            operation_id,
            capture_id,
            expected_revision,
            grant,
        } => {
            e.u16_le(23);
            e.fixed(operation_id.as_bytes());
            e.fixed(capture_id.as_bytes());
            e.u64_le(*expected_revision);
            encode_grant(e, *grant);
        }
        RpcRequest::PublishCapture {
            capture_id,
            expected_revision,
            expected_parent,
            grant,
            attachment_id,
            volume_epoch,
            descriptor_ref,
            advance_head,
        } => {
            e.u16_le(24);
            e.fixed(capture_id.as_bytes());
            e.u64_le(*expected_revision);
            e.fixed(expected_parent.as_bytes());
            encode_grant(e, *grant);
            e.fixed(attachment_id.as_bytes());
            e.u64_le(*volume_epoch);
            e.object_ref(*descriptor_ref);
            e.u8(u8::from(*advance_head));
        }
        RpcRequest::UploadPreparedObject {
            operation_id,
            reference,
            encoded_bytes,
        } => {
            e.u16_le(25);
            e.fixed(operation_id.as_bytes());
            e.object_ref(*reference);
            e.bytes(encoded_bytes)?;
        }
        RpcRequest::UploadProvenanceContext {
            branch_id,
            grant,
            encoded_context,
        } => {
            e.u16_le(26);
            e.fixed(branch_id.as_bytes());
            encode_grant(e, *grant);
            e.bytes(encoded_context)?;
        }
        RpcRequest::GetOperation { operation_id } => {
            e.u16_le(27);
            e.fixed(operation_id.as_bytes());
        }
        RpcRequest::GetCommittedRootTransition { operation_id } => {
            e.u16_le(28);
            e.fixed(operation_id.as_bytes());
        }
        RpcRequest::AbortOperation { operation_id } => {
            e.u16_le(29);
            e.fixed(operation_id.as_bytes());
        }
        RpcRequest::ExpirePreparation { operation_id } => {
            e.u16_le(30);
            e.fixed(operation_id.as_bytes());
        }
        RpcRequest::PinGeneration {
            operation_id,
            generation_id,
            pin_id,
        } => {
            e.u16_le(31);
            e.fixed(operation_id.as_bytes());
            e.fixed(generation_id.as_bytes());
            e.fixed(pin_id.as_bytes());
        }
        RpcRequest::UnpinGeneration {
            operation_id,
            pin_id,
        } => {
            e.u16_le(32);
            e.fixed(operation_id.as_bytes());
            e.fixed(pin_id.as_bytes());
        }
        RpcRequest::OpenGeneration {
            operation_id,
            generation_id,
            protection_id,
        } => {
            e.u16_le(33);
            e.fixed(operation_id.as_bytes());
            e.fixed(generation_id.as_bytes());
            e.fixed(protection_id.as_bytes());
        }
        RpcRequest::OpenLiveLog {
            operation_id,
            branch_id,
            expected_revision,
            expected_root,
            protection_id,
        } => {
            e.u16_le(34);
            e.fixed(operation_id.as_bytes());
            e.fixed(branch_id.as_bytes());
            e.u64_le(*expected_revision);
            e.object_ref(*expected_root);
            e.fixed(protection_id.as_bytes());
        }
        RpcRequest::RenewProtection {
            operation_id,
            protection_id,
        } => {
            e.u16_le(35);
            e.fixed(operation_id.as_bytes());
            e.fixed(protection_id.as_bytes());
        }
        RpcRequest::CloseProtection {
            operation_id,
            protection_id,
        } => {
            e.u16_le(36);
            e.fixed(operation_id.as_bytes());
            e.fixed(protection_id.as_bytes());
        }
        RpcRequest::FetchProtectedObject {
            protection_id,
            reference,
        } => {
            e.u16_le(37);
            e.fixed(protection_id.as_bytes());
            e.object_ref(*reference);
        }
        RpcRequest::ReadBlocksAt {
            protection_id,
            offset,
            length,
        } => {
            e.u16_le(38);
            e.fixed(protection_id.as_bytes());
            e.u64_le(*offset);
            e.u64_le(*length);
        }
        RpcRequest::ReadProvenance {
            protection_id,
            generation_id,
        } => {
            e.u16_le(39);
            e.fixed(protection_id.as_bytes());
            e.fixed(generation_id.as_bytes());
        }
        RpcRequest::DiffGenerations {
            left_protection_id,
            left_generation_id,
            right_protection_id,
            right_generation_id,
        } => {
            e.u16_le(40);
            e.fixed(left_protection_id.as_bytes());
            e.fixed(left_generation_id.as_bytes());
            e.fixed(right_protection_id.as_bytes());
            e.fixed(right_generation_id.as_bytes());
        }
        RpcRequest::ReadLogAt {
            protection_id,
            name,
            from_seq,
            max_batches,
        } => {
            e.u16_le(41);
            e.fixed(protection_id.as_bytes());
            encode_str(e, name)?;
            e.u64_le(*from_seq);
            e.u32_le(
                u32::try_from(*max_batches).map_err(|_| {
                    ServiceError::new(ServiceErrorCode::LimitExceeded, "max_batches")
                })?,
            );
        }
        RpcRequest::ListLogs { protection_id } => {
            e.u16_le(42);
            e.fixed(protection_id.as_bytes());
        }
        RpcRequest::ReadArtifact {
            protection_id,
            name,
            offset,
            length,
        } => {
            e.u16_le(43);
            e.fixed(protection_id.as_bytes());
            encode_str(e, name)?;
            e.u64_le(*offset);
            e.u64_le(*length);
        }
        RpcRequest::ListArtifacts { protection_id } => {
            e.u16_le(44);
            e.fixed(protection_id.as_bytes());
        }
        RpcRequest::CollectReport {
            operation_id,
            budget,
        } => {
            e.u16_le(45);
            e.fixed(operation_id.as_bytes());
            match budget {
                Some(b) => {
                    e.u8(1);
                    e.u64_le(u64::try_from(b.max_in_memory_visited).map_err(|_| {
                        ServiceError::new(
                            ServiceErrorCode::InvalidRequest,
                            "max_in_memory_visited exceeds u64",
                        )
                    })?);
                    e.u32_le(b.max_candidate_pages);
                    e.u64_le(u64::try_from(b.max_candidates_per_page).map_err(|_| {
                        ServiceError::new(
                            ServiceErrorCode::InvalidRequest,
                            "max_candidates_per_page exceeds u64",
                        )
                    })?);
                    e.u64_le(u64::try_from(b.max_report_bytes).map_err(|_| {
                        ServiceError::new(
                            ServiceErrorCode::InvalidRequest,
                            "max_report_bytes exceeds u64",
                        )
                    })?);
                    e.u64_le(b.detail_ttl_nanos);
                    e.u64_le(u64::try_from(b.scan_page_size).map_err(|_| {
                        ServiceError::new(
                            ServiceErrorCode::InvalidRequest,
                            "scan_page_size exceeds u64",
                        )
                    })?);
                }
                None => e.u8(0),
            }
        }
        RpcRequest::GetGcReport {
            report_id,
            page_index,
        } => {
            e.u16_le(46);
            e.fixed(report_id.as_bytes());
            match page_index {
                Some(idx) => {
                    e.u8(1);
                    e.u32_le(*idx);
                }
                None => e.u8(0),
            }
        }
    }
    Ok(())
}

fn decode_request(d: &mut CanonicalDecoder<'_>) -> Result<RpcRequest, ServiceError> {
    let op = d.u16_le()?;
    match op {
        1 => Ok(RpcRequest::GetScope),
        2 => Ok(RpcRequest::Now),
        3 => {
            let create_op_id = OperationId::from_bytes(d.fixed()?);
            let generation_id = GenerationId::from_bytes(d.fixed()?);
            let boundary_id = VolumeBoundaryId::from_bytes(d.fixed()?);
            let geometry = VolumeGeometry {
                capacity_bytes: d.u64_le()?,
                logical_sector_bytes: d.u32_le()?,
                extent_bytes: d.u32_le()?,
            };
            let created_at_unix_ms = d.u64_le()?;
            let branch_id = BranchId::from_bytes(d.fixed()?);
            let name = decode_str(d, 4096)?;
            let retention_period_ms = d.u64_le()?;
            let acquire_op_id = OperationId::from_bytes(d.fixed()?);
            let attach_op_id = OperationId::from_bytes(d.fixed()?);
            let attachment_id = AttachmentId::from_bytes(d.fixed()?);
            let attachment_protection_id = ProtectionId::from_bytes(d.fixed()?);
            Ok(RpcRequest::CreateEmptyWorkspace {
                create_op_id,
                generation_id,
                boundary_id,
                geometry,
                created_at_unix_ms,
                branch_id,
                name,
                retention_period_ms,
                acquire_op_id,
                attach_op_id,
                attachment_id,
                attachment_protection_id,
            })
        }
        4 => Ok(RpcRequest::CreateBranch {
            operation_id: OperationId::from_bytes(d.fixed()?),
            branch_id: BranchId::from_bytes(d.fixed()?),
            name: decode_str(d, 4096)?,
            base: GenerationId::from_bytes(d.fixed()?),
        }),
        5 => Ok(RpcRequest::GetBranch {
            branch_id: BranchId::from_bytes(d.fixed()?),
        }),
        6 => Ok(RpcRequest::BranchByName {
            name: decode_str(d, 4096)?,
        }),
        7 => Ok(RpcRequest::GetGeneration {
            generation_id: GenerationId::from_bytes(d.fixed()?),
        }),
        8 => Ok(RpcRequest::AcquireWriter {
            operation_id: OperationId::from_bytes(d.fixed()?),
            branch_id: BranchId::from_bytes(d.fixed()?),
            expected_revision: d.u64_le()?,
            expected_writer_epoch: d.u64_le()?,
        }),
        9 => Ok(RpcRequest::ForceAcquireWriter {
            operation_id: OperationId::from_bytes(d.fixed()?),
            branch_id: BranchId::from_bytes(d.fixed()?),
            expected_revision: d.u64_le()?,
            expected_writer_epoch: d.u64_le()?,
        }),
        10 => Ok(RpcRequest::RenewWriter {
            operation_id: OperationId::from_bytes(d.fixed()?),
            grant: decode_grant(d)?,
        }),
        11 => Ok(RpcRequest::ReleaseWriter {
            operation_id: OperationId::from_bytes(d.fixed()?),
            grant: decode_grant(d)?,
        }),
        12 => Ok(RpcRequest::AttachVolume {
            operation_id: OperationId::from_bytes(d.fixed()?),
            branch_id: BranchId::from_bytes(d.fixed()?),
            expected_revision: d.u64_le()?,
            grant: decode_grant(d)?,
            attachment_id: AttachmentId::from_bytes(d.fixed()?),
            protection_id: ProtectionId::from_bytes(d.fixed()?),
        }),
        13 => Ok(RpcRequest::RenewAttachmentBase {
            operation_id: OperationId::from_bytes(d.fixed()?),
            branch_id: BranchId::from_bytes(d.fixed()?),
            attachment_id: AttachmentId::from_bytes(d.fixed()?),
            volume_epoch: d.u64_le()?,
            protection_id: ProtectionId::from_bytes(d.fixed()?),
            grant: decode_grant(d)?,
        }),
        14 => Ok(RpcRequest::DetachVolume {
            operation_id: OperationId::from_bytes(d.fixed()?),
            branch_id: BranchId::from_bytes(d.fixed()?),
            attachment_id: AttachmentId::from_bytes(d.fixed()?),
            volume_epoch: d.u64_le()?,
            protection_id: ProtectionId::from_bytes(d.fixed()?),
            grant: decode_grant(d)?,
        }),
        15 => Ok(RpcRequest::CheckAttachmentLive {
            branch_id: BranchId::from_bytes(d.fixed()?),
            attachment_id: AttachmentId::from_bytes(d.fixed()?),
            volume_epoch: d.u64_le()?,
            grant: decode_grant(d)?,
        }),
        16 => Ok(RpcRequest::MoveHead {
            operation_id: OperationId::from_bytes(d.fixed()?),
            branch_id: BranchId::from_bytes(d.fixed()?),
            expected_revision: d.u64_le()?,
            target_generation: GenerationId::from_bytes(d.fixed()?),
            grant: decode_grant(d)?,
            discard_live_state: d.u8()? != 0,
        }),
        17 => Ok(RpcRequest::Promote {
            operation_id: OperationId::from_bytes(d.fixed()?),
            source_branch_id: BranchId::from_bytes(d.fixed()?),
            expected_source_revision: d.u64_le()?,
            destination_branch_id: BranchId::from_bytes(d.fixed()?),
            expected_destination_revision: d.u64_le()?,
            destination_grant: decode_grant(d)?,
            discard_live_state: d.u8()? != 0,
        }),
        18 => Ok(RpcRequest::DeleteBranch {
            operation_id: OperationId::from_bytes(d.fixed()?),
            branch_id: BranchId::from_bytes(d.fixed()?),
            expected_revision: d.u64_le()?,
            grant: decode_grant(d)?,
            discard_live_state: d.u8()? != 0,
        }),
        19 => {
            let operation_id = OperationId::from_bytes(d.fixed()?);
            let branch_id = BranchId::from_bytes(d.fixed()?);
            let expected_revision = d.u64_le()?;
            let grant = decode_grant(d)?;
            let attachment_id = AttachmentId::from_bytes(d.fixed()?);
            let volume_epoch = d.u64_le()?;
            let protection_id = ProtectionId::from_bytes(d.fixed()?);
            let kind = match d.u8()? {
                1 => PreparationKind::VolumeBarrier,
                2 => PreparationKind::LogMutation,
                3 => PreparationKind::ArtifactMutation,
                4 => PreparationKind::CheckpointCapture,
                _ => return Err(FormatError::InvalidEnvelope("invalid preparation kind").into()),
            };
            let expected_roots = match d.u8()? {
                0 => None,
                1 => Some(decode_roots(d)?),
                _ => return Err(FormatError::InvalidEnvelope("invalid expected_roots tag").into()),
            };
            let dep_count = usize::try_from(d.u32_le()?)
                .map_err(|_| ServiceError::new(ServiceErrorCode::LimitExceeded, "dep count"))?;
            if dep_count > 1024 {
                return Err(ServiceError::new(
                    ServiceErrorCode::LimitExceeded,
                    "too many preparation dependencies",
                ));
            }
            let mut dependencies = Vec::with_capacity(dep_count);
            for _ in 0..dep_count {
                dependencies.push(d.object_ref()?);
            }
            let provenance = decode_provenance_binding(d)?;
            Ok(RpcRequest::BeginPreparation {
                request: PreparationRequest {
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
                },
                provenance,
            })
        }
        20 => Ok(RpcRequest::RenewPreparation {
            renewal_id: OperationId::from_bytes(d.fixed()?),
            prepared_id: OperationId::from_bytes(d.fixed()?),
            grant: decode_grant(d)?,
        }),
        21 => Ok(RpcRequest::AuthorizeCaptureChild {
            authorization_id: OperationId::from_bytes(d.fixed()?),
            capture_id: OperationId::from_bytes(d.fixed()?),
            child_id: OperationId::from_bytes(d.fixed()?),
            grant: decode_grant(d)?,
        }),
        22 => {
            let operation_id = OperationId::from_bytes(d.fixed()?);
            let expected_revision = d.u64_le()?;
            let grant = decode_grant(d)?;
            let roots = decode_roots(d)?;
            let capture_child = match d.u8()? {
                0 => None,
                1 => Some((
                    OperationId::from_bytes(d.fixed()?),
                    OperationId::from_bytes(d.fixed()?),
                )),
                _ => return Err(FormatError::InvalidEnvelope("invalid capture_child tag").into()),
            };
            let native_meta = match d.u8()? {
                0 => None,
                1 => {
                    let request_fingerprint = d.fixed::<32>()?;
                    let receipt_bytes = d.bytes(crate::control::MAX_NATIVE_RECEIPT_BYTES)?.to_vec();
                    let claim_log_id = match d.u8()? {
                        0 => None,
                        1 => Some(LogId::from_bytes(d.fixed()?)),
                        _ => {
                            return Err(
                                FormatError::InvalidEnvelope("invalid claim_log_id tag").into()
                            );
                        }
                    };
                    Some(NativeRootTransitionMeta {
                        request_fingerprint,
                        receipt_bytes,
                        claim_log_id,
                    })
                }
                _ => return Err(FormatError::InvalidEnvelope("invalid native_meta tag").into()),
            };
            Ok(RpcRequest::CommitLiveRoots {
                operation_id,
                expected_revision,
                grant,
                roots,
                capture_child,
                native_meta,
            })
        }
        23 => Ok(RpcRequest::SealCapture {
            operation_id: OperationId::from_bytes(d.fixed()?),
            capture_id: OperationId::from_bytes(d.fixed()?),
            expected_revision: d.u64_le()?,
            grant: decode_grant(d)?,
        }),
        24 => Ok(RpcRequest::PublishCapture {
            capture_id: OperationId::from_bytes(d.fixed()?),
            expected_revision: d.u64_le()?,
            expected_parent: GenerationId::from_bytes(d.fixed()?),
            grant: decode_grant(d)?,
            attachment_id: AttachmentId::from_bytes(d.fixed()?),
            volume_epoch: d.u64_le()?,
            descriptor_ref: d.object_ref()?,
            advance_head: d.u8()? != 0,
        }),
        25 => Ok(RpcRequest::UploadPreparedObject {
            operation_id: OperationId::from_bytes(d.fixed()?),
            reference: d.object_ref()?,
            encoded_bytes: d.bytes(MAX_ENCODED_OBJECT_BYTES)?.to_vec(),
        }),
        26 => Ok(RpcRequest::UploadProvenanceContext {
            branch_id: BranchId::from_bytes(d.fixed()?),
            grant: decode_grant(d)?,
            encoded_context: d.bytes(MAX_PROVENANCE_CONTEXT_BYTES)?.to_vec(),
        }),
        27 => Ok(RpcRequest::GetOperation {
            operation_id: OperationId::from_bytes(d.fixed()?),
        }),
        28 => Ok(RpcRequest::GetCommittedRootTransition {
            operation_id: OperationId::from_bytes(d.fixed()?),
        }),
        29 => Ok(RpcRequest::AbortOperation {
            operation_id: OperationId::from_bytes(d.fixed()?),
        }),
        30 => Ok(RpcRequest::ExpirePreparation {
            operation_id: OperationId::from_bytes(d.fixed()?),
        }),
        31 => Ok(RpcRequest::PinGeneration {
            operation_id: OperationId::from_bytes(d.fixed()?),
            generation_id: GenerationId::from_bytes(d.fixed()?),
            pin_id: PinId::from_bytes(d.fixed()?),
        }),
        32 => Ok(RpcRequest::UnpinGeneration {
            operation_id: OperationId::from_bytes(d.fixed()?),
            pin_id: PinId::from_bytes(d.fixed()?),
        }),
        33 => Ok(RpcRequest::OpenGeneration {
            operation_id: OperationId::from_bytes(d.fixed()?),
            generation_id: GenerationId::from_bytes(d.fixed()?),
            protection_id: ProtectionId::from_bytes(d.fixed()?),
        }),
        34 => Ok(RpcRequest::OpenLiveLog {
            operation_id: OperationId::from_bytes(d.fixed()?),
            branch_id: BranchId::from_bytes(d.fixed()?),
            expected_revision: d.u64_le()?,
            expected_root: d.object_ref()?,
            protection_id: ProtectionId::from_bytes(d.fixed()?),
        }),
        35 => Ok(RpcRequest::RenewProtection {
            operation_id: OperationId::from_bytes(d.fixed()?),
            protection_id: ProtectionId::from_bytes(d.fixed()?),
        }),
        36 => Ok(RpcRequest::CloseProtection {
            operation_id: OperationId::from_bytes(d.fixed()?),
            protection_id: ProtectionId::from_bytes(d.fixed()?),
        }),
        37 => Ok(RpcRequest::FetchProtectedObject {
            protection_id: ProtectionId::from_bytes(d.fixed()?),
            reference: d.object_ref()?,
        }),
        38 => Ok(RpcRequest::ReadBlocksAt {
            protection_id: ProtectionId::from_bytes(d.fixed()?),
            offset: d.u64_le()?,
            length: d.u64_le()?,
        }),
        39 => Ok(RpcRequest::ReadProvenance {
            protection_id: ProtectionId::from_bytes(d.fixed()?),
            generation_id: GenerationId::from_bytes(d.fixed()?),
        }),
        40 => Ok(RpcRequest::DiffGenerations {
            left_protection_id: ProtectionId::from_bytes(d.fixed()?),
            left_generation_id: GenerationId::from_bytes(d.fixed()?),
            right_protection_id: ProtectionId::from_bytes(d.fixed()?),
            right_generation_id: GenerationId::from_bytes(d.fixed()?),
        }),
        41 => Ok(RpcRequest::ReadLogAt {
            protection_id: ProtectionId::from_bytes(d.fixed()?),
            name: decode_str(d, 4096)?,
            from_seq: d.u64_le()?,
            max_batches: usize::try_from(d.u32_le()?)
                .map_err(|_| ServiceError::new(ServiceErrorCode::LimitExceeded, "max_batches"))?,
        }),
        42 => Ok(RpcRequest::ListLogs {
            protection_id: ProtectionId::from_bytes(d.fixed()?),
        }),
        43 => Ok(RpcRequest::ReadArtifact {
            protection_id: ProtectionId::from_bytes(d.fixed()?),
            name: decode_str(d, 4096)?,
            offset: d.u64_le()?,
            length: d.u64_le()?,
        }),
        44 => Ok(RpcRequest::ListArtifacts {
            protection_id: ProtectionId::from_bytes(d.fixed()?),
        }),
        45 => {
            let operation_id = OperationId::from_bytes(d.fixed()?);
            let budget = match d.u8()? {
                0 => None,
                1 => Some(GcReportBudget {
                    max_in_memory_visited: usize::try_from(d.u64_le()?).map_err(|_| {
                        ServiceError::new(
                            ServiceErrorCode::InvalidRequest,
                            "max_in_memory_visited overflow",
                        )
                    })?,
                    max_candidate_pages: d.u32_le()?,
                    max_candidates_per_page: usize::try_from(d.u64_le()?).map_err(|_| {
                        ServiceError::new(
                            ServiceErrorCode::InvalidRequest,
                            "max_candidates_per_page overflow",
                        )
                    })?,
                    max_report_bytes: usize::try_from(d.u64_le()?).map_err(|_| {
                        ServiceError::new(
                            ServiceErrorCode::InvalidRequest,
                            "max_report_bytes overflow",
                        )
                    })?,
                    detail_ttl_nanos: d.u64_le()?,
                    scan_page_size: usize::try_from(d.u64_le()?).map_err(|_| {
                        ServiceError::new(
                            ServiceErrorCode::InvalidRequest,
                            "scan_page_size overflow",
                        )
                    })?,
                }),
                _ => return Err(FormatError::InvalidEnvelope("invalid budget tag").into()),
            };
            Ok(RpcRequest::CollectReport {
                operation_id,
                budget,
            })
        }
        46 => {
            let report_id = OperationId::from_bytes(d.fixed()?);
            let page_index = match d.u8()? {
                0 => None,
                1 => Some(d.u32_le()?),
                _ => return Err(FormatError::InvalidEnvelope("invalid page_index tag").into()),
            };
            Ok(RpcRequest::GetGcReport {
                report_id,
                page_index,
            })
        }
        _ => Err(FormatError::InvalidEnvelope("unknown RPC request opcode").into()),
    }
}

// ---------------------------------------------------------------------------
// Typed payload codecs for RPC responses
// ---------------------------------------------------------------------------

/// Encodes [`BranchState`] for RPC response payloads.
pub fn encode_rpc_branch(b: &BranchState) -> Result<Vec<u8>, ServiceError> {
    let mut e = CanonicalEncoder::new();
    e.fixed(b.workspace_id.as_bytes());
    e.fixed(b.branch_id.as_bytes());
    encode_str(&mut e, &b.name)?;
    e.fixed(b.head.as_bytes());
    e.u64_le(b.revision);
    e.u64_le(b.pending_mutation_preparations);
    e.u64_le(b.writer_epoch);
    e.u64_le(b.volume_epoch);
    e.u64_le(b.stream_epoch);
    e.object_ref(b.volume_root);
    e.object_ref(b.logs_root);
    e.object_ref(b.artifacts_root);
    match b.writer {
        Some(g) => {
            e.u8(1);
            encode_grant(&mut e, g);
        }
        None => e.u8(0),
    }
    match b.attachment {
        Some(a) => {
            e.u8(1);
            encode_rpc_attachment_fields(&mut e, a);
        }
        None => e.u8(0),
    }
    match &b.capture {
        Some(c) => {
            e.u8(1);
            encode_rpc_capture_fields(&mut e, c);
        }
        None => e.u8(0),
    }
    e.u8(u8::from(b.deleted));
    Ok(e.finish())
}

/// Decodes [`BranchState`] from an RPC response payload.
pub fn decode_rpc_branch(bytes: &[u8]) -> Result<BranchState, ServiceError> {
    let mut d = CanonicalDecoder::new(bytes);
    let b = BranchState {
        workspace_id: WorkspaceId::from_bytes(d.fixed()?),
        branch_id: BranchId::from_bytes(d.fixed()?),
        name: decode_str(&mut d, 4096)?,
        head: GenerationId::from_bytes(d.fixed()?),
        revision: d.u64_le()?,
        pending_mutation_preparations: d.u64_le()?,
        writer_epoch: d.u64_le()?,
        volume_epoch: d.u64_le()?,
        stream_epoch: d.u64_le()?,
        volume_root: d.object_ref()?,
        logs_root: d.object_ref()?,
        artifacts_root: d.object_ref()?,
        writer: match d.u8()? {
            0 => None,
            1 => Some(decode_grant(&mut d)?),
            _ => return Err(FormatError::InvalidEnvelope("invalid writer tag").into()),
        },
        attachment: match d.u8()? {
            0 => None,
            1 => Some(decode_rpc_attachment_fields(&mut d)?),
            _ => return Err(FormatError::InvalidEnvelope("invalid attachment tag").into()),
        },
        capture: match d.u8()? {
            0 => None,
            1 => Some(decode_rpc_capture_fields(&mut d)?),
            _ => return Err(FormatError::InvalidEnvelope("invalid capture tag").into()),
        },
        deleted: d.u8()? != 0,
    };
    d.finish()?;
    Ok(b)
}

fn encode_rpc_attachment_fields(e: &mut CanonicalEncoder, a: AttachmentState) {
    e.fixed(a.attachment_id.as_bytes());
    e.fixed(a.protection_id.as_bytes());
    e.fixed(a.base_generation.as_bytes());
    e.u64_le(a.writer_epoch);
    e.u64_le(a.volume_epoch);
    e.u64_le(a.protection_expires_at.as_nanos());
}

fn decode_rpc_attachment_fields(
    d: &mut CanonicalDecoder<'_>,
) -> Result<AttachmentState, ServiceError> {
    Ok(AttachmentState {
        attachment_id: AttachmentId::from_bytes(d.fixed()?),
        protection_id: ProtectionId::from_bytes(d.fixed()?),
        base_generation: GenerationId::from_bytes(d.fixed()?),
        writer_epoch: d.u64_le()?,
        volume_epoch: d.u64_le()?,
        protection_expires_at: MonotonicTime::from_nanos(d.u64_le()?),
    })
}

fn encode_rpc_capture_fields(e: &mut CanonicalEncoder, c: &CaptureState) {
    e.fixed(c.operation_id.as_bytes());
    e.fixed(c.principal_id.as_bytes());
    e.fixed(c.protection_id.as_bytes());
    encode_provenance_binding(e, c.provenance);
    e.u64_le(c.expected_revision);
    e.u8(u8::from(c.sealed));
    match c.pending_child {
        Some(child) => {
            e.u8(1);
            e.fixed(child.as_bytes());
        }
        None => e.u8(0),
    }
    match c.sealed_revision {
        Some(rev) => {
            e.u8(1);
            e.u64_le(rev);
        }
        None => e.u8(0),
    }
    match c.sealed_roots {
        Some(roots) => {
            e.u8(1);
            encode_roots(e, roots);
        }
        None => e.u8(0),
    }
}

fn decode_rpc_capture_fields(d: &mut CanonicalDecoder<'_>) -> Result<CaptureState, ServiceError> {
    Ok(CaptureState {
        operation_id: OperationId::from_bytes(d.fixed()?),
        principal_id: PrincipalId::from_bytes(d.fixed()?),
        protection_id: ProtectionId::from_bytes(d.fixed()?),
        provenance: decode_provenance_binding(d)?,
        expected_revision: d.u64_le()?,
        sealed: d.u8()? != 0,
        pending_child: match d.u8()? {
            0 => None,
            1 => Some(OperationId::from_bytes(d.fixed()?)),
            _ => return Err(FormatError::InvalidEnvelope("invalid pending_child tag").into()),
        },
        sealed_revision: match d.u8()? {
            0 => None,
            1 => Some(d.u64_le()?),
            _ => return Err(FormatError::InvalidEnvelope("invalid sealed_revision tag").into()),
        },
        sealed_roots: match d.u8()? {
            0 => None,
            1 => Some(decode_roots(d)?),
            _ => return Err(FormatError::InvalidEnvelope("invalid sealed_roots tag").into()),
        },
    })
}

/// Encodes [`WriterGrant`] for RPC responses.
pub fn encode_rpc_grant(g: WriterGrant) -> Vec<u8> {
    let mut e = CanonicalEncoder::new();
    encode_grant(&mut e, g);
    e.finish()
}

/// Decodes [`WriterGrant`] from RPC responses.
pub fn decode_rpc_grant(bytes: &[u8]) -> Result<WriterGrant, ServiceError> {
    let mut d = CanonicalDecoder::new(bytes);
    let g = decode_grant(&mut d)?;
    d.finish()?;
    Ok(g)
}

/// Encodes [`AttachmentState`] for RPC responses.
pub fn encode_rpc_attachment(a: AttachmentState) -> Vec<u8> {
    let mut e = CanonicalEncoder::new();
    encode_rpc_attachment_fields(&mut e, a);
    e.finish()
}

/// Decodes [`AttachmentState`] from RPC responses.
pub fn decode_rpc_attachment(bytes: &[u8]) -> Result<AttachmentState, ServiceError> {
    let mut d = CanonicalDecoder::new(bytes);
    let a = decode_rpc_attachment_fields(&mut d)?;
    d.finish()?;
    Ok(a)
}

/// Encodes [`CaptureState`] for RPC responses.
pub fn encode_rpc_capture(c: &CaptureState) -> Vec<u8> {
    let mut e = CanonicalEncoder::new();
    encode_rpc_capture_fields(&mut e, c);
    e.finish()
}

/// Decodes [`CaptureState`] from RPC responses.
pub fn decode_rpc_capture(bytes: &[u8]) -> Result<CaptureState, ServiceError> {
    let mut d = CanonicalDecoder::new(bytes);
    let c = decode_rpc_capture_fields(&mut d)?;
    d.finish()?;
    Ok(c)
}

/// Encodes [`GenerationSeed`] for RPC responses.
pub fn encode_rpc_generation_seed(s: GenerationSeed) -> Vec<u8> {
    let mut e = CanonicalEncoder::new();
    e.fixed(s.generation_id.as_bytes());
    e.object_ref(s.descriptor);
    encode_roots(&mut e, s.roots);
    e.finish()
}

/// Decodes [`GenerationSeed`] from RPC responses.
pub fn decode_rpc_generation_seed(bytes: &[u8]) -> Result<GenerationSeed, ServiceError> {
    let mut d = CanonicalDecoder::new(bytes);
    let s = GenerationSeed {
        generation_id: GenerationId::from_bytes(d.fixed()?),
        descriptor: d.object_ref()?,
        roots: decode_roots(&mut d)?,
    };
    d.finish()?;
    Ok(s)
}

/// Encodes [`PublishedGeneration`] for RPC responses.
pub fn encode_rpc_published_generation(p: &PublishedGeneration) -> Result<Vec<u8>, ServiceError> {
    let mut e = CanonicalEncoder::new();
    e.fixed(p.generation_id.as_bytes());
    e.bytes(&encode_rpc_branch(&p.branch)?)?;
    e.u8(u8::from(p.head_advanced));
    Ok(e.finish())
}

/// Decodes [`PublishedGeneration`] from RPC responses.
pub fn decode_rpc_published_generation(bytes: &[u8]) -> Result<PublishedGeneration, ServiceError> {
    let mut d = CanonicalDecoder::new(bytes);
    let generation_id = GenerationId::from_bytes(d.fixed()?);
    let branch_bytes = d.bytes(MAX_RPC_FRAME_BYTES)?;
    let branch = decode_rpc_branch(branch_bytes)?;
    let head_advanced = d.u8()? != 0;
    d.finish()?;
    Ok(PublishedGeneration {
        generation_id,
        branch,
        head_advanced,
    })
}

/// Encodes [`OperationState`] for RPC responses.
pub fn encode_rpc_operation_state(state: &OperationState) -> Result<Vec<u8>, ServiceError> {
    let mut e = CanonicalEncoder::new();
    match state {
        OperationState::Preparing {
            operation_id,
            expires_at,
            branch_id,
        } => {
            e.u8(1);
            e.fixed(operation_id.as_bytes());
            e.u64_le(expires_at.as_nanos());
            e.fixed(branch_id.as_bytes());
        }
        OperationState::Committed(bytes) => {
            e.u8(2);
            e.bytes(bytes)?;
        }
        OperationState::Aborted => e.u8(3),
    }
    Ok(e.finish())
}

/// Decodes [`OperationState`] from RPC responses.
pub fn decode_rpc_operation_state(bytes: &[u8]) -> Result<OperationState, ServiceError> {
    let mut d = CanonicalDecoder::new(bytes);
    let state = match d.u8()? {
        1 => OperationState::Preparing {
            operation_id: OperationId::from_bytes(d.fixed()?),
            expires_at: MonotonicTime::from_nanos(d.u64_le()?),
            branch_id: BranchId::from_bytes(d.fixed()?),
        },
        2 => OperationState::Committed(d.bytes(MAX_RPC_FRAME_BYTES)?.to_vec()),
        3 => OperationState::Aborted,
        _ => return Err(FormatError::InvalidEnvelope("invalid operation state tag").into()),
    };
    d.finish()?;
    Ok(state)
}

/// Encodes `Option<CommittedRootTransition>` for RPC responses.
pub fn encode_rpc_root_transition(
    opt: Option<&CommittedRootTransition>,
) -> Result<Vec<u8>, ServiceError> {
    let mut e = CanonicalEncoder::new();
    match opt {
        None => e.u8(0),
        Some(t) => {
            e.u8(1);
            e.u8(t.kind as u8);
            encode_roots(&mut e, t.previous_roots);
            e.bytes(&encode_rpc_branch(&t.branch)?)?;
            match &t.native_meta {
                None => e.u8(0),
                Some(m) => {
                    e.u8(1);
                    e.fixed(&m.request_fingerprint);
                    e.bytes(&m.receipt_bytes)?;
                    match m.claim_log_id {
                        Some(id) => {
                            e.u8(1);
                            e.fixed(id.as_bytes());
                        }
                        None => e.u8(0),
                    }
                }
            }
        }
    }
    Ok(e.finish())
}

/// Decodes `Option<CommittedRootTransition>` from RPC responses.
pub fn decode_rpc_root_transition(
    bytes: &[u8],
) -> Result<Option<CommittedRootTransition>, ServiceError> {
    let mut d = CanonicalDecoder::new(bytes);
    let out = match d.u8()? {
        0 => None,
        1 => {
            let kind = match d.u8()? {
                1 => PreparationKind::VolumeBarrier,
                2 => PreparationKind::LogMutation,
                3 => PreparationKind::ArtifactMutation,
                4 => PreparationKind::CheckpointCapture,
                _ => return Err(FormatError::InvalidEnvelope("invalid preparation kind").into()),
            };
            let previous_roots = decode_roots(&mut d)?;
            let branch = decode_rpc_branch(d.bytes(MAX_RPC_FRAME_BYTES)?)?;
            let native_meta = match d.u8()? {
                0 => None,
                1 => {
                    let request_fingerprint = d.fixed::<32>()?;
                    let receipt_bytes = d.bytes(crate::control::MAX_NATIVE_RECEIPT_BYTES)?.to_vec();
                    let claim_log_id = match d.u8()? {
                        0 => None,
                        1 => Some(LogId::from_bytes(d.fixed()?)),
                        _ => {
                            return Err(
                                FormatError::InvalidEnvelope("invalid claim_log_id tag").into()
                            );
                        }
                    };
                    Some(NativeRootTransitionMeta {
                        request_fingerprint,
                        receipt_bytes,
                        claim_log_id,
                    })
                }
                _ => return Err(FormatError::InvalidEnvelope("invalid native_meta tag").into()),
            };
            Some(CommittedRootTransition {
                kind,
                previous_roots,
                branch,
                native_meta,
            })
        }
        _ => return Err(FormatError::InvalidEnvelope("invalid root transition tag").into()),
    };
    d.finish()?;
    Ok(out)
}

/// Encodes [`ProvenanceInspection`] for RPC responses.
pub fn encode_rpc_provenance_inspection(p: &ProvenanceInspection) -> Result<Vec<u8>, ServiceError> {
    let mut e = CanonicalEncoder::new();
    e.fixed(p.generation_id.as_bytes());
    e.u8(match p.source_format {
        GenerationDescriptorFormat::V1 => 1,
        GenerationDescriptorFormat::V2 => 2,
    });
    e.u8(p.status as u8);
    e.u8(p.coverage as u8);
    match p.context_root {
        Some(r) => {
            e.u8(1);
            e.object_ref(r);
        }
        None => e.u8(0),
    }
    match &p.context {
        Some(ctx) => {
            e.u8(1);
            e.bytes(&ctx.encode()?)?;
        }
        None => e.u8(0),
    }
    Ok(e.finish())
}

/// Decodes [`ProvenanceInspection`] from RPC responses.
pub fn decode_rpc_provenance_inspection(
    bytes: &[u8],
) -> Result<ProvenanceInspection, ServiceError> {
    let mut d = CanonicalDecoder::new(bytes);
    let generation_id = GenerationId::from_bytes(d.fixed()?);
    let source_format = match d.u8()? {
        1 => GenerationDescriptorFormat::V1,
        2 => GenerationDescriptorFormat::V2,
        _ => return Err(FormatError::InvalidEnvelope("invalid descriptor format").into()),
    };
    let status = match d.u8()? {
        1 => ProvenanceStatus::Recorded,
        2 => ProvenanceStatus::Unattributed,
        _ => return Err(FormatError::InvalidProvenance("invalid provenance status").into()),
    };
    let coverage = match d.u8()? {
        0 => ProvenanceCoverage::None,
        1 => ProvenanceCoverage::GenerationContext,
        _ => return Err(FormatError::InvalidProvenance("invalid provenance coverage").into()),
    };
    let context_root = match d.u8()? {
        0 => None,
        1 => Some(d.object_ref()?),
        _ => return Err(FormatError::InvalidProvenance("invalid context_root tag").into()),
    };
    let context = match d.u8()? {
        0 => None,
        1 => Some(ProvenanceContext::decode(
            d.bytes(MAX_PROVENANCE_CONTEXT_BYTES)?,
        )?),
        _ => return Err(FormatError::InvalidProvenance("invalid context tag").into()),
    };
    d.finish()?;
    Ok(ProvenanceInspection {
        generation_id,
        source_format,
        status,
        coverage,
        context_root,
        context,
    })
}

/// Encodes [`DiffStreamChunk`] for streamed diff responses.
pub fn encode_rpc_diff_chunk(chunk: &DiffStreamChunk) -> Result<Vec<u8>, ServiceError> {
    let mut e = CanonicalEncoder::new();
    match chunk {
        DiffStreamChunk::Extents(ranges) => {
            e.u8(1);
            e.u32_le(u32::try_from(ranges.len()).map_err(|_| {
                ServiceError::new(ServiceErrorCode::LimitExceeded, "too many extent ranges")
            })?);
            for r in ranges {
                e.u64_le(r.start_extent);
                e.u64_le(r.extent_count);
            }
        }
        DiffStreamChunk::Logs(changes) => {
            e.u8(2);
            e.u32_le(u32::try_from(changes.len()).map_err(|_| {
                ServiceError::new(ServiceErrorCode::LimitExceeded, "too many log changes")
            })?);
            for change in changes {
                match change {
                    LogViewChange::Added { name, entry } => {
                        e.u8(1);
                        encode_str(&mut e, name)?;
                        e.bytes(&entry.encode()?)?;
                    }
                    LogViewChange::Removed { name, entry } => {
                        e.u8(2);
                        encode_str(&mut e, name)?;
                        e.bytes(&entry.encode()?)?;
                    }
                    LogViewChange::Updated {
                        name,
                        before,
                        after,
                    } => {
                        e.u8(3);
                        encode_str(&mut e, name)?;
                        e.bytes(&before.encode()?)?;
                        e.bytes(&after.encode()?)?;
                    }
                }
            }
        }
        DiffStreamChunk::Artifacts(changes) => {
            e.u8(3);
            e.u32_le(u32::try_from(changes.len()).map_err(|_| {
                ServiceError::new(ServiceErrorCode::LimitExceeded, "too many artifact changes")
            })?);
            for change in changes {
                match change {
                    ArtifactChange::Added { name, descriptor } => {
                        e.u8(1);
                        encode_str(&mut e, name)?;
                        e.bytes(&descriptor.encode()?)?;
                    }
                    ArtifactChange::Removed { name, descriptor } => {
                        e.u8(2);
                        encode_str(&mut e, name)?;
                        e.bytes(&descriptor.encode()?)?;
                    }
                    ArtifactChange::Updated {
                        name,
                        before,
                        after,
                    } => {
                        e.u8(3);
                        encode_str(&mut e, name)?;
                        e.bytes(&before.encode()?)?;
                        e.bytes(&after.encode()?)?;
                    }
                }
            }
        }
    }
    Ok(e.finish())
}

/// Decodes [`DiffStreamChunk`] from streamed diff responses.
pub fn decode_rpc_diff_chunk(bytes: &[u8]) -> Result<DiffStreamChunk, ServiceError> {
    let mut d = CanonicalDecoder::new(bytes);
    let chunk = match d.u8()? {
        1 => {
            let count = usize::try_from(d.u32_le()?).map_err(|_| {
                ServiceError::new(ServiceErrorCode::LimitExceeded, "diff count overflow")
            })?;
            if count > MAX_DIFF_CHUNK_ITEMS {
                return Err(ServiceError::new(
                    ServiceErrorCode::LimitExceeded,
                    "diff chunk exceeds bound",
                ));
            }
            let mut ranges = Vec::with_capacity(count);
            for _ in 0..count {
                ranges.push(ExtentRange {
                    start_extent: d.u64_le()?,
                    extent_count: d.u64_le()?,
                });
            }
            DiffStreamChunk::Extents(ranges)
        }
        2 => {
            let count = usize::try_from(d.u32_le()?).map_err(|_| {
                ServiceError::new(ServiceErrorCode::LimitExceeded, "diff count overflow")
            })?;
            if count > MAX_DIFF_CHUNK_ITEMS {
                return Err(ServiceError::new(
                    ServiceErrorCode::LimitExceeded,
                    "diff chunk exceeds bound",
                ));
            }
            let mut changes = Vec::with_capacity(count);
            for _ in 0..count {
                match d.u8()? {
                    1 => changes.push(LogViewChange::Added {
                        name: decode_str(&mut d, 4096)?,
                        entry: LogMapEntry::decode(d.bytes(4096)?)?,
                    }),
                    2 => changes.push(LogViewChange::Removed {
                        name: decode_str(&mut d, 4096)?,
                        entry: LogMapEntry::decode(d.bytes(4096)?)?,
                    }),
                    3 => changes.push(LogViewChange::Updated {
                        name: decode_str(&mut d, 4096)?,
                        before: LogMapEntry::decode(d.bytes(4096)?)?,
                        after: LogMapEntry::decode(d.bytes(4096)?)?,
                    }),
                    _ => return Err(FormatError::InvalidEnvelope("invalid log diff tag").into()),
                }
            }
            DiffStreamChunk::Logs(changes)
        }
        3 => {
            let count = usize::try_from(d.u32_le()?).map_err(|_| {
                ServiceError::new(ServiceErrorCode::LimitExceeded, "diff count overflow")
            })?;
            if count > MAX_DIFF_CHUNK_ITEMS {
                return Err(ServiceError::new(
                    ServiceErrorCode::LimitExceeded,
                    "diff chunk exceeds bound",
                ));
            }
            let mut changes = Vec::with_capacity(count);
            for _ in 0..count {
                match d.u8()? {
                    1 => changes.push(ArtifactChange::Added {
                        name: decode_str(&mut d, 4096)?,
                        descriptor: ArtifactDescriptor::decode(d.bytes(64 * 1024)?)?,
                    }),
                    2 => changes.push(ArtifactChange::Removed {
                        name: decode_str(&mut d, 4096)?,
                        descriptor: ArtifactDescriptor::decode(d.bytes(64 * 1024)?)?,
                    }),
                    3 => changes.push(ArtifactChange::Updated {
                        name: decode_str(&mut d, 4096)?,
                        before: ArtifactDescriptor::decode(d.bytes(64 * 1024)?)?,
                        after: ArtifactDescriptor::decode(d.bytes(64 * 1024)?)?,
                    }),
                    _ => {
                        return Err(
                            FormatError::InvalidEnvelope("invalid artifact diff tag").into()
                        );
                    }
                }
            }
            DiffStreamChunk::Artifacts(changes)
        }
        _ => return Err(FormatError::InvalidEnvelope("invalid diff chunk tag").into()),
    };
    d.finish()?;
    Ok(chunk)
}

/// Encodes a named-log list for RPC responses.
pub fn encode_rpc_log_list(entries: &[(String, LogMapEntry)]) -> Result<Vec<u8>, ServiceError> {
    let mut e = CanonicalEncoder::new();
    e.u32_le(
        u32::try_from(entries.len()).map_err(|_| {
            ServiceError::new(ServiceErrorCode::LimitExceeded, "too many log entries")
        })?,
    );
    for (name, entry) in entries {
        encode_str(&mut e, name)?;
        e.bytes(&entry.encode()?)?;
    }
    Ok(e.finish())
}

/// Decodes a named-log list from RPC responses.
pub fn decode_rpc_log_list(bytes: &[u8]) -> Result<Vec<(String, LogMapEntry)>, ServiceError> {
    let mut d = CanonicalDecoder::new(bytes);
    let count = usize::try_from(d.u32_le()?)
        .map_err(|_| ServiceError::new(ServiceErrorCode::LimitExceeded, "list count overflow"))?;
    let mut out = Vec::with_capacity(count.min(4096));
    for _ in 0..count {
        out.push((
            decode_str(&mut d, 4096)?,
            LogMapEntry::decode(d.bytes(4096)?)?,
        ));
    }
    d.finish()?;
    Ok(out)
}

/// Encodes a named-artifact list for RPC responses.
pub fn encode_rpc_artifact_list(
    entries: &[(String, ArtifactDescriptor)],
) -> Result<Vec<u8>, ServiceError> {
    let mut e = CanonicalEncoder::new();
    e.u32_le(u32::try_from(entries.len()).map_err(|_| {
        ServiceError::new(ServiceErrorCode::LimitExceeded, "too many artifact entries")
    })?);
    for (name, desc) in entries {
        encode_str(&mut e, name)?;
        e.bytes(&desc.encode()?)?;
    }
    Ok(e.finish())
}

/// Decodes a named-artifact list from RPC responses.
pub fn decode_rpc_artifact_list(
    bytes: &[u8],
) -> Result<Vec<(String, ArtifactDescriptor)>, ServiceError> {
    let mut d = CanonicalDecoder::new(bytes);
    let count = usize::try_from(d.u32_le()?)
        .map_err(|_| ServiceError::new(ServiceErrorCode::LimitExceeded, "list count overflow"))?;
    let mut out = Vec::with_capacity(count.min(4096));
    for _ in 0..count {
        out.push((
            decode_str(&mut d, 4096)?,
            ArtifactDescriptor::decode(d.bytes(64 * 1024)?)?,
        ));
    }
    d.finish()?;
    Ok(out)
}

/// Encodes one streamed log batch (`(first_seq, end_seq, log_id, records, attachments)`).
pub fn encode_rpc_log_batch(
    log_id: LogId,
    first_seq: u64,
    end_seq: u64,
    records: &[Vec<u8>],
    attachments: &[ObjectRef],
) -> Result<Vec<u8>, ServiceError> {
    let mut e = CanonicalEncoder::new();
    e.fixed(log_id.as_bytes());
    e.u64_le(first_seq);
    e.u64_le(end_seq);
    e.u32_le(u32::try_from(records.len()).map_err(|_| {
        ServiceError::new(ServiceErrorCode::LimitExceeded, "record count overflow")
    })?);
    for rec in records {
        e.bytes(rec)?;
    }
    e.u32_le(u32::try_from(attachments.len()).map_err(|_| {
        ServiceError::new(ServiceErrorCode::LimitExceeded, "attachment count overflow")
    })?);
    for att in attachments {
        e.object_ref(*att);
    }
    Ok(e.finish())
}

/// Decoded streamed log batch payload `(log_id, first_seq, end_seq, records, attachments)`.
pub type RpcLogBatch = (LogId, u64, u64, Vec<Vec<u8>>, Vec<ObjectRef>);

/// Decodes one streamed log batch.
pub fn decode_rpc_log_batch(bytes: &[u8]) -> Result<RpcLogBatch, ServiceError> {
    let mut d = CanonicalDecoder::new(bytes);
    let log_id = LogId::from_bytes(d.fixed()?);
    let first_seq = d.u64_le()?;
    let end_seq = d.u64_le()?;
    let rec_count = usize::try_from(d.u32_le()?)
        .map_err(|_| ServiceError::new(ServiceErrorCode::LimitExceeded, "record count overflow"))?;
    if rec_count > MAX_LOG_RECORDS {
        return Err(ServiceError::new(
            ServiceErrorCode::LimitExceeded,
            "too many log records",
        ));
    }
    let mut records = Vec::with_capacity(rec_count);
    for _ in 0..rec_count {
        records.push(d.bytes(MAX_ENCODED_OBJECT_BYTES)?.to_vec());
    }
    let att_count = usize::try_from(d.u32_le()?).map_err(|_| {
        ServiceError::new(ServiceErrorCode::LimitExceeded, "attachment count overflow")
    })?;
    let mut attachments = Vec::with_capacity(att_count.min(4096));
    for _ in 0..att_count {
        attachments.push(d.object_ref()?);
    }
    d.finish()?;
    Ok((log_id, first_seq, end_seq, records, attachments))
}

/// Encodes a [`GcReportSummary`] RPC response payload.
pub fn encode_rpc_gc_report_summary(summary: &GcReportSummary) -> Vec<u8> {
    summary.encode()
}

/// Decodes a [`GcReportSummary`] RPC response payload.
pub fn decode_rpc_gc_report_summary(bytes: &[u8]) -> Result<GcReportSummary, ServiceError> {
    Ok(GcReportSummary::decode(bytes)?)
}

/// Encodes a [`GcCandidatePage`] RPC response payload.
pub fn encode_rpc_gc_report_page(page: &GcCandidatePage) -> Result<Vec<u8>, ServiceError> {
    Ok(page.encode()?)
}

/// Decodes a [`GcCandidatePage`] RPC response payload.
pub fn decode_rpc_gc_report_page(bytes: &[u8]) -> Result<GcCandidatePage, ServiceError> {
    Ok(GcCandidatePage::decode(bytes)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::OwnerIncarnation;
    use crate::format::ObjectType;
    use crate::identity::{ArtifactId, ObjectId};
    use crate::metadata::MetadataError;
    use crate::time::MonotonicTime;
    use crate::views::{ExtentRange, LogViewChange};

    fn sample_ref(object_type: ObjectType, seed: u8) -> ObjectRef {
        ObjectRef {
            object_type,
            object_id: ObjectId::from_bytes([seed; 32]),
            encoded_len: 128,
        }
    }

    fn sample_grant(branch_id: BranchId) -> WriterGrant {
        WriterGrant {
            branch_id,
            writer_epoch: 3,
            owner_incarnation: OwnerIncarnation::from_u64(7),
            grant_id: OperationId::from_bytes([4; 16]),
            expires_at: MonotonicTime::from_nanos(9_000_000_000),
        }
    }

    fn sample_roots() -> LiveRoots {
        LiveRoots {
            volume: sample_ref(ObjectType::VolumeRoot, 1),
            logs: sample_ref(ObjectType::LogMapRoot, 2),
            artifacts: sample_ref(ObjectType::ArtifactMapRoot, 3),
        }
    }

    #[test]
    fn rpc_envelope_round_trips_all_request_variants() {
        let ws = WorkspaceId::from_bytes([1; 16]);
        let principal = PrincipalId::from_bytes([2; 16]);
        let br = BranchId::from_bytes([3; 16]);
        let gen_id = GenerationId::from_bytes([4; 16]);
        let att_id = AttachmentId::from_bytes([5; 16]);
        let prot_id = ProtectionId::from_bytes([6; 16]);
        let op_id = OperationId::from_bytes([7; 16]);
        let pin_id = PinId::from_bytes([8; 16]);
        let boundary_id = VolumeBoundaryId::from_bytes([9; 16]);
        let grant = sample_grant(br);
        let roots = sample_roots();
        let geometry = VolumeGeometry::new(64 * 1024).unwrap();

        let requests = vec![
            RpcRequest::GetScope,
            RpcRequest::Now,
            RpcRequest::CreateEmptyWorkspace {
                create_op_id: op_id,
                generation_id: gen_id,
                boundary_id,
                geometry,
                created_at_unix_ms: 1234,
                branch_id: br,
                name: "main".to_owned(),
                retention_period_ms: 60_000,
                acquire_op_id: op_id,
                attach_op_id: op_id,
                attachment_id: att_id,
                attachment_protection_id: prot_id,
            },
            RpcRequest::CreateBranch {
                operation_id: op_id,
                branch_id: br,
                name: "dev".to_owned(),
                base: gen_id,
            },
            RpcRequest::GetBranch { branch_id: br },
            RpcRequest::BranchByName {
                name: "dev".to_owned(),
            },
            RpcRequest::GetGeneration {
                generation_id: gen_id,
            },
            RpcRequest::AcquireWriter {
                operation_id: op_id,
                branch_id: br,
                expected_revision: 1,
                expected_writer_epoch: 0,
            },
            RpcRequest::ForceAcquireWriter {
                operation_id: op_id,
                branch_id: br,
                expected_revision: 1,
                expected_writer_epoch: 0,
            },
            RpcRequest::RenewWriter {
                operation_id: op_id,
                grant,
            },
            RpcRequest::ReleaseWriter {
                operation_id: op_id,
                grant,
            },
            RpcRequest::AttachVolume {
                operation_id: op_id,
                branch_id: br,
                expected_revision: 1,
                attachment_id: att_id,
                protection_id: prot_id,
                grant,
            },
            RpcRequest::RenewAttachmentBase {
                operation_id: op_id,
                branch_id: br,
                attachment_id: att_id,
                volume_epoch: 1,
                protection_id: prot_id,
                grant,
            },
            RpcRequest::DetachVolume {
                operation_id: op_id,
                branch_id: br,
                attachment_id: att_id,
                volume_epoch: 1,
                protection_id: prot_id,
                grant,
            },
            RpcRequest::CheckAttachmentLive {
                branch_id: br,
                attachment_id: att_id,
                volume_epoch: 1,
                grant,
            },
            RpcRequest::MoveHead {
                operation_id: op_id,
                branch_id: br,
                expected_revision: 2,
                target_generation: gen_id,
                grant,
                discard_live_state: true,
            },
            RpcRequest::Promote {
                operation_id: op_id,
                source_branch_id: br,
                expected_source_revision: 2,
                destination_branch_id: br,
                expected_destination_revision: 1,
                destination_grant: grant,
                discard_live_state: false,
            },
            RpcRequest::DeleteBranch {
                operation_id: op_id,
                branch_id: br,
                expected_revision: 2,
                grant,
                discard_live_state: true,
            },
            RpcRequest::BeginPreparation {
                request: PreparationRequest {
                    operation_id: op_id,
                    branch_id: br,
                    expected_revision: 2,
                    grant,
                    attachment_id: att_id,
                    volume_epoch: 1,
                    protection_id: prot_id,
                    kind: PreparationKind::CheckpointCapture,
                    expected_roots: Some(roots),
                    dependencies: vec![sample_ref(ObjectType::Extent, 10)],
                },
                provenance: ProvenanceBinding::Recorded {
                    context_root: sample_ref(ObjectType::ProvenanceContext, 11),
                },
            },
            RpcRequest::RenewPreparation {
                renewal_id: op_id,
                prepared_id: op_id,
                grant,
            },
            RpcRequest::AuthorizeCaptureChild {
                authorization_id: op_id,
                capture_id: op_id,
                child_id: op_id,
                grant,
            },
            RpcRequest::CommitLiveRoots {
                operation_id: op_id,
                expected_revision: 2,
                grant,
                roots,
                capture_child: Some((op_id, op_id)),
                native_meta: Some(NativeRootTransitionMeta {
                    request_fingerprint: [0x11; 32],
                    receipt_bytes: vec![1, 2, 3],
                    claim_log_id: Some(LogId::from_bytes([12; 16])),
                }),
            },
            RpcRequest::SealCapture {
                operation_id: op_id,
                capture_id: op_id,
                expected_revision: 3,
                grant,
            },
            RpcRequest::PublishCapture {
                capture_id: op_id,
                expected_revision: 3,
                expected_parent: gen_id,
                grant,
                attachment_id: att_id,
                volume_epoch: 1,
                descriptor_ref: sample_ref(ObjectType::GenerationDescriptor, 13),
                advance_head: true,
            },
            RpcRequest::UploadPreparedObject {
                operation_id: op_id,
                reference: sample_ref(ObjectType::Extent, 14),
                encoded_bytes: vec![1, 2, 3, 4],
            },
            RpcRequest::UploadProvenanceContext {
                branch_id: br,
                grant,
                encoded_context: vec![5, 6, 7],
            },
            RpcRequest::GetOperation {
                operation_id: op_id,
            },
            RpcRequest::GetCommittedRootTransition {
                operation_id: op_id,
            },
            RpcRequest::AbortOperation {
                operation_id: op_id,
            },
            RpcRequest::ExpirePreparation {
                operation_id: op_id,
            },
            RpcRequest::PinGeneration {
                operation_id: op_id,
                generation_id: gen_id,
                pin_id,
            },
            RpcRequest::UnpinGeneration {
                operation_id: op_id,
                pin_id,
            },
            RpcRequest::OpenGeneration {
                operation_id: op_id,
                generation_id: gen_id,
                protection_id: prot_id,
            },
            RpcRequest::OpenLiveLog {
                operation_id: op_id,
                branch_id: br,
                expected_revision: 3,
                expected_root: roots.logs,
                protection_id: prot_id,
            },
            RpcRequest::RenewProtection {
                operation_id: op_id,
                protection_id: prot_id,
            },
            RpcRequest::CloseProtection {
                operation_id: op_id,
                protection_id: prot_id,
            },
            RpcRequest::FetchProtectedObject {
                protection_id: prot_id,
                reference: sample_ref(ObjectType::Extent, 15),
            },
            RpcRequest::ReadBlocksAt {
                protection_id: prot_id,
                offset: 4096,
                length: 8192,
            },
            RpcRequest::ReadProvenance {
                protection_id: prot_id,
                generation_id: gen_id,
            },
            RpcRequest::DiffGenerations {
                left_protection_id: prot_id,
                left_generation_id: gen_id,
                right_protection_id: prot_id,
                right_generation_id: gen_id,
            },
            RpcRequest::ReadLogAt {
                protection_id: prot_id,
                name: "events".to_owned(),
                from_seq: 0,
                max_batches: 16,
            },
            RpcRequest::ListLogs {
                protection_id: prot_id,
            },
            RpcRequest::ReadArtifact {
                protection_id: prot_id,
                name: "plan.txt".to_owned(),
                offset: 0,
                length: 4096,
            },
            RpcRequest::ListArtifacts {
                protection_id: prot_id,
            },
            RpcRequest::CollectReport {
                operation_id: op_id,
                budget: Some(GcReportBudget {
                    max_in_memory_visited: 128,
                    max_candidate_pages: 4,
                    max_candidates_per_page: 16,
                    max_report_bytes: 4096,
                    detail_ttl_nanos: 60_000_000_000,
                    scan_page_size: 32,
                }),
            },
            RpcRequest::GetGcReport {
                report_id: op_id,
                page_index: Some(0),
            },
        ];

        for req in requests {
            let env = RpcEnvelope {
                auth: RpcAuth {
                    workspace_id: ws,
                    principal_id: principal,
                    auth_token: b"tok".to_vec(),
                },
                request: req.clone(),
            };
            let encoded = env.encode().unwrap();
            let decoded = RpcEnvelope::decode(&encoded).unwrap();
            assert_eq!(decoded, env);
        }
    }

    #[test]
    fn rpc_frames_codecs_and_errors_round_trip() {
        let mut buf = Vec::new();
        let frame_data = RpcResponseFrame::Data(b"hello".to_vec());
        let frame_end = RpcResponseFrame::End;
        let frame_err =
            RpcResponseFrame::Error(ServiceError::new(ServiceErrorCode::Unauthorized, "denied"));
        for frame in [&frame_data, &frame_end, &frame_err] {
            let enc = frame.encode().unwrap();
            write_frame(&mut buf, &enc).unwrap();
        }
        let mut cursor = std::io::Cursor::new(buf);
        for expected in [frame_data, frame_end, frame_err] {
            let raw = read_frame(&mut cursor).unwrap();
            assert_eq!(RpcResponseFrame::decode(&raw).unwrap(), expected);
        }

        let br = BranchId::from_bytes([1; 16]);
        let gen_id = GenerationId::from_bytes([2; 16]);
        let att_id = AttachmentId::from_bytes([3; 16]);
        let prot_id = ProtectionId::from_bytes([4; 16]);
        let op_id = OperationId::from_bytes([5; 16]);
        let roots = sample_roots();
        let grant = sample_grant(br);
        let att = AttachmentState {
            attachment_id: att_id,
            protection_id: prot_id,
            base_generation: gen_id,
            writer_epoch: 1,
            volume_epoch: 1,
            protection_expires_at: MonotonicTime::from_nanos(5000),
        };
        assert_eq!(
            decode_rpc_attachment(&encode_rpc_attachment(att)).unwrap(),
            att
        );

        let branch = BranchState {
            workspace_id: WorkspaceId::from_bytes([9; 16]),
            branch_id: br,
            name: "main".to_owned(),
            head: gen_id,
            revision: 2,
            pending_mutation_preparations: 0,
            writer_epoch: 1,
            volume_epoch: 1,
            stream_epoch: 1,
            volume_root: roots.volume,
            logs_root: roots.logs,
            artifacts_root: roots.artifacts,
            writer: Some(grant),
            attachment: Some(att),
            capture: None,
            deleted: false,
        };
        assert_eq!(
            decode_rpc_branch(&encode_rpc_branch(&branch).unwrap()).unwrap(),
            branch
        );

        let seed = GenerationSeed {
            generation_id: gen_id,
            descriptor: sample_ref(ObjectType::GenerationDescriptor, 1),
            roots,
        };
        assert_eq!(
            decode_rpc_generation_seed(&encode_rpc_generation_seed(seed)).unwrap(),
            seed
        );

        let cap = CaptureState {
            operation_id: op_id,
            principal_id: PrincipalId::from_bytes([8; 16]),
            protection_id: prot_id,
            provenance: ProvenanceBinding::Unattributed,
            expected_revision: 2,
            sealed: true,
            pending_child: None,
            sealed_revision: Some(2),
            sealed_roots: Some(roots),
        };
        assert_eq!(decode_rpc_capture(&encode_rpc_capture(&cap)).unwrap(), cap);

        let pub_gen = PublishedGeneration {
            generation_id: gen_id,
            branch: branch.clone(),
            head_advanced: true,
        };
        assert_eq!(
            decode_rpc_published_generation(&encode_rpc_published_generation(&pub_gen).unwrap())
                .unwrap(),
            pub_gen
        );

        let transition = CommittedRootTransition {
            kind: PreparationKind::ArtifactMutation,
            previous_roots: roots,
            branch: branch.clone(),
            native_meta: None,
        };
        assert_eq!(
            decode_rpc_root_transition(&encode_rpc_root_transition(Some(&transition)).unwrap())
                .unwrap(),
            Some(transition)
        );

        let op_state = OperationState::Preparing {
            operation_id: op_id,
            expires_at: MonotonicTime::from_nanos(7000),
            branch_id: br,
        };
        assert_eq!(
            decode_rpc_operation_state(&encode_rpc_operation_state(&op_state).unwrap()).unwrap(),
            op_state
        );

        let insp = ProvenanceInspection {
            generation_id: gen_id,
            source_format: GenerationDescriptorFormat::V2,
            status: ProvenanceStatus::Recorded,
            coverage: ProvenanceCoverage::GenerationContext,
            context_root: Some(sample_ref(ObjectType::ProvenanceContext, 2)),
            context: Some(ProvenanceContext {
                recording_principal: PrincipalId::from_bytes([1; 16]),
                actor: Some("worker".to_owned()),
                execution_id: None,
                session_id: None,
                thread_id: None,
                task_id: None,
                workflow_operation_id: None,
                reason: Some("ok".to_owned()),
                trace_id: None,
                span_id: None,
                parent_span_id: None,
                required_logs_root: roots.logs,
                required_artifacts_root: roots.artifacts,
                optional_links: vec!["https://example.invalid".to_owned()],
            }),
        };
        assert_eq!(
            decode_rpc_provenance_inspection(&encode_rpc_provenance_inspection(&insp).unwrap())
                .unwrap(),
            insp
        );

        let diff_chunk = DiffStreamChunk::Extents(vec![ExtentRange {
            start_extent: 0,
            extent_count: 1,
        }]);
        assert_eq!(
            decode_rpc_diff_chunk(&encode_rpc_diff_chunk(&diff_chunk).unwrap()).unwrap(),
            diff_chunk
        );
        let log_diff = DiffStreamChunk::Logs(vec![LogViewChange::Added {
            name: "events".to_owned(),
            entry: LogMapEntry {
                log_id: LogId::from_bytes([1; 16]),
                first_seq: 0,
                end_seq: 1,
                segment_index_root: sample_ref(ObjectType::LogSegmentIndexNode, 2),
            },
        }]);
        assert_eq!(
            decode_rpc_diff_chunk(&encode_rpc_diff_chunk(&log_diff).unwrap()).unwrap(),
            log_diff
        );

        let logs = vec![(
            "events".to_owned(),
            LogMapEntry {
                log_id: LogId::from_bytes([1; 16]),
                first_seq: 0,
                end_seq: 1,
                segment_index_root: sample_ref(ObjectType::LogSegmentIndexNode, 2),
            },
        )];
        assert_eq!(
            decode_rpc_log_list(&encode_rpc_log_list(&logs).unwrap()).unwrap(),
            logs
        );

        let blob = crate::views::encode_artifact_blob(&[1, 2, 3, 4]).unwrap();
        let blob_ref = ObjectRef::from_encoded(&blob).unwrap();
        let arts = vec![(
            "a.txt".to_owned(),
            ArtifactDescriptor {
                artifact_id: ArtifactId::from_bytes([2; 16]),
                logical_len: 4,
                content_root: blob_ref,
                dependencies: vec![],
            },
        )];
        assert_eq!(
            decode_rpc_artifact_list(&encode_rpc_artifact_list(&arts).unwrap()).unwrap(),
            arts
        );

        let batch = encode_rpc_log_batch(
            LogId::from_bytes([1; 16]),
            0,
            1,
            &[b"rec".to_vec()],
            &[sample_ref(ObjectType::ArtifactBlob, 9)],
        )
        .unwrap();
        let (lid, first, end, recs, atts) = decode_rpc_log_batch(&batch).unwrap();
        assert_eq!(lid, LogId::from_bytes([1; 16]));
        assert_eq!((first, end), (0, 1));
        assert_eq!(recs, vec![b"rec".to_vec()]);
        assert_eq!(atts.len(), 1);

        // Error mappings
        for err in [
            ControlError::Fenced,
            ControlError::Expired,
            ControlError::GenerationExpired,
            ControlError::NotFound,
            ControlError::Conflict,
            ControlError::OperationMismatch,
            ControlError::Aborted,
            ControlError::InProgress,
            ControlError::InvalidRequest("bad"),
            ControlError::InvalidTransition,
            ControlError::CorruptRecord("bad".to_owned()),
            ControlError::Metadata(MetadataError::Backend("io".to_owned())),
        ] {
            let svc_err = ServiceError::from(err);
            let _ = svc_err.to_control_error();
        }
        let fmt_err = ServiceError::from(FormatError::InvalidEnvelope("bad"));
        assert_eq!(fmt_err.code, ServiceErrorCode::IntegrityError);
    }
}
