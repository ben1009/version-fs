//! Common-cut checkpoint capture coordinator and durable capture/freeze intent.
//!
//! Coordinates RFC-002 Section 6.4, 6.5, 8.2, and 8.3 checkpoints across a live
//! [`DurableVolume`], pluggable producer gates (for application, native log, and
//! artifact admission), and filesystem consistency boundaries (`block_cut`,
//! `filesystem_frozen`, and `filesystem_clean_unmount`).
//!
//! Capture and freeze intent is persisted outside the mounted volume so a
//! surviving coordinator or watchdog can reconcile unknown authority outcomes
//! before thawing or reopening admission, without inferring publication from a
//! local freeze flag or undoing already-committed drain barriers.

use std::cell::RefCell;
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use thiserror::Error;
use versionfs_core::control::{
    BranchState, CaptureState, ControlError, LiveRoots, OperationState, PreparationKind,
    PreparationRequest, PublishedGeneration, VerifiedGenerationSeed, WorkspaceControl, WriterGrant,
    decode_branch_operation_result,
};
use versionfs_core::fault::{FailureInjector, FailurePoint, NoFailures};
use versionfs_core::format::{
    CaptureMode, ENVELOPE_HEADER_BYTES, FormatError, MAX_ENCODED_OBJECT_BYTES,
    MAX_PROVENANCE_CONTEXT_BYTES, ObjectRef, ObjectType, ProvenanceBinding,
};
use versionfs_core::graph::DependencyProof;
use versionfs_core::identity::{
    AttachmentId, BranchId, GenerationId, ObjectId, ObjectStoreScopeId, OperationId, PrincipalId,
    ProtectionId, VolumeBoundaryId, WorkspaceId,
};
use versionfs_core::metadata::{MetadataAuthority, MetadataError};
use versionfs_core::persistent_map::{DurableObjectStore, MapObjectStore};

#[cfg(unix)]
use crate::barrier::restrict_journal_directory;
use crate::barrier::{
    BarrierError, BarrierJournal, BarrierJournalScope, DurableVolume, JournalReader,
    VolumeBarrierControl, atomic_replace, create_generation_with_reuse_and_provenance,
    decode_checked, decode_grant, decode_roots, encode_checked, encode_grant,
    encode_optional_operation_id, encode_ref, encode_roots, journal_io, push_u64,
    same_grant_identity, set_private_creation_mode, sync_directory, validate_attachment,
    validate_roots,
};
use crate::volume::BlockDevice;

const CAPTURE_IDS_MAGIC: &[u8; 8] = b"VFSCID01";
const CAPTURE_PENDING_MAGIC: &[u8; 8] = b"VFSCPN01";

/// Default preparation heartbeat interval (three minutes per RFC-002 Section 8.2).
pub const DEFAULT_RENEWAL_INTERVAL: Duration = Duration::from_secs(180);

/// Default number of full Step-1 restarts permitted on acquisition conflict.
pub const DEFAULT_MAX_ACQUISITION_RETRIES: usize = 3;

/// Fresh, never-reused identities allocated before a capture attempt begins.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CaptureIds {
    /// Stable operation ID for capture admission, publication, and reconciliation.
    pub capture_id: OperationId,
    /// Fresh protection ID for the capture preparation's dependency set.
    pub protection_id: ProtectionId,
    /// Opaque Generation identity allocated once for this attempt.
    pub generation_id: GenerationId,
    /// Opaque volume boundary identity recorded in the Generation descriptor.
    pub boundary_id: VolumeBoundaryId,
}

/// Durable lifecycle phase recorded in [`PendingCapture`] outside the volume.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum CapturePhase {
    /// Intent recorded before or during capture-token acquisition.
    Acquiring = 1,
    /// Capture token acquired; filesystem freeze is about to run or in progress.
    Freezing = 2,
    /// Requested boundary is established; draining the final volume barrier.
    BoundaryEstablished = 3,
    /// Block mutation admission closed; sealing the exact revision/root tuple.
    Sealing = 4,
    /// Capture sealed; uploading/verifying the descriptor and publishing.
    Publishing = 5,
    /// Publication committed; syncing local state and releasing freeze/admission.
    Finalizing = 6,
    /// Capture failed; resolving/aborting authority state and thawing if frozen.
    Aborting = 7,
}

impl TryFrom<u8> for CapturePhase {
    type Error = String;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Acquiring),
            2 => Ok(Self::Freezing),
            3 => Ok(Self::BoundaryEstablished),
            4 => Ok(Self::Sealing),
            5 => Ok(Self::Publishing),
            6 => Ok(Self::Finalizing),
            7 => Ok(Self::Aborting),
            other => Err(format!("unknown capture phase tag {other}")),
        }
    }
}

/// Exact local recovery record for one in-flight checkpoint capture.
///
/// Persisted outside the mounted volume before remote capture calls and before
/// calling filesystem freeze so watchdog/restart recovery can reconcile exact
/// authority outcomes before thawing or reopening admission.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PendingCapture {
    /// Workspace, authenticated caller, and immutable-object namespace.
    pub scope: BarrierJournalScope,
    /// Stable operation ID for this capture attempt.
    pub capture_id: OperationId,
    /// Fresh preparation protection ID for this capture attempt.
    pub protection_id: ProtectionId,
    /// Generation identity bound to this attempt.
    pub generation_id: GenerationId,
    /// Volume boundary identity recorded in the Generation descriptor.
    pub boundary_id: VolumeBoundaryId,
    /// Branch being captured.
    pub branch_id: BranchId,
    /// Writable attachment identity bound to this worker.
    pub attachment_id: AttachmentId,
    /// Volume fencing epoch bound to this worker.
    pub volume_epoch: u64,
    /// Original writer grant bound at acquisition. Recovery never adopts a new owner.
    pub writer_grant: WriterGrant,
    /// Exact capture consistency mode requested and established.
    pub mode: CaptureMode,
    /// Whether publication moves the branch head and attachment base (`true`) or stays detached.
    pub advance_head: bool,
    /// Immutable provenance binding fixed at capture admission.
    pub provenance: ProvenanceBinding,
    /// Informational Unix timestamp in milliseconds for the Generation descriptor.
    pub created_at_unix_ms: u64,
    /// Branch head read before draining producers.
    pub expected_parent: GenerationId,
    /// Exact branch revision read before draining producers.
    pub pre_drain_revision: u64,
    /// Exact live roots read before draining producers.
    pub pre_drain_roots: LiveRoots,
    /// Current durable coordinator phase.
    pub phase: CapturePhase,
    /// True from right before `freeze()` is invoked until `thaw()` succeeds.
    /// Always false for `block_cut` and `filesystem_clean_unmount`.
    pub freeze_active: bool,
    /// Stable operation ID reserved before issuing `seal_capture`.
    pub seal_operation_id: Option<OperationId>,
    /// Exact writer grant bound to `seal_operation_id`.
    pub seal_grant: Option<WriterGrant>,
    /// Revision sealed by `seal_capture`.
    pub sealed_revision: Option<u64>,
    /// Live roots sealed by `seal_capture`.
    pub sealed_roots: Option<LiveRoots>,
    /// Unresolved capture preparation-renewal operation ID.
    pub renewal_operation_id: Option<OperationId>,
    /// Exact writer grant bound to `renewal_operation_id`.
    pub renewal_grant: Option<WriterGrant>,
    /// Verified descriptor object uploaded prior to `publish_capture`.
    pub descriptor_ref: Option<ObjectRef>,
    /// Exact writer grant bound to `publish_capture`.
    pub publish_grant: Option<WriterGrant>,
    /// Branch revision observed once publication is confirmed committed.
    pub published_revision: Option<u64>,
}

/// Durable storage for capture IDs and in-flight capture/freeze intent.
///
/// Implementations must store this state outside the exported/mounted volume.
/// Every ID reservation and pending-record write must be durable before
/// returning. [`FileCaptureJournal`] is the reference local implementation.
pub trait CaptureJournal {
    /// Reserves fresh, never-reused identities for one capture attempt.
    fn reserve_capture_ids(&mut self) -> Result<CaptureIds, String>;

    /// Reserves a fresh, never-reused operation ID for sealing or renewal.
    fn reserve_operation_id(&mut self) -> Result<OperationId, String>;

    /// Loads the currently pending capture intent, if any.
    fn load_pending(&mut self) -> Result<Option<PendingCapture>, String>;

    /// Durably writes or updates the pending capture intent.
    fn persist_pending(&mut self, capture: &PendingCapture) -> Result<(), String>;

    /// Durably removes the pending capture intent once terminal and cleaned up.
    fn finish_pending(&mut self, capture_id: OperationId) -> Result<(), String>;
}

/// Crash-safe Unix file journal for one worker's capture and freeze intent.
///
/// Lives in a dedicated `0700` directory outside the mounted volume with `0600`
/// files, an exclusive OS file lock, SHA-256 checksums, and fsynced atomic
/// replacements.
pub struct FileCaptureJournal {
    directory: PathBuf,
    ids_path: PathBuf,
    pending_path: PathBuf,
    _lock: File,
    nonce: [u8; 8],
    next_counter: u64,
    failures: Arc<dyn FailureInjector>,
}

impl FileCaptureJournal {
    /// Opens or initializes one capture-intent journal directory.
    pub fn open(directory: impl AsRef<Path>) -> Result<Self, String> {
        Self::open_with(directory, Arc::new(NoFailures))
    }

    /// Opens a capture-intent journal with instance-scoped fault injection.
    pub fn open_with(
        directory: impl AsRef<Path>,
        failures: Arc<dyn FailureInjector>,
    ) -> Result<Self, String> {
        #[cfg(not(unix))]
        {
            let _ = (directory, failures);
            return Err(
                "FileCaptureJournal requires Unix private permissions; provide another CaptureJournal implementation".to_owned(),
            );
        }

        #[cfg(unix)]
        {
            Self::open_unix(directory.as_ref(), failures)
        }
    }

    #[cfg(unix)]
    fn open_unix(directory: &Path, failures: Arc<dyn FailureInjector>) -> Result<Self, String> {
        let directory = directory.to_path_buf();
        create_dir_all_durable(&directory)?;
        restrict_journal_directory(&directory)?;
        let ids_path = directory.join("capture-ids.v1");
        let pending_path = directory.join("pending-capture.v1");
        let lock_path = directory.join("capture.lock");
        let mut lock_options = OpenOptions::new();
        lock_options
            .create(true)
            .truncate(false)
            .read(true)
            .write(true);
        set_private_creation_mode(&mut lock_options, 0o600);
        let lock = lock_options
            .open(lock_path)
            .map_err(journal_io("open capture journal lock"))?;
        lock.try_lock()
            .map_err(|error| format!("capture journal is already in use: {error}"))?;

        let (nonce, next_counter) = match fs::read(&ids_path) {
            Ok(bytes) => decode_capture_ids(&bytes)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let mut nonce = [0; 8];
                getrandom::fill(&mut nonce)
                    .map_err(|error| format!("generate capture journal ID namespace: {error}"))?;
                let bytes = encode_capture_ids(nonce, 0);
                atomic_replace(&directory, &ids_path, &bytes, failures.as_ref())?;
                (nonce, 0)
            }
            Err(error) => return Err(format!("read capture journal ID state: {error}")),
        };

        Ok(Self {
            directory,
            ids_path,
            pending_path,
            _lock: lock,
            nonce,
            next_counter,
            failures,
        })
    }

    fn reserve_counters(&mut self, count: u64) -> Result<Vec<[u8; 16]>, String> {
        let end = self
            .next_counter
            .checked_add(count)
            .ok_or_else(|| "capture journal ID counter exhausted".to_owned())?;
        atomic_replace(
            &self.directory,
            &self.ids_path,
            &encode_capture_ids(self.nonce, end),
            self.failures.as_ref(),
        )?;
        let ids = (self.next_counter..end)
            .map(|counter| {
                let mut bytes = [0; 16];
                bytes[..8].copy_from_slice(&self.nonce);
                bytes[8..].copy_from_slice(&counter.to_le_bytes());
                bytes
            })
            .collect();
        self.next_counter = end;
        Ok(ids)
    }
}

impl CaptureJournal for FileCaptureJournal {
    fn reserve_capture_ids(&mut self) -> Result<CaptureIds, String> {
        let ids = self.reserve_counters(4)?;
        Ok(CaptureIds {
            capture_id: OperationId::from_bytes(ids[0]),
            protection_id: ProtectionId::from_bytes(ids[1]),
            generation_id: GenerationId::from_bytes(ids[2]),
            boundary_id: VolumeBoundaryId::from_bytes(ids[3]),
        })
    }

    fn reserve_operation_id(&mut self) -> Result<OperationId, String> {
        let [id] = self.reserve_counters(1)?.try_into().map_err(|_| {
            "capture journal reserved an unexpected number of operation IDs".to_owned()
        })?;
        Ok(OperationId::from_bytes(id))
    }

    fn load_pending(&mut self) -> Result<Option<PendingCapture>, String> {
        match fs::read(&self.pending_path) {
            Ok(bytes) => decode_pending_capture(&bytes).map(Some),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(format!("read pending capture journal: {error}")),
        }
    }

    fn persist_pending(&mut self, capture: &PendingCapture) -> Result<(), String> {
        validate_pending_capture(capture)?;
        if let Some(existing) = self.load_pending()? {
            if existing.capture_id != capture.capture_id {
                return Err("another capture is already pending in this journal".to_owned());
            }
            if existing.scope != capture.scope {
                return Err("cannot change a pending capture's workspace scope".to_owned());
            }
        }
        let bytes = encode_pending_capture(capture)?;
        atomic_replace(
            &self.directory,
            &self.pending_path,
            &bytes,
            self.failures.as_ref(),
        )
    }

    fn finish_pending(&mut self, capture_id: OperationId) -> Result<(), String> {
        match self.load_pending()? {
            Some(pending) if pending.capture_id != capture_id => {
                Err("cannot finish a different pending capture".to_owned())
            }
            None => Ok(()),
            Some(_) => {
                fs::remove_file(&self.pending_path)
                    .map_err(journal_io("remove terminal pending capture"))?;
                sync_directory(&self.directory)
            }
        }
    }
}

/// Authority operations required by the checkpoint capture coordinator.
pub trait CaptureControl: VolumeBarrierControl {
    /// Registers or replays a checkpoint capture preparation with explicit provenance.
    fn begin_capture(
        &self,
        request: PreparationRequest,
        provenance: ProvenanceBinding,
    ) -> Result<OperationState, ControlError>;

    /// Seals an open capture at the exact final revision after drain flushes finish.
    fn seal_capture(
        &self,
        operation_id: OperationId,
        capture_id: OperationId,
        expected_revision: u64,
        grant: WriterGrant,
    ) -> Result<CaptureState, ControlError>;

    /// Atomically publishes a Generation from a verified descriptor and sealed capture.
    #[allow(clippy::too_many_arguments)]
    fn publish_capture(
        &self,
        capture_id: OperationId,
        expected_revision: u64,
        expected_parent: GenerationId,
        grant: WriterGrant,
        attachment_id: AttachmentId,
        volume_epoch: u64,
        generation: VerifiedGenerationSeed,
        advance_head: bool,
    ) -> Result<PublishedGeneration, ControlError>;
}

impl<A: MetadataAuthority> CaptureControl for WorkspaceControl<A> {
    fn begin_capture(
        &self,
        request: PreparationRequest,
        provenance: ProvenanceBinding,
    ) -> Result<OperationState, ControlError> {
        WorkspaceControl::begin_preparation_with_provenance(self, request, provenance)
    }

    fn seal_capture(
        &self,
        operation_id: OperationId,
        capture_id: OperationId,
        expected_revision: u64,
        grant: WriterGrant,
    ) -> Result<CaptureState, ControlError> {
        WorkspaceControl::seal_capture(self, operation_id, capture_id, expected_revision, grant)
    }

    fn publish_capture(
        &self,
        capture_id: OperationId,
        expected_revision: u64,
        expected_parent: GenerationId,
        grant: WriterGrant,
        attachment_id: AttachmentId,
        volume_epoch: u64,
        generation: VerifiedGenerationSeed,
        advance_head: bool,
    ) -> Result<PublishedGeneration, ControlError> {
        WorkspaceControl::publish_capture(
            self,
            capture_id,
            expected_revision,
            expected_parent,
            grant,
            attachment_id,
            volume_epoch,
            generation,
            advance_head,
        )
    }
}

/// Admission and drain hook for application commands, native logs, and artifacts.
///
/// P8 connects native log and artifact staging admission to this same hook so
/// every view participates in one common-cut protocol.
pub trait ProducerGate {
    /// Stops admitting new local mutations before the pre-drain authority read.
    fn close_admission(&mut self) -> Result<(), String>;

    /// Waits for already-admitted producers to complete before capture-token CAS.
    fn drain_producers(&mut self) -> Result<(), String>;

    /// Reopens producer admission after publication, definite abort, or restart.
    ///
    /// Implementations must be idempotent when admission is already open so
    /// crash or detached watchdog recovery can safely repeat cleanup after a
    /// partial local failure (for example, if clearing the capture journal fails
    /// after admission was already reopened).
    fn reopen_admission(&mut self) -> Result<(), String>;
}

/// Producer gate for volume-only callers with no separate local producers.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopProducerGate;

impl ProducerGate for NoopProducerGate {
    fn close_admission(&mut self) -> Result<(), String> {
        Ok(())
    }

    fn drain_producers(&mut self) -> Result<(), String> {
        Ok(())
    }

    fn reopen_admission(&mut self) -> Result<(), String> {
        Ok(())
    }
}

/// Filesystem boundary controller for `filesystem_frozen` and `filesystem_clean_unmount`.
///
/// Both `freeze` and `clean_unmount` receive the live [`BlockDevice`] so the
/// filesystem can flush dirty buffers and journal records as authorized
/// capture-child barriers without deadlocking on a closed device gate.
pub trait FilesystemBoundary {
    /// Freezes the mounted reference filesystem while keeping block service alive.
    fn freeze(&mut self, device: &mut dyn BlockDevice) -> Result<(), String>;

    /// Thaws a filesystem frozen by this coordinator or recovered from durable
    /// freeze intent. Must be safe to call when recovering an uncertain freeze.
    fn thaw(&mut self) -> Result<(), String>;

    /// Cleanly unmounts all mounts of the reference filesystem while keeping
    /// block service alive. Lazy or forced unmounts must return an error.
    fn clean_unmount(&mut self, device: &mut dyn BlockDevice) -> Result<(), String>;
}

/// Boundary controller that supports only raw `block_cut` checkpoints.
#[derive(Clone, Copy, Debug, Default)]
pub struct BlockCutOnlyBoundary;

impl FilesystemBoundary for BlockCutOnlyBoundary {
    fn freeze(&mut self, _device: &mut dyn BlockDevice) -> Result<(), String> {
        Err("filesystem_frozen is unavailable on a block-cut-only boundary".to_owned())
    }

    fn thaw(&mut self) -> Result<(), String> {
        Ok(())
    }

    fn clean_unmount(&mut self, _device: &mut dyn BlockDevice) -> Result<(), String> {
        Err("filesystem_clean_unmount is unavailable on a block-cut-only boundary".to_owned())
    }
}

/// Parameters for one checkpoint capture request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CheckpointRequest {
    /// Requested Section 6.4 consistency boundary; only this mode may be persisted.
    pub mode: CaptureMode,
    /// `true` advances branch head and attachment base; `false` publishes detached.
    pub advance_head: bool,
    /// Explicit provenance binding fixed at capture admission.
    pub provenance: ProvenanceBinding,
    /// Informational Unix timestamp in milliseconds stored in the descriptor.
    pub created_at_unix_ms: u64,
    /// Extra immutable dependencies to protect during preparation (context root is added
    /// automatically).
    pub dependencies: Vec<ObjectRef>,
}

impl CheckpointRequest {
    /// Creates an unattributed attached checkpoint request for the selected mode.
    pub const fn attached(mode: CaptureMode, created_at_unix_ms: u64) -> Self {
        Self {
            mode,
            advance_head: true,
            provenance: ProvenanceBinding::Unattributed,
            created_at_unix_ms,
            dependencies: Vec::new(),
        }
    }

    /// Creates an unattributed detached checkpoint request for the selected mode.
    pub const fn detached(mode: CaptureMode, created_at_unix_ms: u64) -> Self {
        Self {
            mode,
            advance_head: false,
            provenance: ProvenanceBinding::Unattributed,
            created_at_unix_ms,
            dependencies: Vec::new(),
        }
    }
}

/// Configuration knobs for [`CheckpointCoordinator`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CaptureConfig {
    /// Maximum full Step-1 restarts on definite capture-token acquisition conflicts.
    pub max_acquisition_retries: usize,
    /// Interval between capture preparation renewals during remote object work.
    pub renewal_interval: Duration,
}

impl Default for CaptureConfig {
    fn default() -> Self {
        Self {
            max_acquisition_retries: DEFAULT_MAX_ACQUISITION_RETRIES,
            renewal_interval: DEFAULT_RENEWAL_INTERVAL,
        }
    }
}

/// Outcome returned by watchdog or coordinator crash recovery.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RecoveredCapture {
    /// No pending capture intent existed in the journal.
    None,
    /// The capture had already committed on the authority; local state was reconciled.
    Published(Box<PublishedGeneration>),
    /// The capture did not commit and was definitively aborted; any freeze intent was thawed.
    Aborted {
        /// Operation ID of the aborted capture attempt.
        capture_id: OperationId,
        /// Whether recovery invoked `thaw()` to release durable freeze intent.
        freeze_thawed: bool,
    },
}

/// Errors returned by [`CheckpointCoordinator`].
#[derive(Debug, Error)]
pub enum CaptureError {
    /// A pending capture intent already exists and must be reconciled first.
    #[error("a previous checkpoint capture is unresolved and must be reconciled first")]
    PendingCaptureUnresolved,
    /// A durable block barrier is unresolved on the volume.
    #[error("an unresolved durable block barrier blocks capture admission")]
    BarrierUnresolved,
    /// The worker's original writer grant, attachment, or volume epoch was fenced.
    #[error("worker attachment or writer grant was fenced")]
    Fenced,
    /// Pre-drain or sealing preconditions conflicted beyond the retry budget.
    #[error("checkpoint capture conflicted with a concurrent branch mutation")]
    Conflict,
    /// The capture preparation was durably aborted.
    #[error("checkpoint capture was aborted")]
    Aborted,
    /// Establishing `filesystem_frozen` failed; no Generation was published.
    #[error("filesystem freeze failed: {0}")]
    FreezeFailed(String),
    /// Releasing a frozen filesystem failed; freeze intent remains durable for retry.
    #[error("filesystem thaw failed: {0}")]
    ThawFailed(String),
    /// Establishing `filesystem_clean_unmount` failed; no Generation was published.
    #[error("filesystem clean unmount failed: {0}")]
    CleanUnmountFailed(String),
    /// The local producer admission/drain hook failed.
    #[error("producer gate failed: {0}")]
    Producer(String),
    /// The durable capture-intent journal failed.
    #[error("capture intent journal failed: {0}")]
    Journal(String),
    /// The pending capture record belongs to another workspace, caller, or object namespace.
    #[error("pending capture belongs to another workspace, principal, or object-store namespace")]
    JournalScopeMismatch,
    /// The authority rejected or could not resolve a capture step.
    #[error(transparent)]
    Control(#[from] ControlError),
    /// A barrier, descriptor build, or volume synchronization step failed.
    #[error(transparent)]
    Barrier(#[from] BarrierError),
    /// Request or provenance format validation failed.
    #[error(transparent)]
    Format(#[from] FormatError),
    /// An injected test failure fired at a named protocol boundary.
    #[error("injected capture failure at {0:?}")]
    InjectedFailure(FailurePoint),
    /// The coordinator or volume was in an invalid state for the requested action.
    #[error("invalid capture state: {0}")]
    InvalidState(&'static str),
}

/// Coordinates RFC-002 common-cut checkpoints and crash/watchdog recovery.
pub struct CheckpointCoordinator<J> {
    journal: J,
    config: CaptureConfig,
    failures: Arc<dyn FailureInjector>,
    last_renewal: Option<Instant>,
    last_renewal_attempt: Option<Instant>,
}

impl<J: CaptureJournal> CheckpointCoordinator<J> {
    /// Creates a coordinator backed by a durable capture-intent journal outside the volume.
    pub fn new(journal: J, config: CaptureConfig) -> Self {
        Self::with_failures(journal, config, Arc::new(NoFailures))
    }

    /// Creates a coordinator with deterministic protocol-boundary fault injection.
    pub fn with_failures(
        journal: J,
        config: CaptureConfig,
        failures: Arc<dyn FailureInjector>,
    ) -> Self {
        Self {
            journal,
            config,
            failures,
            last_renewal: None,
            last_renewal_attempt: None,
        }
    }

    /// Returns immutable access to the underlying capture journal.
    pub const fn journal(&self) -> &J {
        &self.journal
    }

    /// Loads any unresolved capture intent recorded outside the volume.
    pub fn pending_capture(&mut self) -> Result<Option<PendingCapture>, CaptureError> {
        self.journal.load_pending().map_err(CaptureError::Journal)
    }

    /// Executes the full RFC-002 Section 6.5 / 8.2 / 8.3 checkpoint capture protocol.
    ///
    /// Persists only `request.mode`, never refreshes stale pre-drain roots on
    /// acquisition conflict (instead restarting Step 1 with a fresh operation ID
    /// and drain), keeps block I/O active during freeze or clean unmount, and
    /// syncs local volume state before thawing or reopening admission.
    pub fn checkpoint<S, C, BJ, P, B>(
        &mut self,
        volume: &mut DurableVolume<S, C, BJ>,
        producers: &mut P,
        boundary: &mut B,
        request: &CheckpointRequest,
        protected_proofs: &[&DependencyProof],
    ) -> Result<PublishedGeneration, CaptureError>
    where
        S: DurableObjectStore,
        C: CaptureControl,
        BJ: BarrierJournal,
        P: ProducerGate,
        B: FilesystemBoundary,
    {
        if self
            .journal
            .load_pending()
            .map_err(CaptureError::Journal)?
            .is_some()
        {
            return Err(CaptureError::PendingCaptureUnresolved);
        }
        if volume.is_fenced() {
            return Err(CaptureError::Fenced);
        }
        if volume.has_pending_barrier() {
            return Err(CaptureError::BarrierUnresolved);
        }
        if volume.mutations_closed() {
            return Err(CaptureError::InvalidState(
                "block mutation admission is already closed",
            ));
        }
        validate_provenance_binding(request.provenance)?;

        let original_grant = volume.writer_grant();
        let branch_id = volume.branch_id();
        let attachment_id = volume.attachment_id();
        let volume_epoch = volume.volume_epoch();
        let scope = volume.journal_scope();

        // Step 1: Close local admission, read authoritative revision/live roots
        // together, drain producers, and acquire the capture token against those
        // exact pre-drain preconditions. Any definite conflict restarts Step 1
        // with a new operation ID and fresh drain.
        let (mut pending, pre_drain_branch) = self.acquire_with_restarts(
            volume,
            producers,
            request,
            scope,
            branch_id,
            attachment_id,
            volume_epoch,
            original_grant,
        )?;
        self.last_renewal = Some(Instant::now());

        // Sync non-dirty metadata (e.g. revision/native roots) before Step 2
        // without discarding local dirty extents.
        if let Err(error) = volume.sync_branch_state(&pre_drain_branch) {
            return self.abort_and_return_error(
                volume,
                producers,
                boundary,
                &mut pending,
                CaptureError::Barrier(error),
            );
        }
        let barriers_before = volume.committed_barriers();

        // Step 2: Establish the requested Section 6.4 boundary while keeping
        // block service alive so freeze/unmount can flush dirty/journal blocks
        // through authorized capture-child barriers.
        match request.mode {
            CaptureMode::BlockCut => {
                pending.phase = CapturePhase::BoundaryEstablished;
                if let Err(error) = self.persist(&pending) {
                    return self.abort_and_return_error(
                        volume,
                        producers,
                        boundary,
                        &mut pending,
                        error,
                    );
                }
            }
            CaptureMode::FilesystemFrozen => {
                if let Err(injected) = self.failures.check(FailurePoint::FilesystemFreezeBefore) {
                    return self.abort_and_return_error(
                        volume,
                        producers,
                        boundary,
                        &mut pending,
                        CaptureError::FreezeFailed(injected.to_string()),
                    );
                }
                pending.phase = CapturePhase::Freezing;
                pending.freeze_active = true;
                if let Err(error) = self.persist(&pending) {
                    pending.freeze_active = false;
                    return self.abort_and_return_error(
                        volume,
                        producers,
                        boundary,
                        &mut pending,
                        error,
                    );
                }
                if let Err(message) = boundary.freeze(volume) {
                    return self.abort_and_return_error(
                        volume,
                        producers,
                        boundary,
                        &mut pending,
                        CaptureError::FreezeFailed(message),
                    );
                }
                pending.phase = CapturePhase::BoundaryEstablished;
                if let Err(error) = self.persist(&pending) {
                    return self.abort_and_return_error(
                        volume,
                        producers,
                        boundary,
                        &mut pending,
                        error,
                    );
                }
                if let Err(injected) = self.failures.check(FailurePoint::FilesystemFreezeAfter) {
                    // Simulate coordinator interruption after freeze succeeded:
                    // keep durable freeze_active intent intact for watchdog recovery.
                    return Err(CaptureError::InjectedFailure(injected.0));
                }
            }
            CaptureMode::FilesystemCleanUnmount => {
                if let Err(message) = boundary.clean_unmount(volume) {
                    return self.abort_and_return_error(
                        volume,
                        producers,
                        boundary,
                        &mut pending,
                        CaptureError::CleanUnmountFailed(message),
                    );
                }
                pending.phase = CapturePhase::BoundaryEstablished;
                if let Err(error) = self.persist(&pending) {
                    return self.abort_and_return_error(
                        volume,
                        producers,
                        boundary,
                        &mut pending,
                        error,
                    );
                }
            }
        }

        // Issue the final durable child barrier, then close block mutation admission.
        if let Err(error) = volume.flush() {
            if is_unknown_barrier_error(&error) {
                // Leave capture and freeze intent durable for explicit reconciliation.
                return Err(CaptureError::Barrier(error));
            }
            let capture_error = if matches!(error, BarrierError::Fenced) {
                CaptureError::Fenced
            } else {
                CaptureError::Barrier(error)
            };
            return self.abort_and_return_error(
                volume,
                producers,
                boundary,
                &mut pending,
                capture_error,
            );
        }
        volume.set_mutations_closed(true);

        // Step 3: Track only authorized child barrier commits and seal the exact
        // revision and live roots.
        let child_commits = volume.committed_barriers().saturating_sub(barriers_before);
        let Some(expected_sealed_revision) = pending.pre_drain_revision.checked_add(child_commits)
        else {
            return self.abort_and_return_error(
                volume,
                producers,
                boundary,
                &mut pending,
                CaptureError::Conflict,
            );
        };
        let expected_sealed_roots = volume.live_roots();
        if volume.revision() != expected_sealed_revision
            || expected_sealed_roots.logs != pending.pre_drain_roots.logs
            || expected_sealed_roots.artifacts != pending.pre_drain_roots.artifacts
        {
            return self.abort_and_return_error(
                volume,
                producers,
                boundary,
                &mut pending,
                CaptureError::Conflict,
            );
        }

        let sealed = match self.seal_with_grant_refresh(
            volume,
            &mut pending,
            expected_sealed_revision,
            original_grant,
        ) {
            Ok(state) => state,
            Err(error) if is_unknown_capture_error(&error) => {
                // Keep freeze & mutation gate closed until reconciled.
                return Err(error);
            }
            Err(error) => {
                return self.abort_and_return_error(
                    volume,
                    producers,
                    boundary,
                    &mut pending,
                    error,
                );
            }
        };
        if !sealed.sealed
            || sealed.sealed_revision != Some(expected_sealed_revision)
            || sealed.sealed_roots != Some(expected_sealed_roots)
        {
            return self.abort_and_return_error(
                volume,
                producers,
                boundary,
                &mut pending,
                CaptureError::Conflict,
            );
        }
        pending.phase = CapturePhase::Publishing;
        pending.sealed_revision = sealed.sealed_revision;
        pending.sealed_roots = sealed.sealed_roots;
        if let Err(error) = self.persist(&pending) {
            return self.abort_and_return_error(volume, producers, boundary, &mut pending, error);
        }

        // Step 4: Renew capture preparation if due (and throughout descriptor
        // upload and dependency-graph verification), then build, durably store,
        // and verify the complete Generation descriptor.
        if let Err(error) = self.renew_if_due(volume, &mut pending) {
            if is_unknown_capture_error(&error) {
                return Err(error);
            }
            return self.abort_and_return_error(volume, producers, boundary, &mut pending, error);
        }
        let verified = match self.create_verified_generation_with_renewal(
            volume,
            &mut pending,
            expected_sealed_roots,
            protected_proofs,
        ) {
            Ok(seed) => seed,
            Err(error) if is_unknown_capture_error(&error) => {
                // Pause object work and publication until the unresolved
                // renewal operation ID is reconciled.
                return Err(error);
            }
            Err(error) => {
                return self.abort_and_return_error(
                    volume,
                    producers,
                    boundary,
                    &mut pending,
                    error,
                );
            }
        };

        // Step 5: Publish atomically (retrying once if an independent writer
        // heartbeat refreshed the same grant), then sync local volume baseline
        // before thaw/reopen.
        let published = match self.publish_with_grant_refresh(
            volume,
            &mut pending,
            expected_sealed_revision,
            &verified,
            original_grant,
        ) {
            Ok(published) => published,
            Err(error) if is_unknown_capture_error(&error) => {
                // Keep freeze active and mutations closed until reconciled.
                return Err(error);
            }
            Err(error) => {
                return self.abort_and_return_error(
                    volume,
                    producers,
                    boundary,
                    &mut pending,
                    error,
                );
            }
        };

        pending.phase = CapturePhase::Finalizing;
        pending.published_revision = Some(published.branch.revision);
        self.persist(&pending)?;
        if let Err(injected) = self.failures.check(FailurePoint::ReplyBeforeSend) {
            // Simulate interruption after publication committed but before local
            // baseline sync and thaw. Watchdog recovery completes both safely.
            return Err(CaptureError::InjectedFailure(injected.0));
        }

        self.finalize_published(volume, producers, boundary, &mut pending, published)
    }

    /// Reconciles an interrupted capture or freeze intent after a coordinator
    /// crash, watchdog wake-up, or unknown authority outcome.
    ///
    /// Queries the authoritative operation outcome before thawing or reopening
    /// admission. If publication committed, syncs local state (or marks a fenced
    /// volume fenced without hot-rebasing) and returns the original
    /// [`PublishedGeneration`]. Otherwise aborts the capture preparation without
    /// undoing any already-committed child barrier, thaws if `freeze_active`,
    /// and clears the capture intent.
    pub fn recover_pending_capture<S, C, BJ, P, B>(
        &mut self,
        volume: &mut DurableVolume<S, C, BJ>,
        producers: &mut P,
        boundary: &mut B,
    ) -> Result<RecoveredCapture, CaptureError>
    where
        S: DurableObjectStore,
        C: CaptureControl,
        BJ: BarrierJournal,
        P: ProducerGate,
        B: FilesystemBoundary,
    {
        let Some(mut pending) = self.journal.load_pending().map_err(CaptureError::Journal)? else {
            return Ok(RecoveredCapture::None);
        };
        if pending.scope != volume.journal_scope() {
            return Err(CaptureError::JournalScopeMismatch);
        }
        if pending.branch_id != volume.branch_id()
            || pending.attachment_id != volume.attachment_id()
            || pending.volume_epoch != volume.volume_epoch()
            || !same_grant_identity(pending.writer_grant, volume.writer_grant())
        {
            return Err(CaptureError::Fenced);
        }

        // Reconcile the exact authority outcome first (unblocking any toy-kv
        // pending-operation gate). Never infer publication from `freeze_active`.
        match volume.control().get_operation(pending.capture_id) {
            Ok(OperationState::Committed(branch_bytes)) => {
                let committed_branch = decode_branch_operation_result(&branch_bytes)?;
                let published = PublishedGeneration {
                    generation_id: pending.generation_id,
                    branch: committed_branch,
                    head_advanced: pending.advance_head,
                };
                let published =
                    self.finalize_published(volume, producers, boundary, &mut pending, published)?;
                Ok(RecoveredCapture::Published(Box::new(published)))
            }
            Ok(OperationState::Aborted) => {
                let freeze_thawed =
                    self.finish_aborted_recovery(volume, producers, boundary, &mut pending)?;
                Ok(RecoveredCapture::Aborted {
                    capture_id: pending.capture_id,
                    freeze_thawed,
                })
            }
            Ok(OperationState::Preparing { .. }) | Err(ControlError::NotFound) => {
                // First resolve any pending capture-child barrier so abort_operation
                // on the parent capture does not hit `Busy`, while preserving any
                // child barrier that already committed.
                self.resolve_pending_child_for_abort(volume, &pending)?;
                match volume.control().abort_operation(pending.capture_id)? {
                    OperationState::Committed(branch_bytes) => {
                        let committed_branch = decode_branch_operation_result(&branch_bytes)?;
                        let published = PublishedGeneration {
                            generation_id: pending.generation_id,
                            branch: committed_branch,
                            head_advanced: pending.advance_head,
                        };
                        let published = self.finalize_published(
                            volume,
                            producers,
                            boundary,
                            &mut pending,
                            published,
                        )?;
                        Ok(RecoveredCapture::Published(Box::new(published)))
                    }
                    OperationState::Aborted => {
                        let freeze_thawed = self.finish_aborted_recovery(
                            volume,
                            producers,
                            boundary,
                            &mut pending,
                        )?;
                        Ok(RecoveredCapture::Aborted {
                            capture_id: pending.capture_id,
                            freeze_thawed,
                        })
                    }
                    OperationState::Preparing { .. } => Err(CaptureError::Conflict),
                }
            }
            Err(error) => Err(CaptureError::Control(error)),
        }
    }

    /// Reconciles an interrupted capture or freeze intent without requiring the
    /// original [`DurableVolume`] handle.
    ///
    /// Use this watchdog entry point after worker loss and writer takeover when
    /// the original attachment can no longer be reopened via
    /// [`DurableVolume::open`]. It verifies the caller's workspace, principal,
    /// and object-store scope against the journal, reconciles or aborts the
    /// capture operation on the authority, thaws `boundary` if `freeze_active`,
    /// reopens `producers`, and clears the capture journal without mutating or
    /// rebasing any replacement volume.
    pub fn recover_pending_capture_detached<C, P, B>(
        &mut self,
        control: &C,
        producers: &mut P,
        boundary: &mut B,
    ) -> Result<RecoveredCapture, CaptureError>
    where
        C: CaptureControl,
        P: ProducerGate,
        B: FilesystemBoundary,
    {
        let Some(mut pending) = self.journal.load_pending().map_err(CaptureError::Journal)? else {
            return Ok(RecoveredCapture::None);
        };
        if pending.scope.workspace_id != control.workspace_id()
            || pending.scope.principal_id != control.principal_id()
            || pending.scope.object_store_scope != control.object_store_scope()
        {
            return Err(CaptureError::JournalScopeMismatch);
        }

        let finish_detached_abort = |coordinator: &mut Self,
                                     producers: &mut P,
                                     boundary: &mut B,
                                     pending: &mut PendingCapture|
         -> Result<RecoveredCapture, CaptureError> {
            let mut freeze_thawed = false;
            if pending.freeze_active {
                boundary.thaw().map_err(CaptureError::ThawFailed)?;
                pending.freeze_active = false;
                freeze_thawed = true;
            }
            producers
                .reopen_admission()
                .map_err(CaptureError::Producer)?;
            coordinator
                .journal
                .finish_pending(pending.capture_id)
                .map_err(CaptureError::Journal)?;
            coordinator.last_renewal = None;
            coordinator.last_renewal_attempt = None;
            Ok(RecoveredCapture::Aborted {
                capture_id: pending.capture_id,
                freeze_thawed,
            })
        };

        let finish_detached_publish = |coordinator: &mut Self,
                                       producers: &mut P,
                                       boundary: &mut B,
                                       pending: &mut PendingCapture,
                                       branch_bytes: &[u8]|
         -> Result<RecoveredCapture, CaptureError> {
            let committed_branch = decode_branch_operation_result(branch_bytes)?;
            let published = PublishedGeneration {
                generation_id: pending.generation_id,
                branch: committed_branch,
                head_advanced: pending.advance_head,
            };
            if pending.freeze_active {
                boundary.thaw().map_err(CaptureError::ThawFailed)?;
                pending.freeze_active = false;
            }
            producers
                .reopen_admission()
                .map_err(CaptureError::Producer)?;
            coordinator
                .journal
                .finish_pending(pending.capture_id)
                .map_err(CaptureError::Journal)?;
            coordinator.last_renewal = None;
            coordinator.last_renewal_attempt = None;
            Ok(RecoveredCapture::Published(Box::new(published)))
        };

        match control.get_operation(pending.capture_id) {
            Ok(OperationState::Committed(branch_bytes)) => {
                finish_detached_publish(self, producers, boundary, &mut pending, &branch_bytes)
            }
            Ok(OperationState::Aborted) => {
                finish_detached_abort(self, producers, boundary, &mut pending)
            }
            Ok(OperationState::Preparing { .. }) | Err(ControlError::NotFound) => {
                let authority_child = match control.get_branch(pending.branch_id) {
                    Ok(branch) => branch
                        .capture
                        .filter(|capture| capture.operation_id == pending.capture_id)
                        .and_then(|capture| capture.pending_child),
                    Err(ControlError::NotFound) => None,
                    Err(error) => return Err(CaptureError::Control(error)),
                };
                if let Some(child_id) = authority_child {
                    self.abort_uncommitted_child_operation(control, child_id)?;
                }
                match control.abort_operation(pending.capture_id)? {
                    OperationState::Committed(branch_bytes) => finish_detached_publish(
                        self,
                        producers,
                        boundary,
                        &mut pending,
                        &branch_bytes,
                    ),
                    OperationState::Aborted => {
                        finish_detached_abort(self, producers, boundary, &mut pending)
                    }
                    OperationState::Preparing { .. } => Err(CaptureError::Conflict),
                }
            }
            Err(error) => Err(CaptureError::Control(error)),
        }
    }

    /// Resumes an interrupted capture that already reached `Publishing` with a
    /// sealed tuple and verified descriptor, or reconciles a committed outcome.
    ///
    /// If the capture already committed, finalizes local state and returns
    /// [`RecoveredCapture::Published`]. If it is still preparing and sealed,
    /// verifies the stored descriptor and replays `publish_capture` using the
    /// original `capture_id` and `publish_grant`. Any earlier phase or fencing
    /// falls back to [`Self::recover_pending_capture`].
    pub fn resume_pending_capture<S, C, BJ, P, B>(
        &mut self,
        volume: &mut DurableVolume<S, C, BJ>,
        producers: &mut P,
        boundary: &mut B,
        protected_proofs: &[&DependencyProof],
    ) -> Result<RecoveredCapture, CaptureError>
    where
        S: DurableObjectStore,
        C: CaptureControl,
        BJ: BarrierJournal,
        P: ProducerGate,
        B: FilesystemBoundary,
    {
        let Some(mut pending) = self.journal.load_pending().map_err(CaptureError::Journal)? else {
            return Ok(RecoveredCapture::None);
        };
        if pending.scope != volume.journal_scope() {
            return Err(CaptureError::JournalScopeMismatch);
        }
        if pending.branch_id != volume.branch_id()
            || pending.attachment_id != volume.attachment_id()
            || pending.volume_epoch != volume.volume_epoch()
            || !same_grant_identity(pending.writer_grant, volume.writer_grant())
        {
            return Err(CaptureError::Fenced);
        }

        if pending.phase != CapturePhase::Publishing {
            return self.recover_pending_capture(volume, producers, boundary);
        }

        match volume.control().get_operation(pending.capture_id) {
            Ok(OperationState::Committed(branch_bytes)) => {
                let committed_branch = decode_branch_operation_result(&branch_bytes)?;
                let published = PublishedGeneration {
                    generation_id: pending.generation_id,
                    branch: committed_branch,
                    head_advanced: pending.advance_head,
                };
                let published =
                    self.finalize_published(volume, producers, boundary, &mut pending, published)?;
                return Ok(RecoveredCapture::Published(Box::new(published)));
            }
            Ok(OperationState::Preparing { .. }) => {}
            Ok(OperationState::Aborted) | Err(ControlError::NotFound) => {
                return self.recover_pending_capture(volume, producers, boundary);
            }
            Err(error) => return Err(CaptureError::Control(error)),
        }

        let (Some(sealed_revision), Some(sealed_roots)) =
            (pending.sealed_revision, pending.sealed_roots)
        else {
            return self.recover_pending_capture(volume, producers, boundary);
        };

        volume.set_mutations_closed(true);

        if let Err(error) = self.renew_if_due(volume, &mut pending) {
            if is_unknown_capture_error(&error) {
                return Err(error);
            }
            return self.recover_pending_capture(volume, producers, boundary);
        }

        let verified = match self.create_verified_generation_with_renewal(
            volume,
            &mut pending,
            sealed_roots,
            protected_proofs,
        ) {
            Ok(seed) => seed,
            Err(error) if is_unknown_capture_error(&error) => return Err(error),
            Err(_) => return self.recover_pending_capture(volume, producers, boundary),
        };
        if pending
            .descriptor_ref
            .is_some_and(|existing| existing != verified.seed().descriptor)
        {
            return self.recover_pending_capture(volume, producers, boundary);
        }

        let original_grant = pending.writer_grant;
        match self.publish_with_grant_refresh(
            volume,
            &mut pending,
            sealed_revision,
            &verified,
            original_grant,
        ) {
            Ok(published) => {
                pending.phase = CapturePhase::Finalizing;
                pending.published_revision = Some(published.branch.revision);
                self.persist(&pending)?;
                let published =
                    self.finalize_published(volume, producers, boundary, &mut pending, published)?;
                Ok(RecoveredCapture::Published(Box::new(published)))
            }
            Err(error) if is_unknown_capture_error(&error) => Err(error),
            Err(_) => self.recover_pending_capture(volume, producers, boundary),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn acquire_with_restarts<S, C, BJ, P>(
        &mut self,
        volume: &mut DurableVolume<S, C, BJ>,
        producers: &mut P,
        request: &CheckpointRequest,
        scope: BarrierJournalScope,
        branch_id: BranchId,
        attachment_id: AttachmentId,
        volume_epoch: u64,
        original_grant: WriterGrant,
    ) -> Result<(PendingCapture, BranchState), CaptureError>
    where
        S: DurableObjectStore,
        C: CaptureControl,
        BJ: BarrierJournal,
        P: ProducerGate,
    {
        for attempt in 0..=self.config.max_acquisition_retries {
            producers
                .close_admission()
                .map_err(CaptureError::Producer)?;

            let pre_drain = match volume.control().get_branch(branch_id) {
                Ok(branch) => branch,
                Err(ControlError::NotFound) => {
                    volume.mark_fenced();
                    let _ = producers.reopen_admission();
                    return Err(CaptureError::Fenced);
                }
                Err(error) => {
                    let _ = producers.reopen_admission();
                    return Err(CaptureError::Control(error));
                }
            };
            let current_grant = match validate_attachment(
                &pre_drain,
                branch_id,
                attachment_id,
                volume_epoch,
                original_grant,
            ) {
                Ok(grant) => grant,
                Err(BarrierError::Fenced) => {
                    volume.mark_fenced();
                    let _ = producers.reopen_admission();
                    return Err(CaptureError::Fenced);
                }
                Err(error) => {
                    let _ = producers.reopen_admission();
                    return Err(CaptureError::Barrier(error));
                }
            };
            if pre_drain.capture.is_some() {
                let _ = producers.reopen_admission();
                return Err(CaptureError::Control(ControlError::Busy));
            }

            let pre_drain_roots = live_roots_of(&pre_drain);
            if let Err(message) = producers.drain_producers() {
                let _ = producers.reopen_admission();
                return Err(CaptureError::Producer(message));
            }

            let ids = match self.journal.reserve_capture_ids() {
                Ok(ids) => ids,
                Err(message) => {
                    let _ = producers.reopen_admission();
                    return Err(CaptureError::Journal(message));
                }
            };
            let pending = PendingCapture {
                scope,
                capture_id: ids.capture_id,
                protection_id: ids.protection_id,
                generation_id: ids.generation_id,
                boundary_id: ids.boundary_id,
                branch_id,
                attachment_id,
                volume_epoch,
                writer_grant: current_grant,
                mode: request.mode,
                advance_head: request.advance_head,
                provenance: request.provenance,
                created_at_unix_ms: request.created_at_unix_ms,
                expected_parent: pre_drain.head,
                pre_drain_revision: pre_drain.revision,
                pre_drain_roots,
                phase: CapturePhase::Acquiring,
                freeze_active: false,
                seal_operation_id: None,
                seal_grant: None,
                sealed_revision: None,
                sealed_roots: None,
                renewal_operation_id: None,
                renewal_grant: None,
                descriptor_ref: None,
                publish_grant: None,
                published_revision: None,
            };
            if let Err(error) = self.persist(&pending) {
                let _ = producers.reopen_admission();
                return Err(error);
            }

            let prep_request = PreparationRequest {
                operation_id: ids.capture_id,
                branch_id,
                expected_revision: pre_drain.revision,
                grant: current_grant,
                attachment_id,
                volume_epoch,
                protection_id: ids.protection_id,
                kind: PreparationKind::CheckpointCapture,
                expected_roots: None,
                dependencies: request.dependencies.clone(),
            };

            let acquired = self.begin_capture_with_reconciliation(
                volume.control(),
                &prep_request,
                request.provenance,
            );
            match acquired {
                Ok(()) => return Ok((pending, pre_drain)),
                Err(CaptureError::Control(ControlError::Conflict)) => {
                    // Definite conflict: tombstone this attempt's operation ID,
                    // reopen admission before clearing local intent, and restart
                    // Step 1 with a new capture ID and a fresh drain. Never rebase.
                    match volume.control().abort_operation(ids.capture_id) {
                        Ok(OperationState::Aborted) => {}
                        Ok(_) => return Err(CaptureError::Conflict),
                        Err(error) => return Err(CaptureError::Control(error)),
                    }
                    producers
                        .reopen_admission()
                        .map_err(CaptureError::Producer)?;
                    self.journal
                        .finish_pending(ids.capture_id)
                        .map_err(CaptureError::Journal)?;
                    if attempt == self.config.max_acquisition_retries {
                        return Err(CaptureError::Conflict);
                    }
                }
                Err(error) if is_unknown_capture_error(&error) => {
                    // Preserve journaled capture_id so recovery reconciles it.
                    return Err(error);
                }
                Err(error) => {
                    let mut final_error = error;
                    if matches!(&final_error, CaptureError::Control(ControlError::Fenced)) {
                        match volume.control().check_attachment_live(
                            branch_id,
                            attachment_id,
                            volume_epoch,
                            original_grant,
                        ) {
                            Ok(refreshed_grant)
                                if refreshed_grant.expires_at > current_grant.expires_at =>
                            {
                                // An independent heartbeat renewed the same writer grant
                                // during drain_producers(). Tombstone this attempt ID,
                                // reopen admission before clearing local intent, and
                                // restart Step 1 cleanly with the refreshed grant.
                                match volume.control().abort_operation(ids.capture_id) {
                                    Ok(OperationState::Aborted) => {}
                                    Ok(_) => return Err(CaptureError::Conflict),
                                    Err(abort_err) => {
                                        return Err(CaptureError::Control(abort_err));
                                    }
                                }
                                producers
                                    .reopen_admission()
                                    .map_err(CaptureError::Producer)?;
                                self.journal
                                    .finish_pending(ids.capture_id)
                                    .map_err(CaptureError::Journal)?;
                                if attempt == self.config.max_acquisition_retries {
                                    return Err(CaptureError::Conflict);
                                }
                                continue;
                            }
                            Ok(_) => {
                                // Attachment, writer grant, and authority owner
                                // incarnation are still live (for example, a
                                // foreign principal acquired a capture between
                                // pre-drain read and begin_capture). Reject this
                                // capture attempt without permanently fencing.
                            }
                            Err(
                                ControlError::Fenced
                                | ControlError::Expired
                                | ControlError::NotFound,
                            ) => {
                                volume.mark_fenced();
                                final_error = CaptureError::Fenced;
                            }
                            Err(branch_err) if is_unknown_control_error(&branch_err) => {
                                // Cannot confirm whether we are fenced; keep journal intact.
                                return Err(CaptureError::Control(branch_err));
                            }
                            Err(branch_err) => {
                                return Err(CaptureError::Control(branch_err));
                            }
                        }
                    } else if matches!(
                        &final_error,
                        CaptureError::Fenced
                            | CaptureError::Control(ControlError::Expired | ControlError::NotFound)
                    ) {
                        volume.mark_fenced();
                    }

                    // Only clear the local capture intent once the authority
                    // confirms this attempt ID is durably aborted and producer
                    // admission has reopened.
                    if matches!(
                        volume.control().abort_operation(ids.capture_id),
                        Ok(OperationState::Aborted)
                    ) && producers.reopen_admission().is_ok()
                    {
                        let _ = self.journal.finish_pending(ids.capture_id);
                    }
                    return Err(final_error);
                }
            }
        }
        Err(CaptureError::Conflict)
    }

    fn begin_capture_with_reconciliation<C: CaptureControl>(
        &self,
        control: &C,
        request: &PreparationRequest,
        provenance: ProvenanceBinding,
    ) -> Result<(), CaptureError> {
        const MAX_UNKNOWN_RETRIES: usize = 2;
        for attempt in 0..=MAX_UNKNOWN_RETRIES {
            match control.begin_capture(request.clone(), provenance) {
                Ok(OperationState::Preparing { .. }) => return Ok(()),
                Ok(OperationState::Aborted) => return Err(CaptureError::Aborted),
                Ok(OperationState::Committed(_)) => return Err(CaptureError::Conflict),
                Err(error) if is_unknown_control_error(&error) => {
                    match control.get_operation(request.operation_id) {
                        Ok(OperationState::Preparing { .. }) => return Ok(()),
                        Ok(OperationState::Aborted) => return Err(CaptureError::Aborted),
                        Ok(OperationState::Committed(_)) => return Err(CaptureError::Conflict),
                        Err(ControlError::NotFound) if attempt < MAX_UNKNOWN_RETRIES => continue,
                        Err(ControlError::NotFound) => return Err(CaptureError::Control(error)),
                        Err(reconcile_error) if is_unknown_control_error(&reconcile_error) => {
                            return Err(CaptureError::Control(reconcile_error));
                        }
                        Err(_) => return Err(CaptureError::Control(error)),
                    }
                }
                Err(error) => return Err(CaptureError::Control(error)),
            }
        }
        Err(CaptureError::Conflict)
    }

    fn seal_with_reconciliation<C: CaptureControl>(
        &self,
        control: &C,
        seal_op_id: OperationId,
        capture_id: OperationId,
        expected_revision: u64,
        grant: WriterGrant,
    ) -> Result<CaptureState, CaptureError> {
        const MAX_UNKNOWN_RETRIES: usize = 2;
        for attempt in 0..=MAX_UNKNOWN_RETRIES {
            match control.seal_capture(seal_op_id, capture_id, expected_revision, grant) {
                Ok(state) => return Ok(state),
                Err(error) if is_unknown_control_error(&error) => {
                    match control.get_operation(seal_op_id) {
                        Ok(OperationState::Committed(_)) => {
                            let branch = control.get_branch(grant.branch_id)?;
                            let capture = branch.capture.ok_or(CaptureError::Conflict)?;
                            if capture.operation_id == capture_id && capture.sealed {
                                return Ok(capture);
                            }
                            return Err(CaptureError::Conflict);
                        }
                        Ok(OperationState::Aborted) => return Err(CaptureError::Aborted),
                        Ok(OperationState::Preparing { .. }) => return Err(CaptureError::Conflict),
                        Err(ControlError::NotFound) if attempt < MAX_UNKNOWN_RETRIES => continue,
                        Err(ControlError::NotFound) => return Err(CaptureError::Control(error)),
                        Err(reconcile_error) if is_unknown_control_error(&reconcile_error) => {
                            return Err(CaptureError::Control(reconcile_error));
                        }
                        Err(_) => return Err(CaptureError::Control(error)),
                    }
                }
                Err(error) => return Err(CaptureError::Control(error)),
            }
        }
        Err(CaptureError::Conflict)
    }

    fn seal_with_grant_refresh<S, C, BJ>(
        &mut self,
        volume: &mut DurableVolume<S, C, BJ>,
        pending: &mut PendingCapture,
        expected_sealed_revision: u64,
        original_grant: WriterGrant,
    ) -> Result<CaptureState, CaptureError>
    where
        S: DurableObjectStore,
        C: CaptureControl,
        BJ: BarrierJournal,
    {
        const MAX_GRANT_REFRESH_RETRIES: usize = 2;
        for refresh_attempt in 0..=MAX_GRANT_REFRESH_RETRIES {
            let current_grant = self.validate_live_worker(volume, original_grant)?;
            let seal_op_id = self
                .journal
                .reserve_operation_id()
                .map_err(CaptureError::Journal)?;
            pending.phase = CapturePhase::Sealing;
            pending.seal_operation_id = Some(seal_op_id);
            pending.seal_grant = Some(current_grant);
            self.persist(pending)?;

            match self.seal_with_reconciliation(
                volume.control(),
                seal_op_id,
                pending.capture_id,
                expected_sealed_revision,
                current_grant,
            ) {
                Ok(state) => return Ok(state),
                Err(CaptureError::Control(ControlError::Fenced)) => {
                    let refreshed = self.validate_live_worker(volume, original_grant)?;
                    if same_grant_identity(current_grant, refreshed)
                        && refreshed.expires_at > current_grant.expires_at
                    {
                        if refresh_attempt == MAX_GRANT_REFRESH_RETRIES {
                            return Err(CaptureError::Conflict);
                        }
                        match volume.control().abort_operation(seal_op_id) {
                            Ok(OperationState::Aborted) => continue,
                            Ok(OperationState::Committed(_)) => {
                                let branch = volume.control().get_branch(pending.branch_id)?;
                                let capture = branch.capture.ok_or(CaptureError::Conflict)?;
                                if capture.operation_id == pending.capture_id && capture.sealed {
                                    return Ok(capture);
                                }
                                return Err(CaptureError::Conflict);
                            }
                            Ok(OperationState::Preparing { .. }) => {
                                return Err(CaptureError::Conflict);
                            }
                            Err(abort_err) => return Err(CaptureError::Control(abort_err)),
                        }
                    }
                    volume.mark_fenced();
                    return Err(CaptureError::Fenced);
                }
                Err(CaptureError::Control(ControlError::Expired)) => {
                    // Distinguish terminal writer/attachment expiry from
                    // capture-only preparation expiry.
                    if let Err(CaptureError::Fenced) =
                        self.validate_live_worker(volume, original_grant)
                    {
                        return Err(CaptureError::Fenced);
                    }
                    return Err(CaptureError::Control(ControlError::Expired));
                }
                Err(error) => return Err(error),
            }
        }
        Err(CaptureError::Conflict)
    }

    fn publish_with_grant_refresh<S, C, BJ>(
        &mut self,
        volume: &mut DurableVolume<S, C, BJ>,
        pending: &mut PendingCapture,
        sealed_revision: u64,
        verified: &VerifiedGenerationSeed,
        original_grant: WriterGrant,
    ) -> Result<PublishedGeneration, CaptureError>
    where
        S: DurableObjectStore,
        C: CaptureControl,
        BJ: BarrierJournal,
    {
        const MAX_GRANT_REFRESH_RETRIES: usize = 2;
        for refresh_attempt in 0..=MAX_GRANT_REFRESH_RETRIES {
            let publish_grant = self.validate_live_worker(volume, original_grant)?;
            pending.descriptor_ref = Some(verified.seed().descriptor);
            pending.publish_grant = Some(publish_grant);
            self.persist(pending)?;

            match volume.control().publish_capture(
                pending.capture_id,
                sealed_revision,
                pending.expected_parent,
                publish_grant,
                pending.attachment_id,
                pending.volume_epoch,
                verified.clone(),
                pending.advance_head,
            ) {
                Ok(published) => return Ok(published),
                Err(ControlError::Fenced) => {
                    let refreshed = self.validate_live_worker(volume, original_grant)?;
                    if same_grant_identity(publish_grant, refreshed)
                        && refreshed.expires_at > publish_grant.expires_at
                    {
                        if refresh_attempt == MAX_GRANT_REFRESH_RETRIES {
                            return Err(CaptureError::Conflict);
                        }
                        continue;
                    }
                    volume.mark_fenced();
                    return Err(CaptureError::Fenced);
                }
                Err(ControlError::Expired) => {
                    if let Err(CaptureError::Fenced) =
                        self.validate_live_worker(volume, original_grant)
                    {
                        return Err(CaptureError::Fenced);
                    }
                    return Err(CaptureError::Control(ControlError::Expired));
                }
                Err(error) => return Err(CaptureError::Control(error)),
            }
        }
        Err(CaptureError::Conflict)
    }

    fn create_verified_generation_with_renewal<S, C, BJ>(
        &mut self,
        volume: &mut DurableVolume<S, C, BJ>,
        pending: &mut PendingCapture,
        sealed_roots: LiveRoots,
        protected_proofs: &[&DependencyProof],
    ) -> Result<VerifiedGenerationSeed, CaptureError>
    where
        S: DurableObjectStore,
        C: CaptureControl,
        BJ: BarrierJournal,
    {
        let branch_id = volume.branch_id();
        let attachment_id = volume.attachment_id();
        let volume_epoch = volume.volume_epoch();
        let generation_id = pending.generation_id;
        let expected_parent = pending.expected_parent;
        let mode = pending.mode;
        let boundary_id = pending.boundary_id;
        let created_at_unix_ms = pending.created_at_unix_ms;
        let provenance = pending.provenance;

        let (result, renewal_error) = {
            let (store, control) = volume.store_and_control_mut();
            let mut renewing_store = RenewingObjectStore {
                store,
                control,
                coordinator: RefCell::new(self),
                pending: RefCell::new(pending),
                branch_id,
                attachment_id,
                volume_epoch,
                renewal_error: RefCell::new(None),
            };
            let result = create_generation_with_reuse_and_provenance(
                &mut renewing_store,
                generation_id,
                Some(expected_parent),
                sealed_roots,
                mode,
                boundary_id,
                created_at_unix_ms,
                provenance,
                protected_proofs,
            );
            let renewal_error = renewing_store.renewal_error.into_inner();
            (result, renewal_error)
        };
        if let Some(error) = renewal_error {
            if matches!(error, CaptureError::Fenced) {
                volume.mark_fenced();
            }
            return Err(error);
        }
        result.map_err(CaptureError::Barrier)
    }

    fn renew_if_due<S, C, BJ>(
        &mut self,
        volume: &mut DurableVolume<S, C, BJ>,
        pending: &mut PendingCapture,
    ) -> Result<(), CaptureError>
    where
        S: DurableObjectStore,
        C: CaptureControl,
        BJ: BarrierJournal,
    {
        let branch_id = volume.branch_id();
        let attachment_id = volume.attachment_id();
        let volume_epoch = volume.volume_epoch();
        match self.renew_if_due_with_control(
            volume.control(),
            branch_id,
            attachment_id,
            volume_epoch,
            pending,
        ) {
            Ok(()) => Ok(()),
            Err(CaptureError::Fenced) => {
                volume.mark_fenced();
                Err(CaptureError::Fenced)
            }
            Err(error) => Err(error),
        }
    }

    fn renew_if_due_with_control<C: CaptureControl>(
        &mut self,
        control: &C,
        branch_id: BranchId,
        attachment_id: AttachmentId,
        volume_epoch: u64,
        pending: &mut PendingCapture,
    ) -> Result<(), CaptureError> {
        const MAX_GRANT_REFRESH_RETRIES: usize = 2;
        let unresolved = pending.renewal_operation_id.is_some();
        let due = self
            .last_renewal
            .is_none_or(|last| last.elapsed() >= self.config.renewal_interval);
        if !unresolved && !due {
            return Ok(());
        }

        // Reconcile any persisted renewal_operation_id before deciding whether
        // to replay it or allocate a fresh renewal:
        // - Committed + recent attempt: clear without extending twice.
        // - Committed + stale/restarted: clear and issue a fresh renewal so the preparation lease
        //   is actually extended from now.
        // - Aborted: clear and issue a fresh renewal.
        // - NotFound: retry the exact same renewal_operation_id.
        if let Some(existing_id) = pending.renewal_operation_id {
            match control.get_operation(existing_id) {
                Ok(OperationState::Committed(_)) => {
                    pending.renewal_operation_id = None;
                    pending.renewal_grant = None;
                    self.persist(pending)?;
                    let attempt_stale = self
                        .last_renewal_attempt
                        .is_none_or(|last| last.elapsed() >= self.config.renewal_interval);
                    if !attempt_stale {
                        self.last_renewal = self.last_renewal_attempt;
                        return Ok(());
                    }
                }
                Ok(OperationState::Aborted) => {
                    pending.renewal_operation_id = None;
                    pending.renewal_grant = None;
                    self.persist(pending)?;
                }
                Ok(OperationState::Preparing { .. }) | Err(ControlError::NotFound) => {}
                Err(error) => return Err(CaptureError::Control(error)),
            }
        }

        let current_grant = Self::validate_live_worker_with_control(
            control,
            branch_id,
            attachment_id,
            volume_epoch,
            pending.writer_grant,
        )?;
        let (mut renewal_id, mut renewal_grant) =
            match (pending.renewal_operation_id, pending.renewal_grant) {
                (Some(id), Some(grant)) => (id, grant),
                (None, None) => {
                    let id = self
                        .journal
                        .reserve_operation_id()
                        .map_err(CaptureError::Journal)?;
                    pending.renewal_operation_id = Some(id);
                    pending.renewal_grant = Some(current_grant);
                    self.persist(pending)?;
                    (id, current_grant)
                }
                _ => {
                    return Err(CaptureError::InvalidState(
                        "pending capture renewal ID and grant are inconsistent",
                    ));
                }
            };

        for refresh_attempt in 0..=MAX_GRANT_REFRESH_RETRIES {
            let attempt_started = Instant::now();
            self.last_renewal_attempt = Some(attempt_started);
            match control.renew_preparation(renewal_id, pending.capture_id, renewal_grant) {
                Ok(_) => {
                    pending.renewal_operation_id = None;
                    pending.renewal_grant = None;
                    self.persist(pending)?;
                    // Measure the next renewal interval conservatively from
                    // before the RPC so slow replies or journal writes do not
                    // delay the next heartbeat past the authority lease.
                    self.last_renewal = Some(attempt_started);
                    self.last_renewal_attempt = Some(attempt_started);
                    return Ok(());
                }
                Err(error @ (ControlError::Expired | ControlError::Fenced)) => {
                    let refreshed = Self::validate_live_worker_with_control(
                        control,
                        branch_id,
                        attachment_id,
                        volume_epoch,
                        pending.writer_grant,
                    )?;
                    if same_grant_identity(renewal_grant, refreshed)
                        && refreshed.expires_at > renewal_grant.expires_at
                    {
                        if refresh_attempt == MAX_GRANT_REFRESH_RETRIES {
                            return Err(CaptureError::Conflict);
                        }
                        let replacement_id = self
                            .journal
                            .reserve_operation_id()
                            .map_err(CaptureError::Journal)?;
                        pending.renewal_operation_id = Some(replacement_id);
                        pending.renewal_grant = Some(refreshed);
                        self.persist(pending)?;
                        renewal_id = replacement_id;
                        renewal_grant = refreshed;
                        continue;
                    }
                    if matches!(error, ControlError::Fenced) {
                        return Err(CaptureError::Fenced);
                    }
                    return Err(CaptureError::Control(error));
                }
                Err(error) => return Err(CaptureError::Control(error)),
            }
        }
        Err(CaptureError::Conflict)
    }

    fn validate_live_worker<S, C, BJ>(
        &self,
        volume: &mut DurableVolume<S, C, BJ>,
        original_grant: WriterGrant,
    ) -> Result<WriterGrant, CaptureError>
    where
        S: DurableObjectStore,
        C: CaptureControl,
        BJ: BarrierJournal,
    {
        match Self::validate_live_worker_with_control(
            volume.control(),
            volume.branch_id(),
            volume.attachment_id(),
            volume.volume_epoch(),
            original_grant,
        ) {
            Ok(grant) => Ok(grant),
            Err(CaptureError::Fenced) => {
                volume.mark_fenced();
                Err(CaptureError::Fenced)
            }
            Err(error) => Err(error),
        }
    }

    fn validate_live_worker_with_control<C: CaptureControl>(
        control: &C,
        branch_id: BranchId,
        attachment_id: AttachmentId,
        volume_epoch: u64,
        original_grant: WriterGrant,
    ) -> Result<WriterGrant, CaptureError> {
        match control.check_attachment_live(branch_id, attachment_id, volume_epoch, original_grant)
        {
            Ok(grant) => Ok(grant),
            Err(ControlError::Fenced | ControlError::Expired | ControlError::NotFound) => {
                Err(CaptureError::Fenced)
            }
            Err(error) => Err(CaptureError::Control(error)),
        }
    }

    fn resolve_pending_child_for_abort<S, C, BJ>(
        &self,
        volume: &mut DurableVolume<S, C, BJ>,
        pending: &PendingCapture,
    ) -> Result<(), CaptureError>
    where
        S: DurableObjectStore,
        C: CaptureControl,
        BJ: BarrierJournal,
    {
        let authority_child = match volume.control().get_branch(pending.branch_id) {
            Ok(branch) => branch
                .capture
                .filter(|capture| capture.operation_id == pending.capture_id)
                .and_then(|capture| capture.pending_child),
            Err(ControlError::NotFound) => {
                volume.mark_fenced();
                None
            }
            Err(error) => return Err(CaptureError::Control(error)),
        };
        if let Some(child_id) = authority_child {
            self.abort_uncommitted_child_operation(volume.control(), child_id)?;
        }
        self.resolve_local_pending_barrier_for_abort(volume, pending.capture_id, authority_child)
    }

    fn resolve_local_pending_barrier_for_abort<S, C, BJ>(
        &self,
        volume: &mut DurableVolume<S, C, BJ>,
        capture_id: OperationId,
        already_resolved: Option<OperationId>,
    ) -> Result<(), CaptureError>
    where
        S: DurableObjectStore,
        C: CaptureControl,
        BJ: BarrierJournal,
    {
        if volume.pending_barrier_capture_id() == Some(capture_id)
            && let Some(local_child_id) = volume.pending_barrier_operation_id()
        {
            if Some(local_child_id) != already_resolved {
                self.abort_uncommitted_child_operation(volume.control(), local_child_id)?;
            }
            match volume.reconcile_pending_outcome() {
                Ok(()) | Err(BarrierError::Fenced) => {}
                Err(error) => return Err(CaptureError::Barrier(error)),
            }
        }
        Ok(())
    }

    fn abort_uncommitted_child_operation<C: CaptureControl>(
        &self,
        control: &C,
        child_id: OperationId,
    ) -> Result<(), CaptureError> {
        match control.get_operation(child_id) {
            Ok(OperationState::Committed(_)) | Ok(OperationState::Aborted) => Ok(()),
            Ok(OperationState::Preparing { .. }) | Err(ControlError::NotFound) => {
                // Abort an uncommitted child barrier (or tombstone an unstarted
                // ID) so the parent capture and local journal can resolve cleanly
                // without undoing any barrier that committed concurrently.
                let _ = control.abort_operation(child_id)?;
                Ok(())
            }
            Err(error) => Err(CaptureError::Control(error)),
        }
    }

    fn abort_and_return_error<S, C, BJ, P, B>(
        &mut self,
        volume: &mut DurableVolume<S, C, BJ>,
        producers: &mut P,
        boundary: &mut B,
        pending: &mut PendingCapture,
        original_error: CaptureError,
    ) -> Result<PublishedGeneration, CaptureError>
    where
        S: DurableObjectStore,
        C: CaptureControl,
        BJ: BarrierJournal,
        P: ProducerGate,
        B: FilesystemBoundary,
    {
        pending.phase = CapturePhase::Aborting;
        let _ = self.persist(pending);

        if self
            .resolve_pending_child_for_abort(volume, pending)
            .is_err()
        {
            return Err(original_error);
        }

        match volume.control().abort_operation(pending.capture_id) {
            Ok(OperationState::Committed(branch_bytes)) => {
                // Publication had actually committed; honor the durable result.
                let committed_branch = decode_branch_operation_result(&branch_bytes)?;
                let published = PublishedGeneration {
                    generation_id: pending.generation_id,
                    branch: committed_branch,
                    head_advanced: pending.advance_head,
                };
                return self.finalize_published(volume, producers, boundary, pending, published);
            }
            Ok(OperationState::Aborted) => {}
            Ok(OperationState::Preparing { .. }) | Err(_) => {
                // Leave journal intact so watchdog recovery can complete abort/thaw.
                return Err(original_error);
            }
        }

        self.finish_aborted_recovery(volume, producers, boundary, pending)?;
        Err(original_error)
    }

    fn finish_aborted_recovery<S, C, BJ, P, B>(
        &mut self,
        volume: &mut DurableVolume<S, C, BJ>,
        producers: &mut P,
        boundary: &mut B,
        pending: &mut PendingCapture,
    ) -> Result<bool, CaptureError>
    where
        S: DurableObjectStore,
        C: CaptureControl,
        BJ: BarrierJournal,
        P: ProducerGate,
        B: FilesystemBoundary,
    {
        self.resolve_local_pending_barrier_for_abort(volume, pending.capture_id, None)?;
        match volume.control().get_branch(pending.branch_id) {
            Ok(branch) => match volume.sync_branch_state(&branch) {
                Ok(()) => volume.set_mutations_closed(false),
                Err(BarrierError::Fenced) => {
                    volume.mark_fenced();
                    volume.set_mutations_closed(false);
                }
                Err(BarrierError::Conflict) => {
                    // Dirty extents remain local for salvage; reopen mutation gate
                    // only if attachment is still valid.
                    volume.set_mutations_closed(false);
                }
                Err(error) => return Err(CaptureError::Barrier(error)),
            },
            Err(ControlError::NotFound) => {
                volume.mark_fenced();
                volume.set_mutations_closed(false);
            }
            Err(error) => return Err(CaptureError::Control(error)),
        }

        let mut freeze_thawed = false;
        if pending.freeze_active {
            boundary.thaw().map_err(CaptureError::ThawFailed)?;
            pending.freeze_active = false;
            freeze_thawed = true;
        }
        producers
            .reopen_admission()
            .map_err(CaptureError::Producer)?;
        self.journal
            .finish_pending(pending.capture_id)
            .map_err(CaptureError::Journal)?;
        self.last_renewal = None;
        self.last_renewal_attempt = None;
        Ok(freeze_thawed)
    }

    fn finalize_published<S, C, BJ, P, B>(
        &mut self,
        volume: &mut DurableVolume<S, C, BJ>,
        producers: &mut P,
        boundary: &mut B,
        pending: &mut PendingCapture,
        published: PublishedGeneration,
    ) -> Result<PublishedGeneration, CaptureError>
    where
        S: DurableObjectStore,
        C: CaptureControl,
        BJ: BarrierJournal,
        P: ProducerGate,
        B: FilesystemBoundary,
    {
        // Reconcile local volume state BEFORE thawing or reopening admission.
        // Never hot-switch a fenced attachment onto a replacement owner's root.
        match volume.control().get_branch(pending.branch_id) {
            Ok(latest) => match volume.sync_branch_state(&latest) {
                Ok(()) => volume.set_mutations_closed(false),
                Err(BarrierError::Fenced) => {
                    volume.mark_fenced();
                    volume.set_mutations_closed(false);
                }
                Err(error) => return Err(CaptureError::Barrier(error)),
            },
            Err(ControlError::NotFound) => {
                volume.mark_fenced();
                volume.set_mutations_closed(false);
            }
            Err(error) => return Err(CaptureError::Control(error)),
        }

        if pending.freeze_active {
            boundary.thaw().map_err(CaptureError::ThawFailed)?;
            pending.freeze_active = false;
        }
        // Clean-unmount leaves the reference filesystem unmounted; we only
        // reopen the local producer gate.
        producers
            .reopen_admission()
            .map_err(CaptureError::Producer)?;
        self.journal
            .finish_pending(pending.capture_id)
            .map_err(CaptureError::Journal)?;
        self.last_renewal = None;
        self.last_renewal_attempt = None;
        Ok(published)
    }

    fn persist(&mut self, pending: &PendingCapture) -> Result<(), CaptureError> {
        self.journal
            .persist_pending(pending)
            .map_err(CaptureError::Journal)
    }
}

fn live_roots_of(branch: &BranchState) -> LiveRoots {
    LiveRoots {
        volume: branch.volume_root,
        logs: branch.logs_root,
        artifacts: branch.artifacts_root,
    }
}

fn validate_provenance_binding(binding: ProvenanceBinding) -> Result<(), FormatError> {
    if let ProvenanceBinding::Recorded { context_root } = binding {
        let min_len = ENVELOPE_HEADER_BYTES as u64;
        let max_len = MAX_PROVENANCE_CONTEXT_BYTES as u64;
        if context_root.object_type != ObjectType::ProvenanceContext
            || !(min_len..=max_len).contains(&context_root.encoded_len)
        {
            return Err(FormatError::InvalidProvenance(
                "capture context reference is invalid",
            ));
        }
    }
    Ok(())
}

fn is_unknown_control_error(error: &ControlError) -> bool {
    matches!(
        error,
        ControlError::Metadata(
            MetadataError::UnknownOutcome { .. }
                | MetadataError::ReconciliationRequired { .. }
                | MetadataError::Backend(_)
        )
    )
}

fn is_unknown_barrier_error(error: &BarrierError) -> bool {
    matches!(error, BarrierError::Control(control) if is_unknown_control_error(control))
}

fn is_unknown_capture_error(error: &CaptureError) -> bool {
    matches!(error, CaptureError::Control(control) if is_unknown_control_error(control))
        || matches!(error, CaptureError::Barrier(barrier) if is_unknown_barrier_error(barrier))
}

fn encode_capture_ids(nonce: [u8; 8], next_counter: u64) -> Vec<u8> {
    let mut payload = Vec::with_capacity(16);
    payload.extend_from_slice(&nonce);
    payload.extend_from_slice(&next_counter.to_le_bytes());
    encode_checked(CAPTURE_IDS_MAGIC, &payload)
}

fn decode_capture_ids(bytes: &[u8]) -> Result<([u8; 8], u64), String> {
    let payload = decode_checked(CAPTURE_IDS_MAGIC, bytes)?;
    if payload.len() != 16 {
        return Err("invalid capture journal ID state length".to_owned());
    }
    let nonce = payload[..8]
        .try_into()
        .map_err(|_| "invalid capture journal nonce".to_owned())?;
    let next_counter = u64::from_le_bytes(
        payload[8..]
            .try_into()
            .map_err(|_| "invalid capture journal ID counter".to_owned())?,
    );
    Ok((nonce, next_counter))
}

fn validate_pending_capture(capture: &PendingCapture) -> Result<(), String> {
    if capture.writer_grant.branch_id != capture.branch_id {
        return Err("pending capture grant belongs to another branch".to_owned());
    }
    if capture.mode != CaptureMode::FilesystemFrozen && capture.freeze_active {
        return Err("freeze_active is only valid for filesystem_frozen captures".to_owned());
    }
    if capture.seal_operation_id.is_some() != capture.seal_grant.is_some() {
        return Err("pending seal operation ID and grant must be recorded together".to_owned());
    }
    if let Some(grant) = capture.seal_grant
        && !same_grant_identity(capture.writer_grant, grant)
    {
        return Err("pending seal grant does not match the capture writer".to_owned());
    }
    if capture.sealed_revision.is_some() != capture.sealed_roots.is_some() {
        return Err("sealed revision and sealed roots must be recorded together".to_owned());
    }
    if capture.renewal_operation_id.is_some() != capture.renewal_grant.is_some() {
        return Err("pending renewal ID and grant must be recorded together".to_owned());
    }
    if let Some(grant) = capture.renewal_grant
        && !same_grant_identity(capture.writer_grant, grant)
    {
        return Err("pending renewal grant does not match the capture writer".to_owned());
    }
    if let Some(grant) = capture.publish_grant
        && !same_grant_identity(capture.writer_grant, grant)
    {
        return Err("pending publish grant does not match the capture writer".to_owned());
    }
    validate_roots(capture.pre_drain_roots)?;
    if let Some(roots) = capture.sealed_roots {
        validate_roots(roots)?;
    }
    validate_provenance_binding(capture.provenance)
        .map_err(|error| format!("invalid capture provenance binding: {error}"))?;
    if let Some(descriptor_ref) = capture.descriptor_ref {
        let min_len = ENVELOPE_HEADER_BYTES as u64;
        let max_len = MAX_ENCODED_OBJECT_BYTES as u64;
        if descriptor_ref.object_type != ObjectType::GenerationDescriptor
            || !(min_len..=max_len).contains(&descriptor_ref.encoded_len)
        {
            return Err("pending capture descriptor ref is invalid".to_owned());
        }
    }
    Ok(())
}

fn encode_pending_capture(capture: &PendingCapture) -> Result<Vec<u8>, String> {
    validate_pending_capture(capture)?;
    let mut payload = Vec::new();
    payload.extend_from_slice(capture.scope.workspace_id.as_bytes());
    payload.extend_from_slice(capture.scope.principal_id.as_bytes());
    payload.extend_from_slice(capture.scope.object_store_scope.as_bytes());
    payload.extend_from_slice(capture.capture_id.as_bytes());
    payload.extend_from_slice(capture.protection_id.as_bytes());
    payload.extend_from_slice(capture.generation_id.as_bytes());
    payload.extend_from_slice(capture.boundary_id.as_bytes());
    payload.extend_from_slice(capture.branch_id.as_bytes());
    payload.extend_from_slice(capture.attachment_id.as_bytes());
    push_u64(&mut payload, capture.volume_epoch);
    encode_grant(&mut payload, capture.writer_grant);
    payload.push(capture.mode as u8);
    payload.push(u8::from(capture.advance_head));
    match capture.provenance {
        ProvenanceBinding::Unattributed => payload.push(0),
        ProvenanceBinding::Recorded { context_root } => {
            payload.push(1);
            encode_ref(&mut payload, context_root);
        }
    }
    push_u64(&mut payload, capture.created_at_unix_ms);
    payload.extend_from_slice(capture.expected_parent.as_bytes());
    push_u64(&mut payload, capture.pre_drain_revision);
    encode_roots(&mut payload, capture.pre_drain_roots);
    payload.push(capture.phase as u8);
    payload.push(u8::from(capture.freeze_active));
    encode_optional_operation_id(&mut payload, capture.seal_operation_id);
    if let Some(grant) = capture.seal_grant {
        encode_grant(&mut payload, grant);
    }
    match (capture.sealed_revision, capture.sealed_roots) {
        (Some(revision), Some(roots)) => {
            payload.push(1);
            push_u64(&mut payload, revision);
            encode_roots(&mut payload, roots);
        }
        (None, None) => payload.push(0),
        _ => return Err("sealed revision and roots must match".to_owned()),
    }
    encode_optional_operation_id(&mut payload, capture.renewal_operation_id);
    if let Some(grant) = capture.renewal_grant {
        encode_grant(&mut payload, grant);
    }
    match capture.descriptor_ref {
        Some(reference) => {
            payload.push(1);
            encode_ref(&mut payload, reference);
        }
        None => payload.push(0),
    }
    match capture.publish_grant {
        Some(grant) => {
            payload.push(1);
            encode_grant(&mut payload, grant);
        }
        None => payload.push(0),
    }
    match capture.published_revision {
        Some(revision) => {
            payload.push(1);
            push_u64(&mut payload, revision);
        }
        None => payload.push(0),
    }
    Ok(encode_checked(CAPTURE_PENDING_MAGIC, &payload))
}

fn decode_pending_capture(bytes: &[u8]) -> Result<PendingCapture, String> {
    let payload = decode_checked(CAPTURE_PENDING_MAGIC, bytes)?;
    let mut reader = JournalReader::new(payload);
    let scope = BarrierJournalScope {
        workspace_id: WorkspaceId::from_bytes(reader.fixed()?),
        principal_id: PrincipalId::from_bytes(reader.fixed()?),
        object_store_scope: ObjectStoreScopeId::from_bytes(reader.fixed()?),
    };
    let capture_id = OperationId::from_bytes(reader.fixed()?);
    let protection_id = ProtectionId::from_bytes(reader.fixed()?);
    let generation_id = GenerationId::from_bytes(reader.fixed()?);
    let boundary_id = VolumeBoundaryId::from_bytes(reader.fixed()?);
    let branch_id = BranchId::from_bytes(reader.fixed()?);
    let attachment_id = AttachmentId::from_bytes(reader.fixed()?);
    let volume_epoch = reader.u64()?;
    let writer_grant = decode_grant(&mut reader)?;
    let mode = CaptureMode::try_from(reader.u8()?)
        .map_err(|error| format!("invalid capture mode in journal: {error}"))?;
    let advance_head = decode_bool(reader.u8()?)?;
    let provenance = match reader.u8()? {
        0 => ProvenanceBinding::Unattributed,
        1 => ProvenanceBinding::Recorded {
            context_root: reader.object_ref()?,
        },
        _ => return Err("invalid provenance binding tag in capture journal".to_owned()),
    };
    let created_at_unix_ms = reader.u64()?;
    let expected_parent = GenerationId::from_bytes(reader.fixed()?);
    let pre_drain_revision = reader.u64()?;
    let pre_drain_roots = decode_roots(&mut reader)?;
    let phase = CapturePhase::try_from(reader.u8()?)?;
    let freeze_active = decode_bool(reader.u8()?)?;
    let seal_operation_id = reader.optional_operation_id()?;
    let seal_grant = if seal_operation_id.is_some() {
        Some(decode_grant(&mut reader)?)
    } else {
        None
    };
    let (sealed_revision, sealed_roots) = match reader.u8()? {
        0 => (None, None),
        1 => (Some(reader.u64()?), Some(decode_roots(&mut reader)?)),
        _ => return Err("invalid sealed tuple tag in capture journal".to_owned()),
    };
    let renewal_operation_id = reader.optional_operation_id()?;
    let renewal_grant = if renewal_operation_id.is_some() {
        Some(decode_grant(&mut reader)?)
    } else {
        None
    };
    let descriptor_ref = match reader.u8()? {
        0 => None,
        1 => Some(reader.object_ref()?),
        _ => return Err("invalid descriptor ref tag in capture journal".to_owned()),
    };
    let publish_grant = match reader.u8()? {
        0 => None,
        1 => Some(decode_grant(&mut reader)?),
        _ => return Err("invalid publish grant tag in capture journal".to_owned()),
    };
    let published_revision = match reader.u8()? {
        0 => None,
        1 => Some(reader.u64()?),
        _ => return Err("invalid published revision tag in capture journal".to_owned()),
    };
    reader.finish()?;
    let pending = PendingCapture {
        scope,
        capture_id,
        protection_id,
        generation_id,
        boundary_id,
        branch_id,
        attachment_id,
        volume_epoch,
        writer_grant,
        mode,
        advance_head,
        provenance,
        created_at_unix_ms,
        expected_parent,
        pre_drain_revision,
        pre_drain_roots,
        phase,
        freeze_active,
        seal_operation_id,
        seal_grant,
        sealed_revision,
        sealed_roots,
        renewal_operation_id,
        renewal_grant,
        descriptor_ref,
        publish_grant,
        published_revision,
    };
    validate_pending_capture(&pending)?;
    Ok(pending)
}

fn decode_bool(tag: u8) -> Result<bool, String> {
    match tag {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err("invalid boolean tag in capture journal".to_owned()),
    }
}

#[cfg(unix)]
fn create_dir_all_durable(directory: &Path) -> Result<(), String> {
    let mut missing = Vec::new();
    let mut cursor = directory;
    while !cursor.exists() {
        missing.push(cursor.to_path_buf());
        match cursor.parent() {
            Some(parent) if !parent.as_os_str().is_empty() => cursor = parent,
            _ => break,
        }
    }
    fs::create_dir_all(directory).map_err(journal_io("create capture journal directory"))?;
    for created in missing.into_iter().rev() {
        if let Some(parent) = created.parent() {
            if parent.as_os_str().is_empty() {
                sync_directory(Path::new("."))?;
            } else {
                sync_directory(parent)?;
            }
        }
    }
    Ok(())
}

struct RenewingObjectStore<'a, S, C, J> {
    store: &'a mut S,
    control: &'a C,
    coordinator: RefCell<&'a mut CheckpointCoordinator<J>>,
    pending: RefCell<&'a mut PendingCapture>,
    branch_id: BranchId,
    attachment_id: AttachmentId,
    volume_epoch: u64,
    renewal_error: RefCell<Option<CaptureError>>,
}

impl<S, C, J> RenewingObjectStore<'_, S, C, J>
where
    S: DurableObjectStore,
    C: CaptureControl,
    J: CaptureJournal,
{
    fn check_renewal(&self) -> Result<(), String> {
        if self.renewal_error.borrow().is_some() {
            return Err("capture preparation renewal interrupted".to_owned());
        }
        let mut coordinator = self.coordinator.borrow_mut();
        let mut pending = self.pending.borrow_mut();
        if let Err(error) = coordinator.renew_if_due_with_control(
            self.control,
            self.branch_id,
            self.attachment_id,
            self.volume_epoch,
            &mut pending,
        ) {
            *self.renewal_error.borrow_mut() = Some(error);
            return Err("capture preparation renewal interrupted".to_owned());
        }
        Ok(())
    }
}

impl<S, C, J> MapObjectStore for RenewingObjectStore<'_, S, C, J>
where
    S: DurableObjectStore,
    C: CaptureControl,
    J: CaptureJournal,
{
    fn proof_scope(&self) -> ObjectStoreScopeId {
        self.store.proof_scope()
    }

    fn get(&self, id: ObjectId) -> Result<Option<Vec<u8>>, String> {
        self.check_renewal()?;
        self.store.get(id)
    }

    fn get_ref(&self, reference: ObjectRef) -> Result<Option<Vec<u8>>, String> {
        self.check_renewal()?;
        self.store.get_ref(reference)
    }

    fn put(&mut self, id: ObjectId, bytes: Vec<u8>) -> Result<(), String> {
        self.check_renewal()?;
        self.store.put(id, bytes)
    }
}

impl<S, C, J> DurableObjectStore for RenewingObjectStore<'_, S, C, J>
where
    S: DurableObjectStore,
    C: CaptureControl,
    J: CaptureJournal,
{
    fn put_durable(&mut self, reference: ObjectRef, encoded: &[u8]) -> Result<(), String> {
        self.check_renewal()?;
        self.store.put_durable(reference, encoded)
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::collections::{BTreeMap, HashMap};
    use std::num::NonZeroUsize;
    use std::sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    };
    use versionfs_core::control::{OwnerIncarnation, start_authority_owner};
    use versionfs_core::fault::OneShotFailures;
    use versionfs_core::format::{
        EXTENT_BYTES, ProvenanceContext, VolumeGeometry, decode_generation_descriptor,
    };
    use versionfs_core::identity::ObjectId;
    use versionfs_core::metadata::{
        MetadataCursor, MetadataPage, MetadataSnapshot, MetadataTxn, MutationIdentity,
        ReconciledOutcome,
    };
    use versionfs_core::persistent_map::MapObjectStore;
    use versionfs_core::time::MonotonicTime;

    use crate::barrier::{FileBarrierJournal, initialize_empty_workspace};
    use crate::volume::{BlockError, BlockRequest, BlockResponse};

    static NEXT_TEST_DIR: AtomicU64 = AtomicU64::new(0);

    struct TestDir(PathBuf);

    impl TestDir {
        fn new() -> Self {
            let id = NEXT_TEST_DIR.fetch_add(1, Ordering::Relaxed);
            let path =
                std::env::temp_dir().join(format!("versionfs-capture-{}-{id}", std::process::id()));
            fs::create_dir_all(&path).expect("create test directory");
            Self(path)
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[derive(Default)]
    struct MemoryAuthority {
        values: Mutex<BTreeMap<Vec<u8>, Vec<u8>>>,
        now_nanos: AtomicU64,
        next_snapshot_scope: AtomicU64,
    }

    struct MemorySnapshot {
        scope: u64,
        values: BTreeMap<Vec<u8>, Vec<u8>>,
    }

    impl MetadataSnapshot for MemorySnapshot {
        fn cursor_scope(&self) -> u64 {
            self.scope
        }
    }

    struct MemoryTxn<'a> {
        values: &'a BTreeMap<Vec<u8>, Vec<u8>>,
        writes: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
        now_nanos: &'a AtomicU64,
    }

    impl MetadataTxn for MemoryTxn<'_> {
        fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, MetadataError> {
            Ok(match self.writes.get(key) {
                Some(value) => value.clone(),
                None => self.values.get(key).cloned(),
            })
        }

        fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), MetadataError> {
            self.writes.insert(key.to_vec(), Some(value.to_vec()));
            Ok(())
        }

        fn delete(&mut self, key: &[u8]) -> Result<(), MetadataError> {
            self.writes.insert(key.to_vec(), None);
            Ok(())
        }

        fn now(&self) -> MonotonicTime {
            MonotonicTime::from_nanos(self.now_nanos.load(Ordering::SeqCst))
        }
    }

    impl MetadataAuthority for MemoryAuthority {
        type Snapshot = MemorySnapshot;

        fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, MetadataError> {
            Ok(self.values.lock().unwrap().get(key).cloned())
        }

        fn now(&self) -> MonotonicTime {
            MonotonicTime::from_nanos(self.now_nanos.load(Ordering::SeqCst))
        }

        fn transact<R, F>(
            &self,
            _operation: MutationIdentity<'_>,
            mut body: F,
        ) -> Result<R, MetadataError>
        where
            F: FnMut(&mut dyn MetadataTxn) -> Result<R, MetadataError>,
        {
            let mut values = self.values.lock().unwrap();
            let (result, writes) = {
                let mut txn = MemoryTxn {
                    values: &values,
                    writes: BTreeMap::new(),
                    now_nanos: &self.now_nanos,
                };
                let result = body(&mut txn)?;
                (result, txn.writes)
            };
            for (key, value) in writes {
                match value {
                    Some(value) => {
                        values.insert(key, value);
                    }
                    None => {
                        values.remove(&key);
                    }
                }
            }
            Ok(result)
        }

        fn reconcile_unknown(
            &self,
            operation: MutationIdentity<'_>,
        ) -> Result<ReconciledOutcome, MetadataError> {
            Ok(
                match self.values.lock().unwrap().get(operation.outcome_key) {
                    Some(bytes) => ReconciledOutcome::Committed(bytes.clone()),
                    None => ReconciledOutcome::NotCommitted,
                },
            )
        }

        fn snapshot(&self) -> Result<Self::Snapshot, MetadataError> {
            let scope = self.next_snapshot_scope.fetch_add(1, Ordering::Relaxed) + 1;
            Ok(MemorySnapshot {
                scope,
                values: self.values.lock().unwrap().clone(),
            })
        }

        fn scan_prefix(
            &self,
            snapshot: &Self::Snapshot,
            prefix: &[u8],
            cursor: Option<&MetadataCursor>,
            limit: NonZeroUsize,
        ) -> Result<MetadataPage, MetadataError> {
            if let Some(cursor) = cursor
                && (cursor.snapshot_scope() != snapshot.scope || cursor.prefix() != prefix)
            {
                return Err(MetadataError::InvalidRequest("cursor mismatch"));
            }
            let after = cursor.map(MetadataCursor::last_key);
            let mut entries = snapshot
                .values
                .iter()
                .filter(|(k, _)| k.starts_with(prefix) && after.is_none_or(|a| k.as_slice() > a))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect::<Vec<_>>();
            let has_more = entries.len() > limit.get();
            entries.truncate(limit.get());
            let next_cursor = if has_more {
                entries
                    .last()
                    .map(|(k, _)| MetadataCursor::new(snapshot, prefix, k.clone()))
            } else {
                None
            };
            Ok(MetadataPage {
                entries,
                next_cursor,
            })
        }
    }

    #[derive(Clone)]
    struct MemoryStore {
        scope: ObjectStoreScopeId,
        objects: Arc<Mutex<HashMap<ObjectId, Vec<u8>>>>,
    }

    impl Default for MemoryStore {
        fn default() -> Self {
            Self {
                scope: ObjectStoreScopeId::from_bytes([0xC1; 16]),
                objects: Arc::new(Mutex::new(HashMap::new())),
            }
        }
    }

    impl MapObjectStore for MemoryStore {
        fn proof_scope(&self) -> ObjectStoreScopeId {
            self.scope
        }

        fn get(&self, id: ObjectId) -> Result<Option<Vec<u8>>, String> {
            Ok(self.objects.lock().unwrap().get(&id).cloned())
        }

        fn put(&mut self, id: ObjectId, bytes: Vec<u8>) -> Result<(), String> {
            self.objects.lock().unwrap().insert(id, bytes);
            Ok(())
        }
    }

    impl DurableObjectStore for MemoryStore {
        fn put_durable(&mut self, reference: ObjectRef, encoded: &[u8]) -> Result<(), String> {
            reference
                .verify(encoded)
                .map_err(|error| error.to_string())?;
            self.put(reference.object_id, encoded.to_vec())
        }
    }

    type OneShotHook = Arc<Mutex<Option<Box<dyn FnOnce() + Send>>>>;

    #[derive(Clone)]
    struct FaultyControl {
        inner: Arc<WorkspaceControl<MemoryAuthority>>,
        override_inner: Arc<Mutex<Option<Arc<WorkspaceControl<MemoryAuthority>>>>>,
        drop_next_begin_reply: Arc<Mutex<Option<bool>>>, /* Some(true)=after commit,
                                                          * Some(false)=before commit */
        drop_next_publish_reply: Arc<Mutex<Option<bool>>>,
        drop_next_renew_reply: Arc<Mutex<Option<bool>>>,
        drop_next_commit_live_roots_reply: Arc<Mutex<Option<bool>>>,
        fail_next_authorize_backend: Arc<Mutex<bool>>,
        fail_next_begin_backend: Arc<Mutex<Option<bool>>>, // Some(true)=also fail get_operation
        fail_next_get_operation_backend: Arc<Mutex<bool>>,
        on_next_renew: OneShotHook,
        on_next_seal: OneShotHook,
        on_next_publish: OneShotHook,
        begin_ids: Arc<Mutex<Vec<OperationId>>>,
        renew_ids: Arc<Mutex<Vec<OperationId>>>,
    }

    impl FaultyControl {
        fn new(inner: Arc<WorkspaceControl<MemoryAuthority>>) -> Self {
            Self {
                inner,
                override_inner: Arc::new(Mutex::new(None)),
                drop_next_begin_reply: Arc::new(Mutex::new(None)),
                drop_next_publish_reply: Arc::new(Mutex::new(None)),
                drop_next_renew_reply: Arc::new(Mutex::new(None)),
                drop_next_commit_live_roots_reply: Arc::new(Mutex::new(None)),
                fail_next_authorize_backend: Arc::new(Mutex::new(false)),
                fail_next_begin_backend: Arc::new(Mutex::new(None)),
                fail_next_get_operation_backend: Arc::new(Mutex::new(false)),
                on_next_renew: Arc::new(Mutex::new(None)),
                on_next_seal: Arc::new(Mutex::new(None)),
                on_next_publish: Arc::new(Mutex::new(None)),
                begin_ids: Arc::new(Mutex::new(Vec::new())),
                renew_ids: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn active(&self) -> Arc<WorkspaceControl<MemoryAuthority>> {
            self.override_inner
                .lock()
                .unwrap()
                .clone()
                .unwrap_or_else(|| self.inner.clone())
        }
    }

    impl VolumeBarrierControl for FaultyControl {
        fn workspace_id(&self) -> WorkspaceId {
            self.active().workspace_id()
        }

        fn object_store_scope(&self) -> ObjectStoreScopeId {
            self.active().object_store_scope()
        }

        fn principal_id(&self) -> PrincipalId {
            self.active().principal_id()
        }

        fn get_branch(&self, branch_id: BranchId) -> Result<BranchState, ControlError> {
            self.active().get_branch(branch_id)
        }

        fn check_attachment_live(
            &self,
            branch_id: BranchId,
            attachment_id: AttachmentId,
            volume_epoch: u64,
            grant: WriterGrant,
        ) -> Result<WriterGrant, ControlError> {
            self.active()
                .check_attachment_live(branch_id, attachment_id, volume_epoch, grant)
        }

        fn begin_preparation(
            &self,
            request: PreparationRequest,
        ) -> Result<OperationState, ControlError> {
            self.active().begin_preparation(request)
        }

        fn authorize_capture_child(
            &self,
            authorization_id: OperationId,
            capture_id: OperationId,
            child_id: OperationId,
            grant: WriterGrant,
        ) -> Result<(), ControlError> {
            let mut fail = self.fail_next_authorize_backend.lock().unwrap();
            if *fail {
                *fail = false;
                return Err(ControlError::Metadata(MetadataError::Backend(
                    "injected authorize_capture_child transport failure".to_owned(),
                )));
            }
            drop(fail);
            self.active()
                .authorize_capture_child(authorization_id, capture_id, child_id, grant)
        }

        fn renew_preparation(
            &self,
            renewal_id: OperationId,
            prepared_id: OperationId,
            grant: WriterGrant,
        ) -> Result<MonotonicTime, ControlError> {
            self.renew_ids.lock().unwrap().push(renewal_id);
            if let Some(hook) = self.on_next_renew.lock().unwrap().take() {
                hook();
            }
            let active = self.active();
            if let Some(commit_first) = self.drop_next_renew_reply.lock().unwrap().take() {
                if commit_first {
                    let _ = active.renew_preparation(renewal_id, prepared_id, grant)?;
                }
                return Err(ControlError::Metadata(MetadataError::UnknownOutcome {
                    operation_id: renewal_id.as_bytes().to_vec(),
                    cause: "injected unknown renew_preparation outcome".to_owned(),
                }));
            }
            active.renew_preparation(renewal_id, prepared_id, grant)
        }

        fn commit_live_roots(
            &self,
            operation_id: OperationId,
            expected_revision: u64,
            grant: WriterGrant,
            roots: LiveRoots,
            capture_id: Option<OperationId>,
        ) -> Result<BranchState, ControlError> {
            let active = self.active();
            if let Some(commit_first) = self
                .drop_next_commit_live_roots_reply
                .lock()
                .unwrap()
                .take()
            {
                if commit_first {
                    let _ = VolumeBarrierControl::commit_live_roots(
                        active.as_ref(),
                        operation_id,
                        expected_revision,
                        grant,
                        roots,
                        capture_id,
                    )?;
                }
                return Err(ControlError::Metadata(MetadataError::UnknownOutcome {
                    operation_id: operation_id.as_bytes().to_vec(),
                    cause: "injected unknown commit_live_roots outcome".to_owned(),
                }));
            }
            VolumeBarrierControl::commit_live_roots(
                active.as_ref(),
                operation_id,
                expected_revision,
                grant,
                roots,
                capture_id,
            )
        }

        fn abort_operation(
            &self,
            operation_id: OperationId,
        ) -> Result<OperationState, ControlError> {
            self.active().abort_operation(operation_id)
        }

        fn get_operation(&self, operation_id: OperationId) -> Result<OperationState, ControlError> {
            let mut fail = self.fail_next_get_operation_backend.lock().unwrap();
            if *fail {
                *fail = false;
                return Err(ControlError::Metadata(MetadataError::Backend(
                    "injected get_operation transport failure".to_owned(),
                )));
            }
            drop(fail);
            self.active().get_operation(operation_id)
        }
    }

    impl CaptureControl for FaultyControl {
        fn begin_capture(
            &self,
            request: PreparationRequest,
            provenance: ProvenanceBinding,
        ) -> Result<OperationState, ControlError> {
            self.begin_ids.lock().unwrap().push(request.operation_id);
            let active = self.active();
            if let Some(also_fail_get_op) = self.fail_next_begin_backend.lock().unwrap().take() {
                let _ = active.begin_preparation_with_provenance(request.clone(), provenance)?;
                if also_fail_get_op {
                    *self.fail_next_get_operation_backend.lock().unwrap() = true;
                }
                return Err(ControlError::Metadata(MetadataError::Backend(
                    "injected begin_capture transport outage after commit".to_owned(),
                )));
            }
            if let Some(commit_first) = self.drop_next_begin_reply.lock().unwrap().take() {
                if commit_first {
                    let _ =
                        active.begin_preparation_with_provenance(request.clone(), provenance)?;
                }
                return Err(ControlError::Metadata(MetadataError::UnknownOutcome {
                    operation_id: request.operation_id.as_bytes().to_vec(),
                    cause: "injected unknown begin_capture outcome".to_owned(),
                }));
            }
            active.begin_preparation_with_provenance(request, provenance)
        }

        fn seal_capture(
            &self,
            operation_id: OperationId,
            capture_id: OperationId,
            expected_revision: u64,
            grant: WriterGrant,
        ) -> Result<CaptureState, ControlError> {
            if let Some(hook) = self.on_next_seal.lock().unwrap().take() {
                hook();
            }
            self.active()
                .seal_capture(operation_id, capture_id, expected_revision, grant)
        }

        fn publish_capture(
            &self,
            capture_id: OperationId,
            expected_revision: u64,
            expected_parent: GenerationId,
            grant: WriterGrant,
            attachment_id: AttachmentId,
            volume_epoch: u64,
            generation: VerifiedGenerationSeed,
            advance_head: bool,
        ) -> Result<PublishedGeneration, ControlError> {
            if let Some(hook) = self.on_next_publish.lock().unwrap().take() {
                hook();
            }
            let active = self.active();
            if let Some(commit_first) = self.drop_next_publish_reply.lock().unwrap().take() {
                if commit_first {
                    let _ = active.publish_capture(
                        capture_id,
                        expected_revision,
                        expected_parent,
                        grant,
                        attachment_id,
                        volume_epoch,
                        generation,
                        advance_head,
                    )?;
                }
                return Err(ControlError::Metadata(MetadataError::UnknownOutcome {
                    operation_id: capture_id.as_bytes().to_vec(),
                    cause: "injected unknown publish_capture outcome".to_owned(),
                }));
            }
            active.publish_capture(
                capture_id,
                expected_revision,
                expected_parent,
                grant,
                attachment_id,
                volume_epoch,
                generation,
                advance_head,
            )
        }
    }

    #[derive(Default)]
    struct RecordingBoundary {
        freeze_writes: Option<(u64, Vec<u8>, bool)>,
        unmount_writes: Option<(u64, Vec<u8>, bool)>,
        fail_freeze_after_flush: bool,
        fail_unmount: bool,
        freeze_calls: usize,
        thaw_calls: usize,
        unmount_calls: usize,
        frozen: bool,
        unmounted: bool,
    }

    impl FilesystemBoundary for RecordingBoundary {
        fn freeze(&mut self, device: &mut dyn BlockDevice) -> Result<(), String> {
            self.freeze_calls += 1;
            if let Some((offset, data, flush)) = self.freeze_writes.clone() {
                device
                    .dispatch(BlockRequest::Write {
                        offset,
                        data: &data,
                        fua: false,
                    })
                    .map_err(|error| error.to_string())?;
                if flush {
                    device
                        .dispatch(BlockRequest::Flush)
                        .map_err(|error| error.to_string())?;
                }
            }
            if self.fail_freeze_after_flush {
                return Err("fsfreeze ioctl failed after journal flush".to_owned());
            }
            self.frozen = true;
            Ok(())
        }

        fn thaw(&mut self) -> Result<(), String> {
            self.thaw_calls += 1;
            self.frozen = false;
            Ok(())
        }

        fn clean_unmount(&mut self, device: &mut dyn BlockDevice) -> Result<(), String> {
            self.unmount_calls += 1;
            if let Some((offset, data, flush)) = self.unmount_writes.clone() {
                device
                    .dispatch(BlockRequest::Write {
                        offset,
                        data: &data,
                        fua: false,
                    })
                    .map_err(|error| error.to_string())?;
                if flush {
                    device
                        .dispatch(BlockRequest::Flush)
                        .map_err(|error| error.to_string())?;
                }
            }
            if self.fail_unmount {
                return Err("mount is busy; clean unmount failed".to_owned());
            }
            self.unmounted = true;
            Ok(())
        }
    }

    struct CallbackProducerGate<F> {
        close_calls: usize,
        drain_calls: usize,
        reopen_calls: usize,
        on_drain: F,
    }

    impl<F: FnMut(usize) -> Result<(), String>> CallbackProducerGate<F> {
        fn new(on_drain: F) -> Self {
            Self {
                close_calls: 0,
                drain_calls: 0,
                reopen_calls: 0,
                on_drain,
            }
        }
    }

    impl<F: FnMut(usize) -> Result<(), String>> ProducerGate for CallbackProducerGate<F> {
        fn close_admission(&mut self) -> Result<(), String> {
            self.close_calls += 1;
            Ok(())
        }

        fn drain_producers(&mut self) -> Result<(), String> {
            self.drain_calls += 1;
            (self.on_drain)(self.drain_calls)
        }

        fn reopen_admission(&mut self) -> Result<(), String> {
            self.reopen_calls += 1;
            Ok(())
        }
    }

    struct TestFixture {
        _authority: Arc<MemoryAuthority>,
        _owner: OwnerIncarnation,
        control: FaultyControl,
        store: MemoryStore,
        grant: WriterGrant,
        barrier_dir: PathBuf,
        capture_dir: PathBuf,
        _dir: TestDir,
    }

    impl TestFixture {
        fn new() -> Self {
            let dir = TestDir::new();
            let authority = Arc::new(MemoryAuthority::default());
            let owner = start_authority_owner(authority.as_ref(), op(250)).unwrap();
            let inner_control = Arc::new(WorkspaceControl::new(
                authority.clone(),
                WorkspaceId::from_bytes([1; 16]),
                ObjectStoreScopeId::from_bytes([0xC1; 16]),
                PrincipalId::from_bytes([7; 16]),
                owner,
            ));
            let mut store = MemoryStore::default();
            let initialized = initialize_empty_workspace(
                &mut store,
                inner_control.as_ref(),
                op(1),
                GenerationId::from_bytes([2; 16]),
                VolumeBoundaryId::from_bytes([3; 16]),
                VolumeGeometry::new(4 * u64::from(EXTENT_BYTES)).unwrap(),
                100,
                branch(),
                "main",
                60_000,
                op(2),
                op(3),
                attachment(),
                ProtectionId::from_bytes([4; 16]),
            )
            .unwrap();
            let barrier_dir = dir.0.join("barrier");
            let capture_dir = dir.0.join("capture");
            Self {
                _authority: authority,
                _owner: owner,
                control: FaultyControl::new(inner_control),
                store,
                grant: initialized.writer_grant,
                barrier_dir,
                capture_dir,
                _dir: dir,
            }
        }

        fn open_volume(&self) -> DurableVolume<MemoryStore, FaultyControl, FileBarrierJournal> {
            DurableVolume::open(
                self.store.clone(),
                self.control.clone(),
                FileBarrierJournal::open(&self.barrier_dir).unwrap(),
                branch(),
                self.grant,
                attachment(),
                EXTENT_BYTES as usize * 2,
            )
            .unwrap()
        }

        fn coordinator(&self) -> CheckpointCoordinator<FileCaptureJournal> {
            CheckpointCoordinator::new(
                FileCaptureJournal::open(&self.capture_dir).unwrap(),
                CaptureConfig::default(),
            )
        }

        fn coordinator_with_failures(
            &self,
            failures: Arc<dyn FailureInjector>,
        ) -> CheckpointCoordinator<FileCaptureJournal> {
            CheckpointCoordinator::with_failures(
                FileCaptureJournal::open(&self.capture_dir).unwrap(),
                CaptureConfig::default(),
                failures,
            )
        }
    }

    fn op(byte: u8) -> OperationId {
        OperationId::from_bytes([byte; 16])
    }

    fn branch() -> BranchId {
        BranchId::from_bytes([1; 16])
    }

    fn attachment() -> AttachmentId {
        AttachmentId::from_bytes([9; 16])
    }

    #[test]
    fn all_three_capture_modes_persist_exact_mode_and_keep_block_service_alive() {
        for mode in [
            CaptureMode::BlockCut,
            CaptureMode::FilesystemFrozen,
            CaptureMode::FilesystemCleanUnmount,
        ] {
            let fixture = TestFixture::new();
            let mut volume = fixture.open_volume();
            let mut coordinator = fixture.coordinator();
            let mut producers = NoopProducerGate;
            let mut boundary = RecordingBoundary {
                freeze_writes: Some((4096, vec![0xA1; 4096], true)),
                unmount_writes: Some((8192, vec![0xB2; 4096], true)),
                ..RecordingBoundary::default()
            };

            volume
                .dispatch(BlockRequest::Write {
                    offset: 0,
                    data: &[0x11; 4096],
                    fua: false,
                })
                .unwrap();

            let published = coordinator
                .checkpoint(
                    &mut volume,
                    &mut producers,
                    &mut boundary,
                    &CheckpointRequest::attached(mode, 1_000),
                    &[],
                )
                .expect("checkpoint should succeed");

            assert!(published.head_advanced);
            assert_eq!(published.branch.head, published.generation_id);
            assert_eq!(volume.revision(), published.branch.revision);
            assert!(!volume.mutations_closed());
            assert!(coordinator.pending_capture().unwrap().is_none());

            // Verify persisted descriptor records ONLY the established mode.
            let descriptor_bytes = fixture
                .store
                .objects
                .lock()
                .unwrap()
                .values()
                .find_map(|bytes| {
                    decode_generation_descriptor(bytes)
                        .ok()
                        .filter(|d| d.generation_id() == published.generation_id)
                })
                .expect("published descriptor should exist in object store");
            assert_eq!(descriptor_bytes.capture_mode(), mode);

            match mode {
                CaptureMode::BlockCut => {
                    assert_eq!(boundary.freeze_calls, 0);
                    assert_eq!(boundary.thaw_calls, 0);
                    assert_eq!(boundary.unmount_calls, 0);
                }
                CaptureMode::FilesystemFrozen => {
                    assert_eq!(boundary.freeze_calls, 1);
                    assert_eq!(boundary.thaw_calls, 1);
                    assert!(!boundary.frozen);
                    assert_eq!(
                        volume
                            .dispatch(BlockRequest::Read {
                                offset: 4096,
                                length: 4096
                            })
                            .unwrap(),
                        BlockResponse::Read(vec![0xA1; 4096])
                    );
                }
                CaptureMode::FilesystemCleanUnmount => {
                    assert_eq!(boundary.unmount_calls, 1);
                    assert_eq!(boundary.freeze_calls, 0);
                    assert_eq!(boundary.thaw_calls, 0, "clean unmount must never thaw");
                    assert!(boundary.unmounted);
                    assert_eq!(
                        volume
                            .dispatch(BlockRequest::Read {
                                offset: 8192,
                                length: 4096
                            })
                            .unwrap(),
                        BlockResponse::Read(vec![0xB2; 4096])
                    );
                }
            }
        }
    }

    #[test]
    fn detached_publication_retains_generation_without_advancing_head() {
        let fixture = TestFixture::new();
        let initial_head = fixture.control.get_branch(branch()).unwrap().head;
        let mut volume = fixture.open_volume();
        let mut coordinator = fixture.coordinator();
        let mut producers = NoopProducerGate;
        let mut boundary = BlockCutOnlyBoundary;

        volume
            .dispatch(BlockRequest::Write {
                offset: 0,
                data: &[0x44; 4096],
                fua: false,
            })
            .unwrap();

        let published = coordinator
            .checkpoint(
                &mut volume,
                &mut producers,
                &mut boundary,
                &CheckpointRequest::detached(CaptureMode::BlockCut, 2_000),
                &[],
            )
            .expect("detached capture should publish");

        assert!(!published.head_advanced);
        assert_ne!(published.generation_id, initial_head);
        assert_eq!(published.branch.head, initial_head);
        assert_eq!(
            fixture.control.get_branch(branch()).unwrap().head,
            initial_head
        );
        // Detached Generation is still openable as a retained Generation.
        fixture
            .control
            .inner
            .open_generation(
                op(90),
                published.generation_id,
                ProtectionId::from_bytes([90; 16]),
            )
            .expect("detached Generation must be retained");
    }

    #[test]
    fn drain_time_live_mutation_conflicts_and_restarts_step1_with_new_operation_id() {
        let fixture = TestFixture::new();
        let mut volume = fixture.open_volume();
        let mut coordinator = fixture.coordinator();
        let mut boundary = BlockCutOnlyBoundary;

        let inner_control = fixture.control.inner.clone();
        let grant = fixture.grant;
        let mut store_for_drain = fixture.store.clone();
        let mut producers = CallbackProducerGate::new(move |attempt| {
            if attempt == 1 {
                // Simulate a legitimate drain-time native log mutation advancing
                // branch revision between the pre-drain read and token CAS.
                let current = inner_control.get_branch(branch()).unwrap();
                let mut seg_index =
                    versionfs_core::views::LogSegmentIndex::empty(&mut store_for_drain).unwrap();
                let seg_bytes = versionfs_core::format::LogSegment {
                    log_id: versionfs_core::identity::LogId::from_bytes([1; 16]),
                    first_seq: 0,
                    end_seq: 1,
                    records: vec![b"drain-event".to_vec()],
                    attachments: Vec::new(),
                }
                .encode()
                .unwrap();
                let seg_ref = ObjectRef::from_encoded(&seg_bytes).unwrap();
                store_for_drain.put_durable(seg_ref, &seg_bytes).unwrap();
                seg_index = seg_index
                    .insert(
                        &mut store_for_drain,
                        0,
                        versionfs_core::format::SegmentIndexEntry {
                            end_seq: 1,
                            segment: seg_ref,
                        },
                    )
                    .unwrap();
                let next_logs_ref = versionfs_core::views::LogMap::empty(&mut store_for_drain)
                    .unwrap()
                    .insert(
                        &mut store_for_drain,
                        "events",
                        versionfs_core::format::LogMapEntry {
                            log_id: versionfs_core::identity::LogId::from_bytes([1; 16]),
                            first_seq: 0,
                            end_seq: 1,
                            segment_index_root: seg_index.root(),
                        },
                    )
                    .unwrap()
                    .seal(&mut store_for_drain)
                    .unwrap();
                let next_roots = LiveRoots {
                    volume: current.volume_root,
                    logs: next_logs_ref,
                    artifacts: current.artifacts_root,
                };
                inner_control
                    .begin_preparation(PreparationRequest {
                        operation_id: op(70),
                        branch_id: branch(),
                        expected_revision: current.revision,
                        grant,
                        attachment_id: attachment(),
                        volume_epoch: 1,
                        protection_id: ProtectionId::from_bytes([70; 16]),
                        kind: PreparationKind::LogMutation,
                        expected_roots: Some(next_roots),
                        dependencies: vec![next_logs_ref],
                    })
                    .unwrap();
                inner_control
                    .commit_live_roots(op(70), current.revision, grant, next_roots, None)
                    .unwrap();
            }
            Ok(())
        });

        let published = coordinator
            .checkpoint(
                &mut volume,
                &mut producers,
                &mut boundary,
                &CheckpointRequest::attached(CaptureMode::BlockCut, 3_000),
                &[],
            )
            .expect("conflict must restart Step 1 and succeed on fresh drain");

        assert_eq!(producers.close_calls, 2);
        assert_eq!(producers.drain_calls, 2);
        assert_eq!(producers.reopen_calls, 2);
        let begin_ids = fixture.control.begin_ids.lock().unwrap().clone();
        assert_eq!(begin_ids.len(), 2);
        assert_ne!(
            begin_ids[0], begin_ids[1],
            "conflict restart must allocate a brand-new capture operation ID"
        );
        assert_eq!(
            fixture.control.get_operation(begin_ids[0]).unwrap(),
            OperationState::Aborted,
            "abandoned attempt ID must be tombstoned"
        );
        assert_eq!(published.branch.head, published.generation_id);
        assert_eq!(published.branch.revision, 3);
    }

    #[test]
    fn drain_time_volume_barrier_conflicts_and_restarts_step1_with_fresh_roots() {
        let fixture = TestFixture::new();
        let mut volume = fixture.open_volume();
        let mut coordinator = fixture.coordinator();
        let mut boundary = BlockCutOnlyBoundary;

        let inner_control = fixture.control.inner.clone();
        let grant = fixture.grant;
        let mut store_for_drain = fixture.store.clone();
        let mut producers = CallbackProducerGate::new(move |attempt| {
            if attempt == 1 {
                let current = inner_control.get_branch(branch()).unwrap();
                let extent = versionfs_core::format::encode_object(
                    ObjectType::Extent,
                    &vec![0x55; EXTENT_BYTES as usize],
                )
                .unwrap();
                let extent_ref = ObjectRef::from_encoded(&extent).unwrap();
                store_for_drain.put_durable(extent_ref, &extent).unwrap();
                let next_volume_ref = versionfs_core::views::VolumeExtentMap::empty(
                    VolumeGeometry::new(4 * u64::from(EXTENT_BYTES)).unwrap(),
                    &mut store_for_drain,
                )
                .unwrap()
                .insert_extent(&mut store_for_drain, 0, extent_ref)
                .unwrap()
                .seal(&mut store_for_drain)
                .unwrap();
                let next_roots = LiveRoots {
                    volume: next_volume_ref,
                    logs: current.logs_root,
                    artifacts: current.artifacts_root,
                };
                inner_control
                    .begin_preparation(PreparationRequest {
                        operation_id: op(71),
                        branch_id: branch(),
                        expected_revision: current.revision,
                        grant,
                        attachment_id: attachment(),
                        volume_epoch: 1,
                        protection_id: ProtectionId::from_bytes([71; 16]),
                        kind: PreparationKind::VolumeBarrier,
                        expected_roots: Some(next_roots),
                        dependencies: vec![next_volume_ref],
                    })
                    .unwrap();
                inner_control
                    .commit_live_roots(op(71), current.revision, grant, next_roots, None)
                    .unwrap();
            }
            Ok(())
        });

        let published = coordinator
            .checkpoint(
                &mut volume,
                &mut producers,
                &mut boundary,
                &CheckpointRequest::attached(CaptureMode::BlockCut, 3_500),
                &[],
            )
            .expect("drain-time volume barrier conflict must restart Step 1");
        assert_eq!(producers.drain_calls, 2);
        let begin_ids = fixture.control.begin_ids.lock().unwrap().clone();
        assert_eq!(begin_ids.len(), 2);
        assert_ne!(begin_ids[0], begin_ids[1]);
        assert_eq!(published.branch.revision, 3);
        assert_eq!(
            volume
                .dispatch(BlockRequest::Read {
                    offset: 0,
                    length: 4096
                })
                .unwrap(),
            BlockResponse::Read(vec![0x55; 4096])
        );
    }

    #[test]
    fn resume_pending_capture_completes_sealed_publishing_after_uncommitted_unknown_outcome() {
        let fixture = TestFixture::new();
        *fixture.control.drop_next_publish_reply.lock().unwrap() = Some(false);
        let mut volume = fixture.open_volume();
        let mut coordinator = fixture.coordinator();
        let mut producers = NoopProducerGate;
        let mut boundary = RecordingBoundary::default();

        let err = coordinator
            .checkpoint(
                &mut volume,
                &mut producers,
                &mut boundary,
                &CheckpointRequest::attached(CaptureMode::FilesystemFrozen, 12_000),
                &[],
            )
            .expect_err("unknown publish outcome must pause before thawing");
        assert!(is_unknown_capture_error(&err));
        assert!(boundary.frozen);

        let resumed = coordinator
            .resume_pending_capture(&mut volume, &mut producers, &mut boundary, &[])
            .expect("resuming sealed Publishing capture should complete publication");
        let RecoveredCapture::Published(published) = resumed else {
            panic!("expected Published after resuming Publishing capture");
        };
        assert_eq!(
            fixture.control.get_branch(branch()).unwrap().head,
            published.generation_id
        );
        assert_eq!(volume.revision(), published.branch.revision);
        assert!(!boundary.frozen);
        assert_eq!(boundary.thaw_calls, 1);
        assert!(!volume.mutations_closed());
    }

    #[test]
    fn ownership_handover_fences_capture_and_never_adopts_replacement_owner() {
        let fixture = TestFixture::new();
        let mut volume = fixture.open_volume();
        let mut coordinator = fixture.coordinator();
        let mut boundary = BlockCutOnlyBoundary;

        let inner_control = fixture.control.inner.clone();
        let old_grant = fixture.grant;
        let mut producers = CallbackProducerGate::new(|_| {
            // Release old writer and acquire a new replacement writer during drain.
            inner_control.release_writer(op(80), old_grant).unwrap();
            let branch_state = inner_control.get_branch(branch()).unwrap();
            let new_grant = inner_control
                .acquire_writer(
                    op(81),
                    branch(),
                    branch_state.revision,
                    branch_state.writer_epoch,
                )
                .unwrap();
            inner_control
                .attach_volume(
                    op(82),
                    branch(),
                    branch_state.revision,
                    new_grant,
                    AttachmentId::from_bytes([82; 16]),
                    ProtectionId::from_bytes([82; 16]),
                )
                .unwrap();
            Ok(())
        });

        let error = coordinator
            .checkpoint(
                &mut volume,
                &mut producers,
                &mut boundary,
                &CheckpointRequest::attached(CaptureMode::BlockCut, 4_000),
                &[],
            )
            .expect_err("handover during capture must fence the original worker");
        assert!(matches!(
            error,
            CaptureError::Fenced | CaptureError::Control(ControlError::Fenced)
        ));
        assert!(volume.is_fenced());
        assert_eq!(producers.drain_calls, 1, "must not retry under new owner");
        assert!(
            volume
                .dispatch(BlockRequest::Write {
                    offset: 0,
                    data: &[0x99; 4096],
                    fua: false,
                })
                .is_err()
        );
    }

    #[test]
    fn freeze_failure_publishes_nothing_and_preserves_committed_child_flush() {
        let fixture = TestFixture::new();
        let initial_head = fixture.control.get_branch(branch()).unwrap().head;
        let mut volume = fixture.open_volume();
        let mut coordinator = fixture.coordinator();
        let mut producers = NoopProducerGate;
        let mut boundary = RecordingBoundary {
            freeze_writes: Some((4096, vec![0x77; 4096], true)),
            fail_freeze_after_flush: true,
            ..RecordingBoundary::default()
        };

        let err = coordinator
            .checkpoint(
                &mut volume,
                &mut producers,
                &mut boundary,
                &CheckpointRequest::attached(CaptureMode::FilesystemFrozen, 5_000),
                &[],
            )
            .expect_err("freeze failure must abort capture");
        assert!(matches!(err, CaptureError::FreezeFailed(_)));
        assert_eq!(boundary.thaw_calls, 1);
        assert!(coordinator.pending_capture().unwrap().is_none());

        let after = fixture.control.get_branch(branch()).unwrap();
        assert_eq!(after.head, initial_head, "no Generation may be published");
        assert!(after.capture.is_none(), "capture token must be released");
        assert_eq!(
            after.revision, 2,
            "child flush committed before freeze failure must remain committed"
        );
        assert_eq!(
            volume
                .dispatch(BlockRequest::Read {
                    offset: 4096,
                    length: 4096
                })
                .unwrap(),
            BlockResponse::Read(vec![0x77; 4096])
        );
    }

    #[test]
    fn clean_unmount_failure_never_substitutes_another_capture_mode() {
        let fixture = TestFixture::new();
        let initial_head = fixture.control.get_branch(branch()).unwrap().head;
        let mut volume = fixture.open_volume();
        let mut coordinator = fixture.coordinator();
        let mut producers = NoopProducerGate;
        let mut boundary = RecordingBoundary {
            fail_unmount: true,
            ..RecordingBoundary::default()
        };

        let err = coordinator
            .checkpoint(
                &mut volume,
                &mut producers,
                &mut boundary,
                &CheckpointRequest::attached(CaptureMode::FilesystemCleanUnmount, 6_000),
                &[],
            )
            .expect_err("failed clean unmount must abort capture");
        assert!(matches!(err, CaptureError::CleanUnmountFailed(_)));
        assert_eq!(boundary.thaw_calls, 0);
        assert_eq!(
            fixture.control.get_branch(branch()).unwrap().head,
            initial_head
        );
        assert!(
            fixture
                .control
                .get_branch(branch())
                .unwrap()
                .capture
                .is_none()
        );
    }

    #[test]
    fn watchdog_recovers_crash_while_frozen_without_undoing_child_flush() {
        let fixture = TestFixture::new();
        let failures = Arc::new(OneShotFailures::default());
        failures.arm(FailurePoint::FilesystemFreezeAfter);
        let mut volume = fixture.open_volume();
        let mut coordinator = fixture.coordinator_with_failures(failures);
        let mut producers = NoopProducerGate;
        let mut boundary = RecordingBoundary {
            freeze_writes: Some((0, vec![0xCC; 4096], true)),
            ..RecordingBoundary::default()
        };

        let err = coordinator
            .checkpoint(
                &mut volume,
                &mut producers,
                &mut boundary,
                &CheckpointRequest::attached(CaptureMode::FilesystemFrozen, 7_000),
                &[],
            )
            .expect_err("injected crash after freeze should interrupt coordinator");
        assert!(matches!(
            err,
            CaptureError::InjectedFailure(FailurePoint::FilesystemFreezeAfter)
        ));
        assert!(
            boundary.frozen,
            "filesystem remains frozen until watchdog reconciles"
        );
        assert!(
            coordinator
                .pending_capture()
                .unwrap()
                .unwrap()
                .freeze_active
        );
        drop(coordinator);

        // Watchdog opens capture journal outside the mounted volume and recovers.
        let mut watchdog = fixture.coordinator();
        let recovered = watchdog
            .recover_pending_capture(&mut volume, &mut producers, &mut boundary)
            .expect("watchdog recovery should abort capture and thaw");
        assert!(matches!(
            recovered,
            RecoveredCapture::Aborted {
                freeze_thawed: true,
                ..
            }
        ));
        assert!(!boundary.frozen);
        assert_eq!(boundary.thaw_calls, 1);
        assert!(watchdog.pending_capture().unwrap().is_none());
        let branch_after = fixture.control.get_branch(branch()).unwrap();
        assert!(branch_after.capture.is_none());
        assert_eq!(
            branch_after.revision, 2,
            "committed child flush must survive watchdog capture abort"
        );
    }

    #[test]
    fn unknown_begin_capture_reply_reconciles_with_same_operation_id() {
        for commit_first in [true, false] {
            let fixture = TestFixture::new();
            *fixture.control.drop_next_begin_reply.lock().unwrap() = Some(commit_first);
            let mut volume = fixture.open_volume();
            let mut coordinator = fixture.coordinator();
            let mut producers = NoopProducerGate;
            let mut boundary = BlockCutOnlyBoundary;

            let published = coordinator
                .checkpoint(
                    &mut volume,
                    &mut producers,
                    &mut boundary,
                    &CheckpointRequest::attached(CaptureMode::BlockCut, 8_000),
                    &[],
                )
                .expect("unknown begin_capture outcome should reconcile cleanly");
            assert_eq!(published.branch.head, published.generation_id);
            let ids = fixture.control.begin_ids.lock().unwrap().clone();
            if commit_first {
                assert_eq!(ids.len(), 1);
            } else {
                assert_eq!(ids.len(), 2);
                assert_eq!(ids[0], ids[1], "retry must reuse the exact same capture ID");
            }
        }
    }

    #[test]
    fn unknown_publish_reply_keeps_freeze_until_reconciled_then_syncs_and_thaws() {
        for commit_first in [true, false] {
            let fixture = TestFixture::new();
            *fixture.control.drop_next_publish_reply.lock().unwrap() = Some(commit_first);
            let mut volume = fixture.open_volume();
            let mut coordinator = fixture.coordinator();
            let mut producers = NoopProducerGate;
            let mut boundary = RecordingBoundary::default();

            let err = coordinator
                .checkpoint(
                    &mut volume,
                    &mut producers,
                    &mut boundary,
                    &CheckpointRequest::attached(CaptureMode::FilesystemFrozen, 9_000),
                    &[],
                )
                .expect_err("unknown publish outcome must pause before thawing");
            assert!(is_unknown_capture_error(&err));
            assert!(
                boundary.frozen,
                "must not thaw before reconciling publication"
            );
            assert!(
                volume.mutations_closed(),
                "must keep mutations closed until reconciled"
            );
            assert!(matches!(
                volume.dispatch(BlockRequest::Write {
                    offset: 0,
                    data: &[0x12; 4096],
                    fua: false,
                }),
                Err(BlockError::MutationAdmissionClosed)
            ));

            let recovered = coordinator
                .recover_pending_capture(&mut volume, &mut producers, &mut boundary)
                .expect("watchdog recovery should reconcile unknown publish outcome");
            assert!(!boundary.frozen);
            assert_eq!(boundary.thaw_calls, 1);
            assert!(!volume.mutations_closed());
            assert!(coordinator.pending_capture().unwrap().is_none());

            if commit_first {
                match recovered {
                    RecoveredCapture::Published(published) => {
                        assert_eq!(volume.revision(), published.branch.revision);
                        assert_eq!(
                            fixture.control.get_branch(branch()).unwrap().head,
                            published.generation_id
                        );
                    }
                    other => panic!("expected Published recovery, got {other:?}"),
                }
            } else {
                assert!(matches!(
                    recovered,
                    RecoveredCapture::Aborted {
                        freeze_thawed: true,
                        ..
                    }
                ));
                assert!(
                    fixture
                        .control
                        .get_branch(branch())
                        .unwrap()
                        .capture
                        .is_none()
                );
            }
        }
    }

    #[test]
    fn crash_between_publish_and_thaw_recovers_published_generation_and_syncs_baseline() {
        let fixture = TestFixture::new();
        let failures = Arc::new(OneShotFailures::default());
        failures.arm(FailurePoint::ReplyBeforeSend);
        let mut volume = fixture.open_volume();
        let mut coordinator = fixture.coordinator_with_failures(failures);
        let mut producers = NoopProducerGate;
        let mut boundary = RecordingBoundary::default();

        let err = coordinator
            .checkpoint(
                &mut volume,
                &mut producers,
                &mut boundary,
                &CheckpointRequest::attached(CaptureMode::FilesystemFrozen, 10_000),
                &[],
            )
            .expect_err("injected crash before local sync/thaw");
        assert!(matches!(
            err,
            CaptureError::InjectedFailure(FailurePoint::ReplyBeforeSend)
        ));
        assert!(boundary.frozen);
        drop(coordinator);

        let mut watchdog = fixture.coordinator();
        let recovered = watchdog
            .recover_pending_capture(&mut volume, &mut producers, &mut boundary)
            .expect("recovery should finish local sync and thaw");
        let RecoveredCapture::Published(published) = recovered else {
            panic!("expected Published recovery");
        };
        assert_eq!(volume.revision(), published.branch.revision);
        assert!(!boundary.frozen);
        assert!(!volume.mutations_closed());
    }

    #[test]
    fn provenance_recorded_and_missing_context_rejects_publication() {
        let fixture = TestFixture::new();
        let initial = fixture.control.get_branch(branch()).unwrap();
        let mut volume = fixture.open_volume();
        let mut coordinator = fixture.coordinator();
        let mut producers = NoopProducerGate;
        let mut boundary = BlockCutOnlyBoundary;

        let context = ProvenanceContext {
            recording_principal: PrincipalId::from_bytes([7; 16]),
            actor: Some("agent-1".to_owned()),
            execution_id: None,
            session_id: None,
            thread_id: None,
            task_id: None,
            workflow_operation_id: None,
            reason: Some("checkpoint test".to_owned()),
            trace_id: None,
            span_id: None,
            parent_span_id: None,
            required_logs_root: initial.logs_root,
            required_artifacts_root: initial.artifacts_root,
            optional_links: Vec::new(),
        };
        let encoded_context = context.encode().unwrap();
        let context_ref = ObjectRef::from_encoded(&encoded_context).unwrap();

        // Missing context object in store prevents publication cleanly.
        let mut missing_request = CheckpointRequest::attached(CaptureMode::BlockCut, 11_000);
        missing_request.provenance = ProvenanceBinding::Recorded {
            context_root: context_ref,
        };
        assert!(
            coordinator
                .checkpoint(
                    &mut volume,
                    &mut producers,
                    &mut boundary,
                    &missing_request,
                    &[]
                )
                .is_err()
        );
        assert_eq!(
            fixture.control.get_branch(branch()).unwrap().head,
            initial.head
        );
        assert!(
            fixture
                .control
                .get_branch(branch())
                .unwrap()
                .capture
                .is_none()
        );

        // Once durably stored, recorded provenance publishes and verifies cleanly.
        volume
            .store_mut()
            .put_durable(context_ref, &encoded_context)
            .unwrap();
        let published = coordinator
            .checkpoint(
                &mut volume,
                &mut producers,
                &mut boundary,
                &missing_request,
                &[],
            )
            .expect("durable recorded provenance should publish");
        let descriptor = fixture
            .store
            .objects
            .lock()
            .unwrap()
            .values()
            .find_map(|bytes| {
                decode_generation_descriptor(bytes)
                    .ok()
                    .filter(|d| d.generation_id() == published.generation_id)
            })
            .expect("descriptor stored");
        assert_eq!(descriptor.context_root(), Some(context_ref));
    }

    #[test]
    fn capture_journal_enforces_lock_permissions_and_checksums() {
        use std::os::unix::fs::PermissionsExt;

        let dir = TestDir::new();
        let journal_path = dir.0.join("capture-journal");
        let mut journal = FileCaptureJournal::open(&journal_path).unwrap();
        assert_eq!(
            fs::metadata(&journal_path).unwrap().permissions().mode() & 0o777,
            0o700
        );
        assert!(
            FileCaptureJournal::open(&journal_path).is_err(),
            "second owner must be rejected by exclusive lock"
        );
        let ids = journal.reserve_capture_ids().unwrap();
        let pending = PendingCapture {
            scope: BarrierJournalScope {
                workspace_id: WorkspaceId::from_bytes([1; 16]),
                principal_id: PrincipalId::from_bytes([2; 16]),
                object_store_scope: ObjectStoreScopeId::from_bytes([3; 16]),
            },
            capture_id: ids.capture_id,
            protection_id: ids.protection_id,
            generation_id: ids.generation_id,
            boundary_id: ids.boundary_id,
            branch_id: branch(),
            attachment_id: attachment(),
            volume_epoch: 1,
            writer_grant: WriterGrant {
                branch_id: branch(),
                writer_epoch: 1,
                owner_incarnation: OwnerIncarnation::from_u64(1),
                grant_id: op(2),
                expires_at: MonotonicTime::from_nanos(100),
            },
            mode: CaptureMode::FilesystemFrozen,
            advance_head: true,
            provenance: ProvenanceBinding::Unattributed,
            created_at_unix_ms: 42,
            expected_parent: GenerationId::from_bytes([5; 16]),
            pre_drain_revision: 1,
            pre_drain_roots: LiveRoots {
                volume: ObjectRef {
                    object_type: ObjectType::VolumeRoot,
                    object_id: ObjectId::from_bytes([1; 32]),
                    encoded_len: 32,
                },
                logs: ObjectRef {
                    object_type: ObjectType::LogMapRoot,
                    object_id: ObjectId::from_bytes([2; 32]),
                    encoded_len: 32,
                },
                artifacts: ObjectRef {
                    object_type: ObjectType::ArtifactMapRoot,
                    object_id: ObjectId::from_bytes([3; 32]),
                    encoded_len: 32,
                },
            },
            phase: CapturePhase::Freezing,
            freeze_active: true,
            seal_operation_id: None,
            seal_grant: None,
            sealed_revision: None,
            sealed_roots: None,
            renewal_operation_id: None,
            renewal_grant: None,
            descriptor_ref: None,
            publish_grant: None,
            published_revision: None,
        };
        journal.persist_pending(&pending).unwrap();
        assert_eq!(journal.load_pending().unwrap(), Some(pending));
        drop(journal);

        // Corrupt a byte in the pending capture file and verify checksum rejection.
        let pending_file = journal_path.join("pending-capture.v1");
        let mut bytes = fs::read(&pending_file).unwrap();
        bytes[20] ^= 0xFF;
        fs::write(&pending_file, bytes).unwrap();
        let mut reopened = FileCaptureJournal::open(&journal_path).unwrap();
        assert!(reopened.load_pending().is_err());
    }

    #[test]
    fn renew_if_due_replays_unknown_renewal_and_resumes_after_writer_heartbeat() {
        for commit_renewal_first in [true, false] {
            let fixture = TestFixture::new();
            let mut volume = fixture.open_volume();
            let mut coordinator = CheckpointCoordinator::new(
                FileCaptureJournal::open(&fixture.capture_dir).unwrap(),
                CaptureConfig {
                    max_acquisition_retries: DEFAULT_MAX_ACQUISITION_RETRIES,
                    renewal_interval: Duration::ZERO,
                },
            );
            let mut producers = NoopProducerGate;
            let mut boundary = RecordingBoundary::default();

            *fixture.control.drop_next_renew_reply.lock().unwrap() = Some(commit_renewal_first);
            let err = coordinator
                .checkpoint(
                    &mut volume,
                    &mut producers,
                    &mut boundary,
                    &CheckpointRequest::attached(CaptureMode::FilesystemFrozen, 13_000),
                    &[],
                )
                .expect_err(
                    "unknown renew_preparation outcome must pause before descriptor upload",
                );
            assert!(is_unknown_capture_error(&err));
            assert!(boundary.frozen);
            let pending = coordinator
                .pending_capture()
                .unwrap()
                .expect("pending capture must persist unresolved renewal_operation_id");
            let first_renewal_id = pending
                .renewal_operation_id
                .expect("renewal_operation_id must be recorded");
            assert!(
                pending.descriptor_ref.is_none(),
                "must pause before descriptor upload while renewal is unresolved"
            );

            // Advance clock and run an independent writer-lease renewal heartbeat.
            fixture._authority.now_nanos.store(5_000, Ordering::SeqCst);
            let _refreshed = fixture
                .control
                .inner
                .renew_writer(op(95), fixture.grant)
                .expect("independent writer heartbeat");

            // Resume replays the exact unresolved renewal ID first, refreshes
            // publish_grant to the live grant, uploads descriptor, and publishes!
            let resumed = coordinator
                .resume_pending_capture(&mut volume, &mut producers, &mut boundary, &[])
                .expect("resume must reconcile renewal and publish with refreshed grant");
            let RecoveredCapture::Published(published) = resumed else {
                panic!("expected Published after resuming with refreshed writer grant");
            };
            let renew_ids = fixture.control.renew_ids.lock().unwrap().clone();
            assert_eq!(renew_ids[0], first_renewal_id);
            if commit_renewal_first {
                // With renewal_interval = ZERO, the prior committed renewal is already due
                // for a fresh extension, and subsequent object verification reads also renew.
                assert!(renew_ids.len() >= 2);
                assert_ne!(renew_ids[1], first_renewal_id);
            } else {
                // Uncommitted R1 is replayed first, then rebound to R2 on stale writer grant.
                assert_eq!(
                    renew_ids[1], first_renewal_id,
                    "uncommitted renewal must replay the exact original renewal_operation_id first"
                );
                assert!(
                    renew_ids.len() >= 3,
                    "uncommitted renewal with stale grant rebinds to a fresh replacement ID"
                );
                assert_ne!(renew_ids[2], first_renewal_id);
            }
            assert_eq!(
                fixture.control.get_branch(branch()).unwrap().head,
                published.generation_id
            );
            assert!(!boundary.frozen);
            assert!(!volume.mutations_closed());
        }
    }

    #[test]
    fn unstarted_local_pending_barrier_and_aborted_parent_recover_and_thaw_cleanly() {
        let fixture = TestFixture::new();
        let mut volume = fixture.open_volume();
        let mut coordinator = fixture.coordinator();
        let mut producers = NoopProducerGate;
        let mut boundary = RecordingBoundary {
            freeze_writes: Some((0, vec![0x6A; 4096], false)),
            ..RecordingBoundary::default()
        };

        // Fail authorize_capture_child with a backend error during final volume.flush(),
        // leaving volume.pending populated while branch.capture.pending_child is None.
        *fixture.control.fail_next_authorize_backend.lock().unwrap() = true;
        let err = coordinator
            .checkpoint(
                &mut volume,
                &mut producers,
                &mut boundary,
                &CheckpointRequest::attached(CaptureMode::FilesystemFrozen, 14_000),
                &[],
            )
            .expect_err("backend failure on child authorization must pause capture");
        assert!(is_unknown_capture_error(&err));
        assert!(volume.has_pending_barrier());
        assert!(boundary.frozen);
        let capture_id = coordinator.pending_capture().unwrap().unwrap().capture_id;

        // Abort the parent capture externally (simulating expiry or forced handover)
        // while the local volume still holds its unstarted pending child barrier.
        fixture.control.inner.abort_operation(capture_id).unwrap();

        let recovered = coordinator
            .recover_pending_capture(&mut volume, &mut producers, &mut boundary)
            .expect("watchdog recovery must tombstone unstarted local child and thaw");
        assert!(matches!(
            recovered,
            RecoveredCapture::Aborted {
                freeze_thawed: true,
                ..
            }
        ));
        assert!(!boundary.frozen);
        assert!(!volume.has_pending_barrier());
        assert_eq!(
            volume
                .dispatch(BlockRequest::Read {
                    offset: 0,
                    length: 4096,
                })
                .unwrap(),
            BlockResponse::Read(vec![0x6A; 4096]),
            "uncommitted dirty bytes must remain local for salvage"
        );
    }

    #[test]
    fn branch_deleted_after_committed_child_barrier_fences_and_thaws_during_watchdog_recovery() {
        let fixture = TestFixture::new();
        let mut volume = fixture.open_volume();
        let mut coordinator = fixture.coordinator();
        let mut producers = NoopProducerGate;
        let mut boundary = RecordingBoundary {
            freeze_writes: Some((0, vec![0x7B; 4096], false)),
            ..RecordingBoundary::default()
        };

        // Child barrier commits on authority during final flush, then drops its reply.
        *fixture
            .control
            .drop_next_commit_live_roots_reply
            .lock()
            .unwrap() = Some(true);
        let err = coordinator
            .checkpoint(
                &mut volume,
                &mut producers,
                &mut boundary,
                &CheckpointRequest::attached(CaptureMode::FilesystemFrozen, 15_000),
                &[],
            )
            .expect_err("lost child commit reply must pause capture");
        assert!(is_unknown_capture_error(&err));
        assert!(volume.has_pending_barrier());
        assert!(boundary.frozen);

        // Before watchdog runs, an operator forces handover and deletes the branch!
        let current = fixture.control.get_branch(branch()).unwrap();
        let takeover = fixture
            .control
            .inner
            .force_acquire_writer(op(96), branch(), current.revision, current.writer_epoch)
            .expect("force handover");
        fixture
            .control
            .inner
            .delete_branch(op(97), branch(), current.revision, takeover, true)
            .expect("delete branch after child commit");

        let recovered = coordinator
            .recover_pending_capture(&mut volume, &mut producers, &mut boundary)
            .expect("watchdog must fence volume and thaw even after branch deletion");
        assert!(matches!(
            recovered,
            RecoveredCapture::Aborted {
                freeze_thawed: true,
                ..
            }
        ));
        assert!(!boundary.frozen);
        assert!(volume.is_fenced());

        // Also verify checkpoint() on a deleted branch immediately fences a live volume.
        let fresh_volume = TestFixture::new();
        let mut vol2 = fresh_volume.open_volume();
        fresh_volume
            .control
            .inner
            .delete_branch(op(98), branch(), 1, fresh_volume.grant, false)
            .unwrap();
        let mut coord2 = fresh_volume.coordinator();
        assert!(matches!(
            coord2.checkpoint(
                &mut vol2,
                &mut NoopProducerGate,
                &mut BlockCutOnlyBoundary,
                &CheckpointRequest::attached(CaptureMode::BlockCut, 15_500),
                &[],
            ),
            Err(CaptureError::Fenced)
        ));
        assert!(vol2.is_fenced());
    }

    #[test]
    fn backend_error_during_begin_capture_preserves_journal_for_watchdog_recovery() {
        let fixture = TestFixture::new();
        let mut volume = fixture.open_volume();
        let mut coordinator = fixture.coordinator();
        let mut producers = NoopProducerGate;
        let mut boundary = BlockCutOnlyBoundary;

        // begin_capture commits the capture token, then both begin_capture and
        // get_operation fail with Backend transport errors.
        *fixture.control.fail_next_begin_backend.lock().unwrap() = Some(true);
        let err = coordinator
            .checkpoint(
                &mut volume,
                &mut producers,
                &mut boundary,
                &CheckpointRequest::attached(CaptureMode::BlockCut, 16_000),
                &[],
            )
            .expect_err("transport outage during begin_capture must return uncertain error");
        assert!(is_unknown_capture_error(&err));
        assert!(
            coordinator.pending_capture().unwrap().is_some(),
            "pending capture journal must be preserved across backend outage"
        );
        assert!(
            fixture
                .control
                .get_branch(branch())
                .unwrap()
                .capture
                .is_some(),
            "capture token is active on authority"
        );

        let recovered = coordinator
            .recover_pending_capture(&mut volume, &mut producers, &mut boundary)
            .expect("watchdog recovery must abort leaked capture token once backend recovers");
        assert!(matches!(
            recovered,
            RecoveredCapture::Aborted {
                freeze_thawed: false,
                ..
            }
        ));
        assert!(
            fixture
                .control
                .get_branch(branch())
                .unwrap()
                .capture
                .is_none()
        );
        assert!(coordinator.pending_capture().unwrap().is_none());
    }

    #[test]
    fn writer_lease_renewal_during_drain_restarts_step1_instead_of_fencing() {
        let fixture = TestFixture::new();
        let mut volume = fixture.open_volume();
        let mut coordinator = fixture.coordinator();
        let mut boundary = BlockCutOnlyBoundary;

        let inner_control = fixture.control.inner.clone();
        let authority = fixture._authority.clone();
        let initial_grant = fixture.grant;
        let mut producers = CallbackProducerGate::new(move |attempt| {
            if attempt == 1 {
                authority.now_nanos.store(5_000, Ordering::SeqCst);
                inner_control
                    .renew_writer(op(99), initial_grant)
                    .expect("independent writer heartbeat during drain");
            }
            Ok(())
        });

        let published = coordinator
            .checkpoint(
                &mut volume,
                &mut producers,
                &mut boundary,
                &CheckpointRequest::attached(CaptureMode::BlockCut, 17_000),
                &[],
            )
            .expect("writer lease renewal during drain should restart Step 1 and succeed");
        assert_eq!(producers.drain_calls, 2);
        assert!(!volume.is_fenced());
        assert_eq!(published.branch.head, published.generation_id);
    }

    #[test]
    fn sync_branch_state_rejects_already_fenced_volume_even_before_handover() {
        let fixture = TestFixture::new();
        let mut volume = fixture.open_volume();
        volume.mark_fenced();
        let current = fixture.control.get_branch(branch()).unwrap();
        assert!(matches!(
            volume.sync_branch_state(&current),
            Err(BarrierError::Fenced)
        ));
        assert!(volume.is_fenced());
    }

    #[test]
    fn concurrent_writer_heartbeats_during_seal_renew_and_publish_do_not_fence_capture() {
        let fixture = TestFixture::new();
        let mut volume = fixture.open_volume();
        let mut coordinator = CheckpointCoordinator::new(
            FileCaptureJournal::open(&fixture.capture_dir).unwrap(),
            CaptureConfig {
                max_acquisition_retries: DEFAULT_MAX_ACQUISITION_RETRIES,
                renewal_interval: Duration::ZERO,
            },
        );
        let mut producers = NoopProducerGate;
        let mut boundary = RecordingBoundary::default();

        // Tick independent writer heartbeat right before seal_capture, right before
        // renew_preparation, and right before publish_capture.
        let inner1 = fixture.control.inner.clone();
        let auth1 = fixture._authority.clone();
        let grant0 = fixture.grant;
        *fixture.control.on_next_seal.lock().unwrap() = Some(Box::new(move || {
            auth1.now_nanos.store(1_000, Ordering::SeqCst);
            inner1.renew_writer(op(180), grant0).unwrap();
        }));

        let inner2 = fixture.control.inner.clone();
        let auth2 = fixture._authority.clone();
        *fixture.control.on_next_renew.lock().unwrap() = Some(Box::new(move || {
            let current_grant = inner2.get_branch(branch()).unwrap().writer.unwrap();
            auth2.now_nanos.store(2_000, Ordering::SeqCst);
            inner2.renew_writer(op(181), current_grant).unwrap();
        }));

        let inner3 = fixture.control.inner.clone();
        let auth3 = fixture._authority.clone();
        *fixture.control.on_next_publish.lock().unwrap() = Some(Box::new(move || {
            let current_grant = inner3.get_branch(branch()).unwrap().writer.unwrap();
            auth3.now_nanos.store(3_000, Ordering::SeqCst);
            inner3.renew_writer(op(182), current_grant).unwrap();
        }));

        let published = coordinator
            .checkpoint(
                &mut volume,
                &mut producers,
                &mut boundary,
                &CheckpointRequest::attached(CaptureMode::FilesystemFrozen, 18_000),
                &[],
            )
            .expect("concurrent writer heartbeats must never fence a healthy capture");
        assert_eq!(
            fixture.control.get_branch(branch()).unwrap().head,
            published.generation_id
        );
        assert!(!boundary.frozen);
        assert!(!volume.is_fenced());
    }

    #[test]
    fn restarted_coordinator_extends_lease_after_stale_committed_renewal_replay() {
        let fixture = TestFixture::new();
        let mut volume = fixture.open_volume();
        let mut coordinator = CheckpointCoordinator::new(
            FileCaptureJournal::open(&fixture.capture_dir).unwrap(),
            CaptureConfig {
                max_acquisition_retries: DEFAULT_MAX_ACQUISITION_RETRIES,
                renewal_interval: Duration::ZERO,
            },
        );
        let mut producers = NoopProducerGate;
        let mut boundary = RecordingBoundary::default();

        // Commit renewal R1 at t = 0 with a lost reply.
        *fixture.control.drop_next_renew_reply.lock().unwrap() = Some(true);
        let err = coordinator
            .checkpoint(
                &mut volume,
                &mut producers,
                &mut boundary,
                &CheckpointRequest::attached(CaptureMode::FilesystemFrozen, 19_500),
                &[],
            )
            .expect_err("pause on lost renewal reply");
        assert!(is_unknown_capture_error(&err));
        let capture_id = coordinator.pending_capture().unwrap().unwrap().capture_id;
        let OperationState::Preparing {
            expires_at: initial_r1_deadline,
            ..
        } = fixture.control.get_operation(capture_id).unwrap()
        else {
            panic!("capture should be preparing");
        };
        drop(coordinator);

        // Advance clock by 15s (within 30s writer lease) and open a fresh restarted
        // coordinator (`last_renewal == None`). Resume must clear the committed R1
        // and issue a fresh R2 that extends the preparation deadline past R1!
        fixture
            ._authority
            .now_nanos
            .store(15_000_000_000, Ordering::SeqCst);
        let mut restarted = fixture.coordinator();
        *fixture.control.drop_next_publish_reply.lock().unwrap() = Some(false);
        let err2 = restarted
            .resume_pending_capture(&mut volume, &mut producers, &mut boundary, &[])
            .expect_err("pause at publish to inspect extended preparation deadline");
        assert!(is_unknown_capture_error(&err2));
        let OperationState::Preparing {
            expires_at: extended_deadline,
            ..
        } = fixture.control.get_operation(capture_id).unwrap()
        else {
            panic!("capture should still be preparing");
        };
        assert!(
            extended_deadline > initial_r1_deadline,
            "restarted coordinator must extend the preparation deadline beyond the stale R1 replay"
        );
        let resumed = restarted
            .resume_pending_capture(&mut volume, &mut producers, &mut boundary, &[])
            .expect("finish resumed capture");
        assert!(matches!(resumed, RecoveredCapture::Published(_)));
        assert!(!boundary.frozen);
    }

    #[test]
    fn failed_barrier_journal_replacement_does_not_wedge_frozen_capture_recovery() {
        struct FailNextReplaceJournal {
            inner: FileBarrierJournal,
            failures: Arc<OneShotFailures>,
            fail_next_replace: bool,
        }

        impl BarrierJournal for FailNextReplaceJournal {
            fn reserve_barrier_ids(&mut self) -> Result<crate::barrier::BarrierIds, String> {
                self.inner.reserve_barrier_ids()
            }

            fn reserve_operation_id(&mut self) -> Result<OperationId, String> {
                self.inner.reserve_operation_id()
            }

            fn load_pending(&mut self) -> Result<Option<crate::barrier::PendingBarrier>, String> {
                self.inner.load_pending()
            }

            fn persist_pending(
                &mut self,
                barrier: &crate::barrier::PendingBarrier,
            ) -> Result<(), String> {
                self.inner.persist_pending(barrier)
            }

            fn replace_pending(
                &mut self,
                expected_operation_id: OperationId,
                replacement: &crate::barrier::PendingBarrier,
            ) -> Result<(), String> {
                if self.fail_next_replace {
                    self.fail_next_replace = false;
                    self.failures.arm(FailurePoint::JournalBeforePublish);
                }
                self.inner
                    .replace_pending(expected_operation_id, replacement)
            }

            fn finish_pending(&mut self, operation_id: OperationId) -> Result<(), String> {
                self.inner.finish_pending(operation_id)
            }
        }

        let fixture = TestFixture::new();
        let barrier_failures = Arc::new(OneShotFailures::default());
        let journal = FailNextReplaceJournal {
            inner: FileBarrierJournal::open_with(&fixture.barrier_dir, barrier_failures.clone())
                .unwrap(),
            failures: barrier_failures,
            fail_next_replace: true,
        };
        let mut volume = DurableVolume::open(
            fixture.store.clone(),
            fixture.control.clone(),
            journal,
            branch(),
            fixture.grant,
            attachment(),
            EXTENT_BYTES as usize * 2,
        )
        .unwrap();
        let mut coordinator = fixture.coordinator();
        let mut producers = NoopProducerGate;

        // First fail a child barrier authorization so op1 is persisted in FileBarrierJournal,
        // abort op1 externally, then let restart_pending fail specifically inside
        // replace_pending (leaving op1 on disk and op2 in memory).
        let mut boundary = RecordingBoundary {
            freeze_writes: Some((0, vec![0x88; 4096], false)),
            ..RecordingBoundary::default()
        };
        *fixture.control.fail_next_authorize_backend.lock().unwrap() = true;
        let err = coordinator
            .checkpoint(
                &mut volume,
                &mut producers,
                &mut boundary,
                &CheckpointRequest::attached(CaptureMode::FilesystemFrozen, 20_000),
                &[],
            )
            .expect_err("pause with op1 persisted in barrier journal");
        assert!(is_unknown_capture_error(&err));
        assert!(boundary.frozen);
        let op1 = volume.pending_barrier_operation_id().unwrap();

        // Abort op1 so the next volume.flush() enters restart_pending (creating op2
        // replacing op1), and FailNextReplaceJournal injects JournalBeforePublish on
        // replace_pending so disk still holds op1!
        fixture.control.inner.abort_operation(op1).unwrap();
        assert!(matches!(volume.flush(), Err(BarrierError::Journal(_))));
        let op2 = volume.pending_barrier_operation_id().unwrap();
        assert_ne!(op1, op2);

        // Watchdog recovery must reconcile op2/op1 cleanly without mismatching on disk,
        // abort the capture, preserve local dirty bytes, and thaw the frozen filesystem!
        let recovered = coordinator
            .recover_pending_capture(&mut volume, &mut producers, &mut boundary)
            .expect("watchdog must recover cleanly even after failed journal replacement");
        assert!(matches!(
            recovered,
            RecoveredCapture::Aborted {
                freeze_thawed: true,
                ..
            }
        ));
        assert!(!boundary.frozen);
        assert!(!volume.has_pending_barrier());
        assert_eq!(
            volume
                .dispatch(BlockRequest::Read {
                    offset: 0,
                    length: 4096,
                })
                .unwrap(),
            BlockResponse::Read(vec![0x88; 4096])
        );
    }

    #[test]
    fn aborted_or_immediate_committed_renewal_on_same_coordinator_reconciles_cleanly() {
        // Case 1: Aborted unresolved renewal on the same coordinator instance clears
        // R1 and issues R2 instead of aborting the sealed capture.
        let fixture = TestFixture::new();
        let mut volume = fixture.open_volume();
        let mut coordinator = CheckpointCoordinator::new(
            FileCaptureJournal::open(&fixture.capture_dir).unwrap(),
            CaptureConfig {
                max_acquisition_retries: DEFAULT_MAX_ACQUISITION_RETRIES,
                renewal_interval: Duration::ZERO,
            },
        );
        let mut producers = NoopProducerGate;
        let mut boundary = RecordingBoundary::default();
        *fixture.control.drop_next_renew_reply.lock().unwrap() = Some(false);
        let err = coordinator
            .checkpoint(
                &mut volume,
                &mut producers,
                &mut boundary,
                &CheckpointRequest::attached(CaptureMode::FilesystemFrozen, 21_000),
                &[],
            )
            .expect_err("pause on uncommitted unknown renewal");
        assert!(is_unknown_capture_error(&err));
        let r1 = coordinator
            .pending_capture()
            .unwrap()
            .unwrap()
            .renewal_operation_id
            .unwrap();
        fixture.control.inner.abort_operation(r1).unwrap();

        let resumed = coordinator
            .resume_pending_capture(&mut volume, &mut producers, &mut boundary, &[])
            .expect("aborted R1 must clear and issue fresh R2 on same coordinator");
        assert!(matches!(resumed, RecoveredCapture::Published(_)));
        assert!(!boundary.frozen);

        // Case 2: Immediate resume of a committed renewal within renewal_interval does NOT
        // allocate a second renewal ID (no double extension).
        let fixture2 = TestFixture::new();
        let mut volume2 = fixture2.open_volume();
        let mut coord2 = CheckpointCoordinator::new(
            FileCaptureJournal::open(&fixture2.capture_dir).unwrap(),
            CaptureConfig {
                max_acquisition_retries: DEFAULT_MAX_ACQUISITION_RETRIES,
                renewal_interval: Duration::ZERO,
            },
        );
        let mut boundary2 = RecordingBoundary::default();
        *fixture2.control.drop_next_renew_reply.lock().unwrap() = Some(true);
        let err2 = coord2
            .checkpoint(
                &mut volume2,
                &mut producers,
                &mut boundary2,
                &CheckpointRequest::attached(CaptureMode::FilesystemFrozen, 21_500),
                &[],
            )
            .expect_err("pause on committed unknown renewal");
        assert!(is_unknown_capture_error(&err2));
        // Switch coordinator to normal 3-minute renewal interval before immediate resume.
        coord2.config.renewal_interval = DEFAULT_RENEWAL_INTERVAL;
        let resumed2 = coord2
            .resume_pending_capture(&mut volume2, &mut producers, &mut boundary2, &[])
            .expect("immediate resume of committed R1 must succeed without extending twice");
        assert!(matches!(resumed2, RecoveredCapture::Published(_)));
        assert_eq!(
            fixture2.control.renew_ids.lock().unwrap().len(),
            1,
            "must not extend twice on immediate retry of a committed renewal"
        );
    }

    #[test]
    fn transient_finish_pending_failure_does_not_double_count_committed_barriers() {
        struct FailOnceFinishJournal {
            inner: FileBarrierJournal,
            fail_next_finish: bool,
        }

        impl BarrierJournal for FailOnceFinishJournal {
            fn reserve_barrier_ids(&mut self) -> Result<crate::barrier::BarrierIds, String> {
                self.inner.reserve_barrier_ids()
            }

            fn reserve_operation_id(&mut self) -> Result<OperationId, String> {
                self.inner.reserve_operation_id()
            }

            fn load_pending(&mut self) -> Result<Option<crate::barrier::PendingBarrier>, String> {
                self.inner.load_pending()
            }

            fn persist_pending(
                &mut self,
                barrier: &crate::barrier::PendingBarrier,
            ) -> Result<(), String> {
                self.inner.persist_pending(barrier)
            }

            fn replace_pending(
                &mut self,
                expected_operation_id: OperationId,
                replacement: &crate::barrier::PendingBarrier,
            ) -> Result<(), String> {
                self.inner
                    .replace_pending(expected_operation_id, replacement)
            }

            fn finish_pending(&mut self, operation_id: OperationId) -> Result<(), String> {
                if self.fail_next_finish {
                    self.fail_next_finish = false;
                    return Err("transient journal directory sync failure".to_owned());
                }
                self.inner.finish_pending(operation_id)
            }
        }

        let fixture = TestFixture::new();
        let journal = FailOnceFinishJournal {
            inner: FileBarrierJournal::open(&fixture.barrier_dir).unwrap(),
            fail_next_finish: true,
        };
        let mut volume = DurableVolume::open(
            fixture.store.clone(),
            fixture.control.clone(),
            journal,
            branch(),
            fixture.grant,
            attachment(),
            EXTENT_BYTES as usize * 2,
        )
        .unwrap();

        volume
            .dispatch(BlockRequest::Write {
                offset: 0,
                data: &[0x21; 4096],
                fua: false,
            })
            .unwrap();
        assert!(matches!(volume.flush(), Err(BarrierError::Journal(_))));
        assert_eq!(
            volume.committed_barriers(),
            0,
            "failed finish_pending must not increment committed_barriers before clearing pending"
        );
        volume
            .flush()
            .expect("retry should reconcile committed barrier");
        assert_eq!(
            volume.committed_barriers(),
            1,
            "committed barrier must be counted exactly once"
        );
        assert_eq!(volume.revision(), 2);
    }

    #[test]
    fn expired_capture_preparation_during_child_flush_does_not_fence_healthy_attachment() {
        let fixture = TestFixture::new();
        let mut volume = fixture.open_volume();
        let mut coordinator = fixture.coordinator();
        let mut producers = NoopProducerGate;

        // Custom boundary that keeps writer + attachment renewed while advancing the
        // authority clock past the 10-minute capture preparation deadline during freeze!
        struct SlowFreezeBoundary {
            authority: Arc<MemoryAuthority>,
            inner: Arc<WorkspaceControl<MemoryAuthority>>,
            initial_grant: WriterGrant,
            thaw_calls: usize,
        }

        impl FilesystemBoundary for SlowFreezeBoundary {
            fn freeze(&mut self, device: &mut dyn BlockDevice) -> Result<(), String> {
                device
                    .dispatch(BlockRequest::Write {
                        offset: 0,
                        data: &[0x4F; 4096],
                        fua: false,
                    })
                    .map_err(|error| error.to_string())?;
                // Keep writer grant and attachment base alive in 20s/4m steps until
                // the 10-minute (600s) capture preparation deadline has expired.
                let mut now = 0_u64;
                let mut grant = self.initial_grant;
                let mut op_counter = 200_u8;
                while now < 605_000_000_000 {
                    now += 20_000_000_000;
                    self.authority.now_nanos.store(now, Ordering::SeqCst);
                    grant = self
                        .inner
                        .renew_writer(op(op_counter), grant)
                        .map_err(|error| error.to_string())?;
                    op_counter = op_counter.wrapping_add(1);
                    if now.is_multiple_of(240_000_000_000) {
                        let att = self
                            .inner
                            .get_branch(branch())
                            .map_err(|error| error.to_string())?
                            .attachment
                            .unwrap();
                        self.inner
                            .renew_attachment_base(
                                op(op_counter),
                                branch(),
                                att.attachment_id,
                                att.volume_epoch,
                                att.protection_id,
                                grant,
                            )
                            .map_err(|error| error.to_string())?;
                        op_counter = op_counter.wrapping_add(1);
                    }
                }
                Ok(())
            }

            fn thaw(&mut self) -> Result<(), String> {
                self.thaw_calls += 1;
                Ok(())
            }

            fn clean_unmount(&mut self, _device: &mut dyn BlockDevice) -> Result<(), String> {
                Ok(())
            }
        }

        let mut boundary = SlowFreezeBoundary {
            authority: fixture._authority.clone(),
            inner: fixture.control.inner.clone(),
            initial_grant: fixture.grant,
            thaw_calls: 0,
        };

        let err = coordinator
            .checkpoint(
                &mut volume,
                &mut producers,
                &mut boundary,
                &CheckpointRequest::attached(CaptureMode::FilesystemFrozen, 22_000),
                &[],
            )
            .expect_err("expired capture preparation must abort capture");
        assert!(matches!(
            err,
            CaptureError::Barrier(BarrierError::Control(ControlError::Expired))
        ));
        assert_eq!(boundary.thaw_calls, 1);
        assert!(
            !volume.is_fenced(),
            "expired capture preparation must not permanently fence a healthy attachment"
        );
        // Subsequent normal flush on the healthy attachment must still succeed!
        volume
            .flush()
            .expect("healthy attachment must remain writable and flushable after capture expiry");
        assert_eq!(
            volume
                .dispatch(BlockRequest::Read {
                    offset: 0,
                    length: 4096,
                })
                .unwrap(),
            BlockResponse::Read(vec![0x4F; 4096])
        );
    }

    #[test]
    fn authority_owner_restart_during_acquire_fences_volume() {
        let fixture = TestFixture::new();
        let mut volume = fixture.open_volume();
        let mut coordinator = fixture.coordinator();
        let mut boundary = BlockCutOnlyBoundary;

        let authority = fixture._authority.clone();
        let override_inner = fixture.control.override_inner.clone();
        let mut producers = CallbackProducerGate::new(move |attempt| {
            if attempt == 1 {
                // Simulate an authority owner restart while branch.writer and
                // branch.attachment still carry incarnation-1 records, and the
                // client reconnects to the restarted incarnation-2 authority.
                let restarted_owner = start_authority_owner(authority.as_ref(), op(251)).unwrap();
                *override_inner.lock().unwrap() = Some(Arc::new(WorkspaceControl::new(
                    authority.clone(),
                    WorkspaceId::from_bytes([1; 16]),
                    ObjectStoreScopeId::from_bytes([0xC1; 16]),
                    PrincipalId::from_bytes([7; 16]),
                    restarted_owner,
                )));
            }
            Ok(())
        });

        let err = coordinator
            .checkpoint(
                &mut volume,
                &mut producers,
                &mut boundary,
                &CheckpointRequest::attached(CaptureMode::BlockCut, 23_000),
                &[],
            )
            .expect_err("authority owner restart during acquire must fence capture");
        assert!(matches!(err, CaptureError::Fenced));
        assert!(
            volume.is_fenced(),
            "volume must be marked fenced when begin_capture returns Fenced after owner restart"
        );
        assert!(matches!(
            volume.dispatch(BlockRequest::Write {
                offset: 0,
                data: &[0x77; 4096],
                fua: false,
            }),
            Err(BlockError::Barrier(_))
        ));
        assert!(matches!(volume.flush(), Err(BarrierError::Fenced)));
    }

    #[test]
    fn authority_owner_restart_during_renew_fences_volume_even_after_abort_recovery() {
        let fixture = TestFixture::new();
        let mut volume = fixture.open_volume();
        let mut coordinator = CheckpointCoordinator::new(
            FileCaptureJournal::open(&fixture.capture_dir).unwrap(),
            CaptureConfig {
                renewal_interval: Duration::ZERO,
                ..CaptureConfig::default()
            },
        );
        let mut producers = NoopProducerGate;
        let mut boundary = BlockCutOnlyBoundary;

        volume
            .dispatch(BlockRequest::Write {
                offset: 0,
                data: &[0x66; 4096],
                fua: false,
            })
            .unwrap();

        let authority = fixture._authority.clone();
        let override_inner = fixture.control.override_inner.clone();
        *fixture.control.on_next_renew.lock().unwrap() = Some(Box::new(move || {
            // Restart the authority owner right before renew_preparation while
            // branch.writer/branch.attachment still hold incarnation-1 records,
            // and switch the control handle to the restarted incarnation-2 owner
            // so abort_operation + finish_aborted_recovery run cleanly.
            let restarted_owner = start_authority_owner(authority.as_ref(), op(252)).unwrap();
            *override_inner.lock().unwrap() = Some(Arc::new(WorkspaceControl::new(
                authority.clone(),
                WorkspaceId::from_bytes([1; 16]),
                ObjectStoreScopeId::from_bytes([0xC1; 16]),
                PrincipalId::from_bytes([7; 16]),
                restarted_owner,
            )));
        }));

        let err = coordinator
            .checkpoint(
                &mut volume,
                &mut producers,
                &mut boundary,
                &CheckpointRequest::attached(CaptureMode::BlockCut, 24_000),
                &[],
            )
            .expect_err("authority owner restart during renew_preparation must fence capture");
        assert!(matches!(err, CaptureError::Fenced));
        assert!(
            coordinator.pending_capture().unwrap().is_none(),
            "aborted capture journal should be cleared cleanly on restarted authority"
        );
        assert!(
            volume.is_fenced(),
            "volume must remain fenced after abort recovery following owner restart"
        );
        assert!(matches!(
            volume.dispatch(BlockRequest::Write {
                offset: 4096,
                data: &[0x88; 4096],
                fua: false,
            }),
            Err(BlockError::Barrier(_))
        ));
        assert!(matches!(volume.flush(), Err(BarrierError::Fenced)));
    }

    #[test]
    fn authority_owner_restart_during_step2_flush_and_freeze_fences_volume() {
        // Case 1: Authority owner restarts right before Step-2 volume.flush() in BlockCut mode.
        {
            let fixture = TestFixture::new();
            let mut volume = fixture.open_volume();
            let mut coordinator = fixture.coordinator();
            let mut producers = NoopProducerGate;

            volume
                .dispatch(BlockRequest::Write {
                    offset: 0,
                    data: &[0x55; 4096],
                    fua: false,
                })
                .unwrap();

            // Restart the authority owner and reconnect to incarnation 2 right before
            // the capture child barrier flushes in Step 2.
            struct RestartOnFreezeOrFlush {
                authority: Arc<MemoryAuthority>,
                override_inner: Arc<Mutex<Option<Arc<WorkspaceControl<MemoryAuthority>>>>>,
                thaw_calls: usize,
            }

            impl FilesystemBoundary for RestartOnFreezeOrFlush {
                fn freeze(&mut self, device: &mut dyn BlockDevice) -> Result<(), String> {
                    device
                        .dispatch(BlockRequest::Write {
                            offset: 4096,
                            data: &[0x66; 4096],
                            fua: false,
                        })
                        .map_err(|error| error.to_string())?;
                    let restarted_owner =
                        start_authority_owner(self.authority.as_ref(), op(253)).unwrap();
                    *self.override_inner.lock().unwrap() = Some(Arc::new(WorkspaceControl::new(
                        self.authority.clone(),
                        WorkspaceId::from_bytes([1; 16]),
                        ObjectStoreScopeId::from_bytes([0xC1; 16]),
                        PrincipalId::from_bytes([7; 16]),
                        restarted_owner,
                    )));
                    device
                        .dispatch(BlockRequest::Flush)
                        .map_err(|error| error.to_string())?;
                    Ok(())
                }

                fn thaw(&mut self) -> Result<(), String> {
                    self.thaw_calls += 1;
                    Ok(())
                }

                fn clean_unmount(&mut self, _device: &mut dyn BlockDevice) -> Result<(), String> {
                    Ok(())
                }
            }

            // First verify Freeze boundary flushing via BlockDevice::dispatch(Flush) fences volume
            // even though BlockDevice stringifies the BarrierError.
            let mut boundary = RestartOnFreezeOrFlush {
                authority: fixture._authority.clone(),
                override_inner: fixture.control.override_inner.clone(),
                thaw_calls: 0,
            };

            let err = coordinator
                .checkpoint(
                    &mut volume,
                    &mut producers,
                    &mut boundary,
                    &CheckpointRequest::attached(CaptureMode::FilesystemFrozen, 25_000),
                    &[],
                )
                .expect_err("owner restart during freeze flush must fail capture");
            assert!(matches!(err, CaptureError::FreezeFailed(_)));
            assert_eq!(boundary.thaw_calls, 1);
            assert!(coordinator.pending_capture().unwrap().is_none());
            assert!(
                volume.is_fenced(),
                "volume must be fenced after owner restart during freeze BlockRequest::Flush"
            );
            assert!(matches!(
                volume.dispatch(BlockRequest::Write {
                    offset: 8192,
                    data: &[0x99; 4096],
                    fua: false,
                }),
                Err(BlockError::Barrier(_))
            ));
            assert!(matches!(volume.flush(), Err(BarrierError::Fenced)));
        }

        // Case 2: Authority owner restarts right before Step-2 volume.flush() directly
        // (and also covers standalone DurableVolume::flush() fencing on owner restart).
        {
            let fixture = TestFixture::new();
            let mut volume = fixture.open_volume();
            let mut coordinator = fixture.coordinator();
            let mut producers = NoopProducerGate;

            volume
                .dispatch(BlockRequest::Write {
                    offset: 0,
                    data: &[0x55; 4096],
                    fua: false,
                })
                .unwrap();

            struct RestartBeforeStep2Flush {
                authority: Arc<MemoryAuthority>,
                override_inner: Arc<Mutex<Option<Arc<WorkspaceControl<MemoryAuthority>>>>>,
                thaw_calls: usize,
            }

            impl FilesystemBoundary for RestartBeforeStep2Flush {
                fn freeze(&mut self, _device: &mut dyn BlockDevice) -> Result<(), String> {
                    let restarted_owner =
                        start_authority_owner(self.authority.as_ref(), op(254)).unwrap();
                    *self.override_inner.lock().unwrap() = Some(Arc::new(WorkspaceControl::new(
                        self.authority.clone(),
                        WorkspaceId::from_bytes([1; 16]),
                        ObjectStoreScopeId::from_bytes([0xC1; 16]),
                        PrincipalId::from_bytes([7; 16]),
                        restarted_owner,
                    )));
                    Ok(())
                }

                fn thaw(&mut self) -> Result<(), String> {
                    self.thaw_calls += 1;
                    Ok(())
                }

                fn clean_unmount(&mut self, _device: &mut dyn BlockDevice) -> Result<(), String> {
                    Ok(())
                }
            }

            let mut boundary = RestartBeforeStep2Flush {
                authority: fixture._authority.clone(),
                override_inner: fixture.control.override_inner.clone(),
                thaw_calls: 0,
            };

            let err = coordinator
                .checkpoint(
                    &mut volume,
                    &mut producers,
                    &mut boundary,
                    &CheckpointRequest::attached(CaptureMode::FilesystemFrozen, 26_000),
                    &[],
                )
                .expect_err("owner restart before Step-2 volume.flush must fence capture");
            assert!(matches!(err, CaptureError::Fenced));
            assert_eq!(boundary.thaw_calls, 1);
            assert!(coordinator.pending_capture().unwrap().is_none());
            assert!(
                volume.is_fenced(),
                "volume must be fenced after owner restart during Step-2 volume.flush"
            );
            assert!(matches!(
                volume.dispatch(BlockRequest::Write {
                    offset: 4096,
                    data: &[0xAA; 4096],
                    fua: false,
                }),
                Err(BlockError::Barrier(_))
            ));
            assert!(matches!(volume.flush(), Err(BarrierError::Fenced)));
        }

        // Case 3: Standalone DurableVolume::flush() (no capture) fences on authority owner restart.
        {
            let fixture = TestFixture::new();
            let mut volume = fixture.open_volume();
            volume
                .dispatch(BlockRequest::Write {
                    offset: 0,
                    data: &[0x77; 4096],
                    fua: false,
                })
                .unwrap();
            let restarted_owner =
                start_authority_owner(fixture._authority.as_ref(), op(255)).unwrap();
            *fixture.control.override_inner.lock().unwrap() =
                Some(Arc::new(WorkspaceControl::new(
                    fixture._authority.clone(),
                    WorkspaceId::from_bytes([1; 16]),
                    ObjectStoreScopeId::from_bytes([0xC1; 16]),
                    PrincipalId::from_bytes([7; 16]),
                    restarted_owner,
                )));
            assert!(matches!(volume.flush(), Err(BarrierError::Fenced)));
            assert!(volume.is_fenced());
            assert!(matches!(
                volume.dispatch(BlockRequest::Write {
                    offset: 4096,
                    data: &[0xBB; 4096],
                    fua: false,
                }),
                Err(BlockError::Barrier(_))
            ));
        }
    }

    #[test]
    fn fenced_worker_retains_barrier_salvage_journal_when_handover_aborts_child() {
        let fixture = TestFixture::new();
        let mut volume = fixture.open_volume();
        let mut coordinator = fixture.coordinator();
        let mut producers = NoopProducerGate;
        let mut boundary = RecordingBoundary::default();

        // Write dirty data and inject an unknown commit_live_roots outcome (not committed)
        // so the child barrier stays Preparing in the barrier journal when capture pauses.
        volume
            .dispatch(BlockRequest::Write {
                offset: 0,
                data: &[0xAB; 4096],
                fua: false,
            })
            .unwrap();
        *fixture
            .control
            .drop_next_commit_live_roots_reply
            .lock()
            .unwrap() = Some(false);

        let err = coordinator
            .checkpoint(
                &mut volume,
                &mut producers,
                &mut boundary,
                &CheckpointRequest::attached(CaptureMode::FilesystemFrozen, 27_000),
                &[],
            )
            .expect_err("unknown child commit outcome must pause capture");
        assert!(is_unknown_capture_error(&err));
        assert!(boundary.frozen);
        assert!(volume.has_pending_barrier());

        // Force writer handover under another principal: aborts both capture and
        // preparing child barrier in the same transaction that revokes attachment.
        let other_control = WorkspaceControl::new(
            fixture._authority.clone(),
            WorkspaceId::from_bytes([1; 16]),
            ObjectStoreScopeId::from_bytes([0xC1; 16]),
            PrincipalId::from_bytes([8; 16]),
            fixture._owner,
        );
        let current = other_control.get_branch(branch()).unwrap();
        let _new_grant = other_control
            .force_acquire_writer(op(180), branch(), current.revision, current.writer_epoch)
            .expect("forced handover");

        // Watchdog recovery thaws and clears capture intent, marks the old volume
        // fenced, AND preserves the barrier salvage journal on disk.
        let recovered = coordinator
            .recover_pending_capture(&mut volume, &mut producers, &mut boundary)
            .expect("watchdog recovery should finish thaw and capture cleanup");
        assert!(matches!(
            recovered,
            RecoveredCapture::Aborted {
                freeze_thawed: true,
                ..
            }
        ));
        assert!(volume.is_fenced());
        assert!(
            volume.has_pending_barrier(),
            "fenced worker must retain its local pending barrier salvage record"
        );
        drop(volume);
        let mut reopened_journal = FileBarrierJournal::open(&fixture.barrier_dir).unwrap();
        assert!(
            reopened_journal.load_pending().unwrap().is_some(),
            "barrier salvage journal file must remain intact on disk after fenced abort recovery"
        );
    }

    #[test]
    fn recover_pending_capture_detached_thaws_and_reconciles_without_volume_handle() {
        let fixture = TestFixture::new();
        let mut volume = fixture.open_volume();
        let failures = Arc::new(OneShotFailures::default());
        failures.arm(FailurePoint::FilesystemFreezeAfter);
        let mut coordinator = fixture.coordinator_with_failures(failures);
        let mut producers = NoopProducerGate;
        let mut boundary = RecordingBoundary::default();

        let err = coordinator
            .checkpoint(
                &mut volume,
                &mut producers,
                &mut boundary,
                &CheckpointRequest::attached(CaptureMode::FilesystemFrozen, 28_000),
                &[],
            )
            .expect_err("injected crash after freeze");
        assert!(matches!(err, CaptureError::InjectedFailure(_)));
        assert!(boundary.frozen);

        // Lose the original coordinator & volume handle and force writer takeover.
        drop(coordinator);
        drop(volume);
        let other_control = WorkspaceControl::new(
            fixture._authority.clone(),
            WorkspaceId::from_bytes([1; 16]),
            ObjectStoreScopeId::from_bytes([0xC1; 16]),
            PrincipalId::from_bytes([8; 16]),
            fixture._owner,
        );
        let current = other_control.get_branch(branch()).unwrap();
        let _new_grant = other_control
            .force_acquire_writer(op(181), branch(), current.revision, current.writer_epoch)
            .expect("forced handover");

        // Detached watchdog recovery succeeds with just the scope-checked authority
        // handle + original boundary, thawing the filesystem and clearing the journal.
        let mut watchdog = fixture.coordinator();
        let recovered = watchdog
            .recover_pending_capture_detached(&fixture.control, &mut producers, &mut boundary)
            .expect("detached watchdog recovery must succeed after takeover");
        assert!(matches!(
            recovered,
            RecoveredCapture::Aborted {
                freeze_thawed: true,
                ..
            }
        ));
        assert!(!boundary.frozen);
        assert_eq!(boundary.thaw_calls, 1);
        assert!(watchdog.pending_capture().unwrap().is_none());
    }

    #[test]
    fn large_verification_renews_capture_preparation_during_object_reads() {
        #[derive(Clone)]
        struct SlowAdvancingStore {
            inner: MemoryStore,
            authority: Arc<MemoryAuthority>,
            control: Arc<WorkspaceControl<MemoryAuthority>>,
            grant: Arc<Mutex<WriterGrant>>,
            next_op: Arc<Mutex<u8>>,
            armed: Arc<std::sync::atomic::AtomicBool>,
            initial_capture_deadline: Arc<Mutex<Option<u64>>>,
            verification_reads: Arc<Mutex<usize>>,
        }

        impl SlowAdvancingStore {
            fn advance_during_verification(&self) -> Result<(), String> {
                if !self.armed.load(Ordering::SeqCst) {
                    return Ok(());
                }
                let read_index = {
                    let mut count = self.verification_reads.lock().unwrap();
                    *count += 1;
                    *count
                };
                // Advance the authority clock by 200 seconds per object read
                // during descriptor verification so verification crosses the
                // capture's initial 600s deadline while keeping writer and
                // attachment heartbeats live.
                if read_index <= 4 {
                    let target_now =
                        self.authority.now_nanos.load(Ordering::SeqCst) + 200_000_000_000;
                    let mut current_now = self.authority.now_nanos.load(Ordering::SeqCst);
                    let mut grant = self.grant.lock().unwrap();
                    let mut op_byte = self.next_op.lock().unwrap();
                    while current_now < target_now {
                        current_now = (current_now + 20_000_000_000).min(target_now);
                        self.authority
                            .now_nanos
                            .store(current_now, Ordering::SeqCst);
                        *grant = self
                            .control
                            .renew_writer(op(*op_byte), *grant)
                            .map_err(|error| error.to_string())?;
                        *op_byte = op_byte.wrapping_add(1);
                        let att = self
                            .control
                            .get_branch(branch())
                            .map_err(|error| error.to_string())?
                            .attachment
                            .unwrap();
                        if att.protection_expires_at.as_nanos() <= current_now + 60_000_000_000 {
                            self.control
                                .renew_attachment_base(
                                    op(*op_byte),
                                    branch(),
                                    att.attachment_id,
                                    att.volume_epoch,
                                    att.protection_id,
                                    *grant,
                                )
                                .map_err(|error| error.to_string())?;
                            *op_byte = op_byte.wrapping_add(1);
                        }
                    }
                }
                Ok(())
            }
        }

        impl MapObjectStore for SlowAdvancingStore {
            fn proof_scope(&self) -> ObjectStoreScopeId {
                self.inner.proof_scope()
            }

            fn get(&self, id: ObjectId) -> Result<Option<Vec<u8>>, String> {
                self.advance_during_verification()?;
                self.inner.get(id)
            }

            fn get_ref(&self, reference: ObjectRef) -> Result<Option<Vec<u8>>, String> {
                self.advance_during_verification()?;
                self.inner.get_ref(reference)
            }

            fn put(&mut self, id: ObjectId, bytes: Vec<u8>) -> Result<(), String> {
                self.inner.put(id, bytes)
            }
        }

        impl DurableObjectStore for SlowAdvancingStore {
            fn put_durable(&mut self, reference: ObjectRef, encoded: &[u8]) -> Result<(), String> {
                if reference.object_type == ObjectType::GenerationDescriptor
                    && !self.armed.swap(true, Ordering::SeqCst)
                {
                    let capture = self
                        .control
                        .get_branch(branch())
                        .map_err(|error| error.to_string())?
                        .capture
                        .expect("capture must be active when uploading descriptor");
                    if let Ok(OperationState::Preparing { expires_at, .. }) =
                        self.control.get_operation(capture.operation_id)
                    {
                        *self.initial_capture_deadline.lock().unwrap() =
                            Some(expires_at.as_nanos());
                    }
                }
                self.inner.put_durable(reference, encoded)
            }
        }

        let fixture = TestFixture::new();
        let slow_store = SlowAdvancingStore {
            inner: fixture.store.clone(),
            authority: fixture._authority.clone(),
            control: fixture.control.inner.clone(),
            grant: Arc::new(Mutex::new(fixture.grant)),
            next_op: Arc::new(Mutex::new(100)),
            armed: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            initial_capture_deadline: Arc::new(Mutex::new(None)),
            verification_reads: Arc::new(Mutex::new(0)),
        };
        let mut volume = DurableVolume::open(
            slow_store.clone(),
            fixture.control.clone(),
            FileBarrierJournal::open(&fixture.barrier_dir).unwrap(),
            branch(),
            fixture.grant,
            attachment(),
            EXTENT_BYTES as usize * 4,
        )
        .unwrap();
        // Write 3 distinct extents so verification performs multiple object reads.
        for idx in 0..3_u64 {
            volume
                .dispatch(BlockRequest::Write {
                    offset: idx * u64::from(EXTENT_BYTES),
                    data: &[0xC0 + idx as u8; 4096],
                    fua: false,
                })
                .unwrap();
        }
        let mut coordinator = CheckpointCoordinator::new(
            FileCaptureJournal::open(&fixture.capture_dir).unwrap(),
            CaptureConfig {
                renewal_interval: Duration::ZERO,
                ..CaptureConfig::default()
            },
        );
        let mut producers = NoopProducerGate;
        let mut boundary = BlockCutOnlyBoundary;

        let published = coordinator
            .checkpoint(
                &mut volume,
                &mut producers,
                &mut boundary,
                &CheckpointRequest::attached(CaptureMode::BlockCut, 29_000),
                &[],
            )
            .expect(
                "verification must keep renewing capture preparation past its initial deadline",
            );
        assert!(published.head_advanced);
        let initial_deadline = slow_store
            .initial_capture_deadline
            .lock()
            .unwrap()
            .expect("descriptor upload must record initial capture deadline");
        assert!(
            fixture._authority.now_nanos.load(Ordering::SeqCst) > initial_deadline,
            "verification reads must advance authority clock past the capture's initial deadline"
        );
        assert!(
            *slow_store.verification_reads.lock().unwrap() >= 4,
            "verification must have performed multiple timed object reads"
        );
        assert!(
            fixture.control.renew_ids.lock().unwrap().len() >= 3,
            "capture preparation must have been renewed during object verification"
        );
    }

    #[test]
    fn foreign_principal_capture_does_not_permanently_fence_attached_worker() {
        let fixture = TestFixture::new();
        // Principal A is fixture.control (PrincipalId [7; 16]).
        // Principal B shares the same live writer grant on the branch.
        let principal_b_control = WorkspaceControl::new(
            fixture._authority.clone(),
            WorkspaceId::from_bytes([1; 16]),
            ObjectStoreScopeId::from_bytes([0xC1; 16]),
            PrincipalId::from_bytes([8; 16]),
            fixture._owner,
        );
        let mut volume_b = DurableVolume::open(
            fixture.store.clone(),
            principal_b_control,
            FileBarrierJournal::open(&fixture.barrier_dir).unwrap(),
            branch(),
            fixture.grant,
            attachment(),
            EXTENT_BYTES as usize * 2,
        )
        .unwrap();

        // Principal A starts a capture on the branch.
        let current = fixture.control.get_branch(branch()).unwrap();
        fixture
            .control
            .begin_capture(
                PreparationRequest {
                    operation_id: op(190),
                    branch_id: branch(),
                    expected_revision: current.revision,
                    grant: fixture.grant,
                    attachment_id: attachment(),
                    volume_epoch: current.volume_epoch,
                    protection_id: ProtectionId::from_bytes([190; 16]),
                    kind: PreparationKind::CheckpointCapture,
                    expected_roots: None,
                    dependencies: vec![],
                },
                ProvenanceBinding::Unattributed,
            )
            .expect("principal A begins capture");

        // Principal B tries to flush dirty writes during principal A's capture:
        // capture-scope authorization rejects it, but must NOT permanently fence volume B.
        volume_b
            .dispatch(BlockRequest::Write {
                offset: 0,
                data: &[0x3D; 4096],
                fua: false,
            })
            .unwrap();
        let err = volume_b
            .flush()
            .expect_err("flush during foreign principal capture must be rejected");
        assert!(matches!(err, BarrierError::Control(ControlError::Fenced)));
        assert!(
            !volume_b.is_fenced(),
            "foreign-principal capture denial must not permanently fence a live attachment"
        );

        // Once principal A aborts its capture, principal B's worker can flush normally.
        fixture.control.abort_operation(op(190)).unwrap();
        volume_b
            .flush()
            .expect("worker must flush cleanly once foreign capture ends");
    }

    #[test]
    fn expired_writer_during_publish_fences_volume_on_abort_recovery() {
        let fixture = TestFixture::new();
        let mut volume = fixture.open_volume();
        let mut coordinator = fixture.coordinator();
        let mut producers = NoopProducerGate;
        let mut boundary = RecordingBoundary::default();

        volume
            .dispatch(BlockRequest::Write {
                offset: 0,
                data: &[0x2E; 4096],
                fua: false,
            })
            .unwrap();

        // Advance the authority clock past the 60s writer lease right before publish_capture.
        let authority = fixture._authority.clone();
        *fixture.control.on_next_publish.lock().unwrap() = Some(Box::new(move || {
            authority.now_nanos.store(65_000_000_000, Ordering::SeqCst);
        }));

        let err = coordinator
            .checkpoint(
                &mut volume,
                &mut producers,
                &mut boundary,
                &CheckpointRequest::attached(CaptureMode::FilesystemFrozen, 30_000),
                &[],
            )
            .expect_err("expired writer lease during publish must fence capture");
        assert!(matches!(err, CaptureError::Fenced));
        assert_eq!(boundary.thaw_calls, 1);
        assert!(
            volume.is_fenced(),
            "expired writer lease must mark volume fenced before abort recovery reopens writes"
        );
        assert!(matches!(
            volume.dispatch(BlockRequest::Write {
                offset: 4096,
                data: &[0x3F; 4096],
                fua: false,
            }),
            Err(BlockError::Barrier(_))
        ));
    }

    #[test]
    fn prior_heartbeat_before_owner_restart_still_fences_volume_and_sync() {
        let fixture = TestFixture::new();
        let mut volume = fixture.open_volume();
        volume
            .dispatch(BlockRequest::Write {
                offset: 0,
                data: &[0x71; 4096],
                fua: false,
            })
            .unwrap();

        // Independent heartbeat extends G1 -> G2 on incarnation 1.
        fixture._authority.now_nanos.store(5_000, Ordering::SeqCst);
        let _g2 = fixture
            .control
            .inner
            .renew_writer(op(195), fixture.grant)
            .expect("heartbeat extends writer grant");

        // Now restart authority owner (incarnation 1 -> 2) while branch.writer still holds G2!
        let restarted_owner = start_authority_owner(fixture._authority.as_ref(), op(196)).unwrap();
        *fixture.control.override_inner.lock().unwrap() = Some(Arc::new(WorkspaceControl::new(
            fixture._authority.clone(),
            WorkspaceId::from_bytes([1; 16]),
            ObjectStoreScopeId::from_bytes([0xC1; 16]),
            PrincipalId::from_bytes([7; 16]),
            restarted_owner,
        )));

        // Even though G2.expires_at > G1.expires_at, check_attachment_live detects
        // the owner incarnation mismatch and fences immediately on both sync and flush.
        let branch_snapshot = fixture.control.get_branch(branch()).unwrap();
        assert!(matches!(
            volume.sync_branch_state(&branch_snapshot),
            Err(BarrierError::Fenced)
        ));
        assert!(volume.is_fenced());
        assert!(matches!(volume.flush(), Err(BarrierError::Fenced)));
    }

    #[test]
    fn capture_abort_recovery_does_not_abort_unrelated_ordinary_barrier() {
        let fixture = TestFixture::new();
        let mut volume = fixture.open_volume();
        let mut coordinator = fixture.coordinator();
        let mut producers = NoopProducerGate;
        let mut boundary = BlockCutOnlyBoundary;

        // Capture acquisition returns an unknown outcome without committing, and
        // its immediate reconciliation lookup also fails transiently so capture pauses.
        *fixture.control.drop_next_begin_reply.lock().unwrap() = Some(false);
        *fixture
            .control
            .fail_next_get_operation_backend
            .lock()
            .unwrap() = true;
        let err = coordinator
            .checkpoint(
                &mut volume,
                &mut producers,
                &mut boundary,
                &CheckpointRequest::attached(CaptureMode::BlockCut, 31_000),
                &[],
            )
            .expect_err("unknown begin_capture outcome");
        assert!(is_unknown_capture_error(&err));

        // Block admission is still open; an ordinary flush (capture_id: None) prepares
        // and pauses with an unknown commit outcome.
        volume
            .dispatch(BlockRequest::Write {
                offset: 0,
                data: &[0x9A; 4096],
                fua: false,
            })
            .unwrap();
        *fixture
            .control
            .drop_next_commit_live_roots_reply
            .lock()
            .unwrap() = Some(false);
        let flush_err = volume
            .flush()
            .expect_err("ordinary barrier pauses on unknown commit outcome");
        assert!(is_unknown_barrier_error(&flush_err));
        let ordinary_op_id = volume
            .pending_barrier_operation_id()
            .expect("ordinary barrier must be pending locally");

        // Watchdog recovers the capture: it must NOT abort the unrelated ordinary barrier!
        let recovered = coordinator
            .recover_pending_capture(&mut volume, &mut producers, &mut boundary)
            .expect("capture recovery should succeed");
        assert!(matches!(recovered, RecoveredCapture::Aborted { .. }));
        assert!(
            matches!(
                fixture.control.get_operation(ordinary_op_id),
                Ok(OperationState::Preparing { .. })
            ),
            "capture abort recovery must not abort an unrelated ordinary barrier"
        );
        // The ordinary barrier can still finish cleanly on retry.
        volume
            .flush()
            .expect("ordinary barrier must still commit cleanly");
    }

    #[test]
    fn recover_pending_capture_detached_rejects_mismatched_workspace_scope() {
        let fixture = TestFixture::new();
        let failures = Arc::new(OneShotFailures::default());
        failures.arm(FailurePoint::FilesystemFreezeAfter);
        let mut volume = fixture.open_volume();
        let mut coordinator = fixture.coordinator_with_failures(failures);
        let mut producers = CallbackProducerGate::new(|_| Ok(()));
        let mut boundary = RecordingBoundary::default();

        let err = coordinator
            .checkpoint(
                &mut volume,
                &mut producers,
                &mut boundary,
                &CheckpointRequest::attached(CaptureMode::FilesystemFrozen, 32_000),
                &[],
            )
            .expect_err("injected crash after freeze");
        assert!(matches!(err, CaptureError::InjectedFailure(_)));
        assert!(boundary.frozen);
        let capture_id = coordinator
            .pending_capture()
            .unwrap()
            .expect("pending capture must exist")
            .capture_id;
        drop(coordinator);
        drop(volume);

        // Construct a control handle with the SAME principal and object-store
        // scope, but a DIFFERENT workspace ID.
        let wrong_workspace_control = WorkspaceControl::new(
            fixture._authority.clone(),
            WorkspaceId::from_bytes([2; 16]),
            ObjectStoreScopeId::from_bytes([0xC1; 16]),
            PrincipalId::from_bytes([7; 16]),
            fixture._owner,
        );
        let mut watchdog = fixture.coordinator();
        let err = watchdog
            .recover_pending_capture_detached(
                &wrong_workspace_control,
                &mut producers,
                &mut boundary,
            )
            .expect_err("detached recovery must reject mismatched workspace_id");
        assert!(matches!(err, CaptureError::JournalScopeMismatch));
        assert!(boundary.frozen, "must not thaw on workspace mismatch");
        assert_eq!(boundary.thaw_calls, 0);
        assert_eq!(
            producers.reopen_calls, 0,
            "must not reopen admission on workspace mismatch"
        );
        assert!(
            watchdog.pending_capture().unwrap().is_some(),
            "must not delete capture journal on workspace mismatch"
        );
        assert!(
            matches!(
                fixture.control.get_operation(capture_id),
                Ok(OperationState::Preparing { .. })
            ),
            "original workspace capture must remain Preparing"
        );
        assert_eq!(
            wrong_workspace_control.get_operation(capture_id),
            Err(ControlError::NotFound),
            "must not create an aborted tombstone in the wrong workspace"
        );
    }

    #[test]
    fn foreign_principal_capture_during_acquire_does_not_permanently_fence_volume() {
        let fixture = TestFixture::new();
        let mut volume = fixture.open_volume();
        let mut coordinator = fixture.coordinator();
        let mut boundary = BlockCutOnlyBoundary;

        let foreign_control = WorkspaceControl::new(
            fixture._authority.clone(),
            WorkspaceId::from_bytes([1; 16]),
            ObjectStoreScopeId::from_bytes([0xC1; 16]),
            PrincipalId::from_bytes([8; 16]),
            fixture._owner,
        );
        let grant = fixture.grant;
        let foreign_capture_id = op(198);
        let foreign_for_drain = WorkspaceControl::new(
            fixture._authority.clone(),
            WorkspaceId::from_bytes([1; 16]),
            ObjectStoreScopeId::from_bytes([0xC1; 16]),
            PrincipalId::from_bytes([8; 16]),
            fixture._owner,
        );
        let mut producers = CallbackProducerGate::new(move |_| {
            // Between Principal A's pre-drain branch read and begin_capture,
            // Principal B acquires a capture using the same valid writer grant.
            let current = foreign_for_drain.get_branch(branch()).unwrap();
            foreign_for_drain
                .begin_capture(
                    PreparationRequest {
                        operation_id: foreign_capture_id,
                        branch_id: branch(),
                        expected_revision: current.revision,
                        grant,
                        attachment_id: attachment(),
                        volume_epoch: current.volume_epoch,
                        protection_id: ProtectionId::from_bytes([198; 16]),
                        kind: PreparationKind::CheckpointCapture,
                        expected_roots: None,
                        dependencies: vec![],
                    },
                    ProvenanceBinding::Unattributed,
                )
                .expect("foreign principal begins capture during drain");
            Ok(())
        });

        let err = coordinator
            .checkpoint(
                &mut volume,
                &mut producers,
                &mut boundary,
                &CheckpointRequest::attached(CaptureMode::BlockCut, 33_000),
                &[],
            )
            .expect_err("capture acquisition during foreign capture must be rejected");
        assert!(matches!(err, CaptureError::Control(ControlError::Fenced)));
        assert!(
            !volume.is_fenced(),
            "foreign-principal capture during acquire must not permanently fence a live attachment"
        );
        assert!(coordinator.pending_capture().unwrap().is_none());

        // Once the foreign principal aborts its capture, Principal A's volume
        // must still accept normal writes and flush cleanly.
        foreign_control.abort_operation(foreign_capture_id).unwrap();
        volume
            .dispatch(BlockRequest::Write {
                offset: 0,
                data: &[0x5E; 4096],
                fua: false,
            })
            .expect("live attachment must still accept writes after foreign capture ends");
        volume
            .flush()
            .expect("live attachment must still flush cleanly after foreign capture ends");
    }
}
