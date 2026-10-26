//! Durable FLUSH/FUA coordination for one attached block volume.

use sha2::{Digest, Sha256};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Duration, Instant};
use thiserror::Error;
use versionfs_core::control::{
    AttachmentState, BranchState, ControlError, GenerationSeed, LiveRoots, OperationState,
    PreparationKind, PreparationRequest, VerifiedGenerationSeed, WorkspaceControl, WriterGrant,
};
use versionfs_core::fault::{FailureInjector, FailurePoint, NoFailures};
use versionfs_core::format::{
    CaptureMode, GenerationDescriptor, ObjectRef, ObjectType, VolumeGeometry, VolumeRoot,
};
use versionfs_core::graph::{GraphError, verify_generation};
use versionfs_core::identity::{
    AttachmentId, BranchId, GenerationId, ObjectId, OperationId, ProtectionId, VolumeBoundaryId,
};
use versionfs_core::metadata::MetadataAuthority;
use versionfs_core::persistent_map::{DurableObjectStore, MapObjectStore};
use versionfs_core::time::MonotonicTime;
use versionfs_core::views::{ArtifactMap, LogMap, ViewError, VolumeExtentMap};

use crate::volume::{
    BlockDevice, BlockError, BlockRequest, BlockResponse, DirtyVolume, StagedObject, StagedVolume,
};

/// Operation and protection identities allocated before a barrier is sent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BarrierIds {
    /// Stable operation identity reused until the barrier reaches a terminal result.
    pub operation_id: OperationId,
    /// Fresh protection identity for this barrier's immutable dependencies.
    pub protection_id: ProtectionId,
}

/// Exact local recovery record for one pending durable barrier.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PendingBarrier {
    /// Stable operation identity for this barrier.
    pub operation_id: OperationId,
    /// Preparation protection created for this barrier.
    pub protection_id: ProtectionId,
    /// Branch whose live volume root is being replaced.
    pub branch_id: BranchId,
    /// Branch revision captured before preparation begins.
    pub expected_revision: u64,
    /// Initial writer grant, replayed exactly for capture-child authorization.
    /// Its stable identity also fences recovery after a lease renewal.
    pub writer_grant: WriterGrant,
    /// Exact writable attachment identity.
    pub attachment_id: AttachmentId,
    /// Volume fencing epoch for the attachment.
    pub volume_epoch: u64,
    /// Capture whose identified drain child this barrier belongs to.
    pub capture_id: Option<OperationId>,
    /// Stable operation ID used to reserve this barrier as a capture child.
    /// `None` means a capture coordinator already reserved `operation_id`.
    pub capture_authorization_id: Option<OperationId>,
    /// Prior complete live roots.
    pub previous_roots: LiveRoots,
    /// Complete result roots, changing only the volume root.
    pub next_roots: LiveRoots,
    /// Local dirty overlay revision covered by this barrier.
    pub dirty_revision: u64,
    /// Dirty extents that must be restored if this operation did not commit.
    pub dirty_extent_refs: Vec<(u64, ObjectRef)>,
    /// All newly created immutable objects needed to finish the barrier.
    pub objects: Vec<StagedObject>,
    /// An unresolved preparation-renewal ID, replayed before allocating another.
    pub renewal_operation_id: Option<OperationId>,
    /// Exact writer grant fingerprint used by that unresolved renewal.
    pub renewal_grant: Option<WriterGrant>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ResumeStep {
    Continue,
    Restarted,
    Completed,
}

/// Durable storage for barrier IDs and sealed local recovery state.
///
/// Implementations must keep this state outside the exported block volume. ID
/// allocation must be durable before returning, and persist_pending must make
/// the complete record durable before any Metadata Authority or object-store
/// request is sent. [`FileBarrierJournal`] provides the reference local format;
/// worker hosts may provide another implementation with the same guarantees.
pub trait BarrierJournal {
    /// Reserves a never-reused barrier operation ID and preparation protection ID.
    fn reserve_barrier_ids(&mut self) -> Result<BarrierIds, String>;

    /// Reserves a never-reused operation ID for authorization or preparation renewal.
    fn reserve_operation_id(&mut self) -> Result<OperationId, String>;

    /// Loads the one pending barrier recorded for this writable attachment.
    fn load_pending(&mut self) -> Result<Option<PendingBarrier>, String>;

    /// Durably replaces the pending recovery record before remote I/O continues.
    fn persist_pending(&mut self, barrier: &PendingBarrier) -> Result<(), String>;

    /// Atomically replaces one exact terminal pending barrier with its retry.
    /// An uncertain result must be retried with the same IDs and record. An
    /// already identical replacement must be synchronized and accepted.
    fn replace_pending(
        &mut self,
        expected_operation_id: OperationId,
        replacement: &PendingBarrier,
    ) -> Result<(), String>;

    /// Durably removes a terminal pending barrier record.
    fn finish_pending(&mut self, operation_id: OperationId) -> Result<(), String>;
}

const JOURNAL_IDS_MAGIC: &[u8; 8] = b"VFSBID01";
const JOURNAL_PENDING_MAGIC_V1: &[u8; 8] = b"VFSPND01";
const JOURNAL_PENDING_MAGIC: &[u8; 8] = b"VFSPND02";
const CHECKSUM_BYTES: usize = 32;
static NEXT_TEMP_FILE: AtomicU64 = AtomicU64::new(0);

/// Crash-safe file journal for one writable block attachment.
///
/// The directory must be dedicated to this journal on a local filesystem
/// outside the exported volume. This implementation supports Unix, restricts
/// the directory to mode `0700`, and creates files with mode `0600`. Other
/// platforms need a different [`BarrierJournal`] implementation that enforces
/// equivalent ACL and atomic-replacement guarantees. An exclusive OS file lock
/// enforces one process per journal directory. It is safe to reopen the
/// directory after that process exits; this is not a cross-host coordination
/// mechanism. Each ID reservation and pending-record replacement is fsynced
/// before this type returns success.
pub struct FileBarrierJournal {
    directory: PathBuf,
    ids_path: PathBuf,
    pending_path: PathBuf,
    _lock: File,
    nonce: [u8; 8],
    next_counter: u64,
    failures: Arc<dyn FailureInjector>,
}

impl FileBarrierJournal {
    /// Opens or initializes one journal directory.
    pub fn open(directory: impl AsRef<Path>) -> Result<Self, String> {
        Self::open_with(directory, Arc::new(NoFailures))
    }

    /// Opens a journal with instance-scoped storage fault injection.
    pub fn open_with(
        directory: impl AsRef<Path>,
        failures: Arc<dyn FailureInjector>,
    ) -> Result<Self, String> {
        #[cfg(not(unix))]
        {
            let _ = (directory, failures);
            return Err(
                "FileBarrierJournal requires Unix private permissions; provide another BarrierJournal implementation".to_owned(),
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
        fs::create_dir_all(&directory).map_err(journal_io("create journal directory"))?;
        restrict_journal_directory(&directory)?;
        let ids_path = directory.join("barrier-ids.v1");
        let pending_path = directory.join("pending-barrier.v1");
        let lock_path = directory.join("barrier.lock");
        let mut lock_options = OpenOptions::new();
        lock_options
            .create(true)
            .truncate(false)
            .read(true)
            .write(true);
        set_private_creation_mode(&mut lock_options, 0o600);
        let lock = lock_options
            .open(lock_path)
            .map_err(journal_io("open barrier journal lock"))?;
        lock.try_lock()
            .map_err(|error| format!("barrier journal is already in use: {error}"))?;

        let (nonce, next_counter) = match fs::read(&ids_path) {
            Ok(bytes) => decode_ids(&bytes)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let mut nonce = [0; 8];
                getrandom::fill(&mut nonce)
                    .map_err(|error| format!("generate journal ID namespace: {error}"))?;
                let bytes = encode_ids(nonce, 0);
                // The exclusive journal lock covers initialization too. Only
                // a complete, synced temporary record reaches this path.
                atomic_replace(&directory, &ids_path, &bytes, failures.as_ref())?;
                (nonce, 0)
            }
            Err(error) => return Err(format!("read journal ID state: {error}")),
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
            .ok_or_else(|| "barrier journal ID counter exhausted".to_owned())?;
        // Persist the advanced counter before exposing any reserved ID. A
        // failed write may consume IDs, but can never make an exposed ID recur.
        atomic_replace(
            &self.directory,
            &self.ids_path,
            &encode_ids(self.nonce, end),
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

impl BarrierJournal for FileBarrierJournal {
    fn reserve_barrier_ids(&mut self) -> Result<BarrierIds, String> {
        let ids = self.reserve_counters(2)?;
        Ok(BarrierIds {
            operation_id: OperationId::from_bytes(ids[0]),
            protection_id: ProtectionId::from_bytes(ids[1]),
        })
    }

    fn reserve_operation_id(&mut self) -> Result<OperationId, String> {
        let [id] = self.reserve_counters(1)?.try_into().map_err(|_| {
            "barrier journal reserved an unexpected number of operation IDs".to_owned()
        })?;
        Ok(OperationId::from_bytes(id))
    }

    fn load_pending(&mut self) -> Result<Option<PendingBarrier>, String> {
        match fs::read(&self.pending_path) {
            Ok(bytes) => decode_pending_file(&bytes).map(Some),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(format!("read pending barrier journal: {error}")),
        }
    }

    fn persist_pending(&mut self, barrier: &PendingBarrier) -> Result<(), String> {
        validate_pending(barrier)?;
        if let Some(existing) = self.load_pending()?
            && existing.operation_id != barrier.operation_id
        {
            return Err("another barrier is already pending in this journal".to_owned());
        }
        let bytes = encode_pending_file(barrier)?;
        atomic_replace(
            &self.directory,
            &self.pending_path,
            &bytes,
            self.failures.as_ref(),
        )
    }

    fn replace_pending(
        &mut self,
        expected_operation_id: OperationId,
        replacement: &PendingBarrier,
    ) -> Result<(), String> {
        validate_pending(replacement)?;
        match self.load_pending()? {
            Some(existing)
                if existing.operation_id == expected_operation_id || existing == *replacement => {}
            Some(_) => return Err("cannot replace a different pending barrier".to_owned()),
            None => return Err("cannot replace a missing pending barrier".to_owned()),
        }
        let bytes = encode_pending_file(replacement)?;
        atomic_replace(
            &self.directory,
            &self.pending_path,
            &bytes,
            self.failures.as_ref(),
        )
    }

    fn finish_pending(&mut self, operation_id: OperationId) -> Result<(), String> {
        match self.load_pending()? {
            Some(pending) if pending.operation_id != operation_id => {
                Err("cannot finish a different pending barrier".to_owned())
            }
            None => Ok(()),
            Some(_) => {
                fs::remove_file(&self.pending_path)
                    .map_err(journal_io("remove terminal pending barrier"))?;
                sync_directory(&self.directory)
            }
        }
    }
}

fn encode_ids(nonce: [u8; 8], next_counter: u64) -> Vec<u8> {
    let mut payload = Vec::with_capacity(16);
    payload.extend_from_slice(&nonce);
    payload.extend_from_slice(&next_counter.to_le_bytes());
    encode_checked(JOURNAL_IDS_MAGIC, &payload)
}

fn decode_ids(bytes: &[u8]) -> Result<([u8; 8], u64), String> {
    let payload = decode_checked(JOURNAL_IDS_MAGIC, bytes)?;
    if payload.len() != 16 {
        return Err("invalid barrier journal ID state length".to_owned());
    }
    let nonce = payload[..8]
        .try_into()
        .map_err(|_| "invalid barrier journal nonce".to_owned())?;
    let next_counter = u64::from_le_bytes(
        payload[8..]
            .try_into()
            .map_err(|_| "invalid barrier journal ID counter".to_owned())?,
    );
    Ok((nonce, next_counter))
}

fn encode_pending_file(barrier: &PendingBarrier) -> Result<Vec<u8>, String> {
    let mut payload = Vec::new();
    payload.extend_from_slice(barrier.operation_id.as_bytes());
    payload.extend_from_slice(barrier.protection_id.as_bytes());
    payload.extend_from_slice(barrier.branch_id.as_bytes());
    push_u64(&mut payload, barrier.expected_revision);
    encode_grant(&mut payload, barrier.writer_grant);
    payload.extend_from_slice(barrier.attachment_id.as_bytes());
    push_u64(&mut payload, barrier.volume_epoch);
    encode_optional_operation_id(&mut payload, barrier.capture_id);
    encode_optional_operation_id(&mut payload, barrier.capture_authorization_id);
    encode_roots(&mut payload, barrier.previous_roots);
    encode_roots(&mut payload, barrier.next_roots);
    push_u64(&mut payload, barrier.dirty_revision);
    push_count(&mut payload, barrier.dirty_extent_refs.len())?;
    for (number, reference) in &barrier.dirty_extent_refs {
        push_u64(&mut payload, *number);
        encode_ref(&mut payload, *reference);
    }
    push_count(&mut payload, barrier.objects.len())?;
    for object in &barrier.objects {
        encode_ref(&mut payload, object.reference);
        push_u64(&mut payload, object.encoded.len() as u64);
        payload.extend_from_slice(&object.encoded);
    }
    match barrier.renewal_operation_id {
        Some(id) => {
            payload.push(1);
            payload.extend_from_slice(id.as_bytes());
            encode_grant(
                &mut payload,
                barrier
                    .renewal_grant
                    .ok_or_else(|| "pending renewal ID is missing its writer grant".to_owned())?,
            );
        }
        None => {
            if barrier.renewal_grant.is_some() {
                return Err("pending renewal grant is missing its operation ID".to_owned());
            }
            payload.push(0);
        }
    }
    Ok(encode_checked(JOURNAL_PENDING_MAGIC, &payload))
}

fn decode_pending_file(bytes: &[u8]) -> Result<PendingBarrier, String> {
    let (payload, has_capture_fields) = match bytes.get(..8) {
        Some(magic) if magic == JOURNAL_PENDING_MAGIC => {
            (decode_checked(JOURNAL_PENDING_MAGIC, bytes)?, true)
        }
        Some(magic) if magic == JOURNAL_PENDING_MAGIC_V1 => {
            (decode_checked(JOURNAL_PENDING_MAGIC_V1, bytes)?, false)
        }
        _ => return Err("invalid barrier journal header".to_owned()),
    };
    let mut reader = JournalReader::new(payload);
    let operation_id = OperationId::from_bytes(reader.fixed()?);
    let protection_id = ProtectionId::from_bytes(reader.fixed()?);
    let branch_id = BranchId::from_bytes(reader.fixed()?);
    let expected_revision = reader.u64()?;
    let writer_grant = decode_grant(&mut reader)?;
    let attachment_id = AttachmentId::from_bytes(reader.fixed()?);
    let volume_epoch = reader.u64()?;
    let (capture_id, capture_authorization_id) = if has_capture_fields {
        (
            reader.optional_operation_id()?,
            reader.optional_operation_id()?,
        )
    } else {
        (None, None)
    };
    let previous_roots = decode_roots(&mut reader)?;
    let next_roots = decode_roots(&mut reader)?;
    let dirty_revision = reader.u64()?;
    let dirty_count = reader.count()?;
    let mut dirty_extent_refs = Vec::with_capacity(dirty_count);
    for _ in 0..dirty_count {
        dirty_extent_refs.push((reader.u64()?, reader.object_ref()?));
    }
    let object_count = reader.count()?;
    let mut objects = Vec::with_capacity(object_count);
    for _ in 0..object_count {
        let reference = reader.object_ref()?;
        let encoded_len = usize::try_from(reader.u64()?)
            .map_err(|_| "pending object length does not fit memory".to_owned())?;
        let encoded = reader.bytes(encoded_len)?.to_vec();
        objects.push(StagedObject { reference, encoded });
    }
    let (renewal_operation_id, renewal_grant) = match reader.u8()? {
        0 => (None, None),
        1 => (
            Some(OperationId::from_bytes(reader.fixed()?)),
            Some(decode_grant(&mut reader)?),
        ),
        _ => return Err("invalid pending renewal ID tag".to_owned()),
    };
    reader.finish()?;
    let barrier = PendingBarrier {
        operation_id,
        protection_id,
        branch_id,
        expected_revision,
        writer_grant,
        attachment_id,
        volume_epoch,
        capture_id,
        capture_authorization_id,
        previous_roots,
        next_roots,
        dirty_revision,
        dirty_extent_refs,
        objects,
        renewal_operation_id,
        renewal_grant,
    };
    validate_pending(&barrier)?;
    Ok(barrier)
}

fn validate_pending(barrier: &PendingBarrier) -> Result<(), String> {
    if barrier.writer_grant.branch_id != barrier.branch_id {
        return Err("pending barrier grant belongs to another branch".to_owned());
    }
    if barrier.renewal_operation_id.is_some() != barrier.renewal_grant.is_some() {
        return Err("pending renewal ID and grant must be recorded together".to_owned());
    }
    if barrier.capture_authorization_id.is_some() && barrier.capture_id.is_none() {
        return Err("capture authorization ID is missing its capture ID".to_owned());
    }
    if let Some(grant) = barrier.renewal_grant
        && !same_grant_identity(barrier.writer_grant, grant)
    {
        return Err("pending renewal grant does not match the barrier writer".to_owned());
    }
    validate_roots(barrier.previous_roots)?;
    validate_roots(barrier.next_roots)?;
    let mut objects = std::collections::BTreeMap::new();
    for object in &barrier.objects {
        object
            .reference
            .verify(&object.encoded)
            .map_err(|error| format!("invalid staged object in barrier journal: {error}"))?;
        if objects
            .insert(object.reference.object_id, object.encoded.as_slice())
            .is_some_and(|existing| existing != object.encoded)
        {
            return Err("pending barrier repeats an object ID with different bytes".to_owned());
        }
    }
    if !objects.contains_key(&barrier.next_roots.volume.object_id) {
        return Err("pending barrier does not contain its replacement volume root".to_owned());
    }
    let mut extents = std::collections::BTreeSet::new();
    for (number, reference) in &barrier.dirty_extent_refs {
        if reference.object_type != ObjectType::Extent || !extents.insert(*number) {
            return Err("pending barrier has an invalid or repeated dirty extent".to_owned());
        }
        let Some(encoded) = objects.get(&reference.object_id) else {
            return Err("pending dirty extent is absent from its staged objects".to_owned());
        };
        reference
            .verify(encoded)
            .map_err(|error| format!("invalid dirty extent in barrier journal: {error}"))?;
    }
    Ok(())
}

fn validate_roots(roots: LiveRoots) -> Result<(), String> {
    if roots.volume.object_type != ObjectType::VolumeRoot
        || roots.logs.object_type != ObjectType::LogMapRoot
        || roots.artifacts.object_type != ObjectType::ArtifactMapRoot
        || [&roots.volume, &roots.logs, &roots.artifacts]
            .iter()
            .any(|reference| reference.encoded_len < 16)
    {
        return Err("pending barrier contains invalid typed roots".to_owned());
    }
    Ok(())
}

fn encode_grant(bytes: &mut Vec<u8>, grant: WriterGrant) {
    bytes.extend_from_slice(grant.branch_id.as_bytes());
    push_u64(bytes, grant.writer_epoch);
    push_u64(bytes, grant.owner_incarnation.get());
    bytes.extend_from_slice(grant.grant_id.as_bytes());
    push_u64(bytes, grant.expires_at.as_nanos());
}

fn decode_grant(reader: &mut JournalReader<'_>) -> Result<WriterGrant, String> {
    Ok(WriterGrant {
        branch_id: BranchId::from_bytes(reader.fixed()?),
        writer_epoch: reader.u64()?,
        owner_incarnation: versionfs_core::control::OwnerIncarnation::from_u64(reader.u64()?),
        grant_id: OperationId::from_bytes(reader.fixed()?),
        expires_at: MonotonicTime::from_nanos(reader.u64()?),
    })
}

fn encode_roots(bytes: &mut Vec<u8>, roots: LiveRoots) {
    encode_ref(bytes, roots.volume);
    encode_ref(bytes, roots.logs);
    encode_ref(bytes, roots.artifacts);
}

fn decode_roots(reader: &mut JournalReader<'_>) -> Result<LiveRoots, String> {
    Ok(LiveRoots {
        volume: reader.object_ref()?,
        logs: reader.object_ref()?,
        artifacts: reader.object_ref()?,
    })
}

fn encode_ref(bytes: &mut Vec<u8>, reference: ObjectRef) {
    push_u16(bytes, reference.object_type as u16);
    bytes.extend_from_slice(reference.object_id.as_bytes());
    push_u64(bytes, reference.encoded_len);
}

fn encode_optional_operation_id(bytes: &mut Vec<u8>, operation_id: Option<OperationId>) {
    if let Some(operation_id) = operation_id {
        bytes.push(1);
        bytes.extend_from_slice(operation_id.as_bytes());
    } else {
        bytes.push(0);
    }
}

fn push_count(bytes: &mut Vec<u8>, count: usize) -> Result<(), String> {
    let count = u32::try_from(count).map_err(|_| "barrier journal list is too large".to_owned())?;
    bytes.extend_from_slice(&count.to_le_bytes());
    Ok(())
}

fn push_u16(bytes: &mut Vec<u8>, value: u16) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

fn push_u64(bytes: &mut Vec<u8>, value: u64) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

fn encode_checked(magic: &[u8; 8], payload: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(16 + payload.len() + CHECKSUM_BYTES);
    bytes.extend_from_slice(magic);
    push_u64(&mut bytes, payload.len() as u64);
    bytes.extend_from_slice(payload);
    let checksum = Sha256::digest(&bytes);
    bytes.extend_from_slice(&checksum);
    bytes
}

fn decode_checked<'a>(magic: &[u8; 8], bytes: &'a [u8]) -> Result<&'a [u8], String> {
    if bytes.len() < 16 + CHECKSUM_BYTES || &bytes[..8] != magic {
        return Err("invalid barrier journal header".to_owned());
    }
    let payload_len = usize::try_from(u64::from_le_bytes(
        bytes[8..16]
            .try_into()
            .map_err(|_| "invalid barrier journal length".to_owned())?,
    ))
    .map_err(|_| "barrier journal length does not fit memory".to_owned())?;
    let expected_len = 16_usize
        .checked_add(payload_len)
        .and_then(|len| len.checked_add(CHECKSUM_BYTES))
        .ok_or_else(|| "barrier journal length overflow".to_owned())?;
    if expected_len != bytes.len() {
        return Err("barrier journal length does not match its header".to_owned());
    }
    let checksum_start = bytes.len() - CHECKSUM_BYTES;
    if Sha256::digest(&bytes[..checksum_start]).as_slice() != &bytes[checksum_start..] {
        return Err("barrier journal checksum mismatch".to_owned());
    }
    Ok(&bytes[16..checksum_start])
}

fn atomic_replace(
    directory: &Path,
    destination: &Path,
    bytes: &[u8],
    failures: &dyn FailureInjector,
) -> Result<(), String> {
    let (temporary, mut file) = loop {
        let sequence = NEXT_TEMP_FILE.fetch_add(1, Ordering::Relaxed);
        let temporary = directory.join(format!(
            ".barrier-journal-{}-{sequence}.tmp",
            std::process::id()
        ));
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        set_private_creation_mode(&mut options, 0o600);
        match options.open(&temporary) {
            Ok(file) => break (temporary, file),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(format!("create temporary barrier journal: {error}")),
        }
    };
    let write_result = file
        .write_all(bytes)
        .map_err(journal_io("write temporary barrier journal"))
        .and_then(|()| {
            file.sync_all()
                .map_err(journal_io("sync temporary barrier journal"))
        });
    drop(file);
    let write_result = write_result.and_then(|()| {
        failures
            .check(FailurePoint::JournalBeforePublish)
            .map_err(|error| error.to_string())
    });
    if let Err(error) = write_result {
        let _ = fs::remove_file(&temporary);
        return Err(error);
    }
    if let Err(error) = fs::rename(&temporary, destination) {
        let _ = fs::remove_file(&temporary);
        return Err(format!("publish barrier journal file: {error}"));
    }
    failures
        .check(FailurePoint::JournalAfterRenameBeforeDirectorySync)
        .map_err(|error| error.to_string())?;
    sync_directory(directory)
}

#[cfg(unix)]
fn restrict_journal_directory(directory: &Path) -> Result<(), String> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(directory, fs::Permissions::from_mode(0o700))
        .map_err(journal_io("restrict barrier journal directory permissions"))
}

fn set_private_creation_mode(options: &mut OpenOptions, mode: u32) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(mode);
    }
    #[cfg(not(unix))]
    let _ = (options, mode);
}

fn sync_directory(directory: &Path) -> Result<(), String> {
    File::open(directory)
        .and_then(|directory| directory.sync_all())
        .map_err(journal_io("sync barrier journal directory"))
}

fn journal_io(action: &'static str) -> impl FnOnce(std::io::Error) -> String {
    move |error| format!("{action}: {error}")
}

struct JournalReader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> JournalReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], String> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or_else(|| "barrier journal cursor overflow".to_owned())?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| "truncated barrier journal record".to_owned())?;
        self.offset = end;
        Ok(value)
    }

    fn fixed<const N: usize>(&mut self) -> Result<[u8; N], String> {
        self.take(N)?
            .try_into()
            .map_err(|_| "truncated fixed field in barrier journal".to_owned())
    }

    fn u8(&mut self) -> Result<u8, String> {
        Ok(self.fixed::<1>()?[0])
    }

    fn u16(&mut self) -> Result<u16, String> {
        Ok(u16::from_le_bytes(self.fixed()?))
    }

    fn u32(&mut self) -> Result<u32, String> {
        Ok(u32::from_le_bytes(self.fixed()?))
    }

    fn u64(&mut self) -> Result<u64, String> {
        Ok(u64::from_le_bytes(self.fixed()?))
    }

    fn optional_operation_id(&mut self) -> Result<Option<OperationId>, String> {
        match self.u8()? {
            0 => Ok(None),
            1 => Ok(Some(OperationId::from_bytes(self.fixed()?))),
            _ => Err("invalid optional operation ID tag".to_owned()),
        }
    }

    fn count(&mut self) -> Result<usize, String> {
        let count = usize::try_from(self.u32()?)
            .map_err(|_| "barrier journal count does not fit memory".to_owned())?;
        if count > self.bytes.len().saturating_sub(self.offset) {
            return Err("barrier journal count exceeds remaining record".to_owned());
        }
        Ok(count)
    }

    fn bytes(&mut self, length: usize) -> Result<&'a [u8], String> {
        self.take(length)
    }

    fn object_ref(&mut self) -> Result<ObjectRef, String> {
        let object_type = ObjectType::try_from(self.u16()?)
            .map_err(|error| format!("invalid object type in barrier journal: {error}"))?;
        let object_id = ObjectId::from_bytes(self.fixed()?);
        let encoded_len = self.u64()?;
        if !(16..=versionfs_core::format::MAX_ENCODED_OBJECT_BYTES as u64).contains(&encoded_len) {
            return Err("invalid object length in barrier journal".to_owned());
        }
        Ok(ObjectRef {
            object_type,
            object_id,
            encoded_len,
        })
    }

    fn finish(self) -> Result<(), String> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err("barrier journal record has trailing bytes".to_owned())
        }
    }
}

/// The P5 authority operations required by a durable block barrier.
///
/// The attachment runtime remains responsible for renewing the writer lease
/// while this device is open. The barrier coordinator renews preparation
/// protection during long uploads, but does not run the attachment heartbeat.
pub trait VolumeBarrierControl: Send + Sync {
    /// Reads the latest authoritative branch and live roots.
    fn get_branch(&self, branch_id: BranchId) -> Result<BranchState, ControlError>;

    /// Registers or replays a protected volume-root preparation.
    fn begin_preparation(
        &self,
        request: PreparationRequest,
    ) -> Result<OperationState, ControlError>;

    /// Reserves one volume-barrier operation as the active capture's drain child.
    /// Replaying an ID must retain the exact original grant, including its expiry.
    fn authorize_capture_child(
        &self,
        authorization_id: OperationId,
        capture_id: OperationId,
        child_id: OperationId,
        grant: WriterGrant,
    ) -> Result<(), ControlError>;

    /// Renews an active preparation using a fresh ID or replays the same renewal.
    /// A replay must use the exact original grant; a refreshed expiry requires a
    /// fresh renewal operation ID.
    fn renew_preparation(
        &self,
        renewal_id: OperationId,
        prepared_id: OperationId,
        grant: WriterGrant,
    ) -> Result<versionfs_core::time::MonotonicTime, ControlError>;

    /// Atomically advances branch live roots and records the terminal result.
    fn commit_live_roots(
        &self,
        operation_id: OperationId,
        expected_revision: u64,
        grant: WriterGrant,
        roots: LiveRoots,
        capture_id: Option<OperationId>,
    ) -> Result<BranchState, ControlError>;

    /// Aborts a preparation or tombstones an operation ID that never began.
    fn abort_operation(&self, operation_id: OperationId) -> Result<OperationState, ControlError>;

    /// Reads the exact state of a barrier operation for recovery.
    fn get_operation(&self, operation_id: OperationId) -> Result<OperationState, ControlError>;
}

impl<A: MetadataAuthority> VolumeBarrierControl for WorkspaceControl<A> {
    fn get_branch(&self, branch_id: BranchId) -> Result<BranchState, ControlError> {
        WorkspaceControl::get_branch(self, branch_id)
    }

    fn begin_preparation(
        &self,
        request: PreparationRequest,
    ) -> Result<OperationState, ControlError> {
        WorkspaceControl::begin_preparation(self, request)
    }

    fn authorize_capture_child(
        &self,
        authorization_id: OperationId,
        capture_id: OperationId,
        child_id: OperationId,
        grant: WriterGrant,
    ) -> Result<(), ControlError> {
        WorkspaceControl::authorize_capture_child(
            self,
            authorization_id,
            capture_id,
            child_id,
            grant,
        )
    }

    fn renew_preparation(
        &self,
        renewal_id: OperationId,
        prepared_id: OperationId,
        grant: WriterGrant,
    ) -> Result<versionfs_core::time::MonotonicTime, ControlError> {
        WorkspaceControl::renew_preparation(self, renewal_id, prepared_id, grant)
    }

    fn commit_live_roots(
        &self,
        operation_id: OperationId,
        expected_revision: u64,
        grant: WriterGrant,
        roots: LiveRoots,
        capture_id: Option<OperationId>,
    ) -> Result<BranchState, ControlError> {
        WorkspaceControl::commit_live_roots(
            self,
            operation_id,
            expected_revision,
            grant,
            roots,
            capture_id.map(|capture_id| (capture_id, operation_id)),
        )
    }

    fn abort_operation(&self, operation_id: OperationId) -> Result<OperationState, ControlError> {
        WorkspaceControl::abort_operation(self, operation_id)
    }

    fn get_operation(&self, operation_id: OperationId) -> Result<OperationState, ControlError> {
        WorkspaceControl::get_operation(self, operation_id)
    }
}

/// Errors while constructing, publishing, or recovering a volume barrier.
#[derive(Debug, Error)]
pub enum BarrierError {
    /// A block map or immutable object was invalid or unavailable.
    #[error(transparent)]
    Block(#[from] BlockError),
    /// The Metadata Authority rejected or could not resolve this operation.
    #[error(transparent)]
    Control(#[from] ControlError),
    /// The object store failed to establish durable immutable content.
    #[error("durable object-store write failed: {0}")]
    ObjectStore(String),
    /// The local recovery journal failed.
    #[error("barrier recovery journal failed: {0}")]
    Journal(String),
    /// The branch, grant, or attachment does not match the recorded operation.
    #[error("barrier state no longer matches its branch or attachment")]
    Fenced,
    /// An unresolved operation refers to a different branch revision or root.
    #[error("pending barrier conflicts with the authoritative live root")]
    Conflict,
    /// The recorded operation was terminally aborted.
    #[error("pending barrier was aborted; its dirty bytes remain local")]
    Aborted,
    /// The durable root or object graph is malformed.
    #[error(transparent)]
    View(#[from] ViewError),
    /// The complete Generation dependency graph could not be proven.
    #[error(transparent)]
    Graph(#[from] GraphError),
    /// The root object failed typed validation.
    #[error(transparent)]
    Format(#[from] versionfs_core::format::FormatError),
}

/// A workspace, zero-volume Generation, writer grant, and protected attachment.
#[derive(Clone, Debug)]
pub struct InitializedVolumeWorkspace {
    /// Created main branch state after attachment.
    pub branch: BranchState,
    /// Current writer grant returned by the authority.
    pub writer_grant: WriterGrant,
    /// Current protected writable attachment.
    pub attachment: AttachmentState,
}

/// Creates and durably stores the complete empty Generation graph.
///
/// This returns only after every immutable object is durable and readable.
pub fn create_empty_generation<S: DurableObjectStore>(
    store: &mut S,
    generation_id: GenerationId,
    boundary_id: VolumeBoundaryId,
    geometry: VolumeGeometry,
    created_at_unix_ms: u64,
) -> Result<GenerationSeed, BarrierError> {
    let (seed, objects) = {
        let mut staging = crate::volume::StagingMapStore::new(store);
        let volume = VolumeExtentMap::empty(geometry, &mut staging)?;
        let volume_root = volume.seal(&mut staging)?;
        let logs_root = LogMap::empty(&mut staging)?.seal(&mut staging)?;
        let artifacts_root = ArtifactMap::empty(&mut staging)?.seal(&mut staging)?;
        let descriptor = GenerationDescriptor {
            generation_id,
            parent_generation_id: None,
            volume_root,
            logs_root,
            artifacts_root,
            capture_mode: CaptureMode::BlockCut,
            volume_boundary_id: boundary_id,
            created_at_unix_ms,
        }
        .encode()?;
        let descriptor_ref = ObjectRef::from_encoded(&descriptor)?;
        staging
            .put(descriptor_ref.object_id, descriptor)
            .map_err(ViewError::Store)?;
        let seed = GenerationSeed {
            generation_id,
            descriptor: descriptor_ref,
            roots: LiveRoots {
                volume: volume_root,
                logs: logs_root,
                artifacts: artifacts_root,
            },
        };
        (seed, staging.into_objects()?)
    };

    for (reference, encoded) in objects {
        reference.verify(&encoded)?;
        store
            .put_durable(reference, &encoded)
            .map_err(BarrierError::ObjectStore)?;
    }

    verify_generation(store, seed.descriptor)?;

    Ok(seed)
}

/// Creates, durably stores, and verifies a descriptor for already durable roots.
///
/// The roots must be the exact sealed roots produced by the checkpoint
/// coordinator. Verification reads the descriptor's complete strong dependency
/// graph before the returned seed can be passed to the authority publication
/// transaction.
#[allow(clippy::too_many_arguments)]
pub fn create_generation<S: DurableObjectStore>(
    store: &mut S,
    generation_id: GenerationId,
    parent_generation_id: Option<GenerationId>,
    roots: LiveRoots,
    capture_mode: CaptureMode,
    boundary_id: VolumeBoundaryId,
    created_at_unix_ms: u64,
) -> Result<VerifiedGenerationSeed, BarrierError> {
    let descriptor = GenerationDescriptor {
        generation_id,
        parent_generation_id,
        volume_root: roots.volume,
        logs_root: roots.logs,
        artifacts_root: roots.artifacts,
        capture_mode,
        volume_boundary_id: boundary_id,
        created_at_unix_ms,
    }
    .encode()?;
    let descriptor_ref = ObjectRef::from_encoded(&descriptor)?;
    store
        .put_durable(descriptor_ref, &descriptor)
        .map_err(BarrierError::ObjectStore)?;
    let proof = verify_generation(store, descriptor_ref)?;
    Ok(VerifiedGenerationSeed::from_proof(proof))
}

/// Creates a zero-volume workspace, acquires a writer, and protects its base.
#[allow(clippy::too_many_arguments)]
pub fn initialize_empty_workspace<S: DurableObjectStore, A: MetadataAuthority>(
    store: &mut S,
    control: &WorkspaceControl<A>,
    create_operation_id: OperationId,
    generation_id: GenerationId,
    boundary_id: VolumeBoundaryId,
    geometry: VolumeGeometry,
    created_at_unix_ms: u64,
    branch_id: BranchId,
    name: &str,
    retention_period_ms: u64,
    acquire_writer_operation_id: OperationId,
    attach_operation_id: OperationId,
    attachment_id: AttachmentId,
    attachment_protection_id: ProtectionId,
) -> Result<InitializedVolumeWorkspace, BarrierError> {
    let initial = create_empty_generation(
        store,
        generation_id,
        boundary_id,
        geometry,
        created_at_unix_ms,
    )?;
    let created = control.create_workspace(
        create_operation_id,
        branch_id,
        name,
        retention_period_ms,
        initial,
    )?;
    let writer_grant = control.acquire_writer(
        acquire_writer_operation_id,
        branch_id,
        created.revision,
        created.writer_epoch,
    )?;
    let attachment = control.attach_volume(
        attach_operation_id,
        branch_id,
        created.revision,
        writer_grant,
        attachment_id,
        attachment_protection_id,
    )?;
    let branch = control.get_branch(branch_id)?;
    Ok(InitializedVolumeWorkspace {
        branch,
        writer_grant,
        attachment,
    })
}

/// One active durable volume attachment.
///
/// The caller must keep the associated P5 writer lease alive while this value
/// is attached. This type renews operation preparation protection, not writer
/// ownership; a separate attachment lifecycle must heartbeat and fence the
/// writer grant.
pub struct DurableVolume<S, C, J> {
    inner: DirtyVolume<S>,
    control: C,
    journal: J,
    branch_id: BranchId,
    writer_grant: WriterGrant,
    attachment_id: AttachmentId,
    volume_epoch: u64,
    live_roots: LiveRoots,
    revision: u64,
    pending: Option<PendingBarrier>,
    // Retains the trusted builder's metadata validation across uploads/retries.
    // Recovery reconstructs this handle from the validated base and dirty delta.
    pending_map: Option<VolumeExtentMap>,
    // Keeps the exact transition stable until its local publication is durable.
    pending_replaces: Option<OperationId>,
    pending_journaled: bool,
    last_renewal: Option<Instant>,
    // Known ownership loss cannot be reversed beneath a mounted filesystem.
    fenced: bool,
}

impl<S, C, J> DurableVolume<S, C, J>
where
    S: DurableObjectStore,
    C: VolumeBarrierControl,
    J: BarrierJournal,
{
    /// Opens the branch's current live VolumeRoot under an exact writable attachment.
    pub fn open(
        store: S,
        control: C,
        mut journal: J,
        branch_id: BranchId,
        writer_grant: WriterGrant,
        attachment_id: AttachmentId,
        max_dirty_bytes: usize,
    ) -> Result<Self, BarrierError> {
        let branch = control.get_branch(branch_id)?;
        let current_grant = validate_attachment(
            &branch,
            branch_id,
            attachment_id,
            branch.volume_epoch,
            writer_grant,
        )?;
        let attachment = branch.attachment.ok_or(BarrierError::Fenced)?;
        let base = load_map(&store, branch.volume_root)?;
        let mut inner = DirtyVolume::new(store, base, max_dirty_bytes)?;
        let pending = journal.load_pending().map_err(BarrierError::Journal)?;
        if let Some(barrier) = &pending {
            if barrier.branch_id != branch_id
                || barrier.attachment_id != attachment_id
                || barrier.volume_epoch != attachment.volume_epoch
                || !same_grant_identity(barrier.writer_grant, current_grant)
            {
                return Err(BarrierError::Fenced);
            }
            match control.get_operation(barrier.operation_id) {
                Ok(OperationState::Committed(_)) => {}
                Ok(OperationState::Aborted) => {
                    inner.restore_pending_dirty(
                        barrier.dirty_revision,
                        &barrier.dirty_extent_refs,
                        &barrier.objects,
                    )?;
                }
                Ok(OperationState::Preparing { .. }) | Err(ControlError::NotFound) => {
                    inner.restore_pending_dirty(
                        barrier.dirty_revision,
                        &barrier.dirty_extent_refs,
                        &barrier.objects,
                    )?;
                }
                Err(error) => return Err(error.into()),
            }
        }
        let mut volume = Self {
            inner,
            control,
            journal,
            branch_id,
            writer_grant: current_grant,
            attachment_id,
            volume_epoch: attachment.volume_epoch,
            live_roots: roots(&branch),
            revision: branch.revision,
            pending_journaled: pending.is_some(),
            pending,
            pending_map: None,
            pending_replaces: None,
            last_renewal: None,
            fenced: false,
        };
        if volume.pending.is_some() {
            volume.resume_pending()?;
        } else {
            let latest = volume.control.get_branch(branch_id)?;
            let latest_grant = validate_attachment(
                &latest,
                branch_id,
                attachment_id,
                volume.volume_epoch,
                writer_grant,
            )?;
            if latest.volume_root != volume.live_roots.volume {
                let latest_map = volume.inner.load_map(latest.volume_root)?;
                volume.inner.replace_base(latest_map);
            }
            volume.live_roots = roots(&latest);
            volume.revision = latest.revision;
            volume.writer_grant = latest_grant;
        }
        Ok(volume)
    }

    /// Persists all currently dirty writes and advances the authoritative live root.
    ///
    /// Once attachment fencing or terminal lease expiry is observed, this worker
    /// rejects further writes and barriers while retaining its recovery state.
    pub fn flush(&mut self) -> Result<(), BarrierError> {
        if self.fenced {
            return Err(BarrierError::Fenced);
        }
        let result = self.flush_inner();
        // A raw ControlError::Fenced can also mean that a concurrent heartbeat
        // refreshed the exact grant. Stable attachment/epoch validation detects
        // ownership loss without turning that recoverable race into a fence.
        if matches!(
            &result,
            Err(BarrierError::Fenced | BarrierError::Control(ControlError::Expired))
        ) {
            self.fenced = true;
        }
        result
    }

    fn flush_inner(&mut self) -> Result<(), BarrierError> {
        if self.pending.is_none() {
            if self.inner.dirty_extent_count() == 0 {
                let branch = self.control.get_branch(self.branch_id)?;
                self.writer_grant = validate_attachment(
                    &branch,
                    self.branch_id,
                    self.attachment_id,
                    self.volume_epoch,
                    self.writer_grant,
                )?;
                if let Some(child_id) = branch
                    .capture
                    .as_ref()
                    .filter(|capture| !capture.sealed)
                    .and_then(|capture| capture.pending_child)
                    && matches!(
                        self.control.abort_operation(child_id)?,
                        OperationState::Committed(_)
                    )
                {
                    self.refresh_clean_branch()?;
                }
                return Ok(());
            }
            let staged = self.inner.stage_dirty()?.ok_or(BarrierError::Conflict)?;
            let branch = self.control.get_branch(self.branch_id)?;
            self.writer_grant = validate_attachment(
                &branch,
                self.branch_id,
                self.attachment_id,
                self.volume_epoch,
                self.writer_grant,
            )?;
            let (ids, capture_id, capture_authorization_id) =
                self.reserve_barrier_context(&branch)?;
            let next_roots = LiveRoots {
                volume: staged.volume_root,
                logs: self.live_roots.logs,
                artifacts: self.live_roots.artifacts,
            };
            self.pending_map = Some(staged.map);
            let barrier = self.pending_from_stage(
                ids,
                next_roots,
                staged,
                capture_id,
                capture_authorization_id,
            );
            self.pending = Some(barrier);
            self.pending_replaces = None;
            self.pending_journaled = false;
            self.last_renewal = None;
        }

        self.resume_pending()
    }

    /// Returns the latest authoritative root revision observed by this attachment.
    pub const fn revision(&self) -> u64 {
        self.revision
    }

    /// Returns the current live roots observed by this attachment.
    pub const fn live_roots(&self) -> LiveRoots {
        self.live_roots
    }

    fn refresh_clean_branch(&mut self) -> Result<(), BarrierError> {
        let branch = self.control.get_branch(self.branch_id)?;
        let grant = validate_attachment(
            &branch,
            self.branch_id,
            self.attachment_id,
            self.volume_epoch,
            self.writer_grant,
        )?;
        if branch.volume_root != self.live_roots.volume {
            let map = self.inner.load_map(branch.volume_root)?;
            self.inner.replace_base(map);
        }
        self.live_roots = roots(&branch);
        self.revision = branch.revision;
        self.writer_grant = grant;
        Ok(())
    }

    fn pending_from_stage(
        &self,
        ids: BarrierIds,
        next_roots: LiveRoots,
        staged: StagedVolume,
        capture_id: Option<OperationId>,
        capture_authorization_id: Option<OperationId>,
    ) -> PendingBarrier {
        PendingBarrier {
            operation_id: ids.operation_id,
            protection_id: ids.protection_id,
            branch_id: self.branch_id,
            expected_revision: self.revision,
            writer_grant: self.writer_grant,
            attachment_id: self.attachment_id,
            volume_epoch: self.volume_epoch,
            capture_id,
            capture_authorization_id,
            previous_roots: self.live_roots,
            next_roots,
            dirty_revision: staged.dirty_revision,
            dirty_extent_refs: staged.dirty_extent_refs,
            objects: staged.objects,
            renewal_operation_id: None,
            renewal_grant: None,
        }
    }

    fn reserve_barrier_context(
        &mut self,
        branch: &BranchState,
    ) -> Result<(BarrierIds, Option<OperationId>, Option<OperationId>), BarrierError> {
        let mut ids = self
            .journal
            .reserve_barrier_ids()
            .map_err(BarrierError::Journal)?;
        let Some(capture) = branch.capture.as_ref() else {
            return Ok((ids, None, None));
        };
        if capture.sealed {
            return Err(ControlError::Busy.into());
        }
        let capture_id = capture.operation_id;
        let authorization_id = if let Some(child_id) = capture.pending_child {
            // A capture coordinator may have reserved this child before issuing
            // the device drain request. Burn the locally reserved operation ID
            // and use the exact authorized ID for the barrier.
            ids.operation_id = child_id;
            None
        } else {
            Some(
                self.journal
                    .reserve_operation_id()
                    .map_err(BarrierError::Journal)?,
            )
        };
        Ok((ids, Some(capture_id), authorization_id))
    }

    fn authorize_pending_capture_child(
        &self,
        authorization_id: OperationId,
        capture_id: OperationId,
        child_id: OperationId,
        saved_grant: WriterGrant,
    ) -> Result<(), ControlError> {
        match self.control.get_operation(authorization_id) {
            // Reconcile the stable outcome before replaying. This also supports
            // older journal records whose authorization used a refreshed grant.
            Ok(OperationState::Committed(_)) => Ok(()),
            Ok(OperationState::Aborted) => Err(ControlError::Aborted),
            Ok(OperationState::Preparing { .. }) => Err(ControlError::InProgress),
            Err(ControlError::NotFound) => self.control.authorize_capture_child(
                authorization_id,
                capture_id,
                child_id,
                saved_grant,
            ),
            Err(error) => Err(error),
        }
    }

    fn resume_pending(&mut self) -> Result<(), BarrierError> {
        loop {
            self.ensure_pending_journaled()?;
            let barrier = self.pending.as_ref().ok_or(BarrierError::Conflict)?;
            let operation_id = barrier.operation_id;
            let branch_id = barrier.branch_id;
            let expected_revision = barrier.expected_revision;
            let attachment_id = barrier.attachment_id;
            let volume_epoch = barrier.volume_epoch;
            let previous_roots = barrier.previous_roots;
            let next_roots = barrier.next_roots;
            let capture_id = barrier.capture_id;
            let capture_authorization_id = barrier.capture_authorization_id;
            let saved_grant = barrier.writer_grant;
            let branch = self.control.get_branch(self.branch_id)?;
            let current_grant =
                validate_attachment(&branch, branch_id, attachment_id, volume_epoch, saved_grant)?;

            let operation_missing = match self.control.get_operation(operation_id) {
                Ok(OperationState::Committed(_)) => return self.finish_committed(),
                Ok(OperationState::Aborted) => {
                    if self.restart_pending(&branch, current_grant)? {
                        return Ok(());
                    }
                    continue;
                }
                Ok(OperationState::Preparing { .. }) => false,
                Err(ControlError::NotFound) => true,
                Err(error) => return Err(error.into()),
            };

            let active_capture_id = branch
                .capture
                .as_ref()
                .filter(|capture| !capture.sealed)
                .map(|capture| capture.operation_id);
            let child_conflicts = branch.capture.as_ref().is_some_and(|capture| {
                capture
                    .pending_child
                    .is_some_and(|child_id| child_id != operation_id)
            });
            if operation_missing && (active_capture_id != capture_id || child_conflicts) {
                if self.abort_and_restage(operation_id)? {
                    return Ok(());
                }
                continue;
            }

            if branch.revision != expected_revision || roots(&branch) != previous_roots {
                if self.abort_and_restage(operation_id)? {
                    return Ok(());
                }
                continue;
            }
            self.ensure_pending_map()?;

            if matches!(
                self.control.get_operation(operation_id),
                Err(ControlError::NotFound)
            ) {
                if let (Some(capture_id), Some(authorization_id)) =
                    (capture_id, capture_authorization_id)
                {
                    match self.authorize_pending_capture_child(
                        authorization_id,
                        capture_id,
                        operation_id,
                        saved_grant,
                    ) {
                        Ok(()) => {}
                        Err(error) => {
                            let stale_lease =
                                matches!(error, ControlError::Expired | ControlError::Fenced)
                                    && saved_grant.expires_at < current_grant.expires_at;
                            if stale_lease
                                || matches!(
                                    error,
                                    ControlError::Conflict
                                        | ControlError::Busy
                                        | ControlError::NotFound
                                        | ControlError::Aborted
                                )
                            {
                                if self.abort_and_restage(operation_id)? {
                                    return Ok(());
                                }
                                continue;
                            }
                            return Err(error.into());
                        }
                    }
                }
                match self.control.begin_preparation(PreparationRequest {
                    operation_id,
                    branch_id,
                    expected_revision,
                    grant: current_grant,
                    attachment_id,
                    volume_epoch,
                    protection_id: self
                        .pending
                        .as_ref()
                        .ok_or(BarrierError::Conflict)?
                        .protection_id,
                    kind: PreparationKind::VolumeBarrier,
                    expected_roots: Some(next_roots),
                    dependencies: vec![next_roots.volume],
                }) {
                    Ok(OperationState::Committed(_)) => return self.finish_committed(),
                    Ok(OperationState::Preparing { .. }) => {
                        self.last_renewal = Some(Instant::now());
                    }
                    Ok(OperationState::Aborted) => {
                        if self.restart_pending(&branch, current_grant)? {
                            return Ok(());
                        }
                        continue;
                    }
                    Err(ControlError::Conflict) => {
                        if self.abort_and_restage(operation_id)? {
                            return Ok(());
                        }
                        continue;
                    }
                    Err(error) => return Err(error.into()),
                }
            }

            if self.last_renewal.is_none() {
                match self.renew_preparation(current_grant) {
                    Ok(()) => {}
                    Err(BarrierError::Control(ControlError::Expired)) => {
                        if self.abort_and_restage(operation_id)? {
                            return Ok(());
                        }
                        continue;
                    }
                    Err(error) => return Err(error),
                }
            }

            let upload_count = self
                .pending
                .as_ref()
                .map_or(0, |pending| pending.objects.len());
            let mut restarted = false;
            for index in 0..upload_count {
                match self.renew_if_due()? {
                    ResumeStep::Continue => {}
                    ResumeStep::Restarted => {
                        restarted = true;
                        break;
                    }
                    ResumeStep::Completed => return Ok(()),
                }
                let object = &self.pending.as_ref().ok_or(BarrierError::Conflict)?.objects[index];
                object
                    .reference
                    .verify(&object.encoded)
                    .map_err(BarrierError::Format)?;
                self.inner
                    .store_mut()
                    .put_durable(object.reference, &object.encoded)
                    .map_err(BarrierError::ObjectStore)?;
            }
            if restarted {
                continue;
            }

            let latest = self.control.get_branch(self.branch_id)?;
            let current_grant = validate_attachment(
                &latest,
                branch_id,
                attachment_id,
                volume_epoch,
                current_grant,
            )?;
            if latest.revision != expected_revision || roots(&latest) != previous_roots {
                if self.abort_and_restage(operation_id)? {
                    return Ok(());
                }
                continue;
            }
            let committed = match self.control.commit_live_roots(
                operation_id,
                expected_revision,
                current_grant,
                next_roots,
                capture_id,
            ) {
                Ok(branch) => branch,
                Err(ControlError::Conflict) => {
                    if self.abort_and_restage(operation_id)? {
                        return Ok(());
                    }
                    continue;
                }
                Err(ControlError::Expired) => {
                    if self.abort_and_restage(operation_id)? {
                        return Ok(());
                    }
                    continue;
                }
                Err(error) => return Err(error.into()),
            };
            if committed.volume_root != next_roots.volume {
                return Err(BarrierError::Conflict);
            }
            let map = self.pending_map.ok_or(BarrierError::Conflict)?;
            let dirty_revision = self
                .pending
                .as_ref()
                .ok_or(BarrierError::Conflict)?
                .dirty_revision;
            self.inner.commit_durable_map(map, dirty_revision)?;
            self.live_roots = roots(&committed);
            self.revision = committed.revision;
            self.writer_grant = committed.writer.ok_or(BarrierError::Fenced)?;
            self.journal
                .finish_pending(operation_id)
                .map_err(BarrierError::Journal)?;
            self.pending = None;
            self.pending_map = None;
            self.pending_replaces = None;
            self.pending_journaled = false;
            self.last_renewal = None;
            return Ok(());
        }
    }

    fn ensure_pending_journaled(&mut self) -> Result<(), BarrierError> {
        if self.pending_journaled {
            return Ok(());
        }
        let pending = self.pending.as_ref().ok_or(BarrierError::Conflict)?;
        if let Some(previous_operation_id) = self.pending_replaces {
            self.journal
                .replace_pending(previous_operation_id, pending)
                .map_err(BarrierError::Journal)?;
        } else {
            self.journal
                .persist_pending(pending)
                .map_err(BarrierError::Journal)?;
        }
        self.pending_replaces = None;
        self.pending_journaled = true;
        Ok(())
    }

    fn ensure_pending_map(&mut self) -> Result<(), BarrierError> {
        if self.pending_map.is_some() {
            return Ok(());
        }
        // Reconstruct a recovered candidate from the validated base and its
        // bounded dirty delta. The builder validates changed paths without
        // walking immutable subtrees whose metadata was already checked.
        let staged = self.inner.stage_dirty()?.ok_or(BarrierError::Conflict)?;
        let pending = self.pending.as_mut().ok_or(BarrierError::Conflict)?;
        if staged.volume_root != pending.next_roots.volume
            || staged.dirty_revision != pending.dirty_revision
            || staged.dirty_extent_refs != pending.dirty_extent_refs
        {
            return Err(BarrierError::Journal(
                "reconstructed dirty state differs from the pending volume root".to_owned(),
            ));
        }
        pending.objects = staged.objects;
        self.pending_map = Some(staged.map);
        self.pending_journaled = false;
        self.ensure_pending_journaled()
    }

    fn renew_if_due(&mut self) -> Result<ResumeStep, BarrierError> {
        let unresolved = self
            .pending
            .as_ref()
            .is_some_and(|pending| pending.renewal_operation_id.is_some());
        let due = self
            .last_renewal
            .is_none_or(|last| last.elapsed() >= Duration::from_secs(180));
        if unresolved || due {
            let branch = self.control.get_branch(self.branch_id)?;
            let pending = self.pending.as_ref().ok_or(BarrierError::Conflict)?;
            let operation_id = pending.operation_id;
            let grant = validate_attachment(
                &branch,
                pending.branch_id,
                pending.attachment_id,
                pending.volume_epoch,
                pending.writer_grant,
            )?;
            match self.renew_preparation(grant) {
                Ok(()) => {}
                Err(BarrierError::Control(ControlError::Expired)) => {
                    return if self.abort_and_restage(operation_id)? {
                        Ok(ResumeStep::Completed)
                    } else {
                        Ok(ResumeStep::Restarted)
                    };
                }
                Err(error) => return Err(error),
            }
        }
        Ok(ResumeStep::Continue)
    }

    fn abort_and_restage(&mut self, operation_id: OperationId) -> Result<bool, BarrierError> {
        match self.control.abort_operation(operation_id)? {
            OperationState::Committed(_) => {
                self.finish_committed()?;
                Ok(true)
            }
            OperationState::Aborted => {
                let branch = self.control.get_branch(self.branch_id)?;
                let grant = validate_attachment(
                    &branch,
                    self.branch_id,
                    self.attachment_id,
                    self.volume_epoch,
                    self.writer_grant,
                )?;
                self.restart_pending(&branch, grant)
            }
            OperationState::Preparing { .. } => Err(BarrierError::Conflict),
        }
    }

    /// Rebuilds the pending volume map over the latest authoritative roots.
    /// Returns true when no dirty overlay remains and the pending record is gone.
    fn restart_pending(
        &mut self,
        branch: &BranchState,
        grant: WriterGrant,
    ) -> Result<bool, BarrierError> {
        let old_operation_id = self
            .pending
            .as_ref()
            .ok_or(BarrierError::Conflict)?
            .operation_id;
        let base = if branch.volume_root == self.live_roots.volume {
            self.inner.base_map()
        } else {
            self.inner.load_map(branch.volume_root)?
        };
        self.inner.rebase_base(base);
        self.live_roots = roots(branch);
        self.revision = branch.revision;
        self.writer_grant = grant;
        let Some(staged) = self.inner.stage_dirty()? else {
            self.journal
                .finish_pending(old_operation_id)
                .map_err(BarrierError::Journal)?;
            self.pending = None;
            self.pending_map = None;
            self.pending_replaces = None;
            self.pending_journaled = false;
            self.last_renewal = None;
            return Ok(true);
        };
        let (ids, capture_id, capture_authorization_id) = self.reserve_barrier_context(branch)?;
        let next_roots = LiveRoots {
            volume: staged.volume_root,
            logs: self.live_roots.logs,
            artifacts: self.live_roots.artifacts,
        };
        let map = staged.map;
        let replacement = self.pending_from_stage(
            ids,
            next_roots,
            staged,
            capture_id,
            capture_authorization_id,
        );
        // Keep the exact replacement in memory even when rename/sync returns
        // an error: the old or new record may now be the visible journal state.
        self.pending = Some(replacement);
        self.pending_map = Some(map);
        self.pending_replaces = Some(old_operation_id);
        self.pending_journaled = false;
        self.last_renewal = None;
        self.ensure_pending_journaled()?;
        Ok(false)
    }

    fn renew_preparation(&mut self, grant: WriterGrant) -> Result<(), BarrierError> {
        let (renewal_id, renewal_grant) = match self.pending.as_ref() {
            Some(PendingBarrier {
                renewal_operation_id: Some(id),
                renewal_grant: Some(saved_grant),
                ..
            }) => (*id, *saved_grant),
            Some(PendingBarrier {
                renewal_operation_id: None,
                renewal_grant: None,
                ..
            }) => {
                let id = self
                    .journal
                    .reserve_operation_id()
                    .map_err(BarrierError::Journal)?;
                let pending = self.pending.as_mut().ok_or(BarrierError::Conflict)?;
                pending.renewal_operation_id = Some(id);
                pending.renewal_grant = Some(grant);
                self.pending_journaled = false;
                self.ensure_pending_journaled()?;
                (id, grant)
            }
            _ => return Err(BarrierError::Conflict),
        };
        let prepared_id = self
            .pending
            .as_ref()
            .ok_or(BarrierError::Conflict)?
            .operation_id;
        match self
            .control
            .renew_preparation(renewal_id, prepared_id, renewal_grant)
        {
            Ok(_) => {}
            Err(ControlError::Expired | ControlError::Fenced)
                if same_grant_identity(renewal_grant, grant)
                    && renewal_grant.expires_at < grant.expires_at =>
            {
                // An expired result cannot be a committed replay. Its exact
                // request cannot succeed later because the supplied writer
                // grant is expired, so bind the fresh lease to a new ID.
                let replacement_id = self
                    .journal
                    .reserve_operation_id()
                    .map_err(BarrierError::Journal)?;
                let pending = self.pending.as_mut().ok_or(BarrierError::Conflict)?;
                pending.renewal_operation_id = Some(replacement_id);
                pending.renewal_grant = Some(grant);
                self.pending_journaled = false;
                self.ensure_pending_journaled()?;
                return self.renew_preparation(grant);
            }
            Err(error) => return Err(error.into()),
        }
        let pending = self.pending.as_mut().ok_or(BarrierError::Conflict)?;
        pending.renewal_operation_id = None;
        pending.renewal_grant = None;
        self.pending_journaled = false;
        if let Err(error) = self.ensure_pending_journaled() {
            if let Some(pending) = self.pending.as_mut() {
                pending.renewal_operation_id = Some(renewal_id);
                pending.renewal_grant = Some(renewal_grant);
            }
            self.pending_journaled = false;
            return Err(error);
        }
        self.last_renewal = Some(Instant::now());
        Ok(())
    }

    fn finish_committed(&mut self) -> Result<(), BarrierError> {
        let pending = self.pending.as_ref().ok_or(BarrierError::Conflict)?;
        let operation_id = pending.operation_id;
        let dirty_revision = pending.dirty_revision;
        let branch = self.control.get_branch(self.branch_id)?;
        let grant = validate_attachment(
            &branch,
            pending.branch_id,
            pending.attachment_id,
            pending.volume_epoch,
            pending.writer_grant,
        )?;
        let map = if branch.volume_root == self.live_roots.volume {
            self.inner.base_map()
        } else if branch.volume_root == pending.next_roots.volume {
            match self.pending_map {
                Some(map) => map,
                None => self.inner.load_map(branch.volume_root)?,
            }
        } else {
            self.inner.load_map(branch.volume_root)?
        };
        if self.inner.dirty_extent_count() > 0 {
            self.inner.commit_durable_map(map, dirty_revision)?;
        } else {
            self.inner.replace_base(map);
        }
        self.live_roots = roots(&branch);
        self.revision = branch.revision;
        self.writer_grant = grant;
        self.journal
            .finish_pending(operation_id)
            .map_err(BarrierError::Journal)?;
        self.pending = None;
        self.pending_map = None;
        self.pending_replaces = None;
        self.pending_journaled = false;
        self.last_renewal = None;
        Ok(())
    }
}

impl<S, C, J> BlockDevice for DurableVolume<S, C, J>
where
    S: DurableObjectStore,
    C: VolumeBarrierControl,
    J: BarrierJournal,
{
    fn geometry(&self) -> VolumeGeometry {
        self.inner.geometry()
    }

    fn supports_fua(&self) -> bool {
        true
    }

    fn dispatch(&mut self, request: BlockRequest<'_>) -> Result<BlockResponse, BlockError> {
        match request {
            BlockRequest::Read { offset, length } => {
                self.inner.read(offset, length).map(BlockResponse::Read)
            }
            BlockRequest::Write { offset, data, fua } => {
                if self.fenced {
                    return Err(BlockError::Barrier(BarrierError::Fenced.to_string()));
                }
                if self.pending.is_some() {
                    return Err(BlockError::BarrierUnresolved);
                }
                self.inner.write(offset, data)?;
                if fua {
                    self.flush()
                        .map_err(|error| BlockError::Barrier(error.to_string()))?;
                }
                Ok(BlockResponse::Written(data.len()))
            }
            BlockRequest::Flush => self
                .flush()
                .map(|()| BlockResponse::Written(0))
                .map_err(|error| BlockError::Barrier(error.to_string())),
            BlockRequest::Unsupported(operation) => {
                Err(BlockError::UnsupportedOperation(operation))
            }
        }
    }
}

fn roots(branch: &BranchState) -> LiveRoots {
    LiveRoots {
        volume: branch.volume_root,
        logs: branch.logs_root,
        artifacts: branch.artifacts_root,
    }
}

fn same_grant_identity(left: WriterGrant, right: WriterGrant) -> bool {
    left.branch_id == right.branch_id
        && left.writer_epoch == right.writer_epoch
        && left.owner_incarnation == right.owner_incarnation
        && left.grant_id == right.grant_id
}

fn validate_attachment(
    branch: &BranchState,
    branch_id: BranchId,
    attachment_id: AttachmentId,
    volume_epoch: u64,
    prior_grant: WriterGrant,
) -> Result<WriterGrant, BarrierError> {
    let grant = branch.writer.ok_or(BarrierError::Fenced)?;
    let attachment = branch.attachment.ok_or(BarrierError::Fenced)?;
    if branch.branch_id != branch_id
        || !same_grant_identity(grant, prior_grant)
        || attachment.attachment_id != attachment_id
        || attachment.writer_epoch != grant.writer_epoch
        || attachment.volume_epoch != volume_epoch
        || branch.volume_epoch != volume_epoch
    {
        return Err(BarrierError::Fenced);
    }
    Ok(grant)
}

fn load_map<S: MapObjectStore>(
    store: &S,
    volume_root: ObjectRef,
) -> Result<VolumeExtentMap, BarrierError> {
    let encoded = store
        .get_ref(volume_root)
        .map_err(ViewError::Store)?
        .ok_or(ViewError::MissingObject(volume_root.object_id))?;
    volume_root.verify(&encoded)?;
    let decoded = VolumeRoot::decode(&encoded)?;
    Ok(VolumeExtentMap::from_root(decoded)?.validate(store)?)
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex, atomic::AtomicBool};
    use versionfs_core::control::{OwnerIncarnation, WriterGrant};
    use versionfs_core::fault::OneShotFailures;
    use versionfs_core::format::{EXTENT_BYTES, ObjectType};
    use versionfs_core::identity::{ObjectId, ObjectStoreScopeId, WorkspaceId};
    use versionfs_core::metadata::MetadataError;
    use versionfs_core::time::MonotonicTime;

    static NEXT_TEST_DIR: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn generation_builder_persists_the_established_capture_mode_and_proves_roots() {
        let mut store = MemoryStore::default();
        let geometry =
            VolumeGeometry::new(u64::from(EXTENT_BYTES) * 4).expect("valid test geometry");
        let parent = create_empty_generation(
            &mut store,
            GenerationId::from_bytes([1; 16]),
            VolumeBoundaryId::from_bytes([2; 16]),
            geometry,
            10,
        )
        .expect("create and verify parent Generation");

        let child = create_generation(
            &mut store,
            GenerationId::from_bytes([3; 16]),
            Some(parent.generation_id),
            parent.roots,
            CaptureMode::FilesystemCleanUnmount,
            VolumeBoundaryId::from_bytes([4; 16]),
            20,
        )
        .expect("create and verify captured Generation");
        let child_seed = child.seed();
        let descriptor_bytes = store
            .get(child_seed.descriptor.object_id)
            .expect("read descriptor")
            .expect("descriptor exists");
        let descriptor =
            GenerationDescriptor::decode(&descriptor_bytes).expect("canonical descriptor");
        assert_eq!(descriptor.generation_id, child_seed.generation_id);
        assert_eq!(descriptor.parent_generation_id, Some(parent.generation_id));
        assert_eq!(descriptor.capture_mode, CaptureMode::FilesystemCleanUnmount);
        assert_eq!(
            LiveRoots {
                volume: descriptor.volume_root,
                logs: descriptor.logs_root,
                artifacts: descriptor.artifacts_root,
            },
            child_seed.roots
        );
    }

    struct TestDir(PathBuf);

    impl TestDir {
        fn new() -> Self {
            let id = NEXT_TEST_DIR.fetch_add(1, Ordering::Relaxed);
            let path =
                std::env::temp_dir().join(format!("versionfs-barrier-{}-{id}", std::process::id()));
            fs::create_dir_all(&path).expect("create test directory");
            Self(path)
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    struct FailingReplacementJournal {
        inner: FileBarrierJournal,
        failures: Arc<OneShotFailures>,
        fail_next_replace: Option<FailurePoint>,
        attempts: Vec<(OperationId, PendingBarrier)>,
    }

    impl BarrierJournal for FailingReplacementJournal {
        fn reserve_barrier_ids(&mut self) -> Result<BarrierIds, String> {
            self.inner.reserve_barrier_ids()
        }

        fn reserve_operation_id(&mut self) -> Result<OperationId, String> {
            self.inner.reserve_operation_id()
        }

        fn load_pending(&mut self) -> Result<Option<PendingBarrier>, String> {
            self.inner.load_pending()
        }

        fn persist_pending(&mut self, barrier: &PendingBarrier) -> Result<(), String> {
            self.inner.persist_pending(barrier)
        }

        fn replace_pending(
            &mut self,
            expected_operation_id: OperationId,
            replacement: &PendingBarrier,
        ) -> Result<(), String> {
            self.attempts
                .push((expected_operation_id, replacement.clone()));
            if let Some(point) = self.fail_next_replace.take() {
                self.failures.arm(point);
            }
            self.inner
                .replace_pending(expected_operation_id, replacement)
        }

        fn finish_pending(&mut self, operation_id: OperationId) -> Result<(), String> {
            self.inner.finish_pending(operation_id)
        }
    }

    #[derive(Clone, Default)]
    struct MemoryStore {
        objects: Arc<Mutex<HashMap<ObjectId, Vec<u8>>>>,
        fail_next_upload: Arc<AtomicBool>,
        reads: Arc<AtomicU64>,
    }

    impl MapObjectStore for MemoryStore {
        fn proof_scope(&self) -> ObjectStoreScopeId {
            ObjectStoreScopeId::from_bytes([0xA7; 16])
        }

        fn get(&self, id: ObjectId) -> Result<Option<Vec<u8>>, String> {
            self.reads.fetch_add(1, Ordering::Relaxed);
            self.objects
                .lock()
                .map(|objects| objects.get(&id).cloned())
                .map_err(|_| "memory object-store lock poisoned".to_owned())
        }

        fn put(&mut self, id: ObjectId, bytes: Vec<u8>) -> Result<(), String> {
            self.objects
                .lock()
                .map_err(|_| "memory object-store lock poisoned".to_owned())?
                .insert(id, bytes);
            Ok(())
        }
    }

    impl DurableObjectStore for MemoryStore {
        fn put_durable(&mut self, reference: ObjectRef, encoded: &[u8]) -> Result<(), String> {
            reference
                .verify(encoded)
                .map_err(|error| format!("invalid durable object: {error}"))?;
            if self.fail_next_upload.swap(false, Ordering::SeqCst) {
                return Err("injected durable upload failure".to_owned());
            }
            let mut objects = self
                .objects
                .lock()
                .map_err(|_| "memory object-store lock poisoned".to_owned())?;
            if let Some(existing) = objects.get(&reference.object_id) {
                if existing != encoded {
                    return Err("immutable object ID was reused with different bytes".to_owned());
                }
            } else {
                objects.insert(reference.object_id, encoded.to_vec());
            }
            Ok(())
        }
    }

    #[derive(Clone)]
    struct TypedOnlyStore(MemoryStore);

    impl MapObjectStore for TypedOnlyStore {
        fn proof_scope(&self) -> ObjectStoreScopeId {
            self.0.proof_scope()
        }

        fn get(&self, _id: ObjectId) -> Result<Option<Vec<u8>>, String> {
            Err("typed references are required".to_owned())
        }

        fn get_ref(&self, reference: ObjectRef) -> Result<Option<Vec<u8>>, String> {
            self.0.get(reference.object_id)
        }

        fn put(&mut self, id: ObjectId, bytes: Vec<u8>) -> Result<(), String> {
            self.0.put(id, bytes)
        }
    }

    impl DurableObjectStore for TypedOnlyStore {
        fn put_durable(&mut self, reference: ObjectRef, encoded: &[u8]) -> Result<(), String> {
            self.0.put_durable(reference, encoded)
        }
    }

    #[derive(Clone)]
    struct FakeControl(Arc<Mutex<FakeControlState>>);

    #[derive(Clone, Copy)]
    enum AuthorizationFailure {
        BeforeCommit,
        AfterCommit,
    }

    struct FakeControlState {
        branch: BranchState,
        operations: HashMap<OperationId, OperationState>,
        capture_authorizations: HashMap<OperationId, (OperationId, OperationId, WriterGrant)>,
        fail_next_authorization: Option<AuthorizationFailure>,
        renewals: HashMap<OperationId, (MonotonicTime, WriterGrant)>,
        expire_next_renewal: bool,
        lose_next_commit_reply: bool,
        branch_after_committed_read: Option<BranchState>,
        commit_count: usize,
    }

    impl VolumeBarrierControl for FakeControl {
        fn get_branch(&self, branch_id: BranchId) -> Result<BranchState, ControlError> {
            let state = self.0.lock().map_err(|_| ControlError::InvalidTransition)?;
            if state.branch.branch_id != branch_id {
                return Err(ControlError::NotFound);
            }
            Ok(state.branch.clone())
        }

        fn begin_preparation(
            &self,
            request: PreparationRequest,
        ) -> Result<OperationState, ControlError> {
            let mut state = self.0.lock().map_err(|_| ControlError::InvalidTransition)?;
            if let Some(operation) = state.operations.get(&request.operation_id) {
                return Ok(operation.clone());
            }
            let branch = &state.branch;
            let grant = branch.writer.ok_or(ControlError::Fenced)?;
            let attachment = branch.attachment.ok_or(ControlError::Fenced)?;
            if branch.revision != request.expected_revision
                || !same_grant_identity(grant, request.grant)
                || attachment.attachment_id != request.attachment_id
                || attachment.volume_epoch != request.volume_epoch
                || request.kind != PreparationKind::VolumeBarrier
            {
                return Err(ControlError::Conflict);
            }
            if let Some(capture) = &branch.capture
                && (capture.sealed || capture.pending_child != Some(request.operation_id))
            {
                return Err(ControlError::Busy);
            }
            let operation = OperationState::Preparing {
                operation_id: request.operation_id,
                expires_at: MonotonicTime::from_nanos(600_000_000_000),
                branch_id: request.branch_id,
            };
            state
                .operations
                .insert(request.operation_id, operation.clone());
            Ok(operation)
        }

        fn authorize_capture_child(
            &self,
            authorization_id: OperationId,
            capture_id: OperationId,
            child_id: OperationId,
            grant: WriterGrant,
        ) -> Result<(), ControlError> {
            let mut state = self.0.lock().map_err(|_| ControlError::InvalidTransition)?;
            if let Some((prior_capture_id, prior_child_id, prior_grant)) =
                state.capture_authorizations.get(&authorization_id)
            {
                return if (*prior_capture_id, *prior_child_id, *prior_grant)
                    == (capture_id, child_id, grant)
                {
                    Ok(())
                } else {
                    Err(ControlError::OperationMismatch)
                };
            }
            if state.branch.writer != Some(grant) {
                return Err(ControlError::Fenced);
            }
            let failure = state.fail_next_authorization.take();
            if matches!(failure, Some(AuthorizationFailure::BeforeCommit)) {
                return Err(ControlError::Metadata(MetadataError::UnknownOutcome {
                    operation_id: authorization_id.as_bytes().to_vec(),
                    cause: "injected lost authorization before commit".to_owned(),
                }));
            }
            let capture = state
                .branch
                .capture
                .as_mut()
                .ok_or(ControlError::NotFound)?;
            if capture.operation_id != capture_id || capture.sealed {
                return Err(ControlError::InvalidTransition);
            }
            if capture.pending_child.is_some() {
                return Err(ControlError::Busy);
            }
            capture.pending_child = Some(child_id);
            state
                .capture_authorizations
                .insert(authorization_id, (capture_id, child_id, grant));
            state
                .operations
                .insert(authorization_id, OperationState::Committed(Vec::new()));
            if matches!(failure, Some(AuthorizationFailure::AfterCommit)) {
                return Err(ControlError::Metadata(MetadataError::UnknownOutcome {
                    operation_id: authorization_id.as_bytes().to_vec(),
                    cause: "injected lost authorization after commit".to_owned(),
                }));
            }
            Ok(())
        }

        fn renew_preparation(
            &self,
            renewal_id: OperationId,
            prepared_id: OperationId,
            grant: WriterGrant,
        ) -> Result<MonotonicTime, ControlError> {
            let mut state = self.0.lock().map_err(|_| ControlError::InvalidTransition)?;
            if let Some((deadline, original_grant)) = state.renewals.get(&renewal_id) {
                if *original_grant != grant {
                    return Err(ControlError::OperationMismatch);
                }
                return Ok(*deadline);
            }
            if !matches!(
                state.operations.get(&prepared_id),
                Some(OperationState::Preparing { .. })
            ) {
                return Err(ControlError::InvalidTransition);
            }
            if state.expire_next_renewal {
                state.expire_next_renewal = false;
                return Err(ControlError::Expired);
            }
            if state.branch.writer != Some(grant) {
                return Err(ControlError::Fenced);
            }
            let deadline = MonotonicTime::from_nanos(600_000_000_000);
            state.renewals.insert(renewal_id, (deadline, grant));
            Ok(deadline)
        }

        fn commit_live_roots(
            &self,
            operation_id: OperationId,
            expected_revision: u64,
            _grant: WriterGrant,
            roots: LiveRoots,
            capture_id: Option<OperationId>,
        ) -> Result<BranchState, ControlError> {
            let mut state = self.0.lock().map_err(|_| ControlError::InvalidTransition)?;
            if matches!(
                state.operations.get(&operation_id),
                Some(OperationState::Committed(_))
            ) {
                return Ok(state.branch.clone());
            }
            if !matches!(
                state.operations.get(&operation_id),
                Some(OperationState::Preparing { .. })
            ) {
                return Err(ControlError::InvalidTransition);
            }
            if state.branch.revision != expected_revision {
                return Err(ControlError::Conflict);
            }
            match (&mut state.branch.capture, capture_id) {
                (None, None) => {}
                (Some(capture), Some(capture_id))
                    if capture.operation_id == capture_id
                        && !capture.sealed
                        && capture.pending_child == Some(operation_id) =>
                {
                    capture.pending_child = None;
                }
                _ => return Err(ControlError::Busy),
            }
            state.branch.volume_root = roots.volume;
            state.branch.logs_root = roots.logs;
            state.branch.artifacts_root = roots.artifacts;
            state.branch.revision += 1;
            state.commit_count += 1;
            state
                .operations
                .insert(operation_id, OperationState::Committed(Vec::new()));
            if state.lose_next_commit_reply {
                state.lose_next_commit_reply = false;
                return Err(ControlError::Metadata(MetadataError::UnknownOutcome {
                    operation_id: operation_id.as_bytes().to_vec(),
                    cause: "injected lost commit reply".to_owned(),
                }));
            }
            Ok(state.branch.clone())
        }

        fn get_operation(&self, operation_id: OperationId) -> Result<OperationState, ControlError> {
            let mut state = self.0.lock().map_err(|_| ControlError::InvalidTransition)?;
            let operation = state
                .operations
                .get(&operation_id)
                .cloned()
                .ok_or(ControlError::NotFound)?;
            if matches!(operation, OperationState::Committed(_))
                && let Some(branch) = state.branch_after_committed_read.take()
            {
                state.branch = branch;
            }
            Ok(operation)
        }

        fn abort_operation(
            &self,
            operation_id: OperationId,
        ) -> Result<OperationState, ControlError> {
            let mut state = self.0.lock().map_err(|_| ControlError::InvalidTransition)?;
            if let Some(capture) = state.branch.capture.as_mut()
                && capture.pending_child == Some(operation_id)
            {
                capture.pending_child = None;
            }
            let operation = state
                .operations
                .entry(operation_id)
                .or_insert(OperationState::Aborted);
            if matches!(operation, OperationState::Preparing { .. }) {
                *operation = OperationState::Aborted;
            }
            Ok(operation.clone())
        }
    }

    struct TestEnvironment {
        _directory: TestDir,
        store: MemoryStore,
        control: FakeControl,
        branch_id: BranchId,
        grant: WriterGrant,
        attachment_id: AttachmentId,
    }

    impl TestEnvironment {
        fn new() -> Self {
            let mut store = MemoryStore::default();
            let branch_id = BranchId::from_bytes([1; 16]);
            let generation_id = GenerationId::from_bytes([2; 16]);
            let boundary_id = VolumeBoundaryId::from_bytes([3; 16]);
            let seed = create_empty_generation(
                &mut store,
                generation_id,
                boundary_id,
                VolumeGeometry::new(u64::from(EXTENT_BYTES) * 4).expect("valid test geometry"),
                1,
            )
            .expect("create durable empty graph");
            let grant = WriterGrant {
                branch_id,
                writer_epoch: 1,
                owner_incarnation: OwnerIncarnation::from_u64(1),
                grant_id: OperationId::from_bytes([4; 16]),
                expires_at: MonotonicTime::from_nanos(u64::MAX),
            };
            let attachment_id = AttachmentId::from_bytes([5; 16]);
            let branch = BranchState {
                workspace_id: WorkspaceId::from_bytes([6; 16]),
                branch_id,
                name: "main".to_owned(),
                head: generation_id,
                revision: 1,
                pending_mutation_preparations: 0,
                writer_epoch: 1,
                volume_epoch: 1,
                stream_epoch: 1,
                volume_root: seed.roots.volume,
                logs_root: seed.roots.logs,
                artifacts_root: seed.roots.artifacts,
                writer: Some(grant),
                attachment: Some(AttachmentState {
                    attachment_id,
                    protection_id: ProtectionId::from_bytes([7; 16]),
                    base_generation: generation_id,
                    writer_epoch: 1,
                    volume_epoch: 1,
                    protection_expires_at: MonotonicTime::from_nanos(u64::MAX),
                }),
                capture: None,
                deleted: false,
            };
            let control = FakeControl(Arc::new(Mutex::new(FakeControlState {
                branch,
                operations: HashMap::new(),
                capture_authorizations: HashMap::new(),
                fail_next_authorization: None,
                renewals: HashMap::new(),
                expire_next_renewal: false,
                lose_next_commit_reply: false,
                branch_after_committed_read: None,
                commit_count: 0,
            })));
            Self {
                _directory: TestDir::new(),
                store,
                control,
                branch_id,
                grant,
                attachment_id,
            }
        }

        fn open(&self) -> DurableVolume<MemoryStore, FakeControl, FileBarrierJournal> {
            DurableVolume::open(
                self.store.clone(),
                self.control.clone(),
                FileBarrierJournal::open(&self._directory.0).expect("open file journal"),
                self.branch_id,
                self.grant,
                self.attachment_id,
                EXTENT_BYTES as usize * 4,
            )
            .expect("open durable volume")
        }
    }

    fn sample_pending() -> PendingBarrier {
        let branch_id = BranchId::from_bytes([11; 16]);
        let grant = WriterGrant {
            branch_id,
            writer_epoch: 3,
            owner_incarnation: OwnerIncarnation::from_u64(4),
            grant_id: OperationId::from_bytes([12; 16]),
            expires_at: MonotonicTime::from_nanos(99),
        };
        let (previous_volume, _) = typed_object(ObjectType::VolumeRoot, b"old volume root");
        let (logs, _) = typed_object(ObjectType::LogMapRoot, b"logs root");
        let (artifacts, _) = typed_object(ObjectType::ArtifactMapRoot, b"artifacts root");
        let (next_volume, next_volume_bytes) =
            typed_object(ObjectType::VolumeRoot, b"new volume root");
        let (extent, extent_bytes) = typed_object(ObjectType::Extent, b"extent bytes");
        PendingBarrier {
            operation_id: OperationId::from_bytes([13; 16]),
            protection_id: ProtectionId::from_bytes([14; 16]),
            branch_id,
            expected_revision: 5,
            writer_grant: grant,
            attachment_id: AttachmentId::from_bytes([15; 16]),
            volume_epoch: 6,
            capture_id: None,
            capture_authorization_id: None,
            previous_roots: LiveRoots {
                volume: previous_volume,
                logs,
                artifacts,
            },
            next_roots: LiveRoots {
                volume: next_volume,
                logs,
                artifacts,
            },
            dirty_revision: 9,
            dirty_extent_refs: vec![(2, extent)],
            objects: vec![
                StagedObject {
                    reference: next_volume,
                    encoded: next_volume_bytes,
                },
                StagedObject {
                    reference: extent,
                    encoded: extent_bytes,
                },
            ],
            renewal_operation_id: Some(OperationId::from_bytes([16; 16])),
            renewal_grant: Some(grant),
        }
    }

    fn typed_object(object_type: ObjectType, payload: &[u8]) -> (ObjectRef, Vec<u8>) {
        let encoded = versionfs_core::format::encode_object(object_type, payload)
            .expect("encode typed test object");
        let reference = ObjectRef::from_encoded(&encoded).expect("reference typed test object");
        (reference, encoded)
    }

    #[test]
    fn file_journal_reservations_survive_reopen_without_reuse() {
        let directory = TestDir::new();
        let first = {
            let mut journal = FileBarrierJournal::open(&directory.0).expect("open journal");
            let first = journal.reserve_barrier_ids().expect("reserve barrier IDs");
            let renewal = journal.reserve_operation_id().expect("reserve renewal ID");
            assert_ne!(
                first.operation_id.as_bytes(),
                first.protection_id.as_bytes()
            );
            assert_ne!(first.operation_id.as_bytes(), renewal.as_bytes());
            first
        };
        let mut reopened = FileBarrierJournal::open(&directory.0).expect("reopen journal");
        let second = reopened.reserve_barrier_ids().expect("reserve next IDs");
        assert_ne!(first.operation_id, second.operation_id);
        assert_ne!(first.protection_id, second.protection_id);
        assert_ne!(
            first.operation_id.as_bytes(),
            second.protection_id.as_bytes()
        );
        assert_ne!(
            first.protection_id.as_bytes(),
            second.operation_id.as_bytes()
        );
    }

    #[test]
    fn file_journal_reopens_after_interrupted_id_initialization() {
        for point in [
            FailurePoint::JournalBeforePublish,
            FailurePoint::JournalAfterRenameBeforeDirectorySync,
        ] {
            let directory = TestDir::new();
            let failures = Arc::new(OneShotFailures::default());
            failures.arm(point);
            assert!(FileBarrierJournal::open_with(&directory.0, failures).is_err());
            let ids_path = directory.0.join("barrier-ids.v1");
            if point == FailurePoint::JournalBeforePublish {
                assert!(!ids_path.exists());
            } else {
                decode_ids(&fs::read(&ids_path).unwrap()).expect("complete published ID state");
            }

            let first = FileBarrierJournal::open(&directory.0)
                .unwrap()
                .reserve_barrier_ids()
                .unwrap();
            let second = FileBarrierJournal::open(&directory.0)
                .unwrap()
                .reserve_barrier_ids()
                .unwrap();
            assert_ne!(first.operation_id, second.operation_id);
            assert_ne!(first.protection_id, second.protection_id);
        }
    }

    #[test]
    fn opening_a_live_root_uses_its_typed_object_reference() {
        let environment = TestEnvironment::new();
        let branch = environment
            .control
            .get_branch(environment.branch_id)
            .expect("read initial branch");
        let typed_store = TypedOnlyStore(environment.store.clone());

        let map = load_map(&typed_store, branch.volume_root)
            .expect("resolve the volume root with its typed reference");
        assert_eq!(
            map.geometry(),
            VolumeGeometry::new(u64::from(EXTENT_BYTES) * 4).unwrap()
        );
    }

    #[test]
    fn staging_exports_only_map_nodes_reachable_from_the_sealed_root() {
        let environment = TestEnvironment::new();
        let mut worker = environment.open();
        worker
            .inner
            .write(0, &[0xA1; 4096])
            .expect("stage first extent write");
        worker
            .inner
            .write(u64::from(EXTENT_BYTES), &[0xB2; 4096])
            .expect("stage second extent write");

        let staged = worker
            .inner
            .stage_dirty()
            .expect("stage updated persistent map")
            .expect("dirty extents produce a staged map");
        assert_eq!(staged.dirty_extent_refs.len(), 2);
        assert_eq!(
            staged.objects.len(),
            4,
            "only two extents, the final leaf, and the volume root are reachable"
        );
        assert_eq!(
            staged
                .objects
                .iter()
                .filter(|object| object.reference.object_type == ObjectType::VolumeMapNode)
                .count(),
            1,
            "the intermediate leaf from the first insertion must not be uploaded"
        );
        assert!(
            staged
                .objects
                .iter()
                .any(|object| object.reference == staged.volume_root)
        );
    }

    #[test]
    fn flush_and_recovery_reuse_validated_unchanged_extent_subtrees() {
        let environment = TestEnvironment::new();
        let mut store = environment.store.clone();
        let encoded = versionfs_core::format::encode_object(
            ObjectType::Extent,
            &vec![0xB4; EXTENT_BYTES as usize],
        )
        .unwrap();
        let extent = ObjectRef::from_encoded(&encoded).unwrap();
        store.put_durable(extent, &encoded).unwrap();
        let geometry = VolumeGeometry::new(2049 * u64::from(EXTENT_BYTES)).unwrap();
        let mut map = VolumeExtentMap::empty(geometry, &mut store).unwrap();
        for number in 0..2048 {
            map = map.insert_extent(&mut store, number, extent).unwrap();
        }
        let root = map.seal(&mut store).unwrap();
        environment.control.0.lock().unwrap().branch.volume_root = root;
        let mut worker = environment.open();

        for number in [0, 2047] {
            store.reads.store(0, Ordering::Relaxed);
            worker
                .dispatch(BlockRequest::Write {
                    offset: number * u64::from(EXTENT_BYTES),
                    data: &[0xC5; 4096],
                    fua: true,
                })
                .unwrap();
            let reads = store.reads.load(Ordering::Relaxed);
            assert!(
                reads <= 6,
                "a one-extent FUA read {reads} objects, including unchanged map subtrees"
            );
        }

        worker.inner.write(4096, &[0xD6; 4096]).unwrap();
        store.fail_next_upload.store(true, Ordering::SeqCst);
        assert!(matches!(worker.flush(), Err(BarrierError::ObjectStore(_))));
        drop(worker);
        let live_root = environment.control.0.lock().unwrap().branch.volume_root;
        store.reads.store(0, Ordering::Relaxed);
        load_map(&store, live_root).unwrap();
        let imported_base_reads = store.reads.swap(0, Ordering::Relaxed);
        let mut recovered = environment.open();
        let recovery_reads = store.reads.load(Ordering::Relaxed);
        assert!(
            recovery_reads <= imported_base_reads + 6,
            "recovery read {recovery_reads} objects for a base requiring {imported_base_reads} reads"
        );
        assert_eq!(recovered.inner.read(4096, 4096).unwrap(), vec![0xD6; 4096]);

        recovered.inner.write(8192, &[0xE7; 4096]).unwrap();
        environment.control.0.lock().unwrap().lose_next_commit_reply = true;
        assert!(matches!(
            recovered.flush(),
            Err(BarrierError::Control(ControlError::Metadata(
                MetadataError::UnknownOutcome { .. }
            )))
        ));
        store.reads.store(0, Ordering::Relaxed);
        recovered.flush().unwrap();
        assert!(
            store.reads.load(Ordering::Relaxed) <= 2,
            "reconciling a known candidate must reuse its validated map"
        );
    }

    #[test]
    fn file_journal_rejects_concurrent_process_ownership() {
        let directory = TestDir::new();
        let journal = FileBarrierJournal::open(&directory.0).expect("open journal");
        assert!(FileBarrierJournal::open(&directory.0).is_err());
        drop(journal);
        FileBarrierJournal::open(&directory.0).expect("reopen after owner exits");
    }

    #[cfg(unix)]
    #[test]
    fn file_journal_restricts_recovery_data_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let directory = TestDir::new();
        let mut journal = FileBarrierJournal::open(&directory.0).expect("open journal");
        journal.reserve_barrier_ids().expect("reserve IDs");
        journal
            .persist_pending(&sample_pending())
            .expect("persist pending barrier");

        assert_eq!(
            fs::metadata(&directory.0)
                .expect("stat journal directory")
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
        for name in ["barrier.lock", "barrier-ids.v1", "pending-barrier.v1"] {
            assert_eq!(
                fs::metadata(directory.0.join(name))
                    .expect("stat journal file")
                    .permissions()
                    .mode()
                    & 0o777,
                0o600,
                "unexpected permissions for {name}"
            );
        }
    }

    #[test]
    fn file_journal_round_trips_pending_barrier_and_rejects_corruption() {
        let directory = TestDir::new();
        let barrier = sample_pending();
        let mut journal = FileBarrierJournal::open(&directory.0).expect("open journal");
        journal
            .persist_pending(&barrier)
            .expect("persist pending barrier");
        drop(journal);

        let mut reopened = FileBarrierJournal::open(&directory.0).expect("reopen journal");
        assert_eq!(
            reopened.load_pending().expect("read journal"),
            Some(barrier.clone())
        );
        assert!(
            reopened
                .finish_pending(OperationId::from_bytes([99; 16]))
                .is_err()
        );
        reopened
            .finish_pending(barrier.operation_id)
            .expect("finish exact barrier");
        assert_eq!(reopened.load_pending().expect("read cleared journal"), None);

        reopened.persist_pending(&barrier).expect("persist again");
        let mut bytes = fs::read(&reopened.pending_path).expect("read pending bytes");
        bytes[20] ^= 0x80;
        fs::write(&reopened.pending_path, bytes).expect("corrupt pending bytes");
        assert!(reopened.load_pending().is_err());
    }

    #[test]
    fn pending_journal_migrates_the_pre_capture_record_format() {
        let barrier = sample_pending();
        let encoded_v2 = encode_pending_file(&barrier).expect("encode current pending record");
        let payload = decode_checked(JOURNAL_PENDING_MAGIC, &encoded_v2)
            .expect("read current pending payload");
        let prefix_length = 16 + 16 + 16 + 8 + 56 + 16 + 8;
        let mut payload_v1 = payload[..prefix_length].to_vec();
        payload_v1.extend_from_slice(&payload[prefix_length + 2..]);
        let encoded_v1 = encode_checked(JOURNAL_PENDING_MAGIC_V1, &payload_v1);

        let decoded = decode_pending_file(&encoded_v1).expect("decode legacy pending record");
        assert_eq!(decoded, barrier);
        assert_eq!(decoded.capture_id, None);
        assert_eq!(decoded.capture_authorization_id, None);
    }

    #[test]
    fn flush_recovers_after_worker_loss_during_an_object_upload() {
        let environment = TestEnvironment::new();
        let mut worker = environment.open();
        worker
            .inner
            .write(4096, &[0xA3; 4096])
            .expect("stage ordinary write");
        environment
            .store
            .fail_next_upload
            .store(true, Ordering::SeqCst);
        assert!(matches!(worker.flush(), Err(BarrierError::ObjectStore(_))));
        let before_recovery = environment
            .control
            .get_branch(environment.branch_id)
            .expect("read unmodified branch");
        assert_eq!(before_recovery.revision, 1);
        assert!(worker.pending.is_some());
        drop(worker);

        let mut replacement_worker = environment.open();
        replacement_worker
            .flush()
            .expect("resume exact pending barrier");
        let branch = environment
            .control
            .get_branch(environment.branch_id)
            .expect("read committed branch");
        assert_eq!(branch.revision, 2);
        drop(replacement_worker);

        let restored = environment.open();
        assert_eq!(restored.inner.read(4096, 4096).unwrap(), vec![0xA3; 4096]);
        assert!(restored.pending.is_none());
    }

    #[test]
    fn flush_commits_as_an_identified_capture_child() {
        let environment = TestEnvironment::new();
        let capture_id = OperationId::from_bytes([19; 16]);
        environment.control.0.lock().unwrap().branch.capture =
            Some(versionfs_core::control::CaptureState {
                operation_id: capture_id,
                principal_id: versionfs_core::identity::PrincipalId::from_bytes([20; 16]),
                protection_id: ProtectionId::from_bytes([21; 16]),
                expected_revision: 1,
                sealed: false,
                pending_child: None,
                sealed_revision: None,
                sealed_roots: None,
            });

        let mut worker = environment.open();
        worker
            .inner
            .write(4096, &[0xD2; 4096])
            .expect("stage drain write");
        worker.flush().expect("commit capture drain barrier");

        let state = environment.control.0.lock().unwrap();
        assert_eq!(state.branch.revision, 2);
        assert_eq!(state.commit_count, 1);
        assert_eq!(state.capture_authorizations.len(), 1);
        let capture = state.branch.capture.as_ref().expect("capture remains open");
        assert_eq!(capture.operation_id, capture_id);
        assert_eq!(capture.pending_child, None);
        assert_eq!(
            state.branch.volume_root, worker.live_roots.volume,
            "the drain child root is the branch live root"
        );
    }

    #[test]
    fn flush_uses_an_already_authorized_capture_child_id() {
        let environment = TestEnvironment::new();
        let capture_id = OperationId::from_bytes([22; 16]);
        let child_id = OperationId::from_bytes([23; 16]);
        environment.control.0.lock().unwrap().branch.capture =
            Some(versionfs_core::control::CaptureState {
                operation_id: capture_id,
                principal_id: versionfs_core::identity::PrincipalId::from_bytes([24; 16]),
                protection_id: ProtectionId::from_bytes([25; 16]),
                expected_revision: 1,
                sealed: false,
                pending_child: Some(child_id),
                sealed_revision: None,
                sealed_roots: None,
            });

        let mut worker = environment.open();
        worker
            .inner
            .write(4096, &[0xE3; 4096])
            .expect("stage drain write");
        worker.flush().expect("commit preauthorized drain barrier");

        let state = environment.control.0.lock().unwrap();
        assert_eq!(state.branch.revision, 2);
        assert_eq!(state.commit_count, 1);
        assert!(state.capture_authorizations.is_empty());
        assert_eq!(
            state.operations.get(&child_id),
            Some(&OperationState::Committed(Vec::new()))
        );
        assert_eq!(
            state
                .branch
                .capture
                .as_ref()
                .expect("capture remains open")
                .pending_child,
            None
        );
    }

    #[test]
    fn empty_flush_retires_an_unused_capture_child_reservation() {
        let environment = TestEnvironment::new();
        let capture_id = OperationId::from_bytes([26; 16]);
        let child_id = OperationId::from_bytes([27; 16]);
        environment.control.0.lock().unwrap().branch.capture =
            Some(versionfs_core::control::CaptureState {
                operation_id: capture_id,
                principal_id: versionfs_core::identity::PrincipalId::from_bytes([28; 16]),
                protection_id: ProtectionId::from_bytes([29; 16]),
                expected_revision: 1,
                sealed: false,
                pending_child: Some(child_id),
                sealed_revision: None,
                sealed_roots: None,
            });

        let mut worker = environment.open();
        worker.flush().expect("complete empty drain flush");

        let state = environment.control.0.lock().unwrap();
        assert_eq!(
            state.operations.get(&child_id),
            Some(&OperationState::Aborted)
        );
        assert_eq!(
            state
                .branch
                .capture
                .as_ref()
                .expect("capture remains open")
                .pending_child,
            None
        );
    }

    #[test]
    fn clean_flush_cannot_abort_a_replacement_attachments_capture_child() {
        let environment = TestEnvironment::new();
        let mut stale_worker = environment.open();
        let child_id = OperationId::from_bytes([42; 16]);
        {
            let mut state = environment.control.0.lock().unwrap();
            state.branch.volume_epoch += 1;
            let volume_epoch = state.branch.volume_epoch;
            let attachment = state.branch.attachment.as_mut().unwrap();
            attachment.attachment_id = AttachmentId::from_bytes([43; 16]);
            attachment.volume_epoch = volume_epoch;
            state.branch.capture = Some(versionfs_core::control::CaptureState {
                operation_id: OperationId::from_bytes([40; 16]),
                principal_id: versionfs_core::identity::PrincipalId::from_bytes([41; 16]),
                protection_id: ProtectionId::from_bytes([44; 16]),
                expected_revision: 1,
                sealed: false,
                pending_child: Some(child_id),
                sealed_revision: None,
                sealed_roots: None,
            });
        }
        assert!(matches!(stale_worker.flush(), Err(BarrierError::Fenced)));
        let state = environment.control.0.lock().unwrap();
        assert_eq!(
            state.branch.capture.as_ref().unwrap().pending_child,
            Some(child_id)
        );
        assert!(!state.operations.contains_key(&child_id));
    }

    #[test]
    fn known_fencing_stops_writes_and_preserves_dirty_state() {
        for dirty in [false, true] {
            let environment = TestEnvironment::new();
            let mut worker = environment.open();
            if dirty {
                worker
                    .dispatch(BlockRequest::Write {
                        offset: 4096,
                        data: &[0xA8; 4096],
                        fua: false,
                    })
                    .unwrap();
            }
            let original = environment.control.0.lock().unwrap().branch.clone();
            environment.control.0.lock().unwrap().branch.attachment = None;
            assert!(matches!(worker.flush(), Err(BarrierError::Fenced)));
            assert!(matches!(
                worker.dispatch(BlockRequest::Write {
                    offset: 8192,
                    data: &[0xB9; 4096],
                    fua: false,
                }),
                Err(BlockError::Barrier(_))
            ));
            assert_eq!(worker.inner.read(8192, 4096).unwrap(), vec![0; 4096]);
            assert_eq!(
                worker.inner.read(4096, 4096).unwrap(),
                vec![if dirty { 0xA8 } else { 0 }; 4096]
            );
            // A fenced device cannot resume even if an adapter later presents
            // the prior identity; recovery requires a fresh attachment.
            environment.control.0.lock().unwrap().branch = original;
            assert!(matches!(worker.flush(), Err(BarrierError::Fenced)));
        }
    }

    #[test]
    fn committed_recovery_rejects_takeover_before_adopting_a_new_root() {
        let environment = TestEnvironment::new();
        let mut worker = environment.open();
        let original_roots = worker.live_roots;
        worker
            .dispatch(BlockRequest::Write {
                offset: 4096,
                data: &[0xCA; 4096],
                fua: false,
            })
            .unwrap();
        environment.control.0.lock().unwrap().lose_next_commit_reply = true;
        assert!(matches!(
            worker.flush(),
            Err(BarrierError::Control(ControlError::Metadata(
                MetadataError::UnknownOutcome { .. }
            )))
        ));
        let pending = worker.pending.as_ref().unwrap().clone();
        {
            let mut state = environment.control.0.lock().unwrap();
            let mut replacement = state.branch.clone();
            replacement.writer_epoch += 1;
            replacement.volume_epoch += 1;
            replacement.revision += 1;
            replacement.volume_root = original_roots.volume;
            replacement.writer = Some(WriterGrant {
                writer_epoch: replacement.writer_epoch,
                grant_id: OperationId::from_bytes([0xCB; 16]),
                ..environment.grant
            });
            let attachment = replacement.attachment.as_mut().unwrap();
            attachment.writer_epoch = replacement.writer_epoch;
            attachment.volume_epoch = replacement.volume_epoch;
            state.branch_after_committed_read = Some(replacement);
        }
        assert!(matches!(worker.flush(), Err(BarrierError::Fenced)));
        assert_eq!(worker.writer_grant, environment.grant);
        assert_eq!(worker.live_roots, original_roots);
        assert_eq!(worker.inner.read(4096, 4096).unwrap(), vec![0xCA; 4096]);
        assert_eq!(worker.pending.as_ref(), Some(&pending));
        assert_eq!(worker.journal.load_pending().unwrap(), Some(pending));
        assert!(matches!(
            worker.dispatch(BlockRequest::Write {
                offset: 8192,
                data: &[0xDB; 4096],
                fua: false,
            }),
            Err(BlockError::Barrier(_))
        ));
    }

    #[test]
    fn lost_capture_authorization_replies_recover_after_writer_heartbeat() {
        for failure in [
            AuthorizationFailure::AfterCommit,
            AuthorizationFailure::BeforeCommit,
        ] {
            let environment = TestEnvironment::new();
            let original_grant = WriterGrant {
                expires_at: MonotonicTime::from_nanos(u64::MAX - 1),
                ..environment.grant
            };
            {
                let mut state = environment.control.0.lock().unwrap();
                state.branch.writer = Some(original_grant);
                state.branch.capture = Some(versionfs_core::control::CaptureState {
                    operation_id: OperationId::from_bytes([30; 16]),
                    principal_id: versionfs_core::identity::PrincipalId::from_bytes([31; 16]),
                    protection_id: ProtectionId::from_bytes([32; 16]),
                    expected_revision: 1,
                    sealed: false,
                    pending_child: None,
                    sealed_revision: None,
                    sealed_roots: None,
                });
                state.fail_next_authorization = Some(failure);
            }
            let mut worker = environment.open();
            worker.inner.write(4096, &[0xA2; 4096]).unwrap();
            assert!(matches!(
                worker.flush(),
                Err(BarrierError::Control(ControlError::Metadata(
                    MetadataError::UnknownOutcome { .. }
                )))
            ));
            let pending = worker.pending.as_ref().unwrap();
            let original_child_id = pending.operation_id;
            let original_authorization_id = pending.capture_authorization_id.unwrap();
            drop(worker);

            let renewed_grant = WriterGrant {
                expires_at: MonotonicTime::from_nanos(u64::MAX),
                ..original_grant
            };
            environment.control.0.lock().unwrap().branch.writer = Some(renewed_grant);
            let replacement = environment.open();
            assert_eq!(replacement.inner.dirty_extent_count(), 0);
            assert_eq!(
                replacement.inner.read(4096, 4096).unwrap(),
                vec![0xA2; 4096]
            );

            let state = environment.control.0.lock().unwrap();
            assert_eq!(state.commit_count, 1);
            assert_eq!(state.branch.capture.as_ref().unwrap().pending_child, None);
            let [(authorization_id, (_, child_id, request_grant))] =
                state.capture_authorizations.iter().collect::<Vec<_>>()[..]
            else {
                panic!("one capture authorization must commit");
            };
            match failure {
                AuthorizationFailure::AfterCommit => {
                    assert_eq!(*authorization_id, original_authorization_id);
                    assert_eq!(*child_id, original_child_id);
                    assert_eq!(*request_grant, original_grant);
                }
                AuthorizationFailure::BeforeCommit => {
                    assert_ne!(*authorization_id, original_authorization_id);
                    assert_ne!(*child_id, original_child_id);
                    assert_eq!(*request_grant, renewed_grant);
                }
            }
        }
    }

    #[test]
    fn pending_barrier_from_an_old_volume_epoch_is_fenced_without_restoring_it() {
        let environment = TestEnvironment::new();
        let mut worker = environment.open();
        worker
            .inner
            .write(4096, &[0xA3; 4096])
            .expect("stage ordinary write");
        environment
            .store
            .fail_next_upload
            .store(true, Ordering::SeqCst);
        assert!(matches!(worker.flush(), Err(BarrierError::ObjectStore(_))));
        let pending = worker.pending.as_ref().unwrap().clone();
        drop(worker);

        {
            let mut state = environment.control.0.lock().unwrap();
            state.branch.volume_epoch += 1;
            state.branch.attachment.as_mut().unwrap().volume_epoch += 1;
        }

        let journal = FileBarrierJournal::open(&environment._directory.0)
            .expect("reopen journal after old worker exits");
        let result = DurableVolume::open(
            environment.store.clone(),
            environment.control.clone(),
            journal,
            environment.branch_id,
            environment.grant,
            environment.attachment_id,
            EXTENT_BYTES as usize,
        );
        assert!(matches!(result, Err(BarrierError::Fenced)));

        let mut journal = FileBarrierJournal::open(&environment._directory.0)
            .expect("reopen journal after rejected stale attach");
        assert_eq!(
            journal.load_pending().unwrap(),
            Some(pending),
            "stale attachment recovery must not consume or restore its pending state"
        );
    }

    #[test]
    fn expired_preparation_is_aborted_and_rebased_before_recovery() {
        let environment = TestEnvironment::new();
        let mut worker = environment.open();
        worker
            .inner
            .write(4096, &[0xD4; 4096])
            .expect("stage ordinary write");
        environment
            .store
            .fail_next_upload
            .store(true, Ordering::SeqCst);
        assert!(matches!(worker.flush(), Err(BarrierError::ObjectStore(_))));
        let expired_operation_id = worker.pending.as_ref().unwrap().operation_id;
        environment.control.0.lock().unwrap().expire_next_renewal = true;
        drop(worker);

        let replacement_worker = environment.open();
        assert!(replacement_worker.pending.is_none());
        assert_eq!(
            replacement_worker.inner.read(4096, 4096).unwrap(),
            vec![0xD4; 4096]
        );
        let state = environment.control.0.lock().unwrap();
        assert_eq!(
            state.operations.get(&expired_operation_id),
            Some(&OperationState::Aborted)
        );
        assert_eq!(state.branch.revision, 2);
    }

    #[test]
    fn pending_barrier_rebases_after_a_branch_revision_conflict() {
        let environment = TestEnvironment::new();
        let mut worker = environment.open();
        worker
            .inner
            .write(4096, &[0xB7; 4096])
            .expect("stage ordinary write");
        environment
            .store
            .fail_next_upload
            .store(true, Ordering::SeqCst);
        assert!(matches!(worker.flush(), Err(BarrierError::ObjectStore(_))));
        let stale_operation_id = worker.pending.as_ref().unwrap().operation_id;
        {
            let mut state = environment.control.0.lock().unwrap();
            state.branch.revision += 1;
        }
        drop(worker);

        let replacement_worker = environment.open();
        assert!(replacement_worker.pending.is_none());
        assert_eq!(
            replacement_worker.inner.read(4096, 4096).unwrap(),
            vec![0xB7; 4096]
        );
        let state = environment.control.0.lock().unwrap();
        assert_eq!(
            state.operations.get(&stale_operation_id),
            Some(&OperationState::Aborted)
        );
        assert_eq!(state.branch.revision, 3);
    }

    #[test]
    fn pending_replacement_retries_the_exact_record_after_local_io_failure() {
        for point in [
            FailurePoint::JournalAfterRenameBeforeDirectorySync,
            FailurePoint::JournalBeforePublish,
        ] {
            let environment = TestEnvironment::new();
            let failures = Arc::new(OneShotFailures::default());
            let journal = FailingReplacementJournal {
                inner: FileBarrierJournal::open_with(&environment._directory.0, failures.clone())
                    .unwrap(),
                failures,
                fail_next_replace: Some(point),
                attempts: Vec::new(),
            };
            let mut worker = DurableVolume::open(
                environment.store.clone(),
                environment.control.clone(),
                journal,
                environment.branch_id,
                environment.grant,
                environment.attachment_id,
                EXTENT_BYTES as usize,
            )
            .unwrap();
            worker.inner.write(4096, &[0xD8; 4096]).unwrap();
            environment
                .store
                .fail_next_upload
                .store(true, Ordering::SeqCst);
            assert!(matches!(worker.flush(), Err(BarrierError::ObjectStore(_))));
            let old_operation_id = worker.pending.as_ref().unwrap().operation_id;
            environment.control.0.lock().unwrap().branch.revision += 1;

            assert!(matches!(worker.flush(), Err(BarrierError::Journal(_))));
            assert_eq!(environment.control.0.lock().unwrap().commit_count, 0);
            assert!(matches!(
                worker.dispatch(BlockRequest::Write {
                    offset: 0,
                    data: &[0; 4096],
                    fua: false,
                }),
                Err(BlockError::BarrierUnresolved)
            ));
            worker
                .flush()
                .expect("retry the uncertain local replacement");

            let [first_attempt, retry] = worker.journal.attempts.as_slice() else {
                panic!("one replacement and one exact retry are required");
            };
            assert_eq!(first_attempt.0, old_operation_id);
            assert_ne!(first_attempt.1.operation_id, old_operation_id);
            assert_eq!(first_attempt, retry);
            assert_eq!(worker.inner.read(4096, 4096).unwrap(), vec![0xD8; 4096]);
            assert!(worker.pending.is_none());
            assert_eq!(environment.control.0.lock().unwrap().commit_count, 1);
        }
    }

    #[test]
    fn fua_persists_its_tagged_payload_and_prior_flush_boundary() {
        let environment = TestEnvironment::new();
        let mut worker = environment.open();
        worker
            .dispatch(BlockRequest::Write {
                offset: 4096,
                data: &[0x31; 4096],
                fua: false,
            })
            .expect("stage earlier write");
        worker.flush().expect("persist earlier writes");
        worker
            .dispatch(BlockRequest::Write {
                offset: 8192,
                data: &[0xE4; 4096],
                fua: true,
            })
            .expect("persist FUA-tagged payload");
        drop(worker);

        let restored = environment.open();
        assert_eq!(restored.inner.read(4096, 4096).unwrap(), vec![0x31; 4096]);
        assert_eq!(restored.inner.read(8192, 4096).unwrap(), vec![0xE4; 4096]);
        let state = environment.control.0.lock().unwrap();
        assert_eq!(state.commit_count, 2);
    }

    #[test]
    fn lost_commit_reply_blocks_writes_until_the_same_barrier_is_reconciled() {
        let environment = TestEnvironment::new();
        environment.control.0.lock().unwrap().lose_next_commit_reply = true;
        let mut worker = environment.open();
        worker
            .dispatch(BlockRequest::Write {
                offset: 4096,
                data: &[0xB6; 4096],
                fua: false,
            })
            .expect("stage write");
        assert!(matches!(
            worker.flush(),
            Err(BarrierError::Control(ControlError::Metadata(
                MetadataError::UnknownOutcome { .. }
            )))
        ));
        assert!(matches!(
            worker.dispatch(BlockRequest::Write {
                offset: 8192,
                data: &[0xC7; 4096],
                fua: false,
            }),
            Err(BlockError::BarrierUnresolved)
        ));
        drop(worker);

        let replacement_worker = environment.open();
        assert!(replacement_worker.pending.is_none());
        assert_eq!(
            replacement_worker.inner.read(4096, 4096).unwrap(),
            vec![0xB6; 4096]
        );
        assert_eq!(
            environment.control.0.lock().unwrap().commit_count,
            1,
            "recovery must not commit the same barrier twice"
        );
    }

    #[test]
    fn lost_commit_reply_reconciles_without_reuploading_dirty_state() {
        let environment = TestEnvironment::new();
        environment.control.0.lock().unwrap().lose_next_commit_reply = true;
        let mut worker = environment.open();
        worker
            .inner
            .write(4096, &[0xD2; 4096])
            .expect("stage write");
        assert!(matches!(
            worker.flush(),
            Err(BarrierError::Control(ControlError::Metadata(
                MetadataError::UnknownOutcome { .. }
            )))
        ));

        worker
            .flush()
            .expect("reconcile the committed barrier on the same worker");
        assert_eq!(worker.inner.dirty_extent_count(), 0);
        assert_eq!(
            environment.control.0.lock().unwrap().commit_count,
            1,
            "reconciliation must not publish a duplicate barrier"
        );
    }

    #[test]
    fn unresolved_renewal_replays_the_exact_grant_after_a_writer_heartbeat() {
        let environment = TestEnvironment::new();
        let expiring_grant = WriterGrant {
            expires_at: MonotonicTime::from_nanos(u64::MAX - 1),
            ..environment.grant
        };
        environment.control.0.lock().unwrap().branch.writer = Some(expiring_grant);
        let mut worker = environment.open();
        worker
            .inner
            .write(4096, &[0xE1; 4096])
            .expect("stage write");
        let staged = worker
            .inner
            .stage_dirty()
            .expect("stage volume map")
            .expect("one dirty extent");
        let ids = worker
            .journal
            .reserve_barrier_ids()
            .expect("reserve barrier IDs");
        let next_roots = LiveRoots {
            volume: staged.volume_root,
            logs: worker.live_roots.logs,
            artifacts: worker.live_roots.artifacts,
        };
        let mut pending = worker.pending_from_stage(ids, next_roots, staged, None, None);
        let renewal_id = worker
            .journal
            .reserve_operation_id()
            .expect("reserve renewal ID");
        pending.renewal_operation_id = Some(renewal_id);
        let request_grant = worker.writer_grant;
        pending.renewal_grant = Some(request_grant);
        worker
            .journal
            .persist_pending(&pending)
            .expect("persist unresolved renewal");
        {
            let mut state = environment.control.0.lock().unwrap();
            state.operations.insert(
                pending.operation_id,
                OperationState::Preparing {
                    operation_id: pending.operation_id,
                    expires_at: MonotonicTime::from_nanos(600_000_000_000),
                    branch_id: pending.branch_id,
                },
            );
            // Model a renewal that committed before its reply was lost.
            state.renewals.insert(
                renewal_id,
                (MonotonicTime::from_nanos(600_000_000_000), request_grant),
            );
        }
        drop(worker);

        let refreshed_grant = WriterGrant {
            expires_at: MonotonicTime::from_nanos(request_grant.expires_at.as_nanos() + 1),
            ..request_grant
        };
        environment.control.0.lock().unwrap().branch.writer = Some(refreshed_grant);

        let mut replacement = environment.open();
        replacement
            .flush()
            .expect("replay renewal with its exact original grant");
        let state = environment.control.0.lock().unwrap();
        assert!(same_grant_identity(
            state.renewals.get(&renewal_id).unwrap().1,
            request_grant
        ));
        assert_eq!(state.branch.revision, 2);
    }

    #[test]
    fn unresolved_uncommitted_renewal_retries_after_writer_heartbeat() {
        let environment = TestEnvironment::new();
        let expiring_grant = WriterGrant {
            expires_at: MonotonicTime::from_nanos(u64::MAX - 1),
            ..environment.grant
        };
        environment.control.0.lock().unwrap().branch.writer = Some(expiring_grant);
        let mut worker = environment.open();
        worker
            .inner
            .write(4096, &[0xE1; 4096])
            .expect("stage write");
        let staged = worker
            .inner
            .stage_dirty()
            .expect("stage volume map")
            .expect("one dirty extent");
        let ids = worker
            .journal
            .reserve_barrier_ids()
            .expect("reserve barrier IDs");
        let next_roots = LiveRoots {
            volume: staged.volume_root,
            logs: worker.live_roots.logs,
            artifacts: worker.live_roots.artifacts,
        };
        let mut pending = worker.pending_from_stage(ids, next_roots, staged, None, None);
        let expired_renewal_id = worker
            .journal
            .reserve_operation_id()
            .expect("reserve expired renewal ID");
        pending.renewal_operation_id = Some(expired_renewal_id);
        pending.renewal_grant = Some(worker.writer_grant);
        let refreshed_grant = WriterGrant {
            expires_at: MonotonicTime::from_nanos(worker.writer_grant.expires_at.as_nanos() + 1),
            ..worker.writer_grant
        };
        worker
            .journal
            .persist_pending(&pending)
            .expect("persist unresolved renewal");
        let mut control_state = environment.control.0.lock().unwrap();
        control_state.branch.writer = Some(refreshed_grant);
        control_state.operations.insert(
            pending.operation_id,
            OperationState::Preparing {
                operation_id: pending.operation_id,
                expires_at: MonotonicTime::from_nanos(600_000_000_000),
                branch_id: pending.branch_id,
            },
        );
        drop(control_state);
        drop(worker);

        let mut replacement = environment.open();
        replacement
            .flush()
            .expect("retry expired renewal with the refreshed writer grant");
        let state = environment.control.0.lock().unwrap();
        assert!(!state.renewals.contains_key(&expired_renewal_id));
        let [(replacement_id, (_, renewed_grant))] = state.renewals.iter().collect::<Vec<_>>()[..]
        else {
            panic!("one replacement renewal should commit");
        };
        assert_ne!(*replacement_id, expired_renewal_id);
        assert_eq!(*renewed_grant, state.branch.writer.unwrap());
        assert_eq!(state.branch.revision, 2);
    }
}
