//! Durable FLUSH/FUA coordination for one attached block volume.

use sha2::{Digest, Sha256};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use thiserror::Error;
use versionfs_core::control::{
    AttachmentState, BranchState, ControlError, GenerationSeed, LiveRoots, OperationState,
    PreparationKind, PreparationRequest, WorkspaceControl, WriterGrant,
};
use versionfs_core::format::{
    CaptureMode, GenerationDescriptor, ObjectRef, ObjectType, VolumeGeometry, VolumeRoot,
};
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
    /// Writer grant identity used to start the operation.
    pub writer_grant: WriterGrant,
    /// Exact writable attachment identity.
    pub attachment_id: AttachmentId,
    /// Volume fencing epoch for the attachment.
    pub volume_epoch: u64,
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

    /// Reserves a never-reused operation ID for one preparation renewal.
    fn reserve_operation_id(&mut self) -> Result<OperationId, String>;

    /// Loads the one pending barrier recorded for this writable attachment.
    fn load_pending(&mut self) -> Result<Option<PendingBarrier>, String>;

    /// Durably replaces the pending recovery record before remote I/O continues.
    fn persist_pending(&mut self, barrier: &PendingBarrier) -> Result<(), String>;

    /// Durably removes a terminal pending barrier record.
    fn finish_pending(&mut self, operation_id: OperationId) -> Result<(), String>;
}

const JOURNAL_IDS_MAGIC: &[u8; 8] = b"VFSBID01";
const JOURNAL_PENDING_MAGIC: &[u8; 8] = b"VFSPND01";
const CHECKSUM_BYTES: usize = 32;
static NEXT_TEMP_FILE: AtomicU64 = AtomicU64::new(0);

/// Crash-safe file journal for one writable block attachment.
///
/// The directory must be dedicated to this journal on a local filesystem
/// outside the exported volume. On Unix, this type restricts it to mode `0700`
/// and creates files with mode `0600`; other platforms must provide equivalent
/// ACL protection. An exclusive OS file lock enforces one process per journal
/// directory. It is safe to reopen the directory after that process exits; this
/// is not a cross-host coordination mechanism. Each ID reservation and
/// pending-record replacement is fsynced before this type returns success.
pub struct FileBarrierJournal {
    directory: PathBuf,
    ids_path: PathBuf,
    pending_path: PathBuf,
    _lock: File,
    nonce: [u8; 8],
    next_counter: u64,
}

impl FileBarrierJournal {
    /// Opens or initializes one journal directory.
    pub fn open(directory: impl AsRef<Path>) -> Result<Self, String> {
        let directory = directory.as_ref().to_path_buf();
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
                let mut ids_options = OpenOptions::new();
                ids_options.write(true).create_new(true);
                set_private_creation_mode(&mut ids_options, 0o600);
                match ids_options.open(&ids_path) {
                    Ok(mut file) => {
                        file.write_all(&bytes)
                            .map_err(journal_io("initialize journal IDs"))?;
                        file.sync_all()
                            .map_err(journal_io("sync initialized journal IDs"))?;
                        sync_directory(&directory)?;
                        (nonce, 0)
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => decode_ids(
                        &fs::read(&ids_path)
                            .map_err(journal_io("read concurrently initialized journal IDs"))?,
                    )?,
                    Err(error) => return Err(format!("create journal ID state: {error}")),
                }
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
        atomic_replace(&self.directory, &self.pending_path, &bytes)
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
    let payload = decode_checked(JOURNAL_PENDING_MAGIC, bytes)?;
    let mut reader = JournalReader::new(payload);
    let operation_id = OperationId::from_bytes(reader.fixed()?);
    let protection_id = ProtectionId::from_bytes(reader.fixed()?);
    let branch_id = BranchId::from_bytes(reader.fixed()?);
    let expected_revision = reader.u64()?;
    let writer_grant = decode_grant(&mut reader)?;
    let attachment_id = AttachmentId::from_bytes(reader.fixed()?);
    let volume_epoch = reader.u64()?;
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

fn atomic_replace(directory: &Path, destination: &Path, bytes: &[u8]) -> Result<(), String> {
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
    if let Err(error) = write_result {
        let _ = fs::remove_file(&temporary);
        return Err(error);
    }
    if let Err(error) = fs::rename(&temporary, destination) {
        let _ = fs::remove_file(&temporary);
        return Err(format!("publish barrier journal file: {error}"));
    }
    sync_directory(directory)
}

#[cfg(unix)]
fn restrict_journal_directory(directory: &Path) -> Result<(), String> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(directory, fs::Permissions::from_mode(0o700))
        .map_err(journal_io("restrict barrier journal directory permissions"))
}

#[cfg(not(unix))]
fn restrict_journal_directory(_directory: &Path) -> Result<(), String> {
    Ok(())
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
    ) -> Result<BranchState, ControlError>;

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
    ) -> Result<BranchState, ControlError> {
        WorkspaceControl::commit_live_roots(
            self,
            operation_id,
            expected_revision,
            grant,
            roots,
            None,
        )
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

    Ok(seed)
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
    pending_journaled: bool,
    last_renewal: Option<Instant>,
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
        let current_grant = branch.writer.ok_or(BarrierError::Fenced)?;
        let attachment = branch.attachment.ok_or(BarrierError::Fenced)?;
        if !same_grant_identity(current_grant, writer_grant)
            || attachment.attachment_id != attachment_id
            || attachment.writer_epoch != current_grant.writer_epoch
            || attachment.volume_epoch != branch.volume_epoch
        {
            return Err(BarrierError::Fenced);
        }
        let base = load_map(&store, branch.volume_root)?;
        let mut inner = DirtyVolume::new(store, base, max_dirty_bytes)?;
        let mut pending = journal.load_pending().map_err(BarrierError::Journal)?;
        let mut aborted_dirty_root = None;
        if let Some(barrier) = &pending {
            if barrier.branch_id != branch_id
                || barrier.attachment_id != attachment_id
                || !same_grant_identity(barrier.writer_grant, current_grant)
            {
                return Err(BarrierError::Fenced);
            }
            match control.get_operation(barrier.operation_id) {
                Ok(OperationState::Committed(_)) => {
                    let latest = control.get_branch(branch_id)?;
                    inner.replace_base(load_map_from_dirty(&inner, latest.volume_root)?);
                    journal
                        .finish_pending(barrier.operation_id)
                        .map_err(BarrierError::Journal)?;
                    pending = None;
                }
                Ok(OperationState::Aborted) => {
                    if branch.volume_root != barrier.previous_roots.volume {
                        return Err(BarrierError::Conflict);
                    }
                    inner.restore_pending_dirty(
                        barrier.dirty_revision,
                        &barrier.dirty_extent_refs,
                        &barrier.objects,
                    )?;
                    aborted_dirty_root = Some(barrier.previous_roots.volume);
                    journal
                        .finish_pending(barrier.operation_id)
                        .map_err(BarrierError::Journal)?;
                    pending = None;
                }
                Ok(OperationState::Preparing { .. }) | Err(ControlError::NotFound) => {
                    if branch.revision != barrier.expected_revision
                        || roots(&branch) != barrier.previous_roots
                    {
                        return Err(BarrierError::Conflict);
                    }
                    inner.load_staged_map(barrier.next_roots.volume, &barrier.objects)?;
                    inner.restore_pending_dirty(
                        barrier.dirty_revision,
                        &barrier.dirty_extent_refs,
                        &barrier.objects,
                    )?;
                }
                Err(error) => return Err(error.into()),
            }
        }
        let branch = control.get_branch(branch_id)?;
        let pending_journaled = pending.is_some();
        let current_grant = validate_attachment(
            &branch,
            branch_id,
            attachment_id,
            branch.volume_epoch,
            writer_grant,
        )?;
        if let Some(barrier) = &pending
            && (branch.revision != barrier.expected_revision
                || roots(&branch) != barrier.previous_roots)
        {
            return Err(BarrierError::Conflict);
        }
        if aborted_dirty_root.is_some_and(|root| branch.volume_root != root) {
            return Err(BarrierError::Conflict);
        }
        Ok(Self {
            inner,
            control,
            journal,
            branch_id,
            writer_grant: current_grant,
            attachment_id,
            volume_epoch: branch.attachment.ok_or(BarrierError::Fenced)?.volume_epoch,
            live_roots: roots(&branch),
            revision: branch.revision,
            pending,
            pending_journaled,
            last_renewal: None,
        })
    }

    /// Persists all currently dirty writes and advances the authoritative live root.
    pub fn flush(&mut self) -> Result<(), BarrierError> {
        if self.pending.is_none() {
            if self.inner.dirty_extent_count() == 0 {
                return Ok(());
            }
            let staged = self.inner.stage_dirty()?.ok_or(BarrierError::Conflict)?;
            let ids = self
                .journal
                .reserve_barrier_ids()
                .map_err(BarrierError::Journal)?;
            let next_roots = LiveRoots {
                volume: staged.volume_root,
                logs: self.live_roots.logs,
                artifacts: self.live_roots.artifacts,
            };
            let barrier = pending_from_stage(
                self.branch_id,
                self.revision,
                self.writer_grant,
                self.attachment_id,
                self.volume_epoch,
                self.live_roots,
                next_roots,
                ids,
                staged,
            );
            self.pending = Some(barrier);
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

    fn resume_pending(&mut self) -> Result<(), BarrierError> {
        self.ensure_pending_journaled()?;
        let barrier = self.pending.as_ref().ok_or(BarrierError::Conflict)?;
        let operation_id = barrier.operation_id;
        let branch_id = barrier.branch_id;
        let expected_revision = barrier.expected_revision;
        let attachment_id = barrier.attachment_id;
        let volume_epoch = barrier.volume_epoch;
        let previous_roots = barrier.previous_roots;
        let next_roots = barrier.next_roots;
        let saved_grant = barrier.writer_grant;
        let branch = self.control.get_branch(self.branch_id)?;
        let current_grant =
            validate_attachment(&branch, branch_id, attachment_id, volume_epoch, saved_grant)?;

        let operation_state = self.control.get_operation(operation_id);
        let was_preparing = matches!(&operation_state, Ok(OperationState::Preparing { .. }));
        let was_not_found = matches!(&operation_state, Err(ControlError::NotFound));
        match operation_state {
            Ok(OperationState::Committed(_)) => return self.finish_committed(),
            Ok(OperationState::Aborted) => {
                if branch.volume_root != previous_roots.volume {
                    return Err(BarrierError::Conflict);
                }
                self.journal
                    .finish_pending(operation_id)
                    .map_err(BarrierError::Journal)?;
                self.pending = None;
                self.pending_journaled = false;
                return Err(BarrierError::Aborted);
            }
            Ok(OperationState::Preparing { .. }) => {}
            Err(ControlError::NotFound) => {}
            Err(error) => return Err(error.into()),
        }

        if was_preparing || was_not_found {
            if branch.revision != expected_revision || roots(&branch) != previous_roots {
                return Err(BarrierError::Conflict);
            }
            let pending = self.pending.as_ref().ok_or(BarrierError::Conflict)?;
            self.inner
                .load_staged_map(next_roots.volume, &pending.objects)?;
        }

        if was_not_found {
            let state = self.control.begin_preparation(PreparationRequest {
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
            })?;
            if matches!(state, OperationState::Committed(_)) {
                return self.finish_committed();
            }
            self.last_renewal = Some(Instant::now());
        }

        if self.last_renewal.is_none() {
            self.renew_preparation(current_grant)?;
        }
        let upload_count = self
            .pending
            .as_ref()
            .map_or(0, |pending| pending.objects.len());
        for index in 0..upload_count {
            self.maybe_renew()?;
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

        let latest = self.control.get_branch(self.branch_id)?;
        let current_grant = validate_attachment(
            &latest,
            branch_id,
            attachment_id,
            volume_epoch,
            current_grant,
        )?;
        if latest.revision != expected_revision || roots(&latest) != previous_roots {
            return Err(BarrierError::Conflict);
        }
        let committed = self.control.commit_live_roots(
            operation_id,
            expected_revision,
            current_grant,
            next_roots,
        )?;
        let map = self.inner.load_map(committed.volume_root)?;
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
        self.pending_journaled = false;
        self.last_renewal = None;
        Ok(())
    }

    fn ensure_pending_journaled(&mut self) -> Result<(), BarrierError> {
        if self.pending_journaled {
            return Ok(());
        }
        let pending = self.pending.as_ref().ok_or(BarrierError::Conflict)?;
        self.journal
            .persist_pending(pending)
            .map_err(BarrierError::Journal)?;
        self.pending_journaled = true;
        Ok(())
    }

    fn maybe_renew(&mut self) -> Result<(), BarrierError> {
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
            let grant = validate_attachment(
                &branch,
                pending.branch_id,
                pending.attachment_id,
                pending.volume_epoch,
                pending.writer_grant,
            )?;
            self.renew_preparation(grant)?;
        }
        Ok(())
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
            Err(ControlError::Expired)
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
        let map = self.inner.load_map(branch.volume_root)?;
        if self.inner.dirty_extent_count() > 0 {
            self.inner.commit_durable_map(map, dirty_revision)?;
        } else {
            self.inner.replace_base(map);
        }
        self.live_roots = roots(&branch);
        self.revision = branch.revision;
        if let Some(grant) = branch.writer {
            self.writer_grant = grant;
        }
        self.journal
            .finish_pending(operation_id)
            .map_err(BarrierError::Journal)?;
        self.pending = None;
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

fn pending_from_stage(
    branch_id: BranchId,
    expected_revision: u64,
    writer_grant: WriterGrant,
    attachment_id: AttachmentId,
    volume_epoch: u64,
    previous_roots: LiveRoots,
    next_roots: LiveRoots,
    ids: BarrierIds,
    staged: StagedVolume,
) -> PendingBarrier {
    PendingBarrier {
        operation_id: ids.operation_id,
        protection_id: ids.protection_id,
        branch_id,
        expected_revision,
        writer_grant,
        attachment_id,
        volume_epoch,
        previous_roots,
        next_roots,
        dirty_revision: staged.dirty_revision,
        dirty_extent_refs: staged.dirty_extent_refs,
        objects: staged.objects,
        renewal_operation_id: None,
        renewal_grant: None,
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

fn load_map_from_dirty<S: DurableObjectStore>(
    inner: &DirtyVolume<S>,
    volume_root: ObjectRef,
) -> Result<VolumeExtentMap, BarrierError> {
    inner.load_map(volume_root).map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex, atomic::AtomicBool};
    use versionfs_core::control::{OwnerIncarnation, WriterGrant};
    use versionfs_core::format::{EXTENT_BYTES, ObjectType};
    use versionfs_core::identity::{ObjectId, ObjectStoreScopeId, WorkspaceId};
    use versionfs_core::metadata::MetadataError;
    use versionfs_core::time::MonotonicTime;

    static NEXT_TEST_DIR: AtomicU64 = AtomicU64::new(0);

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

    #[derive(Clone, Default)]
    struct MemoryStore {
        objects: Arc<Mutex<HashMap<ObjectId, Vec<u8>>>>,
        fail_next_upload: Arc<AtomicBool>,
    }

    impl MapObjectStore for MemoryStore {
        fn proof_scope(&self) -> ObjectStoreScopeId {
            ObjectStoreScopeId::from_bytes([0xA7; 16])
        }

        fn get(&self, id: ObjectId) -> Result<Option<Vec<u8>>, String> {
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

    struct FakeControlState {
        branch: BranchState,
        operations: HashMap<OperationId, OperationState>,
        renewals: HashMap<OperationId, (MonotonicTime, WriterGrant)>,
        lose_next_commit_reply: bool,
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

        fn renew_preparation(
            &self,
            renewal_id: OperationId,
            prepared_id: OperationId,
            grant: WriterGrant,
        ) -> Result<MonotonicTime, ControlError> {
            let mut state = self.0.lock().map_err(|_| ControlError::InvalidTransition)?;
            if let Some((deadline, original_grant)) = state.renewals.get(&renewal_id) {
                if !same_grant_identity(*original_grant, grant) {
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
            if state
                .branch
                .writer
                .is_some_and(|current| grant.expires_at < current.expires_at)
            {
                return Err(ControlError::Expired);
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
            self.0
                .lock()
                .map_err(|_| ControlError::InvalidTransition)?
                .operations
                .get(&operation_id)
                .cloned()
                .ok_or(ControlError::NotFound)
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
                VolumeGeometry::new(u64::from(EXTENT_BYTES)).expect("valid test geometry"),
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
                renewals: HashMap::new(),
                lose_next_commit_reply: false,
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
                EXTENT_BYTES as usize,
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
            VolumeGeometry::new(u64::from(EXTENT_BYTES)).unwrap()
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
        let mut pending = pending_from_stage(
            worker.branch_id,
            worker.revision,
            worker.writer_grant,
            worker.attachment_id,
            worker.volume_epoch,
            worker.live_roots,
            LiveRoots {
                volume: staged.volume_root,
                logs: worker.live_roots.logs,
                artifacts: worker.live_roots.artifacts,
            },
            ids,
            staged,
        );
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
    fn expired_uncommitted_renewal_restarts_with_a_fresh_id_and_grant() {
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
        let mut pending = pending_from_stage(
            worker.branch_id,
            worker.revision,
            worker.writer_grant,
            worker.attachment_id,
            worker.volume_epoch,
            worker.live_roots,
            LiveRoots {
                volume: staged.volume_root,
                logs: worker.live_roots.logs,
                artifacts: worker.live_roots.artifacts,
            },
            ids,
            staged,
        );
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
