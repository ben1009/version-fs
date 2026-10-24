//! Fixed-geometry block request handling and a bounded worker-local dirty overlay.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use thiserror::Error;
use versionfs_core::format::{
    EXTENT_BYTES, FormatError, ObjectRef, ObjectType, VolumeRoot, encode_object,
};
use versionfs_core::identity::ObjectId;
use versionfs_core::persistent_map::{MapObjectStore, PersistentBTree};
use versionfs_core::views::{ViewError, VolumeExtentMap};

const EXTENT_SIZE: usize = EXTENT_BYTES as usize;

/// Errors returned by the local block-volume data path.
#[derive(Debug, Error)]
pub enum BlockError {
    /// A block request failed geometry or canonical-format validation.
    #[error(transparent)]
    Format(#[from] FormatError),
    /// A persistent-root read failed.
    #[error(transparent)]
    View(#[from] ViewError),
    /// The dirty-overlay capacity is zero or not an extent multiple.
    #[error("dirty-overlay capacity must be a positive extent multiple")]
    InvalidDirtyCapacity,
    /// A write would exceed the bounded dirty-overlay capacity.
    #[error("dirty overlay is full")]
    DirtyCapacityExceeded,
    /// This block device does not implement durable barriers.
    #[error("durable block barriers are unavailable")]
    DurableBarrierUnavailable,
    /// A durable barrier is unresolved; dependent writes cannot pass it.
    #[error("a durable block barrier is unresolved")]
    BarrierUnresolved,
    /// A durable barrier failed before it could be acknowledged.
    #[error("durable block barrier failed: {0}")]
    Barrier(String),
    /// The block operation is not supported by the fixed v0 device.
    #[error("unsupported block operation {0}")]
    UnsupportedOperation(u32),
}

/// A checked block request independent of the Linux ublk transport.
pub enum BlockRequest<'a> {
    /// Reads an aligned byte range.
    Read {
        /// First byte to read.
        offset: u64,
        /// Number of bytes to read.
        length: u64,
    },
    /// Writes an aligned byte range, optionally requiring FUA durability.
    Write {
        /// First byte to write.
        offset: u64,
        /// Bytes supplied by the block request.
        data: &'a [u8],
        /// Whether the request requires its payload to be durable before completion.
        fua: bool,
    },
    /// Requests a durable ordering barrier.
    Flush,
    /// An unsupported Linux block operation code.
    Unsupported(u32),
}

/// A successful block request result.
#[derive(Debug, Eq, PartialEq)]
pub enum BlockResponse {
    /// Data returned by a read request.
    Read(Vec<u8>),
    /// Number of bytes accepted into the local volatile overlay.
    Written(usize),
}

/// Synchronous block-device interface consumed by the Linux ublk transport.
pub trait BlockDevice {
    /// Returns the fixed logical geometry exposed to the kernel.
    fn geometry(&self) -> versionfs_core::format::VolumeGeometry;

    /// Whether the device implements durable FUA requests.
    fn supports_fua(&self) -> bool;

    /// Dispatches one request in the device's single ordered mutation stream.
    fn dispatch(&mut self, request: BlockRequest<'_>) -> Result<BlockResponse, BlockError>;
}

/// A versioned volume with a worker-local, bounded dirty-extent overlay.
///
/// This type models ordinary volatile block I/O. It does not acknowledge FLUSH
/// or FUA and does not publish roots. [`crate::barrier::DurableVolume`] adds the
/// remote durability protocol.
pub struct DirtyVolume<S> {
    store: S,
    base: VolumeExtentMap,
    dirty: BTreeMap<u64, Arc<[u8; EXTENT_SIZE]>>,
    max_dirty_extents: usize,
    revision: u64,
}

impl<S: MapObjectStore> DirtyVolume<S> {
    /// Opens a volume over a protected immutable base and bounds its dirty memory.
    pub fn new(
        store: S,
        base: VolumeExtentMap,
        max_dirty_bytes: usize,
    ) -> Result<Self, BlockError> {
        if max_dirty_bytes == 0 || !max_dirty_bytes.is_multiple_of(EXTENT_SIZE) {
            return Err(BlockError::InvalidDirtyCapacity);
        }
        Ok(Self {
            store,
            base,
            dirty: BTreeMap::new(),
            max_dirty_extents: max_dirty_bytes / EXTENT_SIZE,
            revision: 0,
        })
    }

    /// Returns this volume's fixed logical geometry.
    pub const fn geometry(&self) -> versionfs_core::format::VolumeGeometry {
        self.base.geometry()
    }

    /// Returns the number of dirty extents currently held in memory.
    pub fn dirty_extent_count(&self) -> usize {
        self.dirty.len()
    }

    /// Returns bytes currently held in the dirty overlay.
    pub fn dirty_bytes(&self) -> usize {
        self.dirty.len() * EXTENT_SIZE
    }

    pub(crate) fn store_mut(&mut self) -> &mut S {
        &mut self.store
    }

    /// Reads a checked byte range, resolving dirty extents before the base root.
    pub fn read(&self, offset: u64, length: u64) -> Result<Vec<u8>, BlockError> {
        self.base.geometry().validate_range(offset, length)?;
        let output_len = usize::try_from(length).map_err(|_| {
            versionfs_core::format::FormatError::InvalidGeometry("range does not fit memory")
        })?;
        let mut output = Vec::new();
        output.try_reserve_exact(output_len).map_err(|_| {
            versionfs_core::format::FormatError::InvalidGeometry("range does not fit memory")
        })?;
        output.resize(output_len, 0);
        let extent_bytes = u64::from(EXTENT_BYTES);
        let mut position = offset;
        let mut copied = 0_usize;

        while copied < output.len() {
            let extent_number = position / extent_bytes;
            let in_extent = (position % extent_bytes) as usize;
            let chunk_len = (output.len() - copied).min(EXTENT_SIZE - in_extent);
            if let Some(dirty) = self.dirty.get(&extent_number) {
                output[copied..copied + chunk_len]
                    .copy_from_slice(&dirty[in_extent..in_extent + chunk_len]);
            } else {
                let bytes = self.base.read_range(
                    &self.store,
                    position,
                    u64::try_from(chunk_len).map_err(|_| {
                        versionfs_core::format::FormatError::InvalidGeometry(
                            "range does not fit memory",
                        )
                    })?,
                )?;
                output[copied..copied + chunk_len].copy_from_slice(&bytes);
            }
            position = position.checked_add(chunk_len as u64).ok_or(
                versionfs_core::format::FormatError::InvalidGeometry("range overflow"),
            )?;
            copied += chunk_len;
        }
        Ok(output)
    }

    /// Writes a complete aligned request atomically into the bounded overlay.
    ///
    /// If validation, capacity checking, or a partial-extent base read fails,
    /// none of the request's bytes are installed.
    pub fn write(&mut self, offset: u64, data: &[u8]) -> Result<(), BlockError> {
        let length = u64::try_from(data.len()).map_err(|_| {
            versionfs_core::format::FormatError::InvalidGeometry("write length overflow")
        })?;
        self.base.geometry().validate_range(offset, length)?;
        if data.is_empty() {
            return Ok(());
        }

        let extent_bytes = u64::from(EXTENT_BYTES);
        let end = offset.checked_add(length).ok_or(
            versionfs_core::format::FormatError::InvalidGeometry("range overflow"),
        )?;
        let first_extent = offset / extent_bytes;
        let last_extent = (end - 1) / extent_bytes;
        let required_new = (first_extent..=last_extent)
            .filter(|extent| !self.dirty.contains_key(extent))
            .count();
        if self
            .dirty
            .len()
            .checked_add(required_new)
            .is_none_or(|count| count > self.max_dirty_extents)
        {
            return Err(BlockError::DirtyCapacityExceeded);
        }
        let next_revision = self.revision.checked_add(1).ok_or(
            versionfs_core::format::FormatError::InvalidGeometry("dirty revision overflow"),
        )?;

        let mut updates = BTreeMap::new();
        let mut position = offset;
        let mut consumed = 0_usize;
        while position < end {
            let extent_number = position / extent_bytes;
            let extent_start = extent_number * extent_bytes;
            let in_extent = (position - extent_start) as usize;
            let chunk_len = (EXTENT_SIZE - in_extent).min(data.len() - consumed);
            let full_extent_write = in_extent == 0 && chunk_len == EXTENT_SIZE;
            let mut next = if let Some(dirty) = self.dirty.get(&extent_number) {
                **dirty
            } else if full_extent_write {
                [0; EXTENT_SIZE]
            } else {
                self.base
                    .read_range(&self.store, extent_start, extent_bytes)?
                    .try_into()
                    .map_err(|_| {
                        versionfs_core::format::FormatError::InvalidGeometry(
                            "extent payload has the wrong size",
                        )
                    })?
            };
            next[in_extent..in_extent + chunk_len]
                .copy_from_slice(&data[consumed..consumed + chunk_len]);
            updates.insert(extent_number, Arc::new(next));
            position = position.checked_add(chunk_len as u64).ok_or(
                versionfs_core::format::FormatError::InvalidGeometry("range overflow"),
            )?;
            consumed += chunk_len;
        }

        self.dirty.extend(updates);
        self.revision = next_revision;
        Ok(())
    }

    /// Returns an immutable borrow of one dirty version for bounded upload work.
    ///
    /// The borrow prevents writes to the overlay until the caller finishes using
    /// the sealed bytes, so a hashed/uploaded version cannot change underneath it.
    pub fn seal_dirty(&self) -> SealedDirty<'_> {
        SealedDirty {
            revision: self.revision,
            extents: &self.dirty,
        }
    }

    pub(crate) fn load_map(&self, volume_root: ObjectRef) -> Result<VolumeExtentMap, BlockError> {
        load_map_from_store(&self.store, volume_root)
    }

    /// Validates a candidate map against base objects plus its sealed objects.
    pub(crate) fn load_staged_map(
        &self,
        volume_root: ObjectRef,
        objects: &[StagedObject],
    ) -> Result<VolumeExtentMap, BlockError> {
        let mut staged = StagingMapStore::new(&self.store);
        for object in objects {
            staged
                .put(object.reference.object_id, object.encoded.clone())
                .map_err(ViewError::Store)?;
        }
        load_map_from_store(&staged, volume_root)
    }

    pub(crate) fn replace_base(&mut self, base: VolumeExtentMap) {
        self.base = base;
        self.dirty.clear();
    }

    /// Replaces the authoritative base while preserving the local dirty overlay.
    pub(crate) fn rebase_base(&mut self, base: VolumeExtentMap) {
        self.base = base;
    }

    /// Dispatches supported ordinary I/O and rejects unsupported barriers/ops.
    pub fn dispatch(&mut self, request: BlockRequest<'_>) -> Result<BlockResponse, BlockError> {
        match request {
            BlockRequest::Read { offset, length } => {
                self.read(offset, length).map(BlockResponse::Read)
            }
            BlockRequest::Write { offset, data, fua } => {
                if fua {
                    return Err(BlockError::DurableBarrierUnavailable);
                }
                self.write(offset, data)?;
                Ok(BlockResponse::Written(data.len()))
            }
            BlockRequest::Flush => Err(BlockError::DurableBarrierUnavailable),
            BlockRequest::Unsupported(operation) => {
                Err(BlockError::UnsupportedOperation(operation))
            }
        }
    }

    pub(crate) fn stage_dirty(&self) -> Result<Option<StagedVolume>, BlockError> {
        if self.dirty.is_empty() {
            return Ok(None);
        }

        let mut staging = StagingMapStore::new(&self.store);
        let mut map = self.base.validate(&staging)?;
        let mut dirty_extent_refs = Vec::with_capacity(self.dirty.len());
        for (number, bytes) in &self.dirty {
            let encoded = encode_object(ObjectType::Extent, bytes.as_slice())?;
            let reference = ObjectRef::from_encoded(&encoded)?;
            staging
                .put(reference.object_id, encoded)
                .map_err(ViewError::Store)?;
            map = map.insert_extent(&mut staging, *number, reference)?;
            dirty_extent_refs.push((*number, reference));
        }

        let volume_root = map.seal(&mut staging)?;
        let encoded_root = staging
            .get_ref(volume_root)
            .map_err(ViewError::Store)?
            .ok_or(ViewError::MissingObject(volume_root.object_id))?;
        let root = VolumeRoot::decode(&encoded_root)?;
        let tree = PersistentBTree::from_root(root.extent_map_root).map_err(ViewError::from)?;
        let mut reachable = vec![volume_root];
        reachable.extend(dirty_extent_refs.iter().map(|(_, reference)| *reference));
        let mut pending_nodes = vec![root.extent_map_root];
        let mut visited_nodes = BTreeSet::new();
        while let Some(reference) = pending_nodes.pop() {
            if !staging.contains_staged(reference.object_id)
                || !visited_nodes.insert(reference.object_id)
            {
                continue;
            }
            reachable.push(reference);
            pending_nodes.extend(
                tree.child_refs(&staging, reference)
                    .map_err(ViewError::from)?,
            );
        }
        let objects = staging.into_objects_referenced(reachable)?;
        Ok(Some(StagedVolume {
            volume_root,
            dirty_revision: self.revision,
            dirty_extent_refs,
            objects,
        }))
    }

    pub(crate) fn commit_durable_map(
        &mut self,
        base: VolumeExtentMap,
        expected_revision: u64,
    ) -> Result<(), BlockError> {
        if self.revision != expected_revision {
            return Err(BlockError::BarrierUnresolved);
        }
        self.replace_base(base);
        Ok(())
    }

    pub(crate) fn restore_pending_dirty(
        &mut self,
        dirty_revision: u64,
        dirty_extent_refs: &[(u64, ObjectRef)],
        objects: &[StagedObject],
    ) -> Result<(), BlockError> {
        let object_bytes: BTreeMap<_, _> = objects
            .iter()
            .map(|object| (object.reference.object_id, object.encoded.as_slice()))
            .collect();
        if dirty_extent_refs.len() > self.max_dirty_extents {
            return Err(BlockError::DirtyCapacityExceeded);
        }
        let mut dirty = BTreeMap::new();
        for (number, reference) in dirty_extent_refs {
            if reference.object_type != ObjectType::Extent {
                return Err(FormatError::ObjectReferenceMismatch.into());
            }
            let encoded = object_bytes
                .get(&reference.object_id)
                .ok_or(FormatError::ObjectReferenceMismatch)?;
            let object = reference.verify(encoded)?;
            if object.payload.len() != EXTENT_SIZE {
                return Err(FormatError::InvalidGeometry(
                    "allocated extent payload must be exactly 64 KiB",
                )
                .into());
            }
            let bytes: [u8; EXTENT_SIZE] = object
                .payload
                .try_into()
                .map_err(|_| FormatError::ObjectReferenceMismatch)?;
            dirty.insert(*number, Arc::new(bytes));
        }
        self.dirty = dirty;
        self.revision = dirty_revision;
        Ok(())
    }
}

fn load_map_from_store<S: MapObjectStore>(
    store: &S,
    volume_root: ObjectRef,
) -> Result<VolumeExtentMap, BlockError> {
    let encoded = store
        .get_ref(volume_root)
        .map_err(ViewError::Store)?
        .ok_or(ViewError::MissingObject(volume_root.object_id))?;
    let root = volume_root.verify(&encoded)?;
    if root.object_type != ObjectType::VolumeRoot {
        return Err(FormatError::ObjectReferenceMismatch.into());
    }
    let decoded = versionfs_core::format::VolumeRoot::decode(&encoded)?;
    VolumeExtentMap::from_root(decoded)?
        .validate(store)
        .map_err(Into::into)
}

impl<S: MapObjectStore> BlockDevice for DirtyVolume<S> {
    fn geometry(&self) -> versionfs_core::format::VolumeGeometry {
        self.geometry()
    }

    fn supports_fua(&self) -> bool {
        false
    }

    fn dispatch(&mut self, request: BlockRequest<'_>) -> Result<BlockResponse, BlockError> {
        DirtyVolume::dispatch(self, request)
    }
}

/// One encoded immutable object created while staging a volume barrier.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StagedObject {
    /// Typed content reference for the encoded object.
    pub reference: ObjectRef,
    /// Complete canonical encoded object bytes.
    pub encoded: Vec<u8>,
}

/// A complete unpublished replacement for a dirty volume prefix.
pub(crate) struct StagedVolume {
    pub(crate) volume_root: ObjectRef,
    pub(crate) dirty_revision: u64,
    pub(crate) dirty_extent_refs: Vec<(u64, ObjectRef)>,
    pub(crate) objects: Vec<StagedObject>,
}

pub(crate) struct StagingMapStore<'a, S> {
    base: &'a S,
    staged: BTreeMap<ObjectId, Vec<u8>>,
}

impl<'a, S: MapObjectStore> StagingMapStore<'a, S> {
    pub(crate) fn new(base: &'a S) -> Self {
        Self {
            base,
            staged: BTreeMap::new(),
        }
    }

    pub(crate) fn contains_staged(&self, id: ObjectId) -> bool {
        self.staged.contains_key(&id)
    }

    pub(crate) fn into_objects(self) -> Result<Vec<(ObjectRef, Vec<u8>)>, FormatError> {
        self.staged
            .into_iter()
            .map(|(id, encoded)| {
                let reference = ObjectRef::from_encoded(&encoded)?;
                if reference.object_id != id {
                    return Err(FormatError::ObjectReferenceMismatch);
                }
                Ok((reference, encoded))
            })
            .collect()
    }

    pub(crate) fn into_objects_referenced(
        self,
        references: impl IntoIterator<Item = ObjectRef>,
    ) -> Result<Vec<StagedObject>, FormatError> {
        let mut referenced = BTreeMap::new();
        for reference in references {
            if referenced
                .insert(reference.object_id, reference)
                .is_some_and(|existing| existing != reference)
            {
                return Err(FormatError::ObjectReferenceMismatch);
            }
        }

        let mut objects = Vec::new();
        for (id, encoded) in self.staged {
            let Some(reference) = referenced.get(&id).copied() else {
                continue;
            };
            reference.verify(&encoded)?;
            objects.push(StagedObject { reference, encoded });
        }
        Ok(objects)
    }
}

impl<S: MapObjectStore> MapObjectStore for StagingMapStore<'_, S> {
    fn proof_scope(&self) -> versionfs_core::identity::ObjectStoreScopeId {
        self.base.proof_scope()
    }

    fn get(&self, id: ObjectId) -> Result<Option<Vec<u8>>, String> {
        if let Some(bytes) = self.staged.get(&id) {
            return Ok(Some(bytes.clone()));
        }
        self.base.get(id)
    }

    fn get_ref(&self, reference: ObjectRef) -> Result<Option<Vec<u8>>, String> {
        if let Some(bytes) = self.staged.get(&reference.object_id) {
            reference.verify(bytes).map_err(|error| error.to_string())?;
            return Ok(Some(bytes.clone()));
        }
        self.base.get_ref(reference)
    }

    fn put(&mut self, id: ObjectId, bytes: Vec<u8>) -> Result<(), String> {
        let reference = ObjectRef::from_encoded(&bytes).map_err(|error| error.to_string())?;
        if reference.object_id != id {
            return Err(FormatError::ObjectReferenceMismatch.to_string());
        }
        if let Some(existing) = self.staged.get(&id) {
            if existing != &bytes {
                return Err(FormatError::ObjectReferenceMismatch.to_string());
            }
            return Ok(());
        }
        self.staged.insert(id, bytes);
        Ok(())
    }
}

/// An immutable borrowed version of the current dirty extents.
pub struct SealedDirty<'a> {
    revision: u64,
    extents: &'a BTreeMap<u64, Arc<[u8; EXTENT_SIZE]>>,
}

impl SealedDirty<'_> {
    /// Returns the dirty overlay revision covered by this snapshot.
    pub const fn revision(&self) -> u64 {
        self.revision
    }

    /// Iterates over sealed extent numbers and immutable bytes in order.
    pub fn extents(&self) -> impl ExactSizeIterator<Item = (u64, &[u8])> + '_ {
        self.extents
            .iter()
            .map(|(number, bytes)| (*number, bytes.as_slice()))
    }
}

#[cfg(test)]
mod tests {
    use super::{BlockError, BlockRequest, BlockResponse, DirtyVolume};
    use crate::volume::EXTENT_SIZE;
    use std::collections::HashMap;
    use versionfs_core::format::{
        FormatError, ObjectRef, ObjectType, VolumeGeometry, encode_object,
    };
    use versionfs_core::identity::{ObjectId, ObjectStoreScopeId};
    use versionfs_core::persistent_map::MapObjectStore;
    use versionfs_core::views::VolumeExtentMap;

    #[derive(Default)]
    struct MemoryStore(HashMap<ObjectId, Vec<u8>>);

    impl MapObjectStore for MemoryStore {
        fn proof_scope(&self) -> ObjectStoreScopeId {
            ObjectStoreScopeId::from_bytes([0x51; 16])
        }

        fn get(&self, id: ObjectId) -> Result<Option<Vec<u8>>, String> {
            Ok(self.0.get(&id).cloned())
        }

        fn put(&mut self, id: ObjectId, bytes: Vec<u8>) -> Result<(), String> {
            self.0.insert(id, bytes);
            Ok(())
        }
    }

    fn store_extent(store: &mut MemoryStore, fill: u8) -> ObjectRef {
        let bytes = encode_object(ObjectType::Extent, &vec![fill; EXTENT_SIZE]).unwrap();
        let reference = ObjectRef::from_encoded(&bytes).unwrap();
        store.put(reference.object_id, bytes).unwrap();
        reference
    }

    fn base_volume(store: &mut MemoryStore) -> VolumeExtentMap {
        let geometry = VolumeGeometry::new((4 * EXTENT_SIZE) as u64).unwrap();
        let first = store_extent(store, 0x21);
        let third = store_extent(store, 0x63);
        VolumeExtentMap::empty(geometry, store)
            .unwrap()
            .insert_extent(store, 0, first)
            .unwrap()
            .insert_extent(store, 2, third)
            .unwrap()
    }

    #[test]
    fn dirty_volume_matches_byte_array_model_across_partial_extent_writes() {
        let mut store = MemoryStore::default();
        let base = base_volume(&mut store);
        let mut volume = DirtyVolume::new(store, base, 4 * EXTENT_SIZE).unwrap();
        let mut model = vec![0; 4 * EXTENT_SIZE];
        model[..EXTENT_SIZE].fill(0x21);
        model[2 * EXTENT_SIZE..3 * EXTENT_SIZE].fill(0x63);

        let writes = [
            (4096, vec![0xA1; 12 * 1024]),
            (EXTENT_SIZE - 4096, vec![0xB2; 8 * 1024]),
            (3 * EXTENT_SIZE, vec![0xC3; EXTENT_SIZE]),
        ];
        for (offset, bytes) in writes {
            volume.write(offset as u64, &bytes).unwrap();
            model[offset..offset + bytes.len()].copy_from_slice(&bytes);
        }

        assert_eq!(volume.read(0, model.len() as u64).unwrap(), model);
        assert_eq!(volume.dirty_extent_count(), 3);
        assert_eq!(volume.dirty_bytes(), 3 * EXTENT_SIZE);
    }

    #[test]
    fn dirty_volume_reads_reject_ranges_too_large_for_memory() {
        let mut store = MemoryStore::default();
        let geometry = VolumeGeometry::new(1_u64 << 63).unwrap();
        let base = VolumeExtentMap::empty(geometry, &mut store).unwrap();
        let volume = DirtyVolume::new(store, base, EXTENT_SIZE).unwrap();

        assert!(matches!(
            volume.read(0, 1_u64 << 63),
            Err(BlockError::Format(FormatError::InvalidGeometry(
                "range does not fit memory"
            )))
        ));
    }

    #[test]
    fn dirty_capacity_rejection_leaves_the_entire_request_unmodified() {
        let mut store = MemoryStore::default();
        let base = base_volume(&mut store);
        let mut volume = DirtyVolume::new(store, base, EXTENT_SIZE).unwrap();
        volume.write(0, &vec![0xA4; 4096]).unwrap();

        let before = volume.read(0, (2 * EXTENT_SIZE) as u64).unwrap();
        assert!(matches!(
            volume.write((EXTENT_SIZE - 4096) as u64, &vec![0xB5; 8192]),
            Err(BlockError::DirtyCapacityExceeded)
        ));
        assert_eq!(volume.read(0, (2 * EXTENT_SIZE) as u64).unwrap(), before);
        assert_eq!(volume.dirty_extent_count(), 1);
    }

    #[test]
    fn failed_partial_read_modify_write_does_not_publish_dirty_bytes() {
        let mut store = MemoryStore::default();
        let reference = store_extent(&mut store, 0x42);
        let geometry = VolumeGeometry::new((2 * EXTENT_SIZE) as u64).unwrap();
        let base = VolumeExtentMap::empty(geometry, &mut store)
            .unwrap()
            .insert_extent(&mut store, 0, reference)
            .unwrap();
        store
            .0
            .insert(reference.object_id, vec![0; reference.encoded_len as usize]);
        let mut volume = DirtyVolume::new(store, base, EXTENT_SIZE).unwrap();

        assert!(volume.write(0, &[0x53; 4096]).is_err());
        assert_eq!(volume.dirty_extent_count(), 0);
    }

    #[test]
    fn sealed_dirty_bytes_have_a_stable_revision_and_upload_view() {
        let mut store = MemoryStore::default();
        let base = base_volume(&mut store);
        let mut volume = DirtyVolume::new(store, base, 2 * EXTENT_SIZE).unwrap();
        volume.write(0, &vec![0xD6; 4096]).unwrap();

        {
            let sealed = volume.seal_dirty();
            assert_eq!(sealed.revision(), 1);
            let extents: Vec<_> = sealed.extents().collect();
            assert_eq!(extents.len(), 1);
            assert_eq!(extents[0].0, 0);
            assert_eq!(&extents[0].1[..4096], &vec![0xD6; 4096]);
            assert_eq!(&extents[0].1[4096..], &vec![0x21; EXTENT_SIZE - 4096]);
        }

        volume.write(4096, &vec![0xE7; 4096]).unwrap();
        assert_eq!(volume.seal_dirty().revision(), 2);
    }

    #[test]
    fn dispatch_rejects_barriers_before_changing_volatile_state() {
        let mut store = MemoryStore::default();
        let base = base_volume(&mut store);
        let mut volume = DirtyVolume::new(store, base, EXTENT_SIZE).unwrap();

        assert!(matches!(
            volume.dispatch(BlockRequest::Write {
                offset: 0,
                data: &[0xF8; 4096],
                fua: true,
            }),
            Err(BlockError::DurableBarrierUnavailable)
        ));
        assert!(matches!(
            volume.dispatch(BlockRequest::Flush),
            Err(BlockError::DurableBarrierUnavailable)
        ));
        assert_eq!(volume.dirty_extent_count(), 0);
        assert_eq!(volume.read(0, 4096).unwrap(), vec![0x21; 4096]);

        assert_eq!(
            volume
                .dispatch(BlockRequest::Write {
                    offset: 0,
                    data: &[0xF8; 4096],
                    fua: false,
                })
                .unwrap(),
            BlockResponse::Written(4096)
        );
        assert_eq!(
            volume
                .dispatch(BlockRequest::Read {
                    offset: 0,
                    length: 4096,
                })
                .unwrap(),
            BlockResponse::Read(vec![0xF8; 4096])
        );
        assert!(matches!(
            volume.dispatch(BlockRequest::Unsupported(19)),
            Err(BlockError::UnsupportedOperation(19))
        ));
    }

    #[test]
    fn dirty_capacity_must_be_a_positive_extent_multiple() {
        let mut store = MemoryStore::default();
        let base = base_volume(&mut store);
        assert!(matches!(
            DirtyVolume::new(store, base, 0),
            Err(BlockError::InvalidDirtyCapacity)
        ));
    }
}
