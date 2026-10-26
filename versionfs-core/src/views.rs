//! Typed persistent maps and complete roots for Version FS native views.

use crate::format::{
    ARTIFACT_CHUNK_BYTES, ArtifactChunkEntry, ArtifactDescriptor, ArtifactMapRoot, EXTENT_BYTES,
    FormatError, LogMapEntry, LogMapRoot, MAX_ARTIFACT_BLOB_BYTES, ObjectRef, ObjectType,
    SegmentIndexEntry, VolumeGeometry, VolumeRoot, encode_object,
};
use crate::identity::ObjectId;
use crate::persistent_map::{MapChange, MapObjectStore, PersistentBTree, PersistentMapError};
use thiserror::Error;

/// Errors returned while constructing or reading a typed Version FS view.
#[derive(Debug, Error)]
pub enum ViewError {
    /// A canonical root or leaf value was invalid.
    #[error(transparent)]
    Format(#[from] FormatError),
    /// A persistent map operation failed.
    #[error(transparent)]
    Map(#[from] PersistentMapError),
    /// An object required to open or seal a view was unavailable.
    #[error("missing view object {0}")]
    MissingObject(ObjectId),
    /// The immutable object repository failed.
    #[error("object repository error: {0}")]
    Store(String),
}

/// Persistent map from logical volume extent number to immutable extent.
#[derive(Clone, Copy, Debug)]
pub struct VolumeExtentMap {
    geometry: VolumeGeometry,
    map: PersistentBTree,
    metadata_validated: bool,
}

impl PartialEq for VolumeExtentMap {
    fn eq(&self, other: &Self) -> bool {
        self.geometry == other.geometry && self.map == other.map
    }
}

impl Eq for VolumeExtentMap {}

/// One contiguous run of changed logical volume extents.
///
/// This is a block-range result. It carries no pathname or filesystem meaning.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExtentRange {
    /// First changed extent number.
    pub start_extent: u64,
    /// Number of consecutive changed extents.
    pub extent_count: u64,
}

impl VolumeExtentMap {
    /// Creates a complete empty sparse volume map.
    pub fn empty<S: MapObjectStore>(
        geometry: VolumeGeometry,
        store: &mut S,
    ) -> Result<Self, ViewError> {
        geometry.validate_range(0, 0)?;
        Ok(Self {
            geometry,
            map: PersistentBTree::empty(ObjectType::VolumeMapNode, store)?,
            metadata_validated: true,
        })
    }

    /// Opens the extent map referenced by a complete volume root.
    pub fn from_root(root: VolumeRoot) -> Result<Self, ViewError> {
        root.geometry.validate_range(0, 0)?;
        Ok(Self {
            geometry: root.geometry,
            map: from_typed_map_root(root.extent_map_root, ObjectType::VolumeMapNode)?,
            metadata_validated: false,
        })
    }

    /// Returns a handle whose complete key/reference metadata has been validated.
    ///
    /// Keep this returned handle to avoid rescanning an imported root on later
    /// seals. Mutations preserve its validation status. Extent payloads and
    /// durability are checked separately by the Generation graph verifier.
    ///
    /// # Errors
    ///
    /// Returns an error for unavailable/malformed map nodes, out-of-capacity keys,
    /// or references with an invalid extent type or size.
    pub fn validate<S: MapObjectStore>(&self, store: &S) -> Result<Self, ViewError> {
        if !self.metadata_validated {
            self.entries(store)?;
        }
        Ok(Self {
            metadata_validated: true,
            ..*self
        })
    }

    /// Returns the fixed logical geometry.
    pub const fn geometry(&self) -> VolumeGeometry {
        self.geometry
    }

    /// Returns an extent reference or `None` for a logical zero-filled hole.
    pub fn extent<S: MapObjectStore>(
        &self,
        store: &S,
        extent_number: u64,
    ) -> Result<Option<ObjectRef>, ViewError> {
        self.check_extent_number(extent_number)?;
        Ok(self
            .map
            .get(store, &extent_number.to_be_bytes())?
            .map(|value| decode_extent_ref(&value))
            .transpose()?)
    }

    /// Returns a new map with one non-zero immutable extent reference installed.
    pub fn insert_extent<S: MapObjectStore>(
        &self,
        store: &mut S,
        extent_number: u64,
        extent: ObjectRef,
    ) -> Result<Self, ViewError> {
        self.check_extent_number(extent_number)?;
        let extent = validate_extent_ref(extent)?;
        let map = self.map.insert(
            store,
            extent_number.to_be_bytes().to_vec(),
            encode_ref(extent)?,
        )?;
        Ok(Self {
            geometry: self.geometry,
            map,
            metadata_validated: self.metadata_validated,
        })
    }

    /// Returns a new map with an extent represented as an implicit zero hole.
    pub fn remove_extent<S: MapObjectStore>(
        &self,
        store: &mut S,
        extent_number: u64,
    ) -> Result<Self, ViewError> {
        self.check_extent_number(extent_number)?;
        Ok(Self {
            geometry: self.geometry,
            map: self.map.remove(store, &extent_number.to_be_bytes())?,
            metadata_validated: self.metadata_validated,
        })
    }

    /// Returns all allocated extents in logical order.
    pub fn entries<S: MapObjectStore>(
        &self,
        store: &S,
    ) -> Result<Vec<(u64, ObjectRef)>, ViewError> {
        self.map
            .entries(store)?
            .into_iter()
            .map(|(key, value)| {
                let index = decode_u64_key(&key)?;
                self.check_extent_number(index)?;
                let extent = decode_extent_ref(&value)?;
                Ok((index, extent))
            })
            .collect()
    }

    /// Reads a checked logical block range from this protected immutable root.
    ///
    /// Unmapped extents are zero-filled. Allocated extents are fetched and fully
    /// verified before any requested bytes are copied to the result. The caller
    /// must keep this root protected from reclamation for the duration of reads.
    ///
    /// # Errors
    ///
    /// Returns an error for an unaligned or out-of-capacity range, a missing or
    /// corrupt allocated extent, or an extent with an invalid payload size.
    pub fn read_range<S: MapObjectStore>(
        &self,
        store: &S,
        offset: u64,
        length: u64,
    ) -> Result<Vec<u8>, ViewError> {
        self.geometry.validate_range(offset, length)?;
        let output_len = usize::try_from(length)
            .map_err(|_| FormatError::InvalidGeometry("range does not fit memory"))?;
        let mut output = Vec::new();
        output
            .try_reserve_exact(output_len)
            .map_err(|_| FormatError::InvalidGeometry("range does not fit memory"))?;
        output.resize(output_len, 0);
        let extent_bytes = u64::from(EXTENT_BYTES);
        let mut position = offset;
        let mut written = 0_usize;

        while written < output.len() {
            let extent_number = position / extent_bytes;
            let in_extent = usize::try_from(position % extent_bytes)
                .map_err(|_| FormatError::InvalidGeometry("extent offset does not fit memory"))?;
            let chunk_len = (output.len() - written).min(EXTENT_BYTES as usize - in_extent);

            if let Some(reference) = self.extent(store, extent_number)? {
                let encoded = store
                    .get_ref(reference)
                    .map_err(ViewError::Store)?
                    .ok_or(ViewError::MissingObject(reference.object_id))?;
                let object = reference.verify(&encoded)?;
                if object.payload.len() != EXTENT_BYTES as usize {
                    return Err(FormatError::InvalidGeometry(
                        "allocated extent payload must be exactly 64 KiB",
                    )
                    .into());
                }
                output[written..written + chunk_len]
                    .copy_from_slice(&object.payload[in_extent..in_extent + chunk_len]);
            }

            position = position
                .checked_add(chunk_len as u64)
                .ok_or(FormatError::InvalidGeometry("range overflow"))?;
            written += chunk_len;
        }

        Ok(output)
    }

    /// Returns contiguous changed extent runs between two roots.
    ///
    /// Structural sharing lets the persistent-map diff skip identical subtrees.
    /// The result describes block extents only and does not imply file changes.
    /// The caller must keep both roots protected for the duration of the diff.
    ///
    /// # Errors
    ///
    /// Returns an error when geometries differ or either map contains malformed
    /// extent metadata.
    pub fn diff_extents<S: MapObjectStore>(
        &self,
        newer: &Self,
        store: &S,
    ) -> Result<Vec<ExtentRange>, ViewError> {
        if self.geometry != newer.geometry {
            return Err(FormatError::InvalidGeometry(
                "cannot diff volumes with different geometry",
            )
            .into());
        }

        let changes = self.map.diff(&newer.map, store)?;
        let mut ranges: Vec<ExtentRange> = Vec::new();
        for change in changes {
            let key = match &change {
                MapChange::Added { key, .. }
                | MapChange::Removed { key, .. }
                | MapChange::Updated { key, .. } => key,
            };
            let extent_number = decode_u64_key(key)?;
            self.check_extent_number(extent_number)?;
            match change {
                MapChange::Added { value, .. } | MapChange::Removed { value, .. } => {
                    decode_extent_ref(&value)?;
                }
                MapChange::Updated { before, after, .. } => {
                    decode_extent_ref(&before)?;
                    decode_extent_ref(&after)?;
                }
            }

            if let Some(last) = ranges.last_mut() {
                let next_extent = last
                    .start_extent
                    .checked_add(last.extent_count)
                    .ok_or(FormatError::InvalidGeometry("extent range overflow"))?;
                if next_extent == extent_number {
                    last.extent_count = last
                        .extent_count
                        .checked_add(1)
                        .ok_or(FormatError::InvalidGeometry("extent range overflow"))?;
                    continue;
                }
            }
            ranges.push(ExtentRange {
                start_extent: extent_number,
                extent_count: 1,
            });
        }
        Ok(ranges)
    }

    /// Stores a complete volume root for this immutable extent-map version.
    ///
    /// Unverified handles validate all metadata here; retain the result of
    /// [`Self::validate`] to reuse that validation across subsequent mutations.
    pub fn seal<S: MapObjectStore>(&self, store: &mut S) -> Result<ObjectRef, ViewError> {
        self.validate(store)?;
        let bytes = VolumeRoot {
            geometry: self.geometry,
            extent_map_root: self.map.root(),
        }
        .encode()?;
        store_encoded(store, bytes)
    }

    fn check_extent_number(&self, extent_number: u64) -> Result<(), FormatError> {
        let extent_count = self.geometry.capacity_bytes / u64::from(self.geometry.extent_bytes);
        if extent_number >= extent_count {
            return Err(FormatError::InvalidGeometry(
                "extent number is outside the fixed volume capacity",
            ));
        }
        Ok(())
    }
}

/// Persistent map from segment start sequence to one sealed segment range.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LogSegmentIndex {
    map: PersistentBTree,
}

impl LogSegmentIndex {
    /// Creates an empty segment index.
    pub fn empty<S: MapObjectStore>(store: &mut S) -> Result<Self, ViewError> {
        Ok(Self {
            map: PersistentBTree::empty(ObjectType::LogSegmentIndexNode, store)?,
        })
    }

    /// Opens a segment index at an immutable root node.
    pub fn from_root(root: ObjectRef) -> Result<Self, ViewError> {
        Ok(Self {
            map: from_typed_map_root(root, ObjectType::LogSegmentIndexNode)?,
        })
    }

    /// Returns this index's complete immutable root node.
    pub const fn root(&self) -> ObjectRef {
        self.map.root()
    }

    /// Adds or replaces a segment range keyed by its first sequence number.
    pub fn insert<S: MapObjectStore>(
        &self,
        store: &mut S,
        first_seq: u64,
        entry: SegmentIndexEntry,
    ) -> Result<Self, ViewError> {
        if first_seq >= entry.end_seq {
            return Err(FormatError::InvalidNativeView("empty segment-index range").into());
        }
        Ok(Self {
            map: self
                .map
                .insert(store, first_seq.to_be_bytes().to_vec(), entry.encode()?)?,
        })
    }

    /// Returns all ranges in sequence order.
    pub fn entries<S: MapObjectStore>(
        &self,
        store: &S,
    ) -> Result<Vec<(u64, SegmentIndexEntry)>, ViewError> {
        self.map
            .entries(store)?
            .into_iter()
            .map(|(key, value)| {
                let first_seq = decode_u64_key(&key)?;
                let entry = SegmentIndexEntry::decode(&value)?;
                if first_seq >= entry.end_seq {
                    return Err(FormatError::InvalidNativeView("empty segment-index range").into());
                }
                Ok((first_seq, entry))
            })
            .collect()
    }
}

/// Persistent map from UTF-8 log names to complete log state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LogMap {
    map: PersistentBTree,
    entry_count: u64,
}

impl LogMap {
    /// Creates an empty named-log map.
    pub fn empty<S: MapObjectStore>(store: &mut S) -> Result<Self, ViewError> {
        Ok(Self {
            map: PersistentBTree::empty(ObjectType::LogMapNode, store)?,
            entry_count: 0,
        })
    }

    /// Opens a named-log map using a verified complete root object.
    pub fn open<S: MapObjectStore>(store: &S, root_ref: ObjectRef) -> Result<Self, ViewError> {
        let root = LogMapRoot::decode(&load_verified(store, root_ref)?)?;
        let map = PersistentBTree::from_root(root.log_index_root)?;
        let entry_count = validate_named_map(
            &map,
            store,
            LogMapEntry::decode,
            "log names must be non-empty UTF-8",
        )?;
        if entry_count != root.log_count {
            return Err(
                FormatError::InvalidNativeView("log count differs from map contents").into(),
            );
        }
        Ok(Self { map, entry_count })
    }

    /// Returns the log state bound to a name, if present.
    pub fn get<S: MapObjectStore>(
        &self,
        store: &S,
        name: &str,
    ) -> Result<Option<LogMapEntry>, ViewError> {
        Ok(self
            .map
            .get(store, name.as_bytes())?
            .map(|value| LogMapEntry::decode(&value))
            .transpose()?)
    }

    /// Returns a new map with a named log inserted or replaced.
    pub fn insert<S: MapObjectStore>(
        &self,
        store: &mut S,
        name: &str,
        entry: LogMapEntry,
    ) -> Result<Self, ViewError> {
        if name.is_empty() {
            return Err(FormatError::InvalidNativeView("log name must not be empty").into());
        }
        let value = entry.encode()?;
        let exists = self.map.get(store, name.as_bytes())?.is_some();
        let entry_count = self
            .entry_count
            .checked_add(u64::from(!exists))
            .ok_or(FormatError::InvalidNativeView("log count overflow"))?;
        Ok(Self {
            map: self.map.insert(store, name.as_bytes().to_vec(), value)?,
            entry_count,
        })
    }

    /// Returns a new map with a named log removed.
    pub fn remove<S: MapObjectStore>(&self, store: &mut S, name: &str) -> Result<Self, ViewError> {
        let exists = self.map.get(store, name.as_bytes())?.is_some();
        let entry_count = self
            .entry_count
            .checked_sub(u64::from(exists))
            .ok_or(FormatError::InvalidNativeView("log count underflow"))?;
        Ok(Self {
            map: self.map.remove(store, name.as_bytes())?,
            entry_count,
        })
    }

    /// Stores the complete named-log root for this map version.
    pub fn seal<S: MapObjectStore>(&self, store: &mut S) -> Result<ObjectRef, ViewError> {
        store_encoded(
            store,
            LogMapRoot {
                log_index_root: self.map.root(),
                log_count: self.entry_count,
            }
            .encode()?,
        )
    }
}

/// Persistent map from UTF-8 artifact names to immutable artifact descriptors.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ArtifactMap {
    map: PersistentBTree,
    entry_count: u64,
}

impl ArtifactMap {
    /// Creates an empty named-artifact map.
    pub fn empty<S: MapObjectStore>(store: &mut S) -> Result<Self, ViewError> {
        Ok(Self {
            map: PersistentBTree::empty(ObjectType::ArtifactMapNode, store)?,
            entry_count: 0,
        })
    }

    /// Opens a named-artifact map using a verified complete root object.
    pub fn open<S: MapObjectStore>(store: &S, root_ref: ObjectRef) -> Result<Self, ViewError> {
        let root = ArtifactMapRoot::decode(&load_verified(store, root_ref)?)?;
        let map = PersistentBTree::from_root(root.artifact_index_root)?;
        let entry_count = validate_named_map(
            &map,
            store,
            ArtifactDescriptor::decode,
            "artifact names must be non-empty UTF-8",
        )?;
        if entry_count != root.artifact_count {
            return Err(
                FormatError::InvalidNativeView("artifact count differs from map contents").into(),
            );
        }
        Ok(Self { map, entry_count })
    }

    /// Returns the descriptor bound to a name, if present.
    pub fn get<S: MapObjectStore>(
        &self,
        store: &S,
        name: &str,
    ) -> Result<Option<ArtifactDescriptor>, ViewError> {
        Ok(self
            .map
            .get(store, name.as_bytes())?
            .map(|value| ArtifactDescriptor::decode(&value))
            .transpose()?)
    }

    /// Returns a new map with a named artifact inserted or replaced.
    pub fn insert<S: MapObjectStore>(
        &self,
        store: &mut S,
        name: &str,
        descriptor: &ArtifactDescriptor,
    ) -> Result<Self, ViewError> {
        if name.is_empty() {
            return Err(FormatError::InvalidNativeView("artifact name must not be empty").into());
        }
        let value = descriptor.encode()?;
        let exists = self.map.get(store, name.as_bytes())?.is_some();
        let entry_count = self
            .entry_count
            .checked_add(u64::from(!exists))
            .ok_or(FormatError::InvalidNativeView("artifact count overflow"))?;
        Ok(Self {
            map: self.map.insert(store, name.as_bytes().to_vec(), value)?,
            entry_count,
        })
    }

    /// Returns a new map with a named artifact removed.
    pub fn remove<S: MapObjectStore>(&self, store: &mut S, name: &str) -> Result<Self, ViewError> {
        let exists = self.map.get(store, name.as_bytes())?.is_some();
        let entry_count = self
            .entry_count
            .checked_sub(u64::from(exists))
            .ok_or(FormatError::InvalidNativeView("artifact count underflow"))?;
        Ok(Self {
            map: self.map.remove(store, name.as_bytes())?,
            entry_count,
        })
    }

    /// Stores the complete named-artifact root for this map version.
    pub fn seal<S: MapObjectStore>(&self, store: &mut S) -> Result<ObjectRef, ViewError> {
        store_encoded(
            store,
            ArtifactMapRoot {
                artifact_index_root: self.map.root(),
                artifact_count: self.entry_count,
            }
            .encode()?,
        )
    }
}

/// Persistent map from logical artifact chunk number to chunk object and length.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ArtifactChunkIndex {
    map: PersistentBTree,
}

impl ArtifactChunkIndex {
    /// Creates an empty artifact-chunk index.
    pub fn empty<S: MapObjectStore>(store: &mut S) -> Result<Self, ViewError> {
        Ok(Self {
            map: PersistentBTree::empty(ObjectType::ArtifactIndexNode, store)?,
        })
    }

    /// Opens an artifact-chunk index at its immutable root node.
    pub fn from_root(root: ObjectRef) -> Result<Self, ViewError> {
        Ok(Self {
            map: from_typed_map_root(root, ObjectType::ArtifactIndexNode)?,
        })
    }

    /// Returns this index's complete immutable root node.
    pub const fn root(&self) -> ObjectRef {
        self.map.root()
    }

    /// Adds or replaces one indexed chunk.
    pub fn insert<S: MapObjectStore>(
        &self,
        store: &mut S,
        chunk_number: u64,
        entry: ArtifactChunkEntry,
    ) -> Result<Self, ViewError> {
        Ok(Self {
            map: self
                .map
                .insert(store, chunk_number.to_be_bytes().to_vec(), entry.encode()?)?,
        })
    }

    /// Returns all chunks in logical order.
    pub fn entries<S: MapObjectStore>(
        &self,
        store: &S,
    ) -> Result<Vec<(u64, ArtifactChunkEntry)>, ViewError> {
        self.map
            .entries(store)?
            .into_iter()
            .map(|(key, value)| Ok((decode_u64_key(&key)?, ArtifactChunkEntry::decode(&value)?)))
            .collect()
    }
}

/// Encodes one inline artifact payload (at most 64 KiB).
pub fn encode_artifact_blob(payload: &[u8]) -> Result<Vec<u8>, FormatError> {
    if payload.len() > MAX_ARTIFACT_BLOB_BYTES {
        return Err(FormatError::PayloadTooLarge {
            actual: payload.len(),
            maximum: MAX_ARTIFACT_BLOB_BYTES,
        });
    }
    encode_object(ObjectType::ArtifactBlob, payload)
}

/// Encodes one non-empty fixed-size-or-final artifact chunk (at most 4 MiB).
pub fn encode_artifact_chunk(payload: &[u8]) -> Result<Vec<u8>, FormatError> {
    if payload.is_empty() {
        return Err(FormatError::InvalidNativeView(
            "artifact chunks must not be empty",
        ));
    }
    if payload.len() > ARTIFACT_CHUNK_BYTES {
        return Err(FormatError::PayloadTooLarge {
            actual: payload.len(),
            maximum: ARTIFACT_CHUNK_BYTES,
        });
    }
    encode_object(ObjectType::ArtifactChunk, payload)
}

fn encode_ref(reference: ObjectRef) -> Result<Vec<u8>, FormatError> {
    reference.validate()?;
    let mut encoder = crate::format::CanonicalEncoder::new();
    encoder.object_ref(reference);
    Ok(encoder.finish())
}

fn decode_ref(bytes: &[u8]) -> Result<ObjectRef, FormatError> {
    let mut decoder = crate::format::CanonicalDecoder::new(bytes);
    let reference = decoder.object_ref()?;
    decoder.finish()?;
    Ok(reference)
}

fn validate_extent_ref(reference: ObjectRef) -> Result<ObjectRef, FormatError> {
    if reference.object_type != ObjectType::Extent
        || reference.encoded_len
            != crate::format::ENVELOPE_HEADER_BYTES as u64 + u64::from(crate::format::EXTENT_BYTES)
    {
        return Err(FormatError::ObjectReferenceMismatch);
    }
    reference.validate()?;
    Ok(reference)
}

fn decode_extent_ref(bytes: &[u8]) -> Result<ObjectRef, FormatError> {
    validate_extent_ref(decode_ref(bytes)?)
}

fn decode_u64_key(key: &[u8]) -> Result<u64, FormatError> {
    let bytes: [u8; 8] = key
        .try_into()
        .map_err(|_| FormatError::InvalidNativeView("numeric map key must be 8 bytes"))?;
    Ok(u64::from_be_bytes(bytes))
}

/// Validates an imported named map once and retains its exact count in the handle.
fn validate_named_map<S: MapObjectStore, T>(
    map: &PersistentBTree,
    store: &S,
    decode_value: impl Fn(&[u8]) -> Result<T, FormatError>,
    invalid_name: &'static str,
) -> Result<u64, ViewError> {
    let entries = map.entries(store)?;
    for (name, value) in &entries {
        if name.is_empty() || std::str::from_utf8(name).is_err() {
            return Err(FormatError::InvalidNativeView(invalid_name).into());
        }
        decode_value(value)?;
    }
    Ok(entries.len() as u64)
}

fn store_encoded<S: MapObjectStore>(store: &mut S, bytes: Vec<u8>) -> Result<ObjectRef, ViewError> {
    let reference = ObjectRef::from_encoded(&bytes)?;
    store
        .put(reference.object_id, bytes)
        .map_err(ViewError::Store)?;
    Ok(reference)
}

fn from_typed_map_root(
    root: ObjectRef,
    expected_type: ObjectType,
) -> Result<PersistentBTree, ViewError> {
    if root.object_type != expected_type {
        return Err(FormatError::ObjectReferenceMismatch.into());
    }
    Ok(PersistentBTree::from_root(root)?)
}

fn load_verified<S: MapObjectStore>(store: &S, reference: ObjectRef) -> Result<Vec<u8>, ViewError> {
    let bytes = store
        .get_ref(reference)
        .map_err(ViewError::Store)?
        .ok_or(ViewError::MissingObject(reference.object_id))?;
    reference.verify(&bytes)?;
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::{
        ArtifactChunkIndex, ArtifactMap, ExtentRange, LogMap, LogSegmentIndex, ViewError,
        VolumeExtentMap, encode_ref, store_encoded,
    };
    use crate::format::{
        ArtifactDescriptor, ArtifactMapRoot, FormatError, LogMapEntry, LogMapRoot,
        MAX_PERSISTENT_NODE_BYTES, ObjectRef, ObjectType, VolumeGeometry, VolumeRoot,
        encode_object,
    };
    use crate::identity::{ArtifactId, LogId, ObjectId, ObjectStoreScopeId};
    use crate::persistent_map::{MapObjectStore, PersistentBTree};
    use std::cell::RefCell;
    use std::collections::{HashMap, HashSet};

    #[derive(Default)]
    struct MemoryStore(HashMap<ObjectId, Vec<u8>>);

    impl MapObjectStore for MemoryStore {
        fn proof_scope(&self) -> ObjectStoreScopeId {
            ObjectStoreScopeId::from_bytes([0x5A; 16])
        }

        fn get(&self, id: ObjectId) -> Result<Option<Vec<u8>>, String> {
            Ok(self.0.get(&id).cloned())
        }

        fn put(&mut self, id: ObjectId, bytes: Vec<u8>) -> Result<(), String> {
            self.0.insert(id, bytes);
            Ok(())
        }
    }

    struct ReadTrackingStore<'a> {
        inner: &'a MemoryStore,
        reads: RefCell<HashSet<ObjectId>>,
    }

    impl MapObjectStore for ReadTrackingStore<'_> {
        fn proof_scope(&self) -> ObjectStoreScopeId {
            self.inner.proof_scope()
        }

        fn get(&self, id: ObjectId) -> Result<Option<Vec<u8>>, String> {
            self.reads.borrow_mut().insert(id);
            self.inner.get(id)
        }

        fn put(&mut self, _id: ObjectId, _bytes: Vec<u8>) -> Result<(), String> {
            Err("read tracking store is read-only".to_owned())
        }
    }

    struct NoReadStore<'a>(&'a mut MemoryStore);

    impl MapObjectStore for NoReadStore<'_> {
        fn proof_scope(&self) -> ObjectStoreScopeId {
            self.0.proof_scope()
        }

        fn get(&self, _id: ObjectId) -> Result<Option<Vec<u8>>, String> {
            Err("unexpected object read while sealing".to_owned())
        }

        fn put(&mut self, id: ObjectId, bytes: Vec<u8>) -> Result<(), String> {
            self.0.put(id, bytes)
        }
    }

    #[test]
    fn validated_volume_seal_does_not_read_map_nodes() {
        let mut store = MemoryStore::default();
        let extent = ObjectRef {
            object_type: ObjectType::Extent,
            object_id: ObjectId::from_bytes([1; 32]),
            encoded_len: 16 + 65_536,
        };
        let volume = VolumeExtentMap::empty(VolumeGeometry::new(4 * 65_536).unwrap(), &mut store)
            .unwrap()
            .insert_extent(&mut store, 0, extent)
            .unwrap()
            .remove_extent(&mut store, 1)
            .unwrap();
        let root_ref = volume.seal(&mut NoReadStore(&mut store)).unwrap();
        let root = VolumeRoot::decode(store.0.get(&root_ref.object_id).unwrap()).unwrap();
        let imported = VolumeExtentMap::from_root(root).unwrap();
        assert_eq!(volume, imported);
        assert!(imported.seal(&mut NoReadStore(&mut store)).is_err());

        let validated = imported.validate(&store).unwrap();
        assert_eq!(
            validated.seal(&mut NoReadStore(&mut store)).unwrap(),
            root_ref
        );
        let updated = validated.insert_extent(&mut store, 2, extent).unwrap();
        assert!(updated.seal(&mut NoReadStore(&mut store)).is_ok());
    }

    #[test]
    fn log_counts_survive_replacement_and_removal_without_seal_reads() {
        let mut store = MemoryStore::default();
        let entry = LogMapEntry {
            log_id: LogId::from_bytes([1; 16]),
            first_seq: 0,
            end_seq: 0,
            segment_index_root: LogSegmentIndex::empty(&mut store).unwrap().root(),
        };
        let logs = LogMap::empty(&mut store)
            .unwrap()
            .insert(&mut store, "keep", entry)
            .unwrap()
            .insert(
                &mut store,
                "keep",
                LogMapEntry {
                    first_seq: 1,
                    end_seq: 1,
                    ..entry
                },
            )
            .unwrap()
            .insert(&mut store, "remove", entry)
            .unwrap()
            .remove(&mut store, "absent")
            .unwrap()
            .remove(&mut store, "remove")
            .unwrap();
        let root_ref = logs.seal(&mut NoReadStore(&mut store)).unwrap();
        let root = LogMapRoot::decode(store.0.get(&root_ref.object_id).unwrap()).unwrap();
        assert_eq!(root.log_count, 1);
        let reopened = LogMap::open(&store, root_ref).unwrap();
        assert_eq!(
            reopened.seal(&mut NoReadStore(&mut store)).unwrap(),
            root_ref
        );

        let empty = reopened.remove(&mut store, "keep").unwrap();
        let empty_ref = empty.seal(&mut NoReadStore(&mut store)).unwrap();
        assert_eq!(
            LogMapRoot::decode(store.0.get(&empty_ref.object_id).unwrap())
                .unwrap()
                .log_count,
            0
        );
    }

    #[test]
    fn artifact_counts_survive_replacement_and_removal_without_seal_reads() {
        let mut store = MemoryStore::default();
        let descriptor = ArtifactDescriptor {
            artifact_id: ArtifactId::from_bytes([1; 16]),
            logical_len: 0,
            content_root: ObjectRef {
                object_type: ObjectType::ArtifactBlob,
                object_id: ObjectId::from_bytes([2; 32]),
                encoded_len: 16,
            },
            dependencies: Vec::new(),
        };
        let replacement = ArtifactDescriptor {
            artifact_id: ArtifactId::from_bytes([3; 16]),
            ..descriptor.clone()
        };
        let artifacts = ArtifactMap::empty(&mut store)
            .unwrap()
            .insert(&mut store, "keep", &descriptor)
            .unwrap()
            .insert(&mut store, "keep", &replacement)
            .unwrap()
            .insert(&mut store, "remove", &descriptor)
            .unwrap()
            .remove(&mut store, "absent")
            .unwrap()
            .remove(&mut store, "remove")
            .unwrap();
        let root_ref = artifacts.seal(&mut NoReadStore(&mut store)).unwrap();
        let root = ArtifactMapRoot::decode(store.0.get(&root_ref.object_id).unwrap()).unwrap();
        assert_eq!(root.artifact_count, 1);
        let reopened = ArtifactMap::open(&store, root_ref).unwrap();
        assert_eq!(
            reopened.seal(&mut NoReadStore(&mut store)).unwrap(),
            root_ref
        );

        let empty = reopened.remove(&mut store, "keep").unwrap();
        let empty_ref = empty.seal(&mut NoReadStore(&mut store)).unwrap();
        assert_eq!(
            ArtifactMapRoot::decode(store.0.get(&empty_ref.object_id).unwrap())
                .unwrap()
                .artifact_count,
            0
        );
    }

    #[test]
    fn named_map_open_rejects_malformed_metadata_before_trusting_counts() {
        for (name, value) in [(vec![0xFF], Vec::new()), (b"name".to_vec(), vec![0xFF])] {
            let mut store = MemoryStore::default();
            let log_tree = PersistentBTree::empty(ObjectType::LogMapNode, &mut store)
                .unwrap()
                .insert(&mut store, name.clone(), value.clone())
                .unwrap();
            let root_ref = store_encoded(
                &mut store,
                LogMapRoot {
                    log_index_root: log_tree.root(),
                    log_count: 1,
                }
                .encode()
                .unwrap(),
            )
            .unwrap();
            assert!(LogMap::open(&store, root_ref).is_err());
            let artifact_tree = PersistentBTree::empty(ObjectType::ArtifactMapNode, &mut store)
                .unwrap()
                .insert(&mut store, name, value)
                .unwrap();
            let root_ref = store_encoded(
                &mut store,
                ArtifactMapRoot {
                    artifact_index_root: artifact_tree.root(),
                    artifact_count: 1,
                }
                .encode()
                .unwrap(),
            )
            .unwrap();
            assert!(ArtifactMap::open(&store, root_ref).is_err());
        }
    }

    #[test]
    fn typed_map_roots_reject_a_different_persistent_map_type() {
        let mut store = MemoryStore::default();
        let geometry = VolumeGeometry::new(4 * 1024 * 1024).unwrap();
        let volume_root_ref = VolumeExtentMap::empty(geometry, &mut store)
            .unwrap()
            .seal(&mut store)
            .unwrap();
        let volume_root = VolumeRoot::decode(
            &store
                .get(volume_root_ref.object_id)
                .unwrap()
                .expect("sealed volume root exists"),
        )
        .unwrap();
        let log_index_root = LogSegmentIndex::empty(&mut store).unwrap().root();
        let artifact_index_root = ArtifactChunkIndex::empty(&mut store).unwrap().root();

        assert!(matches!(
            VolumeExtentMap::from_root(VolumeRoot {
                geometry,
                extent_map_root: log_index_root,
            }),
            Err(ViewError::Format(FormatError::ObjectReferenceMismatch))
        ));
        assert!(matches!(
            LogSegmentIndex::from_root(volume_root.extent_map_root),
            Err(ViewError::Format(FormatError::ObjectReferenceMismatch))
        ));
        assert!(matches!(
            ArtifactChunkIndex::from_root(volume_root.extent_map_root),
            Err(ViewError::Format(FormatError::ObjectReferenceMismatch))
        ));

        assert!(VolumeExtentMap::from_root(volume_root).is_ok());
        assert!(LogSegmentIndex::from_root(log_index_root).is_ok());
        assert!(ArtifactChunkIndex::from_root(artifact_index_root).is_ok());
    }

    #[test]
    fn volume_point_reads_reject_wrong_kind_and_size_extent_references() {
        let valid = ObjectRef {
            object_type: ObjectType::Extent,
            object_id: ObjectId::from_bytes([1; 32]),
            encoded_len: 16 + 65_536,
        };
        for invalid in [
            ObjectRef {
                object_type: ObjectType::ArtifactBlob,
                ..valid
            },
            ObjectRef {
                encoded_len: 16,
                ..valid
            },
            ObjectRef {
                encoded_len: valid.encoded_len + 1,
                ..valid
            },
        ] {
            let mut store = MemoryStore::default();
            let tree = PersistentBTree::empty(ObjectType::VolumeMapNode, &mut store)
                .unwrap()
                .insert(
                    &mut store,
                    0_u64.to_be_bytes().to_vec(),
                    encode_ref(invalid).unwrap(),
                )
                .unwrap()
                .insert(
                    &mut store,
                    2_u64.to_be_bytes().to_vec(),
                    encode_ref(valid).unwrap(),
                )
                .unwrap();
            let volume = VolumeExtentMap::from_root(VolumeRoot {
                geometry: VolumeGeometry::new(4 * 65_536).unwrap(),
                extent_map_root: tree.root(),
            })
            .unwrap();

            assert!(matches!(
                volume.extent(&store, 0),
                Err(ViewError::Format(FormatError::ObjectReferenceMismatch))
            ));
            assert!(volume.entries(&store).is_err());
            assert_eq!(volume.extent(&store, 1).unwrap(), None);
            assert_eq!(volume.extent(&store, 2).unwrap(), Some(valid));
            let changed = volume.insert_extent(&mut store, 1, valid).unwrap();
            assert!(changed.validate(&store).is_err());
            assert!(changed.seal(&mut store).is_err());
        }
    }

    #[test]
    fn volume_range_reads_verify_allocated_extents_and_zero_fill_holes() {
        let mut store = MemoryStore::default();
        let geometry = VolumeGeometry::new(4 * u64::from(crate::format::EXTENT_BYTES)).unwrap();
        let first = store_encoded(
            &mut store,
            encode_object(
                ObjectType::Extent,
                &vec![0x31; crate::format::EXTENT_BYTES as usize],
            )
            .unwrap(),
        )
        .unwrap();
        let third = store_encoded(
            &mut store,
            encode_object(
                ObjectType::Extent,
                &vec![0x73; crate::format::EXTENT_BYTES as usize],
            )
            .unwrap(),
        )
        .unwrap();
        let map = VolumeExtentMap::empty(geometry, &mut store)
            .unwrap()
            .insert_extent(&mut store, 0, first)
            .unwrap()
            .insert_extent(&mut store, 2, third)
            .unwrap();

        assert_eq!(map.read_range(&store, 0, 4096).unwrap(), vec![0x31; 4096]);
        assert_eq!(
            map.read_range(&store, u64::from(crate::format::EXTENT_BYTES), 8192)
                .unwrap(),
            vec![0; 8192]
        );
        let crossing = map
            .read_range(&store, u64::from(crate::format::EXTENT_BYTES) - 4096, 8192)
            .unwrap();
        assert_eq!(&crossing[..4096], &[0x31; 4096]);
        assert_eq!(&crossing[4096..], &[0; 4096]);

        store
            .0
            .insert(first.object_id, vec![0; first.encoded_len as usize]);
        assert!(matches!(
            map.read_range(&store, 0, 4096),
            Err(ViewError::Format(FormatError::ObjectReferenceMismatch))
        ));
    }

    #[test]
    fn volume_range_reads_reject_ranges_too_large_for_memory() {
        let mut store = MemoryStore::default();
        let geometry = VolumeGeometry::new(1_u64 << 63).unwrap();
        let map = VolumeExtentMap::empty(geometry, &mut store).unwrap();

        assert!(matches!(
            map.read_range(&store, 0, 1_u64 << 63),
            Err(ViewError::Format(FormatError::InvalidGeometry(
                "range does not fit memory"
            )))
        ));
    }

    #[test]
    fn volume_diff_coalesces_extent_ranges_and_tracks_zero_transitions() {
        let mut store = MemoryStore::default();
        let geometry = VolumeGeometry::new(8 * u64::from(crate::format::EXTENT_BYTES)).unwrap();
        let extent = |store: &mut MemoryStore, byte| {
            store_encoded(
                store,
                encode_object(
                    ObjectType::Extent,
                    &vec![byte; crate::format::EXTENT_BYTES as usize],
                )
                .unwrap(),
            )
            .unwrap()
        };
        let a = extent(&mut store, 0x11);
        let b = extent(&mut store, 0x22);
        let c = extent(&mut store, 0x33);
        let d = extent(&mut store, 0x44);
        let before = VolumeExtentMap::empty(geometry, &mut store)
            .unwrap()
            .insert_extent(&mut store, 0, a)
            .unwrap()
            .insert_extent(&mut store, 2, b)
            .unwrap()
            .insert_extent(&mut store, 3, c)
            .unwrap();
        let after = before
            .remove_extent(&mut store, 0)
            .unwrap()
            .insert_extent(&mut store, 2, d)
            .unwrap()
            .insert_extent(&mut store, 5, a)
            .unwrap()
            .insert_extent(&mut store, 6, b)
            .unwrap();

        assert_eq!(
            before.diff_extents(&after, &store).unwrap(),
            vec![
                super::ExtentRange {
                    start_extent: 0,
                    extent_count: 1,
                },
                super::ExtentRange {
                    start_extent: 2,
                    extent_count: 1,
                },
                super::ExtentRange {
                    start_extent: 5,
                    extent_count: 2,
                },
            ]
        );
        let other_geometry = VolumeExtentMap::empty(
            VolumeGeometry::new(4 * u64::from(crate::format::EXTENT_BYTES)).unwrap(),
            &mut store,
        )
        .unwrap();
        assert!(before.diff_extents(&other_geometry, &store).is_err());
    }

    #[test]
    fn cold_volume_read_fetches_only_its_index_path_and_requested_extent() {
        let mut store = MemoryStore::default();
        let geometry = VolumeGeometry::new(2048 * u64::from(crate::format::EXTENT_BYTES)).unwrap();
        let make_extent = |store: &mut MemoryStore, fill| {
            store_encoded(
                store,
                encode_object(
                    ObjectType::Extent,
                    &vec![fill; crate::format::EXTENT_BYTES as usize],
                )
                .unwrap(),
            )
            .unwrap()
        };
        let common = make_extent(&mut store, 0x18);
        let selected = make_extent(&mut store, 0x29);
        let unrelated = make_extent(&mut store, 0x3A);
        let mut map = VolumeExtentMap::empty(geometry, &mut store).unwrap();
        for extent_number in 0..1500 {
            let reference = match extent_number {
                700 => selected,
                1200 => unrelated,
                _ => common,
            };
            map = map
                .insert_extent(&mut store, extent_number, reference)
                .unwrap();
        }

        let tracking = ReadTrackingStore {
            inner: &store,
            reads: RefCell::new(HashSet::new()),
        };
        let bytes = map
            .read_range(
                &tracking,
                700 * u64::from(crate::format::EXTENT_BYTES),
                4096,
            )
            .unwrap();
        assert_eq!(bytes, vec![0x29; 4096]);
        let reads = tracking.reads.into_inner();
        assert!(reads.contains(&selected.object_id));
        assert!(!reads.contains(&common.object_id));
        assert!(!reads.contains(&unrelated.object_id));
        assert!(reads.len() < 16, "range read fetched unrelated index data");
    }

    #[test]
    fn extent_diff_skips_shared_subtrees_and_never_fetches_extent_payloads() {
        let mut store = MemoryStore::default();
        let geometry = VolumeGeometry::new(2048 * u64::from(crate::format::EXTENT_BYTES)).unwrap();
        let make_extent = |store: &mut MemoryStore, fill| {
            store_encoded(
                store,
                encode_object(
                    ObjectType::Extent,
                    &vec![fill; crate::format::EXTENT_BYTES as usize],
                )
                .unwrap(),
            )
            .unwrap()
        };
        let common = make_extent(&mut store, 0x48);
        let changed = make_extent(&mut store, 0x59);
        let mut before = VolumeExtentMap::empty(geometry, &mut store).unwrap();
        for extent_number in 0..1500 {
            before = before
                .insert_extent(&mut store, extent_number, common)
                .unwrap();
        }
        let after = before.insert_extent(&mut store, 731, changed).unwrap();

        let tracking = ReadTrackingStore {
            inner: &store,
            reads: RefCell::new(HashSet::new()),
        };
        assert_eq!(
            before.diff_extents(&after, &tracking).unwrap(),
            vec![ExtentRange {
                start_extent: 731,
                extent_count: 1,
            }]
        );
        let reads = tracking.reads.into_inner();
        assert!(!reads.contains(&common.object_id));
        assert!(!reads.contains(&changed.object_id));
        assert!(reads.len() < 32, "diff walked the complete persistent map");
    }

    #[test]
    fn artifact_map_splits_an_oversized_insert_into_three_leaf_nodes() {
        let mut store = MemoryStore::default();
        let content_root = ObjectRef {
            object_id: ObjectId::from_bytes([0xB0; 32]),
            object_type: ObjectType::ArtifactBlob,
            encoded_len: 17,
        };
        let dependency = ObjectRef {
            object_id: ObjectId::from_bytes([0xD0; 32]),
            object_type: ObjectType::ArtifactBlob,
            encoded_len: 16,
        };
        let descriptor = |id, dependency_count| ArtifactDescriptor {
            artifact_id: ArtifactId::from_bytes([id; 16]),
            logical_len: 1,
            content_root,
            dependencies: vec![dependency; dependency_count],
        };
        let a = descriptor(1, 712); // about 30 KiB
        let b = descriptor(2, 950); // about 40 KiB
        let c = descriptor(3, 712); // about 30 KiB

        let mut map = ArtifactMap::empty(&mut store).unwrap();
        map = map.insert(&mut store, "A", &a).unwrap();
        map = map.insert(&mut store, "C", &c).unwrap();
        map = map.insert(&mut store, "B", &b).unwrap();

        assert_eq!(map.get(&store, "A").unwrap(), Some(a.clone()));
        assert_eq!(map.get(&store, "B").unwrap(), Some(b.clone()));
        assert_eq!(map.get(&store, "C").unwrap(), Some(c.clone()));
        assert!(
            store
                .0
                .values()
                .all(|encoded| encoded.len() <= MAX_PERSISTENT_NODE_BYTES)
        );

        let root = map.seal(&mut store).unwrap();
        let reopened = ArtifactMap::open(&store, root).unwrap();
        assert_eq!(reopened.get(&store, "A").unwrap(), Some(a));
        assert_eq!(reopened.get(&store, "B").unwrap(), Some(b));
        assert_eq!(reopened.get(&store, "C").unwrap(), Some(c));
    }
}
