//! Typed persistent maps and complete roots for Version FS native views.

use crate::format::{
    ARTIFACT_CHUNK_BYTES, ArtifactChunkEntry, ArtifactDescriptor, ArtifactMapRoot, FormatError,
    LogMapEntry, LogMapRoot, MAX_ARTIFACT_BLOB_BYTES, ObjectRef, ObjectType, SegmentIndexEntry,
    VolumeGeometry, VolumeRoot, encode_object,
};
use crate::identity::ObjectId;
use crate::persistent_map::{MapObjectStore, PersistentBTree, PersistentMapError};
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
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VolumeExtentMap {
    geometry: VolumeGeometry,
    map: PersistentBTree,
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
        })
    }

    /// Opens the extent map referenced by a complete volume root.
    pub fn from_root(root: VolumeRoot) -> Result<Self, ViewError> {
        root.geometry.validate_range(0, 0)?;
        Ok(Self {
            geometry: root.geometry,
            map: from_typed_map_root(root.extent_map_root, ObjectType::VolumeMapNode)?,
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

    /// Stores a complete volume root for this immutable extent-map version.
    pub fn seal<S: MapObjectStore>(&self, store: &mut S) -> Result<ObjectRef, ViewError> {
        // Validate every key before making a complete root object available.
        self.entries(store)?;
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
}

impl LogMap {
    /// Creates an empty named-log map.
    pub fn empty<S: MapObjectStore>(store: &mut S) -> Result<Self, ViewError> {
        Ok(Self {
            map: PersistentBTree::empty(ObjectType::LogMapNode, store)?,
        })
    }

    /// Opens a named-log map using a verified complete root object.
    pub fn open<S: MapObjectStore>(store: &S, root_ref: ObjectRef) -> Result<Self, ViewError> {
        let root = LogMapRoot::decode(&load_verified(store, root_ref)?)?;
        let map = PersistentBTree::from_root(root.log_index_root)?;
        if map.entries(store)?.len() as u64 != root.log_count {
            return Err(
                FormatError::InvalidNativeView("log count differs from map contents").into(),
            );
        }
        Ok(Self { map })
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
        Ok(Self {
            map: self
                .map
                .insert(store, name.as_bytes().to_vec(), entry.encode()?)?,
        })
    }

    /// Returns a new map with a named log removed.
    pub fn remove<S: MapObjectStore>(&self, store: &mut S, name: &str) -> Result<Self, ViewError> {
        Ok(Self {
            map: self.map.remove(store, name.as_bytes())?,
        })
    }

    /// Stores the complete named-log root for this map version.
    pub fn seal<S: MapObjectStore>(&self, store: &mut S) -> Result<ObjectRef, ViewError> {
        let entries = self.map.entries(store)?;
        for (name, value) in &entries {
            if name.is_empty() || std::str::from_utf8(name).is_err() {
                return Err(
                    FormatError::InvalidNativeView("log names must be non-empty UTF-8").into(),
                );
            }
            LogMapEntry::decode(value)?;
        }
        store_encoded(
            store,
            LogMapRoot {
                log_index_root: self.map.root(),
                log_count: entries.len() as u64,
            }
            .encode()?,
        )
    }
}

/// Persistent map from UTF-8 artifact names to immutable artifact descriptors.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ArtifactMap {
    map: PersistentBTree,
}

impl ArtifactMap {
    /// Creates an empty named-artifact map.
    pub fn empty<S: MapObjectStore>(store: &mut S) -> Result<Self, ViewError> {
        Ok(Self {
            map: PersistentBTree::empty(ObjectType::ArtifactMapNode, store)?,
        })
    }

    /// Opens a named-artifact map using a verified complete root object.
    pub fn open<S: MapObjectStore>(store: &S, root_ref: ObjectRef) -> Result<Self, ViewError> {
        let root = ArtifactMapRoot::decode(&load_verified(store, root_ref)?)?;
        let map = PersistentBTree::from_root(root.artifact_index_root)?;
        if map.entries(store)?.len() as u64 != root.artifact_count {
            return Err(
                FormatError::InvalidNativeView("artifact count differs from map contents").into(),
            );
        }
        Ok(Self { map })
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
        Ok(Self {
            map: self
                .map
                .insert(store, name.as_bytes().to_vec(), descriptor.encode()?)?,
        })
    }

    /// Returns a new map with a named artifact removed.
    pub fn remove<S: MapObjectStore>(&self, store: &mut S, name: &str) -> Result<Self, ViewError> {
        Ok(Self {
            map: self.map.remove(store, name.as_bytes())?,
        })
    }

    /// Stores the complete named-artifact root for this map version.
    pub fn seal<S: MapObjectStore>(&self, store: &mut S) -> Result<ObjectRef, ViewError> {
        let entries = self.map.entries(store)?;
        for (name, value) in &entries {
            if name.is_empty() || std::str::from_utf8(name).is_err() {
                return Err(FormatError::InvalidNativeView(
                    "artifact names must be non-empty UTF-8",
                )
                .into());
            }
            ArtifactDescriptor::decode(value)?;
        }
        store_encoded(
            store,
            ArtifactMapRoot {
                artifact_index_root: self.map.root(),
                artifact_count: entries.len() as u64,
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
        .get(reference.object_id)
        .map_err(ViewError::Store)?
        .ok_or(ViewError::MissingObject(reference.object_id))?;
    reference.verify(&bytes)?;
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::{
        ArtifactChunkIndex, ArtifactMap, LogSegmentIndex, ViewError, VolumeExtentMap, encode_ref,
    };
    use crate::format::{
        ArtifactDescriptor, FormatError, MAX_PERSISTENT_NODE_BYTES, ObjectRef, ObjectType,
        VolumeGeometry, VolumeRoot,
    };
    use crate::identity::{ArtifactId, ObjectId, ObjectStoreScopeId};
    use crate::persistent_map::{MapObjectStore, PersistentBTree};
    use std::collections::HashMap;

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
        }
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
