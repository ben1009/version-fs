//! Trusted verification of complete immutable Generation dependency graphs.

use crate::format::{
    ARTIFACT_CHUNK_BYTES, ArtifactChunkEntry, ArtifactDescriptor, ArtifactMapRoot, CaptureMode,
    EXTENT_BYTES, FormatError, GenerationDescriptor, LogMapEntry, LogMapRoot, LogSegment,
    MAX_ARTIFACT_BLOB_BYTES, ObjectRef, ObjectType, SegmentIndexEntry, VolumeRoot, decode_object,
};
use crate::identity::ObjectId;
use crate::persistent_map::{MapObjectStore, PersistentBTree, PersistentMapError};
use std::collections::HashMap;
use thiserror::Error;

/// Errors found while proving that a Generation resolves to a complete object graph.
#[derive(Debug, Error)]
pub enum GraphError {
    /// A canonical object or typed edge was invalid.
    #[error(transparent)]
    Format(#[from] FormatError),
    /// A persistent index could not be traversed or validated.
    #[error(transparent)]
    Map(#[from] PersistentMapError),
    /// The immutable object repository failed.
    #[error("object repository error: {0}")]
    Store(String),
    /// A required referenced object was absent.
    #[error("required object {0} is missing")]
    MissingObject(ObjectId),
    /// The graph is well-formed at the byte level but violates a view invariant.
    #[error("invalid Generation dependency graph: {0}")]
    InvalidGraph(&'static str),
}

/// A proof that a complete Generation descriptor and all of its strong edges
/// were read, checksummed, typed, and validated bottom-up.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyProof {
    descriptor: ObjectRef,
    capture_mode: CaptureMode,
    object_refs: Vec<ObjectRef>,
}

impl DependencyProof {
    /// Returns the verified Generation descriptor object.
    pub const fn descriptor(&self) -> ObjectRef {
        self.descriptor
    }

    /// Returns the exact capture mode recorded in the verified descriptor.
    pub const fn capture_mode(&self) -> CaptureMode {
        self.capture_mode
    }

    /// Returns all verified objects in deterministic object-ID order.
    pub fn objects(&self) -> &[ObjectRef] {
        &self.object_refs
    }
}

/// Verifies a complete Generation graph using a trusted immutable-object reader.
///
/// The descriptor's parent Generation ID is validated as lineage metadata only;
/// it is deliberately not fetched or used to resolve the state.
pub fn verify_generation<S: MapObjectStore>(
    store: &S,
    descriptor_ref: ObjectRef,
) -> Result<DependencyProof, GraphError> {
    if descriptor_ref.object_type != ObjectType::GenerationDescriptor {
        return Err(FormatError::ObjectReferenceMismatch.into());
    }
    let mut verifier = Verifier {
        store,
        objects: HashMap::new(),
    };
    let descriptor_bytes = verifier.load(descriptor_ref)?;
    let descriptor = GenerationDescriptor::decode(&descriptor_bytes)?;

    verifier.verify_volume(descriptor.volume_root)?;
    verifier.verify_logs(descriptor.logs_root)?;
    verifier.verify_artifacts(descriptor.artifacts_root)?;

    let mut object_refs: Vec<_> = verifier.objects.into_values().collect();
    object_refs.sort_unstable_by_key(|reference| reference.object_id);
    Ok(DependencyProof {
        descriptor: descriptor_ref,
        capture_mode: descriptor.capture_mode,
        object_refs,
    })
}

struct Verifier<'a, S> {
    store: &'a S,
    objects: HashMap<ObjectId, ObjectRef>,
}

impl<S: MapObjectStore> Verifier<'_, S> {
    fn load(&mut self, reference: ObjectRef) -> Result<Vec<u8>, GraphError> {
        self.record(reference)?;
        let bytes = self
            .store
            .get(reference.object_id)
            .map_err(GraphError::Store)?
            .ok_or(GraphError::MissingObject(reference.object_id))?;
        reference.verify(&bytes)?;
        Ok(bytes)
    }

    fn record(&mut self, reference: ObjectRef) -> Result<(), GraphError> {
        match self.objects.get(&reference.object_id) {
            Some(known) if *known != reference => Err(FormatError::ObjectReferenceMismatch.into()),
            Some(_) => Ok(()),
            None => {
                self.objects.insert(reference.object_id, reference);
                Ok(())
            }
        }
    }

    fn verify_volume(&mut self, root_ref: ObjectRef) -> Result<(), GraphError> {
        let root = VolumeRoot::decode(&self.load(root_ref)?)?;
        let map = PersistentBTree::from_root(root.extent_map_root)?;
        let (entries, nodes) = map.entries_and_nodes(self.store)?;
        for node in nodes {
            // The map traversal has already fetched, checksummed, and
            // structurally validated each node. Keep only its reference so
            // proving a large volume does not retain every node's bytes.
            self.record(node)?;
        }
        let extent_count = root.geometry.capacity_bytes / u64::from(root.geometry.extent_bytes);
        for (key, value) in entries {
            let extent_number = decode_u64_key(&key)?;
            if extent_number >= extent_count {
                return Err(GraphError::InvalidGraph(
                    "volume extent index exceeds fixed capacity",
                ));
            }
            let extent_ref = decode_object_ref(&value)?;
            if extent_ref.object_type != ObjectType::Extent
                || extent_ref.encoded_len
                    != (crate::format::ENVELOPE_HEADER_BYTES + EXTENT_BYTES as usize) as u64
            {
                return Err(FormatError::ObjectReferenceMismatch.into());
            }
            let bytes = self.load(extent_ref)?;
            let object = decode_object(&bytes)?;
            if object.payload.len() != EXTENT_BYTES as usize {
                return Err(GraphError::InvalidGraph(
                    "allocated volume extent does not contain exactly 64 KiB",
                ));
            }
        }
        Ok(())
    }

    fn verify_logs(&mut self, root_ref: ObjectRef) -> Result<(), GraphError> {
        let root = LogMapRoot::decode(&self.load(root_ref)?)?;
        let map = PersistentBTree::from_root(root.log_index_root)?;
        let (logs, map_nodes) = map.entries_and_nodes(self.store)?;
        if logs.len() as u64 != root.log_count {
            return Err(GraphError::InvalidGraph(
                "log count differs from root metadata",
            ));
        }
        for node in map_nodes {
            self.record(node)?;
        }
        for (name, value) in logs {
            if name.is_empty() || std::str::from_utf8(&name).is_err() {
                return Err(GraphError::InvalidGraph("log name is not non-empty UTF-8"));
            }
            let log = LogMapEntry::decode(&value)?;
            self.verify_log(log)?;
        }
        Ok(())
    }

    fn verify_log(&mut self, log: LogMapEntry) -> Result<(), GraphError> {
        let index = PersistentBTree::from_root(log.segment_index_root)?;
        let (segments, index_nodes) = index.entries_and_nodes(self.store)?;
        for node in index_nodes {
            self.record(node)?;
        }
        let mut next_seq = log.first_seq;
        for (key, value) in segments {
            let first_seq = decode_u64_key(&key)?;
            let segment_index = SegmentIndexEntry::decode(&value)?;
            if first_seq != next_seq || segment_index.end_seq > log.end_seq {
                return Err(GraphError::InvalidGraph(
                    "log segment index contains a gap, overlap, or out-of-range segment",
                ));
            }
            let segment_bytes = self.load(segment_index.segment)?;
            let segment = LogSegment::decode(&segment_bytes)?;
            if segment.log_id != log.log_id
                || segment.first_seq != first_seq
                || segment.end_seq != segment_index.end_seq
            {
                return Err(GraphError::InvalidGraph(
                    "log segment identity or range differs from its index",
                ));
            }
            for attachment in segment.attachments {
                self.verify_payload_object(attachment)?;
            }
            next_seq = segment_index.end_seq;
        }
        if next_seq != log.end_seq {
            return Err(GraphError::InvalidGraph(
                "log segment index does not cover its declared visible range",
            ));
        }
        Ok(())
    }

    fn verify_artifacts(&mut self, root_ref: ObjectRef) -> Result<(), GraphError> {
        let root = ArtifactMapRoot::decode(&self.load(root_ref)?)?;
        let map = PersistentBTree::from_root(root.artifact_index_root)?;
        let (artifacts, map_nodes) = map.entries_and_nodes(self.store)?;
        if artifacts.len() as u64 != root.artifact_count {
            return Err(GraphError::InvalidGraph(
                "artifact count differs from root metadata",
            ));
        }
        for node in map_nodes {
            self.record(node)?;
        }
        for (name, value) in artifacts {
            if name.is_empty() || std::str::from_utf8(&name).is_err() {
                return Err(GraphError::InvalidGraph(
                    "artifact name is not non-empty UTF-8",
                ));
            }
            let descriptor = ArtifactDescriptor::decode(&value)?;
            self.verify_artifact(descriptor)?;
        }
        Ok(())
    }

    fn verify_artifact(&mut self, artifact: ArtifactDescriptor) -> Result<(), GraphError> {
        if artifact.logical_len <= MAX_ARTIFACT_BLOB_BYTES as u64 {
            let object_bytes = self.load(artifact.content_root)?;
            let decoded = decode_object(&object_bytes)?;
            if decoded.object_type != ObjectType::ArtifactBlob
                || decoded.payload.len() > MAX_ARTIFACT_BLOB_BYTES
                || decoded.payload.len() as u64 != artifact.logical_len
            {
                return Err(GraphError::InvalidGraph(
                    "inline artifact type or length differs from its payload",
                ));
            }
        } else {
            let index = PersistentBTree::from_root(artifact.content_root)?;
            let (chunks, index_nodes) = index.entries_and_nodes(self.store)?;
            for node in index_nodes {
                self.record(node)?;
            }
            if chunks.is_empty() {
                return Err(GraphError::InvalidGraph("large artifact has no chunks"));
            }
            let mut next_chunk = 0_u64;
            let mut total_len = 0_u64;
            for (key, value) in chunks {
                let chunk_number = decode_u64_key(&key)?;
                let entry = ArtifactChunkEntry::decode(&value)?;
                if chunk_number != next_chunk {
                    return Err(GraphError::InvalidGraph(
                        "artifact chunk index is not consecutive from zero",
                    ));
                }
                let chunk_bytes = self.load(entry.chunk)?;
                let object = decode_object(&chunk_bytes)?;
                if object.payload.len() != entry.actual_len as usize {
                    return Err(GraphError::InvalidGraph(
                        "artifact chunk length differs from its index",
                    ));
                }
                let is_last = next_chunk + 1 == count_artifact_chunks(artifact.logical_len)?;
                if !is_last && object.payload.len() != ARTIFACT_CHUNK_BYTES {
                    return Err(GraphError::InvalidGraph(
                        "non-final artifact chunk is not exactly 4 MiB",
                    ));
                }
                total_len = total_len
                    .checked_add(object.payload.len() as u64)
                    .ok_or(GraphError::InvalidGraph("artifact length overflow"))?;
                next_chunk = next_chunk
                    .checked_add(1)
                    .ok_or(GraphError::InvalidGraph("artifact chunk count overflow"))?;
            }
            if total_len != artifact.logical_len
                || next_chunk != count_artifact_chunks(artifact.logical_len)?
            {
                return Err(GraphError::InvalidGraph(
                    "artifact chunks do not cover the declared logical length",
                ));
            }
        }
        for dependency in artifact.dependencies {
            self.verify_payload_object(dependency)?;
        }
        Ok(())
    }

    fn verify_payload_object(&mut self, reference: ObjectRef) -> Result<(), GraphError> {
        if !matches!(
            reference.object_type,
            ObjectType::ArtifactBlob | ObjectType::ArtifactChunk
        ) {
            return Err(GraphError::InvalidGraph(
                "log attachments and artifact dependencies must reference payload objects",
            ));
        }
        let bytes = self.load(reference)?;
        let object = decode_object(&bytes)?;
        if object.object_type == ObjectType::ArtifactBlob
            && object.payload.len() > MAX_ARTIFACT_BLOB_BYTES
        {
            return Err(GraphError::InvalidGraph("artifact blob exceeds 64 KiB"));
        }
        if object.object_type == ObjectType::ArtifactChunk
            && (object.payload.is_empty() || object.payload.len() > ARTIFACT_CHUNK_BYTES)
        {
            return Err(GraphError::InvalidGraph("artifact chunk length is invalid"));
        }
        Ok(())
    }
}

fn decode_u64_key(key: &[u8]) -> Result<u64, FormatError> {
    let bytes: [u8; 8] = key
        .try_into()
        .map_err(|_| FormatError::InvalidNativeView("numeric map key must be 8 bytes"))?;
    Ok(u64::from_be_bytes(bytes))
}

fn decode_object_ref(bytes: &[u8]) -> Result<ObjectRef, FormatError> {
    let mut decoder = crate::format::CanonicalDecoder::new(bytes);
    let reference = decoder.object_ref()?;
    decoder.finish()?;
    Ok(reference)
}

fn count_artifact_chunks(logical_len: u64) -> Result<u64, GraphError> {
    let chunk_bytes = ARTIFACT_CHUNK_BYTES as u64;
    logical_len
        .checked_add(chunk_bytes - 1)
        .map(|rounded| rounded / chunk_bytes)
        .ok_or(GraphError::InvalidGraph("artifact chunk count overflow"))
}

#[cfg(test)]
mod tests {
    use super::{GraphError, verify_generation};
    use crate::format::{
        ARTIFACT_CHUNK_BYTES, ArtifactChunkEntry, ArtifactDescriptor, CaptureMode,
        GenerationDescriptor, LogMapEntry, LogSegment, ObjectRef, ObjectType, SegmentIndexEntry,
        VolumeGeometry, encode_object,
    };
    use crate::identity::{ArtifactId, GenerationId, LogId, ObjectId, VolumeBoundaryId};
    use crate::persistent_map::MapObjectStore;
    use crate::views::{
        ArtifactChunkIndex, ArtifactMap, LogMap, LogSegmentIndex, VolumeExtentMap,
        encode_artifact_blob, encode_artifact_chunk,
    };
    use std::collections::HashMap;

    #[derive(Default)]
    struct MemoryStore(HashMap<ObjectId, Vec<u8>>);

    impl MapObjectStore for MemoryStore {
        fn get(&self, id: ObjectId) -> Result<Option<Vec<u8>>, String> {
            Ok(self.0.get(&id).cloned())
        }

        fn put(&mut self, id: ObjectId, bytes: Vec<u8>) -> Result<(), String> {
            self.0.insert(id, bytes);
            Ok(())
        }
    }

    fn store_object(store: &mut MemoryStore, bytes: Vec<u8>) -> ObjectRef {
        let reference = ObjectRef::from_encoded(&bytes).unwrap();
        store.put(reference.object_id, bytes).unwrap();
        reference
    }

    fn empty_generation(
        store: &mut MemoryStore,
        generation_id: GenerationId,
        parent: Option<GenerationId>,
    ) -> ObjectRef {
        let geometry = VolumeGeometry::new(4 * 1024 * 1024).unwrap();
        let volume_map = VolumeExtentMap::empty(geometry, store).unwrap();
        let volume_root = volume_map.seal(store).unwrap();
        let logs = LogMap::empty(store).unwrap().seal(store).unwrap();
        let artifacts = ArtifactMap::empty(store).unwrap().seal(store).unwrap();
        let descriptor = GenerationDescriptor {
            generation_id,
            parent_generation_id: parent,
            volume_root,
            logs_root: logs,
            artifacts_root: artifacts,
            capture_mode: CaptureMode::BlockCut,
            volume_boundary_id: VolumeBoundaryId::from_bytes([8; 16]),
            created_at_unix_ms: 10,
        };
        store_object(store, descriptor.encode().unwrap())
    }

    struct DenyObjectStore<'a> {
        inner: &'a MemoryStore,
        denied: ObjectId,
    }

    impl MapObjectStore for DenyObjectStore<'_> {
        fn get(&self, id: ObjectId) -> Result<Option<Vec<u8>>, String> {
            if id == self.denied {
                return Err("ancestor descriptor reads are denied".to_owned());
            }
            self.inner.get(id)
        }

        fn put(&mut self, _id: ObjectId, _bytes: Vec<u8>) -> Result<(), String> {
            Err("read-only test store".to_owned())
        }
    }

    #[test]
    fn complete_generation_proof_does_not_fetch_parent_lineage() {
        let mut store = MemoryStore::default();
        let parent_id = GenerationId::from_bytes([0xAA; 16]);
        let parent_ref = empty_generation(&mut store, parent_id, None);
        let descriptor_ref = empty_generation(
            &mut store,
            GenerationId::from_bytes([7; 16]),
            Some(parent_id),
        );
        let restricted = DenyObjectStore {
            inner: &store,
            denied: parent_ref.object_id,
        };
        let proof = verify_generation(&restricted, descriptor_ref).unwrap();
        assert_eq!(proof.descriptor(), descriptor_ref);
        assert_eq!(proof.capture_mode(), CaptureMode::BlockCut);
        assert_eq!(proof.objects().len(), 7);
        assert!(
            !proof
                .objects()
                .iter()
                .any(|reference| *reference == parent_ref)
        );
    }

    #[test]
    fn volume_graph_checks_extent_geometry_and_missing_allocated_data() {
        let mut store = MemoryStore::default();
        let geometry = VolumeGeometry::new(4 * 1024 * 1024).unwrap();
        let empty = VolumeExtentMap::empty(geometry, &mut store).unwrap();
        assert!(empty.extent(&store, 0).unwrap().is_none());
        assert!(empty.extent(&store, 64).is_err());

        let extent = store_object(
            &mut store,
            encode_object(ObjectType::Extent, &vec![0xAB; 64 * 1024]).unwrap(),
        );
        let populated = empty.insert_extent(&mut store, 0, extent).unwrap();
        let volume_root = populated.seal(&mut store).unwrap();
        let logs = LogMap::empty(&mut store).unwrap().seal(&mut store).unwrap();
        let artifacts = ArtifactMap::empty(&mut store)
            .unwrap()
            .seal(&mut store)
            .unwrap();
        let descriptor = GenerationDescriptor {
            generation_id: GenerationId::from_bytes([1; 16]),
            parent_generation_id: None,
            volume_root,
            logs_root: logs,
            artifacts_root: artifacts,
            capture_mode: CaptureMode::FilesystemFrozen,
            volume_boundary_id: VolumeBoundaryId::from_bytes([2; 16]),
            created_at_unix_ms: 20,
        };
        let descriptor_ref = store_object(&mut store, descriptor.encode().unwrap());
        assert!(verify_generation(&store, descriptor_ref).is_ok());

        store.0.remove(&extent.object_id);
        assert!(matches!(
            verify_generation(&store, descriptor_ref),
            Err(GraphError::MissingObject(id)) if id == extent.object_id
        ));
    }

    #[test]
    fn logs_and_artifacts_are_strong_typed_edges_in_the_generation_graph() {
        let mut store = MemoryStore::default();
        let geometry = VolumeGeometry::new(4 * 1024 * 1024).unwrap();
        let volume_root = VolumeExtentMap::empty(geometry, &mut store)
            .unwrap()
            .seal(&mut store)
            .unwrap();

        let log_id = LogId::from_bytes([3; 16]);
        let artifact_blob = store_object(&mut store, encode_artifact_blob(b"artifact").unwrap());
        let segment = LogSegment {
            log_id,
            first_seq: 10,
            end_seq: 11,
            records: vec![b"record".to_vec()],
            attachments: vec![artifact_blob],
        };
        let segment_ref = store_object(&mut store, segment.encode().unwrap());
        let segment_index = LogSegmentIndex::empty(&mut store)
            .unwrap()
            .insert(
                &mut store,
                10,
                SegmentIndexEntry {
                    end_seq: 11,
                    segment: segment_ref,
                },
            )
            .unwrap();
        let logs = LogMap::empty(&mut store)
            .unwrap()
            .insert(
                &mut store,
                "events",
                LogMapEntry {
                    log_id,
                    first_seq: 10,
                    end_seq: 11,
                    segment_index_root: segment_index.root(),
                },
            )
            .unwrap()
            .seal(&mut store)
            .unwrap();

        let artifact_chunk_index = ArtifactChunkIndex::empty(&mut store).unwrap();
        let large_payload = vec![0xCD; ARTIFACT_CHUNK_BYTES + 13];
        let mut index = artifact_chunk_index;
        for (number, chunk) in large_payload.chunks(ARTIFACT_CHUNK_BYTES).enumerate() {
            let chunk_ref = store_object(&mut store, encode_artifact_chunk(chunk).unwrap());
            index = index
                .insert(
                    &mut store,
                    number as u64,
                    ArtifactChunkEntry {
                        actual_len: chunk.len() as u32,
                        chunk: chunk_ref,
                    },
                )
                .unwrap();
        }
        let artifacts = ArtifactMap::empty(&mut store)
            .unwrap()
            .insert(
                &mut store,
                "build/output",
                &ArtifactDescriptor {
                    artifact_id: ArtifactId::from_bytes([4; 16]),
                    logical_len: large_payload.len() as u64,
                    content_root: index.root(),
                    dependencies: vec![artifact_blob],
                },
            )
            .unwrap()
            .seal(&mut store)
            .unwrap();

        let descriptor = GenerationDescriptor {
            generation_id: GenerationId::from_bytes([5; 16]),
            parent_generation_id: None,
            volume_root,
            logs_root: logs,
            artifacts_root: artifacts,
            capture_mode: CaptureMode::FilesystemCleanUnmount,
            volume_boundary_id: VolumeBoundaryId::from_bytes([6; 16]),
            created_at_unix_ms: 30,
        };
        let descriptor_ref = store_object(&mut store, descriptor.encode().unwrap());
        let proof = verify_generation(&store, descriptor_ref).unwrap();
        assert!(proof.objects().iter().any(|object| object == &segment_ref));
        assert!(
            proof
                .objects()
                .iter()
                .any(|object| object == &artifact_blob)
        );
    }

    #[test]
    fn rejects_sparse_artifact_chunk_indexes() {
        let mut store = MemoryStore::default();
        let descriptor_ref = empty_generation(&mut store, GenerationId::from_bytes([7; 16]), None);
        let descriptor =
            GenerationDescriptor::decode(&store.0.get(&descriptor_ref.object_id).unwrap()).unwrap();
        let mut artifact_map = ArtifactMap::empty(&mut store).unwrap();
        let chunk = store_object(&mut store, encode_artifact_chunk(b"short").unwrap());
        let chunks = ArtifactChunkIndex::empty(&mut store)
            .unwrap()
            .insert(
                &mut store,
                1,
                ArtifactChunkEntry {
                    actual_len: 5,
                    chunk,
                },
            )
            .unwrap();
        artifact_map = artifact_map
            .insert(
                &mut store,
                "bad",
                &ArtifactDescriptor {
                    artifact_id: ArtifactId::from_bytes([9; 16]),
                    logical_len: (ARTIFACT_CHUNK_BYTES + 5) as u64,
                    content_root: chunks.root(),
                    dependencies: Vec::new(),
                },
            )
            .unwrap();
        let artifacts = artifact_map.seal(&mut store).unwrap();
        let invalid_descriptor = GenerationDescriptor {
            artifacts_root: artifacts,
            ..descriptor
        };
        let invalid_ref = store_object(&mut store, invalid_descriptor.encode().unwrap());
        assert!(matches!(
            verify_generation(&store, invalid_ref),
            Err(GraphError::InvalidGraph(_))
        ));
    }
}
