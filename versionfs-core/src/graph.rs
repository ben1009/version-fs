//! Trusted verification of complete immutable Generation dependency graphs.

use crate::format::{
    ARTIFACT_CHUNK_BYTES, ArtifactChunkEntry, ArtifactDescriptor, ArtifactMapRoot, CaptureMode,
    EXTENT_BYTES, FormatError, GenerationDescriptor, LogMapEntry, LogMapRoot, LogSegment,
    MAX_ARTIFACT_BLOB_BYTES, ObjectRef, ObjectType, SegmentIndexEntry, VolumeRoot, decode_object,
};
use crate::identity::{LogId, ObjectId, ObjectStoreScopeId};
use crate::persistent_map::{MapObjectStore, Node as MapNode, PersistentMapError, load_node};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, OnceLock, Weak};
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
#[derive(Clone)]
pub struct DependencyProof {
    descriptor: ObjectRef,
    generation_descriptor: GenerationDescriptor,
    root: Arc<VerifiedNode>,
    index: Arc<ProofIndex>,
    store_scope: ObjectStoreScopeId,
    object_refs: Arc<OnceLock<Vec<ObjectRef>>>,
}

impl std::fmt::Debug for DependencyProof {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DependencyProof")
            .field("descriptor", &self.descriptor)
            .field("capture_mode", &self.generation_descriptor.capture_mode)
            .field("store_scope", &self.store_scope)
            .finish_non_exhaustive()
    }
}

impl DependencyProof {
    /// Returns the immutable-object namespace in which the graph was verified.
    pub const fn store_scope(&self) -> ObjectStoreScopeId {
        self.store_scope
    }

    /// Returns the verified Generation descriptor object.
    pub const fn descriptor(&self) -> ObjectRef {
        self.descriptor
    }

    /// Returns the exact capture mode recorded in the verified descriptor.
    pub const fn capture_mode(&self) -> CaptureMode {
        self.generation_descriptor.capture_mode
    }

    /// Returns the exact descriptor payload validated by this proof.
    pub const fn generation_descriptor(&self) -> GenerationDescriptor {
        self.generation_descriptor
    }

    /// Returns the exact strong dependency closure in deterministic ID order.
    ///
    /// This materializes the proof DAG and is intended for inspection or
    /// reachability reporting, not for the checkpoint fast path.
    pub fn objects(&self) -> &[ObjectRef] {
        self.object_refs
            .get_or_init(|| {
                let mut found = HashMap::<ObjectId, ObjectRef>::new();
                let mut pending = vec![Arc::clone(&self.root)];
                while let Some(node) = pending.pop() {
                    match found.get(&node.reference.object_id) {
                        Some(existing) if *existing != node.reference => {
                            // A constructed proof cannot contain conflicting references;
                            // keep the first so this inspection API remains infallible.
                        }
                        Some(_) => continue,
                        None => {
                            found.insert(node.reference.object_id, node.reference);
                        }
                    }
                    pending.extend(node.children.iter().cloned());
                }
                let mut objects: Vec<_> = found.into_values().collect();
                objects.sort_unstable_by_key(|reference| reference.object_id);
                objects
            })
            .as_slice()
    }
}

type ProofIndex = HashMap<ObjectId, Weak<VerifiedNode>>;

#[derive(Clone, Debug)]
struct VerifiedNode {
    reference: ObjectRef,
    summary: VerifiedSummary,
    children: Vec<Arc<VerifiedNode>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum VerifiedSummary {
    VolumeRoot,
    LogRoot,
    ArtifactRoot,
    Map(MapSummary),
    LogSegment {
        log_id: LogId,
        first_seq: u64,
        end_seq: u64,
    },
    Descriptor {
        descriptor: GenerationDescriptor,
    },
    Payload {
        payload_len: u64,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct MapSummary {
    object_type: ObjectType,
    level: u16,
    min_key: Option<Vec<u8>>,
    max_key: Option<Vec<u8>>,
    entry_count: u64,
    shape: MapShape,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum MapShape {
    Volume {
        max_extent: Option<u64>,
    },
    LogNames,
    LogSegments {
        first_seq: Option<u64>,
        end_seq: Option<u64>,
        log_id: Option<LogId>,
    },
    ArtifactNames,
    ArtifactChunks {
        first_chunk: Option<u64>,
        next_chunk: Option<u64>,
        total_len: u64,
        last_chunk_len: u32,
    },
}

impl MapSummary {
    fn empty(object_type: ObjectType, level: u16) -> Result<Self, GraphError> {
        Ok(Self {
            object_type,
            level,
            min_key: None,
            max_key: None,
            entry_count: 0,
            shape: match object_type {
                ObjectType::VolumeMapNode => MapShape::Volume { max_extent: None },
                ObjectType::LogMapNode => MapShape::LogNames,
                ObjectType::LogSegmentIndexNode => MapShape::LogSegments {
                    first_seq: None,
                    end_seq: None,
                    log_id: None,
                },
                ObjectType::ArtifactMapNode => MapShape::ArtifactNames,
                ObjectType::ArtifactIndexNode => MapShape::ArtifactChunks {
                    first_chunk: None,
                    next_chunk: None,
                    total_len: 0,
                    last_chunk_len: 0,
                },
                _ => return Err(FormatError::ObjectReferenceMismatch.into()),
            },
        })
    }

    fn for_leaf(object_type: ObjectType, entries: &[(Vec<u8>, Vec<u8>)], shape: MapShape) -> Self {
        Self {
            object_type,
            level: 0,
            min_key: entries.first().map(|entry| entry.0.clone()),
            max_key: entries.last().map(|entry| entry.0.clone()),
            entry_count: entries.len() as u64,
            shape,
        }
    }

    fn add_child(&mut self, child: &Self) -> Result<(), GraphError> {
        if child.object_type != self.object_type || child.level.checked_add(1) != Some(self.level) {
            return Err(GraphError::InvalidGraph(
                "persistent map child type or level differs from its parent",
            ));
        }
        if let (Some(previous_max), Some(child_min)) = (&self.max_key, &child.min_key)
            && child_min <= previous_max
        {
            return Err(GraphError::InvalidGraph(
                "persistent map child key ranges overlap or are unordered",
            ));
        }
        self.entry_count =
            self.entry_count
                .checked_add(child.entry_count)
                .ok_or(GraphError::InvalidGraph(
                    "persistent map entry count overflow",
                ))?;
        if self.min_key.is_none() {
            self.min_key.clone_from(&child.min_key);
        }
        if child.max_key.is_some() {
            self.max_key.clone_from(&child.max_key);
        }
        self.shape = combine_map_shape(&self.shape, &child.shape)?;
        Ok(())
    }
}

fn combine_map_shape(left: &MapShape, right: &MapShape) -> Result<MapShape, GraphError> {
    match (left, right) {
        (MapShape::Volume { max_extent: left }, MapShape::Volume { max_extent: right }) => {
            Ok(MapShape::Volume {
                max_extent: match (left, right) {
                    (Some(left), Some(right)) => Some((*left).max(*right)),
                    (Some(value), None) | (None, Some(value)) => Some(*value),
                    (None, None) => None,
                },
            })
        }
        (MapShape::LogNames, MapShape::LogNames) => Ok(MapShape::LogNames),
        (
            MapShape::LogSegments {
                first_seq: left_first,
                end_seq: left_end,
                log_id: left_id,
            },
            MapShape::LogSegments {
                first_seq: right_first,
                end_seq: right_end,
                log_id: right_id,
            },
        ) => match (
            left_first,
            left_end,
            left_id,
            right_first,
            right_end,
            right_id,
        ) {
            (None, None, None, None, None, None) => Ok(MapShape::LogSegments {
                first_seq: None,
                end_seq: None,
                log_id: None,
            }),
            (Some(first), Some(left_end), Some(left_id), None, None, None) => {
                Ok(MapShape::LogSegments {
                    first_seq: Some(*first),
                    end_seq: Some(*left_end),
                    log_id: Some(*left_id),
                })
            }
            (None, None, None, Some(right_first), Some(end), Some(id)) => {
                Ok(MapShape::LogSegments {
                    first_seq: Some(*right_first),
                    end_seq: Some(*end),
                    log_id: Some(*id),
                })
            }
            (
                Some(first),
                Some(left_end),
                Some(left_id),
                Some(right_first),
                Some(right_end),
                Some(right_id),
            ) if left_end == right_first && left_id == right_id => Ok(MapShape::LogSegments {
                first_seq: Some(*first),
                end_seq: Some(*right_end),
                log_id: Some(*left_id),
            }),
            _ => Err(GraphError::InvalidGraph(
                "log segment subtrees have a gap, overlap, or mixed log identity",
            )),
        },
        (MapShape::ArtifactNames, MapShape::ArtifactNames) => Ok(MapShape::ArtifactNames),
        (
            MapShape::ArtifactChunks {
                first_chunk: left_first,
                next_chunk: left_next,
                total_len: left_total,
                last_chunk_len: left_last_len,
            },
            MapShape::ArtifactChunks {
                first_chunk: right_first,
                next_chunk: right_next,
                total_len: right_total,
                last_chunk_len: right_last_len,
            },
        ) => {
            let (first_chunk, next_chunk, last_chunk_len) =
                match (left_first, left_next, right_first, right_next) {
                    (None, None, None, None) => (None, None, 0),
                    (Some(first), Some(next), None, None) => {
                        (Some(*first), Some(*next), *left_last_len)
                    }
                    (None, None, Some(first), Some(next)) => {
                        (Some(*first), Some(*next), *right_last_len)
                    }
                    (Some(first), Some(left_next), Some(right_first), Some(right_next))
                        if left_next == right_first
                            && *left_last_len as usize == ARTIFACT_CHUNK_BYTES =>
                    {
                        (Some(*first), Some(*right_next), *right_last_len)
                    }
                    _ => {
                        return Err(GraphError::InvalidGraph(
                            "artifact chunk subtrees have a gap, overlap, or short non-final chunk",
                        ));
                    }
                };
            Ok(MapShape::ArtifactChunks {
                first_chunk,
                next_chunk,
                total_len: left_total
                    .checked_add(*right_total)
                    .ok_or(GraphError::InvalidGraph("artifact length overflow"))?,
                last_chunk_len,
            })
        }
        _ => Err(GraphError::InvalidGraph(
            "persistent map child has an incompatible view summary",
        )),
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
    verify_generation_with_reuse(store, descriptor_ref, &[])
}

/// Verifies a Generation while reusing verified immutable subgraphs from roots
/// that the caller is still protecting.
///
/// The resulting dependency closure is derived only from `descriptor_ref`;
/// references that are reachable only from a protected base proof are not
/// retained. The caller must keep each supplied proof's root protected until
/// this call has returned a terminal result.
///
/// Each proof indexes nodes visited by its verification, including reused
/// subtree roots. Descendant indexes are not copied from protected proofs;
/// cache misses read and validate the required objects normally. Enumerating
/// the complete dependency closure is reserved for [`DependencyProof::objects`].
pub fn verify_generation_with_reuse<S: MapObjectStore>(
    store: &S,
    descriptor_ref: ObjectRef,
    protected_proofs: &[&DependencyProof],
) -> Result<DependencyProof, GraphError> {
    if descriptor_ref.object_type != ObjectType::GenerationDescriptor {
        return Err(FormatError::ObjectReferenceMismatch.into());
    }
    let store_scope = store.proof_scope();
    let mut trusted_indexes = Vec::new();
    let mut seen_indexes = HashSet::new();
    for proof in protected_proofs {
        if proof.store_scope != store_scope {
            return Err(GraphError::InvalidGraph(
                "protected proof belongs to a different object-store scope",
            ));
        }
        let identity = Arc::as_ptr(&proof.index) as usize;
        if seen_indexes.insert(identity) {
            trusted_indexes.push(Arc::clone(&proof.index));
        }
    }
    if let Some(proof) = protected_proofs
        .iter()
        .find(|proof| proof.descriptor == descriptor_ref)
    {
        return Ok((**proof).clone());
    }

    let mut verifier = Verifier {
        store,
        trusted_indexes,
        local: HashMap::new(),
        in_progress: HashSet::new(),
    };
    let descriptor_node = verifier.verify_descriptor(descriptor_ref)?;
    let generation_descriptor = match &descriptor_node.summary {
        VerifiedSummary::Descriptor { descriptor } => *descriptor,
        _ => return Err(FormatError::ObjectReferenceMismatch.into()),
    };

    let index = Arc::new(
        verifier
            .local
            .into_iter()
            .map(|(id, node)| (id, Arc::downgrade(&node)))
            .collect(),
    );

    Ok(DependencyProof {
        descriptor: descriptor_ref,
        generation_descriptor,
        root: descriptor_node,
        index,
        store_scope,
        object_refs: Arc::new(OnceLock::new()),
    })
}

struct Verifier<'a, S> {
    store: &'a S,
    trusted_indexes: Vec<Arc<ProofIndex>>,
    local: HashMap<ObjectId, Arc<VerifiedNode>>,
    in_progress: HashSet<ObjectId>,
}

impl<S: MapObjectStore> Verifier<'_, S> {
    fn reusable(&mut self, reference: ObjectRef) -> Result<Option<Arc<VerifiedNode>>, GraphError> {
        if reference.object_type == ObjectType::GenerationDescriptor {
            reference.validate()?;
        }
        if let Some(node) = self.local.get(&reference.object_id) {
            if node.reference != reference {
                return Err(FormatError::ObjectReferenceMismatch.into());
            }
            return Ok(Some(Arc::clone(node)));
        }
        for index in self.trusted_indexes.iter().rev() {
            if let Some(node) = index.get(&reference.object_id).and_then(Weak::upgrade) {
                if node.reference != reference {
                    return Err(FormatError::ObjectReferenceMismatch.into());
                }
                self.local.insert(reference.object_id, Arc::clone(&node));
                return Ok(Some(node));
            }
        }
        Ok(None)
    }

    fn begin(&mut self, reference: ObjectRef) -> Result<(), GraphError> {
        reference.validate()?;
        if !self.in_progress.insert(reference.object_id) {
            return Err(GraphError::InvalidGraph(
                "immutable object graph contains a cycle",
            ));
        }
        Ok(())
    }

    fn finish(
        &mut self,
        reference: ObjectRef,
        summary: VerifiedSummary,
        children: Vec<Arc<VerifiedNode>>,
    ) -> Arc<VerifiedNode> {
        self.in_progress.remove(&reference.object_id);
        let node = Arc::new(VerifiedNode {
            reference,
            summary,
            children,
        });
        self.local.insert(reference.object_id, Arc::clone(&node));
        node
    }

    fn object_bytes(&self, reference: ObjectRef) -> Result<Vec<u8>, GraphError> {
        let bytes = self
            .store
            .get_ref(reference)
            .map_err(GraphError::Store)?
            .ok_or(GraphError::MissingObject(reference.object_id))?;
        reference.verify(&bytes)?;
        Ok(bytes)
    }

    fn verify_descriptor(&mut self, reference: ObjectRef) -> Result<Arc<VerifiedNode>, GraphError> {
        if reference.object_type != ObjectType::GenerationDescriptor {
            return Err(FormatError::ObjectReferenceMismatch.into());
        }
        if let Some(node) = self.reusable(reference)? {
            if matches!(&node.summary, VerifiedSummary::Descriptor { .. }) {
                return Ok(node);
            }
            return Err(FormatError::ObjectReferenceMismatch.into());
        }
        self.begin(reference)?;
        let descriptor = GenerationDescriptor::decode(&self.object_bytes(reference)?)?;
        let volume = self.verify_volume(descriptor.volume_root)?;
        let logs = self.verify_logs(descriptor.logs_root)?;
        let artifacts = self.verify_artifacts(descriptor.artifacts_root)?;
        Ok(self.finish(
            reference,
            VerifiedSummary::Descriptor { descriptor },
            vec![volume, logs, artifacts],
        ))
    }

    fn verify_volume(&mut self, reference: ObjectRef) -> Result<Arc<VerifiedNode>, GraphError> {
        if reference.object_type != ObjectType::VolumeRoot {
            return Err(FormatError::ObjectReferenceMismatch.into());
        }
        if let Some(node) = self.reusable(reference)? {
            if matches!(&node.summary, VerifiedSummary::VolumeRoot) {
                return Ok(node);
            }
            return Err(FormatError::ObjectReferenceMismatch.into());
        }
        self.begin(reference)?;
        let root = VolumeRoot::decode(&self.object_bytes(reference)?)?;
        let map = self.verify_map(root.extent_map_root, ObjectType::VolumeMapNode, None)?;
        let summary = map_summary(&map)?;
        let extent_count = root.geometry.capacity_bytes / u64::from(root.geometry.extent_bytes);
        if matches!(summary.shape, MapShape::Volume { max_extent: Some(max) } if max >= extent_count)
        {
            return Err(GraphError::InvalidGraph(
                "volume extent index exceeds fixed capacity",
            ));
        }
        Ok(self.finish(reference, VerifiedSummary::VolumeRoot, vec![map]))
    }

    fn verify_logs(&mut self, reference: ObjectRef) -> Result<Arc<VerifiedNode>, GraphError> {
        if reference.object_type != ObjectType::LogMapRoot {
            return Err(FormatError::ObjectReferenceMismatch.into());
        }
        if let Some(node) = self.reusable(reference)? {
            if matches!(&node.summary, VerifiedSummary::LogRoot) {
                return Ok(node);
            }
            return Err(FormatError::ObjectReferenceMismatch.into());
        }
        self.begin(reference)?;
        let root = LogMapRoot::decode(&self.object_bytes(reference)?)?;
        let map = self.verify_map(root.log_index_root, ObjectType::LogMapNode, None)?;
        let summary = map_summary(&map)?;
        if summary.entry_count != root.log_count {
            return Err(GraphError::InvalidGraph(
                "log count differs from root metadata",
            ));
        }
        Ok(self.finish(reference, VerifiedSummary::LogRoot, vec![map]))
    }

    fn verify_artifacts(&mut self, reference: ObjectRef) -> Result<Arc<VerifiedNode>, GraphError> {
        if reference.object_type != ObjectType::ArtifactMapRoot {
            return Err(FormatError::ObjectReferenceMismatch.into());
        }
        if let Some(node) = self.reusable(reference)? {
            if matches!(&node.summary, VerifiedSummary::ArtifactRoot) {
                return Ok(node);
            }
            return Err(FormatError::ObjectReferenceMismatch.into());
        }
        self.begin(reference)?;
        let root = ArtifactMapRoot::decode(&self.object_bytes(reference)?)?;
        let map = self.verify_map(root.artifact_index_root, ObjectType::ArtifactMapNode, None)?;
        let summary = map_summary(&map)?;
        if summary.entry_count != root.artifact_count {
            return Err(GraphError::InvalidGraph(
                "artifact count differs from root metadata",
            ));
        }
        Ok(self.finish(reference, VerifiedSummary::ArtifactRoot, vec![map]))
    }

    fn verify_map(
        &mut self,
        reference: ObjectRef,
        expected_type: ObjectType,
        expected_level: Option<u16>,
    ) -> Result<Arc<VerifiedNode>, GraphError> {
        if reference.object_type != expected_type {
            return Err(FormatError::ObjectReferenceMismatch.into());
        }
        if let Some(node) = self.reusable(reference)? {
            if let VerifiedSummary::Map(map) = &node.summary
                && map.object_type == expected_type
            {
                if expected_level.is_some_and(|level| level != map.level) {
                    return Err(GraphError::InvalidGraph(
                        "persistent map child level differs from its parent",
                    ));
                }
                return Ok(node);
            }
            return Err(FormatError::ObjectReferenceMismatch.into());
        }
        self.begin(reference)?;
        let map_node = load_node(self.store, reference, expected_type)?;
        if expected_level.is_some_and(|level| level != map_node.level()) {
            return Err(GraphError::InvalidGraph(
                "persistent map child level differs from its parent",
            ));
        }
        let (summary, children) = match map_node {
            MapNode::Leaf(entries) => {
                let mut children = Vec::new();
                let mut shape = MapSummary::empty(expected_type, 0)?.shape;
                for (key, value) in &entries {
                    let (entry_shape, entry_children) =
                        self.verify_map_entry(expected_type, key, value)?;
                    shape = combine_map_shape(&shape, &entry_shape)?;
                    children.extend(entry_children);
                }
                (
                    MapSummary::for_leaf(expected_type, &entries, shape),
                    children,
                )
            }
            MapNode::Internal { level, children } => {
                let mut summary = MapSummary::empty(expected_type, level)?;
                let mut verified_children = Vec::with_capacity(children.len());
                for (separator, child_ref) in children {
                    let child = self.verify_map(child_ref, expected_type, Some(level - 1))?;
                    let child_summary = map_summary(&child)?;
                    if child_summary.level.checked_add(1) != Some(level)
                        || child_summary.max_key.as_deref() != Some(separator.as_slice())
                    {
                        return Err(GraphError::InvalidGraph(
                            "persistent map child level or separator differs from its parent",
                        ));
                    }
                    summary.add_child(&child_summary)?;
                    verified_children.push(child);
                }
                (summary, verified_children)
            }
        };
        Ok(self.finish(reference, VerifiedSummary::Map(summary), children))
    }

    fn verify_map_entry(
        &mut self,
        node_type: ObjectType,
        key: &[u8],
        value: &[u8],
    ) -> Result<(MapShape, Vec<Arc<VerifiedNode>>), GraphError> {
        match node_type {
            ObjectType::VolumeMapNode => {
                let extent_number = decode_u64_key(key)?;
                let extent_ref = decode_object_ref(value)?;
                if extent_ref.object_type != ObjectType::Extent
                    || extent_ref.encoded_len
                        != (crate::format::ENVELOPE_HEADER_BYTES + EXTENT_BYTES as usize) as u64
                {
                    return Err(FormatError::ObjectReferenceMismatch.into());
                }
                let extent = self.verify_payload(extent_ref)?;
                if payload_len(&extent)? != u64::from(EXTENT_BYTES) {
                    return Err(GraphError::InvalidGraph(
                        "allocated volume extent does not contain exactly 64 KiB",
                    ));
                }
                Ok((
                    MapShape::Volume {
                        max_extent: Some(extent_number),
                    },
                    vec![extent],
                ))
            }
            ObjectType::LogMapNode => {
                if key.is_empty() || std::str::from_utf8(key).is_err() {
                    return Err(GraphError::InvalidGraph("log name is not non-empty UTF-8"));
                }
                let log = LogMapEntry::decode(value)?;
                let index = self.verify_log(log)?;
                Ok((MapShape::LogNames, vec![index]))
            }
            ObjectType::LogSegmentIndexNode => {
                let first_seq = decode_u64_key(key)?;
                let entry = SegmentIndexEntry::decode(value)?;
                let segment = self.verify_segment(entry.segment)?;
                let VerifiedSummary::LogSegment {
                    log_id,
                    first_seq: actual_first,
                    end_seq,
                } = &segment.summary
                else {
                    return Err(FormatError::ObjectReferenceMismatch.into());
                };
                if first_seq != *actual_first || entry.end_seq != *end_seq {
                    return Err(GraphError::InvalidGraph(
                        "log segment identity or range differs from its index",
                    ));
                }
                Ok((
                    MapShape::LogSegments {
                        first_seq: Some(first_seq),
                        end_seq: Some(*end_seq),
                        log_id: Some(*log_id),
                    },
                    vec![segment],
                ))
            }
            ObjectType::ArtifactMapNode => {
                if key.is_empty() || std::str::from_utf8(key).is_err() {
                    return Err(GraphError::InvalidGraph(
                        "artifact name is not non-empty UTF-8",
                    ));
                }
                let artifact = ArtifactDescriptor::decode(value)?;
                Ok((MapShape::ArtifactNames, self.verify_artifact(artifact)?))
            }
            ObjectType::ArtifactIndexNode => {
                let chunk_number = decode_u64_key(key)?;
                let entry = ArtifactChunkEntry::decode(value)?;
                let chunk = self.verify_payload(entry.chunk)?;
                if payload_len(&chunk)? != u64::from(entry.actual_len) {
                    return Err(GraphError::InvalidGraph(
                        "artifact chunk length differs from its index",
                    ));
                }
                Ok((
                    MapShape::ArtifactChunks {
                        first_chunk: Some(chunk_number),
                        next_chunk: Some(
                            chunk_number
                                .checked_add(1)
                                .ok_or(GraphError::InvalidGraph("artifact chunk count overflow"))?,
                        ),
                        total_len: u64::from(entry.actual_len),
                        last_chunk_len: entry.actual_len,
                    },
                    vec![chunk],
                ))
            }
            _ => Err(FormatError::ObjectReferenceMismatch.into()),
        }
    }

    fn verify_log(&mut self, log: LogMapEntry) -> Result<Arc<VerifiedNode>, GraphError> {
        let index = self.verify_map(
            log.segment_index_root,
            ObjectType::LogSegmentIndexNode,
            None,
        )?;
        let summary = map_summary(&index)?;
        let MapShape::LogSegments {
            first_seq,
            end_seq,
            log_id,
        } = summary.shape
        else {
            return Err(GraphError::InvalidGraph(
                "log segment map has an incompatible summary",
            ));
        };
        match (first_seq, end_seq, log_id) {
            (None, None, None) if log.first_seq == log.end_seq => {}
            (Some(first), Some(end), Some(id))
                if first == log.first_seq && end == log.end_seq && id == log.log_id => {}
            _ => {
                return Err(GraphError::InvalidGraph(
                    "log segment index does not cover its declared visible range",
                ));
            }
        }
        Ok(index)
    }

    fn verify_segment(&mut self, reference: ObjectRef) -> Result<Arc<VerifiedNode>, GraphError> {
        if reference.object_type != ObjectType::LogSegment {
            return Err(FormatError::ObjectReferenceMismatch.into());
        }
        if let Some(node) = self.reusable(reference)? {
            if matches!(&node.summary, VerifiedSummary::LogSegment { .. }) {
                return Ok(node);
            }
            return Err(FormatError::ObjectReferenceMismatch.into());
        }
        self.begin(reference)?;
        let segment = LogSegment::decode(&self.object_bytes(reference)?)?;
        let mut children = Vec::with_capacity(segment.attachments.len());
        for attachment in segment.attachments {
            children.push(self.verify_payload(attachment)?);
        }
        Ok(self.finish(
            reference,
            VerifiedSummary::LogSegment {
                log_id: segment.log_id,
                first_seq: segment.first_seq,
                end_seq: segment.end_seq,
            },
            children,
        ))
    }

    fn verify_artifact(
        &mut self,
        artifact: ArtifactDescriptor,
    ) -> Result<Vec<Arc<VerifiedNode>>, GraphError> {
        let mut children = Vec::with_capacity(artifact.dependencies.len() + 1);
        if artifact.logical_len <= MAX_ARTIFACT_BLOB_BYTES as u64 {
            let blob = self.verify_payload(artifact.content_root)?;
            if payload_len(&blob)? != artifact.logical_len {
                return Err(GraphError::InvalidGraph(
                    "inline artifact type or length differs from its payload",
                ));
            }
            children.push(blob);
        } else {
            let index =
                self.verify_map(artifact.content_root, ObjectType::ArtifactIndexNode, None)?;
            let summary = map_summary(&index)?;
            let MapShape::ArtifactChunks {
                first_chunk,
                next_chunk,
                total_len,
                last_chunk_len,
            } = summary.shape
            else {
                return Err(GraphError::InvalidGraph(
                    "artifact chunk map has an incompatible summary",
                ));
            };
            let expected_chunks = count_artifact_chunks(artifact.logical_len)?;
            let expected_last = if artifact
                .logical_len
                .is_multiple_of(ARTIFACT_CHUNK_BYTES as u64)
            {
                ARTIFACT_CHUNK_BYTES as u32
            } else {
                (artifact.logical_len % ARTIFACT_CHUNK_BYTES as u64) as u32
            };
            if first_chunk != Some(0)
                || next_chunk != Some(expected_chunks)
                || total_len != artifact.logical_len
                || last_chunk_len != expected_last
            {
                return Err(GraphError::InvalidGraph(
                    "artifact chunks do not cover the declared logical length",
                ));
            }
            children.push(index);
        }
        for dependency in artifact.dependencies {
            children.push(self.verify_payload(dependency)?);
        }
        Ok(children)
    }

    fn verify_payload(&mut self, reference: ObjectRef) -> Result<Arc<VerifiedNode>, GraphError> {
        if !matches!(
            reference.object_type,
            ObjectType::Extent | ObjectType::ArtifactBlob | ObjectType::ArtifactChunk
        ) {
            return Err(GraphError::InvalidGraph(
                "payload edge references an unsupported object type",
            ));
        }
        if let Some(node) = self.reusable(reference)? {
            if matches!(&node.summary, VerifiedSummary::Payload { .. }) {
                validate_payload_length(reference, payload_len(&node)?)?;
                return Ok(node);
            }
            return Err(FormatError::ObjectReferenceMismatch.into());
        }
        self.begin(reference)?;
        let bytes = self.object_bytes(reference)?;
        let object = decode_object(&bytes)?;
        validate_payload_length(reference, object.payload.len() as u64)?;
        Ok(self.finish(
            reference,
            VerifiedSummary::Payload {
                payload_len: object.payload.len() as u64,
            },
            Vec::new(),
        ))
    }
}

fn map_summary(node: &VerifiedNode) -> Result<MapSummary, GraphError> {
    match &node.summary {
        VerifiedSummary::Map(summary) => Ok(summary.clone()),
        _ => Err(FormatError::ObjectReferenceMismatch.into()),
    }
}

fn payload_len(node: &VerifiedNode) -> Result<u64, GraphError> {
    match &node.summary {
        VerifiedSummary::Payload { payload_len } => Ok(*payload_len),
        _ => Err(FormatError::ObjectReferenceMismatch.into()),
    }
}

fn validate_payload_length(reference: ObjectRef, payload_len: u64) -> Result<(), GraphError> {
    match reference.object_type {
        ObjectType::Extent if payload_len == u64::from(EXTENT_BYTES) => Ok(()),
        ObjectType::ArtifactBlob if payload_len <= MAX_ARTIFACT_BLOB_BYTES as u64 => Ok(()),
        ObjectType::ArtifactChunk
            if payload_len > 0 && payload_len <= ARTIFACT_CHUNK_BYTES as u64 =>
        {
            Ok(())
        }
        ObjectType::Extent => Err(GraphError::InvalidGraph(
            "allocated volume extent does not contain exactly 64 KiB",
        )),
        ObjectType::ArtifactBlob => Err(GraphError::InvalidGraph("artifact blob exceeds 64 KiB")),
        ObjectType::ArtifactChunk => {
            Err(GraphError::InvalidGraph("artifact chunk length is invalid"))
        }
        _ => Err(FormatError::ObjectReferenceMismatch.into()),
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
    use super::{GraphError, verify_generation, verify_generation_with_reuse};
    use crate::format::{
        ARTIFACT_CHUNK_BYTES, ArtifactChunkEntry, ArtifactDescriptor, ArtifactMapRoot,
        CanonicalEncoder, CaptureMode, GenerationDescriptor, LogMapEntry, LogSegment, ObjectRef,
        ObjectType, SegmentIndexEntry, VolumeGeometry, VolumeRoot, encode_object,
    };
    use crate::identity::{
        ArtifactId, GenerationId, LogId, ObjectId, ObjectStoreScopeId, VolumeBoundaryId,
    };
    use crate::persistent_map::MapObjectStore;
    use crate::views::{
        ArtifactChunkIndex, ArtifactMap, LogMap, LogSegmentIndex, VolumeExtentMap,
        encode_artifact_blob, encode_artifact_chunk,
    };
    use std::collections::{HashMap, HashSet};

    #[derive(Default)]
    struct MemoryStore(HashMap<ObjectId, Vec<u8>>);

    impl MapObjectStore for MemoryStore {
        fn proof_scope(&self) -> ObjectStoreScopeId {
            ObjectStoreScopeId::from_bytes([0xA5; 16])
        }

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

    struct TypedOnlyStore<'a>(&'a MemoryStore);

    impl MapObjectStore for TypedOnlyStore<'_> {
        fn proof_scope(&self) -> ObjectStoreScopeId {
            self.0.proof_scope()
        }

        fn get(&self, _id: ObjectId) -> Result<Option<Vec<u8>>, String> {
            Err("typed object reference required".to_owned())
        }

        fn get_ref(&self, reference: ObjectRef) -> Result<Option<Vec<u8>>, String> {
            self.0.get(reference.object_id)
        }

        fn put(&mut self, _id: ObjectId, _bytes: Vec<u8>) -> Result<(), String> {
            Err("read-only test store".to_owned())
        }
    }

    #[test]
    fn complete_generation_verification_uses_typed_object_references() {
        let mut store = MemoryStore::default();
        let descriptor_ref =
            empty_generation(&mut store, GenerationId::from_bytes([0xA1; 16]), None);

        verify_generation(&TypedOnlyStore(&store), descriptor_ref)
            .expect("verify generation through typed-only reads");
    }

    struct DenyObjectStore<'a> {
        inner: &'a MemoryStore,
        denied: HashSet<ObjectId>,
    }

    impl MapObjectStore for DenyObjectStore<'_> {
        fn proof_scope(&self) -> ObjectStoreScopeId {
            self.inner.proof_scope()
        }

        fn get(&self, id: ObjectId) -> Result<Option<Vec<u8>>, String> {
            if self.denied.contains(&id) {
                return Err("protected proof object read is denied".to_owned());
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
            denied: HashSet::from([parent_ref.object_id]),
        };
        let proof = verify_generation(&restricted, descriptor_ref).unwrap();
        assert_eq!(proof.descriptor(), descriptor_ref);
        assert_eq!(proof.capture_mode(), CaptureMode::BlockCut);
        assert_eq!(proof.objects().len(), 7);
        assert!(!proof.objects().contains(&parent_ref));
    }

    #[test]
    fn unchanged_generation_views_do_not_rebuild_the_full_proof_index() {
        let mut store = MemoryStore::default();
        let base_ref = empty_generation(&mut store, GenerationId::from_bytes([0x51; 16]), None);
        let base = GenerationDescriptor::decode(store.0.get(&base_ref.object_id).unwrap()).unwrap();
        let base_proof = verify_generation(&store, base_ref).unwrap();
        let child_ref = store_object(
            &mut store,
            GenerationDescriptor {
                generation_id: GenerationId::from_bytes([0x52; 16]),
                parent_generation_id: Some(base.generation_id),
                ..base
            }
            .encode()
            .unwrap(),
        );
        let restricted = DenyObjectStore {
            inner: &store,
            denied: base_proof
                .objects()
                .iter()
                .map(|object| object.object_id)
                .collect(),
        };

        let child_proof =
            verify_generation_with_reuse(&restricted, child_ref, &[&base_proof]).unwrap();
        // Only the new descriptor and its three reused view roots enter the cache;
        // unchanged descendants remain shared through the proof DAG.
        assert_eq!(child_proof.index.len(), 4);
        assert!(child_proof.index.len() < child_proof.objects().len());
        let mut expected_objects = base_proof.objects().to_vec();
        expected_objects.retain(|object| *object != base_ref);
        expected_objects.push(child_ref);
        expected_objects.sort_unstable_by_key(|object| object.object_id);
        assert_eq!(child_proof.objects(), expected_objects);

        let repeated =
            verify_generation_with_reuse(&restricted, child_ref, &[&child_proof]).unwrap();
        assert!(std::sync::Arc::ptr_eq(&child_proof.index, &repeated.index));
    }

    #[test]
    fn generation_proof_reuses_protected_subtrees_and_reports_exact_closure() {
        let mut store = MemoryStore::default();
        let geometry = VolumeGeometry::new(4 * 1024 * 1024).unwrap();
        let volume_root = VolumeExtentMap::empty(geometry, &mut store)
            .unwrap()
            .seal(&mut store)
            .unwrap();
        let logs_root = LogMap::empty(&mut store).unwrap().seal(&mut store).unwrap();
        let common_blob = store_object(&mut store, encode_artifact_blob(b"common").unwrap());
        let removed_blob = store_object(&mut store, encode_artifact_blob(b"removed").unwrap());
        let mut artifacts = ArtifactMap::empty(&mut store).unwrap();
        for index in 0..1_200_u32 {
            let mut artifact_id = [0; 16];
            artifact_id[..4].copy_from_slice(&index.to_be_bytes());
            let content_root = if index == 600 {
                removed_blob
            } else {
                common_blob
            };
            artifacts = artifacts
                .insert(
                    &mut store,
                    &format!("artifact-{index:04}"),
                    &ArtifactDescriptor {
                        artifact_id: ArtifactId::from_bytes(artifact_id),
                        logical_len: if index == 600 { 7 } else { 6 },
                        content_root,
                        dependencies: Vec::new(),
                    },
                )
                .unwrap();
        }
        let artifacts_root = artifacts.seal(&mut store).unwrap();
        let base_descriptor = GenerationDescriptor {
            generation_id: GenerationId::from_bytes([0x21; 16]),
            parent_generation_id: None,
            volume_root,
            logs_root,
            artifacts_root,
            capture_mode: CaptureMode::BlockCut,
            volume_boundary_id: VolumeBoundaryId::from_bytes([0x22; 16]),
            created_at_unix_ms: 100,
        };
        let base_ref = store_object(&mut store, base_descriptor.encode().unwrap());
        let base_proof = verify_generation(&store, base_ref).unwrap();

        let artifacts = artifacts.remove(&mut store, "artifact-0600").unwrap();
        let child_descriptor = GenerationDescriptor {
            generation_id: GenerationId::from_bytes([0x23; 16]),
            parent_generation_id: Some(base_descriptor.generation_id),
            volume_root,
            logs_root,
            artifacts_root: artifacts.seal(&mut store).unwrap(),
            capture_mode: CaptureMode::BlockCut,
            volume_boundary_id: VolumeBoundaryId::from_bytes([0x24; 16]),
            created_at_unix_ms: 101,
        };
        let child_ref = store_object(&mut store, child_descriptor.encode().unwrap());
        let denied = base_proof
            .objects()
            .iter()
            .map(|reference| reference.object_id)
            .collect();
        let restricted = DenyObjectStore {
            inner: &store,
            denied,
        };

        let child_proof =
            verify_generation_with_reuse(&restricted, child_ref, &[&base_proof]).unwrap();
        let child_objects = child_proof.objects();
        assert!(child_objects.contains(&common_blob));
        assert!(!child_objects.contains(&removed_blob));
        assert!(!child_objects.contains(&base_ref));
        assert!(child_objects.contains(&child_ref));
        assert!(child_objects.len() < base_proof.objects().len() + 10);

        let mut artifact_id = [0; 16];
        artifact_id[0] = 0xFF;
        let reintroduced_artifacts = artifacts
            .insert(
                &mut store,
                "artifact-reintroduced",
                &ArtifactDescriptor {
                    artifact_id: ArtifactId::from_bytes(artifact_id),
                    logical_len: 7,
                    content_root: removed_blob,
                    dependencies: Vec::new(),
                },
            )
            .unwrap();
        let grandchild_descriptor = GenerationDescriptor {
            generation_id: GenerationId::from_bytes([0x25; 16]),
            parent_generation_id: Some(child_descriptor.generation_id),
            volume_root,
            logs_root,
            artifacts_root: reintroduced_artifacts.seal(&mut store).unwrap(),
            capture_mode: CaptureMode::BlockCut,
            volume_boundary_id: VolumeBoundaryId::from_bytes([0x26; 16]),
            created_at_unix_ms: 102,
        };
        let grandchild_ref = store_object(&mut store, grandchild_descriptor.encode().unwrap());
        let deny_removed_payload = DenyObjectStore {
            inner: &store,
            denied: HashSet::from([removed_blob.object_id]),
        };
        assert!(matches!(
            verify_generation_with_reuse(&deny_removed_payload, grandchild_ref, &[&child_proof]),
            Err(GraphError::Store(message)) if message == "protected proof object read is denied"
        ));
    }

    #[test]
    fn generation_proof_reuse_is_scoped_to_the_object_namespace() {
        struct OtherScope<'a>(&'a MemoryStore);

        impl MapObjectStore for OtherScope<'_> {
            fn proof_scope(&self) -> ObjectStoreScopeId {
                ObjectStoreScopeId::from_bytes([0x5A; 16])
            }

            fn get(&self, id: ObjectId) -> Result<Option<Vec<u8>>, String> {
                self.0.get(id)
            }

            fn put(&mut self, _id: ObjectId, _bytes: Vec<u8>) -> Result<(), String> {
                Err("read-only test store".to_owned())
            }
        }

        let mut store = MemoryStore::default();
        let generation_ref =
            empty_generation(&mut store, GenerationId::from_bytes([0x31; 16]), None);
        let proof = verify_generation(&store, generation_ref).unwrap();
        assert!(matches!(
            verify_generation_with_reuse(&OtherScope(&store), generation_ref, &[&proof]),
            Err(GraphError::InvalidGraph(
                "protected proof belongs to a different object-store scope"
            ))
        ));
    }

    #[test]
    fn generation_verification_rejects_overlapping_persistent_map_children() {
        let mut store = MemoryStore::default();
        let blob = store_object(&mut store, encode_artifact_blob(b"").unwrap());
        let mut next_id = 0_u8;
        let mut make_leaf = |entries: &[&str], store: &mut MemoryStore| {
            let mut leaf = CanonicalEncoder::new();
            leaf.u32_le(entries.len() as u32);
            for name in entries {
                let mut artifact_id = [0; 16];
                artifact_id[0] = next_id;
                next_id += 1;
                let descriptor = ArtifactDescriptor {
                    artifact_id: ArtifactId::from_bytes(artifact_id),
                    logical_len: 0,
                    content_root: blob,
                    dependencies: Vec::new(),
                };
                leaf.bytes(name.as_bytes()).unwrap();
                leaf.bytes(&descriptor.encode().unwrap()).unwrap();
            }
            store_object(
                store,
                encode_object(ObjectType::ArtifactMapNode, &leaf.finish()).unwrap(),
            )
        };
        let left = make_leaf(&["a", "z"], &mut store);
        let right = make_leaf(&["m", "zz"], &mut store);
        let mut internal = CanonicalEncoder::new();
        internal.u32_le(u32::MAX);
        internal.u16_le(1);
        internal.u32_le(2);
        internal.bytes(b"z").unwrap();
        internal.object_ref(left);
        internal.bytes(b"zz").unwrap();
        internal.object_ref(right);
        let internal_ref = store_object(
            &mut store,
            encode_object(ObjectType::ArtifactMapNode, &internal.finish()).unwrap(),
        );
        let artifacts_root = store_object(
            &mut store,
            ArtifactMapRoot {
                artifact_index_root: internal_ref,
                artifact_count: 4,
            }
            .encode()
            .unwrap(),
        );
        let base_ref = empty_generation(&mut store, GenerationId::from_bytes([0x41; 16]), None);
        let base = GenerationDescriptor::decode(store.0.get(&base_ref.object_id).unwrap()).unwrap();
        let descriptor = GenerationDescriptor {
            artifacts_root,
            ..base
        };
        let descriptor_ref = store_object(&mut store, descriptor.encode().unwrap());

        assert!(matches!(
            verify_generation(&store, descriptor_ref),
            Err(GraphError::InvalidGraph(
                "persistent map child key ranges overlap or are unordered"
            ))
        ));
    }

    #[test]
    fn generation_verification_checks_child_level_before_descending() {
        let mut store = MemoryStore::default();
        let base_ref = empty_generation(&mut store, GenerationId::from_bytes([0x42; 16]), None);
        let base = GenerationDescriptor::decode(store.0.get(&base_ref.object_id).unwrap()).unwrap();
        let mut volume =
            VolumeRoot::decode(store.0.get(&base.volume_root.object_id).unwrap()).unwrap();
        let forbidden_descendant = volume.extent_map_root;
        for _ in 0..2 {
            let mut internal = CanonicalEncoder::new();
            internal.u32_le(u32::MAX);
            internal.u16_le(1);
            internal.u32_le(1);
            internal.bytes(&0_u64.to_be_bytes()).unwrap();
            internal.object_ref(volume.extent_map_root);
            volume.extent_map_root = store_object(
                &mut store,
                encode_object(ObjectType::VolumeMapNode, &internal.finish()).unwrap(),
            );
        }
        let volume_root = store_object(&mut store, volume.encode().unwrap());
        let descriptor_ref = store_object(
            &mut store,
            GenerationDescriptor {
                volume_root,
                ..base
            }
            .encode()
            .unwrap(),
        );
        let restricted = DenyObjectStore {
            inner: &store,
            denied: HashSet::from([forbidden_descendant.object_id]),
        };

        assert!(matches!(
            verify_generation(&restricted, descriptor_ref),
            Err(GraphError::InvalidGraph(
                "persistent map child level differs from its parent"
            ))
        ));
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
            GenerationDescriptor::decode(store.0.get(&descriptor_ref.object_id).unwrap()).unwrap();
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
