//! Immutable copy-on-write ordered maps stored as bounded canonical objects.

use crate::format::{
    CanonicalDecoder, CanonicalEncoder, FormatError, MAX_PERSISTENT_NODE_BYTES, ObjectRef,
    ObjectType, encode_object, encode_ordered_entries,
};
use crate::identity::{ObjectId, ObjectStoreScopeId};
use std::cmp::Ordering;
use std::collections::VecDeque;
use thiserror::Error;

const INTERNAL_NODE_MARKER: u32 = u32::MAX;
const OBJECT_REFERENCE_BYTES: usize = 42;
const MAX_PERSISTENT_TREE_LEVEL: u16 = 64;

type NodeChildren = Vec<(Vec<u8>, ObjectRef)>;

/// Minimal immutable-object access required by the persistent map algorithms.
///
/// Implementations must treat an object ID as immutable. The map verifies every
/// retrieved value against the complete typed reference before decoding it.
pub trait MapObjectStore {
    /// Returns the stable identity of the immutable-object namespace.
    ///
    /// Verification proofs are reusable only within the same namespace. Stores
    /// that share a content-addressed namespace must return the same identity.
    fn proof_scope(&self) -> ObjectStoreScopeId;

    /// Reads an object by content identity.
    fn get(&self, id: ObjectId) -> Result<Option<Vec<u8>>, String>;

    /// Reads an object using its complete typed reference.
    ///
    /// Backends whose key layout includes the object type should override this
    /// method. The default preserves compatibility with stores keyed only by
    /// content identity.
    fn get_ref(&self, reference: ObjectRef) -> Result<Option<Vec<u8>>, String> {
        self.get(reference.object_id)
    }

    /// Stores bytes under their already-computed content identity.
    fn put(&mut self, id: ObjectId, bytes: Vec<u8>) -> Result<(), String>;
}

/// Immutable object store whose write acknowledgement means the object is durable.
///
/// `put_durable` must verify that `encoded` matches `reference`, conditionally
/// create or verify identical immutable content, and return only after the
/// object is durable and readable through the store's typed read path. A local
/// cache write alone does not satisfy this contract.
pub trait DurableObjectStore: MapObjectStore {
    /// Persist one already-encoded immutable object and verify identical reuse.
    fn put_durable(&mut self, reference: ObjectRef, encoded: &[u8]) -> Result<(), String>;
}

/// Errors returned while reading or mutating a persistent ordered map.
#[derive(Debug, Error)]
pub enum PersistentMapError {
    /// A canonical object or node was malformed.
    #[error(transparent)]
    Format(#[from] FormatError),
    /// The immutable object repository failed.
    #[error("object repository error: {0}")]
    Store(String),
    /// A referenced immutable object was unavailable.
    #[error("missing persistent-map object {0}")]
    MissingObject(ObjectId),
    /// One leaf entry or internal separator cannot fit in a bounded map node.
    #[error("persistent-map entry exceeds the 64 KiB node limit")]
    EntryTooLarge,
    /// The separator keys cannot be arranged into any bounded persistent-tree root.
    #[error("persistent-map separators cannot fit in a bounded root")]
    RootTooLarge,
}

/// One key change produced by a structural map diff.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MapChange {
    /// A key only exists in the newer map.
    Added {
        /// Changed key.
        key: Vec<u8>,
        /// New value.
        value: Vec<u8>,
    },
    /// A key only exists in the older map.
    Removed {
        /// Changed key.
        key: Vec<u8>,
        /// Previous value.
        value: Vec<u8>,
    },
    /// A key exists in both maps with different values.
    Updated {
        /// Changed key.
        key: Vec<u8>,
        /// Previous value.
        before: Vec<u8>,
        /// New value.
        after: Vec<u8>,
    },
}

/// One ordered key/value entry returned by a persistent map.
pub type MapEntry = (Vec<u8>, Vec<u8>);

/// All entries and node references reachable from one persistent map root.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MapContents {
    /// Entries in strict bytewise key order.
    pub entries: Vec<MapEntry>,
    /// Every reachable immutable map-node reference.
    pub node_refs: Vec<ObjectRef>,
}

/// An immutable root for a typed persistent B+ tree.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PersistentBTree {
    node_type: ObjectType,
    root: ObjectRef,
}

impl PersistentBTree {
    /// Creates a new empty map and stores its canonical empty leaf root.
    pub fn empty<S: MapObjectStore>(
        node_type: ObjectType,
        store: &mut S,
    ) -> Result<Self, PersistentMapError> {
        validate_node_type(node_type)?;
        let root = store_node(store, node_type, &Node::Leaf(Vec::new()))?;
        Ok(Self { node_type, root })
    }

    /// Opens a typed map at a complete immutable root reference.
    pub fn from_root(root: ObjectRef) -> Result<Self, PersistentMapError> {
        validate_node_type(root.object_type)?;
        root.validate()?;
        Ok(Self {
            node_type: root.object_type,
            root,
        })
    }

    /// Returns the immutable root node reference.
    pub const fn root(&self) -> ObjectRef {
        self.root
    }

    /// Returns this map's node object type.
    pub const fn node_type(&self) -> ObjectType {
        self.node_type
    }

    /// Looks up a key without materializing unrelated subtrees.
    pub fn get<S: MapObjectStore>(
        &self,
        store: &S,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, PersistentMapError> {
        let mut current = self.root;
        let mut expected_level = None;
        let mut expected_max = None;
        loop {
            let node = load_node(store, current, self.node_type)?;
            if expected_level.is_some_and(|level| level != node.level()) {
                return Err(
                    FormatError::InvalidPersistentNode("child level differs from parent").into(),
                );
            }
            if expected_max
                .as_deref()
                .is_some_and(|maximum: &[u8]| node.max_key() != Some(maximum))
            {
                return Err(FormatError::InvalidPersistentNode(
                    "child maximum differs from parent separator",
                )
                .into());
            }
            match node {
                Node::Leaf(entries) => {
                    return Ok(entries
                        .binary_search_by(|(candidate, _)| candidate.as_slice().cmp(key))
                        .ok()
                        .map(|index| entries[index].1.clone()));
                }
                Node::Internal { level, children } => {
                    let Some(index) = child_index(&children, key) else {
                        return Ok(None);
                    };
                    current = children[index].1;
                    expected_level = Some(level - 1);
                    expected_max = Some(children[index].0.clone());
                }
            }
        }
    }

    /// Returns a new map with one key inserted or replaced, sharing unchanged nodes.
    pub fn insert<S: MapObjectStore>(
        &self,
        store: &mut S,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<Self, PersistentMapError> {
        let edit = insert_node(store, self.root, self.node_type, None, None, key, value)?;
        if !edit.changed {
            return Ok(*self);
        }
        let root = if edit.children.len() == 1 {
            edit.children[0].1
        } else {
            build_root(store, self.node_type, edit.children)?
        };
        Ok(Self {
            node_type: self.node_type,
            root,
        })
    }

    /// Returns a new map with one key removed, sharing unchanged nodes.
    pub fn remove<S: MapObjectStore>(
        &self,
        store: &mut S,
        key: &[u8],
    ) -> Result<Self, PersistentMapError> {
        let edit = remove_node(store, self.root, self.node_type, None, None, key)?;
        if !edit.changed {
            return Ok(*self);
        }
        let root = if edit.children.is_empty() {
            store_node(store, self.node_type, &Node::Leaf(Vec::new()))?
        } else if edit.children.len() > 1 {
            build_root(store, self.node_type, edit.children)?
        } else {
            edit.children[0].1
        };
        let root = collapse_root(store, root, self.node_type)?;
        Ok(Self {
            node_type: self.node_type,
            root,
        })
    }

    /// Returns all entries in strict bytewise key order.
    pub fn entries<S: MapObjectStore>(
        &self,
        store: &S,
    ) -> Result<Vec<MapEntry>, PersistentMapError> {
        let mut entries = Vec::new();
        let mut nodes = Vec::new();
        collect_subtree(
            store,
            self.root,
            self.node_type,
            None,
            None,
            &mut entries,
            &mut nodes,
        )?;
        Ok(entries)
    }

    /// Returns ordered entries and every reachable map-node reference.
    pub fn entries_and_nodes<S: MapObjectStore>(
        &self,
        store: &S,
    ) -> Result<MapContents, PersistentMapError> {
        let mut entries = Vec::new();
        let mut nodes = Vec::new();
        collect_subtree(
            store,
            self.root,
            self.node_type,
            None,
            None,
            &mut entries,
            &mut nodes,
        )?;
        Ok(MapContents {
            entries,
            node_refs: nodes,
        })
    }

    /// Diffs two maps, skipping identical subtrees and falling back to sorted
    /// entry merging where tree boundaries differ.
    pub fn diff<S: MapObjectStore>(
        &self,
        newer: &Self,
        store: &S,
    ) -> Result<Vec<MapChange>, PersistentMapError> {
        if self.node_type != newer.node_type {
            return Err(FormatError::InvalidPersistentNode(
                "cannot diff maps with different node types",
            )
            .into());
        }
        let mut changes = Vec::new();
        diff_subtrees(
            store,
            self.root,
            newer.root,
            self.node_type,
            NodeExpectation::default(),
            NodeExpectation::default(),
            &mut changes,
        )?;
        Ok(changes)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Node {
    Leaf(Vec<(Vec<u8>, Vec<u8>)>),
    Internal {
        level: u16,
        /// Each key is the maximum key contained in the corresponding child.
        children: Vec<(Vec<u8>, ObjectRef)>,
    },
}

#[derive(Clone, Debug, Default)]
struct NodeExpectation {
    level: Option<u16>,
    max_key: Option<Vec<u8>>,
}

impl Node {
    fn entry_count(&self) -> usize {
        match self {
            Self::Leaf(entries) => entries.len(),
            Self::Internal { children, .. } => children.len(),
        }
    }

    fn entry_key(&self, index: usize) -> &[u8] {
        match self {
            Self::Leaf(entries) => &entries[index].0,
            Self::Internal { children, .. } => &children[index].0,
        }
    }

    pub(crate) fn level(&self) -> u16 {
        match self {
            Self::Leaf(_) => 0,
            Self::Internal { level, .. } => *level,
        }
    }

    pub(crate) fn max_key(&self) -> Option<&[u8]> {
        match self {
            Self::Leaf(entries) => entries.last().map(|entry| entry.0.as_slice()),
            Self::Internal { children, .. } => children.last().map(|entry| entry.0.as_slice()),
        }
    }
}

struct NodeEdit {
    children: Vec<(Vec<u8>, ObjectRef)>,
    changed: bool,
}

fn validate_node_type(node_type: ObjectType) -> Result<(), PersistentMapError> {
    if matches!(
        node_type,
        ObjectType::VolumeMapNode
            | ObjectType::LogMapNode
            | ObjectType::LogSegmentIndexNode
            | ObjectType::ArtifactMapNode
            | ObjectType::ArtifactIndexNode
    ) {
        Ok(())
    } else {
        Err(FormatError::InvalidPersistentNode("object type is not a persistent map node").into())
    }
}

pub(crate) fn load_node<S: MapObjectStore>(
    store: &S,
    reference: ObjectRef,
    expected_type: ObjectType,
) -> Result<Node, PersistentMapError> {
    if reference.object_type != expected_type {
        return Err(FormatError::ObjectReferenceMismatch.into());
    }
    let bytes = store
        .get_ref(reference)
        .map_err(PersistentMapError::Store)?
        .ok_or(PersistentMapError::MissingObject(reference.object_id))?;
    let object = reference.verify(&bytes)?;
    Ok(decode_node(object.payload, expected_type)?)
}

fn store_node<S: MapObjectStore>(
    store: &mut S,
    node_type: ObjectType,
    node: &Node,
) -> Result<ObjectRef, PersistentMapError> {
    let bytes = encode_node(node_type, node)?;
    let reference = ObjectRef::from_encoded(&bytes)?;
    store
        .put(reference.object_id, bytes)
        .map_err(PersistentMapError::Store)?;
    Ok(reference)
}

fn encode_node(node_type: ObjectType, node: &Node) -> Result<Vec<u8>, FormatError> {
    validate_node_type(node_type).map_err(|_| {
        FormatError::InvalidPersistentNode("object type is not a persistent map node")
    })?;
    let bytes = match node {
        Node::Leaf(entries) => {
            let refs: Vec<(&[u8], &[u8])> = entries
                .iter()
                .map(|(key, value)| (key.as_slice(), value.as_slice()))
                .collect();
            encode_ordered_entries(&refs)?
        }
        Node::Internal { level, children } => {
            if *level == 0 || *level > MAX_PERSISTENT_TREE_LEVEL || children.is_empty() {
                return Err(FormatError::InvalidPersistentNode(
                    "internal nodes require a bounded positive level and at least one child",
                ));
            }
            ensure_strict_keys(children.iter().map(|entry| entry.0.as_slice()))?;
            let mut encoder = CanonicalEncoder::new();
            encoder.u32_le(INTERNAL_NODE_MARKER);
            encoder.u16_le(*level);
            encoder.u32_le(
                u32::try_from(children.len())
                    .map_err(|_| FormatError::InvalidPersistentNode("too many children"))?,
            );
            for (max_key, child) in children {
                if child.object_type != node_type {
                    return Err(FormatError::ObjectReferenceMismatch);
                }
                child.validate()?;
                encoder.bytes(max_key)?;
                encoder.object_ref(*child);
            }
            encoder.finish()
        }
    };
    let encoded = encode_object(node_type, &bytes)?;
    if encoded.len() > MAX_PERSISTENT_NODE_BYTES {
        return Err(FormatError::InvalidPersistentNode(
            "encoded node exceeds 64 KiB",
        ));
    }
    Ok(encoded)
}

fn decode_node(payload: &[u8], node_type: ObjectType) -> Result<Node, FormatError> {
    if payload.len() + crate::format::ENVELOPE_HEADER_BYTES > MAX_PERSISTENT_NODE_BYTES {
        return Err(FormatError::InvalidPersistentNode(
            "encoded node exceeds 64 KiB",
        ));
    }
    if payload.len() < 4 {
        return Err(FormatError::InvalidPersistentNode("truncated node header"));
    }
    let marker = u32::from_le_bytes(payload[..4].try_into().expect("checked four-byte prefix"));
    if marker != INTERNAL_NODE_MARKER {
        return decode_leaf(payload);
    }

    let mut decoder = CanonicalDecoder::new(&payload[4..]);
    let level = decoder.u16_le()?;
    let count = usize::try_from(decoder.u32_le()?)
        .map_err(|_| FormatError::InvalidPersistentNode("child count overflow"))?;
    if level == 0 || level > MAX_PERSISTENT_TREE_LEVEL || count == 0 {
        return Err(FormatError::InvalidPersistentNode(
            "internal nodes require a bounded positive level and at least one child",
        ));
    }
    let min_child_bytes = 4_usize + OBJECT_REFERENCE_BYTES;
    let required = count
        .checked_mul(min_child_bytes)
        .ok_or(FormatError::InvalidPersistentNode("child count overflow"))?;
    if required > payload.len().saturating_sub(10) {
        return Err(FormatError::InvalidPersistentNode("truncated child list"));
    }
    let mut children = Vec::with_capacity(count);
    for _ in 0..count {
        let max_key = decoder.bytes(MAX_PERSISTENT_NODE_BYTES)?.to_vec();
        let child = decoder.object_ref()?;
        if child.object_type != node_type {
            return Err(FormatError::ObjectReferenceMismatch);
        }
        children.push((max_key, child));
    }
    decoder.finish()?;
    ensure_strict_keys(children.iter().map(|entry| entry.0.as_slice()))?;
    Ok(Node::Internal { level, children })
}

fn decode_leaf(payload: &[u8]) -> Result<Node, FormatError> {
    let mut decoder = CanonicalDecoder::new(payload);
    let count = usize::try_from(decoder.u32_le()?)
        .map_err(|_| FormatError::InvalidPersistentNode("entry count overflow"))?;
    let min_entry_bytes = count
        .checked_mul(8)
        .ok_or(FormatError::InvalidPersistentNode("entry count overflow"))?;
    if min_entry_bytes > payload.len().saturating_sub(4) {
        return Err(FormatError::InvalidPersistentNode("truncated leaf entries"));
    }
    let mut entries = Vec::with_capacity(count);
    for _ in 0..count {
        entries.push((
            decoder.bytes(MAX_PERSISTENT_NODE_BYTES)?.to_vec(),
            decoder.bytes(MAX_PERSISTENT_NODE_BYTES)?.to_vec(),
        ));
    }
    decoder.finish()?;
    ensure_strict_keys(entries.iter().map(|entry| entry.0.as_slice()))?;
    Ok(Node::Leaf(entries))
}

fn ensure_strict_keys<'a>(keys: impl Iterator<Item = &'a [u8]>) -> Result<(), FormatError> {
    let mut previous = None;
    for key in keys {
        if previous.is_some_and(|value: &[u8]| value >= key) {
            return Err(FormatError::UnorderedKeys);
        }
        previous = Some(key);
    }
    Ok(())
}

fn encoded_len(node_type: ObjectType, node: &Node) -> Result<usize, FormatError> {
    let payload_len = match node {
        Node::Leaf(entries) => entries.iter().try_fold(4_usize, |size, (key, value)| {
            size.checked_add(8)
                .and_then(|size| size.checked_add(key.len()))
                .and_then(|size| size.checked_add(value.len()))
                .ok_or(FormatError::InvalidPersistentNode("leaf size overflow"))
        })?,
        Node::Internal { children, .. } => {
            children.iter().try_fold(10_usize, |size, (key, _)| {
                size.checked_add(4 + OBJECT_REFERENCE_BYTES)
                    .and_then(|size| size.checked_add(key.len()))
                    .ok_or(FormatError::InvalidPersistentNode("internal size overflow"))
            })?
        }
    };
    validate_node_type(node_type).map_err(|_| {
        FormatError::InvalidPersistentNode("object type is not a persistent map node")
    })?;
    payload_len
        .checked_add(crate::format::ENVELOPE_HEADER_BYTES)
        .ok_or(FormatError::InvalidPersistentNode("node size overflow"))
}

fn write_or_split<S: MapObjectStore>(
    store: &mut S,
    node_type: ObjectType,
    node: Node,
) -> Result<Vec<(Vec<u8>, ObjectRef)>, PersistentMapError> {
    if encoded_len(node_type, &node)? <= MAX_PERSISTENT_NODE_BYTES {
        let reference = store_node(store, node_type, &node)?;
        return Ok(node
            .max_key()
            .map(|max_key| vec![(max_key.to_vec(), reference)])
            .unwrap_or_default());
    }
    store_parts(store, node_type, split_node(node_type, &node)?)
}

fn store_parts<S: MapObjectStore>(
    store: &mut S,
    node_type: ObjectType,
    parts: Vec<Node>,
) -> Result<NodeChildren, PersistentMapError> {
    parts
        .into_iter()
        .map(|part| {
            let maximum = part.max_key().expect("split nodes are non-empty").to_vec();
            let reference = store_node(store, node_type, &part)?;
            Ok((maximum, reference))
        })
        .collect()
}

fn split_node(node_type: ObjectType, node: &Node) -> Result<Vec<Node>, PersistentMapError> {
    let (entry_count, base_size) = match node {
        Node::Leaf(entries) => (entries.len(), 4 + crate::format::ENVELOPE_HEADER_BYTES),
        Node::Internal { children, .. } => {
            (children.len(), 10 + crate::format::ENVELOPE_HEADER_BYTES)
        }
    };
    let mut ranges = Vec::new();
    let mut range_start = 0;
    let mut current_size = base_size;
    for index in 0..entry_count {
        let item_size = node_entry_size(node, index)?;
        let next_size = current_size
            .checked_add(item_size)
            .ok_or(FormatError::InvalidPersistentNode("node size overflow"))?;
        if next_size > MAX_PERSISTENT_NODE_BYTES {
            if range_start == index {
                return Err(PersistentMapError::EntryTooLarge);
            }
            ranges.push((range_start, index));
            range_start = index;
            current_size = base_size;
        }
        current_size = current_size
            .checked_add(item_size)
            .ok_or(FormatError::InvalidPersistentNode("node size overflow"))?;
        if current_size > MAX_PERSISTENT_NODE_BYTES {
            return Err(PersistentMapError::EntryTooLarge);
        }
    }
    if range_start == entry_count {
        return Err(PersistentMapError::EntryTooLarge);
    }
    ranges.push((range_start, entry_count));

    if ranges.len() < 2 {
        return Err(FormatError::InvalidPersistentNode(
            "split requested for a node that already fits",
        )
        .into());
    }
    validate_node_type(node_type).map_err(|_| {
        FormatError::InvalidPersistentNode("object type is not a persistent map node")
    })?;
    Ok(ranges
        .into_iter()
        .map(|(start, end)| slice_node(node, start, end))
        .collect())
}

fn node_entry_size(node: &Node, index: usize) -> Result<usize, FormatError> {
    match node {
        Node::Leaf(entries) => {
            let (key, value) = &entries[index];
            8_usize
                .checked_add(key.len())
                .and_then(|size| size.checked_add(value.len()))
                .ok_or(FormatError::InvalidPersistentNode("leaf size overflow"))
        }
        Node::Internal { children, .. } => 4_usize
            .checked_add(OBJECT_REFERENCE_BYTES)
            .and_then(|size| size.checked_add(children[index].0.len()))
            .ok_or(FormatError::InvalidPersistentNode("internal size overflow")),
    }
}

fn slice_node(node: &Node, start: usize, end: usize) -> Node {
    match node {
        Node::Leaf(entries) => Node::Leaf(entries[start..end].to_vec()),
        Node::Internal { level, children } => Node::Internal {
            level: *level,
            children: children[start..end].to_vec(),
        },
    }
}

fn build_root<S: MapObjectStore>(
    store: &mut S,
    node_type: ObjectType,
    mut children: Vec<(Vec<u8>, ObjectRef)>,
) -> Result<ObjectRef, PersistentMapError> {
    let mut child_level = load_node(store, children[0].1, node_type)?.level();
    loop {
        if children.len() == 1 {
            return Ok(children[0].1);
        }
        let level = child_level
            .checked_add(1)
            .ok_or(FormatError::InvalidPersistentNode("tree height overflow"))?;
        if level > MAX_PERSISTENT_TREE_LEVEL {
            return Err(FormatError::InvalidPersistentNode("tree height overflow").into());
        }
        let previous_count = children.len();
        let node = Node::Internal { level, children };
        if encoded_len(node_type, &node)? <= MAX_PERSISTENT_NODE_BYTES {
            return store_node(store, node_type, &node);
        }
        let parts = match split_node(node_type, &node) {
            Ok(parts) if parts.len() < previous_count => parts,
            Ok(_) | Err(PersistentMapError::EntryTooLarge) => {
                let Node::Internal {
                    children: original, ..
                } = node
                else {
                    unreachable!("root candidate is internal");
                };
                // Long maxima can prevent any reduction in fanout. Repair only
                // adjacent sibling boundaries, keeping their encoded nodes bounded.
                children = repartition_children(store, node_type, child_level, &original)?
                    .ok_or(PersistentMapError::RootTooLarge)?;
                continue;
            }
            Err(error) => return Err(error),
        };
        let parents = store_parts(store, node_type, parts)?;
        child_level = level;
        children = parents;
    }
}

fn repartition_children<S: MapObjectStore>(
    store: &mut S,
    node_type: ObjectType,
    child_level: u16,
    children: &[(Vec<u8>, ObjectRef)],
) -> Result<Option<NodeChildren>, PersistentMapError> {
    for index in 0..children.len().saturating_sub(1) {
        let pair = &children[index..index + 2];
        let left = load_node(store, pair[0].1, node_type)?;
        let right = load_node(store, pair[1].1, node_type)?;
        if left.level() != child_level
            || right.level() != child_level
            || left.max_key() != Some(pair[0].0.as_slice())
            || right.max_key() != Some(pair[1].0.as_slice())
        {
            return Err(FormatError::InvalidPersistentNode(
                "sibling level or maximum differs from parent",
            )
            .into());
        }
        let original_cost = separator_size(&pair[0].0) + separator_size(&pair[1].0);
        let mut combined = join_siblings(left, right)?;
        loop {
            let parts = partition_for_separators(&combined)?;
            let replacement_cost: usize = parts
                .iter()
                .map(|part| separator_size(part.max_key().expect("non-empty partition")))
                .sum();
            if replacement_cost < original_cost {
                let replacements = store_parts(store, node_type, parts)?;
                let mut updated = children.to_vec();
                updated.splice(index..index + 2, replacements);
                return Ok(Some(updated));
            }
            let Node::Internal {
                level,
                children: descendants,
            } = &mut combined
            else {
                break;
            };
            let Some(repaired) = repartition_children(store, node_type, *level - 1, descendants)?
            else {
                break;
            };
            *descendants = repaired;
        }
    }
    Ok(None)
}

fn join_siblings(left: Node, right: Node) -> Result<Node, FormatError> {
    let joined = match (left, right) {
        (Node::Leaf(mut left), Node::Leaf(right)) => {
            left.extend(right);
            Node::Leaf(left)
        }
        (
            Node::Internal {
                level,
                mut children,
            },
            Node::Internal {
                children: right, ..
            },
        ) => {
            children.extend(right);
            Node::Internal { level, children }
        }
        _ => {
            return Err(FormatError::InvalidPersistentNode(
                "sibling node kinds differ",
            ));
        }
    };
    ensure_strict_keys((0..joined.entry_count()).map(|index| joined.entry_key(index)))?;
    Ok(joined)
}

fn separator_size(key: &[u8]) -> usize {
    4 + OBJECT_REFERENCE_BYTES + key.len()
}

/// Partitions at most two siblings, minimizing the bytes their maxima need in
/// the parent. A sliding minimum over feasible prefix boundaries makes this
/// linear in the number of entries, even when values are empty.
fn partition_for_separators(node: &Node) -> Result<Vec<Node>, PersistentMapError> {
    let entry_count = node.entry_count();
    let base_size =
        crate::format::ENVELOPE_HEADER_BYTES + if matches!(node, Node::Leaf(_)) { 4 } else { 10 };
    let capacity = MAX_PERSISTENT_NODE_BYTES - base_size;
    let mut prefix = Vec::with_capacity(entry_count + 1);
    prefix.push(0_usize);
    for index in 0..entry_count {
        let size = node_entry_size(node, index)?;
        if size > capacity {
            return Err(PersistentMapError::EntryTooLarge);
        }
        prefix.push(prefix[index] + size);
    }
    let mut costs = vec![0_usize; entry_count + 1];
    let mut starts = vec![0_usize; entry_count + 1];
    let mut candidates = VecDeque::from([0]);
    for end in 1..=entry_count {
        while candidates
            .front()
            .is_some_and(|&start| prefix[end] - prefix[start] > capacity)
        {
            candidates.pop_front();
        }
        let start = *candidates.front().expect("each individual entry fits");
        costs[end] = costs[start] + separator_size(node.entry_key(end - 1));
        starts[end] = start;
        while candidates
            .back()
            .is_some_and(|&previous| costs[previous] >= costs[end])
        {
            candidates.pop_back();
        }
        candidates.push_back(end);
    }
    let mut parts = Vec::new();
    let mut end = entry_count;
    while end > 0 {
        let start = starts[end];
        parts.push(slice_node(node, start, end));
        end = start;
    }
    parts.reverse();
    Ok(parts)
}

fn child_index(children: &[(Vec<u8>, ObjectRef)], key: &[u8]) -> Option<usize> {
    children
        .iter()
        .position(|(maximum, _)| maximum.as_slice() >= key)
        .or_else(|| (!children.is_empty()).then_some(children.len() - 1))
}

fn insert_node<S: MapObjectStore>(
    store: &mut S,
    reference: ObjectRef,
    node_type: ObjectType,
    expected_level: Option<u16>,
    expected_max: Option<Vec<u8>>,
    key: Vec<u8>,
    value: Vec<u8>,
) -> Result<NodeEdit, PersistentMapError> {
    let node = load_node(store, reference, node_type)?;
    if expected_level.is_some_and(|level| level != node.level()) {
        return Err(FormatError::InvalidPersistentNode("child level differs from parent").into());
    }
    if expected_max
        .as_deref()
        .is_some_and(|maximum| node.max_key() != Some(maximum))
    {
        return Err(FormatError::InvalidPersistentNode(
            "child maximum differs from parent separator",
        )
        .into());
    }
    match node {
        Node::Leaf(mut entries) => {
            match entries.binary_search_by(|(candidate, _)| candidate.cmp(&key)) {
                Ok(index) if entries[index].1 == value => {
                    return Ok(NodeEdit {
                        children: vec![(entries[index].0.clone(), reference)],
                        changed: false,
                    });
                }
                Ok(index) => entries[index].1 = value,
                Err(index) => entries.insert(index, (key, value)),
            }
            Ok(NodeEdit {
                children: write_or_split(store, node_type, Node::Leaf(entries))?,
                changed: true,
            })
        }
        Node::Internal {
            level,
            mut children,
        } => {
            let index = child_index(&children, &key)
                .ok_or(FormatError::InvalidPersistentNode("empty internal node"))?;
            let child_ref = children[index].1;
            let edit = insert_node(
                store,
                child_ref,
                node_type,
                Some(level - 1),
                Some(children[index].0.clone()),
                key,
                value,
            )?;
            if !edit.changed {
                return Ok(NodeEdit {
                    children: vec![(
                        children.last().expect("non-empty internal node").0.clone(),
                        reference,
                    )],
                    changed: false,
                });
            }
            children.remove(index);
            for (offset, child) in edit.children.into_iter().enumerate() {
                children.insert(index + offset, child);
            }
            Ok(NodeEdit {
                children: write_or_split(store, node_type, Node::Internal { level, children })?,
                changed: true,
            })
        }
    }
}

fn remove_node<S: MapObjectStore>(
    store: &mut S,
    reference: ObjectRef,
    node_type: ObjectType,
    expected_level: Option<u16>,
    expected_max: Option<Vec<u8>>,
    key: &[u8],
) -> Result<NodeEdit, PersistentMapError> {
    let node = load_node(store, reference, node_type)?;
    if expected_level.is_some_and(|level| level != node.level()) {
        return Err(FormatError::InvalidPersistentNode("child level differs from parent").into());
    }
    if expected_max
        .as_deref()
        .is_some_and(|maximum| node.max_key() != Some(maximum))
    {
        return Err(FormatError::InvalidPersistentNode(
            "child maximum differs from parent separator",
        )
        .into());
    }
    match node {
        Node::Leaf(mut entries) => {
            let Ok(index) =
                entries.binary_search_by(|(candidate, _)| candidate.as_slice().cmp(key))
            else {
                return Ok(NodeEdit {
                    children: vec![(
                        entries
                            .last()
                            .map(|entry| entry.0.clone())
                            .unwrap_or_default(),
                        reference,
                    )],
                    changed: false,
                });
            };
            entries.remove(index);
            Ok(NodeEdit {
                children: if entries.is_empty() {
                    Vec::new()
                } else {
                    write_or_split(store, node_type, Node::Leaf(entries))?
                },
                changed: true,
            })
        }
        Node::Internal {
            level,
            mut children,
        } => {
            let Some(index) = child_index(&children, key) else {
                return Ok(NodeEdit {
                    children: vec![(
                        children.last().expect("non-empty internal node").0.clone(),
                        reference,
                    )],
                    changed: false,
                });
            };
            if key > children[index].0.as_slice() {
                return Ok(NodeEdit {
                    children: vec![(
                        children.last().expect("non-empty internal node").0.clone(),
                        reference,
                    )],
                    changed: false,
                });
            }
            let child_ref = children[index].1;
            let edit = remove_node(
                store,
                child_ref,
                node_type,
                Some(level - 1),
                Some(children[index].0.clone()),
                key,
            )?;
            if !edit.changed {
                return Ok(NodeEdit {
                    children: vec![(
                        children.last().expect("non-empty internal node").0.clone(),
                        reference,
                    )],
                    changed: false,
                });
            }
            children.remove(index);
            for (offset, child) in edit.children.into_iter().enumerate() {
                children.insert(index + offset, child);
            }
            let updated = if children.is_empty() {
                Vec::new()
            } else {
                write_or_split(store, node_type, Node::Internal { level, children })?
            };
            Ok(NodeEdit {
                children: updated,
                changed: true,
            })
        }
    }
}

fn collapse_root<S: MapObjectStore>(
    store: &S,
    mut root: ObjectRef,
    node_type: ObjectType,
) -> Result<ObjectRef, PersistentMapError> {
    loop {
        let node = load_node(store, root, node_type)?;
        match node {
            Node::Internal { children, .. } if children.len() == 1 => root = children[0].1,
            _ => return Ok(root),
        }
    }
}

fn collect_subtree<S: MapObjectStore>(
    store: &S,
    reference: ObjectRef,
    node_type: ObjectType,
    expected_level: Option<u16>,
    expected_max: Option<Vec<u8>>,
    entries: &mut Vec<(Vec<u8>, Vec<u8>)>,
    nodes: &mut Vec<ObjectRef>,
) -> Result<(), PersistentMapError> {
    let node = load_node(store, reference, node_type)?;
    if expected_level.is_some_and(|level| level != node.level()) {
        return Err(FormatError::InvalidPersistentNode("child level differs from parent").into());
    }
    if expected_max
        .as_deref()
        .is_some_and(|maximum| node.max_key() != Some(maximum))
    {
        return Err(FormatError::InvalidPersistentNode(
            "child maximum differs from parent separator",
        )
        .into());
    }
    nodes.push(reference);
    match node {
        Node::Leaf(leaf) => entries.extend(leaf),
        Node::Internal { level, children } => {
            for (maximum, child) in children {
                let previous_max = entries.last().map(|(key, _)| key.clone());
                let child_entry_start = entries.len();
                collect_subtree(
                    store,
                    child,
                    node_type,
                    Some(level - 1),
                    Some(maximum),
                    entries,
                    nodes,
                )?;
                if let (Some(previous_max), Some((child_min, _))) =
                    (previous_max, entries.get(child_entry_start))
                    && child_min <= &previous_max
                {
                    return Err(FormatError::InvalidPersistentNode(
                        "adjacent child key ranges overlap or are unordered",
                    )
                    .into());
                }
            }
        }
    }
    Ok(())
}

fn diff_subtrees<S: MapObjectStore>(
    store: &S,
    before_ref: ObjectRef,
    after_ref: ObjectRef,
    node_type: ObjectType,
    before_expected: NodeExpectation,
    after_expected: NodeExpectation,
    changes: &mut Vec<MapChange>,
) -> Result<(), PersistentMapError> {
    let before = load_node(store, before_ref, node_type)?;
    let after = load_node(store, after_ref, node_type)?;
    if before_expected
        .level
        .is_some_and(|level| level != before.level())
        || after_expected
            .level
            .is_some_and(|level| level != after.level())
    {
        return Err(FormatError::InvalidPersistentNode("child level differs from parent").into());
    }
    if before_expected
        .max_key
        .as_deref()
        .is_some_and(|maximum| before.max_key() != Some(maximum))
        || after_expected
            .max_key
            .as_deref()
            .is_some_and(|maximum| after.max_key() != Some(maximum))
    {
        return Err(FormatError::InvalidPersistentNode(
            "child maximum differs from parent separator",
        )
        .into());
    }
    if before_ref == after_ref {
        return Ok(());
    }
    match (&before, &after) {
        (
            Node::Internal {
                level: before_level,
                children: before_children,
            },
            Node::Internal {
                level: after_level,
                children: after_children,
            },
        ) if before_level == after_level
            && before_children.len() == after_children.len()
            && before_children
                .iter()
                .zip(after_children)
                .all(|(left, right)| left.0 == right.0) =>
        {
            for ((before_max, before_child), (after_max, after_child)) in
                before_children.iter().zip(after_children)
            {
                diff_subtrees(
                    store,
                    *before_child,
                    *after_child,
                    node_type,
                    NodeExpectation {
                        level: Some(*before_level - 1),
                        max_key: Some(before_max.clone()),
                    },
                    NodeExpectation {
                        level: Some(*after_level - 1),
                        max_key: Some(after_max.clone()),
                    },
                    changes,
                )?;
            }
            Ok(())
        }
        (Node::Leaf(before_entries), Node::Leaf(after_entries)) => {
            merge_diff(before_entries.clone(), after_entries.clone(), changes);
            Ok(())
        }
        _ => {
            let mut before_entries = Vec::new();
            let mut before_nodes = Vec::new();
            collect_subtree(
                store,
                before_ref,
                node_type,
                before_expected.level,
                before_expected.max_key,
                &mut before_entries,
                &mut before_nodes,
            )?;
            let mut after_entries = Vec::new();
            let mut after_nodes = Vec::new();
            collect_subtree(
                store,
                after_ref,
                node_type,
                after_expected.level,
                after_expected.max_key,
                &mut after_entries,
                &mut after_nodes,
            )?;
            merge_diff(before_entries, after_entries, changes);
            Ok(())
        }
    }
}

fn merge_diff(
    before: Vec<(Vec<u8>, Vec<u8>)>,
    after: Vec<(Vec<u8>, Vec<u8>)>,
    changes: &mut Vec<MapChange>,
) {
    let mut left = before.into_iter().peekable();
    let mut right = after.into_iter().peekable();
    loop {
        match (left.peek(), right.peek()) {
            (Some((left_key, _)), Some((right_key, _))) => match left_key.cmp(right_key) {
                Ordering::Less => {
                    let (key, value) = left.next().expect("peeked entry exists");
                    changes.push(MapChange::Removed { key, value });
                }
                Ordering::Greater => {
                    let (key, value) = right.next().expect("peeked entry exists");
                    changes.push(MapChange::Added { key, value });
                }
                Ordering::Equal => {
                    let (key, before_value) = left.next().expect("peeked entry exists");
                    let (_, after_value) = right.next().expect("peeked entry exists");
                    if before_value != after_value {
                        changes.push(MapChange::Updated {
                            key,
                            before: before_value,
                            after: after_value,
                        });
                    }
                }
            },
            (Some(_), None) => {
                let (key, value) = left.next().expect("peeked entry exists");
                changes.push(MapChange::Removed { key, value });
            }
            (None, Some(_)) => {
                let (key, value) = right.next().expect("peeked entry exists");
                changes.push(MapChange::Added { key, value });
            }
            (None, None) => return,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        INTERNAL_NODE_MARKER, MAX_PERSISTENT_TREE_LEVEL, MapChange, MapObjectStore, Node,
        ObjectStoreScopeId, PersistentBTree, PersistentMapError, decode_node, encode_node,
        load_node, partition_for_separators, store_node,
    };
    use crate::format::{
        CanonicalEncoder, MAX_PERSISTENT_NODE_BYTES, ObjectRef, ObjectType, decode_object,
        encode_object, object_id,
    };
    use crate::identity::ObjectId;
    use std::collections::{BTreeMap, HashMap};

    #[derive(Default)]
    struct MemoryStore {
        objects: HashMap<ObjectId, Vec<u8>>,
        puts: usize,
    }

    impl MapObjectStore for MemoryStore {
        fn proof_scope(&self) -> ObjectStoreScopeId {
            ObjectStoreScopeId::from_bytes([0xA5; 16])
        }

        fn get(&self, id: ObjectId) -> Result<Option<Vec<u8>>, String> {
            Ok(self.objects.get(&id).cloned())
        }

        fn put(&mut self, id: ObjectId, bytes: Vec<u8>) -> Result<(), String> {
            self.puts += 1;
            self.objects.insert(id, bytes);
            Ok(())
        }
    }

    fn key(index: u32) -> Vec<u8> {
        index.to_be_bytes().to_vec()
    }

    fn assert_model(
        tree: &PersistentBTree,
        store: &MemoryStore,
        model: &BTreeMap<Vec<u8>, Vec<u8>>,
    ) {
        let actual = tree.entries(store).unwrap();
        assert_eq!(
            actual,
            model
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect::<Vec<_>>()
        );
        for (key, value) in model {
            assert_eq!(tree.get(store, key).unwrap().as_ref(), Some(value));
        }
    }

    #[test]
    fn copy_on_write_splits_deletes_and_preserves_old_roots() {
        let mut store = MemoryStore::default();
        let mut tree = PersistentBTree::empty(ObjectType::VolumeMapNode, &mut store).unwrap();
        let mut model = BTreeMap::new();
        for index in 0..2400 {
            let key = key(index);
            let value = vec![(index % 251) as u8; 37 + (index as usize % 29)];
            tree = tree.insert(&mut store, key.clone(), value.clone()).unwrap();
            model.insert(key, value);
        }
        assert_model(&tree, &store, &model);
        assert!(store.objects.values().all(|bytes| {
            let object = decode_object(bytes).unwrap();
            object.object_type != ObjectType::VolumeMapNode
                || bytes.len() <= MAX_PERSISTENT_NODE_BYTES
        }));

        let old = tree;
        let old_count = model.len();
        let unchanged = old
            .insert(&mut store, key(1000), model[&key(1000)].clone())
            .unwrap();
        assert_eq!(unchanged.root(), old.root());

        for index in (0..2400).step_by(3) {
            let deleted = key(index);
            tree = tree.remove(&mut store, &deleted).unwrap();
            model.remove(&deleted);
        }
        assert_model(&tree, &store, &model);
        assert_eq!(old.entries(&store).unwrap().len(), old_count);
        assert_ne!(old.root(), tree.root());
    }

    #[test]
    fn insertion_copies_a_path_and_structural_diff_matches_full_model() {
        let mut store = MemoryStore::default();
        let mut before = PersistentBTree::empty(ObjectType::ArtifactIndexNode, &mut store).unwrap();
        for index in 0..3000 {
            before = before
                .insert(&mut store, key(index), vec![index as u8; 24])
                .unwrap();
        }
        let before_root = before.root();
        let before_nodes = before.entries_and_nodes(&store).unwrap().node_refs;
        let root_level = load_node(&store, before_root, ObjectType::ArtifactIndexNode)
            .unwrap()
            .level();
        let after = before
            .insert(&mut store, key(3001), vec![0xEE; 24])
            .unwrap();
        let after_nodes = after.entries_and_nodes(&store).unwrap().node_refs;
        let replaced_old_nodes = before_nodes
            .iter()
            .filter(|node| !after_nodes.contains(node))
            .count();
        assert!(before_nodes.len() > (root_level as usize + 1));
        assert!(replaced_old_nodes <= root_level as usize + 1);
        assert_ne!(before_root, after.root());
        assert_eq!(
            before.diff(&after, &store).unwrap(),
            vec![MapChange::Added {
                key: key(3001),
                value: vec![0xEE; 24],
            }]
        );
        assert!(before.diff(&before, &store).unwrap().is_empty());
    }

    #[test]
    fn internal_node_matches_shared_golden_vector() {
        let node = Node::Internal {
            level: 1,
            children: vec![
                (
                    1_u64.to_be_bytes().to_vec(),
                    ObjectRef {
                        object_type: ObjectType::VolumeMapNode,
                        object_id: ObjectId::from_bytes([0x11; 32]),
                        encoded_len: 20,
                    },
                ),
                (
                    2_u64.to_be_bytes().to_vec(),
                    ObjectRef {
                        object_type: ObjectType::VolumeMapNode,
                        object_id: ObjectId::from_bytes([0x22; 32]),
                        encoded_len: 20,
                    },
                ),
            ],
        };
        let encoded = encode_node(ObjectType::VolumeMapNode, &node).unwrap();
        let fixture = include_str!("../../fixtures/storage-format-v1.tsv")
            .lines()
            .find_map(|line| {
                let mut columns = line.split('\t');
                if columns.next()? == "volume_map_internal_two_children" {
                    Some((columns.next()?.to_owned(), columns.next()?.to_owned()))
                } else {
                    None
                }
            })
            .expect("shared format fixture exists");
        let actual_hex: String = encoded.iter().map(|byte| format!("{byte:02x}")).collect();
        assert_eq!(actual_hex, fixture.0);
        assert_eq!(object_id(&encoded).to_string(), fixture.1);
    }

    #[test]
    fn rejects_oversized_single_entry_and_wrong_node_type() {
        let mut store = MemoryStore::default();
        let tree = PersistentBTree::empty(ObjectType::VolumeMapNode, &mut store).unwrap();
        assert!(matches!(
            tree.insert(&mut store, vec![1], vec![2; MAX_PERSISTENT_NODE_BYTES]),
            Err(PersistentMapError::EntryTooLarge)
        ));
        assert!(
            PersistentBTree::from_root(crate::format::ObjectRef {
                object_type: ObjectType::Extent,
                object_id: ObjectId::from_bytes([0; 32]),
                encoded_len: 16,
            })
            .is_err()
        );
    }

    #[test]
    fn rejects_key_ranges_that_cannot_fit_in_a_bounded_root() {
        let mut store = MemoryStore::default();
        let tree = PersistentBTree::empty(ObjectType::ArtifactMapNode, &mut store).unwrap();
        let tree = tree
            .insert(&mut store, vec![b'a'; 30_000], Vec::new())
            .unwrap();
        let tree = tree
            .insert(&mut store, vec![b'c'; 30_000], Vec::new())
            .unwrap();

        assert!(matches!(
            tree.insert(&mut store, vec![b'b'; 40_000], Vec::new()),
            Err(PersistentMapError::RootTooLarge)
        ));
    }

    #[test]
    fn insertion_repartitions_existing_nodes_to_fit_long_separators() {
        let entries = [
            (b"a".to_vec(), vec![1; 10_000]),
            (vec![b'b'; 40_000], Vec::new()),
            (b"c".to_vec(), vec![3; 20_000]),
            (vec![b'd'; 40_000], Vec::new()),
        ];
        let mut store = MemoryStore::default();
        let mut tree = PersistentBTree::empty(ObjectType::ArtifactMapNode, &mut store).unwrap();
        let mut model = BTreeMap::new();
        for (key, value) in &entries[..3] {
            tree = tree.insert(&mut store, key.clone(), value.clone()).unwrap();
            model.insert(key.clone(), value.clone());
        }
        let before = tree;
        let before_model = model.clone();
        let (key, value) = &entries[3];
        tree = tree.insert(&mut store, key.clone(), value.clone()).unwrap();
        model.insert(key.clone(), value.clone());

        assert_model(&tree, &store, &model);
        assert_model(&before, &store, &before_model);
        assert!(
            store
                .objects
                .values()
                .all(|bytes| bytes.len() <= MAX_PERSISTENT_NODE_BYTES)
        );
    }

    #[test]
    fn separator_partition_matches_exhaustive_bounded_layouts() {
        for sample in 0..32_usize {
            let entries: Vec<_> = (0..8)
                .map(|index| {
                    let key_len = [1, 200, 10_000, 40_000][(sample >> (index % 4 * 2)) & 3];
                    let value_len = [0, 10_000, 20_000][(sample + index * 5) % 3];
                    (vec![index as u8 + 1; key_len], vec![0; value_len])
                })
                .collect();
            let mut best = usize::MAX;
            for mask in 0..1 << (entries.len() - 1) {
                let mut start = 0;
                let mut cost = 0;
                for end in 1..=entries.len() {
                    if end != entries.len() && mask & (1 << (end - 1)) == 0 {
                        continue;
                    }
                    let encoded_size = 20
                        + entries[start..end]
                            .iter()
                            .map(|(key, value)| 8 + key.len() + value.len())
                            .sum::<usize>();
                    if encoded_size > MAX_PERSISTENT_NODE_BYTES {
                        cost = usize::MAX;
                        break;
                    }
                    cost += 46 + entries[end - 1].0.len();
                    start = end;
                }
                best = best.min(cost);
            }
            let parts = partition_for_separators(&Node::Leaf(entries.clone())).unwrap();
            let actual_cost: usize = parts
                .iter()
                .map(|part| 46 + part.max_key().unwrap().len())
                .sum();
            assert_eq!(actual_cost, best, "sample {sample}");
            for part in &parts {
                assert!(
                    encode_node(ObjectType::ArtifactMapNode, part)
                        .unwrap()
                        .len()
                        <= MAX_PERSISTENT_NODE_BYTES
                );
            }
            let actual_entries: Vec<_> = parts
                .into_iter()
                .flat_map(|part| match part {
                    Node::Leaf(entries) => entries,
                    Node::Internal { .. } => unreachable!("partition preserves node kind"),
                })
                .collect();
            assert_eq!(actual_entries, entries);
        }
    }

    #[test]
    fn deletion_rebuilds_multiple_root_levels_when_separators_grow() {
        let mut store = MemoryStore::default();
        let node_type = ObjectType::ArtifactMapNode;
        let keys = [
            vec![b'a'; 10_000],
            vec![b'b'; 20_000],
            vec![b'c'; 40_000],
            vec![b'd'],
            vec![b'e'; 26_000],
            vec![b'f'; 6_000],
        ];
        let model: BTreeMap<_, _> = keys.iter().cloned().map(|key| (key, Vec::new())).collect();
        let mut children = Vec::new();
        for indexes in [&[0][..], &[1], &[2, 3], &[4], &[5]] {
            let entries = indexes
                .iter()
                .map(|&index| (keys[index].clone(), Vec::new()))
                .collect();
            let node = Node::Leaf(entries);
            let maximum = node.max_key().unwrap().to_vec();
            let reference = store_node(&mut store, node_type, &node).unwrap();
            children.push((maximum, reference));
        }
        let root = store_node(
            &mut store,
            node_type,
            &Node::Internal { level: 1, children },
        )
        .unwrap();
        let before = PersistentBTree::from_root(root).unwrap();
        assert_model(&before, &store, &model);

        let after = before.remove(&mut store, b"d").unwrap();
        let mut remaining = model.clone();
        remaining.remove(b"d".as_slice());
        assert_model(&after, &store, &remaining);
        assert_model(&before, &store, &model);
        assert!(
            store
                .objects
                .values()
                .all(|bytes| bytes.len() <= MAX_PERSISTENT_NODE_BYTES)
        );
    }

    #[test]
    fn complete_map_walk_rejects_overlapping_child_key_ranges() {
        let mut store = MemoryStore::default();
        let node_type = ObjectType::ArtifactIndexNode;
        let left = store_node(
            &mut store,
            node_type,
            &Node::Leaf(vec![(key(1), vec![1]), (key(5), vec![5])]),
        )
        .unwrap();
        let right = store_node(
            &mut store,
            node_type,
            &Node::Leaf(vec![(key(3), vec![3]), (key(10), vec![10])]),
        )
        .unwrap();
        let root = store_node(
            &mut store,
            node_type,
            &Node::Internal {
                level: 1,
                children: vec![(key(5), left), (key(10), right)],
            },
        )
        .unwrap();

        let tree = PersistentBTree::from_root(root).unwrap();
        assert!(matches!(
            tree.entries(&store),
            Err(PersistentMapError::Format(
                crate::format::FormatError::InvalidPersistentNode(
                    "adjacent child key ranges overlap or are unordered"
                )
            ))
        ));
    }

    #[test]
    fn decoder_rejects_excessive_persistent_tree_height() {
        let node_type = ObjectType::ArtifactIndexNode;
        let child = |byte| ObjectRef {
            object_type: node_type,
            object_id: ObjectId::from_bytes([byte; 32]),
            encoded_len: 20,
        };
        let mut encoder = CanonicalEncoder::new();
        encoder.u32_le(INTERNAL_NODE_MARKER);
        encoder.u16_le(MAX_PERSISTENT_TREE_LEVEL + 1);
        encoder.u32_le(2);
        encoder.bytes(&key(1)).unwrap();
        encoder.object_ref(child(1));
        encoder.bytes(&key(2)).unwrap();
        encoder.object_ref(child(2));
        let bytes = encode_object(node_type, &encoder.finish()).unwrap();
        let payload = decode_object(&bytes).unwrap().payload;

        assert!(matches!(
            decode_node(payload, node_type),
            Err(crate::format::FormatError::InvalidPersistentNode(
                "internal nodes require a bounded positive level and at least one child"
            ))
        ));
    }
}
