//! Backend-neutral P10 retention closure, disk-spill visited set, and
//! mark-only GC report structures and codecs.
//!
//! Mark-only reports never issue object deletions and never modify Generation
//! publication or retention state.

use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use thiserror::Error;

use crate::format::{
    ArtifactChunkEntry, ArtifactDescriptor, ArtifactMapRoot, CanonicalDecoder, CanonicalEncoder,
    FormatError, LogMapEntry, LogMapRoot, LogSegment, ObjectRef, ObjectType, ProvenanceContext,
    SegmentIndexEntry, VolumeRoot, decode_generation_descriptor,
};
use crate::identity::{ObjectId, OperationId};
use crate::persistent_map::{MapObjectStore, PersistentBTree};
use crate::time::MonotonicTime;

/// Errors produced during bounded mark-only graph traversal or report decoding.
#[derive(Debug, Error)]
pub enum GcError {
    /// Disk-spill I/O failure.
    #[error("gc spill I/O error: {0}")]
    SpillIo(#[from] std::io::Error),
    /// Canonical report encoding/decoding failure.
    #[error("gc format error: {0}")]
    Format(#[from] FormatError),
}

/// Configurable bounds and expiry policy for a mark-only GC report.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GcReportBudget {
    /// Maximum `ObjectId`s held in RAM before spilling a sorted run to disk.
    pub max_in_memory_visited: usize,
    /// Maximum number of candidate pages persisted per report.
    pub max_candidate_pages: u32,
    /// Maximum candidate entries stored per page.
    pub max_candidates_per_page: usize,
    /// Maximum total encoded bytes persisted across all candidate pages.
    pub max_report_bytes: usize,
    /// Duration (nanoseconds) before persisted candidate detail pages expire.
    pub detail_ttl_nanos: u64,
    /// Page size for snapshot-bound metadata prefix scans.
    pub scan_page_size: usize,
}

impl Default for GcReportBudget {
    fn default() -> Self {
        Self {
            max_in_memory_visited: 65_536,
            max_candidate_pages: 256,
            max_candidates_per_page: 512,
            max_report_bytes: 16 * 1024 * 1024,
            detail_ttl_nanos: 3_600_000_000_000,
            scan_page_size: 256,
        }
    }
}

/// Completeness status of a mark-only GC report.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GcReportCompleteness {
    /// All sealed roots, required metadata, and inventory entries were processed within budget.
    Complete,
    /// Report encountered missing/corrupt metadata, uncertain preparations, or budget limits.
    Incomplete {
        /// Number of missing or corrupt objects encountered while traversing roots.
        missing_or_corrupt_objects: u64,
        /// Number of in-progress or unresolved preparations observed at the boundary.
        uncertain_preparations: u64,
        /// Whether candidate page or byte storage budgets were exhausted.
        budget_exhausted: bool,
        /// Number of corrupt/unreadable metadata records encountered while scanning roots.
        missing_metadata: u64,
    },
}

impl GcReportCompleteness {
    /// Returns `true` when the report is complete.
    pub const fn is_complete(self) -> bool {
        matches!(self, Self::Complete)
    }
}

/// Sealed root boundary copied from one read-only authority snapshot and
/// released before any object-store I/O.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SealedRootSet {
    /// Stable report operation identity.
    pub report_id: OperationId,
    /// Authority monotonic clock timestamp when the snapshot was acquired.
    pub boundary_time: MonotonicTime,
    /// Deduplicated strong root references from retained Generations, live branches,
    /// active protections, and known preparation dependencies.
    pub roots: Vec<ObjectRef>,
    /// Number of active/uncertain `Preparing` operations observed at the boundary.
    pub uncertain_preparations: u64,
    /// Number of unreadable/corrupt authority records encountered during snapshot scan.
    pub missing_metadata: u64,
}

impl SealedRootSet {
    /// Encodes the sealed root set canonically.
    pub fn encode(&self) -> Result<Vec<u8>, FormatError> {
        let mut e = CanonicalEncoder::new();
        e.fixed(self.report_id.as_bytes());
        e.u64_le(self.boundary_time.as_nanos());
        e.u64_le(self.uncertain_preparations);
        e.u64_le(self.missing_metadata);
        let count = u32::try_from(self.roots.len())
            .map_err(|_| FormatError::InvalidEnvelope("too many sealed roots"))?;
        e.u32_le(count);
        for root in &self.roots {
            e.object_ref(*root);
        }
        Ok(e.finish())
    }

    /// Decodes a sealed root set.
    pub fn decode(bytes: &[u8]) -> Result<Self, FormatError> {
        let mut d = CanonicalDecoder::new(bytes);
        let report_id = OperationId::from_bytes(d.fixed()?);
        let boundary_time = MonotonicTime::from_nanos(d.u64_le()?);
        let uncertain_preparations = d.u64_le()?;
        let missing_metadata = d.u64_le()?;
        let count = usize::try_from(d.u32_le()?)
            .map_err(|_| FormatError::InvalidEnvelope("sealed root count overflow"))?;
        let mut roots = Vec::with_capacity(count.min(4096));
        for _ in 0..count {
            roots.push(d.object_ref()?);
        }
        d.finish()?;
        Ok(Self {
            report_id,
            boundary_time,
            roots,
            uncertain_preparations,
            missing_metadata,
        })
    }
}

/// Terminal summary for one mark-only GC report.
///
/// Summary metadata retains the operation-result contract even after
/// candidate detail pages expire (`ReportExpired`).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GcReportSummary {
    /// Stable report identity.
    pub report_id: OperationId,
    /// Authority monotonic clock boundary at snapshot time.
    pub boundary_time: MonotonicTime,
    /// Number of sealed roots enumerated from the authority snapshot.
    pub sealed_root_count: u64,
    /// Total distinct reachable objects marked during traversal.
    pub marked_objects: u64,
    /// Total object-store entries observed during the inventory window.
    pub observed_objects: u64,
    /// Total encoded bytes observed during the inventory window.
    pub observed_bytes: u64,
    /// Observed objects not reached from the sealed root set (never safe to delete blindly).
    pub unreachable_objects: u64,
    /// Observed bytes not reached from the sealed root set.
    pub unreachable_bytes: u64,
    /// Number of persisted candidate pages (`0` if none or not yet paginated).
    pub candidate_page_count: u32,
    /// Unix timestamp (ms) when object inventory began.
    pub inventory_started_unix_ms: u64,
    /// Unix timestamp (ms) when object inventory completed.
    pub inventory_finished_unix_ms: u64,
    /// Authority monotonic timestamp after which detail pages return `ReportExpired`.
    pub detail_expires_at: MonotonicTime,
    /// Completeness status of this report.
    pub completeness: GcReportCompleteness,
}

impl GcReportSummary {
    /// Encodes the summary canonically.
    pub fn encode(&self) -> Vec<u8> {
        let mut e = CanonicalEncoder::new();
        e.fixed(self.report_id.as_bytes());
        e.u64_le(self.boundary_time.as_nanos());
        e.u64_le(self.sealed_root_count);
        e.u64_le(self.marked_objects);
        e.u64_le(self.observed_objects);
        e.u64_le(self.observed_bytes);
        e.u64_le(self.unreachable_objects);
        e.u64_le(self.unreachable_bytes);
        e.u32_le(self.candidate_page_count);
        e.u64_le(self.inventory_started_unix_ms);
        e.u64_le(self.inventory_finished_unix_ms);
        e.u64_le(self.detail_expires_at.as_nanos());
        match self.completeness {
            GcReportCompleteness::Complete => e.u8(1),
            GcReportCompleteness::Incomplete {
                missing_or_corrupt_objects,
                uncertain_preparations,
                budget_exhausted,
                missing_metadata,
            } => {
                e.u8(2);
                e.u64_le(missing_or_corrupt_objects);
                e.u64_le(uncertain_preparations);
                e.u8(u8::from(budget_exhausted));
                e.u64_le(missing_metadata);
            }
        }
        e.finish()
    }

    /// Decodes a canonical `GcReportSummary`.
    pub fn decode(bytes: &[u8]) -> Result<Self, FormatError> {
        let mut d = CanonicalDecoder::new(bytes);
        let report_id = OperationId::from_bytes(d.fixed()?);
        let boundary_time = MonotonicTime::from_nanos(d.u64_le()?);
        let sealed_root_count = d.u64_le()?;
        let marked_objects = d.u64_le()?;
        let observed_objects = d.u64_le()?;
        let observed_bytes = d.u64_le()?;
        let unreachable_objects = d.u64_le()?;
        let unreachable_bytes = d.u64_le()?;
        let candidate_page_count = d.u32_le()?;
        let inventory_started_unix_ms = d.u64_le()?;
        let inventory_finished_unix_ms = d.u64_le()?;
        let detail_expires_at = MonotonicTime::from_nanos(d.u64_le()?);
        let completeness = match d.u8()? {
            1 => GcReportCompleteness::Complete,
            2 => GcReportCompleteness::Incomplete {
                missing_or_corrupt_objects: d.u64_le()?,
                uncertain_preparations: d.u64_le()?,
                budget_exhausted: d.u8()? != 0,
                missing_metadata: d.u64_le()?,
            },
            _ => return Err(FormatError::InvalidEnvelope("invalid completeness tag")),
        };
        d.finish()?;
        Ok(Self {
            report_id,
            boundary_time,
            sealed_root_count,
            marked_objects,
            observed_objects,
            observed_bytes,
            unreachable_objects,
            unreachable_bytes,
            candidate_page_count,
            inventory_started_unix_ms,
            inventory_finished_unix_ms,
            detail_expires_at,
            completeness,
        })
    }
}

/// One unreferenced-at-boundary candidate observed during object inventory.
///
/// Candidates never authorize physical deletion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GcCandidateEntry {
    /// Typed object reference observed in object inventory.
    pub object_ref: ObjectRef,
}

/// One bounded page of mark-only GC candidates.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GcCandidatePage {
    /// Zero-based page index.
    pub page_index: u32,
    /// Next page index if more pages exist.
    pub next_page: Option<u32>,
    /// Candidate entries on this page.
    pub entries: Vec<GcCandidateEntry>,
}

impl GcCandidatePage {
    /// Encodes a candidate page canonically.
    pub fn encode(&self) -> Result<Vec<u8>, FormatError> {
        let mut e = CanonicalEncoder::new();
        e.u32_le(self.page_index);
        match self.next_page {
            Some(next) => {
                e.u8(1);
                e.u32_le(next);
            }
            None => e.u8(0),
        }
        let count = u32::try_from(self.entries.len())
            .map_err(|_| FormatError::InvalidEnvelope("too many candidate entries"))?;
        e.u32_le(count);
        for entry in &self.entries {
            e.object_ref(entry.object_ref);
        }
        Ok(e.finish())
    }

    /// Decodes a canonical `GcCandidatePage`.
    pub fn decode(bytes: &[u8]) -> Result<Self, FormatError> {
        let mut d = CanonicalDecoder::new(bytes);
        let page_index = d.u32_le()?;
        let next_page = match d.u8()? {
            0 => None,
            1 => Some(d.u32_le()?),
            _ => return Err(FormatError::InvalidEnvelope("invalid next_page tag")),
        };
        let count = usize::try_from(d.u32_le()?)
            .map_err(|_| FormatError::InvalidEnvelope("candidate count overflow"))?;
        let mut entries = Vec::with_capacity(count.min(4096));
        for _ in 0..count {
            entries.push(GcCandidateEntry {
                object_ref: d.object_ref()?,
            });
        }
        d.finish()?;
        Ok(Self {
            page_index,
            next_page,
            entries,
        })
    }
}

/// Bounded in-memory visited `ObjectId` set that spills sorted 32-byte ID runs
/// to `spill_dir` when RAM exceeds `max_in_memory`.
pub struct DiskSpillVisitedSet {
    spill_dir: PathBuf,
    max_in_memory: usize,
    in_memory: HashSet<ObjectId>,
    spill_runs: Vec<(PathBuf, usize)>,
    total_marked: u64,
}

impl DiskSpillVisitedSet {
    /// Creates a bounded visited set spilling sorted runs into `spill_dir`.
    pub fn new(spill_dir: impl AsRef<Path>, max_in_memory: usize) -> Result<Self, GcError> {
        let spill_dir = spill_dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&spill_dir)?;
        Ok(Self {
            spill_dir,
            max_in_memory: max_in_memory.max(1),
            in_memory: HashSet::with_capacity(max_in_memory.min(4096)),
            spill_runs: Vec::new(),
            total_marked: 0,
        })
    }

    /// Returns the number of spilled disk run files created.
    pub fn spill_run_count(&self) -> usize {
        self.spill_runs.len()
    }

    /// Returns total distinct `ObjectId`s inserted.
    pub const fn total_marked(&self) -> u64 {
        self.total_marked
    }

    /// Returns `true` if `id` is already in the in-memory set or any spilled run.
    pub fn contains(&self, id: &ObjectId) -> Result<bool, GcError> {
        if self.in_memory.contains(id) {
            return Ok(true);
        }
        let target = id.as_bytes();
        for (path, count) in &self.spill_runs {
            if *count == 0 {
                continue;
            }
            let mut file = File::open(path)?;
            let mut low = 0_usize;
            let mut high = *count;
            let mut buf = [0_u8; 32];
            while low < high {
                let mid = low + (high - low) / 2;
                file.seek(SeekFrom::Start((mid as u64) * 32))?;
                file.read_exact(&mut buf)?;
                match buf.cmp(target) {
                    std::cmp::Ordering::Equal => return Ok(true),
                    std::cmp::Ordering::Less => low = mid + 1,
                    std::cmp::Ordering::Greater => high = mid,
                }
            }
        }
        Ok(false)
    }

    /// Inserts `id`, returning `true` if `id` was not previously present.
    pub fn insert(&mut self, id: ObjectId) -> Result<bool, GcError> {
        if self.contains(&id)? {
            return Ok(false);
        }
        if self.in_memory.len() >= self.max_in_memory {
            self.spill_to_disk()?;
        }
        if self.in_memory.insert(id) {
            self.total_marked = self.total_marked.saturating_add(1);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn spill_to_disk(&mut self) -> Result<(), GcError> {
        if self.in_memory.is_empty() {
            return Ok(());
        }
        let mut ids: Vec<ObjectId> = self.in_memory.drain().collect();
        ids.sort_unstable_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
        let run_path = self
            .spill_dir
            .join(format!("visited-run-{}.bin", self.spill_runs.len()));
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&run_path)?;
        for id in &ids {
            file.write_all(id.as_bytes())?;
        }
        file.flush()?;
        self.spill_runs.push((run_path, ids.len()));
        Ok(())
    }
}

impl Drop for DiskSpillVisitedSet {
    fn drop(&mut self) {
        for (path, _) in &self.spill_runs {
            let _ = std::fs::remove_file(path);
        }
    }
}

/// Statistics produced by [`mark_reachable_closure`].
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct GcMarkStats {
    /// Total distinct objects marked reachable.
    pub marked_objects: u64,
    /// Number of missing or corrupt objects encountered (never treated as unreachable).
    pub missing_or_corrupt_objects: u64,
}

/// Traverses all typed strong edges reachable from `sealed.roots`, populating
/// `visited`. Optional links and descriptive IDs are explicitly excluded.
pub fn mark_reachable_closure<S: MapObjectStore>(
    store: &S,
    sealed: &SealedRootSet,
    visited: &mut DiskSpillVisitedSet,
) -> Result<GcMarkStats, GcError> {
    let mut stack: Vec<ObjectRef> = sealed.roots.clone();
    let mut missing_or_corrupt_objects = 0_u64;

    while let Some(current) = stack.pop() {
        if !visited.insert(current.object_id)? {
            continue;
        }
        if is_leaf_object_type(current.object_type) {
            // Leaf payloads (Extent, ArtifactBlob, ArtifactChunk) have no outbound graph edges;
            // still verify existence/integrity when present, or record missing/corrupt.
            match store.get_ref(current) {
                Ok(Some(bytes)) => {
                    if current.verify(&bytes).is_err() {
                        missing_or_corrupt_objects = missing_or_corrupt_objects.saturating_add(1);
                    }
                }
                Ok(None) | Err(_) => {
                    missing_or_corrupt_objects = missing_or_corrupt_objects.saturating_add(1);
                }
            }
            continue;
        }

        let bytes = match store.get_ref(current) {
            Ok(Some(bytes)) => bytes,
            Ok(None) | Err(_) => {
                missing_or_corrupt_objects = missing_or_corrupt_objects.saturating_add(1);
                continue;
            }
        };
        if current.verify(&bytes).is_err() {
            missing_or_corrupt_objects = missing_or_corrupt_objects.saturating_add(1);
            continue;
        }

        match current.object_type {
            ObjectType::GenerationDescriptor => match decode_generation_descriptor(&bytes) {
                Ok(desc) => {
                    stack.push(desc.volume_root());
                    stack.push(desc.logs_root());
                    stack.push(desc.artifacts_root());
                    if let Some(ctx_ref) = desc.context_root() {
                        stack.push(ctx_ref);
                    }
                }
                Err(_) => {
                    missing_or_corrupt_objects = missing_or_corrupt_objects.saturating_add(1);
                }
            },
            ObjectType::ProvenanceContext => match ProvenanceContext::decode(&bytes) {
                Ok(ctx) => {
                    // Follow strong required evidence roots; exclude optional_links and descriptive
                    // IDs.
                    stack.push(ctx.required_logs_root);
                    stack.push(ctx.required_artifacts_root);
                }
                Err(_) => {
                    missing_or_corrupt_objects = missing_or_corrupt_objects.saturating_add(1);
                }
            },
            ObjectType::VolumeRoot => match VolumeRoot::decode(&bytes) {
                Ok(vr) => stack.push(vr.extent_map_root),
                Err(_) => {
                    missing_or_corrupt_objects = missing_or_corrupt_objects.saturating_add(1);
                }
            },
            ObjectType::VolumeMapNode => match PersistentBTree::from_root(current)
                .and_then(|tree| tree.entries_and_nodes(store))
            {
                Ok(contents) => {
                    for node_ref in contents.node_refs {
                        if node_ref != current {
                            visited.insert(node_ref.object_id)?;
                        }
                    }
                    for (_, val) in contents.entries {
                        let mut d = CanonicalDecoder::new(&val);
                        match d.object_ref().and_then(|r| {
                            d.finish()?;
                            Ok(r)
                        }) {
                            Ok(ext_ref) => stack.push(ext_ref),
                            Err(_) => {
                                missing_or_corrupt_objects =
                                    missing_or_corrupt_objects.saturating_add(1);
                            }
                        }
                    }
                }
                Err(_) => {
                    missing_or_corrupt_objects = missing_or_corrupt_objects.saturating_add(1);
                }
            },
            ObjectType::LogMapRoot => match LogMapRoot::decode(&bytes) {
                Ok(lr) => stack.push(lr.log_index_root),
                Err(_) => {
                    missing_or_corrupt_objects = missing_or_corrupt_objects.saturating_add(1);
                }
            },
            ObjectType::LogMapNode => match PersistentBTree::from_root(current)
                .and_then(|tree| tree.entries_and_nodes(store))
            {
                Ok(contents) => {
                    for node_ref in contents.node_refs {
                        if node_ref != current {
                            visited.insert(node_ref.object_id)?;
                        }
                    }
                    for (_, val) in contents.entries {
                        match LogMapEntry::decode(&val) {
                            Ok(entry) => stack.push(entry.segment_index_root),
                            Err(_) => {
                                missing_or_corrupt_objects =
                                    missing_or_corrupt_objects.saturating_add(1);
                            }
                        }
                    }
                }
                Err(_) => {
                    missing_or_corrupt_objects = missing_or_corrupt_objects.saturating_add(1);
                }
            },
            ObjectType::LogSegmentIndexNode => match PersistentBTree::from_root(current)
                .and_then(|tree| tree.entries_and_nodes(store))
            {
                Ok(contents) => {
                    for node_ref in contents.node_refs {
                        if node_ref != current {
                            visited.insert(node_ref.object_id)?;
                        }
                    }
                    for (_, val) in contents.entries {
                        match SegmentIndexEntry::decode(&val) {
                            Ok(entry) => stack.push(entry.segment),
                            Err(_) => {
                                missing_or_corrupt_objects =
                                    missing_or_corrupt_objects.saturating_add(1);
                            }
                        }
                    }
                }
                Err(_) => {
                    missing_or_corrupt_objects = missing_or_corrupt_objects.saturating_add(1);
                }
            },
            ObjectType::LogSegment => match LogSegment::decode(&bytes) {
                Ok(seg) => stack.extend(seg.attachments),
                Err(_) => {
                    missing_or_corrupt_objects = missing_or_corrupt_objects.saturating_add(1);
                }
            },
            ObjectType::ArtifactMapRoot => match ArtifactMapRoot::decode(&bytes) {
                Ok(ar) => stack.push(ar.artifact_index_root),
                Err(_) => {
                    missing_or_corrupt_objects = missing_or_corrupt_objects.saturating_add(1);
                }
            },
            ObjectType::ArtifactMapNode => match PersistentBTree::from_root(current)
                .and_then(|tree| tree.entries_and_nodes(store))
            {
                Ok(contents) => {
                    for node_ref in contents.node_refs {
                        if node_ref != current {
                            visited.insert(node_ref.object_id)?;
                        }
                    }
                    for (_, val) in contents.entries {
                        match ArtifactDescriptor::decode(&val) {
                            Ok(desc) => {
                                stack.push(desc.content_root);
                                stack.extend(desc.dependencies);
                            }
                            Err(_) => {
                                missing_or_corrupt_objects =
                                    missing_or_corrupt_objects.saturating_add(1);
                            }
                        }
                    }
                }
                Err(_) => {
                    missing_or_corrupt_objects = missing_or_corrupt_objects.saturating_add(1);
                }
            },
            ObjectType::ArtifactIndexNode => match PersistentBTree::from_root(current)
                .and_then(|tree| tree.entries_and_nodes(store))
            {
                Ok(contents) => {
                    for node_ref in contents.node_refs {
                        if node_ref != current {
                            visited.insert(node_ref.object_id)?;
                        }
                    }
                    for (_, val) in contents.entries {
                        match ArtifactChunkEntry::decode(&val) {
                            Ok(entry) => stack.push(entry.chunk),
                            Err(_) => {
                                missing_or_corrupt_objects =
                                    missing_or_corrupt_objects.saturating_add(1);
                            }
                        }
                    }
                }
                Err(_) => {
                    missing_or_corrupt_objects = missing_or_corrupt_objects.saturating_add(1);
                }
            },
            ObjectType::Extent | ObjectType::ArtifactBlob | ObjectType::ArtifactChunk => {}
        }
    }

    Ok(GcMarkStats {
        marked_objects: visited.total_marked(),
        missing_or_corrupt_objects,
    })
}

const fn is_leaf_object_type(object_type: ObjectType) -> bool {
    matches!(
        object_type,
        ObjectType::Extent | ObjectType::ArtifactBlob | ObjectType::ArtifactChunk
    )
}
