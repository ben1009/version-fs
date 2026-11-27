//! Backend-neutral P10 retention closure, disk-spill visited set, bounded
//! disk-spill work queue, and mark-only GC report structures and codecs.
//!
//! Mark-only reports never issue object deletions and never modify Generation
//! publication or retention state.

use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use thiserror::Error;

use crate::format::{
    ArtifactChunkEntry, ArtifactDescriptor, ArtifactMapRoot, CanonicalDecoder, CanonicalEncoder,
    FormatError, LogMapEntry, LogMapRoot, LogSegment, ObjectRef, ObjectType, ProvenanceContext,
    SegmentIndexEntry, VolumeRoot, decode_generation_descriptor,
};
use crate::identity::{ObjectId, OperationId};
use crate::persistent_map::{BTreeNodeEdges, MapObjectStore, PersistentBTree};
use crate::time::MonotonicTime;

/// Server hard cap for in-memory visited `ObjectId`s and work-queue items.
pub const SERVER_MAX_IN_MEMORY_VISITED: usize = 65_536;
/// Server hard cap for persisted candidate pages per report.
pub const SERVER_MAX_CANDIDATE_PAGES: u32 = 256;
/// Server hard cap for candidate entries per page.
pub const SERVER_MAX_CANDIDATES_PER_PAGE: usize = 512;
/// Server hard cap for total encoded candidate-page bytes per report (4 MiB).
pub const SERVER_MAX_REPORT_BYTES: usize = 4 * 1024 * 1024;
/// Server hard cap for metadata/inventory scan page size.
pub const SERVER_MAX_SCAN_PAGE_SIZE: usize = 1_000;
/// Server hard cap for report detail TTL (7 days).
pub const SERVER_MAX_DETAIL_TTL_NANOS: u64 = 7 * 24 * 3_600 * 1_000_000_000;
/// Maximum sealed root references stored in a single paged root record.
pub const SEALED_ROOT_PAGE_SIZE: usize = 512;
/// Maximum candidate pages persisted in one authority metadata transaction.
pub const CANDIDATE_PERSIST_BATCH_PAGES: usize = 16;
/// Hard cap on total disk bytes used by spill files for one report (64 MiB).
pub const SERVER_MAX_SPILL_BYTES: u64 = 64 * 1024 * 1024;
/// Maximum number of open spill run file descriptors retained simultaneously.
const MAX_OPEN_SPILL_FDS: usize = 8;

const WORK_QUEUE_RECORD_BYTES: usize = 42;

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
            max_in_memory_visited: SERVER_MAX_IN_MEMORY_VISITED,
            max_candidate_pages: SERVER_MAX_CANDIDATE_PAGES,
            max_candidates_per_page: SERVER_MAX_CANDIDATES_PER_PAGE,
            max_report_bytes: SERVER_MAX_REPORT_BYTES,
            detail_ttl_nanos: 3_600_000_000_000,
            scan_page_size: 256,
        }
    }
}

impl GcReportBudget {
    /// Clamps caller-supplied budgets against hard server upper bounds so no RPC
    /// caller can request unbounded RAM, disk, or authority-transaction sizing.
    pub fn clamped_to_server_limits(self) -> Self {
        Self {
            max_in_memory_visited: self
                .max_in_memory_visited
                .clamp(1, SERVER_MAX_IN_MEMORY_VISITED),
            max_candidate_pages: self.max_candidate_pages.min(SERVER_MAX_CANDIDATE_PAGES),
            max_candidates_per_page: self
                .max_candidates_per_page
                .min(SERVER_MAX_CANDIDATES_PER_PAGE),
            max_report_bytes: self.max_report_bytes.min(SERVER_MAX_REPORT_BYTES),
            detail_ttl_nanos: self.detail_ttl_nanos.min(SERVER_MAX_DETAIL_TTL_NANOS),
            scan_page_size: self.scan_page_size.clamp(1, SERVER_MAX_SCAN_PAGE_SIZE),
        }
    }

    /// Encodes the budget canonically for request fingerprinting.
    pub fn encode(&self) -> Vec<u8> {
        let mut e = CanonicalEncoder::new();
        e.u64_le(self.max_in_memory_visited as u64);
        e.u32_le(self.max_candidate_pages);
        e.u64_le(self.max_candidates_per_page as u64);
        e.u64_le(self.max_report_bytes as u64);
        e.u64_le(self.detail_ttl_nanos);
        e.u64_le(self.scan_page_size as u64);
        e.finish()
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

/// Metadata header for a paginated sealed root boundary copied from one read-only
/// authority snapshot and released before any object-store I/O.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SealedRootHeader {
    /// Stable report operation identity.
    pub report_id: OperationId,
    /// Authority monotonic clock timestamp when the snapshot was acquired.
    pub boundary_time: MonotonicTime,
    /// Number of persisted root pages (`0..root_page_count`).
    pub root_page_count: u32,
    /// Total deduplicated strong roots across all pages.
    pub total_roots: u64,
    /// Number of active/uncertain `Preparing` operations observed at the boundary.
    pub uncertain_preparations: u64,
    /// Number of unreadable/corrupt authority records encountered during snapshot scan.
    pub missing_metadata: u64,
}

impl SealedRootHeader {
    /// Encodes the sealed root header canonically.
    pub fn encode(&self) -> Vec<u8> {
        let mut e = CanonicalEncoder::new();
        e.fixed(self.report_id.as_bytes());
        e.u64_le(self.boundary_time.as_nanos());
        e.u32_le(self.root_page_count);
        e.u64_le(self.total_roots);
        e.u64_le(self.uncertain_preparations);
        e.u64_le(self.missing_metadata);
        e.finish()
    }

    /// Decodes a canonical `SealedRootHeader`.
    pub fn decode(bytes: &[u8]) -> Result<Self, FormatError> {
        let mut d = CanonicalDecoder::new(bytes);
        let report_id = OperationId::from_bytes(d.fixed()?);
        let boundary_time = MonotonicTime::from_nanos(d.u64_le()?);
        let root_page_count = d.u32_le()?;
        let total_roots = d.u64_le()?;
        let uncertain_preparations = d.u64_le()?;
        let missing_metadata = d.u64_le()?;
        d.finish()?;
        Ok(Self {
            report_id,
            boundary_time,
            root_page_count,
            total_roots,
            uncertain_preparations,
            missing_metadata,
        })
    }
}

/// One bounded page of sealed root references.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SealedRootPage {
    /// Zero-based page index.
    pub page_index: u32,
    /// Sealed root references on this page.
    pub roots: Vec<ObjectRef>,
}

impl SealedRootPage {
    /// Encodes a sealed root page canonically.
    pub fn encode(&self) -> Result<Vec<u8>, FormatError> {
        let mut e = CanonicalEncoder::new();
        e.u32_le(self.page_index);
        let count = u32::try_from(self.roots.len())
            .map_err(|_| FormatError::InvalidEnvelope("too many sealed roots in page"))?;
        e.u32_le(count);
        for root in &self.roots {
            e.object_ref(*root);
        }
        Ok(e.finish())
    }

    /// Decodes a sealed root page.
    pub fn decode(bytes: &[u8]) -> Result<Self, FormatError> {
        let mut d = CanonicalDecoder::new(bytes);
        let page_index = d.u32_le()?;
        let count = usize::try_from(d.u32_le()?)
            .map_err(|_| FormatError::InvalidEnvelope("sealed root page count overflow"))?;
        let mut roots = Vec::with_capacity(count.min(SEALED_ROOT_PAGE_SIZE));
        for _ in 0..count {
            roots.push(d.object_ref()?);
        }
        d.finish()?;
        Ok(Self { page_index, roots })
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
    /// Authority monotonic timestamp after which detail pages return `ReportExpired`
    /// within the same authority owner incarnation.
    pub detail_expires_at: MonotonicTime,
    /// Unix-ms wall-clock timestamp after which detail pages expire across authority restarts.
    pub detail_expires_at_unix_ms: u64,
    /// Authority owner incarnation under which `detail_expires_at` was computed.
    pub owner_incarnation_u64: u64,
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
        e.u64_le(self.detail_expires_at_unix_ms);
        e.u64_le(self.owner_incarnation_u64);
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
        let detail_expires_at_unix_ms = d.u64_le()?;
        let owner_incarnation_u64 = d.u64_le()?;
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
            detail_expires_at_unix_ms,
            owner_incarnation_u64,
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

/// Shared disk-byte budget across a report's visited-set and work-queue spill files.
#[derive(Clone, Debug)]
pub struct GcSpillBudget {
    used_bytes: Arc<AtomicU64>,
    max_bytes: u64,
}

impl Default for GcSpillBudget {
    fn default() -> Self {
        Self::new(SERVER_MAX_SPILL_BYTES)
    }
}

impl GcSpillBudget {
    /// Creates a new shared spill disk budget capped at `max_bytes`.
    pub fn new(max_bytes: u64) -> Self {
        Self {
            used_bytes: Arc::new(AtomicU64::new(0)),
            max_bytes,
        }
    }

    /// Returns the number of spill disk bytes currently accounted for.
    pub fn used_bytes(&self) -> u64 {
        self.used_bytes.load(Ordering::Acquire)
    }

    fn try_reserve(&self, bytes: u64) -> Result<(), GcError> {
        let mut cur = self.used_bytes.load(Ordering::Acquire);
        loop {
            let next = cur
                .checked_add(bytes)
                .filter(|&v| v <= self.max_bytes)
                .ok_or_else(|| {
                    GcError::SpillIo(std::io::Error::other("gc spill disk budget exhausted"))
                })?;
            match self.used_bytes.compare_exchange_weak(
                cur,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(()),
                Err(actual) => cur = actual,
            }
        }
    }

    fn release(&self, bytes: u64) {
        let mut cur = self.used_bytes.load(Ordering::Acquire);
        loop {
            let next = cur.saturating_sub(bytes);
            match self.used_bytes.compare_exchange_weak(
                cur,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(actual) => cur = actual,
            }
        }
    }
}

struct SpillRun {
    path: PathBuf,
    file: Option<File>,
    count: usize,
    min_id: ObjectId,
    max_id: ObjectId,
}

/// Bounded in-memory visited set with deterministic sorted-run disk spill.
pub struct DiskSpillVisitedSet {
    in_memory: HashSet<ObjectId>,
    max_in_memory: usize,
    spill_dir: PathBuf,
    spill_runs: Vec<SpillRun>,
    budget: GcSpillBudget,
    total_marked: u64,
}

impl DiskSpillVisitedSet {
    /// Creates a bounded visited set spilling to `spill_dir` once `max_in_memory`
    /// distinct IDs are held in RAM, capped by [`SERVER_MAX_SPILL_BYTES`] on disk
    /// and a bounded pool of open file descriptors.
    pub fn new(spill_dir: impl Into<PathBuf>, max_in_memory: usize) -> Result<Self, GcError> {
        Self::new_with_budget(spill_dir, max_in_memory, GcSpillBudget::default())
    }

    /// Creates a bounded visited set sharing `budget` with other spill structures.
    pub fn new_with_budget(
        spill_dir: impl Into<PathBuf>,
        max_in_memory: usize,
        budget: GcSpillBudget,
    ) -> Result<Self, GcError> {
        let spill_dir = spill_dir.into();
        std::fs::create_dir_all(&spill_dir)?;
        Ok(Self {
            in_memory: HashSet::with_capacity(max_in_memory.min(4096)),
            max_in_memory: max_in_memory.max(1),
            spill_dir,
            spill_runs: Vec::new(),
            budget,
            total_marked: 0,
        })
    }

    /// Total distinct `ObjectId`s marked across memory and disk-spilled runs.
    pub const fn total_marked(&self) -> u64 {
        self.total_marked
    }

    /// Number of disk-spilled sorted runs written so far.
    pub fn spill_run_count(&self) -> usize {
        self.spill_runs.len()
    }

    /// Returns `true` if `id` is in the visited set (checking memory and spilled runs).
    pub fn contains(&self, id: &ObjectId) -> Result<bool, GcError> {
        if self.in_memory.contains(id) {
            return Ok(true);
        }
        let target = id.as_bytes();
        for run in &self.spill_runs {
            if target < run.min_id.as_bytes() || target > run.max_id.as_bytes() {
                continue;
            }
            let opened;
            let file: &File = if let Some(ref cached) = run.file {
                cached
            } else {
                opened = File::open(&run.path)?;
                &opened
            };
            let mut file_ref = file;
            let mut low = 0_usize;
            let mut high = run.count;
            let mut buf = [0_u8; 32];
            while low < high {
                let mid = low + (high - low) / 2;
                file_ref.seek(SeekFrom::Start((mid as u64) * 32))?;
                file_ref.read_exact(&mut buf)?;
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
        let added_bytes = (self.in_memory.len() as u64).saturating_mul(32);
        self.budget.try_reserve(added_bytes)?;
        let mut ids: Vec<ObjectId> = self.in_memory.iter().copied().collect();
        ids.sort_unstable_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
        let min_id = ids[0];
        let max_id = ids[ids.len() - 1];
        let run_path = self
            .spill_dir
            .join(format!("visited-run-{}.bin", self.spill_runs.len()));
        let mut file = match OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&run_path)
        {
            Ok(f) => f,
            Err(err) => {
                self.budget.release(added_bytes);
                return Err(err.into());
            }
        };
        for id in &ids {
            if let Err(err) = file.write_all(id.as_bytes()) {
                self.budget.release(added_bytes);
                let _ = std::fs::remove_file(&run_path);
                return Err(err.into());
            }
        }
        if let Err(err) = file.flush() {
            self.budget.release(added_bytes);
            let _ = std::fs::remove_file(&run_path);
            return Err(err.into());
        }
        let cached_file = if self.spill_runs.len() < MAX_OPEN_SPILL_FDS {
            Some(file)
        } else {
            drop(file);
            None
        };
        self.spill_runs.push(SpillRun {
            path: run_path,
            file: cached_file,
            count: ids.len(),
            min_id,
            max_id,
        });
        self.in_memory.clear();
        Ok(())
    }
}

impl Drop for DiskSpillVisitedSet {
    fn drop(&mut self) {
        for run in &self.spill_runs {
            if std::fs::remove_file(&run.path).is_ok() {
                self.budget.release((run.count as u64).saturating_mul(32));
            }
        }
    }
}

/// Bounded in-memory LIFO work queue that spills overflow chunks to disk.
pub struct DiskSpillWorkQueue {
    in_memory: Vec<ObjectRef>,
    max_in_memory: usize,
    spill_dir: PathBuf,
    spill_chunks: Vec<(PathBuf, usize)>,
    budget: GcSpillBudget,
}

impl DiskSpillWorkQueue {
    /// Creates a bounded work queue spilling overflow chunks to `spill_dir`,
    /// capped by [`SERVER_MAX_SPILL_BYTES`] on disk.
    pub fn new(spill_dir: impl Into<PathBuf>, max_in_memory: usize) -> Result<Self, GcError> {
        Self::new_with_budget(spill_dir, max_in_memory, GcSpillBudget::default())
    }

    /// Creates a bounded work queue sharing `budget` with other spill structures.
    pub fn new_with_budget(
        spill_dir: impl Into<PathBuf>,
        max_in_memory: usize,
        budget: GcSpillBudget,
    ) -> Result<Self, GcError> {
        let spill_dir = spill_dir.into();
        std::fs::create_dir_all(&spill_dir)?;
        Ok(Self {
            in_memory: Vec::with_capacity(max_in_memory.min(4096)),
            max_in_memory: max_in_memory.max(1),
            spill_dir,
            spill_chunks: Vec::new(),
            budget,
        })
    }

    /// Pushes one `ObjectRef`, spilling half the buffer to disk when full.
    pub fn push(&mut self, reference: ObjectRef) -> Result<(), GcError> {
        if self.in_memory.len() >= self.max_in_memory {
            self.spill_oldest_chunk()?;
        }
        self.in_memory.push(reference);
        Ok(())
    }

    /// Pops the next `ObjectRef`, reloading a spilled chunk if memory is empty.
    pub fn pop(&mut self) -> Result<Option<ObjectRef>, GcError> {
        if let Some(item) = self.in_memory.pop() {
            return Ok(Some(item));
        }
        let Some((path, count)) = self.spill_chunks.pop() else {
            return Ok(None);
        };
        let mut file = File::open(&path)?;
        let mut buf = [0_u8; WORK_QUEUE_RECORD_BYTES];
        for _ in 0..count {
            file.read_exact(&mut buf)?;
            let type_code = u16::from_le_bytes([buf[0], buf[1]]);
            let object_type = ObjectType::try_from(type_code)?;
            let mut id_bytes = [0_u8; 32];
            id_bytes.copy_from_slice(&buf[2..34]);
            let mut len_bytes = [0_u8; 8];
            len_bytes.copy_from_slice(&buf[34..42]);
            self.in_memory.push(ObjectRef {
                object_type,
                object_id: ObjectId::from_bytes(id_bytes),
                encoded_len: u64::from_le_bytes(len_bytes),
            });
        }
        if std::fs::remove_file(path).is_ok() {
            self.budget
                .release((count as u64).saturating_mul(WORK_QUEUE_RECORD_BYTES as u64));
        }
        Ok(self.in_memory.pop())
    }

    fn spill_oldest_chunk(&mut self) -> Result<(), GcError> {
        let spill_count = (self.in_memory.len() / 2).max(1);
        let chunk_bytes = (spill_count as u64).saturating_mul(WORK_QUEUE_RECORD_BYTES as u64);
        self.budget.try_reserve(chunk_bytes)?;
        let path = self
            .spill_dir
            .join(format!("work-chunk-{}.bin", self.spill_chunks.len()));
        let mut file = match OpenOptions::new().create_new(true).write(true).open(&path) {
            Ok(f) => f,
            Err(err) => {
                self.budget.release(chunk_bytes);
                return Err(err.into());
            }
        };
        let mut buf = [0_u8; WORK_QUEUE_RECORD_BYTES];
        for item in &self.in_memory[..spill_count] {
            buf[0..2].copy_from_slice(&(item.object_type as u16).to_le_bytes());
            buf[2..34].copy_from_slice(item.object_id.as_bytes());
            buf[34..42].copy_from_slice(&item.encoded_len.to_le_bytes());
            if let Err(err) = file.write_all(&buf) {
                self.budget.release(chunk_bytes);
                let _ = std::fs::remove_file(&path);
                return Err(err.into());
            }
        }
        if let Err(err) = file.flush() {
            self.budget.release(chunk_bytes);
            let _ = std::fs::remove_file(&path);
            return Err(err.into());
        }
        self.in_memory.drain(..spill_count);
        self.spill_chunks.push((path, spill_count));
        Ok(())
    }
}

impl Drop for DiskSpillWorkQueue {
    fn drop(&mut self) {
        for (path, count) in &self.spill_chunks {
            if std::fs::remove_file(path).is_ok() {
                self.budget
                    .release((*count as u64).saturating_mul(WORK_QUEUE_RECORD_BYTES as u64));
            }
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

/// Traverses all typed strong edges reachable from `queue` using node-level B-tree
/// decoding, populating `visited`. Shared COW subtrees already present in `visited`
/// are skipped before fetching or decoding; `optional_links` and descriptive IDs
/// are explicitly excluded.
pub fn mark_reachable_closure<S: MapObjectStore>(
    store: &S,
    queue: &mut DiskSpillWorkQueue,
    visited: &mut DiskSpillVisitedSet,
) -> Result<GcMarkStats, GcError> {
    let mut missing_or_corrupt_objects = 0_u64;

    while let Some(current) = queue.pop()? {
        if !visited.insert(current.object_id)? {
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
                    queue.push(desc.volume_root())?;
                    queue.push(desc.logs_root())?;
                    queue.push(desc.artifacts_root())?;
                    if let Some(ctx_ref) = desc.context_root() {
                        queue.push(ctx_ref)?;
                    }
                }
                Err(_) => {
                    missing_or_corrupt_objects = missing_or_corrupt_objects.saturating_add(1);
                }
            },
            ObjectType::ProvenanceContext => match ProvenanceContext::decode(&bytes) {
                Ok(ctx) => {
                    queue.push(ctx.required_logs_root)?;
                    queue.push(ctx.required_artifacts_root)?;
                }
                Err(_) => {
                    missing_or_corrupt_objects = missing_or_corrupt_objects.saturating_add(1);
                }
            },
            ObjectType::VolumeRoot => match VolumeRoot::decode(&bytes) {
                Ok(vr) => queue.push(vr.extent_map_root)?,
                Err(_) => {
                    missing_or_corrupt_objects = missing_or_corrupt_objects.saturating_add(1);
                }
            },
            ObjectType::VolumeMapNode => {
                match PersistentBTree::decode_node_edges(current, &bytes) {
                    Ok(BTreeNodeEdges::Internal { children }) => {
                        for child in children {
                            queue.push(child)?;
                        }
                    }
                    Ok(BTreeNodeEdges::Leaf { entries }) => {
                        for (_, val) in entries {
                            let mut d = CanonicalDecoder::new(&val);
                            match d.object_ref().and_then(|r| {
                                d.finish()?;
                                Ok(r)
                            }) {
                                Ok(ext_ref) => queue.push(ext_ref)?,
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
                }
            }
            ObjectType::LogMapRoot => match LogMapRoot::decode(&bytes) {
                Ok(lr) => queue.push(lr.log_index_root)?,
                Err(_) => {
                    missing_or_corrupt_objects = missing_or_corrupt_objects.saturating_add(1);
                }
            },
            ObjectType::LogMapNode => match PersistentBTree::decode_node_edges(current, &bytes) {
                Ok(BTreeNodeEdges::Internal { children }) => {
                    for child in children {
                        queue.push(child)?;
                    }
                }
                Ok(BTreeNodeEdges::Leaf { entries }) => {
                    for (_, val) in entries {
                        match LogMapEntry::decode(&val) {
                            Ok(entry) => queue.push(entry.segment_index_root)?,
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
            ObjectType::LogSegmentIndexNode => {
                match PersistentBTree::decode_node_edges(current, &bytes) {
                    Ok(BTreeNodeEdges::Internal { children }) => {
                        for child in children {
                            queue.push(child)?;
                        }
                    }
                    Ok(BTreeNodeEdges::Leaf { entries }) => {
                        for (_, val) in entries {
                            match SegmentIndexEntry::decode(&val) {
                                Ok(entry) => queue.push(entry.segment)?,
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
                }
            }
            ObjectType::LogSegment => match LogSegment::decode(&bytes) {
                Ok(seg) => {
                    for att in seg.attachments {
                        queue.push(att)?;
                    }
                }
                Err(_) => {
                    missing_or_corrupt_objects = missing_or_corrupt_objects.saturating_add(1);
                }
            },
            ObjectType::ArtifactMapRoot => match ArtifactMapRoot::decode(&bytes) {
                Ok(ar) => queue.push(ar.artifact_index_root)?,
                Err(_) => {
                    missing_or_corrupt_objects = missing_or_corrupt_objects.saturating_add(1);
                }
            },
            ObjectType::ArtifactMapNode => {
                match PersistentBTree::decode_node_edges(current, &bytes) {
                    Ok(BTreeNodeEdges::Internal { children }) => {
                        for child in children {
                            queue.push(child)?;
                        }
                    }
                    Ok(BTreeNodeEdges::Leaf { entries }) => {
                        for (_, val) in entries {
                            match ArtifactDescriptor::decode(&val) {
                                Ok(desc) => {
                                    queue.push(desc.content_root)?;
                                    for dep in desc.dependencies {
                                        queue.push(dep)?;
                                    }
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
                }
            }
            ObjectType::ArtifactIndexNode => {
                match PersistentBTree::decode_node_edges(current, &bytes) {
                    Ok(BTreeNodeEdges::Internal { children }) => {
                        for child in children {
                            queue.push(child)?;
                        }
                    }
                    Ok(BTreeNodeEdges::Leaf { entries }) => {
                        for (_, val) in entries {
                            match ArtifactChunkEntry::decode(&val) {
                                Ok(entry) => queue.push(entry.chunk)?,
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
                }
            }
            ObjectType::Extent | ObjectType::ArtifactBlob | ObjectType::ArtifactChunk => {}
        }
    }

    Ok(GcMarkStats {
        marked_objects: visited.total_marked(),
        missing_or_corrupt_objects,
    })
}
