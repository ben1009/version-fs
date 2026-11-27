//! Bounded mark-only retention report execution for `WorkspaceService`.
//!
//! Mark-only reports admit a durable report lifecycle, seal roots from one
//! read-only authority snapshot, release the snapshot before any persistence or
//! object-store I/O, traverse typed strong edges with node-level B-tree decoding
//! and bounded RAM + disk spill, inventory objects over a recorded Unix-ms
//! window, and persist bounded candidate pages in small batches without issuing
//! any object deletions.

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tokio::runtime::{Handle, RuntimeFlavor};
use versionfs_core::control::{ControlError, GcReportState, OwnerIncarnation, WorkspaceControl};
use versionfs_core::format::ObjectRef;
use versionfs_core::gc::{
    CANDIDATE_PERSIST_BATCH_PAGES, DiskSpillVisitedSet, DiskSpillWorkQueue, GcCandidateEntry,
    GcCandidatePage, GcError, GcReportBudget, GcReportCompleteness, GcReportSummary, GcSpillBudget,
    mark_reachable_closure,
};
use versionfs_core::identity::{ObjectId, ObjectStoreScopeId, OperationId, WorkspaceId};
use versionfs_core::metadata::MetadataAuthority;
use versionfs_core::persistent_map::MapObjectStore;

use crate::object_store::{MAX_INVENTORY_PAGE_SIZE, ObjectStore, ObjectStoreError};

/// Errors produced when collecting a mark-only GC report.
#[derive(Debug, Error)]
pub enum GcReportError {
    /// Authority or control-plane failure.
    #[error("control error: {0}")]
    Control(#[from] ControlError),
    /// Mark traversal or disk-spill failure.
    #[error("gc error: {0}")]
    Gc(#[from] GcError),
    /// Object-store inventory or read failure.
    #[error("object-store error: {0}")]
    ObjectStore(#[from] ObjectStoreError),
    /// Local filesystem I/O failure for temporary spill directory.
    #[error("spill directory I/O error: {0}")]
    Io(#[from] std::io::Error),
}

struct SpillDirGuard(PathBuf);

impl Drop for SpillDirGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct GcObjectBridge<O> {
    store: Arc<O>,
    workspace_id: WorkspaceId,
    scope: ObjectStoreScopeId,
    runtime: Handle,
}

impl<O: ObjectStore> MapObjectStore for GcObjectBridge<O> {
    fn proof_scope(&self) -> ObjectStoreScopeId {
        self.scope
    }

    fn get(&self, _id: ObjectId) -> Result<Option<Vec<u8>>, String> {
        Err("typed ObjectRef is required for workspace-scoped object reads".to_owned())
    }

    fn get_ref(&self, reference: ObjectRef) -> Result<Option<Vec<u8>>, String> {
        let store = Arc::clone(&self.store);
        let workspace_id = self.workspace_id;
        let fut = async move {
            match store.get(workspace_id, reference).await {
                Ok(bytes) => Ok(Some(bytes)),
                Err(ObjectStoreError::NotFound { .. }) => Ok(None),
                Err(err) => Err(err.to_string()),
            }
        };
        if Handle::try_current().is_ok() {
            tokio::task::block_in_place(|| self.runtime.block_on(fut))
        } else {
            self.runtime.block_on(fut)
        }
    }

    fn put(&mut self, _id: ObjectId, _bytes: Vec<u8>) -> Result<(), String> {
        Err("mark-only GC traversal is strictly read-only".to_owned())
    }
}

/// Current wall-clock time in Unix milliseconds.
pub fn unix_ms_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

fn active_sealers() -> &'static Mutex<HashSet<(WorkspaceId, OperationId, OperationId)>> {
    static ACTIVE: OnceLock<Mutex<HashSet<(WorkspaceId, OperationId, OperationId)>>> =
        OnceLock::new();
    ACTIVE.get_or_init(|| Mutex::new(HashSet::new()))
}

struct ActiveSealerGuard {
    key: (WorkspaceId, OperationId, OperationId),
}

impl ActiveSealerGuard {
    fn enter(workspace_id: WorkspaceId, report_id: OperationId, attempt_id: OperationId) -> Self {
        let key = (workspace_id, report_id, attempt_id);
        let mut guard = active_sealers()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.insert(key);
        Self { key }
    }
}

impl Drop for ActiveSealerGuard {
    fn drop(&mut self) {
        let mut guard = active_sealers()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.remove(&self.key);
    }
}

fn next_attempt_id(owner_incarnation: OwnerIncarnation) -> OperationId {
    static NEXT_ATTEMPT_SEQ: AtomicU64 = AtomicU64::new(1);
    let seq = NEXT_ATTEMPT_SEQ.fetch_add(1, Ordering::Relaxed);
    let mut bytes = [0_u8; 16];
    bytes[0..8].copy_from_slice(&owner_incarnation.as_u64().to_be_bytes());
    bytes[8..16].copy_from_slice(&seq.to_be_bytes());
    OperationId::from_bytes(bytes)
}

/// Runs a bounded, mark-only retention report for `workspace_id`.
///
/// The authority snapshot is acquired and dropped inside
/// [`WorkspaceControl::seal_gc_report_roots`] before any persistence or
/// object-store I/O occurs, so publication admission and the authority gate are
/// never held across object reads or inventory pagination.
pub fn run_mark_only_report<A, O>(
    control: &WorkspaceControl<A>,
    authority: &Arc<A>,
    object_store: &Arc<O>,
    runtime: &Handle,
    workspace_id: WorkspaceId,
    report_id: OperationId,
    raw_budget: GcReportBudget,
) -> Result<GcReportSummary, GcReportError>
where
    A: MetadataAuthority + Send + Sync + 'static,
    O: ObjectStore + Send + Sync + 'static,
{
    let budget = raw_budget.clamped_to_server_limits();
    if matches!(
        Handle::try_current(),
        Ok(current) if current.runtime_flavor() == RuntimeFlavor::CurrentThread
    ) {
        return Err(GcReportError::Io(std::io::Error::other(
            "mark-only report requires a multi-thread Tokio runtime when invoked on a runtime thread",
        )));
    }

    // Step 1: Register this executor before durable admission so any visible
    // `Sealing` state already has a live executor entry in `active_sealers()`.
    let my_root_attempt_id = next_attempt_id(control.owner_incarnation());
    let sealer_guard = ActiveSealerGuard::enter(workspace_id, report_id, my_root_attempt_id);
    let mut sealed_pair = None;
    for _ in 0..1000 {
        match control.begin_gc_report_attempt(report_id, my_root_attempt_id, budget)? {
            GcReportState::Completed { summary, .. } => return Ok(summary),
            GcReportState::Sealed {
                root_attempt_id,
                header,
                ..
            } => {
                sealed_pair = Some((root_attempt_id, header));
                break;
            }
            GcReportState::Sealing {
                root_attempt_id: active_attempt,
                ..
            } => {
                if active_attempt == my_root_attempt_id {
                    sealed_pair = Some(control.seal_gc_report_roots(
                        report_id,
                        my_root_attempt_id,
                        budget,
                    )?);
                    break;
                }
                let is_alive = active_sealers()
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .contains(&(workspace_id, report_id, active_attempt));
                if is_alive {
                    std::thread::sleep(std::time::Duration::from_millis(5));
                    continue;
                }
                match control.abort_stale_sealing_attempt(report_id, active_attempt, budget)? {
                    GcReportState::Completed { summary, .. } => return Ok(summary),
                    GcReportState::Sealed {
                        root_attempt_id,
                        header,
                        ..
                    } => {
                        sealed_pair = Some((root_attempt_id, header));
                        break;
                    }
                    _ => continue,
                }
            }
            GcReportState::Admitted { .. } => {}
            GcReportState::Aborted { .. } => return Err(ControlError::Aborted.into()),
        }
    }
    drop(sealer_guard);
    let (root_attempt_id, header) = sealed_pair.ok_or(ControlError::InProgress)?;

    // Step 2: Traverse typed strong edges using bounded RAM + shared disk spill
    // budget for both the visited set and the pending work queue.
    static NEXT_SPILL_SEQ: AtomicU64 = AtomicU64::new(0);
    let spill_seq = NEXT_SPILL_SEQ.fetch_add(1, Ordering::Relaxed);
    let spill_dir = std::env::temp_dir().join(format!(
        "versionfs-gc-spill-{report_id}-{}-{spill_seq}",
        std::process::id()
    ));
    std::fs::create_dir(&spill_dir)?;
    let _spill_guard = SpillDirGuard(spill_dir.clone());
    let spill_budget = GcSpillBudget::default();
    let mut visited = DiskSpillVisitedSet::new_with_budget(
        &spill_dir,
        budget.max_in_memory_visited,
        spill_budget.clone(),
    )?;
    let mut work_queue = DiskSpillWorkQueue::new_with_budget(
        &spill_dir,
        budget.max_in_memory_visited,
        spill_budget,
    )?;
    for page_idx in 0..header.root_page_count {
        let root_page = control.read_sealed_root_page(report_id, root_attempt_id, page_idx)?;
        for root in root_page.roots {
            work_queue.push(root)?;
        }
    }

    let bridge = GcObjectBridge {
        store: Arc::clone(object_store),
        workspace_id,
        scope: object_store.scope_id(workspace_id),
        runtime: runtime.clone(),
    };
    let mark_stats = mark_reachable_closure(&bridge, &mut work_queue, &mut visited)?;

    // Step 3: Inventory objects over a recorded Unix-ms window and build
    // bounded candidate pages without issuing any deletions.
    let scan_page_size = budget.scan_page_size.clamp(1, MAX_INVENTORY_PAGE_SIZE);
    let inventory_started_unix_ms = unix_ms_now();
    let mut observed_objects: u64 = 0;
    let mut observed_bytes: u64 = 0;
    let mut unreachable_objects: u64 = 0;
    let mut unreachable_bytes: u64 = 0;

    let mut pages: Vec<GcCandidatePage> = Vec::new();
    let mut current_entries: Vec<GcCandidateEntry> = Vec::new();
    let mut total_persisted_bytes: usize = 0;
    let mut budget_exhausted = false;

    let mut cursor = None;
    loop {
        let store = Arc::clone(object_store);
        let page_cursor = cursor.clone();
        let fut = async move {
            store
                .inventory_page(workspace_id, page_cursor.as_ref(), scan_page_size)
                .await
        };
        let inv_page = if Handle::try_current().is_ok() {
            tokio::task::block_in_place(|| runtime.block_on(fut))
        } else {
            runtime.block_on(fut)
        }?;

        for entry in inv_page.entries {
            observed_objects = observed_objects.saturating_add(1);
            observed_bytes = observed_bytes.saturating_add(entry.observed_encoded_len);

            if !visited.contains(&entry.object_id)? {
                unreachable_objects = unreachable_objects.saturating_add(1);
                unreachable_bytes = unreachable_bytes.saturating_add(entry.observed_encoded_len);

                if !budget_exhausted {
                    if budget.max_candidate_pages == 0 || budget.max_candidates_per_page == 0 {
                        budget_exhausted = true;
                    } else {
                        current_entries.push(GcCandidateEntry {
                            object_ref: ObjectRef {
                                object_type: entry.object_type,
                                object_id: entry.object_id,
                                encoded_len: entry.observed_encoded_len,
                            },
                        });
                        if current_entries.len() >= budget.max_candidates_per_page {
                            flush_candidate_page(
                                &mut pages,
                                &mut current_entries,
                                &mut total_persisted_bytes,
                                &mut budget_exhausted,
                                budget,
                            );
                        }
                    }
                }
            }
        }

        match inv_page.next_cursor {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }

    if !current_entries.is_empty() && !budget_exhausted {
        flush_candidate_page(
            &mut pages,
            &mut current_entries,
            &mut total_persisted_bytes,
            &mut budget_exhausted,
            budget,
        );
    }

    for idx in 0..pages.len() {
        pages[idx].next_page = if idx + 1 < pages.len() {
            Some(u32::try_from(idx + 1).unwrap_or(u32::MAX))
        } else {
            None
        };
    }

    // Persist candidate pages under a fresh `inventory_attempt_id` in bounded
    // batches so the authority commit gate is never held for a large transaction
    // and losing/competing attempts never mix pages with the winning summary.
    let inventory_attempt_id = next_attempt_id(control.owner_incarnation());
    for (batch_idx, batch) in pages.chunks(CANDIDATE_PERSIST_BATCH_PAGES).enumerate() {
        control.persist_gc_report_batch(
            report_id,
            inventory_attempt_id,
            u32::try_from(batch_idx).unwrap_or(u32::MAX),
            batch,
        )?;
    }

    let inventory_finished_unix_ms = unix_ms_now();
    let completeness = if mark_stats.missing_or_corrupt_objects == 0
        && header.uncertain_preparations == 0
        && !budget_exhausted
        && header.missing_metadata == 0
    {
        GcReportCompleteness::Complete
    } else {
        GcReportCompleteness::Incomplete {
            missing_or_corrupt_objects: mark_stats.missing_or_corrupt_objects,
            uncertain_preparations: header.uncertain_preparations,
            budget_exhausted,
            missing_metadata: header.missing_metadata,
        }
    };

    let detail_expires_at = authority
        .now()
        .saturating_add_nanos(budget.detail_ttl_nanos);
    let detail_expires_at_unix_ms =
        inventory_finished_unix_ms.saturating_add(budget.detail_ttl_nanos.div_ceil(1_000_000));
    let summary = GcReportSummary {
        report_id,
        boundary_time: header.boundary_time,
        sealed_root_count: header.total_roots,
        marked_objects: mark_stats.marked_objects,
        observed_objects,
        observed_bytes,
        unreachable_objects,
        unreachable_bytes,
        candidate_page_count: u32::try_from(pages.len()).unwrap_or(u32::MAX),
        inventory_started_unix_ms,
        inventory_finished_unix_ms,
        detail_expires_at,
        detail_expires_at_unix_ms,
        owner_incarnation_u64: control.owner_incarnation().as_u64(),
        completeness,
    };

    // Atomically publish summary bound to `inventory_attempt_id` with
    // first-writer-wins semantics and return the authoritative summary.
    Ok(control.finalize_gc_report(budget, inventory_attempt_id, &summary)?)
}

fn flush_candidate_page(
    pages: &mut Vec<GcCandidatePage>,
    current_entries: &mut Vec<GcCandidateEntry>,
    total_persisted_bytes: &mut usize,
    budget_exhausted: &mut bool,
    budget: GcReportBudget,
) {
    let entries = std::mem::take(current_entries);
    let page_index = u32::try_from(pages.len()).unwrap_or(u32::MAX);
    if page_index >= budget.max_candidate_pages {
        *budget_exhausted = true;
        return;
    }
    let page = GcCandidatePage {
        page_index,
        next_page: Some(page_index.saturating_add(1)),
        entries,
    };
    let encoded_len = page.encode().map(|b| b.len()).unwrap_or(usize::MAX);
    if encoded_len > versionfs_core::rpc::MAX_RPC_FRAME_BYTES
        || total_persisted_bytes.saturating_add(encoded_len) > budget.max_report_bytes
    {
        *budget_exhausted = true;
        return;
    }
    *total_persisted_bytes = total_persisted_bytes.saturating_add(encoded_len);
    pages.push(page);
}
