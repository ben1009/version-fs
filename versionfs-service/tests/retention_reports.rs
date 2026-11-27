//! P10 retention, provenance-aware reachability, disk-spill visited set,
//! budget/expiry, and mark-only GC report integration tests.

use std::net::TcpListener;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use versionfs_core::control::{
    AttachmentState, BranchState, PreparationKind, PreparationRequest, VerifiedGenerationSeed,
    WorkspaceControl, WriterGrant, start_authority_owner,
};
use versionfs_core::fault::NoFailures;
use versionfs_core::format::{
    ArtifactDescriptor, CaptureMode, EXTENT_BYTES, GenerationDescriptorV2, LogMapEntry, LogSegment,
    ObjectRef, ObjectType, ProvenanceBinding, ProvenanceContext, SegmentIndexEntry, VolumeGeometry,
    VolumeRoot, encode_object,
};
use versionfs_core::gc::{GcReportBudget, GcReportCompleteness};
use versionfs_core::graph::verify_generation;
use versionfs_core::identity::{
    ArtifactId, AttachmentId, BranchId, GenerationId, LogId, ObjectId, ObjectStoreScopeId,
    OperationId, PrincipalId, ProtectionId, VolumeBoundaryId, WorkspaceId,
};
use versionfs_core::persistent_map::{DurableObjectStore, MapObjectStore};
use versionfs_core::rpc::{RpcAuth, ServiceErrorCode};
use versionfs_core::time::{Clock, MonotonicTime};
use versionfs_core::views::{
    ArtifactMap, LogMap, LogSegmentIndex, VolumeExtentMap, encode_artifact_blob,
};
use versionfs_sdk::barrier::VolumeBarrierControl;
use versionfs_sdk::capture::CaptureControl;
use versionfs_sdk::client::{ProtectedReadHandle, ServiceClient, ServiceEndpoint};
use versionfs_service::object_store::{DirectoryObjectStore, ObjectStore};
use versionfs_service::service::{ServiceConfig, WorkspaceService, serve_tcp};
use versionfs_service::toy_kv_metadata::ToyKvMetadataAuthority;

static NEXT_DIR: AtomicU64 = AtomicU64::new(0);
static NEXT_ID: AtomicU64 = AtomicU64::new(1);

struct TempDir(std::path::PathBuf);

impl TempDir {
    fn new() -> Self {
        let id = NEXT_DIR.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "versionfs-retention-reports-{}-{id}",
            std::process::id()
        ));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn id16(tag: u8) -> [u8; 16] {
    let n = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    let mut bytes = [tag; 16];
    bytes[8..16].copy_from_slice(&n.to_be_bytes());
    bytes
}

struct TestClock(AtomicU64);

impl TestClock {
    fn new(start: u64) -> Self {
        Self(AtomicU64::new(start))
    }

    fn advance(&self, delta: u64) {
        self.0.fetch_add(delta, Ordering::Relaxed);
    }
}

impl Clock for TestClock {
    fn now(&self) -> MonotonicTime {
        MonotonicTime::from_nanos(self.0.load(Ordering::Relaxed))
    }
}

struct TestHarness {
    _dir: TempDir,
    rt: tokio::runtime::Runtime,
    clock: Arc<TestClock>,
    authority: Arc<ToyKvMetadataAuthority>,
    incarnation: versionfs_core::control::OwnerIncarnation,
    object_store: Arc<DirectoryObjectStore>,
    endpoint: ServiceEndpoint,
    shutdown: Arc<AtomicBool>,
    server_thread: Option<std::thread::JoinHandle<()>>,
}

impl TestHarness {
    fn start() -> Self {
        let dir = TempDir::new();
        let meta_dir = dir.0.join("meta");
        let obj_dir = dir.0.join("objects");
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let clock = Arc::new(TestClock::new(1_000_000_000));
        let authority = Arc::new(
            ToyKvMetadataAuthority::open_with(
                &meta_dir,
                Arc::clone(&clock) as Arc<dyn Clock>,
                Arc::new(NoFailures),
            )
            .unwrap(),
        );
        let incarnation =
            start_authority_owner(authority.as_ref(), OperationId::from_bytes(id16(1))).unwrap();
        let object_store = Arc::new(DirectoryObjectStore::open(&obj_dir).unwrap());
        let service = Arc::new(WorkspaceService::new(
            Arc::clone(&authority),
            incarnation,
            Arc::clone(&object_store),
            rt.handle().clone(),
            ServiceConfig::new(b"secret-token".to_vec()),
        ));
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_bg = Arc::clone(&shutdown);
        let server_thread = std::thread::spawn(move || {
            let _ = serve_tcp(service, listener, shutdown_bg);
        });

        Self {
            _dir: dir,
            rt,
            clock,
            authority,
            incarnation,
            object_store,
            endpoint: ServiceEndpoint::Tcp(addr),
            shutdown,
            server_thread: Some(server_thread),
        }
    }

    fn client(&self, workspace_id: WorkspaceId, principal_id: PrincipalId) -> ServiceClient {
        ServiceClient::connect(
            self.endpoint.clone(),
            RpcAuth {
                workspace_id,
                principal_id,
                auth_token: b"secret-token".to_vec(),
            },
        )
        .unwrap()
    }

    fn put_obj(&self, workspace_id: WorkspaceId, encoded: &[u8]) -> ObjectRef {
        self.rt
            .block_on(self.object_store.put(workspace_id, encoded))
            .unwrap()
            .object_ref
    }
}

impl Drop for TestHarness {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        if let Some(handle) = self.server_thread.take() {
            let _ = handle.join();
        }
    }
}

struct SyncStore<'a> {
    rt: &'a tokio::runtime::Runtime,
    store: &'a Arc<DirectoryObjectStore>,
    ws: WorkspaceId,
}

impl MapObjectStore for SyncStore<'_> {
    fn proof_scope(&self) -> ObjectStoreScopeId {
        self.store.scope_id(self.ws)
    }

    fn get(&self, _id: ObjectId) -> Result<Option<Vec<u8>>, String> {
        Err("typed ObjectRef required".to_owned())
    }

    fn get_ref(&self, reference: ObjectRef) -> Result<Option<Vec<u8>>, String> {
        self.rt.block_on(async {
            match self.store.get(self.ws, reference).await {
                Ok(b) => Ok(Some(b)),
                Err(versionfs_service::object_store::ObjectStoreError::NotFound { .. }) => Ok(None),
                Err(e) => Err(e.to_string()),
            }
        })
    }

    fn put(&mut self, _id: ObjectId, bytes: Vec<u8>) -> Result<(), String> {
        let reference = ObjectRef::from_encoded(&bytes).map_err(|e| e.to_string())?;
        self.put_durable(reference, &bytes)
    }
}

impl DurableObjectStore for SyncStore<'_> {
    fn put_durable(&mut self, _reference: ObjectRef, encoded: &[u8]) -> Result<(), String> {
        self.rt
            .block_on(self.store.put(self.ws, encoded))
            .map(|_| ())
            .map_err(|e| e.to_string())
    }
}

fn setup_workspace(
    harness: &TestHarness,
    ws: WorkspaceId,
    principal: PrincipalId,
) -> (
    ServiceClient,
    BranchId,
    VolumeBoundaryId,
    BranchState,
    WriterGrant,
    AttachmentState,
) {
    let client = harness.client(ws, principal);
    let main_branch = BranchId::from_bytes(id16(2));
    let boundary_id = VolumeBoundaryId::from_bytes(id16(5));
    let geometry = VolumeGeometry::new(64 * 1024).unwrap();
    let init = client
        .create_empty_workspace(
            OperationId::from_bytes(id16(3)),
            GenerationId::from_bytes(id16(4)),
            boundary_id,
            geometry,
            1_700_000_000_000,
            main_branch,
            "main",
            60_000,
            OperationId::from_bytes(id16(6)),
            OperationId::from_bytes(id16(7)),
            AttachmentId::from_bytes(id16(8)),
            ProtectionId::from_bytes(id16(9)),
        )
        .unwrap();
    (
        client,
        main_branch,
        boundary_id,
        init.branch,
        init.writer_grant,
        init.attachment,
    )
}

#[allow(clippy::too_many_arguments)]
fn publish_checkpoint_with_roots(
    harness: &TestHarness,
    client: &ServiceClient,
    ws: WorkspaceId,
    main_branch: BranchId,
    boundary_id: VolumeBoundaryId,
    mut branch: BranchState,
    grant: WriterGrant,
    att: AttachmentState,
    provenance: ProvenanceBinding,
    new_volume_root: ObjectRef,
) -> (BranchState, GenerationId) {
    let prep_op = OperationId::from_bytes(id16(50));
    let roots = versionfs_core::control::LiveRoots {
        volume: new_volume_root,
        logs: branch.logs_root,
        artifacts: branch.artifacts_root,
    };
    client
        .begin_preparation(PreparationRequest {
            operation_id: prep_op,
            branch_id: main_branch,
            expected_revision: branch.revision,
            grant,
            attachment_id: att.attachment_id,
            volume_epoch: att.volume_epoch,
            protection_id: ProtectionId::from_bytes(id16(51)),
            kind: PreparationKind::VolumeBarrier,
            expected_roots: Some(roots),
            dependencies: Vec::new(),
        })
        .unwrap();
    branch = client
        .commit_live_roots(prep_op, branch.revision, grant, roots, None)
        .unwrap();

    let cap_op = OperationId::from_bytes(id16(52));
    client
        .begin_capture(
            PreparationRequest {
                operation_id: cap_op,
                branch_id: main_branch,
                expected_revision: branch.revision,
                grant,
                attachment_id: att.attachment_id,
                volume_epoch: att.volume_epoch,
                protection_id: ProtectionId::from_bytes(id16(53)),
                kind: PreparationKind::CheckpointCapture,
                expected_roots: None,
                dependencies: Vec::new(),
            },
            provenance,
        )
        .unwrap();
    client
        .seal_capture(
            OperationId::from_bytes(id16(54)),
            cap_op,
            branch.revision,
            grant,
        )
        .unwrap();

    let g_next_id = GenerationId::from_bytes(id16(55));
    let g_next_bytes = GenerationDescriptorV2 {
        generation_id: g_next_id,
        parent_generation_id: Some(branch.head),
        volume_root: new_volume_root,
        logs_root: branch.logs_root,
        artifacts_root: branch.artifacts_root,
        provenance,
        capture_mode: CaptureMode::BlockCut,
        volume_boundary_id: boundary_id,
        created_at_unix_ms: 1_700_000_002_000,
    }
    .encode()
    .unwrap();
    let g_next_ref = harness.put_obj(ws, &g_next_bytes);

    let store = SyncStore {
        rt: &harness.rt,
        store: &harness.object_store,
        ws,
    };
    let seed = VerifiedGenerationSeed::from_proof(verify_generation(&store, g_next_ref).unwrap());
    let published = client
        .publish_capture(
            cap_op,
            branch.revision,
            branch.head,
            grant,
            att.attachment_id,
            att.volume_epoch,
            seed,
            true,
        )
        .unwrap();
    (published.branch, g_next_id)
}

#[test]
fn mark_only_report_identifies_orphans_never_deletes_and_does_not_revive_unretained_generation() {
    let harness = TestHarness::start();
    let ws = WorkspaceId::from_bytes(id16(10));
    let principal = PrincipalId::from_bytes(id16(11));
    let (client, main_branch, boundary_id, branch, grant, att) =
        setup_workspace(&harness, ws, principal);

    let gen1_id = branch.head;
    let mut store = SyncStore {
        rt: &harness.rt,
        store: &harness.object_store,
        ws,
    };

    // Put an unreferenced orphan object directly into DirectoryObjectStore.
    let orphan_blob = encode_artifact_blob(b"orphan-data").unwrap();
    let orphan_ref = harness.put_obj(ws, &orphan_blob);

    // Mutate volume root and publish generation #2.
    let vol_bytes = store.get_ref(branch.volume_root).unwrap().unwrap();
    let vol2 = VolumeExtentMap::from_root(VolumeRoot::decode(&vol_bytes).unwrap()).unwrap();
    let ext_bytes = encode_object(ObjectType::Extent, &[0x5a_u8; EXTENT_BYTES as usize]).unwrap();
    let ext_ref = harness.put_obj(ws, &ext_bytes);
    let vol2 = vol2.insert_extent(&mut store, 0, ext_ref).unwrap();
    let vol2_root = vol2.seal(&mut store).unwrap();

    let (_branch2, _gen2_id) = publish_checkpoint_with_roots(
        &harness,
        &client,
        ws,
        main_branch,
        boundary_id,
        branch,
        grant,
        att,
        ProvenanceBinding::Unattributed,
        vol2_root,
    );

    // Collect mark-only report: orphan_ref must appear on page 0 and must NOT be deleted.
    let report1_id = OperationId::from_bytes(id16(14));
    let summary1 = client.collect_report(report1_id, None).unwrap();
    assert_eq!(summary1.completeness, GcReportCompleteness::Complete);
    assert!(summary1.unreachable_objects >= 1);
    assert_eq!(summary1.candidate_page_count, 1);

    let (_, page0) = client.get_gc_report(report1_id, Some(0)).unwrap();
    let page0 = page0.expect("page 0 must exist");
    let candidate_ids: Vec<ObjectId> = page0
        .entries
        .iter()
        .map(|entry| entry.object_ref.object_id)
        .collect();
    assert!(candidate_ids.contains(&orphan_ref.object_id));
    assert!(!candidate_ids.contains(&ext_ref.object_id));

    // Physical bytes remain intact (reports never delete).
    assert!(
        harness
            .rt
            .block_on(harness.object_store.get(ws, orphan_ref))
            .is_ok()
    );

    // Mark generation #1 unretained. Even though its bytes remain in ObjectStore,
    // protection against it must be rejected (bytes never revive eligibility).
    let control = WorkspaceControl::new(
        Arc::clone(&harness.authority),
        ws,
        harness.object_store.scope_id(ws),
        principal,
        harness.incarnation,
    );
    control
        .mark_generation_unretained(OperationId::from_bytes(id16(15)), gen1_id)
        .unwrap();

    let prot_err = ProtectedReadHandle::open(
        client.clone(),
        OperationId::from_bytes(id16(16)),
        gen1_id,
        ProtectionId::from_bytes(id16(17)),
    )
    .err()
    .expect("unretained generation must not open protected read");
    assert!(matches!(
        prot_err,
        versionfs_core::control::ControlError::GenerationExpired
    ));
}

#[test]
fn provenance_required_evidence_kept_optional_links_excluded_and_disk_spill_exercised() {
    let harness = TestHarness::start();
    let ws = WorkspaceId::from_bytes(id16(20));
    let principal = PrincipalId::from_bytes(id16(21));
    let (client, main_branch, boundary_id, branch, grant, att) =
        setup_workspace(&harness, ws, principal);

    let mut store = SyncStore {
        rt: &harness.rt,
        store: &harness.object_store,
        ws,
    };

    // Put required provenance evidence outside the ordinary view roots, plus an optional link
    // target that MUST NOT be retained by strong-edge reachability.
    let req_log_id = LogId::from_bytes(id16(22));
    let req_log_seg = LogSegment {
        log_id: req_log_id,
        first_seq: 0,
        end_seq: 1,
        records: vec![b"evidence-record".to_vec()],
        attachments: Vec::new(),
    }
    .encode()
    .unwrap();
    let req_log_seg_ref = harness.put_obj(ws, &req_log_seg);

    let mut req_idx = LogSegmentIndex::empty(&mut store).unwrap();
    req_idx = req_idx
        .insert(
            &mut store,
            0,
            SegmentIndexEntry {
                end_seq: 1,
                segment: req_log_seg_ref,
            },
        )
        .unwrap();
    let mut req_logs = LogMap::empty(&mut store).unwrap();
    req_logs = req_logs
        .insert(
            &mut store,
            "req-events",
            LogMapEntry {
                log_id: req_log_id,
                first_seq: 0,
                end_seq: 1,
                segment_index_root: req_idx.root(),
            },
        )
        .unwrap();
    let req_logs_root = req_logs.seal(&mut store).unwrap();

    let req_art_blob = encode_artifact_blob(b"req-art").unwrap();
    let req_art_blob_ref = harness.put_obj(ws, &req_art_blob);
    let mut req_arts = ArtifactMap::empty(&mut store).unwrap();
    req_arts = req_arts
        .insert(
            &mut store,
            "req.bin",
            &ArtifactDescriptor {
                artifact_id: ArtifactId::from_bytes(id16(23)),
                logical_len: 7,
                content_root: req_art_blob_ref,
                dependencies: Vec::new(),
            },
        )
        .unwrap();
    let req_arts_root = req_arts.seal(&mut store).unwrap();

    let optional_target = encode_artifact_blob(b"optional-only").unwrap();
    let optional_target_ref = harness.put_obj(ws, &optional_target);

    let prov_ctx = ProvenanceContext {
        recording_principal: principal,
        actor: Some("agent-1".to_owned()),
        execution_id: None,
        session_id: Some("sess-1".to_owned()),
        thread_id: None,
        task_id: None,
        workflow_operation_id: None,
        reason: Some("required evidence".to_owned()),
        trace_id: None,
        span_id: None,
        parent_span_id: None,
        required_logs_root: req_logs_root,
        required_artifacts_root: req_arts_root,
        optional_links: vec![optional_target_ref.object_id.to_string()],
    };
    let prov_ref = harness.put_obj(ws, &prov_ctx.encode().unwrap());

    let (_branch2, _gen2_id) = publish_checkpoint_with_roots(
        &harness,
        &client,
        ws,
        main_branch,
        boundary_id,
        branch.clone(),
        grant,
        att,
        ProvenanceBinding::Recorded {
            context_root: prov_ref,
        },
        branch.volume_root,
    );

    // Pin max_in_memory_visited = 2 to force DiskSpillVisitedSet to spill sorted runs to disk.
    let report_id = OperationId::from_bytes(id16(26));
    let summary = client
        .collect_report(
            report_id,
            Some(GcReportBudget {
                max_in_memory_visited: 2,
                ..GcReportBudget::default()
            }),
        )
        .unwrap();
    assert_eq!(summary.completeness, GcReportCompleteness::Complete);
    assert!(summary.unreachable_objects >= 1);

    let (_, page0) = client.get_gc_report(report_id, Some(0)).unwrap();
    let candidate_ids: Vec<ObjectId> = page0
        .unwrap()
        .entries
        .iter()
        .map(|entry| entry.object_ref.object_id)
        .collect();
    assert!(candidate_ids.contains(&optional_target_ref.object_id));
    assert!(!candidate_ids.contains(&req_log_seg_ref.object_id));
    assert!(!candidate_ids.contains(&req_art_blob_ref.object_id));
    assert!(!candidate_ids.contains(&prov_ref.object_id));
}

#[test]
fn uncertain_preparations_budget_exhaustion_and_detail_expiry_behave_explicitly() {
    let harness = TestHarness::start();
    let ws = WorkspaceId::from_bytes(id16(30));
    let principal = PrincipalId::from_bytes(id16(31));
    let (client, main_branch, _boundary_id, branch, grant, att) =
        setup_workspace(&harness, ws, principal);

    // Put two unreachable orphan objects.
    let o1 = harness.put_obj(ws, &encode_artifact_blob(b"orphan-1").unwrap());
    let o2 = harness.put_obj(ws, &encode_artifact_blob(b"orphan-2").unwrap());
    assert_ne!(o1.object_id, o2.object_id);

    // Leave a Preparing operation active on main_branch -> uncertain_preparations >= 1.
    let prep_op = OperationId::from_bytes(id16(32));
    client
        .begin_preparation(PreparationRequest {
            operation_id: prep_op,
            branch_id: main_branch,
            expected_revision: branch.revision,
            grant,
            attachment_id: att.attachment_id,
            volume_epoch: att.volume_epoch,
            protection_id: ProtectionId::from_bytes(id16(33)),
            kind: PreparationKind::VolumeBarrier,
            expected_roots: Some(versionfs_core::control::LiveRoots {
                volume: branch.volume_root,
                logs: branch.logs_root,
                artifacts: branch.artifacts_root,
            }),
            dependencies: Vec::new(),
        })
        .unwrap();

    let report_id = OperationId::from_bytes(id16(34));
    let summary = client
        .collect_report(
            report_id,
            Some(GcReportBudget {
                max_in_memory_visited: 64,
                max_candidate_pages: 1,
                max_candidates_per_page: 1,
                max_report_bytes: 1024 * 1024,
                detail_ttl_nanos: 5_000_000_000,
                scan_page_size: 16,
            }),
        )
        .unwrap();

    assert_eq!(summary.candidate_page_count, 1);
    assert_eq!(summary.unreachable_objects, 2);
    assert_eq!(
        summary.completeness,
        GcReportCompleteness::Incomplete {
            missing_or_corrupt_objects: 0,
            uncertain_preparations: 1,
            budget_exhausted: true,
            missing_metadata: 0,
        }
    );

    // Page 0 is readable before expiry.
    let (_, page0) = client.get_gc_report(report_id, Some(0)).unwrap();
    assert_eq!(page0.unwrap().entries.len(), 1);

    // Advance monotonic clock past detail_ttl_nanos -> page lookup returns ReportExpired,
    // while summary-only lookup still preserves the terminal summary.
    harness.clock.advance(6_000_000_000);
    let err = client.get_gc_report(report_id, Some(0)).unwrap_err();
    assert_eq!(err.code, ServiceErrorCode::ReportExpired);

    let (summary_after, page_after) = client.get_gc_report(report_id, None).unwrap();
    assert_eq!(summary_after, summary);
    assert!(page_after.is_none());
}
