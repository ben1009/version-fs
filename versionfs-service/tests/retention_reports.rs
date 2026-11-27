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
        std::fs::create_dir(&path).unwrap();
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

#[test]
fn concurrent_same_report_id_returns_identical_terminal_summary_and_rejects_budget_mismatch() {
    let harness = TestHarness::start();
    let ws = WorkspaceId::from_bytes(id16(40));
    let principal = PrincipalId::from_bytes(id16(41));
    let (client, _main_branch, _boundary_id, _branch, _grant, _att) =
        setup_workspace(&harness, ws, principal);

    let report_id = OperationId::from_bytes(id16(42));
    let budget = GcReportBudget::default();

    let c1 = harness.client(ws, principal);
    let c2 = harness.client(ws, principal);
    let t1 = std::thread::spawn(move || c1.collect_report(report_id, Some(budget)));
    let t2 = std::thread::spawn(move || c2.collect_report(report_id, Some(budget)));
    let r1 = t1.join().unwrap().unwrap();
    let r2 = t2.join().unwrap().unwrap();
    assert_eq!(r1, r2);

    // Retrying the same report_id with a different budget must fail with
    // OperationMismatch/Conflict.
    let mismatched_budget = GcReportBudget {
        max_candidate_pages: 7,
        ..budget
    };
    let err = client
        .collect_report(report_id, Some(mismatched_budget))
        .unwrap_err();
    assert_eq!(err.code, ServiceErrorCode::OperationMismatch);
}

#[test]
fn unknown_outcome_during_report_lifecycle_reconciles_and_unblocks_mutations() {
    use versionfs_core::fault::{FailureInjector, FailurePoint, InjectedFailure};

    struct ArmedUnknown(AtomicBool);
    impl FailureInjector for ArmedUnknown {
        fn check(&self, point: FailurePoint) -> Result<(), InjectedFailure> {
            if point == FailurePoint::AuthorityCommitOutcomeUnknownWithoutCommit
                && self.0.swap(false, Ordering::AcqRel)
            {
                return Err(InjectedFailure(point));
            }
            Ok(())
        }
    }

    let dir = TempDir::new();
    let meta_dir = dir.0.join("meta");
    let obj_dir = dir.0.join("objects");
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let clock = Arc::new(TestClock::new(1_000_000_000));
    let faults = Arc::new(ArmedUnknown(AtomicBool::new(false)));
    let authority = Arc::new(
        ToyKvMetadataAuthority::open_with(
            &meta_dir,
            Arc::clone(&clock) as Arc<dyn Clock>,
            Arc::clone(&faults) as Arc<dyn FailureInjector>,
        )
        .unwrap(),
    );
    let incarnation =
        start_authority_owner(authority.as_ref(), OperationId::from_bytes(id16(60))).unwrap();
    let object_store = Arc::new(DirectoryObjectStore::open(&obj_dir).unwrap());
    let ws = WorkspaceId::from_bytes(id16(61));
    let principal = PrincipalId::from_bytes(id16(62));

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

    let client = ServiceClient::connect(
        ServiceEndpoint::Tcp(addr),
        RpcAuth {
            workspace_id: ws,
            principal_id: principal,
            auth_token: b"secret-token".to_vec(),
        },
    )
    .unwrap();
    let main_branch = BranchId::from_bytes(id16(63));
    let boundary_id = VolumeBoundaryId::from_bytes(id16(64));
    let init = client
        .create_empty_workspace(
            OperationId::from_bytes(id16(65)),
            GenerationId::from_bytes(id16(66)),
            boundary_id,
            VolumeGeometry::new(64 * 1024).unwrap(),
            1_700_000_000_000,
            main_branch,
            "main",
            60_000,
            OperationId::from_bytes(id16(67)),
            OperationId::from_bytes(id16(68)),
            AttachmentId::from_bytes(id16(69)),
            ProtectionId::from_bytes(id16(70)),
        )
        .unwrap();

    // Arm unknown outcome for the next metadata commit (GC report admission/sealing).
    let report_id = OperationId::from_bytes(id16(71));
    faults.0.store(true, Ordering::Release);
    let first_err = client.collect_report(report_id, None).unwrap_err();
    assert_eq!(first_err.code, ServiceErrorCode::UnknownOutcome);

    // Retrying with the same report_id reconciles the unknown outcome and succeeds.
    let summary = client.collect_report(report_id, None).unwrap();
    assert_eq!(summary.report_id, report_id);
    assert_eq!(summary.completeness, GcReportCompleteness::Complete);

    // Normal branch mutations are not blocked and succeed.
    let prep_op = OperationId::from_bytes(id16(72));
    client
        .begin_preparation(PreparationRequest {
            operation_id: prep_op,
            branch_id: main_branch,
            expected_revision: init.branch.revision,
            grant: init.writer_grant,
            attachment_id: init.attachment.attachment_id,
            volume_epoch: init.attachment.volume_epoch,
            protection_id: ProtectionId::from_bytes(id16(73)),
            kind: PreparationKind::VolumeBarrier,
            expected_roots: Some(versionfs_core::control::LiveRoots {
                volume: init.branch.volume_root,
                logs: init.branch.logs_root,
                artifacts: init.branch.artifacts_root,
            }),
            dependencies: Vec::new(),
        })
        .unwrap();

    shutdown.store(true, Ordering::Release);
    let _ = server_thread.join();
}

#[test]
fn snapshot_loss_before_sealing_aborts_report_id_permanently() {
    use versionfs_core::metadata::{
        MetadataAuthority, MetadataCursor, MetadataError, MetadataPage, MetadataTxn,
        MutationIdentity, ReconciledOutcome,
    };

    struct SnapshotLossAuthority {
        inner: Arc<ToyKvMetadataAuthority>,
        fail_scan: AtomicBool,
    }

    impl MetadataAuthority for SnapshotLossAuthority {
        type Snapshot = <ToyKvMetadataAuthority as MetadataAuthority>::Snapshot;

        fn now(&self) -> MonotonicTime {
            self.inner.now()
        }

        fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, MetadataError> {
            self.inner.get(key)
        }

        fn transact<R, F>(&self, op: MutationIdentity<'_>, body: F) -> Result<R, MetadataError>
        where
            F: FnMut(&mut dyn MetadataTxn) -> Result<R, MetadataError>,
        {
            self.inner.transact(op, body)
        }

        fn reconcile_unknown(
            &self,
            op: MutationIdentity<'_>,
        ) -> Result<ReconciledOutcome, MetadataError> {
            self.inner.reconcile_unknown(op)
        }

        fn snapshot(&self) -> Result<Self::Snapshot, MetadataError> {
            self.inner.snapshot()
        }

        fn scan_prefix(
            &self,
            snapshot: &Self::Snapshot,
            prefix: &[u8],
            cursor: Option<&MetadataCursor>,
            limit: std::num::NonZeroUsize,
        ) -> Result<MetadataPage, MetadataError> {
            if self.fail_scan.load(Ordering::Acquire) {
                return Err(MetadataError::Backend("snapshot lost".to_owned()));
            }
            self.inner.scan_prefix(snapshot, prefix, cursor, limit)
        }
    }

    let harness = TestHarness::start();
    let ws = WorkspaceId::from_bytes(id16(80));
    let principal = PrincipalId::from_bytes(id16(81));
    let (_client, _main_branch, _boundary_id, _branch, _grant, _att) =
        setup_workspace(&harness, ws, principal);

    let wrapper = Arc::new(SnapshotLossAuthority {
        inner: Arc::clone(&harness.authority),
        fail_scan: AtomicBool::new(true),
    });
    let control = WorkspaceControl::new(
        wrapper,
        ws,
        harness.object_store.scope_id(ws),
        principal,
        harness.incarnation,
    );

    let report_id = OperationId::from_bytes(id16(82));
    let budget = GcReportBudget::default();
    control.begin_gc_report_attempt(report_id, budget).unwrap();

    // Snapshot loss during root scanning fails AND durably aborts report_id.
    let err = control.seal_gc_report_roots(report_id, budget).unwrap_err();
    assert!(matches!(
        err,
        versionfs_core::control::ControlError::Metadata(MetadataError::Backend(_))
    ));

    // Retrying with the same report_id remains permanently Aborted (never acquires a new snapshot).
    let retry_err = control
        .begin_gc_report_attempt(report_id, budget)
        .unwrap_err();
    assert!(matches!(
        retry_err,
        versionfs_core::control::ControlError::Aborted
    ));
}

#[test]
fn authority_restart_expires_details_via_unix_ms_and_preserves_summary() {
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
    let incarnation1 =
        start_authority_owner(authority.as_ref(), OperationId::from_bytes(id16(90))).unwrap();
    let object_store = Arc::new(DirectoryObjectStore::open(&obj_dir).unwrap());
    let ws = WorkspaceId::from_bytes(id16(91));
    let principal = PrincipalId::from_bytes(id16(92));

    let control1 = WorkspaceControl::new(
        Arc::clone(&authority),
        ws,
        object_store.scope_id(ws),
        principal,
        incarnation1,
    );

    // Put one orphan object and collect a report with detail_ttl_nanos = 0.
    let orphan = encode_artifact_blob(b"restart-orphan").unwrap();
    rt.block_on(object_store.put(ws, &orphan)).unwrap();

    let report_id = OperationId::from_bytes(id16(93));
    let summary = versionfs_service::gc_report::run_mark_only_report(
        &control1,
        &authority,
        &object_store,
        rt.handle(),
        ws,
        report_id,
        GcReportBudget {
            detail_ttl_nanos: 0,
            ..GcReportBudget::default()
        },
    )
    .unwrap();
    assert_eq!(summary.candidate_page_count, 1);

    // Simulate authority restart with a new OwnerIncarnation.
    let incarnation2 =
        start_authority_owner(authority.as_ref(), OperationId::from_bytes(id16(94))).unwrap();
    assert_ne!(incarnation1.as_u64(), incarnation2.as_u64());
    let control2 = WorkspaceControl::new(
        Arc::clone(&authority),
        ws,
        object_store.scope_id(ws),
        principal,
        incarnation2,
    );

    let err = control2
        .get_gc_report(
            report_id,
            Some(0),
            versionfs_service::gc_report::unix_ms_now(),
        )
        .unwrap_err();
    assert!(matches!(
        err,
        versionfs_core::control::ControlError::ReportExpired
    ));

    let (summary_only, page) = control2
        .get_gc_report(report_id, None, versionfs_service::gc_report::unix_ms_now())
        .unwrap();
    assert_eq!(summary_only, summary);
    assert!(page.is_none());
}

#[test]
fn cow_shared_btree_subtrees_are_skipped_without_reloading() {
    use std::collections::HashMap;
    use std::sync::Mutex;
    use versionfs_core::gc::{DiskSpillVisitedSet, DiskSpillWorkQueue, mark_reachable_closure};

    struct CountingStore<'a> {
        inner: &'a SyncStore<'a>,
        reads: Mutex<HashMap<ObjectId, usize>>,
    }

    impl MapObjectStore for CountingStore<'_> {
        fn proof_scope(&self) -> ObjectStoreScopeId {
            self.inner.proof_scope()
        }

        fn get(&self, id: ObjectId) -> Result<Option<Vec<u8>>, String> {
            self.inner.get(id)
        }

        fn get_ref(&self, reference: ObjectRef) -> Result<Option<Vec<u8>>, String> {
            *self
                .reads
                .lock()
                .unwrap()
                .entry(reference.object_id)
                .or_insert(0) += 1;
            self.inner.get_ref(reference)
        }

        fn put(&mut self, _id: ObjectId, _bytes: Vec<u8>) -> Result<(), String> {
            Err("read-only".to_owned())
        }
    }

    let harness = TestHarness::start();
    let ws = WorkspaceId::from_bytes(id16(100));
    let mut store = SyncStore {
        rt: &harness.rt,
        store: &harness.object_store,
        ws,
    };

    // Build a multi-level VolumeExtentMap, then create a second root sharing most subtrees.
    let mut vol = VolumeExtentMap::empty(
        VolumeGeometry::new(64 * EXTENT_BYTES as u64).unwrap(),
        &mut store,
    )
    .unwrap();
    for idx in 0..40_u64 {
        let ext =
            encode_object(ObjectType::Extent, &vec![idx as u8; EXTENT_BYTES as usize]).unwrap();
        let ext_ref = harness.put_obj(ws, &ext);
        vol = vol.insert_extent(&mut store, idx, ext_ref).unwrap();
    }
    let r1 = vol.seal(&mut store).unwrap();

    let extra_ext = encode_object(ObjectType::Extent, &[0x99_u8; EXTENT_BYTES as usize]).unwrap();
    let extra_ref = harness.put_obj(ws, &extra_ext);
    let vol2 = vol.insert_extent(&mut store, 40, extra_ref).unwrap();
    let r2 = vol2.seal(&mut store).unwrap();

    let spill_dir = TempDir::new();
    let mut visited = DiskSpillVisitedSet::new(&spill_dir.0, 8).unwrap();
    let mut queue = DiskSpillWorkQueue::new(&spill_dir.0, 8).unwrap();
    queue.push(r1).unwrap();
    queue.push(r2).unwrap();

    let counting = CountingStore {
        inner: &store,
        reads: Mutex::new(HashMap::new()),
    };
    let stats = mark_reachable_closure(&counting, &mut queue, &mut visited).unwrap();
    assert_eq!(stats.missing_or_corrupt_objects, 0);
    assert!(visited.spill_run_count() > 0);

    // Every object (including shared COW B-tree nodes) was fetched at most once.
    for (obj_id, count) in counting.reads.lock().unwrap().iter() {
        assert_eq!(*count, 1, "object {obj_id} was fetched {count} times");
    }
}
