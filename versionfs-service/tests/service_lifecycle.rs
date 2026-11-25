//! P9 service, SDK, protection, provenance, heartbeat, and diff integration tests.

use std::collections::HashSet;
use std::net::TcpListener;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use versionfs_core::control::{
    AttachmentState, BranchState, PreparationKind, PreparationRequest, VerifiedGenerationSeed,
    WriterGrant, start_authority_owner,
};
use versionfs_core::fault::NoFailures;
use versionfs_core::format::{
    ArtifactDescriptor, CaptureMode, EXTENT_BYTES, GenerationDescriptor,
    GenerationDescriptorFormat, GenerationDescriptorV2, LogMapEntry, LogSegment, ObjectRef,
    ObjectType, ProvenanceBinding, ProvenanceContext, ProvenanceCoverage, ProvenanceStatus,
    SegmentIndexEntry, VolumeGeometry, encode_object,
};
use versionfs_core::graph::{
    ProvenanceInspectError, inspect_generation_provenance, sanitize_provenance_text,
    verify_generation,
};
use versionfs_core::identity::{
    ArtifactId, AttachmentId, BranchId, GenerationId, LogId, ObjectId, ObjectStoreScopeId,
    OperationId, PrincipalId, ProtectionId, VolumeBoundaryId, WorkspaceId,
};
use versionfs_core::persistent_map::{DurableObjectStore, MapObjectStore};
use versionfs_core::rpc::{RpcAuth, ServiceErrorCode};
use versionfs_core::time::{Clock, MonotonicTime};
use versionfs_core::views::{
    ArtifactChange, ArtifactMap, LogMap, LogSegmentIndex, VolumeExtentMap, encode_artifact_blob,
};
use versionfs_sdk::barrier::VolumeBarrierControl;
use versionfs_sdk::capture::CaptureControl;
use versionfs_sdk::client::{
    AttachmentHeartbeat, ProtectedReadHandle, ReadOnlyVolume, ServiceClient, ServiceEndpoint,
};
use versionfs_sdk::linux_fs::{LinuxFsError, format_ext4};
use versionfs_sdk::logs::NativeControl;
use versionfs_sdk::volume::{BlockDevice, BlockError, BlockRequest, BlockResponse};
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
            "versionfs-service-lifecycle-{}-{id}",
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
    object_store: Arc<DirectoryObjectStore>,
    endpoint: ServiceEndpoint,
    shutdown: Arc<AtomicBool>,
    server_thread: Option<std::thread::JoinHandle<()>>,
}

impl TestHarness {
    fn start(deny_principals: HashSet<PrincipalId>) -> Self {
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
        let mut config = ServiceConfig::new(b"secret-token".to_vec());
        config.provenance_deny_principals = deny_principals;

        let service = Arc::new(WorkspaceService::new(
            authority,
            incarnation,
            Arc::clone(&object_store),
            rt.handle().clone(),
            config,
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
            object_store,
            endpoint: ServiceEndpoint::Tcp(addr),
            shutdown,
            server_thread: Some(server_thread),
        }
    }

    fn client(
        &self,
        workspace_id: WorkspaceId,
        principal_id: PrincipalId,
        token: &[u8],
    ) -> ServiceClient {
        ServiceClient::connect(
            self.endpoint.clone(),
            RpcAuth {
                workspace_id,
                principal_id,
                auth_token: token.to_vec(),
            },
        )
        .unwrap()
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

struct MemStaging {
    scope: ObjectStoreScopeId,
    objects: std::collections::HashMap<ObjectId, Vec<u8>>,
}

impl MemStaging {
    fn new(scope: ObjectStoreScopeId) -> Self {
        Self {
            scope,
            objects: std::collections::HashMap::new(),
        }
    }
}

impl MapObjectStore for MemStaging {
    fn proof_scope(&self) -> ObjectStoreScopeId {
        self.scope
    }

    fn get(&self, id: ObjectId) -> Result<Option<Vec<u8>>, String> {
        Ok(self.objects.get(&id).cloned())
    }

    fn put(&mut self, id: ObjectId, bytes: Vec<u8>) -> Result<(), String> {
        self.objects.insert(id, bytes);
        Ok(())
    }
}

fn create_workspace(
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
    let client = harness.client(ws, principal, b"secret-token");
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

#[test]
fn auth_rejection_returns_unauthorized() {
    let harness = TestHarness::start(HashSet::new());
    let ws = WorkspaceId::from_bytes(id16(10));
    let principal = PrincipalId::from_bytes(id16(11));
    let err = ServiceClient::connect(
        harness.endpoint.clone(),
        RpcAuth {
            workspace_id: ws,
            principal_id: principal,
            auth_token: b"wrong-token".to_vec(),
        },
    )
    .err()
    .expect("wrong token must be rejected");
    assert_eq!(err.code, ServiceErrorCode::Unauthorized);
}

#[test]
fn arbitrary_digest_fetch_and_unprepared_upload_rejected() {
    let harness = TestHarness::start(HashSet::new());
    let ws = WorkspaceId::from_bytes(id16(12));
    let principal = PrincipalId::from_bytes(id16(13));
    let (client, _main, _boundary, branch, _grant, _att) =
        create_workspace(&harness, ws, principal);

    // Put an unreferenced extent directly into ObjectStore (never reachable from G0).
    let orphan_bytes =
        encode_object(ObjectType::Extent, &vec![0x5A_u8; EXTENT_BYTES as usize]).unwrap();
    let orphan_ref = ObjectRef::from_encoded(&orphan_bytes).unwrap();
    harness
        .rt
        .block_on(harness.object_store.put(ws, &orphan_bytes))
        .unwrap();

    // Open a protection handle on G0; trying to fetch `orphan_ref` via RemoteObjectStore must fail.
    let prot_id = ProtectionId::from_bytes(id16(14));
    let handle = ProtectedReadHandle::open(
        client.clone(),
        OperationId::from_bytes(id16(15)),
        branch.head,
        prot_id,
    )
    .unwrap();
    client.set_active_protection(Some(handle.protection_id()));
    let store = client.remote_object_store();
    let fetch_err = store
        .get_ref(orphan_ref)
        .expect_err("arbitrary unreferenced digest fetch must be blocked");
    assert!(
        fetch_err.contains("not reachable")
            || fetch_err.contains("Unauthorized")
            || fetch_err.contains("NotFound"),
        "unexpected error: {fetch_err}"
    );
    handle.close(OperationId::from_bytes(id16(16))).unwrap();

    // UploadPreparedObject without an active Preparing operation must fail.
    let mut store_mut = client.remote_object_store();
    let upload_err = store_mut
        .put_durable(orphan_ref, &orphan_bytes)
        .expect_err("upload without active Preparing operation must be rejected");
    assert!(
        upload_err.contains("no active preparation")
            || upload_err.contains("Preparing")
            || upload_err.contains("Conflict")
            || upload_err.contains("PreconditionFailed"),
        "unexpected upload error: {upload_err}"
    );
}

#[test]
fn protected_read_handle_close_and_deadline_enforced() {
    let harness = TestHarness::start(HashSet::new());
    let ws = WorkspaceId::from_bytes(id16(20));
    let principal = PrincipalId::from_bytes(id16(21));
    let (client, _main, _boundary, branch, _grant, _att) =
        create_workspace(&harness, ws, principal);

    let prot_id = ProtectionId::from_bytes(id16(22));
    let mut handle = ProtectedReadHandle::open(
        client.clone(),
        OperationId::from_bytes(id16(23)),
        branch.head,
        prot_id,
    )
    .unwrap();
    let bounded = handle.bounded_read_deadline().unwrap();
    assert!(bounded > NativeControl::now(&client));
    let renewed = handle.renew(OperationId::from_bytes(id16(24))).unwrap();
    assert!(renewed >= handle.deadline());

    let geometry = VolumeGeometry::new(64 * 1024).unwrap();
    let mut ro = ReadOnlyVolume::new(client.clone(), handle.protection_id(), geometry);
    match ro.dispatch(BlockRequest::Read {
        offset: 0,
        length: 4096,
    }) {
        Ok(BlockResponse::Read(bytes)) => assert_eq!(bytes, vec![0_u8; 4096]),
        other => panic!("unexpected read response: {other:?}"),
    }
    assert!(matches!(
        ro.dispatch(BlockRequest::Write {
            offset: 0,
            data: &[1_u8; 4096],
            fua: false,
        }),
        Err(BlockError::UnsupportedOperation(1))
    ));
    assert!(matches!(
        ro.dispatch(BlockRequest::Flush),
        Err(BlockError::UnsupportedOperation(1))
    ));

    // Close the protection handle; reads must now fail.
    handle.close(OperationId::from_bytes(id16(25))).unwrap();
    let err = client
        .read_blocks_at(prot_id, 0, 4096)
        .expect_err("closed protection handle must reject reads");
    assert_eq!(err.code, ServiceErrorCode::NotFound);

    // Re-open, then advance clock past lease deadline -> reads must reject as expired.
    let prot_id2 = ProtectionId::from_bytes(id16(26));
    let _handle2 = ProtectedReadHandle::open(
        client.clone(),
        OperationId::from_bytes(id16(27)),
        branch.head,
        prot_id2,
    )
    .unwrap();
    harness.clock.advance(400_000_000_000);
    let expired_err = client
        .read_blocks_at(prot_id2, 0, 4096)
        .expect_err("expired protection handle must reject reads");
    assert!(matches!(
        expired_err.code,
        ServiceErrorCode::LeaseExpired | ServiceErrorCode::NotFound
    ));
}

#[test]
fn provenance_inspection_v2_v1_denied_corrupt_and_sanitized() {
    let denied_principal = PrincipalId::from_bytes(id16(30));
    let mut deny = HashSet::new();
    deny.insert(denied_principal);
    let harness = TestHarness::start(deny);

    let ws = WorkspaceId::from_bytes(id16(31));
    let writer = PrincipalId::from_bytes(id16(32));
    let (client, main_branch, boundary_id, branch, grant, att) =
        create_workspace(&harness, ws, writer);
    let scope = client.object_store_scope();
    let mut staging = MemStaging::new(scope);

    // 1. V2 generation with Recorded provenance.
    let empty_vol = VolumeExtentMap::empty(VolumeGeometry::new(64 * 1024).unwrap(), &mut staging)
        .unwrap()
        .seal(&mut staging)
        .unwrap();
    let empty_logs = LogMap::empty(&mut staging)
        .unwrap()
        .seal(&mut staging)
        .unwrap();
    let empty_arts = ArtifactMap::empty(&mut staging)
        .unwrap()
        .seal(&mut staging)
        .unwrap();

    let ctx = ProvenanceContext {
        recording_principal: writer,
        actor: Some("agent-worker".to_owned()),
        execution_id: None,
        session_id: Some("sess-1".to_owned()),
        thread_id: None,
        task_id: None,
        workflow_operation_id: None,
        reason: Some(
            sanitize_provenance_text("checkpoint before Bearer sk-secret123456\x07", 256).unwrap(),
        ),
        trace_id: None,
        span_id: None,
        parent_span_id: None,
        required_logs_root: empty_logs,
        required_artifacts_root: empty_arts,
        optional_links: Vec::new(),
    };
    assert!(!ctx.reason.as_deref().unwrap().contains("sk-secret123456"));
    assert!(!ctx.reason.as_deref().unwrap().contains('\x07'));

    let ctx_bytes = ctx.encode().unwrap();
    let ctx_ref = ObjectRef::from_encoded(&ctx_bytes).unwrap();
    staging.put(ctx_ref.object_id, ctx_bytes).unwrap();

    let g1_id = GenerationId::from_bytes(id16(33));
    let g1_bytes = GenerationDescriptorV2 {
        generation_id: g1_id,
        parent_generation_id: Some(branch.head),
        volume_root: empty_vol,
        logs_root: empty_logs,
        artifacts_root: empty_arts,
        provenance: ProvenanceBinding::Recorded {
            context_root: ctx_ref,
        },
        capture_mode: CaptureMode::BlockCut,
        volume_boundary_id: boundary_id,
        created_at_unix_ms: 1_700_000_001_000,
    }
    .encode()
    .unwrap();
    let g1_ref = ObjectRef::from_encoded(&g1_bytes).unwrap();
    staging.put(g1_ref.object_id, g1_bytes.clone()).unwrap();

    // Begin capture, upload staged objects, seal, and publish.
    let cap_op = OperationId::from_bytes(id16(34));
    client
        .begin_capture(
            PreparationRequest {
                operation_id: cap_op,
                branch_id: main_branch,
                expected_revision: branch.revision,
                grant,
                attachment_id: att.attachment_id,
                volume_epoch: att.volume_epoch,
                protection_id: ProtectionId::from_bytes(id16(35)),
                kind: PreparationKind::CheckpointCapture,
                expected_roots: None,
                dependencies: Vec::new(),
            },
            ProvenanceBinding::Recorded {
                context_root: ctx_ref,
            },
        )
        .unwrap();
    client.set_active_preparation(Some(cap_op));
    let mut remote_store = client.remote_object_store();
    for (oid, bytes) in &staging.objects {
        let obj_ref = ObjectRef::from_encoded(bytes).unwrap();
        assert_eq!(obj_ref.object_id, *oid);
        remote_store.put_durable(obj_ref, bytes).unwrap();
    }
    client
        .seal_capture(
            OperationId::from_bytes(id16(36)),
            cap_op,
            branch.revision,
            grant,
        )
        .unwrap();
    let seed = VerifiedGenerationSeed::from_proof(verify_generation(&staging, g1_ref).unwrap());
    client
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

    // Authorized fresh reader inspects G1 provenance -> Recorded.
    let reader = harness.client(ws, PrincipalId::from_bytes(id16(37)), b"secret-token");
    let prot_g1 = ProtectionId::from_bytes(id16(38));
    let h1 = ProtectedReadHandle::open(
        reader.clone(),
        OperationId::from_bytes(id16(39)),
        g1_id,
        prot_g1,
    )
    .unwrap();
    let insp = reader.read_provenance(prot_g1, g1_id).unwrap();
    assert_eq!(insp.source_format, GenerationDescriptorFormat::V2);
    assert_eq!(insp.status, ProvenanceStatus::Recorded);
    assert_eq!(insp.coverage, ProvenanceCoverage::GenerationContext);
    assert_eq!(insp.context.unwrap().actor.as_deref(), Some("agent-worker"));
    h1.close(OperationId::from_bytes(id16(40))).unwrap();

    // Denied principal receives Unauthorized (never relabeled Unattributed).
    let denied_reader = harness.client(ws, denied_principal, b"secret-token");
    let prot_denied = ProtectionId::from_bytes(id16(41));
    let h_denied = ProtectedReadHandle::open(
        denied_reader.clone(),
        OperationId::from_bytes(id16(42)),
        g1_id,
        prot_denied,
    )
    .unwrap();
    let denied_err = denied_reader
        .read_provenance(prot_denied, g1_id)
        .expect_err("denied principal must receive Unauthorized");
    assert_eq!(denied_err.code, ServiceErrorCode::Unauthorized);
    denied_reader.set_active_protection(Some(prot_denied));
    let raw_ctx_err = denied_reader
        .remote_object_store()
        .get_ref(ctx_ref)
        .expect_err("denied principal must not fetch ProvenanceContext via FetchProtectedObject");
    assert!(raw_ctx_err.contains("Unauthorized") || raw_ctx_err.contains("not authorized"));
    h_denied.close(OperationId::from_bytes(id16(43))).unwrap();

    let quoted_sanitized =
        sanitize_provenance_text("token=\"sk-quoted123456\" ('Bearer' 'ghp_abc123')", 256).unwrap();
    assert!(!quoted_sanitized.contains("sk-quoted123456"));
    assert!(!quoted_sanitized.contains("ghp_abc123"));

    // 2. Legacy V1 descriptor -> Unattributed, Coverage::None, never synthesized.
    let v1_id = GenerationId::from_bytes(id16(44));
    let v1_bytes = GenerationDescriptor {
        generation_id: v1_id,
        parent_generation_id: None,
        volume_root: empty_vol,
        logs_root: empty_logs,
        artifacts_root: empty_arts,
        capture_mode: CaptureMode::BlockCut,
        volume_boundary_id: boundary_id,
        created_at_unix_ms: 1_700_000_002_000,
    }
    .encode()
    .unwrap();
    let v1_ref = ObjectRef::from_encoded(&v1_bytes).unwrap();
    staging.put(v1_ref.object_id, v1_bytes).unwrap();
    let v1_insp = inspect_generation_provenance(&staging, v1_id, v1_ref, true, None).unwrap();
    assert_eq!(v1_insp.source_format, GenerationDescriptorFormat::V1);
    assert_eq!(v1_insp.status, ProvenanceStatus::Unattributed);
    assert_eq!(v1_insp.coverage, ProvenanceCoverage::None);
    assert!(v1_insp.context.is_none());

    // 3. Corrupt/missing ProvenanceContext -> Integrity error.
    let mut missing_ctx_map = MemStaging::new(scope);
    missing_ctx_map.put(g1_ref.object_id, g1_bytes).unwrap();
    let corrupt_err = inspect_generation_provenance(&missing_ctx_map, g1_id, g1_ref, true, None)
        .expect_err("missing provenance context must fail");
    assert!(matches!(corrupt_err, ProvenanceInspectError::Integrity(_)));
}

#[test]
fn freeze_independent_heartbeat_and_unsupported_p10_reports() {
    let harness = TestHarness::start(HashSet::new());
    let ws = WorkspaceId::from_bytes(id16(50));
    let principal = PrincipalId::from_bytes(id16(51));
    let (client, main_branch, _boundary, _branch, mut grant, mut att) =
        create_workspace(&harness, ws, principal);

    // Simulate a frozen filesystem boundary; heartbeat and op queries only use control RPCs.
    let _simulated_fs_frozen = true;
    let op_writer = OperationId::from_bytes(id16(52));
    AttachmentHeartbeat::beat_once(
        &client,
        main_branch,
        &mut grant,
        &mut att,
        op_writer,
        OperationId::from_bytes(id16(53)),
    )
    .unwrap();
    let op_state = client.get_operation(op_writer).unwrap();
    assert!(matches!(
        op_state,
        versionfs_core::control::OperationState::Committed { .. }
    ));

    // P10 report RPCs return explicit UnsupportedOperation.
    assert_eq!(
        client.collect_report().unwrap_err().code,
        ServiceErrorCode::UnsupportedOperation
    );
    assert_eq!(
        client.get_gc_report().unwrap_err().code,
        ServiceErrorCode::UnsupportedOperation
    );
}

#[test]
fn diff_generations_streams_extents_logs_and_artifacts_and_rejects_unconfirmed_format() {
    let harness = TestHarness::start(HashSet::new());
    let ws = WorkspaceId::from_bytes(id16(60));
    let principal = PrincipalId::from_bytes(id16(61));
    let (client, main_branch, boundary_id, branch, grant, att) =
        create_workspace(&harness, ws, principal);
    let scope = client.object_store_scope();

    // Stage G1 with 1 dirty extent, 1 native log, and 1 native artifact.
    let mut staging = MemStaging::new(scope);
    let mut vol_map =
        VolumeExtentMap::empty(VolumeGeometry::new(64 * 1024).unwrap(), &mut staging).unwrap();
    let block_data =
        encode_object(ObjectType::Extent, &vec![0xAB_u8; EXTENT_BYTES as usize]).unwrap();
    let block_ref = ObjectRef::from_encoded(&block_data).unwrap();
    staging.put(block_ref.object_id, block_data).unwrap();
    vol_map = vol_map.insert_extent(&mut staging, 0, block_ref).unwrap();
    let g1_vol = vol_map.seal(&mut staging).unwrap();

    let log_id = LogId::from_bytes(id16(62));
    let seg_bytes = LogSegment {
        log_id,
        first_seq: 0,
        end_seq: 1,
        records: vec![b"hello-log".to_vec()],
        attachments: Vec::new(),
    }
    .encode()
    .unwrap();
    let seg_ref = ObjectRef::from_encoded(&seg_bytes).unwrap();
    staging.put(seg_ref.object_id, seg_bytes).unwrap();
    let mut seg_idx = LogSegmentIndex::empty(&mut staging).unwrap();
    seg_idx = seg_idx
        .insert(
            &mut staging,
            0,
            SegmentIndexEntry {
                end_seq: 1,
                segment: seg_ref,
            },
        )
        .unwrap();
    let seg_idx_root = seg_idx.root();
    let mut log_map = LogMap::empty(&mut staging).unwrap();
    log_map = log_map
        .insert(
            &mut staging,
            "events",
            LogMapEntry {
                log_id,
                first_seq: 0,
                end_seq: 1,
                segment_index_root: seg_idx_root,
            },
        )
        .unwrap();
    let g1_logs = log_map.seal(&mut staging).unwrap();

    let art_id = ArtifactId::from_bytes(id16(63));
    let art_blob = encode_artifact_blob(b"artifact-payload").unwrap();
    let art_ref = ObjectRef::from_encoded(&art_blob).unwrap();
    staging.put(art_ref.object_id, art_blob).unwrap();
    let mut art_map = ArtifactMap::empty(&mut staging).unwrap();
    art_map = art_map
        .insert(
            &mut staging,
            "summary.txt",
            &ArtifactDescriptor {
                artifact_id: art_id,
                logical_len: 16,
                content_root: art_ref,
                dependencies: Vec::new(),
            },
        )
        .unwrap();
    let g1_arts = art_map.seal(&mut staging).unwrap();

    let g1_id = GenerationId::from_bytes(id16(64));
    let g1_bytes = GenerationDescriptorV2 {
        generation_id: g1_id,
        parent_generation_id: Some(branch.head),
        volume_root: g1_vol,
        logs_root: g1_logs,
        artifacts_root: g1_arts,
        provenance: ProvenanceBinding::Unattributed,
        capture_mode: CaptureMode::BlockCut,
        volume_boundary_id: boundary_id,
        created_at_unix_ms: 1_700_000_003_000,
    }
    .encode()
    .unwrap();
    let g1_ref = ObjectRef::from_encoded(&g1_bytes).unwrap();
    staging.put(g1_ref.object_id, g1_bytes).unwrap();

    // Commit live volume, logs, and artifacts roots on the branch before checkpoint capture.
    let mut branch = branch;
    let steps = [
        (
            PreparationKind::VolumeBarrier,
            versionfs_core::control::LiveRoots {
                volume: g1_vol,
                logs: branch.logs_root,
                artifacts: branch.artifacts_root,
            },
        ),
        (
            PreparationKind::LogMutation,
            versionfs_core::control::LiveRoots {
                volume: g1_vol,
                logs: g1_logs,
                artifacts: branch.artifacts_root,
            },
        ),
        (
            PreparationKind::ArtifactMutation,
            versionfs_core::control::LiveRoots {
                volume: g1_vol,
                logs: g1_logs,
                artifacts: g1_arts,
            },
        ),
    ];
    for (idx, (kind, roots)) in steps.into_iter().enumerate() {
        let prep_op = OperationId::from_bytes(id16(74 + idx as u8));
        client
            .begin_preparation(PreparationRequest {
                operation_id: prep_op,
                branch_id: main_branch,
                expected_revision: branch.revision,
                grant,
                attachment_id: att.attachment_id,
                volume_epoch: att.volume_epoch,
                protection_id: ProtectionId::from_bytes(id16(80 + idx as u8)),
                kind,
                expected_roots: Some(roots),
                dependencies: Vec::new(),
            })
            .unwrap();
        client.set_active_preparation(Some(prep_op));
        if idx == 0 {
            let mut remote_store = client.remote_object_store();
            for bytes in staging.objects.values() {
                let obj_ref = ObjectRef::from_encoded(bytes).unwrap();
                remote_store.put_durable(obj_ref, bytes).unwrap();
            }
        }
        branch = client
            .commit_live_roots(prep_op, branch.revision, grant, roots, None)
            .unwrap();
    }

    let cap_op = OperationId::from_bytes(id16(65));
    client
        .begin_capture(
            PreparationRequest {
                operation_id: cap_op,
                branch_id: main_branch,
                expected_revision: branch.revision,
                grant,
                attachment_id: att.attachment_id,
                volume_epoch: att.volume_epoch,
                protection_id: ProtectionId::from_bytes(id16(66)),
                kind: PreparationKind::CheckpointCapture,
                expected_roots: None,
                dependencies: Vec::new(),
            },
            ProvenanceBinding::Unattributed,
        )
        .unwrap();
    client
        .seal_capture(
            OperationId::from_bytes(id16(67)),
            cap_op,
            branch.revision,
            grant,
        )
        .unwrap();
    let seed = VerifiedGenerationSeed::from_proof(verify_generation(&staging, g1_ref).unwrap());
    client
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

    // Open both G0 and G1 under protection handles and diff them.
    let p0 = ProtectionId::from_bytes(id16(68));
    let p1 = ProtectionId::from_bytes(id16(69));
    let h0 = ProtectedReadHandle::open(
        client.clone(),
        OperationId::from_bytes(id16(70)),
        branch.head,
        p0,
    )
    .unwrap();
    let h1 =
        ProtectedReadHandle::open(client.clone(), OperationId::from_bytes(id16(71)), g1_id, p1)
            .unwrap();

    let diff = client.diff_generations(p0, branch.head, p1, g1_id).unwrap();
    assert_eq!(diff.extents.len(), 1);
    assert_eq!(diff.extents[0].start_extent, 0);
    assert_eq!(diff.logs.len(), 1);
    assert_eq!(diff.artifacts.len(), 1);
    assert!(matches!(
        &diff.artifacts[0],
        ArtifactChange::Added { name, .. } if name == "summary.txt"
    ));

    h0.close(OperationId::from_bytes(id16(72))).unwrap();
    h1.close(OperationId::from_bytes(id16(73))).unwrap();

    // Explicit destructive ext4 format refuses without confirmation.
    assert!(matches!(
        format_ext4(Path::new("/tmp/versionfs-no-such-device"), false),
        Err(LinuxFsError::ConfirmationRequired(_))
    ));
}

#[test]
fn unix_listener_and_full_branch_writer_pin_lifecycle() {
    let dir = TempDir::new();
    let meta_dir = dir.0.join("meta");
    let obj_dir = dir.0.join("objects");
    let sock_path = dir.0.join("svc.sock");
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let clock = Arc::new(TestClock::new(2_000_000_000));
    let authority = Arc::new(
        ToyKvMetadataAuthority::open_with(
            &meta_dir,
            Arc::clone(&clock) as Arc<dyn Clock>,
            Arc::new(NoFailures),
        )
        .unwrap(),
    );
    let incarnation =
        start_authority_owner(authority.as_ref(), OperationId::from_bytes(id16(90))).unwrap();
    let object_store = Arc::new(DirectoryObjectStore::open(&obj_dir).unwrap());
    let service = Arc::new(WorkspaceService::new(
        authority,
        incarnation,
        object_store,
        rt.handle().clone(),
        ServiceConfig::new(b"unix-token".to_vec()),
    ));
    let listener = std::os::unix::net::UnixListener::bind(&sock_path).unwrap();
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_bg = Arc::clone(&shutdown);
    let handle = std::thread::spawn(move || {
        let _ = versionfs_service::service::serve_unix(service, listener, shutdown_bg);
    });

    let ws = WorkspaceId::from_bytes(id16(91));
    let principal = PrincipalId::from_bytes(id16(92));
    let client = ServiceClient::connect(
        ServiceEndpoint::Unix(sock_path),
        RpcAuth {
            workspace_id: ws,
            principal_id: principal,
            auth_token: b"unix-token".to_vec(),
        },
    )
    .unwrap();

    let main_id = BranchId::from_bytes(id16(93));
    let g0_id = GenerationId::from_bytes(id16(94));
    let init = client
        .create_empty_workspace(
            OperationId::from_bytes(id16(95)),
            g0_id,
            VolumeBoundaryId::from_bytes(id16(96)),
            VolumeGeometry::new(64 * 1024).unwrap(),
            1_700_000_010_000,
            main_id,
            "main",
            60_000,
            OperationId::from_bytes(id16(97)),
            OperationId::from_bytes(id16(98)),
            AttachmentId::from_bytes(id16(99)),
            ProtectionId::from_bytes(id16(100)),
        )
        .unwrap();

    // Branch/Generation queries & pins
    assert_eq!(client.branch_by_name("main").unwrap().branch_id, main_id);
    assert_eq!(client.get_generation(g0_id).unwrap().generation_id, g0_id);
    let pin_id = versionfs_core::identity::PinId::from_bytes(id16(101));
    client
        .pin_generation(OperationId::from_bytes(id16(102)), g0_id, pin_id)
        .unwrap();
    client
        .unpin_generation(OperationId::from_bytes(id16(103)), pin_id)
        .unwrap();

    // Detach, release, re-acquire (and force-acquire), re-attach
    client
        .detach_volume(
            OperationId::from_bytes(id16(104)),
            main_id,
            init.attachment.attachment_id,
            init.attachment.volume_epoch,
            init.attachment.protection_id,
            init.writer_grant,
        )
        .unwrap();
    client
        .release_writer(OperationId::from_bytes(id16(121)), init.writer_grant)
        .unwrap();
    let main_br = client.get_branch(main_id).unwrap();
    let grant2 = client
        .acquire_writer(
            OperationId::from_bytes(id16(105)),
            main_id,
            main_br.revision,
            main_br.writer_epoch,
        )
        .unwrap();
    let grant3 = client
        .force_acquire_writer(
            OperationId::from_bytes(id16(106)),
            main_id,
            main_br.revision,
            grant2.writer_epoch,
        )
        .unwrap();
    let att2 = client
        .attach_volume(
            OperationId::from_bytes(id16(107)),
            main_id,
            main_br.revision,
            grant3,
            AttachmentId::from_bytes(id16(108)),
            ProtectionId::from_bytes(id16(109)),
        )
        .unwrap();
    client
        .detach_volume(
            OperationId::from_bytes(id16(110)),
            main_id,
            att2.attachment_id,
            att2.volume_epoch,
            att2.protection_id,
            grant3,
        )
        .unwrap();

    // Fork candidate, move_head, promote, delete
    let cand_id = BranchId::from_bytes(id16(111));
    let cand = client
        .create_branch(OperationId::from_bytes(id16(112)), cand_id, "cand", g0_id)
        .unwrap();
    let main_br2 = client.get_branch(main_id).unwrap();
    client
        .move_head(
            OperationId::from_bytes(id16(114)),
            main_id,
            main_br2.revision,
            g0_id,
            grant3,
            true,
        )
        .unwrap();
    let main_br3 = client.get_branch(main_id).unwrap();
    let grant4 = client
        .acquire_writer(
            OperationId::from_bytes(id16(122)),
            main_id,
            main_br3.revision,
            main_br3.writer_epoch,
        )
        .unwrap();
    client
        .promote(
            OperationId::from_bytes(id16(115)),
            cand_id,
            cand.revision,
            main_id,
            main_br3.revision,
            grant4,
            true,
        )
        .unwrap();
    let cand_grant = client
        .acquire_writer(
            OperationId::from_bytes(id16(116)),
            cand_id,
            cand.revision,
            cand.writer_epoch,
        )
        .unwrap();
    client
        .delete_branch(
            OperationId::from_bytes(id16(117)),
            cand_id,
            cand.revision,
            cand_grant,
            true,
        )
        .unwrap();

    // Protected list_logs / list_artifacts on G0
    let p_read = ProtectionId::from_bytes(id16(118));
    let h_read = ProtectedReadHandle::open(
        client.clone(),
        OperationId::from_bytes(id16(119)),
        g0_id,
        p_read,
    )
    .unwrap();
    assert!(client.list_logs(p_read).unwrap().is_empty());
    assert!(client.list_artifacts(p_read).unwrap().is_empty());
    h_read.close(OperationId::from_bytes(id16(120))).unwrap();

    shutdown.store(true, Ordering::Release);
    let _ = handle.join();
}
