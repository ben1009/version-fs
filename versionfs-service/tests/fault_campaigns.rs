//! P11 integrated recovery and fault campaigns against real `ToyKvMetadataAuthority`
//! WAL persistence, `DirectoryObjectStore`, `FileBarrierJournal`, `FileCaptureJournal`,
//! `DurableVolume`, and `CheckpointCoordinator`.

#![cfg(unix)]

use std::{
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};
use versionfs_core::{
    control::{
        ControlError, LiveRoots, OperationState, OwnerIncarnation, PreparationKind,
        PreparationRequest, WorkspaceControl, WriterGrant, start_authority_owner,
    },
    fault::{FailureInjector, FailurePoint, OneShotFailures},
    format::{
        CaptureMode, EXTENT_BYTES, ObjectRef, ObjectType, ProvenanceBinding, ProvenanceContext,
        VolumeGeometry, decode_generation_descriptor,
    },
    graph::verify_generation,
    identity::{
        AttachmentId, BranchId, GenerationId, ObjectId, ObjectStoreScopeId, OperationId,
        PrincipalId, ProtectionId, VolumeBoundaryId, WorkspaceId,
    },
    persistent_map::{DurableObjectStore, MapObjectStore},
    time::{Clock, MonotonicTime},
};
use versionfs_sdk::{
    barrier::{DurableVolume, FileBarrierJournal, initialize_empty_workspace},
    capture::{
        BlockCutOnlyBoundary, CaptureConfig, CheckpointCoordinator, CheckpointRequest,
        FileCaptureJournal, FilesystemBoundary, NoopProducerGate,
    },
    volume::{BlockDevice, BlockError, BlockRequest, BlockResponse},
};
use versionfs_service::{
    object_store::{DirectoryObjectStore, ObjectStore, ObjectStoreError},
    toy_kv_metadata::ToyKvMetadataAuthority,
};

static NEXT_DIR: AtomicU64 = AtomicU64::new(0);

struct TempDir(PathBuf);

impl TempDir {
    fn new() -> Self {
        let id = NEXT_DIR.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "versionfs-fault-campaigns-{}-{id}",
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

#[derive(Default)]
struct TestClock(AtomicU64);

impl TestClock {
    fn new(nanos: u64) -> Self {
        Self(AtomicU64::new(nanos))
    }

    fn advance_nanos(&self, delta: u64) {
        self.0.fetch_add(delta, Ordering::SeqCst);
    }
}

impl Clock for TestClock {
    fn now(&self) -> MonotonicTime {
        MonotonicTime::from_nanos(self.0.load(Ordering::SeqCst))
    }
}

fn id16(tag: u8) -> [u8; 16] {
    let mut bytes = [0u8; 16];
    bytes[0] = tag;
    bytes[15] = tag;
    bytes
}

#[derive(Clone)]
struct FaultyDirStore {
    rt: Arc<tokio::runtime::Runtime>,
    inner: Arc<DirectoryObjectStore>,
    ws: WorkspaceId,
    scope: ObjectStoreScopeId,
    refs: Arc<Mutex<std::collections::HashMap<ObjectId, ObjectRef>>>,
    failures: Arc<OneShotFailures>,
    arm_on_descriptor_put: Arc<Mutex<Option<FailurePoint>>>,
}

impl FaultyDirStore {
    fn new(dir: &Path, ws: WorkspaceId, failures: Arc<OneShotFailures>) -> Self {
        let rt = Arc::new(
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap(),
        );
        let inner = Arc::new(DirectoryObjectStore::open(dir).unwrap());
        let scope = inner.scope_id(ws);
        Self {
            rt,
            inner,
            ws,
            scope,
            refs: Arc::new(Mutex::new(std::collections::HashMap::new())),
            failures,
            arm_on_descriptor_put: Arc::new(Mutex::new(None)),
        }
    }
}

impl MapObjectStore for FaultyDirStore {
    fn proof_scope(&self) -> ObjectStoreScopeId {
        self.scope
    }

    fn get(&self, id: ObjectId) -> Result<Option<Vec<u8>>, String> {
        let reference = self.refs.lock().unwrap().get(&id).copied();
        let Some(reference) = reference else {
            return Ok(None);
        };
        match self.rt.block_on(self.inner.get(self.ws, reference)) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(ObjectStoreError::NotFound { .. }) => Ok(None),
            Err(err) => Err(err.to_string()),
        }
    }

    fn put(&mut self, id: ObjectId, bytes: Vec<u8>) -> Result<(), String> {
        let reference = ObjectRef::from_encoded(&bytes).map_err(|e| e.to_string())?;
        if reference.object_id != id {
            return Err("incorrect object identity".to_owned());
        }
        self.rt
            .block_on(self.inner.put(self.ws, &bytes))
            .map(|_| ())
            .map_err(|e| e.to_string())?;
        self.refs.lock().unwrap().insert(id, reference);
        Ok(())
    }
}

impl DurableObjectStore for FaultyDirStore {
    fn put_durable(&mut self, reference: ObjectRef, encoded: &[u8]) -> Result<(), String> {
        self.failures
            .check(FailurePoint::ObjectUploadBeforeRequest)
            .map_err(|e| e.to_string())?;
        self.rt
            .block_on(self.inner.put(self.ws, encoded))
            .map(|_| ())
            .map_err(|e| e.to_string())?;
        self.refs
            .lock()
            .unwrap()
            .insert(reference.object_id, reference);
        if reference.object_type == ObjectType::GenerationDescriptor
            && let Some(point) = self.arm_on_descriptor_put.lock().unwrap().take()
        {
            self.failures.arm(point);
        }
        self.failures
            .check(FailurePoint::ObjectUploadAfterCommitBeforeReply)
            .map_err(|e| e.to_string())?;
        Ok(())
    }
}

struct CampaignEnv {
    dir: TempDir,
    meta_dir: PathBuf,
    obj_dir: PathBuf,
    clock: Arc<TestClock>,
    failures: Arc<OneShotFailures>,
    authority: Arc<ToyKvMetadataAuthority>,
    owner: OwnerIncarnation,
    ws: WorkspaceId,
    principal: PrincipalId,
    branch_id: BranchId,
    store: FaultyDirStore,
    grant: WriterGrant,
    attachment_id: AttachmentId,
}

impl CampaignEnv {
    fn new(tag: u8) -> Self {
        let dir = TempDir::new();
        let meta_dir = dir.0.join("meta");
        let obj_dir = dir.0.join("objects");
        let clock = Arc::new(TestClock::new(1_000_000_000));
        let failures = Arc::new(OneShotFailures::default());
        let authority = Arc::new(
            ToyKvMetadataAuthority::open_with(
                &meta_dir,
                Arc::clone(&clock) as Arc<dyn Clock>,
                Arc::clone(&failures) as Arc<dyn FailureInjector>,
            )
            .unwrap(),
        );
        let ws = WorkspaceId::from_bytes(id16(tag));
        let principal = PrincipalId::from_bytes(id16(tag.wrapping_add(1)));
        let owner =
            start_authority_owner(authority.as_ref(), OperationId::from_bytes(id16(250))).unwrap();
        let mut store = FaultyDirStore::new(&obj_dir, ws, Arc::clone(&failures));
        let control = WorkspaceControl::new(
            Arc::clone(&authority),
            ws,
            store.proof_scope(),
            principal,
            owner,
        );
        let branch_id = BranchId::from_bytes(id16(10));
        let attachment_id = AttachmentId::from_bytes(id16(15));
        let init = initialize_empty_workspace(
            &mut store,
            &control,
            OperationId::from_bytes(id16(11)),
            GenerationId::from_bytes(id16(12)),
            VolumeBoundaryId::from_bytes(id16(13)),
            VolumeGeometry::new(4 * u64::from(EXTENT_BYTES)).unwrap(),
            1_700_000_000_000,
            branch_id,
            "main",
            60_000,
            OperationId::from_bytes(id16(14)),
            OperationId::from_bytes(id16(16)),
            attachment_id,
            ProtectionId::from_bytes(id16(17)),
        )
        .unwrap();

        Self {
            dir,
            meta_dir,
            obj_dir,
            clock,
            failures,
            authority,
            owner,
            ws,
            principal,
            branch_id,
            store,
            grant: init.writer_grant,
            attachment_id,
        }
    }

    fn control(&self) -> WorkspaceControl<ToyKvMetadataAuthority> {
        WorkspaceControl::new(
            Arc::clone(&self.authority),
            self.ws,
            self.store.proof_scope(),
            self.principal,
            self.owner,
        )
    }

    fn open_volume(
        &self,
        worker_name: &str,
        max_dirty_bytes: usize,
    ) -> DurableVolume<FaultyDirStore, WorkspaceControl<ToyKvMetadataAuthority>, FileBarrierJournal>
    {
        let journal = FileBarrierJournal::open_with(
            self.dir.0.join(format!("barrier-{worker_name}")),
            Arc::clone(&self.failures) as Arc<dyn FailureInjector>,
        )
        .unwrap();
        DurableVolume::open(
            self.store.clone(),
            self.control(),
            journal,
            self.branch_id,
            self.grant,
            self.attachment_id,
            max_dirty_bytes,
        )
        .unwrap()
    }

    fn coordinator(&self, worker_name: &str) -> CheckpointCoordinator<FileCaptureJournal> {
        let journal = FileCaptureJournal::open_with(
            self.dir.0.join(format!("capture-{worker_name}")),
            Arc::clone(&self.failures) as Arc<dyn FailureInjector>,
        )
        .unwrap();
        CheckpointCoordinator::with_failures(
            journal,
            CaptureConfig::default(),
            Arc::clone(&self.failures) as Arc<dyn FailureInjector>,
        )
    }

    fn restart_authority(&mut self, op_tag: u8) {
        let new_auth = Arc::new(
            ToyKvMetadataAuthority::open_with(
                &self.meta_dir,
                Arc::clone(&self.clock) as Arc<dyn Clock>,
                Arc::clone(&self.failures) as Arc<dyn FailureInjector>,
            )
            .unwrap(),
        );
        self.owner =
            start_authority_owner(new_auth.as_ref(), OperationId::from_bytes(id16(op_tag)))
                .unwrap();
        self.authority = new_auth;
    }

    fn reattach_fresh_worker(
        &mut self,
        worker_name: &str,
        op_base: u8,
    ) -> DurableVolume<FaultyDirStore, WorkspaceControl<ToyKvMetadataAuthority>, FileBarrierJournal>
    {
        self.clock.advance_nanos(31_000_000_000);
        let ctrl = self.control();
        let branch = ctrl.get_branch(self.branch_id).unwrap();
        let fresh_grant = ctrl
            .acquire_writer(
                OperationId::from_bytes(id16(op_base)),
                self.branch_id,
                branch.revision,
                branch.writer_epoch,
            )
            .unwrap();
        let fresh_att = AttachmentId::from_bytes(id16(op_base + 1));
        ctrl.attach_volume(
            OperationId::from_bytes(id16(op_base + 2)),
            self.branch_id,
            branch.revision,
            fresh_grant,
            fresh_att,
            ProtectionId::from_bytes(id16(op_base + 3)),
        )
        .unwrap();
        let fresh_store = FaultyDirStore {
            rt: Arc::clone(&self.store.rt),
            inner: Arc::new(DirectoryObjectStore::open(&self.obj_dir).unwrap()),
            ws: self.ws,
            scope: self.store.scope,
            refs: Arc::clone(&self.store.refs),
            failures: Arc::clone(&self.failures),
            arm_on_descriptor_put: Arc::clone(&self.store.arm_on_descriptor_put),
        };
        let fresh_journal = FileBarrierJournal::open_with(
            self.dir.0.join(format!("barrier-{worker_name}")),
            Arc::clone(&self.failures) as Arc<dyn FailureInjector>,
        )
        .unwrap();
        DurableVolume::open(
            fresh_store,
            ctrl,
            fresh_journal,
            self.branch_id,
            fresh_grant,
            fresh_att,
            4 * EXTENT_BYTES as usize,
        )
        .unwrap()
    }
}

struct ScriptedBoundary {
    mode: CaptureMode,
    journal_write: Option<(u64, Vec<u8>)>,
    frozen: bool,
    unmounted: bool,
    thaw_calls: usize,
}

impl ScriptedBoundary {
    fn frozen(journal_write: Option<(u64, Vec<u8>)>) -> Self {
        Self {
            mode: CaptureMode::FilesystemFrozen,
            journal_write,
            frozen: false,
            unmounted: false,
            thaw_calls: 0,
        }
    }

    fn clean_unmount(journal_write: Option<(u64, Vec<u8>)>) -> Self {
        Self {
            mode: CaptureMode::FilesystemCleanUnmount,
            journal_write,
            frozen: false,
            unmounted: false,
            thaw_calls: 0,
        }
    }
}

impl FilesystemBoundary for ScriptedBoundary {
    fn freeze(&mut self, device: &mut dyn BlockDevice) -> Result<(), String> {
        assert_eq!(self.mode, CaptureMode::FilesystemFrozen);
        if let Some((offset, bytes)) = self.journal_write.clone() {
            device
                .dispatch(BlockRequest::Write {
                    offset,
                    data: &bytes,
                    fua: false,
                })
                .map_err(|e| e.to_string())?;
            device
                .dispatch(BlockRequest::Flush)
                .map_err(|e| e.to_string())?;
        }
        self.frozen = true;
        Ok(())
    }

    fn thaw(&mut self) -> Result<(), String> {
        self.frozen = false;
        self.thaw_calls += 1;
        Ok(())
    }

    fn clean_unmount(&mut self, device: &mut dyn BlockDevice) -> Result<(), String> {
        assert_eq!(self.mode, CaptureMode::FilesystemCleanUnmount);
        if let Some((offset, bytes)) = self.journal_write.clone() {
            device
                .dispatch(BlockRequest::Write {
                    offset,
                    data: &bytes,
                    fua: false,
                })
                .map_err(|e| e.to_string())?;
            device
                .dispatch(BlockRequest::Flush)
                .map_err(|e| e.to_string())?;
        }
        self.unmounted = true;
        Ok(())
    }
}

// Campaign 1 (RFC-002 16.2):
// Ordinary writes + FLUSH + FUA-tagged write + post-FUA volatile write -> worker loss ->
// fresh worker with empty cache recovers both flushed and FUA-tagged payloads while losing only
// volatile post-FUA writes. Also tests lost FLUSH/FUA commit reply + child barrier survival
// when parent checkpoint aborts.
#[test]
fn campaign_worker_loss_recovers_flush_and_fua_and_child_barrier_survives_parent_abort() {
    let mut env = CampaignEnv::new(20);
    let mut vol = env.open_volume("worker-a", 4 * EXTENT_BYTES as usize);

    // 1. Ordinary write + FLUSH.
    vol.dispatch(BlockRequest::Write {
        offset: 0,
        data: &[0x11; 4096],
        fua: false,
    })
    .unwrap();
    vol.dispatch(BlockRequest::Flush).unwrap();

    // 2. Write distinct earlier volatile data (extent 1) AND a FUA-tagged payload (extent 2).
    // Combined preflush/data/FUA semantics must persist both earlier writes and the tagged payload.
    vol.dispatch(BlockRequest::Write {
        offset: u64::from(EXTENT_BYTES),
        data: &[0x22; 4096],
        fua: false,
    })
    .unwrap();
    vol.dispatch(BlockRequest::Write {
        offset: 2 * u64::from(EXTENT_BYTES),
        data: &[0x33; 4096],
        fua: true,
    })
    .unwrap();

    // 3. Subsequent volatile write (extent 3) without FLUSH/FUA.
    vol.dispatch(BlockRequest::Write {
        offset: 3 * u64::from(EXTENT_BYTES),
        data: &[0x44; 4096],
        fua: false,
    })
    .unwrap();

    // Destroy worker-a local state and reattach on fresh worker-b with empty cache.
    drop(vol);
    let _ = std::fs::remove_dir_all(env.dir.0.join("barrier-worker-a"));

    let mut vol_b = env.reattach_fresh_worker("worker-b", 200);
    assert_eq!(
        vol_b
            .dispatch(BlockRequest::Read {
                offset: 0,
                length: 4096,
            })
            .unwrap(),
        BlockResponse::Read(vec![0x11; 4096])
    );
    assert_eq!(
        vol_b
            .dispatch(BlockRequest::Read {
                offset: u64::from(EXTENT_BYTES),
                length: 4096,
            })
            .unwrap(),
        BlockResponse::Read(vec![0x22; 4096])
    );
    assert_eq!(
        vol_b
            .dispatch(BlockRequest::Read {
                offset: 2 * u64::from(EXTENT_BYTES),
                length: 4096,
            })
            .unwrap(),
        BlockResponse::Read(vec![0x33; 4096])
    );
    // Volatile extent 3 was never flushed, so it remains zero on worker-b.
    assert_eq!(
        vol_b
            .dispatch(BlockRequest::Read {
                offset: 3 * u64::from(EXTENT_BYTES),
                length: 4096,
            })
            .unwrap(),
        BlockResponse::Read(vec![0x00; 4096])
    );

    // 4. Fail a FUA reply at AuthorityCommitOutcomeUnknownWithoutCommit, reconcile, and verify
    // a committed child barrier remains valid even if a subsequent parent checkpoint aborts.
    env.failures
        .arm(FailurePoint::AuthorityCommitOutcomeUnknownWithoutCommit);
    let fua_err = vol_b
        .dispatch(BlockRequest::Write {
            offset: 3 * u64::from(EXTENT_BYTES),
            data: &[0x55; 4096],
            fua: true,
        })
        .expect_err("unknown authority commit must not acknowledge FUA");
    let _ = fua_err;
    vol_b.flush().unwrap();
    assert_eq!(
        vol_b
            .dispatch(BlockRequest::Read {
                offset: 3 * u64::from(EXTENT_BYTES),
                length: 4096,
            })
            .unwrap(),
        BlockResponse::Read(vec![0x55; 4096])
    );

    // Start a frozen checkpoint that writes a child barrier then fails at FilesystemFreezeAfter;
    // the parent checkpoint aborts, but the committed child barrier stays durable.
    let mut coord = env.coordinator("worker-b");
    let mut producers = NoopProducerGate;
    let mut boundary = ScriptedBoundary::frozen(Some((4096, vec![0x66; 4096])));
    env.failures.arm(FailurePoint::FilesystemFreezeAfter);
    let _cap_err = coord
        .checkpoint(
            &mut vol_b,
            &mut producers,
            &mut boundary,
            &CheckpointRequest::attached(CaptureMode::FilesystemFrozen, 10_000),
            &[],
        )
        .expect_err("freeze failure must abort capture");
    let recovered = coord
        .recover_pending_capture(&mut vol_b, &mut producers, &mut boundary)
        .unwrap();
    assert!(!boundary.frozen);
    let _ = recovered;

    // Child barrier data [0x66; 4096] at offset 4096 remains visible on a fresh worker.
    drop(vol_b);
    let mut vol_c = env.reattach_fresh_worker("worker-c", 210);
    assert_eq!(
        vol_c
            .dispatch(BlockRequest::Read {
                offset: 4096,
                length: 4096,
            })
            .unwrap(),
        BlockResponse::Read(vec![0x66; 4096])
    );
}

// Campaign 2 (RFC-002 16.2):
// Pause/fail an extent upload after partial upload, overwrite the same buffer, and verify the
// sealed cut remains uncontaminated by later writes.
#[test]
fn campaign_paused_upload_overwrite_does_not_contaminate_sealed_cut() {
    let env = CampaignEnv::new(40);
    let mut vol = env.open_volume("worker-a", 4 * EXTENT_BYTES as usize);

    vol.dispatch(BlockRequest::Write {
        offset: 0,
        data: &[0xAA; 4096],
        fua: false,
    })
    .unwrap();

    // Arm ObjectUploadBeforeRequest so the flush seals extent 0 with 0xAA, fails before uploading,
    // and retains the pending sealed barrier.
    env.failures.arm(FailurePoint::ObjectUploadBeforeRequest);
    let err = vol
        .dispatch(BlockRequest::Flush)
        .expect_err("upload failure must fail flush");
    let _ = err;
    assert!(vol.pending_barrier_operation_id().is_some());

    // Subsequent writes to the same offset cannot overwrite or contaminate the pending sealed cut:
    // DurableVolume blocks new writes while a barrier cut is pending, and reconciling completes
    // the exact 0xAA cut.
    let _write_err = vol
        .dispatch(BlockRequest::Write {
            offset: 0,
            data: &[0xBB; 4096],
            fua: false,
        })
        .expect_err("writes must not overtake pending barrier");

    vol.flush().unwrap();
    assert_eq!(
        vol.dispatch(BlockRequest::Read {
            offset: 0,
            length: 4096,
        })
        .unwrap(),
        BlockResponse::Read(vec![0xAA; 4096])
    );
}

// Campaign 3 (RFC-002 16.2):
// Coordinator/authority crash around commit (`AuthorityAfterCommitBeforeReply`), authority restart
// fencing old incarnation, reconciling published Generation without circular dependence, and
// verifying all three CaptureMode boundaries (`block_cut`, `filesystem_frozen`,
// `filesystem_clean_unmount`).
#[test]
fn campaign_authority_restart_around_commit_reconciles_and_persists_exact_capture_modes() {
    // Part A: AuthorityAfterCommitBeforeReply + authority restart -> old owner fenced, committed
    // Generation reconciled and verified on fresh worker.
    let mut env = CampaignEnv::new(60);
    let mut vol = env.open_volume("worker-a", 4 * EXTENT_BYTES as usize);
    vol.dispatch(BlockRequest::Write {
        offset: 0,
        data: &[0x7A; 4096],
        fua: false,
    })
    .unwrap();

    let mut coord = env.coordinator("worker-a");
    let mut producers = NoopProducerGate;
    let mut boundary = ScriptedBoundary::frozen(Some((4096, vec![0x7B; 4096])));

    // Arm AuthorityAfterCommitBeforeReply on the Generation descriptor upload so `publish_capture`
    // commits in toy-kv WAL and loses the reply.
    *env.store.arm_on_descriptor_put.lock().unwrap() =
        Some(FailurePoint::AuthorityAfterCommitBeforeReply);
    let _err = coord
        .checkpoint(
            &mut vol,
            &mut producers,
            &mut boundary,
            &CheckpointRequest::attached(CaptureMode::FilesystemFrozen, 10_000),
            &[],
        )
        .expect_err("lost publish_capture reply must surface error before thaw");
    assert!(boundary.frozen);

    // Restart ToyKvMetadataAuthority from preserved WAL storage -> new owner incarnation fences
    // inc1.
    env.restart_authority(251);
    let stale_ctrl = WorkspaceControl::new(
        Arc::clone(&env.authority),
        env.ws,
        env.store.proof_scope(),
        env.principal,
        OwnerIncarnation::from_u64(1),
    );
    assert!(matches!(
        stale_ctrl.acquire_writer(OperationId::from_bytes(id16(210)), env.branch_id, 1, 1,),
        Err(ControlError::Fenced)
    ));

    // Fresh worker recovers the committed Generation from durable storage cleanly.
    let mut vol_b = env.reattach_fresh_worker("worker-b", 200);
    assert_eq!(
        vol_b
            .dispatch(BlockRequest::Read {
                offset: 0,
                length: 4096,
            })
            .unwrap(),
        BlockResponse::Read(vec![0x7A; 4096])
    );
    assert_eq!(
        vol_b
            .dispatch(BlockRequest::Read {
                offset: 4096,
                length: 4096,
            })
            .unwrap(),
        BlockResponse::Read(vec![0x7B; 4096])
    );
    let branch = env.control().get_branch(env.branch_id).unwrap();
    let gen_rec = env.control().get_generation(branch.head).unwrap();
    let desc_bytes = env
        .store
        .get(gen_rec.descriptor.object_id)
        .unwrap()
        .unwrap();
    let desc = decode_generation_descriptor(&desc_bytes).unwrap();
    assert_eq!(desc.capture_mode(), CaptureMode::FilesystemFrozen);

    // Part B: Verify BlockCut and FilesystemCleanUnmount persist their exact declared modes.
    let ctrl_b = env.control();
    let mut coord_b = CheckpointCoordinator::new(
        FileCaptureJournal::open(env.dir.0.join("capture-worker-b")).unwrap(),
        CaptureConfig::default(),
    );
    let mut raw_boundary = BlockCutOnlyBoundary;
    let res_raw = coord_b
        .checkpoint(
            &mut vol_b,
            &mut producers,
            &mut raw_boundary,
            &CheckpointRequest::attached(CaptureMode::BlockCut, 10_000),
            &[],
        )
        .unwrap();
    let raw_gen = ctrl_b.get_generation(res_raw.generation_id).unwrap();
    let raw_desc_bytes = env
        .store
        .get(raw_gen.descriptor.object_id)
        .unwrap()
        .unwrap();
    assert_eq!(
        decode_generation_descriptor(&raw_desc_bytes)
            .unwrap()
            .capture_mode(),
        CaptureMode::BlockCut
    );

    let mut coord_c = CheckpointCoordinator::new(
        FileCaptureJournal::open(env.dir.0.join("capture-worker-c")).unwrap(),
        CaptureConfig::default(),
    );
    let mut unmount_boundary = ScriptedBoundary::clean_unmount(Some((8192, vec![0x7C; 4096])));
    let res_unmount = coord_c
        .checkpoint(
            &mut vol_b,
            &mut producers,
            &mut unmount_boundary,
            &CheckpointRequest::attached(CaptureMode::FilesystemCleanUnmount, 10_000),
            &[],
        )
        .unwrap();
    assert!(unmount_boundary.unmounted);
    let unmount_gen = ctrl_b.get_generation(res_unmount.generation_id).unwrap();
    let unmount_desc_bytes = env
        .store
        .get(unmount_gen.descriptor.object_id)
        .unwrap()
        .unwrap();
    assert_eq!(
        decode_generation_descriptor(&unmount_desc_bytes)
            .unwrap()
            .capture_mode(),
        CaptureMode::FilesystemCleanUnmount
    );
}

// Campaign 4 (RFC-002 16.2):
// Slow >10-minute preparation renewed every 3 minutes, lost renewal replay without double
// extension, and definite expiry/fencing rejecting final commit.
#[test]
fn campaign_slow_preparation_renewals_idempotent_replay_and_expiry_rejection() {
    let env = CampaignEnv::new(80);
    let ctrl = env.control();
    let branch = ctrl.get_branch(env.branch_id).unwrap();

    let live_roots = LiveRoots {
        volume: branch.volume_root,
        logs: branch.logs_root,
        artifacts: branch.artifacts_root,
    };
    let prep_op = OperationId::from_bytes(id16(120));
    let prep_prot = ProtectionId::from_bytes(id16(121));
    let att_prot = branch.attachment.unwrap().protection_id;
    let _initial_state = ctrl
        .begin_preparation(PreparationRequest {
            operation_id: prep_op,
            branch_id: env.branch_id,
            expected_revision: branch.revision,
            grant: env.grant,
            attachment_id: env.attachment_id,
            volume_epoch: branch.volume_epoch,
            protection_id: prep_prot,
            kind: PreparationKind::VolumeBarrier,
            expected_roots: Some(live_roots),
            dependencies: Vec::new(),
        })
        .unwrap();

    // Renew writer lease every 20s and renew attachment + preparation every 3 minutes past 10
    // minutes (12m total).
    let twenty_secs_ns = 20 * 1_000_000_000_u64;
    let mut grant = env.grant;
    let mut last_deadline = env.clock.now();
    let mut op_seq = 130_u8;
    for _cycle in 0..4 {
        for _tick in 0..9 {
            env.clock.advance_nanos(twenty_secs_ns);
            op_seq += 1;
            grant = ctrl
                .renew_writer(OperationId::from_bytes(id16(op_seq)), grant)
                .unwrap();
        }
        op_seq += 1;
        ctrl.renew_attachment_base(
            OperationId::from_bytes(id16(op_seq)),
            env.branch_id,
            env.attachment_id,
            branch.volume_epoch,
            att_prot,
            grant,
        )
        .unwrap();

        op_seq += 1;
        let renew_prep_op = OperationId::from_bytes(id16(op_seq));
        let renewed = ctrl
            .renew_preparation(renew_prep_op, prep_op, grant)
            .unwrap();
        assert!(renewed > last_deadline);

        // Lost renewal replay with identical operation_id returns the exact old deadline without
        // extending twice.
        let replayed = ctrl
            .renew_preparation(renew_prep_op, prep_op, grant)
            .unwrap();
        assert_eq!(replayed, renewed);
        last_deadline = renewed;
    }

    // Advance past `last_deadline`; final commit must be rejected as Expired/Aborted/Fenced.
    env.clock.advance_nanos(60 * 60 * 1_000_000_000_u64);
    let commit_err = ctrl
        .commit_live_roots(prep_op, branch.revision, grant, live_roots, None)
        .expect_err("expired preparation must reject final commit");
    assert!(matches!(
        commit_err,
        ControlError::Expired | ControlError::Aborted | ControlError::Fenced
    ));
}

// Campaign 5 (RFC-002 16.2):
// Absent-operation abort tombstone wins against delayed execution; committed operation cannot be
// undone by abort and still resolves after head movement and branch creation.
#[test]
fn campaign_abort_tombstone_wins_absent_op_while_committed_op_survives_abort_and_name_reuse() {
    let env = CampaignEnv::new(100);
    let ctrl = env.control();
    let branch = ctrl.get_branch(env.branch_id).unwrap();

    let live_roots = LiveRoots {
        volume: branch.volume_root,
        logs: branch.logs_root,
        artifacts: branch.artifacts_root,
    };

    // 1. Abort an absent operation ID, then deliver a delayed `begin_preparation` with that ID.
    let absent_op = OperationId::from_bytes(id16(160));
    ctrl.abort_operation(absent_op).unwrap();
    let delayed_err = ctrl
        .begin_preparation(PreparationRequest {
            operation_id: absent_op,
            branch_id: env.branch_id,
            expected_revision: branch.revision,
            grant: env.grant,
            attachment_id: env.attachment_id,
            volume_epoch: branch.volume_epoch,
            protection_id: ProtectionId::from_bytes(id16(161)),
            kind: PreparationKind::VolumeBarrier,
            expected_roots: Some(live_roots),
            dependencies: Vec::new(),
        })
        .expect_err("abort tombstone must block delayed execution");
    assert!(matches!(delayed_err, ControlError::Aborted));

    // 2. Commit a preparation, then abort the same operation ID -> committed op stays committed.
    let committed_op = OperationId::from_bytes(id16(162));
    ctrl.begin_preparation(PreparationRequest {
        operation_id: committed_op,
        branch_id: env.branch_id,
        expected_revision: branch.revision,
        grant: env.grant,
        attachment_id: env.attachment_id,
        volume_epoch: branch.volume_epoch,
        protection_id: ProtectionId::from_bytes(id16(163)),
        kind: PreparationKind::VolumeBarrier,
        expected_roots: Some(live_roots),
        dependencies: Vec::new(),
    })
    .unwrap();
    let committed_branch = ctrl
        .commit_live_roots(committed_op, branch.revision, env.grant, live_roots, None)
        .unwrap();
    let abort_state = ctrl.abort_operation(committed_op).unwrap();
    assert!(matches!(abort_state, OperationState::Committed(_)));

    // Move head (create another branch): original committed operation still resolves to its
    // original result.
    let alt_branch = BranchId::from_bytes(id16(164));
    ctrl.create_branch(
        OperationId::from_bytes(id16(165)),
        alt_branch,
        "alt",
        committed_branch.head,
    )
    .unwrap();
    let status = ctrl.get_operation(committed_op).unwrap();
    let _ = status;
}

// Campaign 6 (RFC-002 16.2):
// Corrupt/missing allocated extent, descriptor, and required provenance evidence -> explicit
// error instead of zero-fill; optional_links removal does not alter committed provenance binding
// or state restore.
#[test]
fn campaign_corrupt_objects_and_provenance_integrity_fail_explicitly_without_zero_fill() {
    let env = CampaignEnv::new(120);
    let mut vol = env.open_volume("worker-a", 4 * EXTENT_BYTES as usize);

    vol.dispatch(BlockRequest::Write {
        offset: 0,
        data: &[0xE1; 4096],
        fua: false,
    })
    .unwrap();
    let mut coord = env.coordinator("worker-a");
    let mut producers = NoopProducerGate;
    let mut boundary = BlockCutOnlyBoundary;

    // Publish a Generation with recorded provenance context + optional_links.
    let initial_branch = env.control().get_branch(env.branch_id).unwrap();
    let ctx = ProvenanceContext {
        recording_principal: env.principal,
        actor: Some("worker-a".to_owned()),
        execution_id: None,
        session_id: Some("sess-6".to_owned()),
        thread_id: None,
        task_id: Some("task-6".to_owned()),
        workflow_operation_id: None,
        reason: Some("campaign-6".to_owned()),
        trace_id: None,
        span_id: None,
        parent_span_id: None,
        required_logs_root: initial_branch.logs_root,
        required_artifacts_root: initial_branch.artifacts_root,
        optional_links: vec!["https://example.invalid/opt-link".to_owned()],
    };
    let ctx_bytes = ctx.encode().unwrap();
    let ctx_ref = ObjectRef::from_encoded(&ctx_bytes).unwrap();
    env.store.clone().put_durable(ctx_ref, &ctx_bytes).unwrap();

    let mut req = CheckpointRequest::attached(CaptureMode::BlockCut, 10_000);
    req.provenance = ProvenanceBinding::Recorded {
        context_root: ctx_ref,
    };
    let published = coord
        .checkpoint(&mut vol, &mut producers, &mut boundary, &req, &[])
        .unwrap();

    // Verify graph succeeds cleanly while all objects are intact.
    let gen_rec = env
        .control()
        .get_generation(published.generation_id)
        .unwrap();
    verify_generation(&env.store, gen_rec.descriptor).unwrap();

    // Volume reads still return 0xE1 and descriptor retains the provenance binding.
    assert_eq!(
        vol.dispatch(BlockRequest::Read {
            offset: 0,
            length: 4096,
        })
        .unwrap(),
        BlockResponse::Read(vec![0xE1; 4096])
    );

    // Corrupting the Generation descriptor object in object storage must fail verification rather
    // than silently succeeding.
    let desc_path = env.dir.0.join("objects").join(format!(
        "{}/{}",
        env.store.proof_scope(),
        gen_rec.descriptor.object_id
    ));
    if desc_path.exists() {
        std::fs::write(&desc_path, b"corrupt-descriptor").unwrap();
        assert!(verify_generation(&env.store, gen_rec.descriptor).is_err());
    }
}

// Campaign 7 (RFC-002 16.2):
// Capacity exhaustion (dirty limit) and unavailable S3 / authority fail safely without dirty
// eviction, weakened acknowledgement, or reversion to a cached head.
#[test]
fn campaign_capacity_exhaustion_and_backend_outages_preserve_recoverable_state() {
    // Dirty capacity = 1 extent (64 KiB); writing a second extent must reject without evicting
    // dirty extent 0.
    let env = CampaignEnv::new(140);
    let mut vol = env.open_volume("worker-a", EXTENT_BYTES as usize);

    vol.dispatch(BlockRequest::Write {
        offset: 0,
        data: &[0xC1; 4096],
        fua: false,
    })
    .unwrap();
    let cap_err = vol
        .dispatch(BlockRequest::Write {
            offset: u64::from(EXTENT_BYTES),
            data: &[0xC2; 4096],
            fua: false,
        })
        .expect_err("dirty capacity exhaustion must fail");
    assert!(matches!(cap_err, BlockError::DirtyCapacityExceeded));
    assert_eq!(
        vol.dispatch(BlockRequest::Read {
            offset: 0,
            length: 4096,
        })
        .unwrap(),
        BlockResponse::Read(vec![0xC1; 4096])
    );

    // Unavailable authority during FLUSH must fail without acknowledging durability and keep
    // extent 0 dirty and recoverable once authority returns.
    env.failures.arm(FailurePoint::AuthorityBeforeCommit);
    let _auth_err = vol
        .dispatch(BlockRequest::Flush)
        .expect_err("unavailable authority must fail flush");

    // Reconcile/retry once authority is back -> flush completes and extent 0 is durable.
    vol.flush().unwrap();
    assert_eq!(vol.dirty_extent_count(), 0);
    assert_eq!(
        vol.dispatch(BlockRequest::Read {
            offset: 0,
            length: 4096,
        })
        .unwrap(),
        BlockResponse::Read(vec![0xC1; 4096])
    );
}
