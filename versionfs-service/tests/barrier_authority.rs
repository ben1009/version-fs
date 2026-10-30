//! Durable barrier recovery against the real toy-kv authority outcome gate.

use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};
use versionfs_core::{
    control::{
        ControlError, OwnerIncarnation, PreparationKind, PreparationRequest, WorkspaceControl,
        WriterGrant, start_authority_owner,
    },
    fault::{FailurePoint, OneShotFailures},
    format::{EXTENT_BYTES, ObjectRef, VolumeGeometry},
    identity::{
        AttachmentId, BranchId, GenerationId, ObjectId, ObjectStoreScopeId, OperationId,
        PrincipalId, ProtectionId, VolumeBoundaryId, WorkspaceId,
    },
    metadata::{MetadataAuthority, MetadataError},
    persistent_map::{DurableObjectStore, MapObjectStore},
    time::{Clock, MonotonicTime},
};
use versionfs_sdk::{
    barrier::{
        BarrierError, BarrierJournal, DurableVolume, FileBarrierJournal, initialize_empty_workspace,
    },
    volume::{BlockDevice, BlockRequest, BlockResponse},
};
use versionfs_service::toy_kv_metadata::ToyKvMetadataAuthority;

static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new() -> Self {
        let id = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "versionfs-barrier-authority-{}-{id}",
            std::process::id()
        ));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[derive(Default)]
struct TestClock(AtomicU64);

impl Clock for TestClock {
    fn now(&self) -> MonotonicTime {
        MonotonicTime::from_nanos(self.0.load(Ordering::SeqCst))
    }
}

#[derive(Clone)]
struct MemoryObjects {
    scope: ObjectStoreScopeId,
    objects: Arc<Mutex<HashMap<ObjectId, Vec<u8>>>>,
    failures: Arc<OneShotFailures>,
    arm_on_upload: Arc<Mutex<Option<FailurePoint>>>,
}

impl Default for MemoryObjects {
    fn default() -> Self {
        Self::with_scope(ObjectStoreScopeId::from_bytes([0xA6; 16]))
    }
}

impl MemoryObjects {
    fn with_scope(scope: ObjectStoreScopeId) -> Self {
        Self {
            scope,
            objects: Arc::new(Mutex::new(HashMap::new())),
            failures: Arc::new(OneShotFailures::default()),
            arm_on_upload: Arc::new(Mutex::new(None)),
        }
    }
}

impl MapObjectStore for MemoryObjects {
    fn proof_scope(&self) -> ObjectStoreScopeId {
        self.scope
    }

    fn get(&self, id: ObjectId) -> Result<Option<Vec<u8>>, String> {
        Ok(self.objects.lock().unwrap().get(&id).cloned())
    }

    fn put(&mut self, id: ObjectId, bytes: Vec<u8>) -> Result<(), String> {
        let reference = ObjectRef::from_encoded(&bytes).map_err(|error| error.to_string())?;
        if reference.object_id != id {
            return Err("incorrect object identity".to_owned());
        }
        let mut objects = self.objects.lock().unwrap();
        if objects.get(&id).is_some_and(|existing| *existing != bytes) {
            return Err("immutable object overwrite".to_owned());
        }
        objects.insert(id, bytes);
        Ok(())
    }
}

impl DurableObjectStore for MemoryObjects {
    fn put_durable(&mut self, reference: ObjectRef, encoded: &[u8]) -> Result<(), String> {
        reference
            .verify(encoded)
            .map_err(|error| error.to_string())?;
        self.put(reference.object_id, encoded.to_vec())?;
        if let Some(point) = self.arm_on_upload.lock().unwrap().take() {
            self.failures.arm(point);
        }
        Ok(())
    }
}

struct Fixture {
    authority: Arc<ToyKvMetadataAuthority>,
    owner: OwnerIncarnation,
    store: MemoryObjects,
    grant: WriterGrant,
    clock: Arc<TestClock>,
    journal_path: PathBuf,
    _directory: TestDirectory,
}

impl Fixture {
    fn new() -> Self {
        let directory = TestDirectory::new();
        let mut store = MemoryObjects::default();
        let clock = Arc::new(TestClock::default());
        let authority = Arc::new(
            ToyKvMetadataAuthority::open_with(
                directory.0.join("metadata"),
                clock.clone(),
                store.failures.clone(),
            )
            .unwrap(),
        );
        let owner = start_authority_owner(authority.as_ref(), op(250)).unwrap();
        let control = control(authority.clone(), owner);
        let initialized = initialize_empty_workspace(
            &mut store,
            &control,
            op(1),
            GenerationId::from_bytes([2; 16]),
            VolumeBoundaryId::from_bytes([3; 16]),
            VolumeGeometry::new(4 * u64::from(EXTENT_BYTES)).unwrap(),
            1,
            branch(),
            "main",
            60_000,
            op(2),
            op(3),
            attachment(),
            ProtectionId::from_bytes([4; 16]),
        )
        .unwrap();
        Self {
            authority,
            owner,
            store,
            grant: initialized.writer_grant,
            clock,
            journal_path: directory.0.join("journal"),
            _directory: directory,
        }
    }

    fn open(
        &self,
    ) -> DurableVolume<MemoryObjects, WorkspaceControl<ToyKvMetadataAuthority>, FileBarrierJournal>
    {
        DurableVolume::open(
            self.store.clone(),
            control(self.authority.clone(), self.owner),
            FileBarrierJournal::open(&self.journal_path).unwrap(),
            branch(),
            self.grant,
            attachment(),
            EXTENT_BYTES as usize,
        )
        .expect("open or recover the block worker")
    }
}

fn control(
    authority: Arc<ToyKvMetadataAuthority>,
    owner: OwnerIncarnation,
) -> WorkspaceControl<ToyKvMetadataAuthority> {
    control_with_scope(authority, owner, ObjectStoreScopeId::from_bytes([0xA6; 16]))
}

fn control_with_scope(
    authority: Arc<ToyKvMetadataAuthority>,
    owner: OwnerIncarnation,
    object_store_scope: ObjectStoreScopeId,
) -> WorkspaceControl<ToyKvMetadataAuthority> {
    WorkspaceControl::new(
        authority,
        WorkspaceId::from_bytes([1; 16]),
        object_store_scope,
        PrincipalId::from_bytes([1; 16]),
        owner,
    )
}

fn op(byte: u8) -> OperationId {
    OperationId::from_bytes([byte; 16])
}

fn branch() -> BranchId {
    BranchId::from_bytes([1; 16])
}

fn attachment() -> AttachmentId {
    AttachmentId::from_bytes([1; 16])
}

#[test]
fn workspace_initialization_rejects_a_mismatched_object_store_namespace() {
    let fixture = Fixture::new();
    let mut store = fixture.store.clone();
    let before = store.objects.lock().unwrap().len();
    let mismatched_control = control_with_scope(
        fixture.authority.clone(),
        fixture.owner,
        ObjectStoreScopeId::from_bytes([0xA7; 16]),
    );

    assert!(matches!(
        initialize_empty_workspace(
            &mut store,
            &mismatched_control,
            op(40),
            GenerationId::from_bytes([41; 16]),
            VolumeBoundaryId::from_bytes([42; 16]),
            VolumeGeometry::new(4 * u64::from(EXTENT_BYTES)).unwrap(),
            43,
            BranchId::from_bytes([44; 16]),
            "wrong-store",
            60_000,
            op(45),
            op(46),
            AttachmentId::from_bytes([47; 16]),
            ProtectionId::from_bytes([48; 16]),
        ),
        Err(BarrierError::ObjectStoreScopeMismatch)
    ));
    assert_eq!(store.objects.lock().unwrap().len(), before);
    assert_eq!(
        mismatched_control.get_branch(BranchId::from_bytes([44; 16])),
        Err(ControlError::NotFound)
    );
}

#[test]
fn volume_open_rejects_a_different_store_even_with_identical_base_objects() {
    let fixture = Fixture::new();
    let foreign = MemoryObjects::with_scope(ObjectStoreScopeId::from_bytes([0xA7; 16]));
    *foreign.objects.lock().unwrap() = fixture.store.objects.lock().unwrap().clone();
    let workspace_control = control(fixture.authority.clone(), fixture.owner);
    let before = workspace_control.get_branch(branch()).unwrap();
    assert!(foreign.get(before.volume_root.object_id).unwrap().is_some());

    let result = DurableVolume::open(
        foreign,
        workspace_control,
        FileBarrierJournal::open(&fixture.journal_path).unwrap(),
        branch(),
        fixture.grant,
        attachment(),
        EXTENT_BYTES as usize,
    );
    assert!(matches!(
        result,
        Err(BarrierError::ObjectStoreScopeMismatch)
    ));
    assert_eq!(
        control(fixture.authority.clone(), fixture.owner)
            .get_branch(branch())
            .unwrap(),
        before
    );
}

#[test]
fn terminal_writer_expiry_stops_the_worker_and_keeps_its_recovery_data() {
    let fixture = Fixture::new();
    let mut worker = fixture.open();
    worker
        .dispatch(BlockRequest::Write {
            offset: 4096,
            data: &[0xEA; 4096],
            fua: false,
        })
        .unwrap();
    fixture
        .clock
        .0
        .store(fixture.grant.expires_at.as_nanos(), Ordering::SeqCst);
    assert!(matches!(
        worker.flush(),
        Err(BarrierError::Control(ControlError::Expired))
    ));
    assert!(
        worker
            .dispatch(BlockRequest::Write {
                offset: 8192,
                data: &[0xFB; 4096],
                fua: false,
            })
            .is_err()
    );
    assert!(matches!(worker.flush(), Err(BarrierError::Fenced)));
    assert_eq!(
        worker
            .dispatch(BlockRequest::Read {
                offset: 4096,
                length: 4096
            })
            .unwrap(),
        BlockResponse::Read(vec![0xEA; 4096])
    );
    assert_eq!(
        worker
            .dispatch(BlockRequest::Read {
                offset: 8192,
                length: 4096
            })
            .unwrap(),
        BlockResponse::Read(vec![0; 4096])
    );
    drop(worker);
    assert!(
        FileBarrierJournal::open(&fixture.journal_path)
            .unwrap()
            .load_pending()
            .unwrap()
            .is_some()
    );
}

#[test]
fn barriers_reconcile_the_real_authority_gate_on_retry_and_worker_reopen() {
    for point in [
        FailurePoint::AuthorityAfterCommitBeforeReply,
        FailurePoint::AuthorityCommitOutcomeUnknownWithoutCommit,
    ] {
        for reopen in [false, true] {
            let fixture = Fixture::new();
            let mut worker = fixture.open();
            worker
                .dispatch(BlockRequest::Write {
                    offset: 4096,
                    data: &[0xD4; 4096],
                    fua: false,
                })
                .unwrap();
            *fixture.store.arm_on_upload.lock().unwrap() = Some(point);
            assert!(matches!(
                worker.flush(),
                Err(BarrierError::Control(ControlError::Metadata(
                    MetadataError::UnknownOutcome { .. }
                )))
            ));
            assert!(matches!(
                fixture.authority.get(b"probe"),
                Err(MetadataError::ReconciliationRequired { .. })
            ));
            if reopen {
                drop(worker);
                worker = fixture.open();
            } else {
                worker.flush().expect("reconcile before reading the branch");
            }
            assert_eq!(
                worker
                    .dispatch(BlockRequest::Read {
                        offset: 4096,
                        length: 4096
                    })
                    .unwrap(),
                BlockResponse::Read(vec![0xD4; 4096])
            );
            let branch = control(fixture.authority.clone(), fixture.owner)
                .get_branch(branch())
                .unwrap();
            assert_eq!(branch.revision, 2, "the barrier must advance the root once");
            assert_eq!(branch.volume_root, worker.live_roots().volume);
            drop(worker);
            assert!(
                FileBarrierJournal::open(&fixture.journal_path)
                    .unwrap()
                    .load_pending()
                    .unwrap()
                    .is_none()
            );
        }
    }
}

#[test]
fn branch_reads_reconcile_unknown_capture_cleanup_without_a_pending_barrier() {
    for point in [
        FailurePoint::AuthorityAfterCommitBeforeReply,
        FailurePoint::AuthorityCommitOutcomeUnknownWithoutCommit,
    ] {
        let fixture = Fixture::new();
        let control = control(fixture.authority.clone(), fixture.owner);
        let mut worker = fixture.open();
        control
            .begin_preparation(PreparationRequest {
                operation_id: op(10),
                branch_id: branch(),
                expected_revision: 1,
                grant: fixture.grant,
                attachment_id: attachment(),
                volume_epoch: 1,
                protection_id: ProtectionId::from_bytes([10; 16]),
                kind: PreparationKind::CheckpointCapture,
                expected_roots: None,
                dependencies: vec![],
            })
            .unwrap();
        control
            .authorize_capture_child(op(11), op(10), op(12), fixture.grant)
            .unwrap();
        fixture.store.failures.arm(point);
        assert!(matches!(
            worker.flush(),
            Err(BarrierError::Control(ControlError::Metadata(
                MetadataError::UnknownOutcome { .. }
            )))
        ));
        worker
            .flush()
            .expect("recover an unknown no-op drain outcome");
        assert_eq!(
            control
                .get_branch(branch())
                .unwrap()
                .capture
                .unwrap()
                .pending_child,
            None
        );
        control
            .seal_capture(op(13), op(10), 1, fixture.grant)
            .unwrap();
    }
}
