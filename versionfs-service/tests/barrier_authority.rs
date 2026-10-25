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

struct FixedClock;

impl Clock for FixedClock {
    fn now(&self) -> MonotonicTime {
        MonotonicTime::from_nanos(0)
    }
}

#[derive(Clone, Default)]
struct MemoryObjects {
    objects: Arc<Mutex<HashMap<ObjectId, Vec<u8>>>>,
    failures: Arc<OneShotFailures>,
    arm_on_upload: Arc<Mutex<Option<FailurePoint>>>,
}

impl MapObjectStore for MemoryObjects {
    fn proof_scope(&self) -> ObjectStoreScopeId {
        ObjectStoreScopeId::from_bytes([0xA6; 16])
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
    journal_path: PathBuf,
    _directory: TestDirectory,
}

impl Fixture {
    fn new() -> Self {
        let directory = TestDirectory::new();
        let mut store = MemoryObjects::default();
        let authority = Arc::new(
            ToyKvMetadataAuthority::open_with(
                directory.0.join("metadata"),
                Arc::new(FixedClock),
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
    WorkspaceControl::new(
        authority,
        WorkspaceId::from_bytes([1; 16]),
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
