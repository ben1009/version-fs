//! End-to-end checkpoint capture coordinator tests against the real toy-kv
//! Metadata Authority outcome gate, durable barrier journal, and capture journal.

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
        ControlError, OwnerIncarnation, WorkspaceControl, WriterGrant, start_authority_owner,
    },
    fault::{FailurePoint, OneShotFailures},
    format::{
        CaptureMode, EXTENT_BYTES, ObjectRef, ProvenanceBinding, ProvenanceContext, VolumeGeometry,
        decode_generation_descriptor,
    },
    graph::verify_generation,
    identity::{
        AttachmentId, BranchId, GenerationId, ObjectId, ObjectStoreScopeId, OperationId,
        PrincipalId, ProtectionId, VolumeBoundaryId, WorkspaceId,
    },
    metadata::{MetadataAuthority, MetadataError},
    persistent_map::{DurableObjectStore, MapObjectStore},
    time::{Clock, MonotonicTime},
};
use versionfs_sdk::{
    barrier::{DurableVolume, FileBarrierJournal, initialize_empty_workspace},
    capture::{
        BlockCutOnlyBoundary, CaptureConfig, CaptureError, CheckpointCoordinator,
        CheckpointRequest, FileCaptureJournal, FilesystemBoundary, NoopProducerGate, ProducerGate,
        RecoveredCapture,
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
            "versionfs-capture-authority-{}-{id}",
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
    arm_on_descriptor_upload: Arc<Mutex<Option<FailurePoint>>>,
}

impl Default for MemoryObjects {
    fn default() -> Self {
        Self {
            scope: ObjectStoreScopeId::from_bytes([0xD1; 16]),
            objects: Arc::new(Mutex::new(HashMap::new())),
            failures: Arc::new(OneShotFailures::default()),
            arm_on_descriptor_upload: Arc::new(Mutex::new(None)),
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
        if reference.object_type == versionfs_core::format::ObjectType::GenerationDescriptor
            && let Some(point) = self.arm_on_descriptor_upload.lock().unwrap().take()
        {
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
    _clock: Arc<TestClock>,
    barrier_dir: PathBuf,
    capture_dir: PathBuf,
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
            _clock: clock,
            barrier_dir: directory.0.join("barrier-journal"),
            capture_dir: directory.0.join("capture-journal"),
            _directory: directory,
        }
    }

    fn open_volume(
        &self,
    ) -> DurableVolume<MemoryObjects, WorkspaceControl<ToyKvMetadataAuthority>, FileBarrierJournal>
    {
        DurableVolume::open(
            self.store.clone(),
            control(self.authority.clone(), self.owner),
            FileBarrierJournal::open(&self.barrier_dir).unwrap(),
            branch(),
            self.grant,
            attachment(),
            EXTENT_BYTES as usize * 2,
        )
        .unwrap()
    }

    fn coordinator(&self) -> CheckpointCoordinator<FileCaptureJournal> {
        CheckpointCoordinator::new(
            FileCaptureJournal::open(&self.capture_dir).unwrap(),
            CaptureConfig::default(),
        )
    }

    fn coordinator_with_failures(
        &self,
        failures: Arc<OneShotFailures>,
    ) -> CheckpointCoordinator<FileCaptureJournal> {
        CheckpointCoordinator::with_failures(
            FileCaptureJournal::open(&self.capture_dir).unwrap(),
            CaptureConfig::default(),
            failures,
        )
    }
}

fn control(
    authority: Arc<ToyKvMetadataAuthority>,
    owner: OwnerIncarnation,
) -> WorkspaceControl<ToyKvMetadataAuthority> {
    WorkspaceControl::new(
        authority,
        WorkspaceId::from_bytes([1; 16]),
        ObjectStoreScopeId::from_bytes([0xD1; 16]),
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

#[derive(Default)]
struct LiveFreezeBoundary {
    journal_write: Option<(u64, Vec<u8>)>,
    frozen: bool,
    thaw_calls: usize,
    unmounted: bool,
}

impl FilesystemBoundary for LiveFreezeBoundary {
    fn freeze(&mut self, device: &mut dyn BlockDevice) -> Result<(), String> {
        if let Some((offset, bytes)) = self.journal_write.clone() {
            device
                .dispatch(BlockRequest::Write {
                    offset,
                    data: &bytes,
                    fua: false,
                })
                .map_err(|error| error.to_string())?;
            device
                .dispatch(BlockRequest::Flush)
                .map_err(|error| error.to_string())?;
        }
        self.frozen = true;
        Ok(())
    }

    fn thaw(&mut self) -> Result<(), String> {
        self.thaw_calls += 1;
        self.frozen = false;
        Ok(())
    }

    fn clean_unmount(&mut self, device: &mut dyn BlockDevice) -> Result<(), String> {
        if let Some((offset, bytes)) = self.journal_write.clone() {
            device
                .dispatch(BlockRequest::Write {
                    offset,
                    data: &bytes,
                    fua: false,
                })
                .map_err(|error| error.to_string())?;
            device
                .dispatch(BlockRequest::Flush)
                .map_err(|error| error.to_string())?;
        }
        self.unmounted = true;
        Ok(())
    }
}

struct DrainTimeBarrierProducer {
    drained: usize,
    commit_once: Option<Box<dyn FnOnce()>>,
}

impl ProducerGate for DrainTimeBarrierProducer {
    fn close_admission(&mut self) -> Result<(), String> {
        Ok(())
    }

    fn drain_producers(&mut self) -> Result<(), String> {
        self.drained += 1;
        if let Some(action) = self.commit_once.take() {
            action();
        }
        Ok(())
    }

    fn reopen_admission(&mut self) -> Result<(), String> {
        Ok(())
    }
}

#[test]
fn toy_kv_publish_unknown_outcome_reconciles_before_thawing_and_syncs_local_volume() {
    for point in [
        FailurePoint::AuthorityAfterCommitBeforeReply,
        FailurePoint::AuthorityCommitOutcomeUnknownWithoutCommit,
    ] {
        let fixture = Fixture::new();
        let mut volume = fixture.open_volume();
        let mut coordinator = fixture.coordinator();
        let mut producers = NoopProducerGate;
        let mut boundary = LiveFreezeBoundary {
            journal_write: Some((4096, vec![0x9B; 4096])),
            ..LiveFreezeBoundary::default()
        };

        *fixture.store.arm_on_descriptor_upload.lock().unwrap() = Some(point);
        let err = coordinator
            .checkpoint(
                &mut volume,
                &mut producers,
                &mut boundary,
                &CheckpointRequest::attached(CaptureMode::FilesystemFrozen, 10_000),
                &[],
            )
            .expect_err("unknown publish_capture outcome must pause before thaw");
        assert!(matches!(
            err,
            CaptureError::Control(ControlError::Metadata(MetadataError::UnknownOutcome { .. }))
        ));
        assert!(
            boundary.frozen,
            "filesystem must stay frozen while outcome is unknown"
        );
        assert!(
            matches!(
                fixture.authority.get(b"probe"),
                Err(MetadataError::ReconciliationRequired { .. })
            ),
            "toy-kv gate must stay closed until reconciliation"
        );
        drop(coordinator);

        let mut watchdog = fixture.coordinator();
        let recovered = watchdog
            .recover_pending_capture(&mut volume, &mut producers, &mut boundary)
            .expect("watchdog recovery should reconcile toy-kv gate and thaw");
        assert!(!boundary.frozen);
        assert_eq!(boundary.thaw_calls, 1);
        assert!(!volume.mutations_closed());

        let branch_now = control(fixture.authority.clone(), fixture.owner)
            .get_branch(branch())
            .unwrap();
        match point {
            FailurePoint::AuthorityAfterCommitBeforeReply => {
                let RecoveredCapture::Published(published) = recovered else {
                    panic!("committed publication must recover as Published");
                };
                assert_eq!(branch_now.head, published.generation_id);
                assert_eq!(volume.revision(), branch_now.revision);
            }
            FailurePoint::AuthorityCommitOutcomeUnknownWithoutCommit => {
                assert!(matches!(
                    recovered,
                    RecoveredCapture::Aborted {
                        freeze_thawed: true,
                        ..
                    }
                ));
                assert!(branch_now.capture.is_none());
                // Child barrier committed during freeze stays durable even though capture aborted.
                assert_eq!(branch_now.revision, 2);
                assert_eq!(
                    volume
                        .dispatch(BlockRequest::Read {
                            offset: 4096,
                            length: 4096
                        })
                        .unwrap(),
                    BlockResponse::Read(vec![0x9B; 4096])
                );
            }
            _ => unreachable!(),
        }
    }
}

#[test]
fn toy_kv_capture_records_provenance_and_verifies_on_fresh_worker() {
    let fixture = Fixture::new();
    let initial = control(fixture.authority.clone(), fixture.owner)
        .get_branch(branch())
        .unwrap();
    let mut volume = fixture.open_volume();
    let mut coordinator = fixture.coordinator();
    let mut producers = NoopProducerGate;
    let mut boundary = LiveFreezeBoundary {
        journal_write: Some((0, vec![0x5E; 4096])),
        ..LiveFreezeBoundary::default()
    };

    let context = ProvenanceContext {
        recording_principal: PrincipalId::from_bytes([1; 16]),
        actor: Some("worker-a".to_owned()),
        execution_id: None,
        session_id: Some("sess-1".to_owned()),
        thread_id: None,
        task_id: Some("task-1".to_owned()),
        workflow_operation_id: None,
        reason: Some("clean unmount checkpoint".to_owned()),
        trace_id: None,
        span_id: None,
        parent_span_id: None,
        required_logs_root: initial.logs_root,
        required_artifacts_root: initial.artifacts_root,
        optional_links: Vec::new(),
    };
    let encoded_context = context.encode().unwrap();
    let context_ref = ObjectRef::from_encoded(&encoded_context).unwrap();
    volume
        .store_mut()
        .put_durable(context_ref, &encoded_context)
        .unwrap();

    let mut request = CheckpointRequest::attached(CaptureMode::FilesystemCleanUnmount, 20_000);
    request.provenance = ProvenanceBinding::Recorded {
        context_root: context_ref,
    };

    let published = coordinator
        .checkpoint(&mut volume, &mut producers, &mut boundary, &request, &[])
        .expect("clean-unmount checkpoint with recorded provenance should publish");
    assert!(boundary.unmounted);
    assert_eq!(boundary.thaw_calls, 0);

    // Fresh worker verifies the complete Generation graph independently from the object store.
    let descriptor_ref = fixture
        .store
        .objects
        .lock()
        .unwrap()
        .values()
        .find_map(|bytes| {
            let decoded = decode_generation_descriptor(bytes).ok()?;
            if decoded.generation_id() == published.generation_id {
                Some(ObjectRef::from_encoded(bytes).unwrap())
            } else {
                None
            }
        })
        .expect("published descriptor in durable object store");
    let proof = verify_generation(&fixture.store, descriptor_ref)
        .expect("fresh worker must verify complete Generation graph");
    assert_eq!(
        proof.generation_descriptor().capture_mode(),
        CaptureMode::FilesystemCleanUnmount
    );
    assert_eq!(
        proof.generation_descriptor().context_root(),
        Some(context_ref)
    );
    assert_eq!(
        proof.provenance_recording_principal(),
        Some(PrincipalId::from_bytes([1; 16]))
    );
}

#[test]
fn toy_kv_drain_conflict_restarts_and_watchdog_fences_without_hot_rebasing() {
    let fixture = Fixture::new();
    let mut volume = fixture.open_volume();
    let failures = Arc::new(OneShotFailures::default());
    let mut coordinator = fixture.coordinator_with_failures(failures.clone());
    let mut boundary = LiveFreezeBoundary::default();

    // Commit a live log mutation during the first drain so acquisition conflicts
    // and restarts Step 1 with a new operation ID.
    let ctrl = control(fixture.authority.clone(), fixture.owner);
    let grant = fixture.grant;
    let mut store_for_drain = fixture.store.clone();
    let mut producers = DrainTimeBarrierProducer {
        drained: 0,
        commit_once: Some(Box::new(move || {
            let current = ctrl.get_branch(branch()).unwrap();
            let mut seg_index =
                versionfs_core::views::LogSegmentIndex::empty(&mut store_for_drain).unwrap();
            let seg_bytes = versionfs_core::format::LogSegment {
                log_id: versionfs_core::identity::LogId::from_bytes([1; 16]),
                first_seq: 0,
                end_seq: 1,
                records: vec![b"drain-event".to_vec()],
                attachments: Vec::new(),
            }
            .encode()
            .unwrap();
            let seg_ref = ObjectRef::from_encoded(&seg_bytes).unwrap();
            store_for_drain.put_durable(seg_ref, &seg_bytes).unwrap();
            seg_index = seg_index
                .insert(
                    &mut store_for_drain,
                    0,
                    versionfs_core::format::SegmentIndexEntry {
                        end_seq: 1,
                        segment: seg_ref,
                    },
                )
                .unwrap();
            let logs_ref = versionfs_core::views::LogMap::empty(&mut store_for_drain)
                .unwrap()
                .insert(
                    &mut store_for_drain,
                    "events",
                    versionfs_core::format::LogMapEntry {
                        log_id: versionfs_core::identity::LogId::from_bytes([1; 16]),
                        first_seq: 0,
                        end_seq: 1,
                        segment_index_root: seg_index.root(),
                    },
                )
                .unwrap()
                .seal(&mut store_for_drain)
                .unwrap();
            let next_roots = versionfs_core::control::LiveRoots {
                volume: current.volume_root,
                logs: logs_ref,
                artifacts: current.artifacts_root,
            };
            ctrl.begin_preparation(versionfs_core::control::PreparationRequest {
                operation_id: op(60),
                branch_id: branch(),
                expected_revision: current.revision,
                grant,
                attachment_id: attachment(),
                volume_epoch: 1,
                protection_id: ProtectionId::from_bytes([60; 16]),
                kind: versionfs_core::control::PreparationKind::LogMutation,
                expected_roots: Some(next_roots),
                dependencies: vec![logs_ref],
            })
            .unwrap();
            ctrl.commit_live_roots(op(60), current.revision, grant, next_roots, None)
                .unwrap();
        })),
    };

    // Also arm a crash right after freeze on the fresh retry to exercise
    // fenced watchdog recovery.
    failures.arm(FailurePoint::FilesystemFreezeAfter);
    let err = coordinator
        .checkpoint(
            &mut volume,
            &mut producers,
            &mut boundary,
            &CheckpointRequest::attached(CaptureMode::FilesystemFrozen, 30_000),
            &[],
        )
        .expect_err("crash after freeze on restarted capture");
    assert!(matches!(
        err,
        CaptureError::InjectedFailure(FailurePoint::FilesystemFreezeAfter)
    ));
    assert_eq!(producers.drained, 2, "must have restarted Step 1 cleanly");
    assert!(boundary.frozen);

    // Watchdog reconciles the interrupted frozen capture cleanly.
    let recovered = coordinator
        .recover_pending_capture(&mut volume, &mut NoopProducerGate, &mut boundary)
        .expect("watchdog recovery should abort and thaw");
    assert!(matches!(
        recovered,
        RecoveredCapture::Aborted {
            freeze_thawed: true,
            ..
        }
    ));
    assert!(!boundary.frozen);

    // Now test a clean block_cut after recovery.
    let published = coordinator
        .checkpoint(
            &mut volume,
            &mut NoopProducerGate,
            &mut BlockCutOnlyBoundary,
            &CheckpointRequest::attached(CaptureMode::BlockCut, 31_000),
            &[],
        )
        .expect("subsequent checkpoint should succeed cleanly");
    assert_eq!(published.branch.head, published.generation_id);
}

#[test]
fn toy_kv_watchdog_recovery_fences_replaced_attachment_without_hot_rebasing() {
    let fixture = Fixture::new();
    let mut volume = fixture.open_volume();
    let failures = Arc::new(OneShotFailures::default());
    failures.arm(FailurePoint::ReplyBeforeSend);
    let mut coordinator = fixture.coordinator_with_failures(failures);
    let mut boundary = LiveFreezeBoundary {
        journal_write: Some((0, vec![0x33; 4096])),
        ..LiveFreezeBoundary::default()
    };

    let err = coordinator
        .checkpoint(
            &mut volume,
            &mut NoopProducerGate,
            &mut boundary,
            &CheckpointRequest::attached(CaptureMode::FilesystemFrozen, 40_000),
            &[],
        )
        .expect_err("crash after publish before local sync and thaw");
    assert!(matches!(
        err,
        CaptureError::InjectedFailure(FailurePoint::ReplyBeforeSend)
    ));
    assert!(boundary.frozen);
    let published_root = control(fixture.authority.clone(), fixture.owner)
        .get_branch(branch())
        .unwrap()
        .volume_root;

    // Before watchdog runs, the old writer expires and a replacement owner takes over.
    fixture._clock.0.store(
        fixture.grant.expires_at.as_nanos().saturating_add(1),
        Ordering::SeqCst,
    );
    let ctrl = control(fixture.authority.clone(), fixture.owner);
    let current = ctrl.get_branch(branch()).unwrap();
    let new_grant = ctrl
        .acquire_writer(op(91), branch(), current.revision, current.writer_epoch)
        .unwrap();
    let new_attachment = AttachmentId::from_bytes([92; 16]);
    ctrl.attach_volume(
        op(92),
        branch(),
        current.revision,
        new_grant,
        new_attachment,
        ProtectionId::from_bytes([92; 16]),
    )
    .unwrap();

    // Watchdog reconciles the committed publication, thaws the old frozen filesystem,
    // and marks the old volume fenced without hot-rebasing it.
    let recovered = coordinator
        .recover_pending_capture(&mut volume, &mut NoopProducerGate, &mut boundary)
        .expect("watchdog must recover committed outcome and thaw even after fencing");
    let RecoveredCapture::Published(published) = recovered else {
        panic!("expected Published recovery");
    };
    assert_eq!(published.branch.volume_root, published_root);
    assert!(!boundary.frozen);
    assert_eq!(boundary.thaw_calls, 1);
    assert!(volume.is_fenced());
    assert!(
        volume
            .dispatch(BlockRequest::Write {
                offset: 4096,
                data: &[0x99; 4096],
                fua: false,
            })
            .is_err(),
        "fenced attachment must reject further writes"
    );
}
