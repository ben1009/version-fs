//! Control-protocol integration tests against the toy-kv Metadata Authority.

use std::{
    collections::HashMap,
    env,
    num::NonZeroUsize,
    path::PathBuf,
    sync::{
        Arc, Barrier,
        atomic::{AtomicU64, Ordering},
    },
    thread,
};

use versionfs_core::{
    control::{
        ControlError, GenerationSeed, LiveRoots, OperationState, PreparationKind,
        PreparationRequest, WorkspaceControl, start_authority_owner,
    },
    fault::{FailureInjector, FailurePoint, OneShotFailures},
    format::{
        CaptureMode, EXTENT_BYTES, FormatError, ObjectRef, ObjectType, VolumeGeometry,
        encode_object,
    },
    identity::{
        AttachmentId, BranchId, GenerationId, ObjectId, ObjectStoreScopeId, OperationId, PinId,
        PrincipalId, ProtectionId, VolumeBoundaryId, WorkspaceId,
    },
    metadata::{
        MetadataAuthority, MetadataCursor, MetadataError, MetadataPage, MetadataTxn,
        MutationIdentity, ReconciledOutcome,
    },
    persistent_map::{DurableObjectStore, MapObjectStore},
    time::{Clock, MonotonicTime},
};
use versionfs_sdk::barrier::{VolumeBarrierControl, create_empty_generation, create_generation};
use versionfs_service::toy_kv_metadata::ToyKvMetadataAuthority;

static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(0);

struct TestDir(PathBuf);

impl TestDir {
    fn new() -> Self {
        let id = NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed);
        let path = env::temp_dir().join(format!("versionfs-control-{}-{id}", std::process::id()));
        std::fs::create_dir_all(&path).expect("create control test directory");
        Self(path)
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct TestObjectStore {
    objects: HashMap<ObjectId, Vec<u8>>,
    scope: ObjectStoreScopeId,
}

impl Default for TestObjectStore {
    fn default() -> Self {
        Self::with_scope(ObjectStoreScopeId::from_bytes([0xD4; 16]))
    }
}

impl TestObjectStore {
    fn with_scope(scope: ObjectStoreScopeId) -> Self {
        Self {
            objects: HashMap::new(),
            scope,
        }
    }
}

impl MapObjectStore for TestObjectStore {
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

impl DurableObjectStore for TestObjectStore {
    fn put_durable(&mut self, reference: ObjectRef, encoded: &[u8]) -> Result<(), String> {
        reference
            .verify(encoded)
            .map_err(|error| format!("invalid durable test object: {error}"))?;
        match self.objects.get(&reference.object_id) {
            Some(existing) if existing != encoded => {
                Err("immutable test object ID was reused with different bytes".to_owned())
            }
            Some(_) => Ok(()),
            None => {
                self.objects.insert(reference.object_id, encoded.to_vec());
                Ok(())
            }
        }
    }
}

#[derive(Default)]
struct TestClock(AtomicU64);

impl TestClock {
    fn set(&self, nanos: u64) {
        self.0.store(nanos, Ordering::SeqCst);
    }
}

impl Clock for TestClock {
    fn now(&self) -> MonotonicTime {
        MonotonicTime::from_nanos(self.0.load(Ordering::SeqCst))
    }
}

// Advances the authority clock after the domain buffers its outcome, just
// before the final gated lease decision.
struct AdvanceAtOutcome {
    inner: Arc<ToyKvMetadataAuthority>,
    clock: Arc<TestClock>,
    advance_to: AtomicU64,
}

struct AdvanceAtOutcomeTxn<'a> {
    inner: &'a mut dyn MetadataTxn,
    outcome_key: &'a [u8],
    clock: &'a TestClock,
    advance_to: &'a AtomicU64,
}

impl MetadataTxn for AdvanceAtOutcomeTxn<'_> {
    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, MetadataError> {
        self.inner.get(key)
    }

    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), MetadataError> {
        self.inner.put(key, value)?;
        if key == self.outcome_key {
            let deadline = self.advance_to.swap(0, Ordering::SeqCst);
            if deadline != 0 {
                self.clock.set(deadline);
            }
        }
        Ok(())
    }

    fn delete(&mut self, key: &[u8]) -> Result<(), MetadataError> {
        self.inner.delete(key)
    }

    fn now(&self) -> MonotonicTime {
        self.inner.now()
    }
}

impl MetadataAuthority for AdvanceAtOutcome {
    type Snapshot = <ToyKvMetadataAuthority as MetadataAuthority>::Snapshot;

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, MetadataError> {
        self.inner.get(key)
    }

    fn transact<R, F>(
        &self,
        operation: MutationIdentity<'_>,
        mut body: F,
    ) -> Result<R, MetadataError>
    where
        F: FnMut(&mut dyn MetadataTxn) -> Result<R, MetadataError>,
    {
        self.inner.transact(operation, |txn| {
            body(&mut AdvanceAtOutcomeTxn {
                inner: txn,
                outcome_key: operation.outcome_key,
                clock: &self.clock,
                advance_to: &self.advance_to,
            })
        })
    }

    fn reconcile_unknown(
        &self,
        operation: MutationIdentity<'_>,
    ) -> Result<ReconciledOutcome, MetadataError> {
        self.inner.reconcile_unknown(operation)
    }

    fn snapshot(&self) -> Result<Self::Snapshot, MetadataError> {
        self.inner.snapshot()
    }

    fn scan_prefix(
        &self,
        snapshot: &Self::Snapshot,
        prefix: &[u8],
        cursor: Option<&MetadataCursor>,
        limit: NonZeroUsize,
    ) -> Result<MetadataPage, MetadataError> {
        self.inner.scan_prefix(snapshot, prefix, cursor, limit)
    }
}

struct TestEnv {
    _dir: TestDir,
    clock: Arc<TestClock>,
    authority: Arc<ToyKvMetadataAuthority>,
    workspace_id: WorkspaceId,
    principal_id: PrincipalId,
    owner: versionfs_core::control::OwnerIncarnation,
    failures: Arc<OneShotFailures>,
    control: WorkspaceControl<ToyKvMetadataAuthority>,
}

impl TestEnv {
    fn new() -> Self {
        Self::with_initial_seed(seed(1))
    }

    fn with_initial_seed(initial_seed: GenerationSeed) -> Self {
        let dir = TestDir::new();
        let clock = Arc::new(TestClock::default());
        let failures = Arc::new(OneShotFailures::default());
        let failure_injector: Arc<dyn FailureInjector> = failures.clone();
        let authority = Arc::new(
            ToyKvMetadataAuthority::open_with(
                &dir.0,
                Arc::clone(&clock) as Arc<dyn Clock>,
                failure_injector,
            )
            .expect("open toy-kv authority"),
        );
        let owner = start_authority_owner(authority.as_ref(), op(250)).expect("start owner");
        assert_eq!(
            start_authority_owner(authority.as_ref(), op(250)).expect("replay owner start"),
            owner
        );
        let workspace_id = workspace(1);
        let principal_id = principal(1);
        let control = control_for(Arc::clone(&authority), workspace_id, principal_id, owner);
        control
            .create_workspace(op(1), branch(1), "main", 60_000, initial_seed)
            .expect("create initial workspace");
        Self {
            _dir: dir,
            clock,
            authority,
            workspace_id,
            principal_id,
            owner,
            failures,
            control,
        }
    }

    fn control_for_owner(
        &self,
        owner: versionfs_core::control::OwnerIncarnation,
    ) -> WorkspaceControl<ToyKvMetadataAuthority> {
        control_for(
            Arc::clone(&self.authority),
            self.workspace_id,
            self.principal_id,
            owner,
        )
    }

    fn writer_and_attachment(&self) -> versionfs_core::control::WriterGrant {
        let grant = self
            .control
            .acquire_writer(op(2), branch(1), 1, 0)
            .expect("acquire writer");
        self.control
            .attach_volume(op(3), branch(1), 1, grant, attachment(1), protection(1))
            .expect("attach volume");
        grant
    }

    fn start_sealed_capture(
        &self,
        capture_id: OperationId,
        protection_id: ProtectionId,
        grant: versionfs_core::control::WriterGrant,
        roots: LiveRoots,
    ) {
        self.control
            .begin_preparation(PreparationRequest {
                operation_id: capture_id,
                branch_id: branch(1),
                expected_revision: 1,
                grant,
                attachment_id: attachment(1),
                volume_epoch: 1,
                protection_id,
                kind: PreparationKind::CheckpointCapture,
                expected_roots: None,
                dependencies: vec![roots.volume, roots.logs, roots.artifacts],
            })
            .expect("begin protected capture");
        self.control
            .seal_capture(op(251), capture_id, 1, grant)
            .expect("seal capture");
    }
}

fn advancing_control(env: &TestEnv) -> (Arc<AdvanceAtOutcome>, WorkspaceControl<AdvanceAtOutcome>) {
    let authority = Arc::new(AdvanceAtOutcome {
        inner: Arc::clone(&env.authority),
        clock: Arc::clone(&env.clock),
        advance_to: AtomicU64::new(0),
    });
    let control = WorkspaceControl::new(
        Arc::clone(&authority),
        env.workspace_id,
        ObjectStoreScopeId::from_bytes([0xD4; 16]),
        env.principal_id,
        env.owner,
    );
    (authority, control)
}

fn maintain_leases(
    env: &TestEnv,
    mut grant: versionfs_core::control::WriterGrant,
    steps: u8,
    renew_attachment: bool,
) -> versionfs_core::control::WriterGrant {
    for step in 1..=steps {
        env.clock.set(u64::from(step) * 20_000_000_000);
        grant = env.control.renew_writer(op(100 + step), grant).unwrap();
        if renew_attachment && step % 14 == 0 {
            env.control
                .renew_attachment_base(
                    op(200 + step / 14),
                    branch(1),
                    attachment(1),
                    1,
                    protection(1),
                    grant,
                )
                .unwrap();
        }
    }
    grant
}

#[test]
fn retired_protection_ids_cannot_be_rebound_or_release_another_reader() {
    let env = TestEnv::new();
    env.control
        .register_generation(op(10), seed(2))
        .expect("register retained Generation");
    env.control
        .open_generation(op(11), generation(2), protection(10))
        .expect("open original reader");
    env.control
        .close_protection(op(12), protection(10))
        .expect("close original reader");
    assert_eq!(
        env.control
            .open_generation(op(13), generation(1), protection(10)),
        Err(ControlError::Conflict)
    );
    assert_eq!(
        env.control
            .open_live_log(op(14), branch(1), 1, roots(1).logs, protection(10)),
        Err(ControlError::Conflict)
    );

    env.control
        .open_generation(op(15), generation(2), protection(11))
        .expect("open fresh reader identity");
    assert_eq!(
        env.control.close_protection(op(16), protection(10)),
        Err(ControlError::NotFound)
    );
    assert_eq!(
        env.control.renew_protection(op(17), protection(10)),
        Err(ControlError::NotFound)
    );
    assert_eq!(
        env.control
            .mark_generation_unretained(op(18), generation(2)),
        Err(ControlError::Busy)
    );

    let deadline = env
        .control
        .open_live_log(op(19), branch(1), 1, roots(1).logs, protection(12))
        .expect("open live-log reader");
    env.clock.set(deadline.as_nanos());
    env.control
        .expire_protection(op(20), protection(12))
        .expect("expire live-log reader");
    assert_eq!(
        env.control
            .open_generation(op(21), generation(1), protection(12)),
        Err(ControlError::Conflict)
    );
    env.control
        .expire_protection(op(22), protection(11))
        .expect("expire Generation reader");
    assert_eq!(
        env.control
            .open_generation(op(23), generation(1), protection(11)),
        Err(ControlError::Conflict)
    );
    env.control
        .mark_generation_unretained(op(24), generation(2))
        .expect("reader counters were released exactly once");
}

#[test]
fn zero_preparation_identities_remain_queryable_and_cleanup_releases_admission() {
    let env = TestEnv::new();
    let grant = env.writer_and_attachment();
    let preparing = env
        .control
        .begin_preparation(PreparationRequest {
            operation_id: op(30),
            branch_id: branch(1),
            expected_revision: 1,
            grant,
            attachment_id: attachment(1),
            volume_epoch: 1,
            protection_id: protection(0),
            kind: PreparationKind::CheckpointCapture,
            expected_roots: None,
            dependencies: vec![],
        })
        .expect("zero protection ID is an opaque identity");
    assert_eq!(env.control.get_operation(op(30)), Ok(preparing));
    assert_eq!(
        env.control.abort_operation(op(30)),
        Ok(OperationState::Aborted)
    );
    assert!(env.control.get_branch(branch(1)).unwrap().capture.is_none());
    assert_eq!(
        env.control
            .open_generation(op(31), generation(1), protection(0)),
        Err(ControlError::Conflict)
    );

    env.control
        .create_branch(op(32), branch(0), "zero", generation(1))
        .expect("create zero branch identity");
    let zero_grant = env
        .control
        .acquire_writer(op(33), branch(0), 1, 0)
        .expect("acquire zero branch writer");
    env.control
        .attach_volume(
            op(34),
            branch(0),
            1,
            zero_grant,
            attachment(0),
            protection(34),
        )
        .expect("attach zero branch");
    let preparing = env
        .control
        .begin_preparation(PreparationRequest {
            operation_id: op(35),
            branch_id: branch(0),
            expected_revision: 1,
            grant: zero_grant,
            attachment_id: attachment(0),
            volume_epoch: 1,
            protection_id: protection(35),
            kind: PreparationKind::VolumeBarrier,
            expected_roots: Some(replace_volume_root(roots(1), 35)),
            dependencies: vec![],
        })
        .expect("zero branch ID survives preparation decoding");
    assert_eq!(env.control.get_operation(op(35)), Ok(preparing.clone()));
    let OperationState::Preparing {
        expires_at,
        branch_id,
        ..
    } = preparing
    else {
        panic!("expected preparation");
    };
    assert_eq!(branch_id, branch(0));
    env.clock.set(expires_at.as_nanos());
    assert_eq!(
        env.control.expire_preparation(op(35)),
        Ok(OperationState::Aborted)
    );
    assert_eq!(
        env.control
            .get_branch(branch(0))
            .unwrap()
            .pending_mutation_preparations,
        0
    );
    assert_eq!(
        env.control
            .open_generation(op(36), generation(1), protection(35)),
        Err(ControlError::Conflict)
    );
    env.control
        .expire_protection(op(37), protection(34))
        .expect("expire attachment after preparation cleanup");
    assert_eq!(
        env.control
            .open_generation(op(38), generation(1), protection(34)),
        Err(ControlError::Conflict)
    );
}

#[test]
fn capture_children_and_cleanup_keep_the_original_principal_scope() {
    let env = TestEnv::new();
    let grant = env.writer_and_attachment();
    let other = control_for(
        Arc::clone(&env.authority),
        env.workspace_id,
        principal(2),
        env.owner,
    );

    for (marker, expire) in [(100, false), (120, true)] {
        let capture_id = op(marker);
        let child_id = op(marker + 2);
        let OperationState::Preparing { expires_at, .. } = env
            .control
            .begin_preparation(PreparationRequest {
                operation_id: capture_id,
                branch_id: branch(1),
                expected_revision: 1,
                grant,
                attachment_id: attachment(1),
                volume_epoch: 1,
                protection_id: protection(marker),
                kind: PreparationKind::CheckpointCapture,
                expected_roots: None,
                dependencies: vec![],
            })
            .expect("begin caller-scoped capture")
        else {
            panic!("capture must remain in preparation");
        };
        assert_eq!(
            env.control
                .get_branch(branch(1))
                .unwrap()
                .capture
                .unwrap()
                .principal_id,
            env.principal_id
        );
        assert_eq!(
            other.authorize_capture_child(op(marker + 1), capture_id, child_id, grant),
            Err(ControlError::Fenced)
        );
        env.control
            .authorize_capture_child(op(marker + 1), capture_id, child_id, grant)
            .expect("owning caller reserves its child");
        assert_eq!(
            other.begin_preparation(PreparationRequest {
                operation_id: child_id,
                branch_id: branch(1),
                expected_revision: 1,
                grant,
                attachment_id: attachment(1),
                volume_epoch: 1,
                protection_id: protection(marker + 3),
                kind: PreparationKind::VolumeBarrier,
                expected_roots: Some(replace_volume_root(roots(1), marker)),
                dependencies: vec![],
            }),
            Err(ControlError::Fenced)
        );
        assert_eq!(
            other.seal_capture(op(marker + 4), capture_id, 1, grant),
            Err(ControlError::Fenced)
        );

        assert_eq!(other.abort_operation(child_id), Ok(OperationState::Aborted));
        assert_eq!(
            env.control.get_operation(child_id),
            Err(ControlError::NotFound)
        );
        assert_eq!(
            env.control
                .get_branch(branch(1))
                .unwrap()
                .capture
                .unwrap()
                .pending_child,
            Some(child_id)
        );
        let terminal = if expire {
            env.clock.set(expires_at.as_nanos());
            env.control.expire_preparation(capture_id)
        } else {
            env.control.abort_operation(capture_id)
        };
        assert_eq!(terminal, Ok(OperationState::Aborted));
        assert_eq!(
            env.control.get_operation(child_id),
            Ok(OperationState::Aborted)
        );
        assert!(env.control.get_branch(branch(1)).unwrap().capture.is_none());
    }
}

#[test]
fn generation_catalog_rejects_mistyped_roots_before_persisting_an_operation() {
    let env = TestEnv::new();
    let mut bad_volume = seed(50);
    bad_volume.roots.volume = object(ObjectType::LogMapRoot, 50);
    assert_eq!(
        env.control.register_generation(op(51), bad_volume),
        Err(ControlError::Format(FormatError::ObjectReferenceMismatch))
    );
    assert_eq!(
        env.control.get_operation(op(51)),
        Err(ControlError::NotFound)
    );

    let mut bad_descriptor = seed(52);
    bad_descriptor.descriptor = object(ObjectType::VolumeRoot, 52);
    assert_eq!(
        env.control.register_generation(op(53), bad_descriptor),
        Err(ControlError::Format(FormatError::ObjectReferenceMismatch))
    );
    assert_eq!(
        env.control.get_operation(op(53)),
        Err(ControlError::NotFound)
    );
}

fn control_for(
    authority: Arc<ToyKvMetadataAuthority>,
    workspace_id: WorkspaceId,
    principal_id: PrincipalId,
    owner: versionfs_core::control::OwnerIncarnation,
) -> WorkspaceControl<ToyKvMetadataAuthority> {
    WorkspaceControl::new(
        authority,
        workspace_id,
        ObjectStoreScopeId::from_bytes([0xD4; 16]),
        principal_id,
        owner,
    )
}

fn id<const N: usize>(byte: u8) -> [u8; N] {
    [byte; N]
}

fn op(byte: u8) -> OperationId {
    OperationId::from_bytes(id(byte))
}

fn workspace(byte: u8) -> WorkspaceId {
    WorkspaceId::from_bytes(id(byte))
}

fn principal(byte: u8) -> PrincipalId {
    PrincipalId::from_bytes(id(byte))
}

fn branch(byte: u8) -> BranchId {
    BranchId::from_bytes(id(byte))
}

fn generation(byte: u8) -> GenerationId {
    GenerationId::from_bytes(id(byte))
}

fn attachment(byte: u8) -> AttachmentId {
    AttachmentId::from_bytes(id(byte))
}

fn protection(byte: u8) -> ProtectionId {
    ProtectionId::from_bytes(id(byte))
}

fn pin(byte: u8) -> PinId {
    PinId::from_bytes(id(byte))
}

fn object(kind: ObjectType, marker: u8) -> ObjectRef {
    let bytes = encode_object(kind, &[marker]).expect("encode typed test object");
    ObjectRef::from_encoded(&bytes).expect("reference encoded test object")
}

fn roots(marker: u8) -> LiveRoots {
    LiveRoots {
        volume: object(ObjectType::VolumeRoot, marker),
        logs: object(ObjectType::LogMapRoot, marker),
        artifacts: object(ObjectType::ArtifactMapRoot, marker),
    }
}

fn replace_volume_root(current: LiveRoots, marker: u8) -> LiveRoots {
    LiveRoots {
        volume: object(ObjectType::VolumeRoot, marker),
        logs: current.logs,
        artifacts: current.artifacts,
    }
}

fn seed(marker: u8) -> GenerationSeed {
    GenerationSeed {
        generation_id: generation(marker),
        descriptor: object(ObjectType::GenerationDescriptor, marker),
        roots: roots(marker),
    }
}

fn empty_generation(
    store: &mut TestObjectStore,
    generation_id: GenerationId,
    boundary_id: u8,
) -> GenerationSeed {
    create_empty_generation(
        store,
        generation_id,
        VolumeBoundaryId::from_bytes(id(boundary_id)),
        VolumeGeometry::new(u64::from(EXTENT_BYTES) * 4).expect("valid test geometry"),
        u64::from(boundary_id),
    )
    .expect("create verified empty Generation graph")
}

#[test]
fn operation_results_are_replay_safe_and_branch_names_use_atomic_point_claims() {
    let env = TestEnv::new();
    let created = env
        .control
        .create_branch(op(10), branch(2), "agent", generation(1))
        .expect("create branch");
    assert_eq!(
        env.control
            .create_branch(op(10), branch(2), "agent", generation(1))
            .expect("replay branch create"),
        created
    );
    assert_eq!(
        env.control
            .create_branch(op(10), branch(2), "renamed", generation(1)),
        Err(ControlError::OperationMismatch)
    );
    assert_eq!(
        env.control
            .create_branch(op(11), branch(3), "agent", generation(1)),
        Err(ControlError::Conflict)
    );
    assert_eq!(env.control.branch_by_name("agent").unwrap(), created);

    let barrier = Arc::new(Barrier::new(3));
    let racers = (0..2)
        .map(|offset| {
            let authority = Arc::clone(&env.authority);
            let barrier = Arc::clone(&barrier);
            let workspace_id = env.workspace_id;
            let principal_id = env.principal_id;
            let owner = env.owner;
            thread::spawn(move || {
                let control = control_for(authority, workspace_id, principal_id, owner);
                barrier.wait();
                control.create_branch(
                    op(20 + offset),
                    branch(20 + offset),
                    "racing-name",
                    generation(1),
                )
            })
        })
        .collect::<Vec<_>>();
    barrier.wait();
    let results = racers
        .into_iter()
        .map(|thread| thread.join().expect("branch claimant did not panic"))
        .collect::<Vec<_>>();
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|result| matches!(result, Err(ControlError::Conflict)))
            .count(),
        1
    );
}

#[test]
fn attached_capture_publication_moves_head_and_attachment_base_atomically() {
    let mut object_store = TestObjectStore::default();
    let initial = empty_generation(&mut object_store, generation(1), 30);
    let new_generation = create_generation(
        &mut object_store,
        generation(2),
        Some(initial.generation_id),
        initial.roots,
        CaptureMode::FilesystemCleanUnmount,
        VolumeBoundaryId::from_bytes(id(31)),
        31,
    )
    .expect("create verified child Generation");
    let env = TestEnv::with_initial_seed(initial);
    let grant = env.writer_and_attachment();
    let capture_id = op(30);
    env.start_sealed_capture(capture_id, protection(30), grant, initial.roots);

    let wrong_parent = create_generation(
        &mut object_store,
        generation(4),
        Some(generation(9)),
        initial.roots,
        CaptureMode::BlockCut,
        VolumeBoundaryId::from_bytes(id(32)),
        32,
    )
    .expect("create verified Generation with wrong lineage");
    assert_eq!(
        env.control.publish_capture(
            capture_id,
            1,
            generation(1),
            grant,
            attachment(1),
            1,
            wrong_parent,
            true,
        ),
        Err(ControlError::InvalidTransition)
    );

    let different_roots = create_empty_generation(
        &mut object_store,
        generation(5),
        VolumeBoundaryId::from_bytes(id(33)),
        VolumeGeometry::new(u64::from(EXTENT_BYTES) * 8).expect("valid alternate geometry"),
        33,
    )
    .expect("create alternate valid Generation graph")
    .roots;
    let wrong_roots = create_generation(
        &mut object_store,
        generation(6),
        Some(generation(1)),
        different_roots,
        CaptureMode::BlockCut,
        VolumeBoundaryId::from_bytes(id(34)),
        34,
    )
    .expect("create verified Generation with different sealed roots");
    assert!(matches!(
        env.control.publish_capture(
            capture_id,
            1,
            generation(1),
            grant,
            attachment(1),
            1,
            wrong_roots,
            true,
        ),
        Err(ControlError::InvalidTransition)
    ));

    env.failures
        .arm(FailurePoint::AuthorityAfterCommitBeforeReply);
    assert!(matches!(
        env.control.publish_capture(
            capture_id,
            1,
            generation(1),
            grant,
            attachment(1),
            1,
            new_generation.clone(),
            true,
        ),
        Err(ControlError::Metadata(
            versionfs_core::metadata::MetadataError::UnknownOutcome { .. }
        ))
    ));

    let published = env
        .control
        .publish_capture(
            capture_id,
            1,
            generation(1),
            grant,
            attachment(1),
            1,
            new_generation.clone(),
            true,
        )
        .expect("reconcile the committed publication");
    assert_eq!(published.generation_id, generation(2));
    assert!(published.head_advanced);
    assert_eq!(published.branch.head, generation(2));
    assert_eq!(published.branch.revision, 2);
    assert!(published.branch.capture.is_none());
    let attachment_state = published.branch.attachment.expect("retained attachment");
    assert_eq!(attachment_state.base_generation, generation(2));
    assert_eq!(
        env.control
            .get_branch(branch(1))
            .expect("read published branch"),
        published.branch
    );
    let renewed_attachment = env
        .control
        .renew_attachment_base(op(32), branch(1), attachment(1), 1, protection(1), grant)
        .expect("renew the attachment protection on the published Generation");
    assert_eq!(renewed_attachment.base_generation, generation(2));
    env.control
        .mark_generation_unretained(op(33), generation(1))
        .expect("old head has no remaining branch or attachment references");
    assert_eq!(
        env.control
            .publish_capture(
                capture_id,
                1,
                generation(1),
                grant,
                attachment(1),
                1,
                new_generation.clone(),
                true,
            )
            .expect("replay publication"),
        published
    );
    assert_eq!(
        env.control.publish_capture(
            capture_id,
            1,
            generation(1),
            grant,
            attachment(1),
            1,
            new_generation.clone(),
            false,
        ),
        Err(ControlError::OperationMismatch)
    );
    let operation = env.control.get_operation(capture_id);
    assert!(
        matches!(operation, Ok(OperationState::Committed(_))),
        "unexpected publication outcome: {operation:?}"
    );
    assert_eq!(
        env.control
            .mark_generation_unretained(op(31), generation(2)),
        Err(ControlError::Busy)
    );
}

#[test]
fn capture_publication_rechecks_each_lease_after_buffering_its_outcome() {
    for lease in ["writer", "attachment", "preparation"] {
        let mut store = TestObjectStore::default();
        let initial = empty_generation(&mut store, generation(1), 70);
        let child = create_generation(
            &mut store,
            generation(2),
            Some(generation(1)),
            initial.roots,
            CaptureMode::BlockCut,
            VolumeBoundaryId::from_bytes(id(71)),
            71,
        )
        .unwrap();
        let env = TestEnv::with_initial_seed(initial);
        let mut grant = env.writer_and_attachment();
        let capture_id = op(30);
        env.start_sealed_capture(capture_id, protection(30), grant, initial.roots);
        match lease {
            "attachment" => grant = maintain_leases(&env, grant, 14, false),
            "preparation" => grant = maintain_leases(&env, grant, 29, true),
            _ => {}
        }
        let before = env.control.get_branch(branch(1)).unwrap();
        let preparation_deadline = match env.control.get_operation(capture_id).unwrap() {
            OperationState::Preparing { expires_at, .. } => expires_at,
            other => panic!("unexpected capture state: {other:?}"),
        };
        let attachment_deadline = before.attachment.unwrap().protection_expires_at;
        let deadline = match lease {
            "writer" => grant.expires_at,
            "attachment" => attachment_deadline,
            _ => preparation_deadline,
        };
        assert_eq!(
            deadline,
            grant
                .expires_at
                .min(attachment_deadline)
                .min(preparation_deadline)
        );
        let (authority, control) = advancing_control(&env);
        authority
            .advance_to
            .store(deadline.as_nanos(), Ordering::SeqCst);

        assert_eq!(
            control.publish_capture(
                capture_id,
                1,
                generation(1),
                grant,
                attachment(1),
                1,
                child,
                true,
            ),
            Err(ControlError::Expired),
            "{lease} expired before the commit decision"
        );
        assert_eq!(env.control.get_branch(branch(1)).unwrap(), before);
        assert!(matches!(
            env.control.get_operation(capture_id),
            Ok(OperationState::Preparing { .. })
        ));
        assert_eq!(
            env.control
                .open_generation(op(80), generation(2), protection(80)),
            Err(ControlError::NotFound)
        );
    }
}

#[test]
fn committed_capture_replays_after_lease_expiry_when_the_reply_was_lost() {
    let mut store = TestObjectStore::default();
    let initial = empty_generation(&mut store, generation(1), 72);
    let child = create_generation(
        &mut store,
        generation(2),
        Some(generation(1)),
        initial.roots,
        CaptureMode::BlockCut,
        VolumeBoundaryId::from_bytes(id(73)),
        73,
    )
    .unwrap();
    let env = TestEnv::with_initial_seed(initial);
    let grant = env.writer_and_attachment();
    let capture_id = op(30);
    env.start_sealed_capture(capture_id, protection(30), grant, initial.roots);
    env.failures
        .arm(FailurePoint::AuthorityAfterCommitBeforeReply);
    assert!(matches!(
        env.control.publish_capture(
            capture_id,
            1,
            generation(1),
            grant,
            attachment(1),
            1,
            child.clone(),
            true
        ),
        Err(ControlError::Metadata(MetadataError::UnknownOutcome { .. }))
    ));

    env.clock.set(grant.expires_at.as_nanos());
    let published = env
        .control
        .publish_capture(
            capture_id,
            1,
            generation(1),
            grant,
            attachment(1),
            1,
            child,
            true,
        )
        .expect("reconcile a committed capture after lease expiry");
    assert_eq!(published.branch.head, generation(2));
}

#[test]
fn live_root_commit_and_child_renewal_recheck_parent_capture_deadlines() {
    for (parent_capture, renew_child) in [(false, false), (true, false), (true, true)] {
        let env = TestEnv::new();
        let mut grant = env.writer_and_attachment();
        let capture_id = op(30);
        if parent_capture {
            env.control
                .begin_preparation(PreparationRequest {
                    operation_id: capture_id,
                    branch_id: branch(1),
                    expected_revision: 1,
                    grant,
                    attachment_id: attachment(1),
                    volume_epoch: 1,
                    protection_id: protection(30),
                    kind: PreparationKind::CheckpointCapture,
                    expected_roots: None,
                    dependencies: vec![],
                })
                .unwrap();
            grant = maintain_leases(&env, grant, 29, true);
            env.control
                .authorize_capture_child(op(31), capture_id, op(32), grant)
                .unwrap();
        }
        let barrier_id = if parent_capture { op(32) } else { op(5) };
        let next_roots = replace_volume_root(roots(1), 9);
        env.control
            .begin_preparation(PreparationRequest {
                operation_id: barrier_id,
                branch_id: branch(1),
                expected_revision: 1,
                grant,
                attachment_id: attachment(1),
                volume_epoch: 1,
                protection_id: protection(32),
                kind: PreparationKind::VolumeBarrier,
                expected_roots: Some(next_roots),
                dependencies: vec![],
            })
            .unwrap();
        let before = env.control.get_branch(branch(1)).unwrap();
        let before_operation = env.control.get_operation(barrier_id).unwrap();
        let deadline = if parent_capture {
            match env.control.get_operation(capture_id).unwrap() {
                OperationState::Preparing { expires_at, .. } => expires_at,
                other => panic!("unexpected capture state: {other:?}"),
            }
        } else {
            grant.expires_at
        };
        let (authority, control) = advancing_control(&env);
        authority
            .advance_to
            .store(deadline.as_nanos(), Ordering::SeqCst);
        let child = parent_capture.then_some((capture_id, barrier_id));
        let result = if renew_child {
            control
                .renew_preparation(op(33), barrier_id, grant)
                .map(|_| ())
        } else {
            control
                .commit_live_roots(barrier_id, 1, grant, next_roots, child)
                .map(|_| ())
        };
        assert_eq!(result, Err(ControlError::Expired));
        assert_eq!(env.control.get_branch(branch(1)).unwrap(), before);
        assert_eq!(env.control.get_operation(barrier_id), Ok(before_operation));
    }
}

#[test]
fn lease_dependent_control_operations_recheck_after_buffering_outcomes() {
    for action in [
        "renew_writer",
        "begin",
        "renew_preparation",
        "authorize",
        "seal",
    ] {
        let env = TestEnv::new();
        let grant = env.writer_and_attachment();
        let capture_id = op(30);
        let request = |operation_id| PreparationRequest {
            operation_id,
            branch_id: branch(1),
            expected_revision: 1,
            grant,
            attachment_id: attachment(1),
            volume_epoch: 1,
            protection_id: protection(30),
            kind: PreparationKind::CheckpointCapture,
            expected_roots: None,
            dependencies: vec![],
        };
        let existing_capture = !matches!(action, "renew_writer" | "begin");
        if existing_capture {
            env.control.begin_preparation(request(capture_id)).unwrap();
        }
        let before = env.control.get_branch(branch(1)).unwrap();
        let previous_capture =
            existing_capture.then(|| env.control.get_operation(capture_id).unwrap());
        let (authority, control) = advancing_control(&env);
        authority
            .advance_to
            .store(grant.expires_at.as_nanos(), Ordering::SeqCst);
        let operation_id = op(40);
        let result = match action {
            "renew_writer" => control.renew_writer(operation_id, grant).map(|_| ()),
            "begin" => control.begin_preparation(request(operation_id)).map(|_| ()),
            "renew_preparation" => control
                .renew_preparation(operation_id, capture_id, grant)
                .map(|_| ()),
            "authorize" => control.authorize_capture_child(operation_id, capture_id, op(41), grant),
            "seal" => control
                .seal_capture(operation_id, capture_id, 1, grant)
                .map(|_| ()),
            _ => unreachable!(),
        };
        assert_eq!(result, Err(ControlError::Expired), "{action}");
        assert_eq!(env.control.get_branch(branch(1)).unwrap(), before);
        assert_eq!(
            env.control.get_operation(operation_id),
            Err(ControlError::NotFound)
        );
        if let Some(previous) = previous_capture {
            assert_eq!(env.control.get_operation(capture_id), Ok(previous));
        }
    }
}

#[test]
fn reader_protection_does_not_open_or_renew_after_its_deadline() {
    let env = TestEnv::new();
    let (authority, control) = advancing_control(&env);
    authority.advance_to.store(
        versionfs_core::control::READER_PROTECTION_NANOS,
        Ordering::SeqCst,
    );
    assert_eq!(
        control.open_generation(op(50), generation(1), protection(50)),
        Err(ControlError::Expired)
    );
    let deadline = env
        .control
        .open_generation(op(51), generation(1), protection(50))
        .expect("failed open left no protection claim");
    authority
        .advance_to
        .store(deadline.as_nanos(), Ordering::SeqCst);
    assert_eq!(
        control.renew_protection(op(52), protection(50)),
        Err(ControlError::Expired)
    );
    assert_eq!(
        env.control.get_operation(op(52)),
        Err(ControlError::NotFound)
    );
}

#[test]
fn capture_publication_rejects_a_verified_graph_from_another_object_namespace() {
    let mut target_store = TestObjectStore::default();
    let initial = empty_generation(&mut target_store, generation(1), 60);
    let env = TestEnv::with_initial_seed(initial);
    let grant = env.writer_and_attachment();
    let capture_id = op(61);
    env.start_sealed_capture(capture_id, protection(61), grant, initial.roots);

    let mut foreign_store = TestObjectStore::with_scope(ObjectStoreScopeId::from_bytes([0xD5; 16]));
    let foreign_initial = empty_generation(&mut foreign_store, generation(1), 60);
    assert_eq!(foreign_initial.roots, initial.roots);
    let foreign_generation = create_generation(
        &mut foreign_store,
        generation(2),
        Some(generation(1)),
        foreign_initial.roots,
        CaptureMode::BlockCut,
        VolumeBoundaryId::from_bytes(id(62)),
        62,
    )
    .expect("verify child Generation in foreign object namespace");

    assert_eq!(
        env.control.publish_capture(
            capture_id,
            1,
            generation(1),
            grant,
            attachment(1),
            1,
            foreign_generation,
            true,
        ),
        Err(ControlError::InvalidTransition)
    );
    assert!(env.control.get_branch(branch(1)).unwrap().capture.is_some());
    assert!(matches!(
        env.control.get_operation(capture_id),
        Ok(OperationState::Preparing { operation_id, .. }) if operation_id == capture_id
    ));
}

#[test]
fn detached_capture_publication_retains_generation_without_rebasing_branch() {
    let mut object_store = TestObjectStore::default();
    let initial = empty_generation(&mut object_store, generation(1), 40);
    let new_generation = create_generation(
        &mut object_store,
        generation(3),
        Some(initial.generation_id),
        initial.roots,
        CaptureMode::FilesystemFrozen,
        VolumeBoundaryId::from_bytes(id(41)),
        41,
    )
    .expect("create verified detached Generation");
    let env = TestEnv::with_initial_seed(initial);
    let grant = env.writer_and_attachment();
    let capture_id = op(40);
    env.start_sealed_capture(capture_id, protection(40), grant, initial.roots);

    let published = env
        .control
        .publish_capture(
            capture_id,
            1,
            generation(1),
            grant,
            attachment(1),
            1,
            new_generation,
            false,
        )
        .expect("publish detached Generation");
    assert!(!published.head_advanced);
    assert_eq!(published.branch.head, generation(1));
    assert_eq!(published.branch.revision, 1);
    assert!(published.branch.capture.is_none());
    assert_eq!(
        published
            .branch
            .attachment
            .expect("attachment remains on original base")
            .base_generation,
        generation(1)
    );

    env.control
        .open_generation(op(41), generation(3), protection(41))
        .expect("detached Generation remains retained and readable");
    assert_eq!(
        env.control
            .mark_generation_unretained(op(42), generation(3)),
        Err(ControlError::Busy)
    );
    env.control
        .close_protection(op(43), protection(41))
        .expect("release detached Generation reader");
    env.control
        .mark_generation_unretained(op(44), generation(3))
        .expect("allow retention release after reader closes");
    assert_eq!(
        env.control
            .open_generation(op(45), generation(3), protection(45)),
        Err(ControlError::GenerationExpired)
    );
}

#[test]
fn prepared_live_root_commit_is_atomic_replayable_and_keeps_attachment_base_protected() {
    let env = TestEnv::new();
    let grant = env.writer_and_attachment();
    assert_eq!(
        env.control
            .acquire_writer(op(2), branch(1), 1, 0)
            .expect("replay writer grant"),
        grant
    );
    assert_eq!(
        env.control.acquire_writer(op(4), branch(1), 1, 1),
        Err(ControlError::Busy)
    );

    let next_roots = replace_volume_root(roots(1), 9);
    let preparation = env
        .control
        .begin_preparation(PreparationRequest {
            operation_id: op(5),
            branch_id: branch(1),
            expected_revision: 1,
            grant,
            attachment_id: attachment(1),
            volume_epoch: 1,
            protection_id: protection(2),
            kind: PreparationKind::VolumeBarrier,
            expected_roots: Some(next_roots),
            dependencies: vec![],
        })
        .expect("register protected volume barrier");
    assert!(matches!(preparation, OperationState::Preparing { .. }));
    assert_eq!(
        env.control
            .begin_preparation(PreparationRequest {
                operation_id: op(5),
                branch_id: branch(1),
                expected_revision: 1,
                grant,
                attachment_id: attachment(1),
                volume_epoch: 1,
                protection_id: protection(2),
                kind: PreparationKind::VolumeBarrier,
                expected_roots: Some(next_roots),
                dependencies: vec![],
            })
            .expect("replay preparation"),
        preparation
    );
    assert_eq!(
        env.control.begin_preparation(PreparationRequest {
            operation_id: op(5),
            branch_id: branch(1),
            expected_revision: 1,
            grant,
            attachment_id: attachment(1),
            volume_epoch: 1,
            protection_id: protection(2),
            kind: PreparationKind::VolumeBarrier,
            expected_roots: Some(roots(10)),
            dependencies: vec![],
        }),
        Err(ControlError::OperationMismatch)
    );

    let committed = env
        .control
        .commit_live_roots(op(5), 1, grant, next_roots, None)
        .expect("publish durable branch roots");
    assert_eq!(committed.revision, 2);
    assert_eq!(committed.volume_root, next_roots.volume);
    assert_eq!(
        env.control
            .open_generation(op(18), generation(1), protection(2)),
        Err(ControlError::Conflict)
    );
    assert_eq!(
        env.control
            .commit_live_roots(op(5), 1, grant, next_roots, None)
            .expect("replay live-root commit"),
        committed
    );
    assert_eq!(
        env.control
            .commit_live_roots(op(5), 1, grant, roots(10), None),
        Err(ControlError::OperationMismatch)
    );

    env.clock.set(1);
    let renewed = env
        .control
        .renew_attachment_base(op(6), branch(1), attachment(1), 1, protection(1), grant)
        .expect("renew original Generation protection after live-root advance");
    assert_eq!(renewed.base_generation, generation(1));
    assert!(renewed.protection_expires_at > committed.attachment.unwrap().protection_expires_at);

    let log_deadline = env
        .control
        .open_live_log(
            op(8),
            branch(1),
            committed.revision,
            next_roots.logs,
            protection(6),
        )
        .expect("open protected live log root");
    assert!(log_deadline > env.clock.now());
    assert_eq!(
        env.control.open_live_log(
            op(9),
            branch(1),
            committed.revision,
            next_roots.logs,
            protection(6)
        ),
        Err(ControlError::Conflict)
    );
    env.control
        .close_protection(op(10), protection(6))
        .expect("close live-log protection");

    env.control
        .detach_volume(op(11), branch(1), attachment(1), 1, protection(1), grant)
        .expect("detach volume");
    assert!(
        env.control
            .get_branch(branch(1))
            .unwrap()
            .attachment
            .is_none()
    );

    let reattached = env
        .control
        .attach_volume(
            op(12),
            branch(1),
            committed.revision,
            grant,
            attachment(1),
            protection(12),
        )
        .expect("reattach from the branch's durable live root");
    env.clock.set(2);
    let renewed_reattachment = env
        .control
        .renew_attachment_base(
            op(13),
            branch(1),
            attachment(1),
            reattached.volume_epoch,
            reattached.protection_id,
            grant,
        )
        .expect("renew protection for a base whose live roots advanced");
    assert_eq!(reattached.base_generation, generation(1));
    assert_eq!(renewed_reattachment.base_generation, generation(1));
    assert!(renewed_reattachment.protection_expires_at > reattached.protection_expires_at);
    assert_eq!(reattached.volume_epoch, 2);
    assert_eq!(
        env.control
            .detach_volume(op(16), branch(1), attachment(1), 1, protection(1), grant),
        Err(ControlError::Fenced)
    );
    assert_eq!(
        env.control.renew_attachment_base(
            op(17),
            branch(1),
            attachment(1),
            1,
            protection(1),
            grant
        ),
        Err(ControlError::Fenced)
    );
    assert_eq!(
        env.control.get_branch(branch(1)).unwrap().attachment,
        Some(renewed_reattachment)
    );
    assert_eq!(
        env.control.begin_preparation(PreparationRequest {
            operation_id: op(14),
            branch_id: branch(1),
            expected_revision: committed.revision,
            grant,
            attachment_id: attachment(1),
            volume_epoch: 1,
            protection_id: protection(14),
            kind: PreparationKind::VolumeBarrier,
            expected_roots: Some(roots(14)),
            dependencies: vec![],
        }),
        Err(ControlError::Fenced)
    );
    env.control
        .begin_preparation(PreparationRequest {
            operation_id: op(15),
            branch_id: branch(1),
            expected_revision: committed.revision,
            grant,
            attachment_id: attachment(1),
            volume_epoch: reattached.volume_epoch,
            protection_id: protection(15),
            kind: PreparationKind::VolumeBarrier,
            expected_roots: Some(replace_volume_root(next_roots, 15)),
            dependencies: vec![],
        })
        .expect("accept the current attachment epoch");
    env.control
        .abort_operation(op(15))
        .expect("close current-epoch preparation");
}

#[test]
fn capture_admits_only_identified_children_and_abort_releases_the_token() {
    let env = TestEnv::new();
    let grant = env.writer_and_attachment();
    let capture_request = || {
        env.control.begin_preparation(PreparationRequest {
            operation_id: op(10),
            branch_id: branch(1),
            expected_revision: 1,
            grant,
            attachment_id: attachment(1),
            volume_epoch: 1,
            protection_id: protection(3),
            kind: PreparationKind::CheckpointCapture,
            expected_roots: None,
            dependencies: vec![],
        })
    };
    let capture_state = capture_request().expect("begin checkpoint capture");
    assert!(matches!(capture_state, OperationState::Preparing { .. }));
    VolumeBarrierControl::authorize_capture_child(&env.control, op(11), op(10), op(12), grant)
        .expect("admit identified drain child");
    assert_eq!(
        env.control.begin_preparation(PreparationRequest {
            operation_id: op(13),
            branch_id: branch(1),
            expected_revision: 1,
            grant,
            attachment_id: attachment(1),
            volume_epoch: 1,
            protection_id: protection(4),
            kind: PreparationKind::VolumeBarrier,
            expected_roots: Some(roots(11)),
            dependencies: vec![],
        }),
        Err(ControlError::Busy)
    );
    VolumeBarrierControl::begin_preparation(
        &env.control,
        PreparationRequest {
            operation_id: op(12),
            branch_id: branch(1),
            expected_revision: 1,
            grant,
            attachment_id: attachment(1),
            volume_epoch: 1,
            protection_id: protection(5),
            kind: PreparationKind::VolumeBarrier,
            expected_roots: Some(replace_volume_root(roots(1), 12)),
            dependencies: vec![],
        },
    )
    .expect("register admitted child preparation");
    let after_child = VolumeBarrierControl::commit_live_roots(
        &env.control,
        op(12),
        1,
        grant,
        replace_volume_root(roots(1), 12),
        Some(op(10)),
    )
    .expect("commit admitted drain child");
    assert_eq!(after_child.revision, 2);
    let sealed = env
        .control
        .seal_capture(op(14), op(10), 2, grant)
        .expect("seal exact final capture boundary");
    assert!(sealed.sealed);
    assert_eq!(sealed.sealed_revision, Some(2));
    assert_eq!(sealed.sealed_roots, Some(replace_volume_root(roots(1), 12)));

    assert_eq!(
        env.control
            .abort_operation(op(10))
            .expect("abort sealed capture"),
        OperationState::Aborted
    );
    assert!(env.control.get_branch(branch(1)).unwrap().capture.is_none());
    assert_eq!(capture_request(), Ok(OperationState::Aborted));
    assert_eq!(
        env.control.get_operation(op(10)).unwrap(),
        OperationState::Aborted
    );

    assert_eq!(
        env.control
            .abort_operation(op(30))
            .expect("tombstone unseen request"),
        OperationState::Aborted
    );
    assert_eq!(
        env.control
            .create_branch(op(30), branch(30), "delayed", generation(1)),
        Err(ControlError::Aborted)
    );
    assert_eq!(
        env.control.get_operation(op(30)).unwrap(),
        OperationState::Aborted
    );
}

#[test]
fn capture_waits_for_mutation_preparations_and_renewed_writer_can_finish_one() {
    let env = TestEnv::new();
    let grant = env.writer_and_attachment();
    let roots = replace_volume_root(roots(1), 60);
    env.control
        .begin_preparation(PreparationRequest {
            operation_id: op(60),
            branch_id: branch(1),
            expected_revision: 1,
            grant,
            attachment_id: attachment(1),
            volume_epoch: 1,
            protection_id: protection(60),
            kind: PreparationKind::VolumeBarrier,
            expected_roots: Some(roots),
            dependencies: vec![],
        })
        .expect("begin mutation preparation");

    assert_eq!(
        env.control.begin_preparation(PreparationRequest {
            operation_id: op(61),
            branch_id: branch(1),
            expected_revision: 1,
            grant,
            attachment_id: attachment(1),
            volume_epoch: 1,
            protection_id: protection(61),
            kind: PreparationKind::CheckpointCapture,
            expected_roots: None,
            dependencies: vec![],
        }),
        Err(ControlError::Busy)
    );
    assert_eq!(
        env.control
            .detach_volume(op(62), branch(1), attachment(1), 1, protection(1), grant),
        Err(ControlError::Busy)
    );

    env.clock.set(1);
    let renewed_grant = env
        .control
        .renew_writer(op(63), grant)
        .expect("renew the active writer lease");
    let committed = env
        .control
        .commit_live_roots(op(60), 1, renewed_grant, roots, None)
        .expect("finish preparation under the renewed grant");
    assert_eq!(committed.revision, 2);
    assert_eq!(committed.pending_mutation_preparations, 0);

    env.control
        .begin_preparation(PreparationRequest {
            operation_id: op(64),
            branch_id: branch(1),
            expected_revision: 2,
            grant: renewed_grant,
            attachment_id: attachment(1),
            volume_epoch: 1,
            protection_id: protection(64),
            kind: PreparationKind::CheckpointCapture,
            expected_roots: None,
            dependencies: vec![],
        })
        .expect("capture admission opens after mutation completion");
    env.control
        .abort_operation(op(64))
        .expect("release capture token");
}

#[test]
fn capture_child_cannot_be_used_for_log_mutation() {
    let env = TestEnv::new();
    let grant = env.writer_and_attachment();
    env.control
        .begin_preparation(PreparationRequest {
            operation_id: op(70),
            branch_id: branch(1),
            expected_revision: 1,
            grant,
            attachment_id: attachment(1),
            volume_epoch: 1,
            protection_id: protection(70),
            kind: PreparationKind::CheckpointCapture,
            expected_roots: None,
            dependencies: vec![],
        })
        .expect("begin capture");
    env.control
        .authorize_capture_child(op(71), op(70), op(72), grant)
        .expect("authorize drain child");
    assert_eq!(
        env.control.begin_preparation(PreparationRequest {
            operation_id: op(72),
            branch_id: branch(1),
            expected_revision: 1,
            grant,
            attachment_id: attachment(1),
            volume_epoch: 1,
            protection_id: protection(72),
            kind: PreparationKind::LogMutation,
            expected_roots: Some(roots(72)),
            dependencies: vec![],
        }),
        Err(ControlError::Busy)
    );
    env.control
        .abort_operation(op(70))
        .expect("abort capture and retire unused child");
    assert_eq!(
        env.control.get_operation(op(72)),
        Ok(OperationState::Aborted)
    );
}

#[test]
fn expired_attachment_protection_rejects_preparation_even_with_renewed_writer() {
    let env = TestEnv::new();
    let mut grant = env.writer_and_attachment();
    for index in 0..10_u8 {
        let now = u64::from(index + 1) * 29_000_000_000;
        env.clock.set(now);
        grant = env
            .control
            .renew_writer(op(80 + index), grant)
            .expect("keep writer lease current");
    }
    env.clock.set(300_000_000_000);
    assert_eq!(
        env.control.begin_preparation(PreparationRequest {
            operation_id: op(90),
            branch_id: branch(1),
            expected_revision: 1,
            grant,
            attachment_id: attachment(1),
            volume_epoch: 1,
            protection_id: protection(90),
            kind: PreparationKind::VolumeBarrier,
            expected_roots: Some(roots(90)),
            dependencies: vec![],
        }),
        Err(ControlError::Expired)
    );
}

#[test]
fn abandoned_capture_child_authorization_is_reclaimed_by_abort_or_expiry() {
    let env = TestEnv::new();
    let grant = env.writer_and_attachment();
    let begin_capture = |operation_id, protection_id| {
        env.control.begin_preparation(PreparationRequest {
            operation_id,
            branch_id: branch(1),
            expected_revision: 1,
            grant,
            attachment_id: attachment(1),
            volume_epoch: 1,
            protection_id,
            kind: PreparationKind::CheckpointCapture,
            expected_roots: None,
            dependencies: vec![],
        })
    };
    let begin_child = |operation_id, protection_id| {
        env.control.begin_preparation(PreparationRequest {
            operation_id,
            branch_id: branch(1),
            expected_revision: 1,
            grant,
            attachment_id: attachment(1),
            volume_epoch: 1,
            protection_id,
            kind: PreparationKind::VolumeBarrier,
            expected_roots: Some(roots(20)),
            dependencies: vec![],
        })
    };

    begin_capture(op(40), protection(40)).expect("begin first capture");
    env.control
        .authorize_capture_child(op(41), op(40), op(42), grant)
        .expect("authorize child before worker starts it");
    assert_eq!(
        env.control.renew_writer(op(42), grant),
        Err(ControlError::OperationMismatch)
    );
    assert_eq!(
        env.control.get_operation(op(42)),
        Err(ControlError::NotFound)
    );
    assert_eq!(
        env.control
            .abort_operation(op(42))
            .expect("abort unseen child and release its reservation"),
        OperationState::Aborted
    );
    assert_eq!(
        env.control
            .get_branch(branch(1))
            .unwrap()
            .capture
            .as_ref()
            .and_then(|capture| capture.pending_child),
        None
    );
    assert_eq!(
        env.control.get_operation(op(42)).unwrap(),
        OperationState::Aborted
    );

    env.control
        .authorize_capture_child(op(43), op(40), op(44), grant)
        .expect("authorize another child after releasing the first reservation");
    assert_eq!(
        env.control
            .abort_operation(op(40))
            .expect("abort capture and retire its unseen child"),
        OperationState::Aborted
    );
    assert!(env.control.get_branch(branch(1)).unwrap().capture.is_none());
    assert_eq!(
        env.control.get_operation(op(44)).unwrap(),
        OperationState::Aborted
    );
    assert_eq!(
        begin_child(op(44), protection(44)),
        Err(ControlError::Aborted)
    );

    let OperationState::Preparing {
        expires_at: deadline,
        ..
    } = begin_capture(op(50), protection(50)).expect("begin second capture")
    else {
        panic!("capture should remain in preparation");
    };
    env.control
        .authorize_capture_child(op(51), op(50), op(52), grant)
        .expect("authorize child before worker starts it");
    env.clock.set(deadline.as_nanos());
    assert_eq!(
        env.control
            .expire_preparation(op(50))
            .expect("expire capture and retire absent child"),
        OperationState::Aborted
    );
    assert!(env.control.get_branch(branch(1)).unwrap().capture.is_none());
    assert_eq!(
        env.control.get_operation(op(52)).unwrap(),
        OperationState::Aborted
    );
    assert_eq!(
        begin_child(op(52), protection(52)),
        Err(ControlError::Aborted)
    );
}

#[test]
fn owner_restart_fences_old_grants_and_expired_read_protections_release_retention() {
    let env = TestEnv::new();
    let old_grant = env.writer_and_attachment();
    let next_owner = start_authority_owner(env.authority.as_ref(), op(251)).expect("restart owner");
    assert!(next_owner > env.owner);
    assert_eq!(
        env.control.renew_writer(op(4), old_grant),
        Err(ControlError::Fenced)
    );

    let next_control = env.control_for_owner(next_owner);
    assert_eq!(
        next_control.renew_writer(op(252), old_grant),
        Err(ControlError::Fenced)
    );
    let new_grant = next_control
        .acquire_writer(op(5), branch(1), 1, old_grant.writer_epoch)
        .expect("new owner takes fenced branch");
    assert!(new_grant.writer_epoch > old_grant.writer_epoch);
    assert_eq!(
        next_control.open_generation(op(30), generation(1), protection(1)),
        Err(ControlError::Conflict)
    );
    assert!(
        next_control
            .get_branch(branch(1))
            .unwrap()
            .attachment
            .is_none()
    );

    next_control
        .register_generation(op(6), seed(2))
        .expect("register separately retained Generation");
    let first_deadline = next_control
        .open_generation(op(7), generation(2), protection(7))
        .expect("open protected Generation");
    assert_eq!(
        next_control.mark_generation_unretained(op(8), generation(2)),
        Err(ControlError::Busy)
    );
    env.clock.set(1);
    let renewed_deadline = next_control
        .renew_protection(op(9), protection(7))
        .expect("renew reader protection");
    assert!(renewed_deadline > first_deadline);
    assert_eq!(
        next_control
            .renew_protection(op(9), protection(7))
            .expect("replay protection renewal"),
        renewed_deadline
    );
    next_control
        .close_protection(op(10), protection(7))
        .expect("close reader protection");
    next_control
        .mark_generation_unretained(op(11), generation(2))
        .expect("release retention after final reader closes");
    assert_eq!(
        next_control.open_generation(op(12), generation(2), protection(8)),
        Err(ControlError::GenerationExpired)
    );

    next_control
        .register_generation(op(16), seed(3))
        .expect("register Generation for expiry test");
    let expiry_deadline = next_control
        .open_generation(op(17), generation(3), protection(9))
        .expect("open Generation with expiring protection");
    env.clock.set(expiry_deadline.as_nanos());
    assert_eq!(
        next_control.renew_protection(op(18), protection(9)),
        Err(ControlError::Expired)
    );
    next_control
        .expire_protection(op(19), protection(9))
        .expect("expire reader protection and release its retention count");
    next_control
        .mark_generation_unretained(op(20), generation(3))
        .expect("mark Generation unretained after expiry cleanup");

    next_control
        .pin_generation(op(13), generation(1), pin(1))
        .expect("pin retained Generation");
    next_control
        .unpin_generation(op(14), pin(1))
        .expect("release Generation pin");
    assert_eq!(
        next_control.pin_generation(op(15), generation(1), pin(1)),
        Err(ControlError::Conflict)
    );
}

#[test]
fn preparation_renewal_replays_and_expiry_clears_capture_admission() {
    let env = TestEnv::new();
    let grant = env.writer_and_attachment();
    let initial = env
        .control
        .begin_preparation(PreparationRequest {
            operation_id: op(10),
            branch_id: branch(1),
            expected_revision: 1,
            grant,
            attachment_id: attachment(1),
            volume_epoch: 1,
            protection_id: protection(10),
            kind: PreparationKind::CheckpointCapture,
            expected_roots: None,
            dependencies: vec![],
        })
        .expect("begin capture preparation");
    let OperationState::Preparing {
        expires_at: initial_deadline,
        ..
    } = initial
    else {
        panic!("capture should remain in preparation");
    };

    env.clock.set(1);
    let renewed_deadline = env
        .control
        .renew_preparation(op(11), op(10), grant)
        .expect("renew protected capture");
    assert!(renewed_deadline > initial_deadline);
    assert_eq!(
        env.control
            .renew_preparation(op(11), op(10), grant)
            .expect("replay preparation renewal"),
        renewed_deadline
    );
    assert!(matches!(
        env.control.expire_preparation(op(10)),
        Err(ControlError::Busy)
    ));

    env.clock.set(renewed_deadline.as_nanos());
    assert_eq!(
        env.control
            .expire_preparation(op(10))
            .expect("expire abandoned capture"),
        OperationState::Aborted
    );
    assert!(env.control.get_branch(branch(1)).unwrap().capture.is_none());
    assert_eq!(
        env.control.get_operation(op(10)).unwrap(),
        OperationState::Aborted
    );
    let new_grant = env
        .control
        .acquire_writer(op(12), branch(1), 1, grant.writer_epoch)
        .expect("acquire after expired capture cleanup");
    assert!(new_grant.writer_epoch > grant.writer_epoch);
}
