//! Core control-state-machine regression tests with an in-memory authority.

use std::{
    collections::{BTreeMap, HashMap},
    num::NonZeroUsize,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use versionfs_core::{
    control::{
        ControlError, LiveRoots, OperationState, PreparationKind, PreparationRequest,
        VerifiedGenerationSeed, WorkspaceControl, start_authority_owner,
    },
    format::{
        CaptureMode, GenerationDescriptorV2, ObjectRef, ObjectType, ProvenanceBinding,
        VolumeGeometry, encode_object,
    },
    graph::verify_generation,
    identity::{
        AttachmentId, BranchId, GenerationId, ObjectId, ObjectStoreScopeId, OperationId,
        PrincipalId, ProtectionId, VolumeBoundaryId, WorkspaceId,
    },
    metadata::{
        MetadataAuthority, MetadataCursor, MetadataError, MetadataPage, MetadataSnapshot,
        MetadataTxn, MutationIdentity, ReconciledOutcome,
    },
    persistent_map::MapObjectStore,
    time::MonotonicTime,
    views::{ArtifactMap, LogMap, VolumeExtentMap},
};

#[derive(Default)]
struct MemoryAuthority {
    values: Mutex<BTreeMap<Vec<u8>, Vec<u8>>>,
    now_nanos: AtomicU64,
    next_snapshot_scope: AtomicU64,
}

struct MemorySnapshot {
    scope: u64,
    values: BTreeMap<Vec<u8>, Vec<u8>>,
}

#[derive(Default)]
struct MemoryObjectStore(HashMap<ObjectId, Vec<u8>>);

impl MapObjectStore for MemoryObjectStore {
    fn proof_scope(&self) -> ObjectStoreScopeId {
        ObjectStoreScopeId::from_bytes([0xA5; 16])
    }

    fn get(&self, id: ObjectId) -> Result<Option<Vec<u8>>, String> {
        Ok(self.0.get(&id).cloned())
    }

    fn put(&mut self, id: ObjectId, bytes: Vec<u8>) -> Result<(), String> {
        self.0.insert(id, bytes);
        Ok(())
    }
}

impl MetadataSnapshot for MemorySnapshot {
    fn cursor_scope(&self) -> u64 {
        self.scope
    }
}

struct MemoryTxn<'a> {
    values: &'a BTreeMap<Vec<u8>, Vec<u8>>,
    writes: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    now_nanos: &'a AtomicU64,
}

impl MetadataTxn for MemoryTxn<'_> {
    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, MetadataError> {
        Ok(match self.writes.get(key) {
            Some(value) => value.clone(),
            None => self.values.get(key).cloned(),
        })
    }

    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), MetadataError> {
        self.writes.insert(key.to_vec(), Some(value.to_vec()));
        Ok(())
    }

    fn delete(&mut self, key: &[u8]) -> Result<(), MetadataError> {
        self.writes.insert(key.to_vec(), None);
        Ok(())
    }

    fn now(&self) -> MonotonicTime {
        MonotonicTime::from_nanos(self.now_nanos.load(Ordering::SeqCst))
    }
}

impl MetadataAuthority for MemoryAuthority {
    type Snapshot = MemorySnapshot;

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, MetadataError> {
        Ok(self
            .values
            .lock()
            .expect("authority lock")
            .get(key)
            .cloned())
    }

    fn transact<R, F>(
        &self,
        _operation: MutationIdentity<'_>,
        mut body: F,
    ) -> Result<R, MetadataError>
    where
        F: FnMut(&mut dyn MetadataTxn) -> Result<R, MetadataError>,
    {
        let mut values = self.values.lock().expect("authority lock");
        let (result, writes) = {
            let mut txn = MemoryTxn {
                values: &values,
                writes: BTreeMap::new(),
                now_nanos: &self.now_nanos,
            };
            let result = body(&mut txn)?;
            (result, txn.writes)
        };
        for (key, value) in writes {
            match value {
                Some(value) => {
                    values.insert(key, value);
                }
                None => {
                    values.remove(&key);
                }
            }
        }
        Ok(result)
    }

    fn reconcile_unknown(
        &self,
        _operation: MutationIdentity<'_>,
    ) -> Result<ReconciledOutcome, MetadataError> {
        Ok(ReconciledOutcome::NotCommitted)
    }

    fn snapshot(&self) -> Result<Self::Snapshot, MetadataError> {
        let scope = self.next_snapshot_scope.fetch_add(1, Ordering::Relaxed) + 1;
        Ok(MemorySnapshot {
            scope,
            values: self.values.lock().expect("authority lock").clone(),
        })
    }

    fn scan_prefix(
        &self,
        snapshot: &Self::Snapshot,
        prefix: &[u8],
        cursor: Option<&MetadataCursor>,
        limit: NonZeroUsize,
    ) -> Result<MetadataPage, MetadataError> {
        if let Some(cursor) = cursor
            && (cursor.snapshot_scope() != snapshot.scope || cursor.prefix() != prefix)
        {
            return Err(MetadataError::InvalidRequest(
                "cursor does not match snapshot and prefix",
            ));
        }
        let after = cursor.map(MetadataCursor::last_key);
        let mut entries = snapshot
            .values
            .iter()
            .filter(|(key, _)| {
                key.starts_with(prefix) && after.is_none_or(|last| key.as_slice() > last)
            })
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<Vec<_>>();
        let has_more = entries.len() > limit.get();
        entries.truncate(limit.get());
        let next_cursor = if has_more {
            entries
                .last()
                .map(|(key, _)| MetadataCursor::new(snapshot, prefix, key.clone()))
        } else {
            None
        };
        Ok(MetadataPage {
            entries,
            next_cursor,
        })
    }
}

fn control() -> WorkspaceControl<MemoryAuthority> {
    let authority = Arc::new(MemoryAuthority::default());
    let owner = start_authority_owner(authority.as_ref(), op(250)).expect("start owner");
    let control = WorkspaceControl::new(
        authority,
        workspace(1),
        ObjectStoreScopeId::from_bytes([0xA5; 16]),
        principal(1),
        owner,
    );
    control
        .create_workspace(op(1), branch(1), "main", 60_000, verified_seed(1))
        .expect("create workspace");
    control
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

fn object(kind: ObjectType, marker: u8) -> ObjectRef {
    let bytes = encode_object(kind, &[marker]).expect("encode typed object");
    ObjectRef::from_encoded(&bytes).expect("decode typed object reference")
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

fn replace_log_root(current: LiveRoots, marker: u8) -> LiveRoots {
    LiveRoots {
        volume: current.volume,
        logs: object(ObjectType::LogMapRoot, marker),
        artifacts: current.artifacts,
    }
}

fn replace_artifact_root(current: LiveRoots, marker: u8) -> LiveRoots {
    LiveRoots {
        volume: current.volume,
        logs: current.logs,
        artifacts: object(ObjectType::ArtifactMapRoot, marker),
    }
}

fn verified_seed(marker: u8) -> VerifiedGenerationSeed {
    let mut store = MemoryObjectStore::default();
    let geometry = VolumeGeometry::new(4 * 65_536).expect("valid test geometry");
    let volume_root = VolumeExtentMap::empty(geometry, &mut store)
        .expect("create empty volume map")
        .seal(&mut store)
        .expect("seal empty volume map");
    let logs_root = LogMap::empty(&mut store)
        .expect("create empty log map")
        .seal(&mut store)
        .expect("seal empty log map");
    let artifacts_root = ArtifactMap::empty(&mut store)
        .expect("create empty artifact map")
        .seal(&mut store)
        .expect("seal empty artifact map");
    let descriptor_bytes = GenerationDescriptorV2 {
        generation_id: generation(marker),
        parent_generation_id: None,
        volume_root,
        logs_root,
        artifacts_root,
        provenance: ProvenanceBinding::Unattributed,
        capture_mode: CaptureMode::BlockCut,
        volume_boundary_id: VolumeBoundaryId::from_bytes([marker; 16]),
        created_at_unix_ms: u64::from(marker),
    }
    .encode()
    .expect("encode Generation descriptor");
    let descriptor = ObjectRef::from_encoded(&descriptor_bytes).expect("reference descriptor");
    store
        .put(descriptor.object_id, descriptor_bytes)
        .expect("persist Generation descriptor");
    let proof = verify_generation(&store, descriptor).expect("verify Generation graph");
    VerifiedGenerationSeed::from_proof(proof)
}

#[test]
fn core_protocol_replays_operations_and_rejects_stale_attachment_epochs() {
    let control = control();
    let created = control
        .create_branch(op(10), branch(2), "agent", generation(1))
        .expect("create branch");
    assert_eq!(
        control
            .create_branch(op(10), branch(2), "agent", generation(1))
            .expect("replay branch creation"),
        created
    );
    assert_eq!(
        control.create_branch(op(10), branch(2), "renamed", generation(1)),
        Err(ControlError::OperationMismatch)
    );

    let writer = control
        .acquire_writer(op(20), branch(1), 1, 0)
        .expect("acquire writer");
    control
        .attach_volume(op(21), branch(1), 1, writer, attachment(1), protection(1))
        .expect("attach volume");
    control
        .detach_volume(op(22), branch(1), attachment(1), 1, protection(1), writer)
        .expect("detach volume");
    assert_eq!(
        control.attach_volume(op(31), branch(1), 1, writer, attachment(1), protection(1)),
        Err(ControlError::Conflict)
    );
    let current = control
        .attach_volume(op(23), branch(1), 1, writer, attachment(1), protection(2))
        .expect("reattach same device identity");
    assert_eq!(current.volume_epoch, 2);
    control
        .detach_volume(op(22), branch(1), attachment(1), 1, protection(1), writer)
        .expect("replay old detach without touching the replacement");
    assert_eq!(
        control.detach_volume(
            op(22),
            branch(1),
            attachment(1),
            current.volume_epoch,
            current.protection_id,
            writer,
        ),
        Err(ControlError::OperationMismatch)
    );
    for (marker, volume_epoch, protection_id) in [
        (25, 1, protection(1)),
        (27, current.volume_epoch, protection(1)),
        (29, 1, current.protection_id),
    ] {
        assert_eq!(
            control.renew_attachment_base(
                op(marker),
                branch(1),
                attachment(1),
                volume_epoch,
                protection_id,
                writer,
            ),
            Err(ControlError::Fenced)
        );
        assert_eq!(
            control.detach_volume(
                op(marker + 1),
                branch(1),
                attachment(1),
                volume_epoch,
                protection_id,
                writer,
            ),
            Err(ControlError::Fenced)
        );
    }
    assert_eq!(
        control
            .get_branch(branch(1))
            .expect("read current attachment")
            .attachment,
        Some(current)
    );

    let stale_request = PreparationRequest {
        operation_id: op(24),
        branch_id: branch(1),
        expected_revision: 1,
        grant: writer,
        attachment_id: attachment(1),
        volume_epoch: 1,
        protection_id: protection(3),
        kind: PreparationKind::VolumeBarrier,
        expected_roots: Some(roots(3)),
        dependencies: vec![],
    };
    assert_eq!(
        control.begin_preparation(stale_request),
        Err(ControlError::Fenced)
    );
}

#[test]
fn core_protocol_rejects_non_volume_capture_children() {
    let control = control();
    let writer = control
        .acquire_writer(op(30), branch(1), 1, 0)
        .expect("acquire writer");
    control
        .attach_volume(op(31), branch(1), 1, writer, attachment(1), protection(1))
        .expect("attach volume");
    control
        .begin_preparation(PreparationRequest {
            operation_id: op(32),
            branch_id: branch(1),
            expected_revision: 1,
            grant: writer,
            attachment_id: attachment(1),
            volume_epoch: 1,
            protection_id: protection(2),
            kind: PreparationKind::CheckpointCapture,
            expected_roots: None,
            dependencies: vec![],
        })
        .expect("begin capture");
    control
        .authorize_capture_child(op(33), op(32), op(34), writer)
        .expect("authorize drain child");

    assert_eq!(
        control.begin_preparation(PreparationRequest {
            operation_id: op(34),
            branch_id: branch(1),
            expected_revision: 1,
            grant: writer,
            attachment_id: attachment(1),
            volume_epoch: 1,
            protection_id: protection(3),
            kind: PreparationKind::LogMutation,
            expected_roots: Some(roots(4)),
            dependencies: vec![],
        }),
        Err(ControlError::Busy)
    );
    assert_eq!(control.abort_operation(op(34)), Ok(OperationState::Aborted));
    assert_eq!(
        control
            .get_branch(branch(1))
            .expect("read capture after child abort")
            .capture
            .expect("capture remains open")
            .pending_child,
        None
    );
    assert!(
        control
            .seal_capture(op(35), op(32), 1, writer)
            .expect("seal after aborting unstarted child")
            .sealed
    );
    control
        .abort_operation(op(32))
        .expect("abort capture and tombstone child");
    assert_eq!(control.get_operation(op(34)), Ok(OperationState::Aborted));
}

#[test]
fn core_protocol_limits_each_preparation_to_its_view_root() {
    let control = control();
    let writer = control
        .acquire_writer(op(40), branch(1), 1, 0)
        .expect("acquire writer");
    control
        .attach_volume(op(41), branch(1), 1, writer, attachment(1), protection(1))
        .expect("attach volume");
    let current = control.get_branch(branch(1)).expect("read initial branch");
    let initial = LiveRoots {
        volume: current.volume_root,
        logs: current.logs_root,
        artifacts: current.artifacts_root,
    };

    assert!(matches!(
        control.begin_preparation(PreparationRequest {
            operation_id: op(42),
            branch_id: branch(1),
            expected_revision: 1,
            grant: writer,
            attachment_id: attachment(1),
            volume_epoch: 1,
            protection_id: protection(2),
            kind: PreparationKind::VolumeBarrier,
            expected_roots: Some(roots(2)),
            dependencies: vec![],
        }),
        Err(ControlError::InvalidRequest(_))
    ));

    for (operation_id, protection_id, kind, next) in [
        (
            op(43),
            protection(3),
            PreparationKind::VolumeBarrier,
            replace_volume_root(initial, 3),
        ),
        (
            op(44),
            protection(4),
            PreparationKind::LogMutation,
            replace_log_root(initial, 4),
        ),
        (
            op(45),
            protection(5),
            PreparationKind::ArtifactMutation,
            replace_artifact_root(initial, 5),
        ),
    ] {
        control
            .begin_preparation(PreparationRequest {
                operation_id,
                branch_id: branch(1),
                expected_revision: 1,
                grant: writer,
                attachment_id: attachment(1),
                volume_epoch: 1,
                protection_id,
                kind,
                expected_roots: Some(next),
                dependencies: vec![],
            })
            .expect("accept preparation scoped to its view root");
        control
            .abort_operation(operation_id)
            .expect("abort root-scope preparation");
    }
    for (operation_id, protection_id) in [
        (op(46), protection(3)),
        (op(47), protection(4)),
        (op(48), protection(5)),
    ] {
        assert_eq!(
            control.open_generation(operation_id, generation(1), protection_id),
            Err(ControlError::Conflict)
        );
    }
}

#[test]
fn core_protocol_reserves_capture_child_ids_against_unrelated_operations() {
    let control = control();
    let writer = control
        .acquire_writer(op(50), branch(1), 1, 0)
        .expect("acquire capture writer");
    control
        .attach_volume(op(51), branch(1), 1, writer, attachment(1), protection(1))
        .expect("attach capture volume");
    control
        .create_branch(op(52), branch(2), "other", generation(1))
        .expect("create independent branch");
    let other_writer = control
        .acquire_writer(op(53), branch(2), 1, 0)
        .expect("acquire independent writer");
    control
        .attach_volume(
            op(54),
            branch(2),
            1,
            other_writer,
            attachment(2),
            protection(2),
        )
        .expect("attach independent volume");
    control
        .begin_preparation(PreparationRequest {
            operation_id: op(55),
            branch_id: branch(1),
            expected_revision: 1,
            grant: writer,
            attachment_id: attachment(1),
            volume_epoch: 1,
            protection_id: protection(3),
            kind: PreparationKind::CheckpointCapture,
            expected_roots: None,
            dependencies: vec![],
        })
        .expect("begin capture");
    control
        .authorize_capture_child(op(56), op(55), op(57), writer)
        .expect("reserve drain child ID");

    assert_eq!(
        control.renew_writer(op(57), writer),
        Err(ControlError::OperationMismatch)
    );
    for (kind, expected_roots) in [
        (
            PreparationKind::VolumeBarrier,
            Some(replace_volume_root(roots(1), 3)),
        ),
        (PreparationKind::CheckpointCapture, None),
    ] {
        assert_eq!(
            control.begin_preparation(PreparationRequest {
                operation_id: op(57),
                branch_id: branch(2),
                expected_revision: 1,
                grant: other_writer,
                attachment_id: attachment(2),
                volume_epoch: 1,
                protection_id: protection(4),
                kind,
                expected_roots,
                dependencies: vec![],
            }),
            Err(ControlError::OperationMismatch)
        );
    }
    assert_eq!(control.get_operation(op(57)), Err(ControlError::NotFound));
    let other = control
        .get_branch(branch(2))
        .expect("read independent branch");
    assert_eq!(other.pending_mutation_preparations, 0);
    assert!(other.capture.is_none());
    assert_eq!(control.abort_operation(op(55)), Ok(OperationState::Aborted));
    assert_eq!(control.get_operation(op(57)), Ok(OperationState::Aborted));
    assert!(
        control
            .get_branch(branch(1))
            .expect("read released capture")
            .capture
            .is_none()
    );
}

#[test]
fn branch_lifecycle_move_head_promote_delete_aba_and_name_reuse() {
    let control = control();
    // Register G2 and G3 with distinct roots so head resets visibly swap live roots.
    control
        .register_generation(op(100), verified_seed(2))
        .expect("register G2");
    control
        .register_generation(op(101), verified_seed(3))
        .expect("register G3");

    // Fork candidate branch from G2 (metadata-only, no writer/attachment).
    let candidate = control
        .create_branch(op(102), branch(2), "candidate", generation(2))
        .expect("fork candidate");
    assert_eq!(candidate.head, generation(2));
    assert!(candidate.writer.is_none());
    assert!(candidate.attachment.is_none());

    // Add uncheckpointed live volume state on candidate so we can prove promote
    // adopts ONLY candidate.head (G2), never its uncheckpointed live root.
    let cand_writer = control
        .acquire_writer(op(103), branch(2), 1, 0)
        .expect("acquire candidate writer");
    control
        .attach_volume(
            op(104),
            branch(2),
            1,
            cand_writer,
            attachment(2),
            protection(20),
        )
        .expect("attach candidate");
    let cand_live_roots = replace_volume_root(candidate_roots(&candidate), 99);
    control
        .begin_preparation(PreparationRequest {
            operation_id: op(105),
            branch_id: branch(2),
            expected_revision: 1,
            grant: cand_writer,
            attachment_id: attachment(2),
            volume_epoch: 1,
            protection_id: protection(21),
            kind: PreparationKind::VolumeBarrier,
            expected_roots: Some(cand_live_roots),
            dependencies: vec![cand_live_roots.volume],
        })
        .expect("prepare candidate barrier");
    let candidate = control
        .commit_live_roots(op(105), 1, cand_writer, cand_live_roots, None)
        .expect("commit uncheckpointed live state on candidate");
    assert_eq!(candidate.revision, 2);
    assert_eq!(candidate.volume_root, cand_live_roots.volume);

    // Put uncheckpointed live state on main as well.
    let main_before = control.get_branch(branch(1)).expect("read main");
    let main_writer = control
        .acquire_writer(op(106), branch(1), 1, 0)
        .expect("acquire main writer");
    control
        .attach_volume(
            op(107),
            branch(1),
            1,
            main_writer,
            attachment(1),
            protection(22),
        )
        .expect("attach main");
    let main_live_roots = replace_volume_root(candidate_roots(&main_before), 88);
    control
        .begin_preparation(PreparationRequest {
            operation_id: op(108),
            branch_id: branch(1),
            expected_revision: 1,
            grant: main_writer,
            attachment_id: attachment(1),
            volume_epoch: 1,
            protection_id: protection(23),
            kind: PreparationKind::VolumeBarrier,
            expected_roots: Some(main_live_roots),
            dependencies: vec![main_live_roots.volume],
        })
        .expect("prepare main barrier");
    let main_dirty = control
        .commit_live_roots(op(108), 1, main_writer, main_live_roots, None)
        .expect("commit live state on main");
    assert_eq!(main_dirty.revision, 2);

    // Promote without discard_live_state rejects because main has uncheckpointed live state.
    assert_eq!(
        control.promote(op(109), branch(2), 2, branch(1), 2, main_writer, false),
        Err(ControlError::LiveStateDiscardRequired)
    );
    // Stale source revision conflicts.
    assert_eq!(
        control.promote(op(110), branch(2), 1, branch(1), 2, main_writer, true),
        Err(ControlError::Conflict)
    );
    // Self-promote is invalid.
    assert!(matches!(
        control.promote(op(111), branch(1), 2, branch(1), 2, main_writer, true),
        Err(ControlError::InvalidRequest(_))
    ));

    // Promote candidate -> main with explicit discard_live_state adopts G2 (NOT cand_live_roots).
    let promoted = control
        .promote(op(112), branch(2), 2, branch(1), 2, main_writer, true)
        .expect("promote candidate head onto main");
    assert_eq!(promoted.head, generation(2));
    assert_eq!(promoted.revision, 3);
    assert_eq!(promoted.volume_root, verified_seed(2).seed().roots.volume);
    assert_ne!(promoted.volume_root, cand_live_roots.volume);
    assert!(promoted.writer.is_none());
    assert!(promoted.attachment.is_none());
    assert!(promoted.writer_epoch > main_writer.writer_epoch);
    assert!(promoted.volume_epoch > main_dirty.volume_epoch);
    assert!(promoted.stream_epoch > main_dirty.stream_epoch);
    // Old writer and attachment protection are fenced/retired.
    assert_eq!(
        control.renew_writer(op(113), main_writer),
        Err(ControlError::Fenced)
    );
    // Replay of promote returns the exact committed result.
    assert_eq!(
        control
            .promote(op(112), branch(2), 2, branch(1), 2, main_writer, true)
            .expect("replay promote"),
        promoted
    );

    // Now G1 has zero branch_refs and zero attachment_refs, so it can be marked unretained.
    control
        .mark_generation_unretained(op(114), generation(1))
        .expect("G1 has no remaining references");

    // Acquire a new writer on main and verify moving to expired G1 rejects, while moving to G3
    // works.
    let main_writer2 = control
        .acquire_writer(op(115), branch(1), promoted.revision, promoted.writer_epoch)
        .expect("acquire writer on promoted main");
    assert_eq!(
        control.move_head(
            op(116),
            branch(1),
            promoted.revision,
            generation(1),
            main_writer2,
            false,
        ),
        Err(ControlError::GenerationExpired)
    );
    let moved_g3 = control
        .move_head(
            op(117),
            branch(1),
            promoted.revision,
            generation(3),
            main_writer2,
            false,
        )
        .expect("adopt G3");
    assert_eq!(moved_g3.head, generation(3));
    assert_eq!(moved_g3.revision, 4);

    // Head-away / head-back ABA test: move back to G2, then stale revision=3 must conflict!
    let main_writer3 = control
        .acquire_writer(op(118), branch(1), moved_g3.revision, moved_g3.writer_epoch)
        .expect("acquire writer for rollback");
    let rolled_back = control
        .move_head(
            op(119),
            branch(1),
            moved_g3.revision,
            generation(2),
            main_writer3,
            false,
        )
        .expect("roll back to G2");
    assert_eq!(rolled_back.head, generation(2));
    assert_eq!(rolled_back.revision, 5);
    let main_writer4 = control
        .acquire_writer(
            op(120),
            branch(1),
            rolled_back.revision,
            rolled_back.writer_epoch,
        )
        .expect("acquire writer after rollback");
    assert_eq!(
        control.move_head(op(121), branch(1), 3, generation(3), main_writer4, false),
        Err(ControlError::Conflict),
        "stale revision from earlier G2 state must not pass CAS"
    );

    // Delete candidate branch: requires discard_live_state because it has uncheckpointed writes.
    assert_eq!(
        control.delete_branch(op(122), branch(2), 2, cand_writer, false),
        Err(ControlError::LiveStateDiscardRequired)
    );
    control
        .delete_branch(op(123), branch(2), 2, cand_writer, true)
        .expect("delete candidate with explicit discard");
    assert_eq!(control.get_branch(branch(2)), Err(ControlError::NotFound));
    assert_eq!(
        control.branch_by_name("candidate"),
        Err(ControlError::NotFound)
    );

    // Recreate name "candidate" with a new stable branch ID; old branch ID stays tombstoned.
    assert_eq!(
        control.create_branch(op(124), branch(2), "candidate", generation(2)),
        Err(ControlError::Conflict)
    );
    let recreated = control
        .create_branch(op(125), branch(3), "candidate", generation(3))
        .expect("reuse branch name with fresh stable ID");
    assert_eq!(recreated.branch_id, branch(3));
    assert_eq!(recreated.head, generation(3));

    // Replaying the old delete_branch or old create_branch cannot touch the new branch.
    control
        .delete_branch(op(123), branch(2), 2, cand_writer, true)
        .expect("replay old delete safely");
    assert_eq!(
        control
            .branch_by_name("candidate")
            .expect("new branch unaffected by old delete replay")
            .branch_id,
        branch(3)
    );
    assert_eq!(
        control
            .create_branch(op(102), branch(2), "candidate", generation(2))
            .expect("replay old create returns original branch")
            .branch_id,
        branch(2)
    );
}

#[test]
fn force_acquire_writer_fences_active_writer_capture_and_unstarted_child() {
    let control = control();
    let old_writer = control
        .acquire_writer(op(140), branch(1), 1, 0)
        .expect("acquire initial writer");
    control
        .attach_volume(
            op(141),
            branch(1),
            1,
            old_writer,
            attachment(1),
            protection(40),
        )
        .expect("attach initial volume");
    control
        .begin_preparation(PreparationRequest {
            operation_id: op(142),
            branch_id: branch(1),
            expected_revision: 1,
            grant: old_writer,
            attachment_id: attachment(1),
            volume_epoch: 1,
            protection_id: protection(41),
            kind: PreparationKind::CheckpointCapture,
            expected_roots: None,
            dependencies: vec![],
        })
        .expect("begin capture");
    control
        .authorize_capture_child(op(143), op(142), op(144), old_writer)
        .expect("reserve unstarted capture child");

    // Normal acquire_writer is rejected as Busy.
    assert_eq!(
        control.acquire_writer(op(145), branch(1), 1, old_writer.writer_epoch),
        Err(ControlError::Busy)
    );

    // Forced handover atomically fences old writer/attachment, aborts open capture,
    // tombstones unstarted child, and grants replacement writer.
    let replacement = control
        .force_acquire_writer(op(146), branch(1), 1, old_writer.writer_epoch)
        .expect("force writer handover");
    assert!(replacement.writer_epoch > old_writer.writer_epoch);
    assert_eq!(
        control
            .force_acquire_writer(op(146), branch(1), 1, old_writer.writer_epoch)
            .expect("replay forced handover returns same grant"),
        replacement
    );

    let after = control.get_branch(branch(1)).expect("read branch");
    assert!(after.capture.is_none());
    assert!(after.attachment.is_none());
    assert_eq!(after.writer, Some(replacement));
    assert!(after.volume_epoch > 1);
    assert!(after.stream_epoch > 1);
    assert_eq!(control.get_operation(op(142)), Ok(OperationState::Aborted));
    assert_eq!(control.get_operation(op(144)), Ok(OperationState::Aborted));
    assert_eq!(
        control.renew_writer(op(147), old_writer),
        Err(ControlError::Fenced)
    );
}

#[test]
fn force_acquire_writer_aborts_preparing_capture_child_across_principals_and_owner_restart() {
    let authority = Arc::new(MemoryAuthority::default());
    let owner1 = start_authority_owner(authority.as_ref(), op(250)).expect("start owner 1");
    let principal_a = WorkspaceControl::new(
        authority.clone(),
        workspace(1),
        ObjectStoreScopeId::from_bytes([0xA5; 16]),
        principal(1),
        owner1,
    );
    let principal_b = WorkspaceControl::new(
        authority.clone(),
        workspace(1),
        ObjectStoreScopeId::from_bytes([0xA5; 16]),
        principal(2),
        owner1,
    );
    principal_a
        .create_workspace(op(1), branch(1), "main", 60_000, verified_seed(1))
        .expect("create workspace");
    let writer_a = principal_a
        .acquire_writer(op(2), branch(1), 1, 0)
        .expect("acquire writer A");
    principal_a
        .attach_volume(op(3), branch(1), 1, writer_a, attachment(1), protection(1))
        .expect("attach volume A");
    principal_a
        .begin_preparation(PreparationRequest {
            operation_id: op(4),
            branch_id: branch(1),
            expected_revision: 1,
            grant: writer_a,
            attachment_id: attachment(1),
            volume_epoch: 1,
            protection_id: protection(2),
            kind: PreparationKind::CheckpointCapture,
            expected_roots: None,
            dependencies: vec![],
        })
        .expect("begin capture");
    principal_a
        .authorize_capture_child(op(5), op(4), op(6), writer_a)
        .expect("authorize child barrier");
    let next_roots = replace_volume_root(
        candidate_roots(&principal_a.get_branch(branch(1)).unwrap()),
        42,
    );
    principal_a
        .begin_preparation(PreparationRequest {
            operation_id: op(6),
            branch_id: branch(1),
            expected_revision: 1,
            grant: writer_a,
            attachment_id: attachment(1),
            volume_epoch: 1,
            protection_id: protection(3),
            kind: PreparationKind::VolumeBarrier,
            expected_roots: Some(next_roots),
            dependencies: vec![next_roots.volume],
        })
        .expect("begin child barrier preparation");

    // Principal B forces handover while Principal A's child barrier is actively Preparing.
    let writer_b = principal_b
        .force_acquire_writer(op(10), branch(1), 1, writer_a.writer_epoch)
        .expect("forced handover must fence capture and preparing child across principals");
    assert!(writer_b.writer_epoch > writer_a.writer_epoch);
    assert_eq!(
        principal_a.get_operation(op(4)),
        Ok(OperationState::Aborted)
    );
    assert_eq!(
        principal_a.get_operation(op(6)),
        Ok(OperationState::Aborted)
    );
    let after = principal_b.get_branch(branch(1)).expect("read branch");
    assert!(after.capture.is_none());
    assert!(after.attachment.is_none());
    assert_eq!(after.pending_mutation_preparations, 0);

    // Attach under Principal B, start another capture + preparing child, then restart authority
    // owner.
    principal_b
        .attach_volume(op(11), branch(1), 1, writer_b, attachment(2), protection(4))
        .expect("attach under writer B");
    principal_b
        .begin_preparation(PreparationRequest {
            operation_id: op(12),
            branch_id: branch(1),
            expected_revision: 1,
            grant: writer_b,
            attachment_id: attachment(2),
            volume_epoch: after.volume_epoch,
            protection_id: protection(5),
            kind: PreparationKind::CheckpointCapture,
            expected_roots: None,
            dependencies: vec![],
        })
        .expect("begin second capture");
    principal_b
        .authorize_capture_child(op(13), op(12), op(14), writer_b)
        .expect("authorize second child");
    principal_b
        .begin_preparation(PreparationRequest {
            operation_id: op(14),
            branch_id: branch(1),
            expected_revision: 1,
            grant: writer_b,
            attachment_id: attachment(2),
            volume_epoch: after.volume_epoch,
            protection_id: protection(6),
            kind: PreparationKind::VolumeBarrier,
            expected_roots: Some(next_roots),
            dependencies: vec![next_roots.volume],
        })
        .expect("begin second child preparation");

    let owner2 = start_authority_owner(authority.as_ref(), op(251)).expect("restart owner");
    let principal_c = WorkspaceControl::new(
        authority,
        workspace(1),
        ObjectStoreScopeId::from_bytes([0xA5; 16]),
        principal(3),
        owner2,
    );
    let writer_c = principal_c
        .force_acquire_writer(op(20), branch(1), 1, writer_b.writer_epoch)
        .expect("forced handover after owner restart must clear stale preparing child");
    assert_eq!(writer_c.owner_incarnation, owner2);
    assert_eq!(
        principal_b.get_operation(op(12)),
        Ok(OperationState::Aborted)
    );
    assert_eq!(
        principal_b.get_operation(op(14)),
        Ok(OperationState::Aborted)
    );
    assert!(principal_c.get_branch(branch(1)).unwrap().capture.is_none());
}

fn candidate_roots(branch: &versionfs_core::control::BranchState) -> LiveRoots {
    LiveRoots {
        volume: branch.volume_root,
        logs: branch.logs_root,
        artifacts: branch.artifacts_root,
    }
}
