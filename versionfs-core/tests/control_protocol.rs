//! Core control-state-machine regression tests with an in-memory authority.

use std::{
    collections::BTreeMap,
    num::NonZeroUsize,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use versionfs_core::{
    control::{
        ControlError, GenerationSeed, LiveRoots, OperationState, PreparationKind,
        PreparationRequest, WorkspaceControl, start_authority_owner,
    },
    format::{ObjectRef, ObjectType, encode_object},
    identity::{
        AttachmentId, BranchId, GenerationId, ObjectStoreScopeId, OperationId, PrincipalId,
        ProtectionId, WorkspaceId,
    },
    metadata::{
        MetadataAuthority, MetadataCursor, MetadataError, MetadataPage, MetadataSnapshot,
        MetadataTxn, MutationIdentity, ReconciledOutcome,
    },
    time::MonotonicTime,
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
        .create_workspace(op(1), branch(1), "main", 60_000, seed(1))
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

fn seed(marker: u8) -> GenerationSeed {
    GenerationSeed {
        generation_id: generation(marker),
        descriptor: object(ObjectType::GenerationDescriptor, marker),
        roots: roots(marker),
    }
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
    let initial = roots(1);

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
