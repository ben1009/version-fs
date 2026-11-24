//! End-to-end P8 native ordered logs, named artifacts, and P7 common-cut
//! checkpoint integration tests against the real toy-kv Metadata Authority.

#![cfg(unix)]

use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};
use versionfs_core::{
    control::{ControlError, OwnerIncarnation, WorkspaceControl, start_authority_owner},
    fault::NoFailures,
    format::{
        ARTIFACT_CHUNK_BYTES, CaptureMode, EXTENT_BYTES, MAX_LOG_RECORD_BYTES, MAX_LOG_RECORDS,
        ObjectRef, ObjectType, VolumeGeometry,
    },
    graph::verify_generation,
    identity::{
        ArtifactId, AttachmentId, BranchId, GenerationId, LogId, ObjectId, ObjectStoreScopeId,
        OperationId, PrincipalId, ProtectionId, VolumeBoundaryId, WorkspaceId,
    },
    persistent_map::{DurableObjectStore, MapObjectStore},
    time::{Clock, MonotonicTime},
};
use versionfs_sdk::{
    artifacts::{
        ArtifactClient, ArtifactError, NativeProducerGate, list_artifacts_from_root,
        read_attachment_object, store_durable_artifact_payload,
    },
    barrier::{DurableVolume, FileBarrierJournal, initialize_empty_workspace},
    capture::{
        BlockCutOnlyBoundary, CaptureConfig, CheckpointCoordinator, CheckpointRequest,
        FileCaptureJournal, NoopProducerGate,
    },
    logs::{
        LiveLogCursor, LogClient, LogError, NativeAdmissionGate, NativeWriteContext, StreamId,
        list_logs_from_root,
    },
    volume::{BlockDevice, BlockRequest},
};
use versionfs_service::toy_kv_metadata::ToyKvMetadataAuthority;

static NEXT_DIR: AtomicU64 = AtomicU64::new(0);

struct TestDir(PathBuf);

impl TestDir {
    fn new() -> Self {
        let id = NEXT_DIR.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "versionfs-native-views-{}-{id}",
            std::process::id()
        ));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}

impl Drop for TestDir {
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
}

impl Default for MemoryObjects {
    fn default() -> Self {
        Self {
            scope: ObjectStoreScopeId::from_bytes([0xE1; 16]),
            objects: Arc::new(Mutex::new(HashMap::new())),
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
        let reference = ObjectRef::from_encoded(&bytes).map_err(|e| e.to_string())?;
        if reference.object_id != id {
            return Err("object ID mismatch".to_owned());
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
        reference.verify(encoded).map_err(|e| e.to_string())?;
        self.put(reference.object_id, encoded.to_vec())
    }
}

fn id_from_u64(tag: u8, n: u64) -> [u8; 16] {
    let mut bytes = [0u8; 16];
    bytes[0] = tag;
    bytes[8..16].copy_from_slice(&n.to_le_bytes());
    bytes
}

fn op(n: u64) -> OperationId {
    OperationId::from_bytes(id_from_u64(1, n))
}

fn prot(n: u64) -> ProtectionId {
    ProtectionId::from_bytes(id_from_u64(2, n))
}

fn log_id(n: u64) -> LogId {
    LogId::from_bytes(id_from_u64(3, n))
}

fn artifact_id(n: u64) -> ArtifactId {
    ArtifactId::from_bytes(id_from_u64(4, n))
}

fn branch_id(n: u64) -> BranchId {
    BranchId::from_bytes(id_from_u64(5, n))
}

fn attachment_id(n: u64) -> AttachmentId {
    AttachmentId::from_bytes(id_from_u64(6, n))
}

struct Fixture {
    _dir: TestDir,
    _clock: Arc<TestClock>,
    authority: Arc<ToyKvMetadataAuthority>,
    owner: OwnerIncarnation,
    control: WorkspaceControl<ToyKvMetadataAuthority>,
    store: MemoryObjects,
    ctx: NativeWriteContext,
    barrier_dir: PathBuf,
    capture_dir: PathBuf,
    next_id: Arc<AtomicU64>,
}

impl Fixture {
    fn new() -> Self {
        let dir = TestDir::new();
        let clock = Arc::new(TestClock::default());
        let authority = Arc::new(
            ToyKvMetadataAuthority::open_with(
                dir.0.join("meta"),
                clock.clone(),
                Arc::new(NoFailures),
            )
            .unwrap(),
        );
        let owner = start_authority_owner(authority.as_ref(), op(900_000)).unwrap();
        let control = WorkspaceControl::new(
            authority.clone(),
            WorkspaceId::from_bytes([0x11; 16]),
            ObjectStoreScopeId::from_bytes([0xE1; 16]),
            PrincipalId::from_bytes([0x22; 16]),
            owner,
        );
        let mut store = MemoryObjects::default();
        let init = initialize_empty_workspace(
            &mut store,
            &control,
            op(1),
            GenerationId::from_bytes([0x33; 16]),
            VolumeBoundaryId::from_bytes([0x44; 16]),
            VolumeGeometry::new(4 * u64::from(EXTENT_BYTES)).unwrap(),
            60_000,
            branch_id(1),
            "main",
            60_000,
            op(2),
            op(3),
            attachment_id(1),
            prot(1),
        )
        .unwrap();
        let ctx = NativeWriteContext {
            branch_id: branch_id(1),
            grant: init.writer_grant,
            attachment_id: attachment_id(1),
            volume_epoch: init.attachment.volume_epoch,
        };
        Self {
            barrier_dir: dir.0.join("barrier"),
            capture_dir: dir.0.join("capture"),
            _dir: dir,
            _clock: clock,
            authority,
            owner,
            control,
            store,
            ctx,
            next_id: Arc::new(AtomicU64::new(100)),
        }
    }

    fn alloc_op(&self) -> OperationId {
        op(self.next_id.fetch_add(1, Ordering::Relaxed))
    }

    fn alloc_prot(&self) -> ProtectionId {
        prot(self.next_id.fetch_add(1, Ordering::Relaxed))
    }

    fn open_volume(
        &self,
    ) -> DurableVolume<MemoryObjects, WorkspaceControl<ToyKvMetadataAuthority>, FileBarrierJournal>
    {
        let control = WorkspaceControl::new(
            self.authority.clone(),
            WorkspaceId::from_bytes([0x11; 16]),
            ObjectStoreScopeId::from_bytes([0xE1; 16]),
            PrincipalId::from_bytes([0x22; 16]),
            self.owner,
        );
        DurableVolume::open(
            self.store.clone(),
            control,
            FileBarrierJournal::open(&self.barrier_dir).unwrap(),
            self.ctx.branch_id,
            self.ctx.grant,
            self.ctx.attachment_id,
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
}

#[test]
fn log_create_conditional_append_exact_receipt_replay_and_attachments() {
    let f = Fixture::new();
    let mut store = f.store.clone();
    let logs = LogClient::new();

    let (stream_id, created_branch) = logs
        .create_log(
            &mut store,
            &f.control,
            f.ctx,
            f.alloc_op(),
            f.alloc_prot(),
            "events",
            log_id(1),
            || f.alloc_op(),
        )
        .expect("create_log should succeed");
    assert_eq!(stream_id.branch_id, branch_id(1));
    assert_eq!(stream_id.log_id, log_id(1));
    assert_eq!(created_branch.revision, 2);

    // Prepare both an inline blob attachment (<= 64 KiB) and a 4 MiB+ chunked attachment.
    let small_payload = b"small explicit log attachment".to_vec();
    let small_att = store_durable_artifact_payload(&mut store, &small_payload).unwrap();
    assert_eq!(small_att.content_root.object_type, ObjectType::ArtifactBlob);
    assert_eq!(small_att.payload_refs.len(), 1);

    let large_payload = vec![0x5A; ARTIFACT_CHUNK_BYTES + 256];
    let large_att = store_durable_artifact_payload(&mut store, &large_payload).unwrap();
    assert_eq!(
        large_att.content_root.object_type,
        ObjectType::ArtifactIndexNode
    );
    assert_eq!(large_att.payload_refs.len(), 2);

    let mut all_attachments = small_att.payload_refs.clone();
    all_attachments.extend(large_att.payload_refs.clone());

    // Batch limit validations.
    assert!(matches!(
        logs.append(
            &mut store,
            &f.control,
            f.ctx,
            "events",
            stream_id,
            0,
            f.alloc_op(),
            f.alloc_prot(),
            vec![],
            vec![],
            || f.alloc_op(),
        ),
        Err(LogError::EmptyBatch)
    ));
    assert!(matches!(
        logs.append(
            &mut store,
            &f.control,
            f.ctx,
            "events",
            stream_id,
            0,
            f.alloc_op(),
            f.alloc_prot(),
            vec![vec![1]; MAX_LOG_RECORDS + 1],
            vec![],
            || f.alloc_op(),
        ),
        Err(LogError::TooManyRecords { .. })
    ));
    assert!(matches!(
        logs.append(
            &mut store,
            &f.control,
            f.ctx,
            "events",
            stream_id,
            0,
            f.alloc_op(),
            f.alloc_prot(),
            vec![vec![0; MAX_LOG_RECORD_BYTES]],
            vec![],
            || f.alloc_op(),
        ),
        Err(LogError::BatchTooLarge { .. })
    ));
    assert!(matches!(
        logs.append(
            &mut store,
            &f.control,
            f.ctx,
            "events",
            stream_id,
            u64::MAX,
            f.alloc_op(),
            f.alloc_prot(),
            vec![b"overflow".to_vec()],
            vec![],
            || f.alloc_op(),
        ),
        Err(LogError::SequenceOverflow)
    ));

    // Append batch 1: [0, 2) with explicit attachments.
    let append1_id = f.alloc_op();
    let batch1_records = vec![b"rec-0".to_vec(), b"rec-1".to_vec()];
    let receipt1 = logs
        .append(
            &mut store,
            &f.control,
            f.ctx,
            "events",
            stream_id,
            0,
            append1_id,
            f.alloc_prot(),
            batch1_records.clone(),
            all_attachments.clone(),
            || f.alloc_op(),
        )
        .expect("append batch 1");
    assert_eq!(receipt1.start_seq, 0);
    assert_eq!(receipt1.end_seq, 2);
    assert_eq!(receipt1.append_id, append1_id);

    // Conditional tail mismatch rejected.
    assert!(matches!(
        logs.append(
            &mut store,
            &f.control,
            f.ctx,
            "events",
            stream_id,
            0,
            f.alloc_op(),
            f.alloc_prot(),
            vec![b"stale".to_vec()],
            vec![],
            || f.alloc_op(),
        ),
        Err(LogError::MatchSeqMismatch {
            expected: 0,
            actual: 2
        })
    ));

    // Append batch 2: [2, 5), advancing the tail past batch 1.
    let receipt2 = logs
        .append(
            &mut store,
            &f.control,
            f.ctx,
            "events",
            stream_id,
            2,
            f.alloc_op(),
            f.alloc_prot(),
            vec![b"rec-2".to_vec(), b"rec-3".to_vec(), b"rec-4".to_vec()],
            vec![],
            || f.alloc_op(),
        )
        .expect("append batch 2");
    assert_eq!(receipt2.start_seq, 2);
    assert_eq!(receipt2.end_seq, 5);

    // Replaying append1_id AFTER tail advanced to 5 returns the exact original receipt
    // without allocating a second range.
    let replayed = logs
        .append(
            &mut store,
            &f.control,
            f.ctx,
            "events",
            stream_id,
            0,
            append1_id,
            f.alloc_prot(),
            batch1_records,
            all_attachments.clone(),
            || f.alloc_op(),
        )
        .expect("matching append retry must return original receipt");
    assert_eq!(replayed, receipt1);

    // Replaying append1_id with changed payload fails with OperationMismatch.
    assert!(matches!(
        logs.append(
            &mut store,
            &f.control,
            f.ctx,
            "events",
            stream_id,
            0,
            append1_id,
            f.alloc_prot(),
            vec![b"different-0".to_vec(), b"rec-1".to_vec()],
            all_attachments,
            || f.alloc_op(),
        ),
        Err(LogError::Control(ControlError::OperationMismatch))
    ));

    // Poll live log from cursor 0 and verify records + explicit attachments.
    let (batches, cursor) = logs
        .poll_live_log(
            &f.store,
            &f.control,
            "events",
            LiveLogCursor {
                stream_id,
                next_seq: 0,
            },
            10,
            f.alloc_op(),
            f.alloc_prot(),
            f.alloc_op(),
        )
        .expect("poll_live_log");
    assert_eq!(batches.len(), 2);
    assert_eq!(batches[0].first_seq, 0);
    assert_eq!(batches[0].end_seq, 2);
    assert_eq!(batches[0].attachments.len(), 3);
    assert_eq!(
        read_attachment_object(&f.store, batches[0].attachments[0]).unwrap(),
        small_payload
    );
    let mut reconstructed = read_attachment_object(&f.store, batches[0].attachments[1]).unwrap();
    reconstructed
        .extend_from_slice(&read_attachment_object(&f.store, batches[0].attachments[2]).unwrap());
    assert_eq!(reconstructed, large_payload);
    assert_eq!(cursor.next_seq, 5);
}

#[test]
fn batch_boundary_trim_cursor_expiry_and_retained_generation_survival() {
    let f = Fixture::new();
    let mut store = f.store.clone();
    let logs = LogClient::new();

    let (stream_id, _) = logs
        .create_log(
            &mut store,
            &f.control,
            f.ctx,
            f.alloc_op(),
            f.alloc_prot(),
            "events",
            log_id(10),
            || f.alloc_op(),
        )
        .unwrap();
    let att_payload = b"retained-attachment-survives-live-trim".to_vec();
    let att = store_durable_artifact_payload(&mut store, &att_payload).unwrap();

    logs.append(
        &mut store,
        &f.control,
        f.ctx,
        "events",
        stream_id,
        0,
        f.alloc_op(),
        f.alloc_prot(),
        vec![b"b1-0".to_vec(), b"b1-1".to_vec()],
        att.payload_refs.clone(),
        || f.alloc_op(),
    )
    .unwrap();
    logs.append(
        &mut store,
        &f.control,
        f.ctx,
        "events",
        stream_id,
        2,
        f.alloc_op(),
        f.alloc_prot(),
        vec![b"b2-2".to_vec(), b"b2-3".to_vec(), b"b2-4".to_vec()],
        vec![],
        || f.alloc_op(),
    )
    .unwrap();

    // Checkpoint via P7 coordinator -> publishes G1 capturing live logs_root.
    let mut volume = f.open_volume();
    let mut coordinator = f.coordinator();
    let mut producers = NoopProducerGate;
    let mut boundary = BlockCutOnlyBoundary;
    let g1 = coordinator
        .checkpoint(
            &mut volume,
            &mut producers,
            &mut boundary,
            &CheckpointRequest::attached(CaptureMode::BlockCut, 10_000),
            &[],
        )
        .expect("checkpoint G1 with live logs");
    drop(volume);

    // Mid-batch trim is rejected.
    assert!(matches!(
        logs.trim_log(
            &mut store,
            &f.control,
            f.ctx,
            "events",
            stream_id,
            1,
            f.alloc_op(),
            f.alloc_prot(),
            || f.alloc_op(),
        ),
        Err(LogError::InvalidTrimBoundary {
            requested_seq: 1,
            first_seq: 0,
            next_seq: 5
        })
    ));

    // Whole-batch-boundary trim to seq 2 succeeds.
    let (trimmed_view, _) = logs
        .trim_log(
            &mut store,
            &f.control,
            f.ctx,
            "events",
            stream_id,
            2,
            f.alloc_op(),
            f.alloc_prot(),
            || f.alloc_op(),
        )
        .expect("whole-batch trim to 2");
    assert_eq!(trimmed_view.first_seq, 2);
    assert_eq!(trimmed_view.next_seq, 5);

    // Cursor before trimmed start returns explicit CursorExpired.
    assert!(matches!(
        logs.poll_live_log(
            &f.store,
            &f.control,
            "events",
            LiveLogCursor {
                stream_id,
                next_seq: 0,
            },
            10,
            f.alloc_op(),
            f.alloc_prot(),
            f.alloc_op(),
        ),
        Err(LogError::CursorExpired {
            cursor_seq: 0,
            first_seq: 2
        })
    ));

    // Delete live log and recreate with a fresh LogId -> old cursor sees StreamReplaced.
    logs.delete_log(
        &mut store,
        &f.control,
        f.ctx,
        "events",
        stream_id,
        f.alloc_op(),
        f.alloc_prot(),
        || f.alloc_op(),
    )
    .expect("delete live log");
    let (recreated_stream, _) = logs
        .create_log(
            &mut store,
            &f.control,
            f.ctx,
            f.alloc_op(),
            f.alloc_prot(),
            "events",
            log_id(11),
            || f.alloc_op(),
        )
        .expect("recreate log with new LogId");
    assert_ne!(recreated_stream.log_id, stream_id.log_id);
    assert!(matches!(
        logs.poll_live_log(
            &f.store,
            &f.control,
            "events",
            LiveLogCursor {
                stream_id,
                next_seq: 2,
            },
            10,
            f.alloc_op(),
            f.alloc_prot(),
            f.alloc_op(),
        ),
        Err(LogError::StreamReplaced { .. })
    ));

    // Protected historical read on G1 (without mounting volume!) STILL returns
    // trimmed/deleted range [0, 2) and its explicit attachment payload.
    let (hist_view, hist_batches) = logs
        .read_historical_log(
            &f.store,
            &f.control,
            g1.generation_id,
            "events",
            0,
            10,
            f.alloc_op(),
            f.alloc_prot(),
            f.alloc_op(),
        )
        .expect("historical read must survive live trim and delete");
    assert_eq!(hist_view.first_seq, 0);
    assert_eq!(hist_view.next_seq, 5);
    assert_eq!(hist_batches.len(), 2);
    assert_eq!(
        hist_batches[0].records,
        vec![b"b1-0".to_vec(), b"b1-1".to_vec()]
    );
    assert_eq!(
        read_attachment_object(&f.store, hist_batches[0].attachments[0]).unwrap(),
        att_payload
    );
}

#[test]
fn fork_and_rollback_create_independent_streams_and_replace_old_cursors() {
    let f = Fixture::new();
    let mut store = f.store.clone();
    let logs = LogClient::new();

    let (main_stream, _) = logs
        .create_log(
            &mut store,
            &f.control,
            f.ctx,
            f.alloc_op(),
            f.alloc_prot(),
            "audit",
            log_id(20),
            || f.alloc_op(),
        )
        .unwrap();
    logs.append(
        &mut store,
        &f.control,
        f.ctx,
        "audit",
        main_stream,
        0,
        f.alloc_op(),
        f.alloc_prot(),
        vec![b"shared-0".to_vec(), b"shared-1".to_vec()],
        vec![],
        || f.alloc_op(),
    )
    .unwrap();

    // Publish G1 on main.
    let mut volume = f.open_volume();
    let mut coordinator = f.coordinator();
    let mut producers = NoopProducerGate;
    let mut boundary = BlockCutOnlyBoundary;
    let g1 = coordinator
        .checkpoint(
            &mut volume,
            &mut producers,
            &mut boundary,
            &CheckpointRequest::attached(CaptureMode::BlockCut, 20_000),
            &[],
        )
        .unwrap();
    drop(volume);

    // Fork `feature` from G1, acquire writer + attach, and append independently at seq 2.
    let fork_branch = f
        .control
        .create_branch(f.alloc_op(), branch_id(2), "feature", g1.generation_id)
        .unwrap();
    let fork_grant = f
        .control
        .acquire_writer(
            f.alloc_op(),
            branch_id(2),
            fork_branch.revision,
            fork_branch.writer_epoch,
        )
        .unwrap();
    let fork_att = f
        .control
        .attach_volume(
            f.alloc_op(),
            branch_id(2),
            fork_branch.revision,
            fork_grant,
            attachment_id(2),
            f.alloc_prot(),
        )
        .unwrap();
    let fork_ctx = NativeWriteContext {
        branch_id: branch_id(2),
        grant: fork_grant,
        attachment_id: attachment_id(2),
        volume_epoch: fork_att.volume_epoch,
    };
    let fork_stream = StreamId {
        branch_id: branch_id(2),
        stream_epoch: fork_branch.stream_epoch,
        log_id: log_id(20),
    };

    // Append divergent records at seq 2 on both main and feature.
    logs.append(
        &mut store,
        &f.control,
        f.ctx,
        "audit",
        main_stream,
        2,
        f.alloc_op(),
        f.alloc_prot(),
        vec![b"main-2".to_vec()],
        vec![],
        || f.alloc_op(),
    )
    .unwrap();
    logs.append(
        &mut store,
        &f.control,
        fork_ctx,
        "audit",
        fork_stream,
        2,
        f.alloc_op(),
        f.alloc_prot(),
        vec![b"feature-2".to_vec()],
        vec![],
        || f.alloc_op(),
    )
    .unwrap();

    // Verify independent tails while sharing historical segment [0, 2).
    let (main_batches, _) = logs
        .poll_live_log(
            &f.store,
            &f.control,
            "audit",
            LiveLogCursor {
                stream_id: main_stream,
                next_seq: 0,
            },
            10,
            f.alloc_op(),
            f.alloc_prot(),
            f.alloc_op(),
        )
        .unwrap();
    let (fork_batches, _) = logs
        .poll_live_log(
            &f.store,
            &f.control,
            "audit",
            LiveLogCursor {
                stream_id: fork_stream,
                next_seq: 0,
            },
            10,
            f.alloc_op(),
            f.alloc_prot(),
            f.alloc_op(),
        )
        .unwrap();
    assert_eq!(main_batches[0].records, fork_batches[0].records);
    assert_eq!(main_batches[1].records, vec![b"main-2".to_vec()]);
    assert_eq!(fork_batches[1].records, vec![b"feature-2".to_vec()]);

    // Roll back main to G1 (discarding uncheckpointed `main-2`) -> stream_epoch advances.
    let current_main = f.control.get_branch(branch_id(1)).unwrap();
    let rolled_back = f
        .control
        .move_head(
            f.alloc_op(),
            branch_id(1),
            current_main.revision,
            g1.generation_id,
            f.ctx.grant,
            true,
        )
        .unwrap();
    assert!(rolled_back.stream_epoch > main_stream.stream_epoch);

    // Old cursor on main sees explicit StreamReplaced.
    assert!(matches!(
        logs.poll_live_log(
            &f.store,
            &f.control,
            "audit",
            LiveLogCursor {
                stream_id: main_stream,
                next_seq: 2,
            },
            10,
            f.alloc_op(),
            f.alloc_prot(),
            f.alloc_op(),
        ),
        Err(LogError::StreamReplaced { .. })
    ));

    // Re-acquire writer + attach on rolled-back main and append independently under new
    // stream_epoch.
    let new_grant = f
        .control
        .acquire_writer(
            f.alloc_op(),
            branch_id(1),
            rolled_back.revision,
            rolled_back.writer_epoch,
        )
        .unwrap();
    let new_att = f
        .control
        .attach_volume(
            f.alloc_op(),
            branch_id(1),
            rolled_back.revision,
            new_grant,
            attachment_id(3),
            f.alloc_prot(),
        )
        .unwrap();
    let reset_ctx = NativeWriteContext {
        branch_id: branch_id(1),
        grant: new_grant,
        attachment_id: attachment_id(3),
        volume_epoch: new_att.volume_epoch,
    };
    let reset_stream = StreamId {
        branch_id: branch_id(1),
        stream_epoch: rolled_back.stream_epoch,
        log_id: log_id(20),
    };
    let receipt = logs
        .append(
            &mut store,
            &f.control,
            reset_ctx,
            "audit",
            reset_stream,
            2,
            f.alloc_op(),
            f.alloc_prot(),
            vec![b"after-reset-2".to_vec()],
            vec![],
            || f.alloc_op(),
        )
        .expect("independent append after rollback must succeed");
    assert_eq!(receipt.start_seq, 2);
    assert_eq!(receipt.end_seq, 3);
}

#[test]
fn gated_native_artifacts_and_common_cut_checkpoint_capture_real_native_roots() {
    let f = Fixture::new();
    let mut store = f.store.clone();
    let admission = NativeAdmissionGate::new();
    let logs = LogClient::with_admission(admission.clone());
    let artifacts = ArtifactClient::with_admission(admission.clone());

    // Create a native log and append one batch before checkpoint.
    let (stream_id, _) = logs
        .create_log(
            &mut store,
            &f.control,
            f.ctx,
            f.alloc_op(),
            f.alloc_prot(),
            "tool-calls",
            log_id(30),
            || f.alloc_op(),
        )
        .unwrap();
    logs.append(
        &mut store,
        &f.control,
        f.ctx,
        "tool-calls",
        stream_id,
        0,
        f.alloc_op(),
        f.alloc_prot(),
        vec![b"call-0".to_vec()],
        vec![],
        || f.alloc_op(),
    )
    .unwrap();

    // Build NativeProducerGate sharing the exact same NativeAdmissionGate.
    let gate_control = WorkspaceControl::new(
        f.authority.clone(),
        WorkspaceId::from_bytes([0x11; 16]),
        ObjectStoreScopeId::from_bytes([0xE1; 16]),
        PrincipalId::from_bytes([0x22; 16]),
        f.owner,
    );
    let id_counter = f.next_id.clone();
    let mut gate = NativeProducerGate::new(
        admission.clone(),
        f.store.clone(),
        gate_control,
        f.ctx,
        move || {
            let n1 = id_counter.fetch_add(1, Ordering::Relaxed);
            let n2 = id_counter.fetch_add(1, Ordering::Relaxed);
            (op(n1), prot(n2))
        },
    );

    // Stage both an inline blob artifact (<= 64 KiB) and a 4 MiB+ chunked artifact
    // with explicit ArtifactIds (never inferred from name or path).
    let small_bytes = b"report-summary-json".to_vec();
    let large_bytes = vec![0x7C; ARTIFACT_CHUNK_BYTES + 1024];
    gate.stage_put(
        "reports/summary.json",
        artifact_id(100),
        small_bytes.clone(),
        vec![],
    )
    .unwrap();
    gate.stage_put(
        "blobs/model.bin",
        artifact_id(200),
        large_bytes.clone(),
        vec![],
    )
    .unwrap();
    assert_eq!(gate.staged_len(), 2);

    // Verify admission gating rejects new native mutations while closed.
    admission.close();
    assert!(matches!(
        logs.append(
            &mut store,
            &f.control,
            f.ctx,
            "tool-calls",
            stream_id,
            1,
            f.alloc_op(),
            f.alloc_prot(),
            vec![b"blocked".to_vec()],
            vec![],
            || f.alloc_op(),
        ),
        Err(LogError::AdmissionClosed)
    ));
    assert!(matches!(
        gate.stage_put("blocked", artifact_id(300), vec![1], vec![]),
        Err(ArtifactError::AdmissionClosed)
    ));
    admission.reopen();

    // Run P7 CheckpointCoordinator with NativeProducerGate:
    // On attempt 1, drain_producers() flushes the 2 staged artifacts into
    // branch.artifacts_root AFTER P7's pre-drain snapshot -> P7's existing CAS
    // restarts Step 1 cleanly with a fresh capture ID and binds the real
    // non-empty logs_root and artifacts_root into the published Generation!
    let mut volume = f.open_volume();
    volume
        .dispatch(BlockRequest::Write {
            offset: 0,
            data: &[0xEE; 4096],
            fua: false,
        })
        .unwrap();
    let mut coordinator = f.coordinator();
    let mut boundary = BlockCutOnlyBoundary;
    let published = coordinator
        .checkpoint(
            &mut volume,
            &mut gate,
            &mut boundary,
            &CheckpointRequest::attached(CaptureMode::BlockCut, 30_000),
            &[],
        )
        .expect("common-cut checkpoint must flush staged artifacts and capture real native roots");
    assert_eq!(gate.staged_len(), 0);
    assert!(!admission.is_closed());

    // Verify complete Generation graph (volume + logs + blob/chunked artifacts).
    let seed = f.control.get_generation(published.generation_id).unwrap();
    verify_generation(&f.store, seed.descriptor)
        .expect("published Generation with native logs and chunked artifacts must verify");

    // Inspect native views from the published Generation WITHOUT mounting the volume.
    let listed_logs = list_logs_from_root(&f.store, seed.roots.logs).unwrap();
    assert_eq!(listed_logs.len(), 1);
    assert_eq!(listed_logs[0].name, "tool-calls");
    assert_eq!(listed_logs[0].next_seq, 1);

    let listed_artifacts = list_artifacts_from_root(&f.store, seed.roots.artifacts).unwrap();
    assert_eq!(listed_artifacts.len(), 2);

    let (small_desc, read_small) = artifacts
        .read_historical_artifact(
            &f.store,
            &f.control,
            published.generation_id,
            "reports/summary.json",
            f.alloc_op(),
            f.alloc_prot(),
            f.alloc_op(),
        )
        .unwrap();
    assert_eq!(small_desc.artifact_id, artifact_id(100));
    assert_eq!(
        small_desc.content_root.object_type,
        ObjectType::ArtifactBlob
    );
    assert_eq!(read_small, small_bytes);

    let (large_desc, read_large) = artifacts
        .read_historical_artifact(
            &f.store,
            &f.control,
            published.generation_id,
            "blobs/model.bin",
            f.alloc_op(),
            f.alloc_prot(),
            f.alloc_op(),
        )
        .unwrap();
    assert_eq!(large_desc.artifact_id, artifact_id(200));
    assert_eq!(
        large_desc.content_root.object_type,
        ObjectType::ArtifactIndexNode
    );
    assert_eq!(read_large, large_bytes);
}

#[test]
fn native_review_regressions_heartbeat_refresh_replay_abort_and_dependency_verification() {
    let f = Fixture::new();
    let mut store = f.store.clone();
    let logs = LogClient::new();
    let artifacts = ArtifactClient::new();

    // 1. Writer heartbeat after NativeProducerGate creation must NOT fail flush_staged_artifacts.
    let admission = NativeAdmissionGate::new();
    let gate_control = WorkspaceControl::new(
        f.authority.clone(),
        WorkspaceId::from_bytes([0x11; 16]),
        ObjectStoreScopeId::from_bytes([0xE1; 16]),
        PrincipalId::from_bytes([0x22; 16]),
        f.owner,
    );
    let id_counter = f.next_id.clone();
    let mut gate =
        NativeProducerGate::new(admission, f.store.clone(), gate_control, f.ctx, move || {
            let n1 = id_counter.fetch_add(1, Ordering::Relaxed);
            let n2 = id_counter.fetch_add(1, Ordering::Relaxed);
            (op(n1), prot(n2))
        });
    gate.stage_put("after-heartbeat", artifact_id(1), b"hello".to_vec(), vec![])
        .unwrap();
    // Advance clock and renew writer grant independently.
    f._clock.0.store(5_000_000_000, Ordering::SeqCst);
    let _renewed = f.control.renew_writer(f.alloc_op(), f.ctx.grant).unwrap();
    let mut volume = f.open_volume();
    let mut coordinator = f.coordinator();
    let mut boundary = BlockCutOnlyBoundary;
    let published = coordinator
        .checkpoint(
            &mut volume,
            &mut gate,
            &mut boundary,
            &CheckpointRequest::attached(CaptureMode::BlockCut, 40_000),
            &[],
        )
        .expect("flush_staged_artifacts must refresh writer grant after heartbeat");
    assert_eq!(published.branch.head, published.generation_id);
    drop(volume);

    // 2. put_artifact replay with same-length DIFFERENT payload must fail with OperationMismatch.
    let put_op = f.alloc_op();
    let (desc_a, _) = artifacts
        .put_artifact(
            &mut store,
            &f.control,
            f.ctx,
            put_op,
            f.alloc_prot(),
            "cfg",
            artifact_id(2),
            b"AAAA",
            vec![],
            || f.alloc_op(),
        )
        .unwrap();
    // Exact replay succeeds.
    let (desc_a_replay, _) = artifacts
        .put_artifact(
            &mut store,
            &f.control,
            f.ctx,
            put_op,
            f.alloc_prot(),
            "cfg",
            artifact_id(2),
            b"AAAA",
            vec![],
            || f.alloc_op(),
        )
        .unwrap();
    assert_eq!(desc_a_replay, desc_a);
    // Same-length different payload fails with OperationMismatch.
    assert!(matches!(
        artifacts.put_artifact(
            &mut store,
            &f.control,
            f.ctx,
            put_op,
            f.alloc_prot(),
            "cfg",
            artifact_id(2),
            b"BBBB",
            vec![],
            || f.alloc_op(),
        ),
        Err(ArtifactError::Control(ControlError::OperationMismatch))
    ));

    // 3. Cross-operation ID reuse must return OperationMismatch (not fabricated receipt).
    let (stream_id, _) = logs
        .create_log(
            &mut store,
            &f.control,
            f.ctx,
            f.alloc_op(),
            f.alloc_prot(),
            "events",
            log_id(40),
            || f.alloc_op(),
        )
        .unwrap();
    let append_op = f.alloc_op();
    logs.append(
        &mut store,
        &f.control,
        f.ctx,
        "events",
        stream_id,
        0,
        append_op,
        f.alloc_prot(),
        vec![b"e0".to_vec()],
        vec![],
        || f.alloc_op(),
    )
    .unwrap();
    let unrelated_artifact_op = f.alloc_op();
    artifacts
        .put_artifact(
            &mut store,
            &f.control,
            f.ctx,
            unrelated_artifact_op,
            f.alloc_prot(),
            "other",
            artifact_id(3),
            b"xyz",
            vec![],
            || f.alloc_op(),
        )
        .unwrap();
    // Reusing unrelated_artifact_op or op(2) (acquire_writer) as append_id must fail with
    // OperationMismatch.
    assert!(matches!(
        logs.append(
            &mut store,
            &f.control,
            f.ctx,
            "events",
            stream_id,
            0,
            unrelated_artifact_op,
            f.alloc_prot(),
            vec![b"e0".to_vec()],
            vec![],
            || f.alloc_op(),
        ),
        Err(LogError::Control(ControlError::OperationMismatch))
    ));
    assert!(matches!(
        logs.append(
            &mut store,
            &f.control,
            f.ctx,
            "events",
            stream_id,
            0,
            op(2),
            f.alloc_prot(),
            vec![b"e0".to_vec()],
            vec![],
            || f.alloc_op(),
        ),
        Err(LogError::Control(ControlError::OperationMismatch))
    ));
    // Reusing append_op as delete_artifact must fail with OperationMismatch.
    assert!(matches!(
        artifacts.delete_artifact(
            &mut store,
            &f.control,
            f.ctx,
            append_op,
            f.alloc_prot(),
            "absent",
            || f.alloc_op(),
        ),
        Err(ArtifactError::Control(ControlError::OperationMismatch))
    ));

    // 4. Upload failure aborts preparation (no wedged pending_mutation_preparations) and surfaces
    //    ObjectStore error rather than ControlError::CorruptRecord.
    #[derive(Clone)]
    struct FailingPutStore(MemoryObjects);
    impl MapObjectStore for FailingPutStore {
        fn proof_scope(&self) -> ObjectStoreScopeId {
            self.0.proof_scope()
        }

        fn get(&self, id: ObjectId) -> Result<Option<Vec<u8>>, String> {
            self.0.get(id)
        }

        fn put(&mut self, id: ObjectId, bytes: Vec<u8>) -> Result<(), String> {
            self.0.put(id, bytes)
        }
    }
    impl DurableObjectStore for FailingPutStore {
        fn put_durable(&mut self, _reference: ObjectRef, _encoded: &[u8]) -> Result<(), String> {
            Err("simulated S3 outage".to_owned())
        }
    }
    let mut failing_store = FailingPutStore(f.store.clone());
    let err = logs
        .append(
            &mut failing_store,
            &f.control,
            f.ctx,
            "events",
            stream_id,
            1,
            f.alloc_op(),
            f.alloc_prot(),
            vec![b"fail-upload".to_vec()],
            vec![],
            || f.alloc_op(),
        )
        .expect_err("failing put_durable must return ObjectStore error");
    assert!(matches!(err, LogError::ObjectStore(_)));
    assert_eq!(
        f.control
            .get_branch(branch_id(1))
            .unwrap()
            .pending_mutation_preparations,
        0,
        "upload failure must abort active preparation so branch is not wedged with Busy"
    );

    // 5. Missing/unverified attachment or dependency ObjectRef rejected BEFORE commit.
    let missing_blob_bytes =
        versionfs_core::views::encode_artifact_blob(b"never-uploaded").unwrap();
    let missing_ref = ObjectRef::from_encoded(&missing_blob_bytes).unwrap();
    let rev_before = f.control.get_branch(branch_id(1)).unwrap().revision;
    assert!(matches!(
        logs.append(
            &mut store,
            &f.control,
            f.ctx,
            "events",
            stream_id,
            1,
            f.alloc_op(),
            f.alloc_prot(),
            vec![b"dangling".to_vec()],
            vec![missing_ref],
            || f.alloc_op(),
        ),
        Err(LogError::View(
            versionfs_core::views::ViewError::MissingObject(_)
        ))
    ));
    assert!(matches!(
        artifacts.put_artifact(
            &mut store,
            &f.control,
            f.ctx,
            f.alloc_op(),
            f.alloc_prot(),
            "dangling-art",
            artifact_id(99),
            b"ok",
            vec![missing_ref],
            || f.alloc_op(),
        ),
        Err(ArtifactError::View(
            versionfs_core::views::ViewError::MissingObject(_)
        ))
    ));
    assert_eq!(
        f.control.get_branch(branch_id(1)).unwrap().revision,
        rev_before,
        "dangling attachments/dependencies must be rejected before committing"
    );

    // 6. Reusing a committed LogMutation operation_id from another log on a no-op trim_log must
    //    fail with OperationMismatch (and exact trim_log replay must succeed).
    let (other_stream_id, _) = logs
        .create_log(
            &mut store,
            &f.control,
            f.ctx,
            f.alloc_op(),
            f.alloc_prot(),
            "other-events",
            log_id(41),
            || f.alloc_op(),
        )
        .unwrap();
    let other_append_op = f.alloc_op();
    logs.append(
        &mut store,
        &f.control,
        f.ctx,
        "other-events",
        other_stream_id,
        0,
        other_append_op,
        f.alloc_prot(),
        vec![b"other-0".to_vec()],
        vec![],
        || f.alloc_op(),
    )
    .unwrap();
    assert!(matches!(
        logs.trim_log(
            &mut store,
            &f.control,
            f.ctx,
            "events",
            stream_id,
            0,
            other_append_op,
            f.alloc_prot(),
            || f.alloc_op(),
        ),
        Err(LogError::Control(ControlError::OperationMismatch))
    ));
    let trim_op = f.alloc_op();
    logs.trim_log(
        &mut store,
        &f.control,
        f.ctx,
        "events",
        stream_id,
        0,
        trim_op,
        f.alloc_prot(),
        || f.alloc_op(),
    )
    .unwrap();
    // Exact replay of no-op trim_log succeeds.
    logs.trim_log(
        &mut store,
        &f.control,
        f.ctx,
        "events",
        stream_id,
        0,
        trim_op,
        f.alloc_prot(),
        || f.alloc_op(),
    )
    .unwrap();
}
