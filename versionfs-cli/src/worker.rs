//! Block-server worker daemon managing a live `DurableVolume`, clean
//! `WorkspaceObjectCache`, `NativeProducerGate`, `CheckpointCoordinator`,
//! and freeze-independent `AttachmentHeartbeat` over a local control socket.

use serde::{Deserialize, Serialize};
use std::io::{Read, Write};
#[cfg(unix)]
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use versionfs_core::control::{AttachmentState, WriterGrant};
use versionfs_core::format::{CaptureMode, ProvenanceBinding, ProvenanceContext};
use versionfs_core::graph::sanitize_provenance_text;
use versionfs_core::identity::{
    ArtifactId, BranchId, ObjectStoreScopeId, OperationId, PrincipalId, ProtectionId, WorkspaceId,
};
use versionfs_core::rpc::RpcAuth;
use versionfs_sdk::artifacts::NativeProducerGate;
use versionfs_sdk::barrier::{DurableVolume, FileBarrierJournal, VolumeBarrierControl};
use versionfs_sdk::capture::{
    BlockCutOnlyBoundary, CaptureConfig, CheckpointCoordinator, CheckpointRequest,
    FileCaptureJournal, RecoveredCapture,
};
use versionfs_sdk::client::{
    AttachmentHeartbeat, RemoteObjectStore, ServiceClient, ServiceEndpoint,
};
use versionfs_sdk::logs::{NativeAdmissionGate, NativeWriteContext};
use versionfs_sdk::object_cache::{CachedMapStore, WorkspaceObjectCache};
use versionfs_sdk::volume::{BlockDevice, BlockRequest, BlockResponse};

const MAX_WORKER_FRAME: usize = 16 * 1024 * 1024;

/// Optional provenance metadata supplied when requesting a checkpoint.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct WorkerProvenanceInput {
    /// Actor/agent name.
    pub actor: Option<String>,
    /// Execution ID.
    pub execution_id: Option<String>,
    /// Session ID.
    pub session_id: Option<String>,
    /// Thread ID.
    pub thread_id: Option<String>,
    /// Task ID.
    pub task_id: Option<String>,
    /// Workflow operation ID.
    pub workflow_operation_id: Option<String>,
    /// Concise reason.
    pub reason: Option<String>,
    /// Trace ID.
    pub trace_id: Option<String>,
    /// Span ID.
    pub span_id: Option<String>,
    /// Parent span ID.
    pub parent_span_id: Option<String>,
    /// Required log-map root hex (`ObjectRef` serialized or omitted to use branch live logs_root).
    pub required_logs_root: Option<String>,
    /// Required artifact-map root hex (or omitted to use branch live artifacts_root).
    pub required_artifacts_root: Option<String>,
    /// Descriptive optional external links (never fetched).
    #[serde(default)]
    pub optional_links: Vec<String>,
    /// Opt-in raw conversational payload note (sanitized before ingestion; disabled by default).
    #[serde(default)]
    pub include_raw_payload: bool,
    /// Optional raw payload text (ignored unless `include_raw_payload` is true).
    pub raw_payload: Option<String>,
}

/// Requests accepted by the worker block-server control socket.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum WorkerRequest {
    /// Writes aligned bytes to the worker-local volatile dirty overlay.
    Write {
        /// Byte offset.
        offset: u64,
        /// Hex-encoded payload bytes.
        data_hex: String,
    },
    /// Reads aligned bytes from the dirty overlay or backing store/cache.
    Read {
        /// Byte offset.
        offset: u64,
        /// Byte length.
        length: u64,
    },
    /// Flushes dirty extents to a durable live `VolumeRoot` on the authority.
    Flush,
    /// Stages a named artifact put in `NativeProducerGate`.
    StageArtifact {
        /// Artifact name.
        name: String,
        /// Explicit 16-byte artifact ID hex.
        artifact_id_hex: String,
        /// Hex-encoded payload bytes.
        payload_hex: String,
    },
    /// Stages a named artifact removal in `NativeProducerGate`.
    StageRemoveArtifact {
        /// Artifact name.
        name: String,
    },
    /// Runs a common-cut checkpoint through `CheckpointCoordinator`.
    Checkpoint {
        /// `block_cut`, `filesystem_frozen`, or `filesystem_clean_unmount`.
        mode: String,
        /// Whether to advance branch head.
        advance_head: bool,
        /// Informational Unix timestamp in ms.
        created_at_unix_ms: u64,
        /// Optional provenance metadata.
        provenance: Option<Box<WorkerProvenanceInput>>,
    },
    /// Recovers any pending local capture journal on this worker.
    RecoverCapture,
    /// Reconciles any pending local barrier journal on this worker.
    ReconcileBarrier,
    /// Returns live worker status.
    Status,
    /// Cleanly detaches the volume, releases the writer grant, and stops the worker.
    Detach,
}

/// Responses returned by the worker block-server control socket.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum WorkerResponse {
    /// Explicit local volatile write acknowledgement.
    WriteAck {
        /// Bytes accepted locally.
        bytes_written: usize,
        /// Always `true`: normal writes are local volatile state only.
        local_volatile_only: bool,
    },
    /// Read data bytes.
    ReadData {
        /// Hex-encoded bytes.
        data_hex: String,
    },
    /// Explicit durable FLUSH/FUA acknowledgement.
    FlushAck {
        /// Branch ID.
        branch_id: String,
        /// Committed branch revision.
        revision: u64,
        /// Committed live `VolumeRoot` ID.
        volume_root: String,
        /// Always `true`: durable live volume boundary, not a Generation checkpoint.
        durable_live_volume: bool,
    },
    /// Explicit published Generation acknowledgement.
    CheckpointAck {
        /// Published Generation ID.
        generation_id: String,
        /// Branch ID.
        branch_id: String,
        /// Branch revision.
        revision: u64,
        /// Established capture mode.
        capture_mode: String,
        /// Bound provenance status (`recorded` or `unattributed`).
        provenance_status: String,
        /// Whether branch head advanced.
        head_advanced: bool,
        /// Always `true`: retained Generation binding volume/log/artifact views.
        published_generation: bool,
    },
    /// Generic success with message.
    Ok {
        /// Outcome description.
        message: String,
    },
    /// Worker status snapshot.
    Status {
        /// Attached branch ID.
        branch_id: String,
        /// Dirty extent count in memory.
        dirty_extents: usize,
        /// Whether the attachment heartbeat observed fencing.
        fenced: bool,
        /// Active attachment ID.
        attachment_id: String,
        /// Current volume epoch.
        volume_epoch: u64,
    },
    /// Error returned by the worker.
    Error {
        /// Error message.
        message: String,
    },
}

/// Configuration for running [`run_worker_server`].
#[derive(Clone, Debug)]
pub struct WorkerConfig {
    /// Service endpoint (`tcp://...` or `unix://...`).
    pub endpoint: ServiceEndpoint,
    /// Authenticated caller scope.
    pub auth: RpcAuth,
    /// Attached branch ID.
    pub branch_id: BranchId,
    /// Active writer grant.
    pub grant: WriterGrant,
    /// Active attachment state.
    pub attachment: AttachmentState,
    /// Clean object cache directory.
    pub cache_dir: PathBuf,
    /// Private barrier journal directory.
    pub journal_dir: PathBuf,
    /// Private capture journal directory.
    pub capture_dir: PathBuf,
    /// Clean object cache capacity in bytes.
    pub cache_bytes: u64,
    /// Dirty overlay capacity in bytes.
    pub dirty_bytes: usize,
    /// Local Unix control socket path.
    pub control_socket: PathBuf,
    /// Heartbeat renewal interval.
    pub heartbeat_interval: Duration,
}

/// Runs the block-server worker process until `Detach` or shutdown.
#[cfg(unix)]
pub fn run_worker_server(config: WorkerConfig) -> Result<(), String> {
    let client = ServiceClient::connect(config.endpoint.clone(), config.auth.clone())
        .map_err(|e| e.to_string())?;
    client.set_active_protection(Some(config.attachment.protection_id));

    let scope = client.object_store_scope();
    let cache = WorkspaceObjectCache::open(
        &config.cache_dir,
        config.auth.workspace_id,
        scope,
        config.cache_bytes,
    )
    .map_err(|e| e.to_string())?;
    let cached_store =
        CachedMapStore::new(client.remote_object_store(), cache).map_err(|e| e.to_string())?;

    let barrier_journal = FileBarrierJournal::open(&config.journal_dir)?;
    let capture_journal = FileCaptureJournal::open(&config.capture_dir)?;

    let mut volume = DurableVolume::open(
        cached_store,
        client.clone(),
        barrier_journal,
        config.branch_id,
        config.grant,
        config.attachment.attachment_id,
        config.dirty_bytes,
    )
    .map_err(|e| e.to_string())?;

    let admission = NativeAdmissionGate::new();
    let mut producer_gate = NativeProducerGate::new(
        admission,
        client.remote_object_store(),
        client.clone(),
        NativeWriteContext {
            branch_id: config.branch_id,
            attachment_id: config.attachment.attachment_id,
            volume_epoch: config.attachment.volume_epoch,
            grant: config.grant,
        },
        || (alloc_op_id(), alloc_prot_id()),
    );
    let mut coordinator = CheckpointCoordinator::new(capture_journal, CaptureConfig::default());

    let (stop_tx, stop_rx) = std::sync::mpsc::channel();
    let mut heartbeat = AttachmentHeartbeat::start(
        client.clone(),
        config.branch_id,
        config.grant,
        config.attachment,
        config.heartbeat_interval,
        Some(stop_tx),
        alloc_op_id,
    );

    if config.control_socket.exists() {
        let _ = std::fs::remove_file(&config.control_socket);
    }
    if let Some(parent) = config.control_socket.parent() {
        std::fs::create_dir_all(parent).map_err(|e| format!("create socket parent: {e}"))?;
    }
    let listener = UnixListener::bind(&config.control_socket)
        .map_err(|e| format!("bind {}: {e}", config.control_socket.display()))?;
    listener
        .set_nonblocking(true)
        .map_err(|e| format!("set_nonblocking: {e}"))?;

    println!("READY {}", config.control_socket.display());

    let done = Arc::new(AtomicBool::new(false));
    while !done.load(Ordering::Acquire) {
        if stop_rx.try_recv().is_ok() {
            break;
        }
        match listener.accept() {
            Ok((mut stream, _)) => {
                let _ = stream.set_nonblocking(false);
                let _ = stream.set_read_timeout(Some(Duration::from_secs(10)));
                let _ = stream.set_write_timeout(Some(Duration::from_secs(10)));
                let req_bytes = match read_worker_frame(&mut stream) {
                    Ok(b) => b,
                    Err(_) => continue,
                };
                let req: WorkerRequest = match serde_json::from_slice(&req_bytes) {
                    Ok(r) => r,
                    Err(e) => {
                        let _ = send_worker_response(
                            &mut stream,
                            &WorkerResponse::Error {
                                message: format!("invalid worker request: {e}"),
                            },
                        );
                        continue;
                    }
                };
                let mut should_exit = false;
                let resp = handle_worker_request(
                    req,
                    &client,
                    &config,
                    &heartbeat,
                    &mut volume,
                    &mut producer_gate,
                    &mut coordinator,
                    &mut should_exit,
                );
                let _ = send_worker_response(&mut stream, &resp);
                if should_exit {
                    done.store(true, Ordering::Release);
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(20));
            }
            Err(e) => {
                heartbeat.stop();
                return Err(format!("accept failed: {e}"));
            }
        }
    }

    heartbeat.stop();
    let _ = std::fs::remove_file(&config.control_socket);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn handle_worker_request(
    req: WorkerRequest,
    client: &ServiceClient,
    config: &WorkerConfig,
    heartbeat: &AttachmentHeartbeat,
    volume: &mut DurableVolume<
        CachedMapStore<RemoteObjectStore>,
        ServiceClient,
        FileBarrierJournal,
    >,
    producer_gate: &mut NativeProducerGate<
        RemoteObjectStore,
        ServiceClient,
        impl FnMut() -> (OperationId, ProtectionId),
    >,
    coordinator: &mut CheckpointCoordinator<FileCaptureJournal>,
    should_exit: &mut bool,
) -> WorkerResponse {
    let current_grant = heartbeat.current_grant();
    volume.set_writer_grant(current_grant);
    producer_gate.set_writer_grant(current_grant);
    match req {
        WorkerRequest::Write { offset, data_hex } => {
            let data = match decode_hex_bytes(&data_hex) {
                Ok(b) => b,
                Err(message) => return WorkerResponse::Error { message },
            };
            match volume.dispatch(BlockRequest::Write {
                offset,
                data: &data,
                fua: false,
            }) {
                Ok(BlockResponse::Written(bytes_written)) => WorkerResponse::WriteAck {
                    bytes_written,
                    local_volatile_only: true,
                },
                Ok(_) => WorkerResponse::Error {
                    message: "unexpected response from write".to_owned(),
                },
                Err(e) => WorkerResponse::Error {
                    message: e.to_string(),
                },
            }
        }
        WorkerRequest::Read { offset, length } => {
            match volume.dispatch(BlockRequest::Read { offset, length }) {
                Ok(BlockResponse::Read(bytes)) => WorkerResponse::ReadData {
                    data_hex: encode_hex_bytes(&bytes),
                },
                Ok(_) => WorkerResponse::Error {
                    message: "unexpected response from read".to_owned(),
                },
                Err(e) => WorkerResponse::Error {
                    message: e.to_string(),
                },
            }
        }
        WorkerRequest::Flush => match volume.dispatch(BlockRequest::Flush) {
            Ok(_) => match client.get_branch(config.branch_id) {
                Ok(branch) => WorkerResponse::FlushAck {
                    branch_id: branch.branch_id.to_string(),
                    revision: branch.revision,
                    volume_root: branch.volume_root.object_id.to_string(),
                    durable_live_volume: true,
                },
                Err(e) => WorkerResponse::Error {
                    message: e.to_string(),
                },
            },
            Err(e) => WorkerResponse::Error {
                message: e.to_string(),
            },
        },
        WorkerRequest::StageArtifact {
            name,
            artifact_id_hex,
            payload_hex,
        } => {
            let id_arr: [u8; 16] = match decode_hex_16(&artifact_id_hex) {
                Ok(a) => a,
                Err(message) => return WorkerResponse::Error { message },
            };
            let payload = match decode_hex_bytes(&payload_hex) {
                Ok(b) => b,
                Err(message) => return WorkerResponse::Error { message },
            };
            match producer_gate.stage_put(name, ArtifactId::from_bytes(id_arr), payload, vec![]) {
                Ok(()) => WorkerResponse::Ok {
                    message: "staged artifact".to_owned(),
                },
                Err(e) => WorkerResponse::Error {
                    message: e.to_string(),
                },
            }
        }
        WorkerRequest::StageRemoveArtifact { name } => match producer_gate.stage_remove(name) {
            Ok(()) => WorkerResponse::Ok {
                message: "staged artifact removal".to_owned(),
            },
            Err(e) => WorkerResponse::Error {
                message: e.to_string(),
            },
        },
        WorkerRequest::Checkpoint {
            mode,
            advance_head,
            created_at_unix_ms,
            provenance,
        } => {
            let capture_mode = match CaptureMode::from_name(&mode) {
                Ok(m) => m,
                Err(e) => {
                    return WorkerResponse::Error {
                        message: e.to_string(),
                    };
                }
            };
            let cur_grant = heartbeat.current_grant();
            let provenance_binding = match provenance {
                None => ProvenanceBinding::Unattributed,
                Some(input) => {
                    match build_and_upload_provenance(client, config.branch_id, cur_grant, &input) {
                        Ok(b) => b,
                        Err(message) => return WorkerResponse::Error { message },
                    }
                }
            };
            let provenance_status = match provenance_binding {
                ProvenanceBinding::Recorded { .. } => "recorded",
                ProvenanceBinding::Unattributed => "unattributed",
            }
            .to_owned();
            let mut boundary = BlockCutOnlyBoundary;
            let mut request = if advance_head {
                CheckpointRequest::attached(capture_mode, created_at_unix_ms)
            } else {
                CheckpointRequest::detached(capture_mode, created_at_unix_ms)
            };
            request.provenance = provenance_binding;
            match coordinator.checkpoint(volume, producer_gate, &mut boundary, &request, &[]) {
                Ok(published) => WorkerResponse::CheckpointAck {
                    generation_id: published.generation_id.to_string(),
                    branch_id: published.branch.branch_id.to_string(),
                    revision: published.branch.revision,
                    capture_mode: capture_mode.as_str().to_owned(),
                    provenance_status,
                    head_advanced: published.head_advanced,
                    published_generation: true,
                },
                Err(e) => WorkerResponse::Error {
                    message: e.to_string(),
                },
            }
        }
        WorkerRequest::RecoverCapture => {
            let mut boundary = BlockCutOnlyBoundary;
            match coordinator.recover_pending_capture(volume, producer_gate, &mut boundary) {
                Ok(RecoveredCapture::None) => WorkerResponse::Ok {
                    message: "no pending capture".to_owned(),
                },
                Ok(RecoveredCapture::Published(pub_gen)) => WorkerResponse::Ok {
                    message: format!("recovered published generation {}", pub_gen.generation_id),
                },
                Ok(RecoveredCapture::Aborted { freeze_thawed, .. }) => WorkerResponse::Ok {
                    message: format!("aborted pending capture (thawed={freeze_thawed})"),
                },
                Err(e) => WorkerResponse::Error {
                    message: e.to_string(),
                },
            }
        }
        WorkerRequest::ReconcileBarrier => match volume.reconcile_pending_outcome() {
            Ok(()) => WorkerResponse::Ok {
                message: "reconciled pending barrier".to_owned(),
            },
            Err(e) => WorkerResponse::Error {
                message: e.to_string(),
            },
        },
        WorkerRequest::Status => {
            let att = heartbeat.current_attachment();
            WorkerResponse::Status {
                branch_id: config.branch_id.to_string(),
                dirty_extents: volume.dirty_extent_count(),
                fenced: heartbeat.is_fenced(),
                attachment_id: att.attachment_id.to_string(),
                volume_epoch: att.volume_epoch,
            }
        }
        WorkerRequest::Detach => {
            if let Err(e) = volume.flush() {
                return WorkerResponse::Error {
                    message: format!("flush before detach failed: {e}"),
                };
            }
            let cur_grant = heartbeat.current_grant();
            let cur_att = heartbeat.current_attachment();
            if let Err(e) = client.detach_volume(
                alloc_op_id(),
                config.branch_id,
                cur_att.attachment_id,
                cur_att.volume_epoch,
                cur_att.protection_id,
                cur_grant,
            ) {
                return WorkerResponse::Error {
                    message: e.to_string(),
                };
            }
            if let Err(e) = client.release_writer(alloc_op_id(), cur_grant) {
                return WorkerResponse::Error {
                    message: e.to_string(),
                };
            }
            *should_exit = true;
            WorkerResponse::Ok {
                message: "detached and released writer".to_owned(),
            }
        }
    }
}

fn build_and_upload_provenance(
    client: &ServiceClient,
    branch_id: BranchId,
    grant: WriterGrant,
    input: &WorkerProvenanceInput,
) -> Result<ProvenanceBinding, String> {
    let branch = client.get_branch(branch_id).map_err(|e| e.to_string())?;
    let reason = if let Some(r) = &input.reason {
        Some(sanitize_provenance_text(r, 4096).map_err(|e| e.to_string())?)
    } else if input.include_raw_payload {
        match &input.raw_payload {
            Some(raw) => Some(sanitize_provenance_text(raw, 4096).map_err(|e| e.to_string())?),
            None => None,
        }
    } else {
        None
    };
    let mut optional_links = Vec::with_capacity(input.optional_links.len());
    for link in &input.optional_links {
        optional_links.push(sanitize_provenance_text(link, 4096).map_err(|e| e.to_string())?);
    }
    optional_links.sort();
    optional_links.dedup();

    let ctx = ProvenanceContext {
        recording_principal: client.auth().principal_id,
        actor: sanitize_opt(input.actor.as_deref())?,
        execution_id: sanitize_opt(input.execution_id.as_deref())?,
        session_id: sanitize_opt(input.session_id.as_deref())?,
        thread_id: sanitize_opt(input.thread_id.as_deref())?,
        task_id: sanitize_opt(input.task_id.as_deref())?,
        workflow_operation_id: sanitize_opt(input.workflow_operation_id.as_deref())?,
        reason,
        trace_id: sanitize_opt(input.trace_id.as_deref())?,
        span_id: sanitize_opt(input.span_id.as_deref())?,
        parent_span_id: sanitize_opt(input.parent_span_id.as_deref())?,
        required_logs_root: branch.logs_root,
        required_artifacts_root: branch.artifacts_root,
        optional_links,
    };
    let encoded = ctx.encode().map_err(|e| e.to_string())?;
    let ctx_ref = client
        .upload_provenance_context(branch_id, grant, encoded)
        .map_err(|e| e.to_string())?;
    Ok(ProvenanceBinding::Recorded {
        context_root: ctx_ref,
    })
}

fn sanitize_opt(val: Option<&str>) -> Result<Option<String>, String> {
    val.map(|v| sanitize_provenance_text(v, 4096).map_err(|e| e.to_string()))
        .transpose()
}

/// Sends one [`WorkerRequest`] to a running worker's local control socket.
#[cfg(unix)]
pub fn send_worker_command(
    control_socket: &Path,
    request: &WorkerRequest,
) -> Result<WorkerResponse, String> {
    let mut stream = UnixStream::connect(control_socket)
        .map_err(|e| format!("connect worker socket {}: {e}", control_socket.display()))?;
    let encoded = serde_json::to_vec(request).map_err(|e| e.to_string())?;
    write_worker_frame(&mut stream, &encoded)?;
    let resp_bytes = read_worker_frame(&mut stream)?;
    serde_json::from_slice(&resp_bytes).map_err(|e| e.to_string())
}

fn send_worker_response<W: Write>(writer: &mut W, resp: &WorkerResponse) -> Result<(), String> {
    let encoded = serde_json::to_vec(resp).map_err(|e| e.to_string())?;
    write_worker_frame(writer, &encoded)
}

fn write_worker_frame<W: Write>(writer: &mut W, payload: &[u8]) -> Result<(), String> {
    if payload.len() > MAX_WORKER_FRAME {
        return Err("worker frame exceeds maximum".to_owned());
    }
    let len = u32::try_from(payload.len()).map_err(|_| "worker frame too large".to_owned())?;
    writer
        .write_all(&len.to_le_bytes())
        .map_err(|e| e.to_string())?;
    writer.write_all(payload).map_err(|e| e.to_string())?;
    writer.flush().map_err(|e| e.to_string())?;
    Ok(())
}

fn read_worker_frame<R: Read>(reader: &mut R) -> Result<Vec<u8>, String> {
    let mut len_buf = [0_u8; 4];
    reader.read_exact(&mut len_buf).map_err(|e| e.to_string())?;
    let len = u32::from_le_bytes(len_buf) as usize;
    if len > MAX_WORKER_FRAME {
        return Err("worker frame exceeds maximum".to_owned());
    }
    let mut payload = vec![0_u8; len];
    reader.read_exact(&mut payload).map_err(|e| e.to_string())?;
    Ok(payload)
}

static NEXT_LOCAL_SEQ: AtomicU64 = AtomicU64::new(1);

fn unique_16(tag: u8) -> [u8; 16] {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(1, |d| d.as_nanos()) as u64;
    let seq = NEXT_LOCAL_SEQ.fetch_add(1, Ordering::Relaxed);
    let mut out = [0_u8; 16];
    out[0..8].copy_from_slice(&nanos.to_le_bytes());
    out[8..12].copy_from_slice(&std::process::id().to_le_bytes());
    out[12..15].copy_from_slice(&seq.to_le_bytes()[..3]);
    out[15] = tag;
    out
}

/// Allocates a unique operation ID.
pub fn alloc_op_id() -> OperationId {
    OperationId::from_bytes(unique_16(0xA1))
}

/// Allocates a unique protection ID.
pub fn alloc_prot_id() -> ProtectionId {
    ProtectionId::from_bytes(unique_16(0xB2))
}

/// Decodes a hex-encoded 16-byte ID.
pub fn decode_hex_16(hex: &str) -> Result<[u8; 16], String> {
    let hex = hex.strip_prefix("0x").unwrap_or(hex);
    if hex.len() != 32 {
        return Err(format!("expected 32 hex chars, got {}", hex.len()));
    }
    let mut out = [0_u8; 16];
    for i in 0..16 {
        out[i] = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16)
            .map_err(|e| format!("invalid hex byte: {e}"))?;
    }
    Ok(out)
}

/// Decodes arbitrary hex bytes.
pub fn decode_hex_bytes(hex: &str) -> Result<Vec<u8>, String> {
    let hex = hex.strip_prefix("0x").unwrap_or(hex);
    if !hex.len().is_multiple_of(2) {
        return Err("hex string must have even length".to_owned());
    }
    let mut out = Vec::with_capacity(hex.len() / 2);
    for i in 0..(hex.len() / 2) {
        out.push(
            u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16)
                .map_err(|e| format!("invalid hex byte: {e}"))?,
        );
    }
    Ok(out)
}

/// Encodes bytes as lowercase hex.
pub fn encode_hex_bytes(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        s.push(char::from(HEX[(b >> 4) as usize]));
        s.push(char::from(HEX[(b & 0x0f) as usize]));
    }
    s
}

/// Parses a `WorkspaceId` from 32-char hex.
pub fn parse_workspace_id(hex: &str) -> Result<WorkspaceId, String> {
    Ok(WorkspaceId::from_bytes(decode_hex_16(hex)?))
}

/// Parses a `PrincipalId` from 32-char hex.
pub fn parse_principal_id(hex: &str) -> Result<PrincipalId, String> {
    Ok(PrincipalId::from_bytes(decode_hex_16(hex)?))
}

/// Parses an `ObjectStoreScopeId` from 32-char hex.
pub fn parse_scope_id(hex: &str) -> Result<ObjectStoreScopeId, String> {
    Ok(ObjectStoreScopeId::from_bytes(decode_hex_16(hex)?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_and_id_parsing_and_worker_framing_round_trip() {
        let raw = [0xAB_u8; 16];
        let hex = encode_hex_bytes(&raw);
        assert_eq!(decode_hex_16(&hex).unwrap(), raw);
        assert_eq!(decode_hex_bytes(&hex).unwrap(), raw);
        assert!(decode_hex_16("abcd").is_err());
        assert!(decode_hex_bytes("abc").is_err());
        assert!(decode_hex_bytes("zzzz").is_err());

        let ws = parse_workspace_id(&hex).unwrap();
        assert_eq!(ws, WorkspaceId::from_bytes(raw));
        let principal = parse_principal_id(&hex).unwrap();
        assert_eq!(principal, PrincipalId::from_bytes(raw));
        assert_eq!(
            parse_scope_id(&hex).unwrap(),
            ObjectStoreScopeId::from_bytes(raw)
        );

        assert_eq!(sanitize_opt(None).unwrap(), None);
        assert!(sanitize_opt(Some("  ")).is_err());
        let sanitized = sanitize_opt(Some("Bearer sk-secret123456\x07"))
            .unwrap()
            .unwrap();
        assert!(!sanitized.contains("sk-secret123456"));

        let mut buf = Vec::new();
        let req = WorkerRequest::Status;
        write_worker_frame(&mut buf, &serde_json::to_vec(&req).unwrap()).unwrap();
        let mut cursor = std::io::Cursor::new(buf);
        let read_req: WorkerRequest =
            serde_json::from_slice(&read_worker_frame(&mut cursor).unwrap()).unwrap();
        assert!(matches!(read_req, WorkerRequest::Status));
    }
}
