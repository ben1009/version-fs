//! `versionfs` CLI composing device attach/detach, explicit ext4 helpers,
//! checkpoints, branch lifecycle, native logs/artifacts, pins, provenance
//! inspection, structural diff, and operation recovery.

use clap::{Parser, Subcommand};
use serde_json::{Value, json};
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use versionfs_cli::worker::{
    WorkerConfig, WorkerProvenanceInput, WorkerRequest, WorkerResponse, alloc_op_id, alloc_prot_id,
    decode_hex_16, decode_hex_bytes, encode_hex_bytes, parse_principal_id, parse_workspace_id,
    run_worker_server, send_worker_command,
};
use versionfs_core::control::{AttachmentState, WriterGrant};
use versionfs_core::format::{GenerationDescriptorFormat, ProvenanceStatus, VolumeGeometry};
use versionfs_core::identity::{
    ArtifactId, AttachmentId, BranchId, GenerationId, LogId, OperationId, PinId, ProtectionId,
    VolumeBoundaryId,
};
use versionfs_core::rpc::RpcAuth;
use versionfs_sdk::artifacts::ArtifactClient;
use versionfs_sdk::barrier::VolumeBarrierControl;
use versionfs_sdk::client::{ServiceClient, ServiceEndpoint};
use versionfs_sdk::linux_fs::{format_ext4, mount_ext4, unmount_ext4};
use versionfs_sdk::logs::{LiveLogCursor, LogClient, NativeControl, NativeWriteContext, StreamId};

#[derive(Parser)]
#[command(name = "versionfs", about = "Version FS CLI")]
struct Cli {
    #[arg(long, default_value = "tcp://127.0.0.1:7443")]
    endpoint: String,
    #[arg(long)]
    workspace_id: String,
    #[arg(long)]
    principal_id: String,
    #[arg(long, default_value = "versionfs-dev-token")]
    auth_token: String,
    #[arg(long, default_value_t = false)]
    json: bool,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Workspace {
        #[command(subcommand)]
        sub: WorkspaceCmd,
    },
    Branch {
        #[command(subcommand)]
        sub: BranchCmd,
    },
    Writer {
        #[command(subcommand)]
        sub: WriterCmd,
    },
    Volume {
        #[command(subcommand)]
        sub: VolumeCmd,
    },
    Fs {
        #[command(subcommand)]
        sub: FsCmd,
    },
    Checkpoint {
        #[command(subcommand)]
        sub: CheckpointCmd,
    },
    Generation {
        #[command(subcommand)]
        sub: GenerationCmd,
    },
    Log {
        #[command(subcommand)]
        sub: LogCmd,
    },
    Artifact {
        #[command(subcommand)]
        sub: ArtifactCmd,
    },
    Op {
        #[command(subcommand)]
        sub: OpCmd,
    },
    Report {
        #[command(subcommand)]
        sub: ReportCmd,
    },
}

#[derive(Subcommand)]
enum WorkspaceCmd {
    Create {
        #[arg(long)]
        capacity_bytes: u64,
        #[arg(long, default_value = "main")]
        name: String,
        #[arg(long, default_value_t = 86_400_000)]
        retention_ms: u64,
        #[arg(long)]
        branch_id: Option<String>,
        #[arg(long)]
        generation_id: Option<String>,
        #[arg(long)]
        attachment_id: Option<String>,
    },
}

#[derive(Subcommand)]
enum BranchCmd {
    Create {
        #[arg(long)]
        name: String,
        #[arg(long)]
        base_generation: String,
        #[arg(long)]
        branch_id: Option<String>,
    },
    Get {
        #[arg(long)]
        branch_id: String,
    },
    ByName {
        #[arg(long)]
        name: String,
    },
    MoveHead {
        #[arg(long)]
        branch_id: String,
        #[arg(long)]
        expected_revision: u64,
        #[arg(long)]
        target_generation: String,
        #[arg(long, default_value_t = false)]
        discard_live_state: bool,
    },
    Promote {
        #[arg(long)]
        source_branch_id: String,
        #[arg(long)]
        expected_source_revision: u64,
        #[arg(long)]
        destination_branch_id: String,
        #[arg(long)]
        expected_destination_revision: u64,
        #[arg(long, default_value_t = false)]
        discard_live_state: bool,
    },
    Delete {
        #[arg(long)]
        branch_id: String,
        #[arg(long)]
        expected_revision: u64,
        #[arg(long, default_value_t = false)]
        discard_live_state: bool,
    },
}

#[derive(Subcommand)]
enum WriterCmd {
    Acquire {
        #[arg(long)]
        branch_id: String,
        #[arg(long)]
        expected_revision: u64,
        #[arg(long)]
        expected_writer_epoch: u64,
        #[arg(long, default_value_t = false)]
        force: bool,
    },
    Renew {
        #[arg(long)]
        branch_id: String,
    },
    Release {
        #[arg(long)]
        branch_id: String,
    },
}

#[derive(Subcommand)]
enum VolumeCmd {
    Attach {
        #[arg(long)]
        branch_id: String,
        #[arg(long)]
        expected_revision: u64,
        #[arg(long)]
        attachment_id: Option<String>,
    },
    RenewBase {
        #[arg(long)]
        branch_id: String,
    },
    Detach {
        #[arg(long)]
        control_socket: Option<PathBuf>,
        #[arg(long)]
        branch_id: Option<String>,
    },
    Serve {
        #[arg(long)]
        branch_id: String,
        #[arg(long)]
        cache_dir: PathBuf,
        #[arg(long)]
        journal_dir: PathBuf,
        #[arg(long)]
        capture_dir: PathBuf,
        #[arg(long)]
        control_socket: PathBuf,
        #[arg(long, default_value_t = 64 * 1024 * 1024)]
        cache_bytes: u64,
        #[arg(long, default_value_t = 16 * 1024 * 1024)]
        dirty_bytes: usize,
        #[arg(long, default_value_t = 15_000)]
        heartbeat_ms: u64,
    },
    Write {
        #[arg(long)]
        control_socket: PathBuf,
        #[arg(long)]
        offset: u64,
        #[arg(long)]
        data_hex: String,
    },
    Read {
        #[arg(long)]
        control_socket: PathBuf,
        #[arg(long)]
        offset: u64,
        #[arg(long)]
        length: u64,
    },
    Flush {
        #[arg(long)]
        control_socket: PathBuf,
    },
    ReadBlocks {
        #[arg(long)]
        generation_id: String,
        #[arg(long)]
        offset: u64,
        #[arg(long)]
        length: u64,
    },
    Status {
        #[arg(long)]
        control_socket: PathBuf,
    },
    ReconcileBarrier {
        #[arg(long)]
        control_socket: PathBuf,
    },
}

#[derive(Subcommand)]
enum FsCmd {
    Format {
        #[arg(long)]
        device: PathBuf,
        #[arg(long, default_value_t = false)]
        yes_format_destructive: bool,
    },
    Mount {
        #[arg(long)]
        device: PathBuf,
        #[arg(long)]
        mount_point: PathBuf,
    },
    Unmount {
        #[arg(long)]
        mount_point: PathBuf,
    },
}

#[derive(Subcommand)]
#[allow(clippy::large_enum_variant)]
enum CheckpointCmd {
    Create {
        #[arg(long)]
        control_socket: PathBuf,
        #[arg(long, default_value = "block_cut")]
        mode: String,
        #[arg(long, default_value_t = false)]
        detached: bool,
        #[arg(long)]
        actor: Option<String>,
        #[arg(long)]
        execution_id: Option<String>,
        #[arg(long)]
        session_id: Option<String>,
        #[arg(long)]
        thread_id: Option<String>,
        #[arg(long)]
        task_id: Option<String>,
        #[arg(long)]
        workflow_operation_id: Option<String>,
        #[arg(long)]
        reason: Option<String>,
        #[arg(long)]
        trace_id: Option<String>,
        #[arg(long)]
        span_id: Option<String>,
        #[arg(long)]
        parent_span_id: Option<String>,
        #[arg(long)]
        optional_link: Vec<String>,
        #[arg(long, default_value_t = false)]
        include_raw_payload: bool,
        #[arg(long)]
        raw_payload: Option<String>,
    },
    Recover {
        #[arg(long)]
        control_socket: PathBuf,
    },
}

#[derive(Subcommand)]
enum GenerationCmd {
    Open {
        #[arg(long)]
        generation_id: String,
        #[arg(long)]
        protection_id: Option<String>,
    },
    Renew {
        #[arg(long)]
        protection_id: String,
    },
    Close {
        #[arg(long)]
        protection_id: String,
    },
    Pin {
        #[arg(long)]
        generation_id: String,
        #[arg(long)]
        pin_id: Option<String>,
    },
    Unpin {
        #[arg(long)]
        pin_id: String,
    },
    InspectProvenance {
        #[arg(long)]
        generation_id: String,
    },
    Diff {
        #[arg(long)]
        left_generation_id: String,
        #[arg(long)]
        right_generation_id: String,
    },
}

#[derive(Subcommand)]
enum LogCmd {
    Create {
        #[arg(long)]
        branch_id: String,
        #[arg(long)]
        name: String,
        #[arg(long)]
        log_id: Option<String>,
        #[arg(long)]
        operation_id: Option<String>,
    },
    Append {
        #[arg(long)]
        branch_id: String,
        #[arg(long)]
        name: String,
        #[arg(long)]
        stream_epoch: u64,
        #[arg(long)]
        log_id: String,
        #[arg(long)]
        match_seq: u64,
        #[arg(long)]
        record: Vec<String>,
        #[arg(long)]
        operation_id: Option<String>,
    },
    Trim {
        #[arg(long)]
        branch_id: String,
        #[arg(long)]
        name: String,
        #[arg(long)]
        stream_epoch: u64,
        #[arg(long)]
        log_id: String,
        #[arg(long)]
        trim_to_seq: u64,
        #[arg(long)]
        operation_id: Option<String>,
    },
    Delete {
        #[arg(long)]
        branch_id: String,
        #[arg(long)]
        name: String,
        #[arg(long)]
        stream_epoch: u64,
        #[arg(long)]
        log_id: String,
        #[arg(long)]
        operation_id: Option<String>,
    },
    Read {
        #[arg(long)]
        generation_id: String,
        #[arg(long)]
        name: String,
        #[arg(long, default_value_t = 0)]
        from_seq: u64,
        #[arg(long, default_value_t = 64)]
        max_batches: usize,
    },
    Tail {
        #[arg(long)]
        branch_id: String,
        #[arg(long)]
        name: String,
        #[arg(long)]
        stream_epoch: u64,
        #[arg(long)]
        log_id: String,
        #[arg(long, default_value_t = 0)]
        from_seq: u64,
        #[arg(long, default_value_t = 64)]
        max_batches: usize,
    },
    List {
        #[arg(long)]
        generation_id: String,
    },
}

#[derive(Subcommand)]
enum ArtifactCmd {
    Put {
        #[arg(long)]
        branch_id: String,
        #[arg(long)]
        name: String,
        #[arg(long)]
        artifact_id: Option<String>,
        #[arg(long)]
        payload_hex: String,
        #[arg(long)]
        operation_id: Option<String>,
    },
    Remove {
        #[arg(long)]
        branch_id: String,
        #[arg(long)]
        name: String,
        #[arg(long)]
        operation_id: Option<String>,
    },
    StagePut {
        #[arg(long)]
        control_socket: PathBuf,
        #[arg(long)]
        name: String,
        #[arg(long)]
        artifact_id: Option<String>,
        #[arg(long)]
        payload_hex: String,
    },
    StageRemove {
        #[arg(long)]
        control_socket: PathBuf,
        #[arg(long)]
        name: String,
    },
    Get {
        #[arg(long)]
        generation_id: String,
        #[arg(long)]
        name: String,
        #[arg(long, default_value_t = 0)]
        offset: u64,
        #[arg(long, default_value_t = 4 * 1024 * 1024)]
        length: u64,
    },
    List {
        #[arg(long)]
        generation_id: String,
    },
}

#[derive(Subcommand)]
enum OpCmd {
    Status {
        #[arg(long)]
        operation_id: String,
    },
    Abort {
        #[arg(long)]
        operation_id: String,
    },
    Expire {
        #[arg(long)]
        operation_id: String,
    },
}

#[derive(Subcommand)]
enum ReportCmd {
    Collect,
    Get,
}

fn main() {
    let cli = Cli::parse();
    let json_mode = cli.json;
    match run(cli) {
        Ok(val) => {
            println!("{}", serde_json::to_string_pretty(&val).unwrap());
        }
        Err(err) => {
            if json_mode {
                eprintln!("{}", json!({ "error": err }));
            } else {
                eprintln!("error: {err}");
            }
            std::process::exit(1);
        }
    }
}

fn run(cli: Cli) -> Result<Value, String> {
    let workspace_id = parse_workspace_id(&cli.workspace_id)?;
    let principal_id = parse_principal_id(&cli.principal_id)?;
    let endpoint = ServiceEndpoint::parse(&cli.endpoint).map_err(|e| e.to_string())?;
    let auth = RpcAuth {
        workspace_id,
        principal_id,
        auth_token: cli.auth_token.into_bytes(),
    };

    // Offline local helpers (e.g. fs format/mount/unmount) do not require service connection.
    if let Commands::Fs { sub } = &cli.command {
        return match sub {
            FsCmd::Format {
                device,
                yes_format_destructive,
            } => {
                format_ext4(device, *yes_format_destructive).map_err(|e| e.to_string())?;
                Ok(json!({ "formatted": device.display().to_string(), "destructive": true }))
            }
            FsCmd::Mount {
                device,
                mount_point,
            } => {
                mount_ext4(device, mount_point).map_err(|e| e.to_string())?;
                Ok(json!({ "mounted": mount_point.display().to_string() }))
            }
            FsCmd::Unmount { mount_point } => {
                unmount_ext4(mount_point).map_err(|e| e.to_string())?;
                Ok(json!({ "unmounted": mount_point.display().to_string() }))
            }
        };
    }

    let client =
        ServiceClient::connect(endpoint.clone(), auth.clone()).map_err(|e| e.to_string())?;

    match cli.command {
        Commands::Workspace { sub } => match sub {
            WorkspaceCmd::Create {
                capacity_bytes,
                name,
                retention_ms,
                branch_id,
                generation_id,
                attachment_id,
            } => {
                let geometry = VolumeGeometry::new(capacity_bytes).map_err(|e| e.to_string())?;
                let br_id = parse_opt_id(branch_id.as_deref(), BranchId::from_bytes, 0x11)?;
                let gen_id =
                    parse_opt_id(generation_id.as_deref(), GenerationId::from_bytes, 0x22)?;
                let att_id =
                    parse_opt_id(attachment_id.as_deref(), AttachmentId::from_bytes, 0x33)?;
                let res = client
                    .create_empty_workspace(
                        alloc_op_id(),
                        gen_id,
                        VolumeBoundaryId::from_bytes(*alloc_op_id().as_bytes()),
                        geometry,
                        now_unix_ms(),
                        br_id,
                        &name,
                        retention_ms,
                        alloc_op_id(),
                        alloc_op_id(),
                        att_id,
                        alloc_prot_id(),
                    )
                    .map_err(|e| e.to_string())?;
                Ok(json!({
                    "branch": branch_json(&res.branch),
                    "writer_grant": grant_json(res.writer_grant),
                    "attachment": attachment_json(res.attachment),
                    "object_store_scope": res.object_store_scope.to_string(),
                }))
            }
        },
        Commands::Branch { sub } => match sub {
            BranchCmd::Create {
                name,
                base_generation,
                branch_id,
            } => {
                let base = GenerationId::from_bytes(decode_hex_16(&base_generation)?);
                let br_id = parse_opt_id(branch_id.as_deref(), BranchId::from_bytes, 0x12)?;
                let branch = client
                    .create_branch(alloc_op_id(), br_id, &name, base)
                    .map_err(|e| e.to_string())?;
                Ok(branch_json(&branch))
            }
            BranchCmd::Get { branch_id } => {
                let br = client
                    .get_branch(BranchId::from_bytes(decode_hex_16(&branch_id)?))
                    .map_err(|e| e.to_string())?;
                Ok(branch_json(&br))
            }
            BranchCmd::ByName { name } => {
                let br = client.branch_by_name(&name).map_err(|e| e.to_string())?;
                Ok(branch_json(&br))
            }
            BranchCmd::MoveHead {
                branch_id,
                expected_revision,
                target_generation,
                discard_live_state,
            } => {
                let br_id = BranchId::from_bytes(decode_hex_16(&branch_id)?);
                let target = GenerationId::from_bytes(decode_hex_16(&target_generation)?);
                let br = client.get_branch(br_id).map_err(|e| e.to_string())?;
                let grant = br.writer.ok_or("branch has no active writer grant")?;
                let updated = client
                    .move_head(
                        alloc_op_id(),
                        br_id,
                        expected_revision,
                        target,
                        grant,
                        discard_live_state,
                    )
                    .map_err(|e| e.to_string())?;
                Ok(branch_json(&updated))
            }
            BranchCmd::Promote {
                source_branch_id,
                expected_source_revision,
                destination_branch_id,
                expected_destination_revision,
                discard_live_state,
            } => {
                let src = BranchId::from_bytes(decode_hex_16(&source_branch_id)?);
                let dst = BranchId::from_bytes(decode_hex_16(&destination_branch_id)?);
                let dst_br = client.get_branch(dst).map_err(|e| e.to_string())?;
                let grant = dst_br
                    .writer
                    .ok_or("destination branch has no active writer grant")?;
                let updated = client
                    .promote(
                        alloc_op_id(),
                        src,
                        expected_source_revision,
                        dst,
                        expected_destination_revision,
                        grant,
                        discard_live_state,
                    )
                    .map_err(|e| e.to_string())?;
                Ok(branch_json(&updated))
            }
            BranchCmd::Delete {
                branch_id,
                expected_revision,
                discard_live_state,
            } => {
                let br_id = BranchId::from_bytes(decode_hex_16(&branch_id)?);
                let br = client.get_branch(br_id).map_err(|e| e.to_string())?;
                let grant = br.writer.ok_or("branch has no active writer grant")?;
                client
                    .delete_branch(
                        alloc_op_id(),
                        br_id,
                        expected_revision,
                        grant,
                        discard_live_state,
                    )
                    .map_err(|e| e.to_string())?;
                Ok(json!({ "deleted": br_id.to_string() }))
            }
        },
        Commands::Writer { sub } => match sub {
            WriterCmd::Acquire {
                branch_id,
                expected_revision,
                expected_writer_epoch,
                force,
            } => {
                let br_id = BranchId::from_bytes(decode_hex_16(&branch_id)?);
                let grant = if force {
                    client.force_acquire_writer(
                        alloc_op_id(),
                        br_id,
                        expected_revision,
                        expected_writer_epoch,
                    )
                } else {
                    client.acquire_writer(
                        alloc_op_id(),
                        br_id,
                        expected_revision,
                        expected_writer_epoch,
                    )
                }
                .map_err(|e| e.to_string())?;
                Ok(grant_json(grant))
            }
            WriterCmd::Renew { branch_id } => {
                let br = client
                    .get_branch(BranchId::from_bytes(decode_hex_16(&branch_id)?))
                    .map_err(|e| e.to_string())?;
                let grant = br.writer.ok_or("no active writer grant")?;
                let renewed = client
                    .renew_writer(alloc_op_id(), grant)
                    .map_err(|e| e.to_string())?;
                Ok(grant_json(renewed))
            }
            WriterCmd::Release { branch_id } => {
                let br = client
                    .get_branch(BranchId::from_bytes(decode_hex_16(&branch_id)?))
                    .map_err(|e| e.to_string())?;
                let grant = br.writer.ok_or("no active writer grant")?;
                client
                    .release_writer(alloc_op_id(), grant)
                    .map_err(|e| e.to_string())?;
                Ok(json!({ "released": branch_id }))
            }
        },
        Commands::Volume { sub } => match sub {
            VolumeCmd::Attach {
                branch_id,
                expected_revision,
                attachment_id,
            } => {
                let br_id = BranchId::from_bytes(decode_hex_16(&branch_id)?);
                let br = client.get_branch(br_id).map_err(|e| e.to_string())?;
                let grant = br.writer.ok_or("no active writer grant on branch")?;
                let att_id =
                    parse_opt_id(attachment_id.as_deref(), AttachmentId::from_bytes, 0x34)?;
                let att = client
                    .attach_volume(
                        alloc_op_id(),
                        br_id,
                        expected_revision,
                        grant,
                        att_id,
                        alloc_prot_id(),
                    )
                    .map_err(|e| e.to_string())?;
                Ok(attachment_json(att))
            }
            VolumeCmd::RenewBase { branch_id } => {
                let br_id = BranchId::from_bytes(decode_hex_16(&branch_id)?);
                let br = client.get_branch(br_id).map_err(|e| e.to_string())?;
                let grant = br.writer.ok_or("no active writer grant")?;
                let att = br.attachment.ok_or("no active attachment")?;
                let renewed = client
                    .renew_attachment_base(
                        alloc_op_id(),
                        br_id,
                        att.attachment_id,
                        att.volume_epoch,
                        att.protection_id,
                        grant,
                    )
                    .map_err(|e| e.to_string())?;
                Ok(attachment_json(renewed))
            }
            VolumeCmd::Detach {
                control_socket,
                branch_id,
            } => {
                if let Some(sock) = control_socket {
                    let resp = send_worker_command(&sock, &WorkerRequest::Detach)?;
                    return worker_resp_json(resp);
                }
                let br_id = BranchId::from_bytes(decode_hex_16(
                    branch_id
                        .as_deref()
                        .ok_or("provide --control-socket or --branch-id")?,
                )?);
                let br = client.get_branch(br_id).map_err(|e| e.to_string())?;
                let grant = br.writer.ok_or("no active writer grant")?;
                let att = br.attachment.ok_or("no active attachment")?;
                client
                    .detach_volume(
                        alloc_op_id(),
                        br_id,
                        att.attachment_id,
                        att.volume_epoch,
                        att.protection_id,
                        grant,
                    )
                    .map_err(|e| e.to_string())?;
                Ok(json!({ "detached": br_id.to_string() }))
            }
            VolumeCmd::Serve {
                branch_id,
                cache_dir,
                journal_dir,
                capture_dir,
                control_socket,
                cache_bytes,
                dirty_bytes,
                heartbeat_ms,
            } => {
                let br_id = BranchId::from_bytes(decode_hex_16(&branch_id)?);
                let br = client.get_branch(br_id).map_err(|e| e.to_string())?;
                let grant = br.writer.ok_or("branch has no active writer grant")?;
                let attachment = br
                    .attachment
                    .ok_or("branch has no active writable attachment")?;
                run_worker_server(WorkerConfig {
                    endpoint,
                    auth,
                    branch_id: br_id,
                    grant,
                    attachment,
                    cache_dir,
                    journal_dir,
                    capture_dir,
                    cache_bytes,
                    dirty_bytes,
                    control_socket,
                    heartbeat_interval: Duration::from_millis(heartbeat_ms),
                })?;
                Ok(json!({ "stopped": true }))
            }
            VolumeCmd::Write {
                control_socket,
                offset,
                data_hex,
            } => worker_resp_json(send_worker_command(
                &control_socket,
                &WorkerRequest::Write { offset, data_hex },
            )?),
            VolumeCmd::Read {
                control_socket,
                offset,
                length,
            } => worker_resp_json(send_worker_command(
                &control_socket,
                &WorkerRequest::Read { offset, length },
            )?),
            VolumeCmd::Flush { control_socket } => {
                worker_resp_json(send_worker_command(&control_socket, &WorkerRequest::Flush)?)
            }
            VolumeCmd::ReadBlocks {
                generation_id,
                offset,
                length,
            } => {
                let gen_id = GenerationId::from_bytes(decode_hex_16(&generation_id)?);
                let prot_id = alloc_prot_id();
                client
                    .open_generation(alloc_op_id(), gen_id, prot_id)
                    .map_err(|e| e.to_string())?;
                let res = client
                    .read_blocks_at(prot_id, offset, length)
                    .map_err(|e| e.to_string());
                let _ = client.close_protection(alloc_op_id(), prot_id);
                let bytes = res?;
                Ok(json!({
                    "generation_id": gen_id.to_string(),
                    "offset": offset,
                    "length": bytes.len(),
                    "data_hex": encode_hex_bytes(&bytes),
                }))
            }
            VolumeCmd::Status { control_socket } => worker_resp_json(send_worker_command(
                &control_socket,
                &WorkerRequest::Status,
            )?),
            VolumeCmd::ReconcileBarrier { control_socket } => worker_resp_json(
                send_worker_command(&control_socket, &WorkerRequest::ReconcileBarrier)?,
            ),
        },
        Commands::Fs { .. } => unreachable!("handled before connecting"),
        Commands::Checkpoint { sub } => match sub {
            CheckpointCmd::Create {
                control_socket,
                mode,
                detached,
                actor,
                execution_id,
                session_id,
                thread_id,
                task_id,
                workflow_operation_id,
                reason,
                trace_id,
                span_id,
                parent_span_id,
                optional_link,
                include_raw_payload,
                raw_payload,
            } => {
                let has_prov = actor.is_some()
                    || execution_id.is_some()
                    || session_id.is_some()
                    || thread_id.is_some()
                    || task_id.is_some()
                    || workflow_operation_id.is_some()
                    || reason.is_some()
                    || trace_id.is_some()
                    || span_id.is_some()
                    || parent_span_id.is_some()
                    || !optional_link.is_empty()
                    || include_raw_payload;
                let provenance = if has_prov {
                    Some(WorkerProvenanceInput {
                        actor,
                        execution_id,
                        session_id,
                        thread_id,
                        task_id,
                        workflow_operation_id,
                        reason,
                        trace_id,
                        span_id,
                        parent_span_id,
                        required_logs_root: None,
                        required_artifacts_root: None,
                        optional_links: optional_link,
                        include_raw_payload,
                        raw_payload,
                    })
                } else {
                    None
                };
                worker_resp_json(send_worker_command(
                    &control_socket,
                    &WorkerRequest::Checkpoint {
                        mode,
                        advance_head: !detached,
                        created_at_unix_ms: now_unix_ms(),
                        provenance: provenance.map(Box::new),
                    },
                )?)
            }
            CheckpointCmd::Recover { control_socket } => worker_resp_json(send_worker_command(
                &control_socket,
                &WorkerRequest::RecoverCapture,
            )?),
        },
        Commands::Generation { sub } => match sub {
            GenerationCmd::Open {
                generation_id,
                protection_id,
            } => {
                let gen_id = GenerationId::from_bytes(decode_hex_16(&generation_id)?);
                let prot_id =
                    parse_opt_id(protection_id.as_deref(), ProtectionId::from_bytes, 0x55)?;
                let deadline = client
                    .open_generation(alloc_op_id(), gen_id, prot_id)
                    .map_err(|e| e.to_string())?;
                Ok(json!({
                    "generation_id": gen_id.to_string(),
                    "protection_id": prot_id.to_string(),
                    "expires_at_ns": deadline.as_nanos(),
                }))
            }
            GenerationCmd::Renew { protection_id } => {
                let prot_id = ProtectionId::from_bytes(decode_hex_16(&protection_id)?);
                let deadline = client
                    .renew_protection(alloc_op_id(), prot_id)
                    .map_err(|e| e.to_string())?;
                Ok(json!({
                    "protection_id": prot_id.to_string(),
                    "expires_at_ns": deadline.as_nanos(),
                }))
            }
            GenerationCmd::Close { protection_id } => {
                let prot_id = ProtectionId::from_bytes(decode_hex_16(&protection_id)?);
                client
                    .close_protection(alloc_op_id(), prot_id)
                    .map_err(|e| e.to_string())?;
                Ok(json!({ "closed": prot_id.to_string() }))
            }
            GenerationCmd::Pin {
                generation_id,
                pin_id,
            } => {
                let gen_id = GenerationId::from_bytes(decode_hex_16(&generation_id)?);
                let p_id = parse_opt_id(pin_id.as_deref(), PinId::from_bytes, 0x66)?;
                client
                    .pin_generation(alloc_op_id(), gen_id, p_id)
                    .map_err(|e| e.to_string())?;
                Ok(json!({
                    "generation_id": gen_id.to_string(),
                    "pin_id": p_id.to_string(),
                }))
            }
            GenerationCmd::Unpin { pin_id } => {
                let p_id = PinId::from_bytes(decode_hex_16(&pin_id)?);
                client
                    .unpin_generation(alloc_op_id(), p_id)
                    .map_err(|e| e.to_string())?;
                Ok(json!({ "unpinned": p_id.to_string() }))
            }
            GenerationCmd::InspectProvenance { generation_id } => {
                let gen_id = GenerationId::from_bytes(decode_hex_16(&generation_id)?);
                let prot_id = alloc_prot_id();
                client
                    .open_generation(alloc_op_id(), gen_id, prot_id)
                    .map_err(|e| e.to_string())?;
                let res = client
                    .read_provenance(prot_id, gen_id)
                    .map_err(|e| e.to_string());
                let _ = client.close_protection(alloc_op_id(), prot_id);
                let insp = res?;
                Ok(json!({
                    "generation_id": insp.generation_id.to_string(),
                    "source_format": match insp.source_format {
                        GenerationDescriptorFormat::V1 => "v1",
                        GenerationDescriptorFormat::V2 => "v2",
                    },
                    "status": match insp.status {
                        ProvenanceStatus::Recorded => "recorded",
                        ProvenanceStatus::Unattributed => "unattributed",
                    },
                    "coverage": format!("{:?}", insp.coverage),
                    "context_root": insp.context_root.map(|r| r.object_id.to_string()),
                    "context": insp.context.map(|c| json!({
                        "recording_principal": c.recording_principal.to_string(),
                        "actor": c.actor,
                        "execution_id": c.execution_id,
                        "session_id": c.session_id,
                        "thread_id": c.thread_id,
                        "task_id": c.task_id,
                        "workflow_operation_id": c.workflow_operation_id,
                        "reason": c.reason,
                        "trace_id": c.trace_id,
                        "span_id": c.span_id,
                        "parent_span_id": c.parent_span_id,
                        "required_logs_root": c.required_logs_root.object_id.to_string(),
                        "required_artifacts_root": c.required_artifacts_root.object_id.to_string(),
                        "optional_links_unverified_external": c.optional_links,
                    })),
                }))
            }
            GenerationCmd::Diff {
                left_generation_id,
                right_generation_id,
            } => {
                let left_id = GenerationId::from_bytes(decode_hex_16(&left_generation_id)?);
                let right_id = GenerationId::from_bytes(decode_hex_16(&right_generation_id)?);
                let left_prot = alloc_prot_id();
                let right_prot = alloc_prot_id();
                client
                    .open_generation(alloc_op_id(), left_id, left_prot)
                    .map_err(|e| e.to_string())?;
                if let Err(e) = client.open_generation(alloc_op_id(), right_id, right_prot) {
                    let _ = client.close_protection(alloc_op_id(), left_prot);
                    return Err(e.to_string());
                }
                let res = client
                    .diff_generations(left_prot, left_id, right_prot, right_id)
                    .map_err(|e| e.to_string());
                let _ = client.close_protection(alloc_op_id(), left_prot);
                let _ = client.close_protection(alloc_op_id(), right_prot);
                let diff = res?;
                Ok(json!({
                    "extent_ranges": diff.extents.iter().map(|r| json!({
                        "start_extent": r.start_extent,
                        "extent_count": r.extent_count,
                    })).collect::<Vec<_>>(),
                    "log_changes": diff.logs.len(),
                    "artifact_changes": diff.artifacts.len(),
                }))
            }
        },
        Commands::Log { sub } => match sub {
            LogCmd::Create {
                branch_id,
                name,
                log_id,
                operation_id,
            } => {
                let br_id = BranchId::from_bytes(decode_hex_16(&branch_id)?);
                let ctx = write_ctx(&client, br_id)?;
                let l_id = parse_opt_id(log_id.as_deref(), LogId::from_bytes, 0x77)?;
                let op_id = parse_opt_id(operation_id.as_deref(), OperationId::from_bytes, 0x78)?;
                let mut store = client.remote_object_store();
                let (stream_id, branch) = LogClient::new()
                    .create_log(
                        &mut store,
                        &client,
                        ctx,
                        op_id,
                        alloc_prot_id(),
                        &name,
                        l_id,
                        alloc_op_id,
                    )
                    .map_err(|e| e.to_string())?;
                Ok(json!({
                    "stream_id": {
                        "branch_id": stream_id.branch_id.to_string(),
                        "stream_epoch": stream_id.stream_epoch,
                        "log_id": stream_id.log_id.to_string(),
                    },
                    "branch_revision": branch.revision,
                }))
            }
            LogCmd::Append {
                branch_id,
                name,
                stream_epoch,
                log_id,
                match_seq,
                record,
                operation_id,
            } => {
                let br_id = BranchId::from_bytes(decode_hex_16(&branch_id)?);
                let l_id = LogId::from_bytes(decode_hex_16(&log_id)?);
                let ctx = write_ctx(&client, br_id)?;
                let op_id = parse_opt_id(operation_id.as_deref(), OperationId::from_bytes, 0x79)?;
                let records = record.into_iter().map(|s| s.into_bytes()).collect();
                let mut store = client.remote_object_store();
                let receipt = LogClient::new()
                    .append(
                        &mut store,
                        &client,
                        ctx,
                        &name,
                        StreamId {
                            branch_id: br_id,
                            stream_epoch,
                            log_id: l_id,
                        },
                        match_seq,
                        op_id,
                        alloc_prot_id(),
                        records,
                        vec![],
                        alloc_op_id,
                    )
                    .map_err(|e| e.to_string())?;
                Ok(json!({
                    "durable_log_range": true,
                    "append_id": receipt.append_id.to_string(),
                    "start_seq": receipt.start_seq,
                    "end_seq": receipt.end_seq,
                    "branch_revision": receipt.branch_revision,
                }))
            }
            LogCmd::Trim {
                branch_id,
                name,
                stream_epoch,
                log_id,
                trim_to_seq,
                operation_id,
            } => {
                let br_id = BranchId::from_bytes(decode_hex_16(&branch_id)?);
                let l_id = LogId::from_bytes(decode_hex_16(&log_id)?);
                let ctx = write_ctx(&client, br_id)?;
                let op_id = parse_opt_id(operation_id.as_deref(), OperationId::from_bytes, 0x7A)?;
                let mut store = client.remote_object_store();
                let (view, branch) = LogClient::new()
                    .trim_log(
                        &mut store,
                        &client,
                        ctx,
                        &name,
                        StreamId {
                            branch_id: br_id,
                            stream_epoch,
                            log_id: l_id,
                        },
                        trim_to_seq,
                        op_id,
                        alloc_prot_id(),
                        alloc_op_id,
                    )
                    .map_err(|e| e.to_string())?;
                Ok(json!({
                    "name": view.name,
                    "first_seq": view.first_seq,
                    "next_seq": view.next_seq,
                    "branch_revision": branch.revision,
                }))
            }
            LogCmd::Delete {
                branch_id,
                name,
                stream_epoch,
                log_id,
                operation_id,
            } => {
                let br_id = BranchId::from_bytes(decode_hex_16(&branch_id)?);
                let l_id = LogId::from_bytes(decode_hex_16(&log_id)?);
                let ctx = write_ctx(&client, br_id)?;
                let op_id = parse_opt_id(operation_id.as_deref(), OperationId::from_bytes, 0x7B)?;
                let mut store = client.remote_object_store();
                let branch = LogClient::new()
                    .delete_log(
                        &mut store,
                        &client,
                        ctx,
                        &name,
                        StreamId {
                            branch_id: br_id,
                            stream_epoch,
                            log_id: l_id,
                        },
                        op_id,
                        alloc_prot_id(),
                        alloc_op_id,
                    )
                    .map_err(|e| e.to_string())?;
                Ok(json!({ "deleted": name, "branch_revision": branch.revision }))
            }
            LogCmd::Read {
                generation_id,
                name,
                from_seq,
                max_batches,
            } => {
                let gen_id = GenerationId::from_bytes(decode_hex_16(&generation_id)?);
                let prot_id = alloc_prot_id();
                client
                    .open_generation(alloc_op_id(), gen_id, prot_id)
                    .map_err(|e| e.to_string())?;
                let res = client
                    .read_log_at(prot_id, &name, from_seq, max_batches)
                    .map_err(|e| e.to_string());
                let _ = client.close_protection(alloc_op_id(), prot_id);
                let (view, batches) = res?;
                Ok(json!({
                    "name": view.name,
                    "first_seq": view.first_seq,
                    "next_seq": view.next_seq,
                    "batches": batches.iter().map(|b| json!({
                        "first_seq": b.first_seq,
                        "end_seq": b.end_seq,
                        "records": b.records.iter().map(|r| String::from_utf8_lossy(r).to_string()).collect::<Vec<_>>(),
                    })).collect::<Vec<_>>(),
                }))
            }
            LogCmd::Tail {
                branch_id,
                name,
                stream_epoch,
                log_id,
                from_seq,
                max_batches,
            } => {
                let br_id = BranchId::from_bytes(decode_hex_16(&branch_id)?);
                let l_id = LogId::from_bytes(decode_hex_16(&log_id)?);
                let store = client.remote_object_store();
                let (batches, cursor) = LogClient::new()
                    .poll_live_log(
                        &store,
                        &client,
                        &name,
                        LiveLogCursor {
                            stream_id: StreamId {
                                branch_id: br_id,
                                stream_epoch,
                                log_id: l_id,
                            },
                            next_seq: from_seq,
                        },
                        max_batches,
                        alloc_op_id(),
                        alloc_prot_id(),
                        alloc_op_id(),
                        alloc_op_id,
                    )
                    .map_err(|e| e.to_string())?;
                Ok(json!({
                    "next_seq": cursor.next_seq,
                    "batches": batches.iter().map(|b| json!({
                        "first_seq": b.first_seq,
                        "end_seq": b.end_seq,
                        "records": b.records.iter().map(|r| String::from_utf8_lossy(r).to_string()).collect::<Vec<_>>(),
                    })).collect::<Vec<_>>(),
                }))
            }
            LogCmd::List { generation_id } => {
                let gen_id = GenerationId::from_bytes(decode_hex_16(&generation_id)?);
                let prot_id = alloc_prot_id();
                client
                    .open_generation(alloc_op_id(), gen_id, prot_id)
                    .map_err(|e| e.to_string())?;
                let res = client.list_logs(prot_id).map_err(|e| e.to_string());
                let _ = client.close_protection(alloc_op_id(), prot_id);
                let logs = res?;
                Ok(json!(
                    logs.into_iter()
                        .map(|l| json!({
                            "name": l.name,
                            "log_id": l.log_id.to_string(),
                            "first_seq": l.first_seq,
                            "next_seq": l.next_seq,
                        }))
                        .collect::<Vec<_>>()
                ))
            }
        },
        Commands::Artifact { sub } => match sub {
            ArtifactCmd::Put {
                branch_id,
                name,
                artifact_id,
                payload_hex,
                operation_id,
            } => {
                let br_id = BranchId::from_bytes(decode_hex_16(&branch_id)?);
                let a_id = parse_opt_id(artifact_id.as_deref(), ArtifactId::from_bytes, 0x81)?;
                let op_id = parse_opt_id(operation_id.as_deref(), OperationId::from_bytes, 0x82)?;
                let payload = decode_hex_bytes(&payload_hex)?;
                let ctx = write_ctx(&client, br_id)?;
                let mut store = client.remote_object_store();
                let (desc, branch) = ArtifactClient::new()
                    .put_artifact(
                        &mut store,
                        &client,
                        ctx,
                        op_id,
                        alloc_prot_id(),
                        &name,
                        a_id,
                        &payload,
                        vec![],
                        alloc_op_id,
                    )
                    .map_err(|e| e.to_string())?;
                Ok(json!({
                    "artifact_id": desc.artifact_id.to_string(),
                    "logical_len": desc.logical_len,
                    "branch_revision": branch.revision,
                }))
            }
            ArtifactCmd::Remove {
                branch_id,
                name,
                operation_id,
            } => {
                let br_id = BranchId::from_bytes(decode_hex_16(&branch_id)?);
                let op_id = parse_opt_id(operation_id.as_deref(), OperationId::from_bytes, 0x83)?;
                let ctx = write_ctx(&client, br_id)?;
                let mut store = client.remote_object_store();
                let branch = ArtifactClient::new()
                    .delete_artifact(
                        &mut store,
                        &client,
                        ctx,
                        op_id,
                        alloc_prot_id(),
                        &name,
                        alloc_op_id,
                    )
                    .map_err(|e| e.to_string())?;
                Ok(json!({ "removed": name, "branch_revision": branch.revision }))
            }
            ArtifactCmd::StagePut {
                control_socket,
                name,
                artifact_id,
                payload_hex,
            } => {
                let a_id = parse_opt_id(artifact_id.as_deref(), ArtifactId::from_bytes, 0x84)?;
                worker_resp_json(send_worker_command(
                    &control_socket,
                    &WorkerRequest::StageArtifact {
                        name,
                        artifact_id_hex: encode_hex_bytes(a_id.as_bytes()),
                        payload_hex,
                    },
                )?)
            }
            ArtifactCmd::StageRemove {
                control_socket,
                name,
            } => worker_resp_json(send_worker_command(
                &control_socket,
                &WorkerRequest::StageRemoveArtifact { name },
            )?),
            ArtifactCmd::Get {
                generation_id,
                name,
                offset,
                length,
            } => {
                let gen_id = GenerationId::from_bytes(decode_hex_16(&generation_id)?);
                let prot_id = alloc_prot_id();
                client
                    .open_generation(alloc_op_id(), gen_id, prot_id)
                    .map_err(|e| e.to_string())?;
                let res = client
                    .read_artifact(prot_id, &name, offset, length)
                    .map_err(|e| e.to_string());
                let _ = client.close_protection(alloc_op_id(), prot_id);
                let (desc, bytes) = res?;
                Ok(json!({
                    "artifact_id": desc.artifact_id.to_string(),
                    "logical_len": desc.logical_len,
                    "data_hex": encode_hex_bytes(&bytes),
                }))
            }
            ArtifactCmd::List { generation_id } => {
                let gen_id = GenerationId::from_bytes(decode_hex_16(&generation_id)?);
                let prot_id = alloc_prot_id();
                client
                    .open_generation(alloc_op_id(), gen_id, prot_id)
                    .map_err(|e| e.to_string())?;
                let res = client.list_artifacts(prot_id).map_err(|e| e.to_string());
                let _ = client.close_protection(alloc_op_id(), prot_id);
                let items = res?;
                Ok(json!(
                    items
                        .into_iter()
                        .map(|(name, d)| json!({
                            "name": name,
                            "artifact_id": d.artifact_id.to_string(),
                            "logical_len": d.logical_len,
                        }))
                        .collect::<Vec<_>>()
                ))
            }
        },
        Commands::Op { sub } => match sub {
            OpCmd::Status { operation_id } => {
                let op_id = OperationId::from_bytes(decode_hex_16(&operation_id)?);
                let st = client.get_operation(op_id).map_err(|e| e.to_string())?;
                Ok(json!({ "operation_id": op_id.to_string(), "state": format!("{st:?}") }))
            }
            OpCmd::Abort { operation_id } => {
                let op_id = OperationId::from_bytes(decode_hex_16(&operation_id)?);
                let st = client.abort_operation(op_id).map_err(|e| e.to_string())?;
                Ok(json!({ "operation_id": op_id.to_string(), "state": format!("{st:?}") }))
            }
            OpCmd::Expire { operation_id } => {
                let op_id = OperationId::from_bytes(decode_hex_16(&operation_id)?);
                let st = client
                    .expire_preparation(op_id)
                    .map_err(|e| e.to_string())?;
                Ok(json!({ "operation_id": op_id.to_string(), "state": format!("{st:?}") }))
            }
        },
        Commands::Report { sub } => match sub {
            ReportCmd::Collect => {
                client.collect_report().map_err(|e| e.to_string())?;
                Ok(json!({}))
            }
            ReportCmd::Get => {
                client.get_gc_report().map_err(|e| e.to_string())?;
                Ok(json!({}))
            }
        },
    }
}

fn write_ctx(client: &ServiceClient, branch_id: BranchId) -> Result<NativeWriteContext, String> {
    let br = client.get_branch(branch_id).map_err(|e| e.to_string())?;
    let grant = br.writer.ok_or("branch has no active writer grant")?;
    let att = br
        .attachment
        .ok_or("branch has no active writable attachment")?;
    client.set_active_protection(Some(att.protection_id));
    Ok(NativeWriteContext {
        branch_id,
        attachment_id: att.attachment_id,
        volume_epoch: att.volume_epoch,
        grant,
    })
}

fn worker_resp_json(resp: WorkerResponse) -> Result<Value, String> {
    match resp {
        WorkerResponse::Error { message } => Err(message),
        other => serde_json::to_value(other).map_err(|e| e.to_string()),
    }
}

fn branch_json(b: &versionfs_core::control::BranchState) -> Value {
    json!({
        "workspace_id": b.workspace_id.to_string(),
        "branch_id": b.branch_id.to_string(),
        "name": b.name,
        "head": b.head.to_string(),
        "revision": b.revision,
        "writer_epoch": b.writer_epoch,
        "volume_epoch": b.volume_epoch,
        "stream_epoch": b.stream_epoch,
        "volume_root": b.volume_root.object_id.to_string(),
        "logs_root": b.logs_root.object_id.to_string(),
        "artifacts_root": b.artifacts_root.object_id.to_string(),
    })
}

fn grant_json(g: WriterGrant) -> Value {
    json!({
        "branch_id": g.branch_id.to_string(),
        "writer_epoch": g.writer_epoch,
        "owner_incarnation": g.owner_incarnation.get(),
        "grant_id": g.grant_id.to_string(),
        "expires_at_ns": g.expires_at.as_nanos(),
    })
}

fn attachment_json(a: AttachmentState) -> Value {
    json!({
        "attachment_id": a.attachment_id.to_string(),
        "protection_id": a.protection_id.to_string(),
        "base_generation": a.base_generation.to_string(),
        "volume_epoch": a.volume_epoch,
        "protection_expires_at_ns": a.protection_expires_at.as_nanos(),
    })
}

fn parse_opt_id<T>(raw: Option<&str>, ctor: fn([u8; 16]) -> T, _tag: u8) -> Result<T, String> {
    match raw {
        Some(hex) => Ok(ctor(decode_hex_16(hex)?)),
        None => Ok(ctor(*alloc_op_id().as_bytes())),
    }
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead, BufReader};
    use std::path::Path;
    use std::process::{Child, Command, Stdio};
    use std::time::Instant;

    struct TempDir(PathBuf);

    impl TempDir {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!(
                "versionfs-cli-unit-{}-{}",
                std::process::id(),
                now_unix_ms()
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

    struct ChildGuard(Option<Child>);

    impl Drop for ChildGuard {
        fn drop(&mut self) {
            if let Some(mut c) = self.0.take() {
                let _ = c.kill();
                let _ = c.wait();
            }
        }
    }

    fn ensure_versionfsd() -> PathBuf {
        let exe = std::env::current_exe().unwrap();
        let target_dir = exe
            .ancestors()
            .find(|p| p.file_name().is_some_and(|n| n == "debug"))
            .unwrap_or_else(|| exe.parent().unwrap());
        let bin = target_dir.join("versionfsd");
        if !bin.exists() {
            let status = Command::new(env!("CARGO"))
                .args([
                    "build",
                    "--offline",
                    "--locked",
                    "-p",
                    "versionfs-service",
                    "--bin",
                    "versionfsd",
                ])
                .status()
                .unwrap();
            assert!(status.success());
        }
        assert!(bin.exists(), "missing {}", bin.display());
        bin
    }

    fn cli_call(ep: &str, ws: &str, pr: &str, args: &[&str]) -> Result<Value, String> {
        let mut full = vec![
            "versionfs",
            "--endpoint",
            ep,
            "--workspace-id",
            ws,
            "--principal-id",
            pr,
            "--auth-token",
            "unit-tok",
            "--json",
        ];
        full.extend_from_slice(args);
        let cli = Cli::try_parse_from(full).map_err(|e| e.to_string())?;
        run(cli)
    }

    fn wait_sock(path: &Path) {
        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(10) {
            if path.exists() {
                return;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        panic!("socket {} did not appear", path.display());
    }

    #[test]
    fn in_process_cli_and_worker_exercise_all_commands() {
        let dir = TempDir::new();
        let meta_dir = dir.0.join("meta");
        let obj_dir = dir.0.join("obj");

        let mut svc = Command::new(ensure_versionfsd())
            .args([
                "--metadata-dir",
                meta_dir.to_str().unwrap(),
                "--object-dir",
                obj_dir.to_str().unwrap(),
                "--listen",
                "tcp://127.0.0.1:0",
                "--auth-token",
                "unit-tok",
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let mut ready = String::new();
        BufReader::new(svc.stdout.take().unwrap())
            .read_line(&mut ready)
            .unwrap();
        let _svc_guard = ChildGuard(Some(svc));
        let ep = ready.trim().strip_prefix("READY ").unwrap().to_owned();

        let ws = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let pr = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

        // Offline fs format without --confirm-destructive fails.
        assert!(cli_call(&ep, ws, pr, &["fs", "format", "--device", "/tmp/no-dev"]).is_err());

        let created = cli_call(
            &ep,
            ws,
            pr,
            &["workspace", "create", "--capacity-bytes", "131072"],
        )
        .unwrap();
        let main_br = created["branch"]["branch_id"].as_str().unwrap().to_owned();
        let g0 = created["branch"]["head"].as_str().unwrap().to_owned();
        let stream_epoch = created["branch"]["stream_epoch"].as_u64().unwrap();

        let sock = dir.0.join("worker.sock");
        let ep_bg = ep.clone();
        let br_bg = main_br.clone();
        let dir_bg = dir.0.clone();
        let sock_bg = sock.clone();
        let worker_thread = std::thread::spawn(move || {
            let cli = Cli::try_parse_from([
                "versionfs",
                "--endpoint",
                &ep_bg,
                "--workspace-id",
                ws,
                "--principal-id",
                pr,
                "--auth-token",
                "unit-tok",
                "volume",
                "serve",
                "--branch-id",
                &br_bg,
                "--cache-dir",
                dir_bg.join("cache").to_str().unwrap(),
                "--journal-dir",
                dir_bg.join("journal").to_str().unwrap(),
                "--capture-dir",
                dir_bg.join("capture").to_str().unwrap(),
                "--control-socket",
                sock_bg.to_str().unwrap(),
            ])
            .unwrap();
            run(cli).unwrap();
        });
        wait_sock(&sock);
        let sock_s = sock.to_str().unwrap();

        // Volume write/read/flush/status/reconcile-barrier
        cli_call(
            &ep,
            ws,
            pr,
            &[
                "volume",
                "write",
                "--control-socket",
                sock_s,
                "--offset",
                "0",
                "--data-hex",
                &"11223344".repeat(1024),
            ],
        )
        .unwrap();
        let rd = cli_call(
            &ep,
            ws,
            pr,
            &[
                "volume",
                "read",
                "--control-socket",
                sock_s,
                "--offset",
                "0",
                "--length",
                "4096",
            ],
        )
        .unwrap();
        assert_eq!(rd["data_hex"], Value::String("11223344".repeat(1024)));
        cli_call(
            &ep,
            ws,
            pr,
            &["volume", "flush", "--control-socket", sock_s],
        )
        .unwrap();
        cli_call(
            &ep,
            ws,
            pr,
            &["volume", "status", "--control-socket", sock_s],
        )
        .unwrap();
        cli_call(
            &ep,
            ws,
            pr,
            &["volume", "reconcile-barrier", "--control-socket", sock_s],
        )
        .unwrap();

        // Artifact stage-put / stage-remove / put / remove / list / get
        cli_call(
            &ep,
            ws,
            pr,
            &[
                "artifact",
                "stage-put",
                "--control-socket",
                sock_s,
                "--name",
                "temp.txt",
                "--payload-hex",
                "616263",
            ],
        )
        .unwrap();
        cli_call(
            &ep,
            ws,
            pr,
            &[
                "artifact",
                "stage-remove",
                "--control-socket",
                sock_s,
                "--name",
                "temp.txt",
            ],
        )
        .unwrap();
        cli_call(
            &ep,
            ws,
            pr,
            &[
                "artifact",
                "put",
                "--branch-id",
                &main_br,
                "--name",
                "note.txt",
                "--payload-hex",
                "6e6f7465",
            ],
        )
        .unwrap();

        // Log create / append / tail / trim / delete
        let log_res = cli_call(
            &ep,
            ws,
            pr,
            &["log", "create", "--branch-id", &main_br, "--name", "events"],
        )
        .unwrap();
        let log_id = log_res["stream_id"]["log_id"].as_str().unwrap().to_owned();
        cli_call(
            &ep,
            ws,
            pr,
            &[
                "log",
                "append",
                "--branch-id",
                &main_br,
                "--name",
                "events",
                "--stream-epoch",
                &stream_epoch.to_string(),
                "--log-id",
                &log_id,
                "--match-seq",
                "0",
                "--record",
                "r0",
                "--record",
                "r1",
            ],
        )
        .unwrap();
        cli_call(
            &ep,
            ws,
            pr,
            &[
                "log",
                "append",
                "--branch-id",
                &main_br,
                "--name",
                "events",
                "--stream-epoch",
                &stream_epoch.to_string(),
                "--log-id",
                &log_id,
                "--match-seq",
                "2",
                "--record",
                "r2",
            ],
        )
        .unwrap();
        cli_call(
            &ep,
            ws,
            pr,
            &[
                "log",
                "tail",
                "--branch-id",
                &main_br,
                "--name",
                "events",
                "--stream-epoch",
                &stream_epoch.to_string(),
                "--log-id",
                &log_id,
            ],
        )
        .unwrap();
        cli_call(
            &ep,
            ws,
            pr,
            &[
                "log",
                "trim",
                "--branch-id",
                &main_br,
                "--name",
                "events",
                "--stream-epoch",
                &stream_epoch.to_string(),
                "--log-id",
                &log_id,
                "--trim-to-seq",
                "2",
            ],
        )
        .unwrap();

        // Checkpoint with provenance + optional link + raw payload opt-in
        let g1_res = cli_call(
            &ep,
            ws,
            pr,
            &[
                "checkpoint",
                "create",
                "--control-socket",
                sock_s,
                "--mode",
                "block_cut",
                "--actor",
                "unit-agent",
                "--execution-id",
                "exec-1",
                "--session-id",
                "sess-1",
                "--thread-id",
                "thr-1",
                "--task-id",
                "task-1",
                "--workflow-operation-id",
                "wf-1",
                "--reason",
                "unit test",
                "--trace-id",
                "tr-1",
                "--span-id",
                "sp-1",
                "--parent-span-id",
                "psp-1",
                "--optional-link",
                "https://example.invalid/doc",
                "--include-raw-payload",
                "--raw-payload",
                "hello",
            ],
        )
        .unwrap();
        let g1 = g1_res["generation_id"].as_str().unwrap().to_owned();
        cli_call(
            &ep,
            ws,
            pr,
            &["checkpoint", "recover", "--control-socket", sock_s],
        )
        .unwrap();

        // Generation open / renew / pin / unpin / inspect-provenance / diff / close / read-blocks /
        // logs / artifacts
        let open_res =
            cli_call(&ep, ws, pr, &["generation", "open", "--generation-id", &g1]).unwrap();
        let prot_id = open_res["protection_id"].as_str().unwrap().to_owned();
        cli_call(
            &ep,
            ws,
            pr,
            &["generation", "renew", "--protection-id", &prot_id],
        )
        .unwrap();
        cli_call(
            &ep,
            ws,
            pr,
            &["generation", "close", "--protection-id", &prot_id],
        )
        .unwrap();
        let pin_res =
            cli_call(&ep, ws, pr, &["generation", "pin", "--generation-id", &g1]).unwrap();
        let pin_id = pin_res["pin_id"].as_str().unwrap().to_owned();
        cli_call(&ep, ws, pr, &["generation", "unpin", "--pin-id", &pin_id]).unwrap();
        cli_call(
            &ep,
            ws,
            pr,
            &["generation", "inspect-provenance", "--generation-id", &g1],
        )
        .unwrap();
        cli_call(
            &ep,
            ws,
            pr,
            &[
                "generation",
                "diff",
                "--left-generation-id",
                &g0,
                "--right-generation-id",
                &g1,
            ],
        )
        .unwrap();
        cli_call(
            &ep,
            ws,
            pr,
            &[
                "volume",
                "read-blocks",
                "--generation-id",
                &g1,
                "--offset",
                "0",
                "--length",
                "4096",
            ],
        )
        .unwrap();
        cli_call(&ep, ws, pr, &["log", "list", "--generation-id", &g1]).unwrap();
        cli_call(
            &ep,
            ws,
            pr,
            &[
                "log",
                "read",
                "--generation-id",
                &g1,
                "--name",
                "events",
                "--from-seq",
                "2",
            ],
        )
        .unwrap();
        cli_call(&ep, ws, pr, &["artifact", "list", "--generation-id", &g1]).unwrap();
        cli_call(
            &ep,
            ws,
            pr,
            &[
                "artifact",
                "get",
                "--generation-id",
                &g1,
                "--name",
                "note.txt",
            ],
        )
        .unwrap();

        // Log delete + Artifact remove
        cli_call(
            &ep,
            ws,
            pr,
            &[
                "log",
                "delete",
                "--branch-id",
                &main_br,
                "--name",
                "events",
                "--stream-epoch",
                &stream_epoch.to_string(),
                "--log-id",
                &log_id,
            ],
        )
        .unwrap();
        cli_call(
            &ep,
            ws,
            pr,
            &[
                "artifact",
                "remove",
                "--branch-id",
                &main_br,
                "--name",
                "note.txt",
            ],
        )
        .unwrap();

        // Branch by-name / renew-base / detach / writer renew+release+acquire / branch
        // create+move-head+promote+delete
        cli_call(&ep, ws, pr, &["branch", "by-name", "--name", "main"]).unwrap();
        cli_call(
            &ep,
            ws,
            pr,
            &["volume", "renew-base", "--branch-id", &main_br],
        )
        .unwrap();
        cli_call(&ep, ws, pr, &["writer", "renew", "--branch-id", &main_br]).unwrap();
        cli_call(
            &ep,
            ws,
            pr,
            &[
                "volume",
                "detach",
                "--control-socket",
                sock_s,
                "--branch-id",
                &main_br,
            ],
        )
        .unwrap();
        worker_thread.join().unwrap();

        let br_after = cli_call(&ep, ws, pr, &["branch", "get", "--branch-id", &main_br]).unwrap();
        let rev_after = br_after["revision"].as_u64().unwrap();
        cli_call(
            &ep,
            ws,
            pr,
            &[
                "branch",
                "move-head",
                "--branch-id",
                &main_br,
                "--expected-revision",
                &rev_after.to_string(),
                "--target-generation",
                &g0,
                "--discard-live-state",
            ],
        )
        .unwrap();

        let cand = cli_call(
            &ep,
            ws,
            pr,
            &["branch", "create", "--name", "c1", "--base-generation", &g1],
        )
        .unwrap();
        let cand_id = cand["branch_id"].as_str().unwrap().to_owned();
        let cand_rev = cand["revision"].as_u64().unwrap();
        let cand_epoch = cand["writer_epoch"].as_u64().unwrap();
        cli_call(
            &ep,
            ws,
            pr,
            &[
                "writer",
                "acquire",
                "--branch-id",
                &cand_id,
                "--expected-revision",
                &cand_rev.to_string(),
                "--expected-writer-epoch",
                &cand_epoch.to_string(),
            ],
        )
        .unwrap();
        cli_call(&ep, ws, pr, &["writer", "release", "--branch-id", &cand_id]).unwrap();
        let cand2 = cli_call(&ep, ws, pr, &["branch", "get", "--branch-id", &cand_id]).unwrap();
        cli_call(
            &ep,
            ws,
            pr,
            &[
                "writer",
                "acquire",
                "--branch-id",
                &cand_id,
                "--expected-revision",
                &cand2["revision"].as_u64().unwrap().to_string(),
                "--expected-writer-epoch",
                &cand2["writer_epoch"].as_u64().unwrap().to_string(),
                "--force",
            ],
        )
        .unwrap();
        cli_call(
            &ep,
            ws,
            pr,
            &[
                "branch",
                "delete",
                "--branch-id",
                &cand_id,
                "--expected-revision",
                &cand2["revision"].as_u64().unwrap().to_string(),
                "--discard-live-state",
            ],
        )
        .unwrap();

        // Op & report commands
        assert!(cli_call(&ep, ws, pr, &["report", "collect"]).is_err());
        assert!(cli_call(&ep, ws, pr, &["report", "get"]).is_err());
        let op_hex = "cccccccccccccccccccccccccccccccc";
        let _ = cli_call(&ep, ws, pr, &["op", "abort", "--operation-id", op_hex]);
        let _ = cli_call(&ep, ws, pr, &["op", "status", "--operation-id", op_hex]);
        let _ = cli_call(&ep, ws, pr, &["op", "expire", "--operation-id", op_hex]);
    }
}
