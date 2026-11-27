//! Typed, authorized client transport, protection-scoped remote object store,
//! freeze-independent attachment heartbeats, read-only volume, and explicit
//! durability acknowledgements.

use std::io::BufWriter;
use std::net::{SocketAddr, TcpStream};
#[cfg(unix)]
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;
use versionfs_core::control::{
    AttachmentState, BranchState, CaptureState, CommittedRootTransition, ControlError,
    GenerationSeed, LiveRoots, NativeRootTransitionMeta, OperationState, PreparationRequest,
    PublishedGeneration, REMOTE_READ_DEADLINE_NANOS, VerifiedGenerationSeed, WriterGrant,
};
use versionfs_core::format::{
    ArtifactDescriptor, CaptureMode, LogMapEntry, ObjectRef, ProvenanceBinding, ProvenanceStatus,
    VolumeGeometry,
};
use versionfs_core::gc::{GcCandidatePage, GcReportBudget, GcReportSummary};
use versionfs_core::graph::ProvenanceInspection;
use versionfs_core::identity::{
    AttachmentId, BranchId, GenerationId, ObjectId, ObjectStoreScopeId, OperationId, PinId,
    PrincipalId, ProtectionId, VolumeBoundaryId, WorkspaceId,
};
use versionfs_core::persistent_map::{DurableObjectStore, MapObjectStore};
use versionfs_core::rpc::{
    DiffStreamChunk, RpcAuth, RpcEnvelope, RpcRequest, RpcResponseFrame, ServiceError,
    ServiceErrorCode, decode_rpc_artifact_list, decode_rpc_attachment, decode_rpc_branch,
    decode_rpc_capture, decode_rpc_diff_chunk, decode_rpc_gc_report_page,
    decode_rpc_gc_report_summary, decode_rpc_generation_seed, decode_rpc_grant,
    decode_rpc_log_batch, decode_rpc_log_list, decode_rpc_operation_state,
    decode_rpc_provenance_inspection, decode_rpc_published_generation, decode_rpc_root_transition,
    read_frame, write_frame,
};
use versionfs_core::time::MonotonicTime;
use versionfs_core::views::{ArtifactChange, ExtentRange, LogViewChange};

use crate::barrier::VolumeBarrierControl;
use crate::capture::CaptureControl;
use crate::logs::{AppendReceipt, LogBatchView, LogView, NativeControl};
use crate::volume::{BlockDevice, BlockError, BlockRequest, BlockResponse};

/// Parsed service endpoint (`tcp://host:port` or `unix:///path`).
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ServiceEndpoint {
    /// TCP socket address.
    Tcp(SocketAddr),
    /// Unix domain socket path.
    Unix(PathBuf),
}

impl ServiceEndpoint {
    /// Parses `tcp://...` or `unix://...`.
    pub fn parse(raw: &str) -> Result<Self, ServiceError> {
        if let Some(addr) = raw.strip_prefix("tcp://") {
            let parsed: SocketAddr = addr.parse().map_err(|e| {
                ServiceError::new(
                    ServiceErrorCode::InvalidRequest,
                    format!("invalid tcp endpoint '{raw}': {e}"),
                )
            })?;
            Ok(Self::Tcp(parsed))
        } else if let Some(path) = raw.strip_prefix("unix://") {
            if path.is_empty() {
                return Err(ServiceError::new(
                    ServiceErrorCode::InvalidRequest,
                    "unix:// endpoint path must not be empty",
                ));
            }
            Ok(Self::Unix(PathBuf::from(path)))
        } else {
            Err(ServiceError::new(
                ServiceErrorCode::InvalidRequest,
                format!("endpoint must start with tcp:// or unix://: {raw}"),
            ))
        }
    }
}

/// Structural diff across two protected Generations (never infers file paths).
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GenerationDiff {
    /// Changed logical block extent ranges.
    pub extents: Vec<ExtentRange>,
    /// Changed native ordered-log views.
    pub logs: Vec<LogViewChange>,
    /// Changed native named-artifact descriptors.
    pub artifacts: Vec<ArtifactChange>,
}

/// Explicit local volatile block-write acknowledgement (RFC-002 Section 13).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WriteAck {
    /// Bytes accepted into the worker-local volatile dirty overlay.
    pub bytes_written: usize,
    /// Explicitly `true`: normal block writes are local volatile state only.
    pub local_volatile_only: bool,
}

/// Explicit durable FLUSH/FUA live-volume acknowledgement (RFC-002 Section 13).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FlushAck {
    /// Branch whose live `VolumeRoot` advanced.
    pub branch_id: BranchId,
    /// Committed authoritative branch revision.
    pub revision: u64,
    /// Committed durable live `VolumeRoot`.
    pub volume_root: ObjectRef,
    /// Explicitly `true`: durable live volume boundary, not a Generation checkpoint.
    pub durable_live_volume: bool,
}

/// Explicit durable native-log append acknowledgement (RFC-002 Section 13).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AppendAck {
    /// Exact durable range receipt.
    pub receipt: AppendReceipt,
    /// Explicitly `true`: durable live log range, not a Generation checkpoint.
    pub durable_log_range: bool,
}

/// Explicit published retained Generation acknowledgement (RFC-002 Section 13).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CheckpointAck {
    /// Published Generation identity and branch state.
    pub published: PublishedGeneration,
    /// Consistency boundary mode actually established.
    pub capture_mode: CaptureMode,
    /// Explicit provenance status bound in the descriptor.
    pub provenance_status: ProvenanceStatus,
    /// Explicitly `true`: retained Generation binding volume/log/artifact views.
    pub published_generation: bool,
}

/// Result returned by [`ServiceClient::create_empty_workspace`].
#[derive(Clone, Debug)]
pub struct RemoteInitializedWorkspace {
    /// Created main branch state after initial attachment.
    pub branch: BranchState,
    /// Initial writer grant.
    pub writer_grant: WriterGrant,
    /// Initial protected writable attachment.
    pub attachment: AttachmentState,
    /// Workspace's authoritative `ObjectStoreScopeId`.
    pub object_store_scope: ObjectStoreScopeId,
}

#[derive(Default)]
struct ClientSessionState {
    active_protection: Option<ProtectionId>,
    active_capture_protection: Option<ProtectionId>,
    active_preparation: Option<OperationId>,
    recent_uploads: std::collections::HashMap<ObjectId, Vec<u8>>,
}

impl ClientSessionState {
    fn cache_uploaded(&mut self, id: ObjectId, bytes: Vec<u8>) {
        if self.recent_uploads.len() >= 512 {
            self.recent_uploads.clear();
        }
        self.recent_uploads.insert(id, bytes);
    }
}

/// Authorized, bounded client for `versionfsd` implementing [`VolumeBarrierControl`],
/// [`CaptureControl`], and [`NativeControl`].
#[derive(Clone)]
pub struct ServiceClient {
    endpoint: ServiceEndpoint,
    auth: RpcAuth,
    object_store_scope: ObjectStoreScopeId,
    session: Arc<Mutex<ClientSessionState>>,
}

impl ServiceClient {
    /// Connects to `endpoint` with caller credentials and fetches the workspace's
    /// authoritative [`ObjectStoreScopeId`].
    pub fn connect(endpoint: ServiceEndpoint, auth: RpcAuth) -> Result<Self, ServiceError> {
        let mut client = Self {
            endpoint,
            auth,
            object_store_scope: ObjectStoreScopeId::from_bytes([0; 16]),
            session: Arc::new(Mutex::new(ClientSessionState::default())),
        };
        let frames = client.call_stream(RpcRequest::GetScope)?;
        let bytes = single_frame(&frames)?;
        let arr: [u8; 16] = bytes.try_into().map_err(|_| {
            ServiceError::new(ServiceErrorCode::IntegrityError, "invalid scope length")
        })?;
        client.object_store_scope = ObjectStoreScopeId::from_bytes(arr);
        Ok(client)
    }

    /// Returns the workspace's authoritative [`ObjectStoreScopeId`].
    pub const fn object_store_scope(&self) -> ObjectStoreScopeId {
        self.object_store_scope
    }

    /// Returns the caller's [`RpcAuth`].
    pub const fn auth(&self) -> &RpcAuth {
        &self.auth
    }

    /// Sets the active protection handle used by [`RemoteObjectStore`] reads.
    pub fn set_active_protection(&self, protection_id: Option<ProtectionId>) {
        if let Ok(mut guard) = self.session.lock() {
            guard.active_protection = protection_id;
        }
    }

    /// Sets the active preparing operation ID used by [`RemoteObjectStore`] uploads.
    pub fn set_active_preparation(&self, operation_id: Option<OperationId>) {
        if let Ok(mut guard) = self.session.lock() {
            guard.active_preparation = operation_id;
        }
    }

    /// Returns a [`RemoteObjectStore`] sharing this client's protection/preparation state.
    pub fn remote_object_store(&self) -> RemoteObjectStore {
        RemoteObjectStore {
            client: self.clone(),
        }
    }

    fn call_stream(&self, request: RpcRequest) -> Result<Vec<Vec<u8>>, ServiceError> {
        const MAX_BUFFERED_RESPONSE_BYTES: usize = 32 * 1024 * 1024;
        let mut out = Vec::new();
        let mut total_bytes = 0_usize;
        self.call_stream_with(request, |chunk| {
            total_bytes = total_bytes.checked_add(chunk.len()).ok_or_else(|| {
                ServiceError::new(ServiceErrorCode::LimitExceeded, "response size overflow")
            })?;
            if total_bytes > MAX_BUFFERED_RESPONSE_BYTES {
                return Err(ServiceError::new(
                    ServiceErrorCode::LimitExceeded,
                    "buffered RPC response exceeds 32 MiB limit",
                ));
            }
            out.push(chunk);
            Ok(())
        })?;
        Ok(out)
    }

    fn call_stream_with<F>(&self, request: RpcRequest, mut on_data: F) -> Result<(), ServiceError>
    where
        F: FnMut(Vec<u8>) -> Result<(), ServiceError>,
    {
        let env = RpcEnvelope {
            auth: self.auth.clone(),
            request,
        };
        let encoded = env.encode()?;
        match &self.endpoint {
            ServiceEndpoint::Tcp(addr) => {
                let mut stream = TcpStream::connect(addr).map_err(|e| {
                    ServiceError::new(
                        ServiceErrorCode::BackendError,
                        format!("connect {addr}: {e}"),
                    )
                })?;
                exchange_frames_with(&mut stream, &encoded, &mut on_data)
            }
            ServiceEndpoint::Unix(path) => {
                #[cfg(unix)]
                {
                    let mut stream = UnixStream::connect(path).map_err(|e| {
                        ServiceError::new(
                            ServiceErrorCode::BackendError,
                            format!("connect {}: {e}", path.display()),
                        )
                    })?;
                    exchange_frames_with(&mut stream, &encoded, &mut on_data)
                }
                #[cfg(not(unix))]
                {
                    let _ = path;
                    Err(ServiceError::new(
                        ServiceErrorCode::UnsupportedOperation,
                        "unix:// endpoints are only supported on Unix",
                    ))
                }
            }
        }
    }

    /// Creates an empty workspace, genesis Generation, writer grant, and initial attachment.
    #[allow(clippy::too_many_arguments)]
    pub fn create_empty_workspace(
        &self,
        create_op_id: OperationId,
        generation_id: GenerationId,
        boundary_id: VolumeBoundaryId,
        geometry: VolumeGeometry,
        created_at_unix_ms: u64,
        branch_id: BranchId,
        name: &str,
        retention_period_ms: u64,
        acquire_op_id: OperationId,
        attach_op_id: OperationId,
        attachment_id: AttachmentId,
        attachment_protection_id: ProtectionId,
    ) -> Result<RemoteInitializedWorkspace, ServiceError> {
        let frames = self.call_stream(RpcRequest::CreateEmptyWorkspace {
            create_op_id,
            generation_id,
            boundary_id,
            geometry,
            created_at_unix_ms,
            branch_id,
            name: name.to_owned(),
            retention_period_ms,
            acquire_op_id,
            attach_op_id,
            attachment_id,
            attachment_protection_id,
        })?;
        if frames.len() != 4 {
            return Err(ServiceError::new(
                ServiceErrorCode::IntegrityError,
                "unexpected frame count from CreateEmptyWorkspace",
            ));
        }
        let branch = decode_rpc_branch(&frames[0])?;
        let writer_grant = decode_rpc_grant(&frames[1])?;
        let attachment = decode_rpc_attachment(&frames[2])?;
        let scope_arr: [u8; 16] = frames[3].as_slice().try_into().map_err(|_| {
            ServiceError::new(ServiceErrorCode::IntegrityError, "invalid scope bytes")
        })?;
        self.set_active_protection(Some(attachment.protection_id));
        Ok(RemoteInitializedWorkspace {
            branch,
            writer_grant,
            attachment,
            object_store_scope: ObjectStoreScopeId::from_bytes(scope_arr),
        })
    }

    /// Creates a branch from a retained Generation.
    pub fn create_branch(
        &self,
        operation_id: OperationId,
        branch_id: BranchId,
        name: &str,
        base: GenerationId,
    ) -> Result<BranchState, ServiceError> {
        let frames = self.call_stream(RpcRequest::CreateBranch {
            operation_id,
            branch_id,
            name: name.to_owned(),
            base,
        })?;
        decode_rpc_branch(single_frame(&frames)?)
    }

    /// Resolves a branch by display name.
    pub fn branch_by_name(&self, name: &str) -> Result<BranchState, ServiceError> {
        let frames = self.call_stream(RpcRequest::BranchByName {
            name: name.to_owned(),
        })?;
        decode_rpc_branch(single_frame(&frames)?)
    }

    /// Acquires a writer lease.
    pub fn acquire_writer(
        &self,
        operation_id: OperationId,
        branch_id: BranchId,
        expected_revision: u64,
        expected_writer_epoch: u64,
    ) -> Result<WriterGrant, ServiceError> {
        let frames = self.call_stream(RpcRequest::AcquireWriter {
            operation_id,
            branch_id,
            expected_revision,
            expected_writer_epoch,
        })?;
        decode_rpc_grant(single_frame(&frames)?)
    }

    /// Forces writer handover.
    pub fn force_acquire_writer(
        &self,
        operation_id: OperationId,
        branch_id: BranchId,
        expected_revision: u64,
        expected_writer_epoch: u64,
    ) -> Result<WriterGrant, ServiceError> {
        let frames = self.call_stream(RpcRequest::ForceAcquireWriter {
            operation_id,
            branch_id,
            expected_revision,
            expected_writer_epoch,
        })?;
        decode_rpc_grant(single_frame(&frames)?)
    }

    /// Renews a writer lease.
    pub fn renew_writer(
        &self,
        operation_id: OperationId,
        grant: WriterGrant,
    ) -> Result<WriterGrant, ServiceError> {
        let frames = self.call_stream(RpcRequest::RenewWriter {
            operation_id,
            grant,
        })?;
        decode_rpc_grant(single_frame(&frames)?)
    }

    /// Releases a writer lease.
    pub fn release_writer(
        &self,
        operation_id: OperationId,
        grant: WriterGrant,
    ) -> Result<(), ServiceError> {
        let _ = self.call_stream(RpcRequest::ReleaseWriter {
            operation_id,
            grant,
        })?;
        Ok(())
    }

    /// Attaches a writable volume and records its protection handle as active.
    pub fn attach_volume(
        &self,
        operation_id: OperationId,
        branch_id: BranchId,
        expected_revision: u64,
        grant: WriterGrant,
        attachment_id: AttachmentId,
        protection_id: ProtectionId,
    ) -> Result<AttachmentState, ServiceError> {
        let frames = self.call_stream(RpcRequest::AttachVolume {
            operation_id,
            branch_id,
            expected_revision,
            grant,
            attachment_id,
            protection_id,
        })?;
        let att = decode_rpc_attachment(single_frame(&frames)?)?;
        self.set_active_protection(Some(att.protection_id));
        Ok(att)
    }

    /// Renews an attachment base protection.
    pub fn renew_attachment_base(
        &self,
        operation_id: OperationId,
        branch_id: BranchId,
        attachment_id: AttachmentId,
        volume_epoch: u64,
        protection_id: ProtectionId,
        grant: WriterGrant,
    ) -> Result<AttachmentState, ServiceError> {
        let frames = self.call_stream(RpcRequest::RenewAttachmentBase {
            operation_id,
            branch_id,
            attachment_id,
            volume_epoch,
            protection_id,
            grant,
        })?;
        decode_rpc_attachment(single_frame(&frames)?)
    }

    /// Detaches a clean, drained writable attachment.
    pub fn detach_volume(
        &self,
        operation_id: OperationId,
        branch_id: BranchId,
        attachment_id: AttachmentId,
        volume_epoch: u64,
        protection_id: ProtectionId,
        grant: WriterGrant,
    ) -> Result<(), ServiceError> {
        let _ = self.call_stream(RpcRequest::DetachVolume {
            operation_id,
            branch_id,
            attachment_id,
            volume_epoch,
            protection_id,
            grant,
        })?;
        self.set_active_protection(None);
        Ok(())
    }

    /// Moves a branch head to a retained Generation.
    pub fn move_head(
        &self,
        operation_id: OperationId,
        branch_id: BranchId,
        expected_revision: u64,
        target_generation: GenerationId,
        grant: WriterGrant,
        discard_live_state: bool,
    ) -> Result<BranchState, ServiceError> {
        let frames = self.call_stream(RpcRequest::MoveHead {
            operation_id,
            branch_id,
            expected_revision,
            target_generation,
            grant,
            discard_live_state,
        })?;
        decode_rpc_branch(single_frame(&frames)?)
    }

    /// Promotes a source branch head onto a destination branch.
    #[allow(clippy::too_many_arguments)]
    pub fn promote(
        &self,
        operation_id: OperationId,
        source_branch_id: BranchId,
        expected_source_revision: u64,
        destination_branch_id: BranchId,
        expected_destination_revision: u64,
        destination_grant: WriterGrant,
        discard_live_state: bool,
    ) -> Result<BranchState, ServiceError> {
        let frames = self.call_stream(RpcRequest::Promote {
            operation_id,
            source_branch_id,
            expected_source_revision,
            destination_branch_id,
            expected_destination_revision,
            destination_grant,
            discard_live_state,
        })?;
        decode_rpc_branch(single_frame(&frames)?)
    }

    /// Deletes a branch.
    pub fn delete_branch(
        &self,
        operation_id: OperationId,
        branch_id: BranchId,
        expected_revision: u64,
        grant: WriterGrant,
        discard_live_state: bool,
    ) -> Result<(), ServiceError> {
        let _ = self.call_stream(RpcRequest::DeleteBranch {
            operation_id,
            branch_id,
            expected_revision,
            grant,
            discard_live_state,
        })?;
        Ok(())
    }

    /// Pins a retained Generation.
    pub fn pin_generation(
        &self,
        operation_id: OperationId,
        generation_id: GenerationId,
        pin_id: PinId,
    ) -> Result<(), ServiceError> {
        let _ = self.call_stream(RpcRequest::PinGeneration {
            operation_id,
            generation_id,
            pin_id,
        })?;
        Ok(())
    }

    /// Unpins a Generation pin.
    pub fn unpin_generation(
        &self,
        operation_id: OperationId,
        pin_id: PinId,
    ) -> Result<(), ServiceError> {
        let _ = self.call_stream(RpcRequest::UnpinGeneration {
            operation_id,
            pin_id,
        })?;
        Ok(())
    }

    /// Expires an expired or fenced preparation.
    pub fn expire_preparation(
        &self,
        operation_id: OperationId,
    ) -> Result<OperationState, ServiceError> {
        let frames = self.call_stream(RpcRequest::ExpirePreparation { operation_id })?;
        decode_rpc_operation_state(single_frame(&frames)?)
    }

    /// Validates and uploads a bounded `ProvenanceContext` under an active writer grant.
    pub fn upload_provenance_context(
        &self,
        branch_id: BranchId,
        grant: WriterGrant,
        encoded_context: Vec<u8>,
    ) -> Result<ObjectRef, ServiceError> {
        let cached_bytes = encoded_context.clone();
        let frames = self.call_stream(RpcRequest::UploadProvenanceContext {
            branch_id,
            grant,
            encoded_context,
        })?;
        let bytes = single_frame(&frames)?;
        if bytes.len() != 42 {
            return Err(ServiceError::new(
                ServiceErrorCode::IntegrityError,
                "invalid ObjectRef response length",
            ));
        }
        let object_type =
            versionfs_core::format::ObjectType::try_from(u16::from_le_bytes([bytes[0], bytes[1]]))?;
        let object_id = ObjectId::from_bytes(bytes[2..34].try_into().unwrap());
        let encoded_len = u64::from_le_bytes(bytes[34..42].try_into().unwrap());
        let obj_ref = ObjectRef {
            object_type,
            object_id,
            encoded_len,
        };
        obj_ref.verify(&cached_bytes)?;
        if let Ok(mut guard) = self.session.lock() {
            guard.cache_uploaded(object_id, cached_bytes);
        }
        Ok(obj_ref)
    }

    /// Reads verified block bytes from a protected Generation (`read_blocks_at`).
    pub fn read_blocks_at(
        &self,
        protection_id: ProtectionId,
        offset: u64,
        length: u64,
    ) -> Result<Vec<u8>, ServiceError> {
        let frames = self.call_stream(RpcRequest::ReadBlocksAt {
            protection_id,
            offset,
            length,
        })?;
        let total: usize = frames.iter().map(Vec::len).sum();
        let mut out = Vec::with_capacity(total);
        for frame in frames {
            out.extend_from_slice(&frame);
        }
        Ok(out)
    }

    /// Inspects a protected Generation's provenance binding and context.
    pub fn read_provenance(
        &self,
        protection_id: ProtectionId,
        generation_id: GenerationId,
    ) -> Result<ProvenanceInspection, ServiceError> {
        let frames = self.call_stream(RpcRequest::ReadProvenance {
            protection_id,
            generation_id,
        })?;
        decode_rpc_provenance_inspection(single_frame(&frames)?)
    }

    /// Streams structural extent, log, and artifact diffs between two protected Generations.
    pub fn diff_generations(
        &self,
        left_protection_id: ProtectionId,
        left_generation_id: GenerationId,
        right_protection_id: ProtectionId,
        right_generation_id: GenerationId,
    ) -> Result<GenerationDiff, ServiceError> {
        let mut diff = GenerationDiff::default();
        self.call_stream_with(
            RpcRequest::DiffGenerations {
                left_protection_id,
                left_generation_id,
                right_protection_id,
                right_generation_id,
            },
            |frame| {
                match decode_rpc_diff_chunk(&frame)? {
                    DiffStreamChunk::Extents(ranges) => diff.extents.extend(ranges),
                    DiffStreamChunk::Logs(changes) => diff.logs.extend(changes),
                    DiffStreamChunk::Artifacts(changes) => diff.artifacts.extend(changes),
                }
                Ok(())
            },
        )?;
        Ok(diff)
    }

    /// Reads verified log batches under a protected Generation or LiveLog handle.
    pub fn read_log_at(
        &self,
        protection_id: ProtectionId,
        name: &str,
        from_seq: u64,
        max_batches: usize,
    ) -> Result<(LogView, Vec<LogBatchView>), ServiceError> {
        let mut entry: Option<LogMapEntry> = None;
        let mut batches = Vec::new();
        self.call_stream_with(
            RpcRequest::ReadLogAt {
                protection_id,
                name: name.to_owned(),
                from_seq,
                max_batches,
            },
            |frame| {
                if entry.is_none() {
                    entry = Some(LogMapEntry::decode(&frame)?);
                } else {
                    let (log_id, first_seq, end_seq, records, attachments) =
                        decode_rpc_log_batch(&frame)?;
                    batches.push(LogBatchView {
                        log_id,
                        first_seq,
                        end_seq,
                        records,
                        attachments,
                    });
                }
                Ok(())
            },
        )?;
        let entry = entry.ok_or_else(|| {
            ServiceError::new(
                ServiceErrorCode::IntegrityError,
                "missing LogMapEntry frame from ReadLogAt",
            )
        })?;
        Ok((LogView::from_entry(name, entry), batches))
    }

    /// Lists all named logs under an active protection handle.
    pub fn list_logs(&self, protection_id: ProtectionId) -> Result<Vec<LogView>, ServiceError> {
        let frames = self.call_stream(RpcRequest::ListLogs { protection_id })?;
        let entries = decode_rpc_log_list(single_frame(&frames)?)?;
        Ok(entries
            .into_iter()
            .map(|(name, entry)| LogView::from_entry(name, entry))
            .collect())
    }

    /// Reads a bounded slice of a named artifact under an active Generation protection.
    pub fn read_artifact(
        &self,
        protection_id: ProtectionId,
        name: &str,
        offset: u64,
        length: u64,
    ) -> Result<(ArtifactDescriptor, Vec<u8>), ServiceError> {
        let frames = self.call_stream(RpcRequest::ReadArtifact {
            protection_id,
            name: name.to_owned(),
            offset,
            length,
        })?;
        let Some((first, rest)) = frames.split_first() else {
            return Err(ServiceError::new(
                ServiceErrorCode::IntegrityError,
                "missing ArtifactDescriptor frame from ReadArtifact",
            ));
        };
        let desc = ArtifactDescriptor::decode(first)?;
        let total: usize = rest.iter().map(Vec::len).sum();
        let mut bytes = Vec::with_capacity(total);
        for chunk in rest {
            bytes.extend_from_slice(chunk);
        }
        Ok((desc, bytes))
    }

    /// Lists all named artifacts under an active Generation protection.
    pub fn list_artifacts(
        &self,
        protection_id: ProtectionId,
    ) -> Result<Vec<(String, ArtifactDescriptor)>, ServiceError> {
        let frames = self.call_stream(RpcRequest::ListArtifacts { protection_id })?;
        decode_rpc_artifact_list(single_frame(&frames)?)
    }

    /// Collects a bounded mark-only retention/GC report (P10).
    pub fn collect_report(
        &self,
        operation_id: OperationId,
        budget: Option<GcReportBudget>,
    ) -> Result<GcReportSummary, ServiceError> {
        let frames = self.call_stream(RpcRequest::CollectReport {
            operation_id,
            budget,
        })?;
        decode_rpc_gc_report_summary(single_frame(&frames)?)
    }

    /// Looks up a persisted mark-only GC report summary and optional candidate page (P10).
    pub fn get_gc_report(
        &self,
        report_id: OperationId,
        page_index: Option<u32>,
    ) -> Result<(GcReportSummary, Option<GcCandidatePage>), ServiceError> {
        let frames = self.call_stream(RpcRequest::GetGcReport {
            report_id,
            page_index,
        })?;
        match frames.as_slice() {
            [summary_frame] => Ok((decode_rpc_gc_report_summary(summary_frame)?, None)),
            [summary_frame, page_frame] => Ok((
                decode_rpc_gc_report_summary(summary_frame)?,
                Some(decode_rpc_gc_report_page(page_frame)?),
            )),
            _ => Err(ServiceError::new(
                ServiceErrorCode::BackendError,
                "unexpected frame count in GetGcReport response",
            )),
        }
    }
}

impl VolumeBarrierControl for ServiceClient {
    fn workspace_id(&self) -> WorkspaceId {
        self.auth.workspace_id
    }

    fn object_store_scope(&self) -> ObjectStoreScopeId {
        self.object_store_scope
    }

    fn principal_id(&self) -> PrincipalId {
        self.auth.principal_id
    }

    fn get_branch(&self, branch_id: BranchId) -> Result<BranchState, ControlError> {
        let frames = self
            .call_stream(RpcRequest::GetBranch { branch_id })
            .map_err(|e| e.to_control_error())?;
        decode_rpc_branch(single_frame(&frames).map_err(|e| e.to_control_error())?)
            .map_err(|e| e.to_control_error())
    }

    fn check_attachment_live(
        &self,
        branch_id: BranchId,
        attachment_id: AttachmentId,
        volume_epoch: u64,
        grant: WriterGrant,
    ) -> Result<WriterGrant, ControlError> {
        let frames = self
            .call_stream(RpcRequest::CheckAttachmentLive {
                branch_id,
                attachment_id,
                volume_epoch,
                grant,
            })
            .map_err(|e| e.to_control_error())?;
        decode_rpc_grant(single_frame(&frames).map_err(|e| e.to_control_error())?)
            .map_err(|e| e.to_control_error())
    }

    fn begin_preparation(
        &self,
        request: PreparationRequest,
    ) -> Result<OperationState, ControlError> {
        self.begin_capture(request, ProvenanceBinding::Unattributed)
    }

    fn authorize_capture_child(
        &self,
        authorization_id: OperationId,
        capture_id: OperationId,
        child_id: OperationId,
        grant: WriterGrant,
    ) -> Result<(), ControlError> {
        let _ = self
            .call_stream(RpcRequest::AuthorizeCaptureChild {
                authorization_id,
                capture_id,
                child_id,
                grant,
            })
            .map_err(|e| e.to_control_error())?;
        Ok(())
    }

    fn renew_preparation(
        &self,
        renewal_id: OperationId,
        prepared_id: OperationId,
        grant: WriterGrant,
    ) -> Result<MonotonicTime, ControlError> {
        let frames = self
            .call_stream(RpcRequest::RenewPreparation {
                renewal_id,
                prepared_id,
                grant,
            })
            .map_err(|e| e.to_control_error())?;
        self.set_active_preparation(Some(prepared_id));
        decode_rpc_time(single_frame(&frames).map_err(|e| e.to_control_error())?)
            .map_err(|e| e.to_control_error())
    }

    fn commit_live_roots(
        &self,
        operation_id: OperationId,
        expected_revision: u64,
        grant: WriterGrant,
        roots: LiveRoots,
        capture_id: Option<OperationId>,
    ) -> Result<BranchState, ControlError> {
        self.commit_live_roots_with_meta(
            operation_id,
            expected_revision,
            grant,
            roots,
            capture_id,
            None,
        )
    }

    fn commit_live_roots_with_meta(
        &self,
        operation_id: OperationId,
        expected_revision: u64,
        grant: WriterGrant,
        roots: LiveRoots,
        capture_id: Option<OperationId>,
        native_meta: Option<NativeRootTransitionMeta>,
    ) -> Result<BranchState, ControlError> {
        let frames = self
            .call_stream(RpcRequest::CommitLiveRoots {
                operation_id,
                expected_revision,
                grant,
                roots,
                capture_child: capture_id.map(|c| (c, operation_id)),
                native_meta,
            })
            .map_err(|e| e.to_control_error())?;
        self.set_active_preparation(capture_id);
        decode_rpc_branch(single_frame(&frames).map_err(|e| e.to_control_error())?)
            .map_err(|e| e.to_control_error())
    }

    fn abort_operation(&self, operation_id: OperationId) -> Result<OperationState, ControlError> {
        let frames = self
            .call_stream(RpcRequest::AbortOperation { operation_id })
            .map_err(|e| e.to_control_error())?;
        self.set_active_preparation(None);
        decode_rpc_operation_state(single_frame(&frames).map_err(|e| e.to_control_error())?)
            .map_err(|e| e.to_control_error())
    }

    fn get_operation(&self, operation_id: OperationId) -> Result<OperationState, ControlError> {
        let frames = self
            .call_stream(RpcRequest::GetOperation { operation_id })
            .map_err(|e| e.to_control_error())?;
        decode_rpc_operation_state(single_frame(&frames).map_err(|e| e.to_control_error())?)
            .map_err(|e| e.to_control_error())
    }

    fn get_committed_root_transition(
        &self,
        operation_id: OperationId,
    ) -> Result<Option<CommittedRootTransition>, ControlError> {
        let frames = self
            .call_stream(RpcRequest::GetCommittedRootTransition { operation_id })
            .map_err(|e| e.to_control_error())?;
        decode_rpc_root_transition(single_frame(&frames).map_err(|e| e.to_control_error())?)
            .map_err(|e| e.to_control_error())
    }
}

impl CaptureControl for ServiceClient {
    fn begin_capture(
        &self,
        request: PreparationRequest,
        provenance: ProvenanceBinding,
    ) -> Result<OperationState, ControlError> {
        let capture_prot = (request.kind
            == versionfs_core::control::PreparationKind::CheckpointCapture)
            .then_some(request.protection_id);
        let frames = self
            .call_stream(RpcRequest::BeginPreparation {
                request,
                provenance,
            })
            .map_err(|e| e.to_control_error())?;
        let state =
            decode_rpc_operation_state(single_frame(&frames).map_err(|e| e.to_control_error())?)
                .map_err(|e| e.to_control_error())?;
        if let OperationState::Preparing { operation_id, .. } = state {
            self.set_active_preparation(Some(operation_id));
            if let Some(prot_id) = capture_prot
                && let Ok(mut guard) = self.session.lock()
            {
                guard.active_capture_protection = Some(prot_id);
            }
        }
        Ok(state)
    }

    fn seal_capture(
        &self,
        operation_id: OperationId,
        capture_id: OperationId,
        expected_revision: u64,
        grant: WriterGrant,
    ) -> Result<CaptureState, ControlError> {
        let frames = self
            .call_stream(RpcRequest::SealCapture {
                operation_id,
                capture_id,
                expected_revision,
                grant,
            })
            .map_err(|e| e.to_control_error())?;
        decode_rpc_capture(single_frame(&frames).map_err(|e| e.to_control_error())?)
            .map_err(|e| e.to_control_error())
    }

    fn publish_capture(
        &self,
        capture_id: OperationId,
        expected_revision: u64,
        expected_parent: GenerationId,
        grant: WriterGrant,
        attachment_id: AttachmentId,
        volume_epoch: u64,
        generation: VerifiedGenerationSeed,
        advance_head: bool,
    ) -> Result<PublishedGeneration, ControlError> {
        let frames = self
            .call_stream(RpcRequest::PublishCapture {
                capture_id,
                expected_revision,
                expected_parent,
                grant,
                attachment_id,
                volume_epoch,
                descriptor_ref: generation.seed().descriptor,
                advance_head,
            })
            .map_err(|e| e.to_control_error())?;
        self.set_active_preparation(None);
        decode_rpc_published_generation(single_frame(&frames).map_err(|e| e.to_control_error())?)
            .map_err(|e| e.to_control_error())
    }
}

impl NativeControl for ServiceClient {
    fn open_live_log(
        &self,
        operation_id: OperationId,
        branch_id: BranchId,
        expected_revision: u64,
        expected_root: ObjectRef,
        protection_id: ProtectionId,
    ) -> Result<MonotonicTime, ControlError> {
        let frames = self
            .call_stream(RpcRequest::OpenLiveLog {
                operation_id,
                branch_id,
                expected_revision,
                expected_root,
                protection_id,
            })
            .map_err(|e| e.to_control_error())?;
        let deadline = decode_rpc_time(single_frame(&frames).map_err(|e| e.to_control_error())?)
            .map_err(|e| e.to_control_error())?;
        self.set_active_protection(Some(protection_id));
        Ok(deadline)
    }

    fn open_generation(
        &self,
        operation_id: OperationId,
        generation_id: GenerationId,
        protection_id: ProtectionId,
    ) -> Result<MonotonicTime, ControlError> {
        let frames = self
            .call_stream(RpcRequest::OpenGeneration {
                operation_id,
                generation_id,
                protection_id,
            })
            .map_err(|e| e.to_control_error())?;
        let deadline = decode_rpc_time(single_frame(&frames).map_err(|e| e.to_control_error())?)
            .map_err(|e| e.to_control_error())?;
        self.set_active_protection(Some(protection_id));
        Ok(deadline)
    }

    fn get_generation(&self, generation_id: GenerationId) -> Result<GenerationSeed, ControlError> {
        let frames = self
            .call_stream(RpcRequest::GetGeneration { generation_id })
            .map_err(|e| e.to_control_error())?;
        decode_rpc_generation_seed(single_frame(&frames).map_err(|e| e.to_control_error())?)
            .map_err(|e| e.to_control_error())
    }

    fn renew_protection(
        &self,
        operation_id: OperationId,
        protection_id: ProtectionId,
    ) -> Result<MonotonicTime, ControlError> {
        let frames = self
            .call_stream(RpcRequest::RenewProtection {
                operation_id,
                protection_id,
            })
            .map_err(|e| e.to_control_error())?;
        decode_rpc_time(single_frame(&frames).map_err(|e| e.to_control_error())?)
            .map_err(|e| e.to_control_error())
    }

    fn close_protection(
        &self,
        operation_id: OperationId,
        protection_id: ProtectionId,
    ) -> Result<(), ControlError> {
        let _ = self
            .call_stream(RpcRequest::CloseProtection {
                operation_id,
                protection_id,
            })
            .map_err(|e| e.to_control_error())?;
        if let Ok(mut guard) = self.session.lock()
            && guard.active_protection == Some(protection_id)
        {
            guard.active_protection = None;
        }
        Ok(())
    }

    fn now(&self) -> MonotonicTime {
        match self.call_stream(RpcRequest::Now) {
            Ok(frames) => single_frame(&frames)
                .and_then(decode_rpc_time)
                .unwrap_or_else(|_| MonotonicTime::from_nanos(0)),
            Err(_) => MonotonicTime::from_nanos(0),
        }
    }
}

/// Protection-scoped, preparation-authorized remote object store backed by [`ServiceClient`].
///
/// Arbitrary digest lookup is never used: `get_ref` requires an active protection
/// handle whose graph closure reaches `reference`, and `put_durable` requires an
/// active `Preparing` operation on `ServiceClient`.
#[derive(Clone)]
pub struct RemoteObjectStore {
    client: ServiceClient,
}

impl MapObjectStore for RemoteObjectStore {
    fn proof_scope(&self) -> ObjectStoreScopeId {
        self.client.object_store_scope()
    }

    fn get(&self, _id: ObjectId) -> Result<Option<Vec<u8>>, String> {
        Err("typed ObjectRef with active protection is required".to_owned())
    }

    fn get_ref(&self, reference: ObjectRef) -> Result<Option<Vec<u8>>, String> {
        let (protection_id, capture_protection) = {
            let guard = self
                .client
                .session
                .lock()
                .map_err(|_| "client session lock poisoned".to_owned())?;
            if let Some(bytes) = guard.recent_uploads.get(&reference.object_id)
                && reference.verify(bytes).is_ok()
            {
                return Ok(Some(bytes.clone()));
            }
            let primary = guard
                .active_protection
                .or(guard.active_capture_protection)
                .ok_or_else(|| "no active protection handle for remote object read".to_owned())?;
            (primary, guard.active_capture_protection)
        };
        match self.client.call_stream(RpcRequest::FetchProtectedObject {
            protection_id,
            reference,
        }) {
            Ok(frames) => {
                let bytes = single_frame(&frames).map_err(|e| e.to_string())?;
                reference.verify(bytes).map_err(|e| e.to_string())?;
                Ok(Some(bytes.to_vec()))
            }
            Err(err)
                if err.code == ServiceErrorCode::Unauthorized
                    && capture_protection.is_some()
                    && capture_protection != Some(protection_id) =>
            {
                match self.client.call_stream(RpcRequest::FetchProtectedObject {
                    protection_id: capture_protection.unwrap(),
                    reference,
                }) {
                    Ok(frames) => {
                        let bytes = single_frame(&frames).map_err(|e| e.to_string())?;
                        reference.verify(bytes).map_err(|e| e.to_string())?;
                        Ok(Some(bytes.to_vec()))
                    }
                    Err(err2) if err2.code == ServiceErrorCode::NotFound => Ok(None),
                    Err(err2) => Err(err2.to_string()),
                }
            }
            Err(err) if err.code == ServiceErrorCode::NotFound => Ok(None),
            Err(err) => Err(err.to_string()),
        }
    }

    fn put(&mut self, id: ObjectId, bytes: Vec<u8>) -> Result<(), String> {
        let reference = ObjectRef::from_encoded(&bytes).map_err(|e| e.to_string())?;
        if reference.object_id != id {
            return Err("object ID mismatch".to_owned());
        }
        self.put_durable(reference, &bytes)
    }
}

impl DurableObjectStore for RemoteObjectStore {
    fn put_durable(&mut self, reference: ObjectRef, encoded: &[u8]) -> Result<(), String> {
        reference.verify(encoded).map_err(|e| e.to_string())?;
        let operation_id = self
            .client
            .session
            .lock()
            .map_err(|_| "client session lock poisoned".to_owned())?
            .active_preparation
            .ok_or_else(|| "no active preparation operation for remote object upload".to_owned())?;
        let _ = self
            .client
            .call_stream(RpcRequest::UploadPreparedObject {
                operation_id,
                reference,
                encoded_bytes: encoded.to_vec(),
            })
            .map_err(|e| e.to_string())?;
        if let Ok(mut guard) = self.client.session.lock() {
            guard.cache_uploaded(reference.object_id, encoded.to_vec());
        }
        Ok(())
    }
}

/// Bounded, renewable read handle wrapping `open_generation` / `close_protection`.
pub struct ProtectedReadHandle {
    client: ServiceClient,
    generation_id: GenerationId,
    protection_id: ProtectionId,
    deadline: MonotonicTime,
}

impl ProtectedReadHandle {
    /// Opens a Generation under a fresh protection handle.
    pub fn open(
        client: ServiceClient,
        open_op_id: OperationId,
        generation_id: GenerationId,
        protection_id: ProtectionId,
    ) -> Result<Self, ControlError> {
        let deadline = client.open_generation(open_op_id, generation_id, protection_id)?;
        Ok(Self {
            client,
            generation_id,
            protection_id,
            deadline,
        })
    }

    /// Returns the protected Generation ID.
    pub const fn generation_id(&self) -> GenerationId {
        self.generation_id
    }

    /// Returns the protection handle ID.
    pub const fn protection_id(&self) -> ProtectionId {
        self.protection_id
    }

    /// Returns the current protection deadline on the authority clock.
    pub const fn deadline(&self) -> MonotonicTime {
        self.deadline
    }

    /// Returns the bounded remote read deadline (`min(deadline, now + 30s)`).
    pub fn bounded_read_deadline(&self) -> Result<MonotonicTime, ControlError> {
        let now = self.client.now();
        if self.deadline <= now {
            return Err(ControlError::Expired);
        }
        Ok(self
            .deadline
            .min(now.saturating_add_nanos(REMOTE_READ_DEADLINE_NANOS)))
    }

    /// Renews the underlying read protection before expiry.
    pub fn renew(&mut self, renewal_op_id: OperationId) -> Result<MonotonicTime, ControlError> {
        let deadline = self
            .client
            .renew_protection(renewal_op_id, self.protection_id)?;
        self.deadline = deadline;
        Ok(deadline)
    }

    /// Explicitly closes the protection handle.
    pub fn close(self, close_op_id: OperationId) -> Result<(), ControlError> {
        self.client
            .close_protection(close_op_id, self.protection_id)
    }
}

/// Immutable read-only [`BlockDevice`] backed by `read_blocks_at` under a [`ProtectedReadHandle`].
///
/// Write and Flush requests fail explicitly with [`BlockError::UnsupportedOperation`].
pub struct ReadOnlyVolume {
    client: ServiceClient,
    protection_id: ProtectionId,
    geometry: VolumeGeometry,
}

impl ReadOnlyVolume {
    /// Opens a read-only block device for a protected Generation.
    pub fn new(
        client: ServiceClient,
        protection_id: ProtectionId,
        geometry: VolumeGeometry,
    ) -> Self {
        Self {
            client,
            protection_id,
            geometry,
        }
    }
}

impl BlockDevice for ReadOnlyVolume {
    fn geometry(&self) -> VolumeGeometry {
        self.geometry
    }

    fn supports_fua(&self) -> bool {
        false
    }

    fn dispatch(&mut self, request: BlockRequest<'_>) -> Result<BlockResponse, BlockError> {
        match request {
            BlockRequest::Read { offset, length } => {
                self.geometry.validate_range(offset, length)?;
                let bytes = self
                    .client
                    .read_blocks_at(self.protection_id, offset, length)
                    .map_err(|e| BlockError::Barrier(e.to_string()))?;
                Ok(BlockResponse::Read(bytes))
            }
            BlockRequest::Write { .. } | BlockRequest::Flush => {
                Err(BlockError::UnsupportedOperation(1))
            }
            BlockRequest::Unsupported(code) => Err(BlockError::UnsupportedOperation(code)),
        }
    }
}

/// Freeze-independent background heartbeat supervisor for one writable attachment.
///
/// Periodically renews both the writer lease (`renew_writer`) and attachment-base
/// protection (`renew_attachment_base`) using control-plane RPCs only — never
/// touching any mounted filesystem. If ownership is fenced or expires, sets
/// `fenced` and optionally signals `stop_tx` so a mounted block device stops.
pub struct AttachmentHeartbeat {
    fenced: Arc<AtomicBool>,
    grant: Arc<Mutex<WriterGrant>>,
    attachment: Arc<Mutex<AttachmentState>>,
    stop_tx: mpsc::Sender<()>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl AttachmentHeartbeat {
    /// Spawns a freeze-independent heartbeat loop renewing `grant` and `attachment`
    /// every `interval` until stopped or fenced.
    pub fn start<F>(
        client: ServiceClient,
        branch_id: BranchId,
        initial_grant: WriterGrant,
        initial_attachment: AttachmentState,
        interval: Duration,
        device_stop_tx: Option<mpsc::Sender<()>>,
        mut next_op_id: F,
    ) -> Self
    where
        F: FnMut() -> OperationId + Send + 'static,
    {
        let fenced = Arc::new(AtomicBool::new(false));
        let grant = Arc::new(Mutex::new(initial_grant));
        let attachment = Arc::new(Mutex::new(initial_attachment));
        let (stop_tx, stop_rx) = mpsc::channel();

        let fenced_bg = Arc::clone(&fenced);
        let grant_bg = Arc::clone(&grant);
        let att_bg = Arc::clone(&attachment);

        let thread = std::thread::spawn(move || {
            let mut pending_writer_op: Option<OperationId> = None;
            loop {
                match stop_rx.recv_timeout(interval) {
                    Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => break,
                    Err(mpsc::RecvTimeoutError::Timeout) => {}
                }
                let cur_grant = *grant_bg
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let cur_att = *att_bg
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);

                // Reuse the same operation ID across transient/ambiguous errors so a
                // committed renewal whose reply was lost replays idempotently.
                let writer_op = *pending_writer_op.get_or_insert_with(&mut next_op_id);
                match client.renew_writer(writer_op, cur_grant) {
                    Ok(new_grant) => {
                        pending_writer_op = None;
                        *grant_bg
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner) = new_grant;
                        match client.renew_attachment_base(
                            next_op_id(),
                            branch_id,
                            cur_att.attachment_id,
                            cur_att.volume_epoch,
                            cur_att.protection_id,
                            new_grant,
                        ) {
                            Ok(new_att) => {
                                *att_bg
                                    .lock()
                                    .unwrap_or_else(std::sync::PoisonError::into_inner) = new_att;
                            }
                            Err(err)
                                if matches!(
                                    err.code,
                                    ServiceErrorCode::Fenced
                                        | ServiceErrorCode::LeaseExpired
                                        | ServiceErrorCode::NotFound
                                ) =>
                            {
                                fenced_bg.store(true, Ordering::Release);
                                if let Some(tx) = &device_stop_tx {
                                    let _ = tx.send(());
                                }
                                break;
                            }
                            Err(_) => {}
                        }
                    }
                    Err(err)
                        if matches!(
                            err.code,
                            ServiceErrorCode::Fenced
                                | ServiceErrorCode::LeaseExpired
                                | ServiceErrorCode::NotFound
                        ) =>
                    {
                        fenced_bg.store(true, Ordering::Release);
                        if let Some(tx) = &device_stop_tx {
                            let _ = tx.send(());
                        }
                        break;
                    }
                    Err(_) => {}
                }
            }
        });

        Self {
            fenced,
            grant,
            attachment,
            stop_tx,
            thread: Some(thread),
        }
    }

    /// Performs one synchronous heartbeat round immediately (useful in deterministic tests).
    pub fn beat_once(
        client: &ServiceClient,
        branch_id: BranchId,
        grant: &mut WriterGrant,
        attachment: &mut AttachmentState,
        op_writer: OperationId,
        op_att: OperationId,
    ) -> Result<(), ServiceError> {
        let new_grant = client.renew_writer(op_writer, *grant)?;
        *grant = new_grant;
        let new_att = client.renew_attachment_base(
            op_att,
            branch_id,
            attachment.attachment_id,
            attachment.volume_epoch,
            attachment.protection_id,
            new_grant,
        )?;
        *attachment = new_att;
        Ok(())
    }

    /// Returns `true` if the heartbeat observed terminal fencing or lease expiry.
    pub fn is_fenced(&self) -> bool {
        self.fenced.load(Ordering::Acquire)
    }

    /// Returns the latest acknowledged [`WriterGrant`].
    pub fn current_grant(&self) -> WriterGrant {
        *self
            .grant
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Returns the latest acknowledged [`AttachmentState`].
    pub fn current_attachment(&self) -> AttachmentState {
        *self
            .attachment
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Stops the background heartbeat thread cleanly.
    pub fn stop(&mut self) {
        let _ = self.stop_tx.send(());
        if let Some(handle) = self.thread.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for AttachmentHeartbeat {
    fn drop(&mut self) {
        self.stop();
    }
}

fn exchange_frames_with<S: std::io::Read + std::io::Write, F>(
    stream: &mut S,
    encoded_req: &[u8],
    on_data: &mut F,
) -> Result<(), ServiceError>
where
    F: FnMut(Vec<u8>) -> Result<(), ServiceError>,
{
    {
        let mut writer = BufWriter::new(&mut *stream);
        write_frame(&mut writer, encoded_req)?;
    }
    loop {
        let raw = read_frame(stream)?;
        match RpcResponseFrame::decode(&raw)? {
            RpcResponseFrame::Data(chunk) => on_data(chunk)?,
            RpcResponseFrame::End => return Ok(()),
            RpcResponseFrame::Error(err) => return Err(err),
        }
    }
}

fn single_frame(frames: &[Vec<u8>]) -> Result<&[u8], ServiceError> {
    if frames.len() != 1 {
        return Err(ServiceError::new(
            ServiceErrorCode::IntegrityError,
            format!("expected 1 response frame, got {}", frames.len()),
        ));
    }
    Ok(frames[0].as_slice())
}

fn decode_rpc_time(bytes: &[u8]) -> Result<MonotonicTime, ServiceError> {
    let raw: [u8; 8] = bytes.try_into().map_err(|_| {
        ServiceError::new(
            ServiceErrorCode::IntegrityError,
            "invalid MonotonicTime bytes",
        )
    })?;
    Ok(MonotonicTime::from_nanos(u64::from_le_bytes(raw)))
}
