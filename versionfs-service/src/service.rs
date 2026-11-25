//! Typed, authorized, bounded streaming service handlers and listener for
//! Version FS (RFC-002 Section 12).
//!
//! Arbitrary digest access is never exposed publicly: object reads require an
//! active, unexpired protection handle whose graph closure reaches the requested
//! object, and remote reads enforce the 30-second bounded read deadline.

use std::collections::HashSet;
use std::io::BufWriter;
use std::net::{TcpListener, TcpStream};
#[cfg(unix)]
use std::os::unix::net::{UnixListener, UnixStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::runtime::Handle;
use versionfs_core::control::{
    OwnerIncarnation, ProtectionScope, REMOTE_READ_DEADLINE_NANOS, WorkspaceControl,
};
use versionfs_core::format::{
    ArtifactDescriptor, EXTENT_BYTES, FormatError, LogMapEntry, MAX_ENCODED_OBJECT_BYTES,
    ObjectRef, ObjectType, ProvenanceBinding, ProvenanceContext, VolumeGeometry, VolumeRoot,
    decode_generation_descriptor,
};
use versionfs_core::graph::{inspect_generation_provenance, verify_generation};
use versionfs_core::identity::{
    GenerationId, ObjectId, ObjectStoreScopeId, PrincipalId, ProtectionId, WorkspaceId,
};
use versionfs_core::metadata::MetadataAuthority;
use versionfs_core::persistent_map::{DurableObjectStore, MapObjectStore, PersistentBTree};
use versionfs_core::rpc::{
    DiffStreamChunk, MAX_DIFF_CHUNK_ITEMS, MAX_LOG_READ_BATCHES, MAX_STREAM_READ_BYTES, RpcAuth,
    RpcEnvelope, RpcRequest, RpcResponseFrame, ServiceError, ServiceErrorCode,
    encode_rpc_artifact_list, encode_rpc_attachment, encode_rpc_branch, encode_rpc_capture,
    encode_rpc_diff_chunk, encode_rpc_generation_seed, encode_rpc_grant, encode_rpc_log_batch,
    encode_rpc_log_list, encode_rpc_operation_state, encode_rpc_provenance_inspection,
    encode_rpc_published_generation, encode_rpc_root_transition, read_frame, write_frame,
};
use versionfs_core::time::MonotonicTime;
use versionfs_core::views::{
    ArtifactChunkIndex, ArtifactMap, LogMap, LogSegmentIndex, ViewError, VolumeExtentMap,
    decode_artifact_blob, decode_artifact_chunk,
};

use crate::object_store::{ObjectStore, ObjectStoreError};

const STREAM_CHUNK_BYTES: usize = 64 * 1024;

/// Configuration for [`WorkspaceService`].
#[derive(Clone, Debug)]
pub struct ServiceConfig {
    /// Shared authentication token required on every RPC envelope.
    pub auth_token: Vec<u8>,
    /// Optional allow-list of authenticated principals (`None` permits any principal
    /// presenting the valid workspace auth token).
    pub allowed_principals: Option<HashSet<PrincipalId>>,
    /// Principals explicitly denied provenance context reads (returns `Unauthorized`
    /// without relabeling recorded provenance as unattributed).
    pub provenance_deny_principals: HashSet<PrincipalId>,
}

impl ServiceConfig {
    /// Creates a service configuration requiring `auth_token`.
    pub fn new(auth_token: impl Into<Vec<u8>>) -> Self {
        Self {
            auth_token: auth_token.into(),
            allowed_principals: None,
            provenance_deny_principals: HashSet::new(),
        }
    }
}

/// Authorized Version FS service combining a `MetadataAuthority` and an `ObjectStore`.
pub struct WorkspaceService<A, O> {
    authority: Arc<A>,
    owner_incarnation: OwnerIncarnation,
    object_store: Arc<O>,
    runtime: Handle,
    config: ServiceConfig,
}

impl<A, O> WorkspaceService<A, O>
where
    A: MetadataAuthority + Send + Sync + 'static,
    O: ObjectStore + Send + Sync + 'static,
{
    /// Constructs a new service over an owned authority, incarnation, and object store.
    pub fn new(
        authority: Arc<A>,
        owner_incarnation: OwnerIncarnation,
        object_store: Arc<O>,
        runtime: Handle,
        config: ServiceConfig,
    ) -> Self {
        Self {
            authority,
            owner_incarnation,
            object_store,
            runtime,
            config,
        }
    }

    fn authenticate(&self, auth: &RpcAuth) -> Result<(), ServiceError> {
        if !constant_time_eq(&auth.auth_token, &self.config.auth_token) {
            return Err(ServiceError::new(
                ServiceErrorCode::Unauthorized,
                "invalid service authentication token",
            ));
        }
        if let Some(allowed) = &self.config.allowed_principals
            && !allowed.contains(&auth.principal_id)
        {
            return Err(ServiceError::new(
                ServiceErrorCode::Unauthorized,
                "principal is not authorized for this service",
            ));
        }
        Ok(())
    }

    fn control_for(&self, auth: &RpcAuth) -> WorkspaceControl<A> {
        let scope = self.object_store.scope_id(auth.workspace_id);
        WorkspaceControl::new(
            Arc::clone(&self.authority),
            auth.workspace_id,
            scope,
            auth.principal_id,
            self.owner_incarnation,
        )
    }

    fn store_bridge(&self, workspace_id: WorkspaceId) -> ServiceObjectBridge<O> {
        ServiceObjectBridge {
            store: Arc::clone(&self.object_store),
            workspace_id,
            scope: self.object_store.scope_id(workspace_id),
            runtime: self.runtime.clone(),
        }
    }

    /// Handles one decoded [`RpcEnvelope`], streaming 0..N data frames to `sink`.
    pub fn handle_request(
        &self,
        envelope: RpcEnvelope,
        sink: &mut dyn FnMut(RpcResponseFrame) -> Result<(), ServiceError>,
    ) {
        if let Err(err) = self.authenticate(&envelope.auth) {
            let _ = sink(RpcResponseFrame::Error(err));
            return;
        }
        match self.dispatch(&envelope.auth, envelope.request, sink) {
            Ok(()) => {
                let _ = sink(RpcResponseFrame::End);
            }
            Err(err) => {
                let _ = sink(RpcResponseFrame::Error(err));
            }
        }
    }

    fn dispatch(
        &self,
        auth: &RpcAuth,
        request: RpcRequest,
        sink: &mut dyn FnMut(RpcResponseFrame) -> Result<(), ServiceError>,
    ) -> Result<(), ServiceError> {
        let control = self.control_for(auth);
        let bridge = self.store_bridge(auth.workspace_id);

        match request {
            RpcRequest::GetScope => {
                let scope = self.object_store.scope_id(auth.workspace_id);
                sink(RpcResponseFrame::Data(scope.as_bytes().to_vec()))?;
            }
            RpcRequest::Now => {
                let now = control.now();
                sink(RpcResponseFrame::Data(
                    now.as_nanos().to_le_bytes().to_vec(),
                ))?;
            }
            RpcRequest::CreateEmptyWorkspace {
                create_op_id,
                generation_id,
                boundary_id,
                geometry,
                created_at_unix_ms,
                branch_id,
                name,
                retention_period_ms,
                acquire_op_id,
                attach_op_id,
                attachment_id,
                attachment_protection_id,
            } => {
                let mut bridge_mut = bridge;
                let seed = create_empty_generation_on_store(
                    &mut bridge_mut,
                    generation_id,
                    boundary_id,
                    geometry,
                    created_at_unix_ms,
                )?;
                let created = control.create_workspace(
                    create_op_id,
                    branch_id,
                    &name,
                    retention_period_ms,
                    seed,
                )?;
                let grant = control.acquire_writer(
                    acquire_op_id,
                    branch_id,
                    created.revision,
                    created.writer_epoch,
                )?;
                let attachment = control.attach_volume(
                    attach_op_id,
                    branch_id,
                    created.revision,
                    grant,
                    attachment_id,
                    attachment_protection_id,
                )?;
                let branch = control.get_branch(branch_id)?;
                sink(RpcResponseFrame::Data(encode_rpc_branch(&branch)?))?;
                sink(RpcResponseFrame::Data(encode_rpc_grant(grant)))?;
                sink(RpcResponseFrame::Data(encode_rpc_attachment(attachment)))?;
                sink(RpcResponseFrame::Data(
                    self.object_store
                        .scope_id(auth.workspace_id)
                        .as_bytes()
                        .to_vec(),
                ))?;
            }
            RpcRequest::CreateBranch {
                operation_id,
                branch_id,
                name,
                base,
            } => {
                let branch = control.create_branch(operation_id, branch_id, &name, base)?;
                sink(RpcResponseFrame::Data(encode_rpc_branch(&branch)?))?;
            }
            RpcRequest::GetBranch { branch_id } => {
                let branch = control.get_branch(branch_id)?;
                sink(RpcResponseFrame::Data(encode_rpc_branch(&branch)?))?;
            }
            RpcRequest::BranchByName { name } => {
                let branch = control.branch_by_name(&name)?;
                sink(RpcResponseFrame::Data(encode_rpc_branch(&branch)?))?;
            }
            RpcRequest::GetGeneration { generation_id } => {
                let seed = control.get_generation(generation_id)?;
                sink(RpcResponseFrame::Data(encode_rpc_generation_seed(seed)))?;
            }
            RpcRequest::AcquireWriter {
                operation_id,
                branch_id,
                expected_revision,
                expected_writer_epoch,
            } => {
                let grant = control.acquire_writer(
                    operation_id,
                    branch_id,
                    expected_revision,
                    expected_writer_epoch,
                )?;
                sink(RpcResponseFrame::Data(encode_rpc_grant(grant)))?;
            }
            RpcRequest::ForceAcquireWriter {
                operation_id,
                branch_id,
                expected_revision,
                expected_writer_epoch,
            } => {
                let grant = control.force_acquire_writer(
                    operation_id,
                    branch_id,
                    expected_revision,
                    expected_writer_epoch,
                )?;
                sink(RpcResponseFrame::Data(encode_rpc_grant(grant)))?;
            }
            RpcRequest::RenewWriter {
                operation_id,
                grant,
            } => {
                let renewed = control.renew_writer(operation_id, grant)?;
                sink(RpcResponseFrame::Data(encode_rpc_grant(renewed)))?;
            }
            RpcRequest::ReleaseWriter {
                operation_id,
                grant,
            } => {
                control.release_writer(operation_id, grant)?;
            }
            RpcRequest::AttachVolume {
                operation_id,
                branch_id,
                expected_revision,
                grant,
                attachment_id,
                protection_id,
            } => {
                let att = control.attach_volume(
                    operation_id,
                    branch_id,
                    expected_revision,
                    grant,
                    attachment_id,
                    protection_id,
                )?;
                sink(RpcResponseFrame::Data(encode_rpc_attachment(att)))?;
            }
            RpcRequest::RenewAttachmentBase {
                operation_id,
                branch_id,
                attachment_id,
                volume_epoch,
                protection_id,
                grant,
            } => {
                let att = control.renew_attachment_base(
                    operation_id,
                    branch_id,
                    attachment_id,
                    volume_epoch,
                    protection_id,
                    grant,
                )?;
                sink(RpcResponseFrame::Data(encode_rpc_attachment(att)))?;
            }
            RpcRequest::DetachVolume {
                operation_id,
                branch_id,
                attachment_id,
                volume_epoch,
                protection_id,
                grant,
            } => {
                control.detach_volume(
                    operation_id,
                    branch_id,
                    attachment_id,
                    volume_epoch,
                    protection_id,
                    grant,
                )?;
            }
            RpcRequest::CheckAttachmentLive {
                branch_id,
                attachment_id,
                volume_epoch,
                grant,
            } => {
                let live =
                    control.check_attachment_live(branch_id, attachment_id, volume_epoch, grant)?;
                sink(RpcResponseFrame::Data(encode_rpc_grant(live)))?;
            }
            RpcRequest::MoveHead {
                operation_id,
                branch_id,
                expected_revision,
                target_generation,
                grant,
                discard_live_state,
            } => {
                let branch = control.move_head(
                    operation_id,
                    branch_id,
                    expected_revision,
                    target_generation,
                    grant,
                    discard_live_state,
                )?;
                sink(RpcResponseFrame::Data(encode_rpc_branch(&branch)?))?;
            }
            RpcRequest::Promote {
                operation_id,
                source_branch_id,
                expected_source_revision,
                destination_branch_id,
                expected_destination_revision,
                destination_grant,
                discard_live_state,
            } => {
                let branch = control.promote(
                    operation_id,
                    source_branch_id,
                    expected_source_revision,
                    destination_branch_id,
                    expected_destination_revision,
                    destination_grant,
                    discard_live_state,
                )?;
                sink(RpcResponseFrame::Data(encode_rpc_branch(&branch)?))?;
            }
            RpcRequest::DeleteBranch {
                operation_id,
                branch_id,
                expected_revision,
                grant,
                discard_live_state,
            } => {
                control.delete_branch(
                    operation_id,
                    branch_id,
                    expected_revision,
                    grant,
                    discard_live_state,
                )?;
            }
            RpcRequest::BeginPreparation {
                request,
                provenance,
            } => {
                let state = match provenance {
                    ProvenanceBinding::Unattributed => control.begin_preparation(request)?,
                    binding => control.begin_preparation_with_provenance(request, binding)?,
                };
                sink(RpcResponseFrame::Data(encode_rpc_operation_state(&state)?))?;
            }
            RpcRequest::RenewPreparation {
                renewal_id,
                prepared_id,
                grant,
            } => {
                let deadline = control.renew_preparation(renewal_id, prepared_id, grant)?;
                sink(RpcResponseFrame::Data(
                    deadline.as_nanos().to_le_bytes().to_vec(),
                ))?;
            }
            RpcRequest::AuthorizeCaptureChild {
                authorization_id,
                capture_id,
                child_id,
                grant,
            } => {
                control.authorize_capture_child(authorization_id, capture_id, child_id, grant)?;
            }
            RpcRequest::CommitLiveRoots {
                operation_id,
                expected_revision,
                grant,
                roots,
                capture_child,
                native_meta,
            } => {
                let branch = control.commit_live_roots_with_meta(
                    operation_id,
                    expected_revision,
                    grant,
                    roots,
                    capture_child,
                    native_meta,
                )?;
                sink(RpcResponseFrame::Data(encode_rpc_branch(&branch)?))?;
            }
            RpcRequest::SealCapture {
                operation_id,
                capture_id,
                expected_revision,
                grant,
            } => {
                let cap =
                    control.seal_capture(operation_id, capture_id, expected_revision, grant)?;
                sink(RpcResponseFrame::Data(encode_rpc_capture(&cap)))?;
            }
            RpcRequest::PublishCapture {
                capture_id,
                expected_revision,
                expected_parent,
                grant,
                attachment_id,
                volume_epoch,
                descriptor_ref,
                advance_head,
            } => {
                // Verify the complete Generation graph server-side rather than
                // trusting any client-claimed seed or proof.
                let proof = verify_generation(&bridge, descriptor_ref).map_err(|e| {
                    ServiceError::new(ServiceErrorCode::IntegrityError, e.to_string())
                })?;
                let seed = versionfs_core::control::VerifiedGenerationSeed::from_proof(proof);
                let published = control.publish_capture(
                    capture_id,
                    expected_revision,
                    expected_parent,
                    grant,
                    attachment_id,
                    volume_epoch,
                    seed,
                    advance_head,
                )?;
                sink(RpcResponseFrame::Data(encode_rpc_published_generation(
                    &published,
                )?))?;
            }
            RpcRequest::UploadPreparedObject {
                operation_id,
                reference,
                encoded_bytes,
            } => {
                if encoded_bytes.len() > MAX_ENCODED_OBJECT_BYTES {
                    return Err(ServiceError::new(
                        ServiceErrorCode::LimitExceeded,
                        "encoded object exceeds canonical limit",
                    ));
                }
                match control.get_operation(operation_id)? {
                    versionfs_core::control::OperationState::Preparing { .. } => {}
                    versionfs_core::control::OperationState::Aborted => {
                        return Err(ServiceError::new(
                            ServiceErrorCode::Aborted,
                            "preparation was aborted",
                        ));
                    }
                    versionfs_core::control::OperationState::Committed(_) => {
                        return Err(ServiceError::new(
                            ServiceErrorCode::OperationMismatch,
                            "cannot upload objects for an already-committed operation",
                        ));
                    }
                }
                reference.verify(&encoded_bytes)?;
                let mut bridge_mut = bridge;
                bridge_mut
                    .put_durable(reference, &encoded_bytes)
                    .map_err(|msg| ServiceError::new(ServiceErrorCode::BackendError, msg))?;
            }
            RpcRequest::UploadProvenanceContext {
                branch_id,
                grant,
                encoded_context,
            } => {
                let branch = control.get_branch(branch_id)?;
                let current_writer = branch
                    .writer
                    .ok_or_else(|| ServiceError::new(ServiceErrorCode::Fenced, "no writer"))?;
                if current_writer != grant || grant.expires_at <= control.now() {
                    return Err(ServiceError::new(
                        ServiceErrorCode::Fenced,
                        "writer grant is not live for provenance context upload",
                    ));
                }
                let context = ProvenanceContext::decode(&encoded_context)?;
                if context.recording_principal != auth.principal_id {
                    return Err(ServiceError::new(
                        ServiceErrorCode::Unauthorized,
                        "provenance recording principal must match authenticated caller",
                    ));
                }
                // Verify required evidence roots exist and are valid in this workspace's store.
                LogMap::open(&bridge, context.required_logs_root).map_err(map_view_err)?;
                ArtifactMap::open(&bridge, context.required_artifacts_root)
                    .map_err(map_view_err)?;
                let context_ref = ObjectRef::from_encoded(&encoded_context)?;
                let mut bridge_mut = bridge;
                bridge_mut
                    .put_durable(context_ref, &encoded_context)
                    .map_err(|msg| ServiceError::new(ServiceErrorCode::BackendError, msg))?;
                let mut e = versionfs_core::format::CanonicalEncoder::new();
                e.object_ref(context_ref);
                sink(RpcResponseFrame::Data(e.finish()))?;
            }
            RpcRequest::GetOperation { operation_id } => {
                let state = control.get_operation(operation_id)?;
                sink(RpcResponseFrame::Data(encode_rpc_operation_state(&state)?))?;
            }
            RpcRequest::GetCommittedRootTransition { operation_id } => {
                let transition = control.get_committed_root_transition(operation_id)?;
                sink(RpcResponseFrame::Data(encode_rpc_root_transition(
                    transition.as_ref(),
                )?))?;
            }
            RpcRequest::AbortOperation { operation_id } => {
                let state = control.abort_operation(operation_id)?;
                sink(RpcResponseFrame::Data(encode_rpc_operation_state(&state)?))?;
            }
            RpcRequest::ExpirePreparation { operation_id } => {
                let state = control.expire_preparation(operation_id)?;
                sink(RpcResponseFrame::Data(encode_rpc_operation_state(&state)?))?;
            }
            RpcRequest::PinGeneration {
                operation_id,
                generation_id,
                pin_id,
            } => {
                control.pin_generation(operation_id, generation_id, pin_id)?;
            }
            RpcRequest::UnpinGeneration {
                operation_id,
                pin_id,
            } => {
                control.unpin_generation(operation_id, pin_id)?;
            }
            RpcRequest::OpenGeneration {
                operation_id,
                generation_id,
                protection_id,
            } => {
                let deadline =
                    control.open_generation(operation_id, generation_id, protection_id)?;
                sink(RpcResponseFrame::Data(
                    deadline.as_nanos().to_le_bytes().to_vec(),
                ))?;
            }
            RpcRequest::OpenLiveLog {
                operation_id,
                branch_id,
                expected_revision,
                expected_root,
                protection_id,
            } => {
                let deadline = control.open_live_log(
                    operation_id,
                    branch_id,
                    expected_revision,
                    expected_root,
                    protection_id,
                )?;
                sink(RpcResponseFrame::Data(
                    deadline.as_nanos().to_le_bytes().to_vec(),
                ))?;
            }
            RpcRequest::RenewProtection {
                operation_id,
                protection_id,
            } => {
                let deadline = control.renew_protection(operation_id, protection_id)?;
                sink(RpcResponseFrame::Data(
                    deadline.as_nanos().to_le_bytes().to_vec(),
                ))?;
            }
            RpcRequest::CloseProtection {
                operation_id,
                protection_id,
            } => {
                control.close_protection(operation_id, protection_id)?;
            }
            RpcRequest::FetchProtectedObject {
                protection_id,
                reference,
            } => {
                let allow_provenance = !self
                    .config
                    .provenance_deny_principals
                    .contains(&auth.principal_id);
                if !allow_provenance && reference.object_type == ObjectType::ProvenanceContext {
                    return Err(ServiceError::new(
                        ServiceErrorCode::Unauthorized,
                        "caller is not authorized to read provenance context",
                    ));
                }
                let (scope, read_deadline) = begin_protected_read(&control, protection_id)?;
                if !is_object_reachable(&bridge, &control, &scope, reference, allow_provenance)? {
                    return Err(ServiceError::new(
                        ServiceErrorCode::Unauthorized,
                        "requested object is not reachable under the active protection handle",
                    ));
                }
                let bytes = bridge
                    .get_ref(reference)
                    .map_err(|msg| ServiceError::new(ServiceErrorCode::BackendError, msg))?
                    .ok_or_else(|| {
                        ServiceError::new(
                            ServiceErrorCode::NotFound,
                            format!("object {} not found", reference.object_id),
                        )
                    })?;
                reference.verify(&bytes)?;
                finish_protected_read(&control, protection_id, read_deadline)?;
                sink(RpcResponseFrame::Data(bytes))?;
            }
            RpcRequest::ReadBlocksAt {
                protection_id,
                offset,
                length,
            } => {
                if length > MAX_STREAM_READ_BYTES {
                    return Err(ServiceError::new(
                        ServiceErrorCode::LimitExceeded,
                        format!(
                            "read_blocks_at length {length} exceeds maximum {MAX_STREAM_READ_BYTES}"
                        ),
                    ));
                }
                let (scope, read_deadline) = begin_protected_read(&control, protection_id)?;
                let volume_root = match scope {
                    ProtectionScope::GenerationRead { generation_id, .. } => {
                        control.get_generation(generation_id)?.roots.volume
                    }
                    ProtectionScope::AttachmentBase {
                        branch_id, roots, ..
                    } => {
                        let branch = control.get_branch(branch_id).ok();
                        branch.map_or(roots.volume, |b| b.volume_root)
                    }
                    _ => {
                        return Err(ServiceError::new(
                            ServiceErrorCode::InvalidRequest,
                            "protection handle does not cover a block volume",
                        ));
                    }
                };
                let map = load_volume_map(&bridge, volume_root)?;
                let bytes = map
                    .read_range(&bridge, offset, length)
                    .map_err(map_view_err)?;
                for chunk in bytes.chunks(STREAM_CHUNK_BYTES) {
                    finish_protected_read(&control, protection_id, read_deadline)?;
                    sink(RpcResponseFrame::Data(chunk.to_vec()))?;
                }
            }
            RpcRequest::ReadProvenance {
                protection_id,
                generation_id,
            } => {
                let (scope, read_deadline) = begin_protected_read(&control, protection_id)?;
                match scope {
                    ProtectionScope::GenerationRead {
                        generation_id: protected_gen,
                        ..
                    } if protected_gen == generation_id => {}
                    _ => {
                        return Err(ServiceError::new(
                            ServiceErrorCode::InvalidRequest,
                            "protection handle does not match requested Generation",
                        ));
                    }
                }
                let seed = control.get_generation(generation_id)?;
                let desc_bytes = bridge
                    .get_ref(seed.descriptor)
                    .map_err(|msg| ServiceError::new(ServiceErrorCode::BackendError, msg))?
                    .ok_or_else(|| {
                        ServiceError::new(
                            ServiceErrorCode::IntegrityError,
                            "missing Generation descriptor",
                        )
                    })?;
                seed.descriptor.verify(&desc_bytes)?;
                let decoded = decode_generation_descriptor(&desc_bytes)?;
                let expected_principal = match decoded.context_root() {
                    Some(ctx_ref) => control.get_provenance_context_principal(ctx_ref.object_id)?,
                    None => None,
                };
                let caller_authorized = !self
                    .config
                    .provenance_deny_principals
                    .contains(&auth.principal_id);
                let inspection = inspect_generation_provenance(
                    &bridge,
                    generation_id,
                    seed.descriptor,
                    caller_authorized,
                    expected_principal,
                )?;
                finish_protected_read(&control, protection_id, read_deadline)?;
                sink(RpcResponseFrame::Data(encode_rpc_provenance_inspection(
                    &inspection,
                )?))?;
            }
            RpcRequest::DiffGenerations {
                left_protection_id,
                left_generation_id,
                right_protection_id,
                right_generation_id,
            } => {
                let (left_scope, left_deadline) =
                    begin_protected_read(&control, left_protection_id)?;
                let (right_scope, right_deadline) =
                    begin_protected_read(&control, right_protection_id)?;
                match (&left_scope, &right_scope) {
                    (
                        ProtectionScope::GenerationRead {
                            generation_id: l, ..
                        },
                        ProtectionScope::GenerationRead {
                            generation_id: r, ..
                        },
                    ) if *l == left_generation_id && *r == right_generation_id => {}
                    _ => {
                        return Err(ServiceError::new(
                            ServiceErrorCode::InvalidRequest,
                            "protection handles do not match requested Generations",
                        ));
                    }
                }
                let left_seed = control.get_generation(left_generation_id)?;
                let right_seed = control.get_generation(right_generation_id)?;

                let left_vol = load_volume_map(&bridge, left_seed.roots.volume)?;
                let right_vol = load_volume_map(&bridge, right_seed.roots.volume)?;
                let extent_ranges = left_vol
                    .diff_extents(&right_vol, &bridge)
                    .map_err(map_view_err)?;
                for chunk in extent_ranges.chunks(MAX_DIFF_CHUNK_ITEMS) {
                    finish_protected_read(&control, left_protection_id, left_deadline)?;
                    finish_protected_read(&control, right_protection_id, right_deadline)?;
                    sink(RpcResponseFrame::Data(encode_rpc_diff_chunk(
                        &DiffStreamChunk::Extents(chunk.to_vec()),
                    )?))?;
                }

                let left_logs =
                    LogMap::open(&bridge, left_seed.roots.logs).map_err(map_view_err)?;
                let right_logs =
                    LogMap::open(&bridge, right_seed.roots.logs).map_err(map_view_err)?;
                let log_changes = left_logs
                    .diff_logs(&right_logs, &bridge)
                    .map_err(map_view_err)?;
                for chunk in log_changes.chunks(MAX_DIFF_CHUNK_ITEMS) {
                    finish_protected_read(&control, left_protection_id, left_deadline)?;
                    finish_protected_read(&control, right_protection_id, right_deadline)?;
                    sink(RpcResponseFrame::Data(encode_rpc_diff_chunk(
                        &DiffStreamChunk::Logs(chunk.to_vec()),
                    )?))?;
                }

                let left_arts =
                    ArtifactMap::open(&bridge, left_seed.roots.artifacts).map_err(map_view_err)?;
                let right_arts =
                    ArtifactMap::open(&bridge, right_seed.roots.artifacts).map_err(map_view_err)?;
                let art_changes = left_arts
                    .diff_artifacts(&right_arts, &bridge)
                    .map_err(map_view_err)?;
                for chunk in art_changes.chunks(MAX_DIFF_CHUNK_ITEMS) {
                    finish_protected_read(&control, left_protection_id, left_deadline)?;
                    finish_protected_read(&control, right_protection_id, right_deadline)?;
                    sink(RpcResponseFrame::Data(encode_rpc_diff_chunk(
                        &DiffStreamChunk::Artifacts(chunk.to_vec()),
                    )?))?;
                }
            }
            RpcRequest::ReadLogAt {
                protection_id,
                name,
                from_seq,
                max_batches,
            } => {
                if max_batches > MAX_LOG_READ_BATCHES {
                    return Err(ServiceError::new(
                        ServiceErrorCode::LimitExceeded,
                        format!("max_batches {max_batches} exceeds maximum {MAX_LOG_READ_BATCHES}"),
                    ));
                }
                let (scope, read_deadline) = begin_protected_read(&control, protection_id)?;
                let logs_root = match scope {
                    ProtectionScope::GenerationRead { generation_id, .. } => {
                        control.get_generation(generation_id)?.roots.logs
                    }
                    ProtectionScope::LiveLogRead { root, .. } => root,
                    _ => {
                        return Err(ServiceError::new(
                            ServiceErrorCode::InvalidRequest,
                            "protection handle does not cover a log map",
                        ));
                    }
                };
                let log_map = LogMap::open(&bridge, logs_root).map_err(map_view_err)?;
                let Some(entry) = log_map.get(&bridge, &name).map_err(map_view_err)? else {
                    return Err(ServiceError::new(
                        ServiceErrorCode::NotFound,
                        format!("log {name} not found"),
                    ));
                };
                if from_seq < entry.first_seq {
                    return Err(ServiceError {
                        code: ServiceErrorCode::CursorExpired,
                        message: format!(
                            "cursor {from_seq} precedes retained first_seq {}",
                            entry.first_seq
                        ),
                        detail_u64_a: from_seq,
                        detail_u64_b: entry.first_seq,
                    });
                }
                // Frame 0: LogMapEntry metadata
                sink(RpcResponseFrame::Data(entry.encode()?))?;
                if max_batches > 0 && from_seq < entry.end_seq {
                    let seg_index = LogSegmentIndex::from_root(entry.segment_index_root)
                        .map_err(map_view_err)?;
                    let selected = seg_index
                        .entries_from(&bridge, from_seq, max_batches)
                        .map_err(map_view_err)?;
                    for (seg_first, seg_entry) in selected {
                        finish_protected_read(&control, protection_id, read_deadline)?;
                        let bytes = bridge
                            .get_ref(seg_entry.segment)
                            .map_err(|msg| ServiceError::new(ServiceErrorCode::BackendError, msg))?
                            .ok_or_else(|| {
                                ServiceError::new(
                                    ServiceErrorCode::IntegrityError,
                                    "missing log segment",
                                )
                            })?;
                        seg_entry.segment.verify(&bytes)?;
                        let segment = versionfs_core::format::LogSegment::decode(&bytes)?;
                        if segment.log_id != entry.log_id
                            || segment.first_seq != seg_first
                            || segment.end_seq != seg_entry.end_seq
                        {
                            return Err(ServiceError::new(
                                ServiceErrorCode::IntegrityError,
                                "log segment metadata mismatch",
                            ));
                        }
                        let (first_seq, records) = if from_seq > segment.first_seq {
                            let skip =
                                usize::try_from(from_seq - segment.first_seq).map_err(|_| {
                                    ServiceError::new(
                                        ServiceErrorCode::IntegrityError,
                                        "segment slice overflow",
                                    )
                                })?;
                            if skip >= segment.records.len() {
                                return Err(ServiceError::new(
                                    ServiceErrorCode::IntegrityError,
                                    "invalid segment record slice",
                                ));
                            }
                            let mut records = segment.records;
                            let suffix = records.split_off(skip);
                            (from_seq, suffix)
                        } else {
                            (segment.first_seq, segment.records)
                        };
                        finish_protected_read(&control, protection_id, read_deadline)?;
                        sink(RpcResponseFrame::Data(encode_rpc_log_batch(
                            segment.log_id,
                            first_seq,
                            segment.end_seq,
                            &records,
                            &segment.attachments,
                        )?))?;
                    }
                }
            }
            RpcRequest::ListLogs { protection_id } => {
                let (scope, read_deadline) = begin_protected_read(&control, protection_id)?;
                let logs_root = match scope {
                    ProtectionScope::GenerationRead { generation_id, .. } => {
                        control.get_generation(generation_id)?.roots.logs
                    }
                    ProtectionScope::LiveLogRead { root, .. } => root,
                    _ => {
                        return Err(ServiceError::new(
                            ServiceErrorCode::InvalidRequest,
                            "protection handle does not cover a log map",
                        ));
                    }
                };
                let entries = LogMap::open(&bridge, logs_root)
                    .map_err(map_view_err)?
                    .entries(&bridge)
                    .map_err(map_view_err)?;
                finish_protected_read(&control, protection_id, read_deadline)?;
                sink(RpcResponseFrame::Data(encode_rpc_log_list(&entries)?))?;
            }
            RpcRequest::ReadArtifact {
                protection_id,
                name,
                offset,
                length,
            } => {
                if length > MAX_STREAM_READ_BYTES {
                    return Err(ServiceError::new(
                        ServiceErrorCode::LimitExceeded,
                        format!(
                            "read_artifact length {length} exceeds maximum {MAX_STREAM_READ_BYTES}"
                        ),
                    ));
                }
                let (scope, read_deadline) = begin_protected_read(&control, protection_id)?;
                let artifacts_root = match scope {
                    ProtectionScope::GenerationRead { generation_id, .. } => {
                        control.get_generation(generation_id)?.roots.artifacts
                    }
                    _ => {
                        return Err(ServiceError::new(
                            ServiceErrorCode::InvalidRequest,
                            "protection handle does not cover an artifact map",
                        ));
                    }
                };
                let map = ArtifactMap::open(&bridge, artifacts_root).map_err(map_view_err)?;
                let Some(descriptor) = map.get(&bridge, &name).map_err(map_view_err)? else {
                    return Err(ServiceError::new(
                        ServiceErrorCode::NotFound,
                        format!("artifact {name} not found"),
                    ));
                };
                if offset > descriptor.logical_len {
                    return Err(ServiceError::new(
                        ServiceErrorCode::InvalidGeometry,
                        "artifact read offset exceeds logical_len",
                    ));
                }
                let bytes = read_bounded_artifact_slice(
                    &bridge,
                    &control,
                    protection_id,
                    read_deadline,
                    &descriptor,
                    offset,
                    length,
                )?;
                sink(RpcResponseFrame::Data(descriptor.encode()?))?;
                for chunk in bytes.chunks(STREAM_CHUNK_BYTES) {
                    finish_protected_read(&control, protection_id, read_deadline)?;
                    sink(RpcResponseFrame::Data(chunk.to_vec()))?;
                }
            }
            RpcRequest::ListArtifacts { protection_id } => {
                let (scope, read_deadline) = begin_protected_read(&control, protection_id)?;
                let artifacts_root = match scope {
                    ProtectionScope::GenerationRead { generation_id, .. } => {
                        control.get_generation(generation_id)?.roots.artifacts
                    }
                    _ => {
                        return Err(ServiceError::new(
                            ServiceErrorCode::InvalidRequest,
                            "protection handle does not cover an artifact map",
                        ));
                    }
                };
                let entries = ArtifactMap::open(&bridge, artifacts_root)
                    .map_err(map_view_err)?
                    .entries(&bridge)
                    .map_err(map_view_err)?;
                finish_protected_read(&control, protection_id, read_deadline)?;
                sink(RpcResponseFrame::Data(encode_rpc_artifact_list(&entries)?))?;
            }
            RpcRequest::CollectReport | RpcRequest::GetGcReport => {
                return Err(ServiceError::new(
                    ServiceErrorCode::UnsupportedOperation,
                    "P10 retention and mark-only GC reports are not implemented yet",
                ));
            }
        }
        Ok(())
    }
}

const MAX_CONCURRENT_CONNECTIONS: usize = 128;

struct ActiveConnectionGuard(Arc<std::sync::atomic::AtomicUsize>);

impl Drop for ActiveConnectionGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

fn try_acquire_connection(
    active: &Arc<std::sync::atomic::AtomicUsize>,
) -> Option<ActiveConnectionGuard> {
    let mut current = active.load(Ordering::Acquire);
    loop {
        if current >= MAX_CONCURRENT_CONNECTIONS {
            return None;
        }
        match active.compare_exchange_weak(
            current,
            current + 1,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => return Some(ActiveConnectionGuard(Arc::clone(active))),
            Err(observed) => current = observed,
        }
    }
}

/// Serves [`WorkspaceService`] over a bound [`TcpListener`] until `shutdown` is set.
pub fn serve_tcp<A, O>(
    service: Arc<WorkspaceService<A, O>>,
    listener: TcpListener,
    shutdown: Arc<AtomicBool>,
) -> std::io::Result<()>
where
    A: MetadataAuthority + Send + Sync + 'static,
    O: ObjectStore + Send + Sync + 'static,
{
    listener.set_nonblocking(true)?;
    let active = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    while !shutdown.load(Ordering::Acquire) {
        match listener.accept() {
            Ok((stream, _addr)) => {
                let Some(guard) = try_acquire_connection(&active) else {
                    drop(stream);
                    continue;
                };
                let _ = stream.set_nonblocking(false);
                let svc = Arc::clone(&service);
                std::thread::spawn(move || {
                    let _guard = guard;
                    handle_tcp_connection(svc, stream);
                });
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(std::time::Duration::from_millis(20));
            }
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

fn handle_tcp_connection<A, O>(service: Arc<WorkspaceService<A, O>>, mut stream: TcpStream)
where
    A: MetadataAuthority + Send + Sync + 'static,
    O: ObjectStore + Send + Sync + 'static,
{
    let _ = stream.set_read_timeout(Some(std::time::Duration::from_secs(30)));
    let _ = stream.set_write_timeout(Some(std::time::Duration::from_secs(30)));
    let writer_stream = match stream.try_clone() {
        Ok(s) => s,
        Err(_) => return,
    };
    let mut writer = BufWriter::new(writer_stream);
    while let Ok(raw_req) = read_frame(&mut stream) {
        match RpcEnvelope::decode(&raw_req) {
            Ok(env) => {
                service.handle_request(env, &mut |frame| {
                    let encoded = frame.encode()?;
                    write_frame(&mut writer, &encoded)
                });
            }
            Err(err) => {
                if let Ok(encoded) = RpcResponseFrame::Error(err).encode() {
                    let _ = write_frame(&mut writer, &encoded);
                }
            }
        }
    }
}

/// Serves [`WorkspaceService`] over a bound Unix domain [`UnixListener`] until `shutdown` is set.
#[cfg(unix)]
pub fn serve_unix<A, O>(
    service: Arc<WorkspaceService<A, O>>,
    listener: UnixListener,
    shutdown: Arc<AtomicBool>,
) -> std::io::Result<()>
where
    A: MetadataAuthority + Send + Sync + 'static,
    O: ObjectStore + Send + Sync + 'static,
{
    listener.set_nonblocking(true)?;
    let active = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    while !shutdown.load(Ordering::Acquire) {
        match listener.accept() {
            Ok((stream, _addr)) => {
                let Some(guard) = try_acquire_connection(&active) else {
                    drop(stream);
                    continue;
                };
                let _ = stream.set_nonblocking(false);
                let svc = Arc::clone(&service);
                std::thread::spawn(move || {
                    let _guard = guard;
                    handle_unix_connection(svc, stream);
                });
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(std::time::Duration::from_millis(20));
            }
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

#[cfg(unix)]
fn handle_unix_connection<A, O>(service: Arc<WorkspaceService<A, O>>, mut stream: UnixStream)
where
    A: MetadataAuthority + Send + Sync + 'static,
    O: ObjectStore + Send + Sync + 'static,
{
    let _ = stream.set_read_timeout(Some(std::time::Duration::from_secs(30)));
    let _ = stream.set_write_timeout(Some(std::time::Duration::from_secs(30)));
    let writer_stream = match stream.try_clone() {
        Ok(s) => s,
        Err(_) => return,
    };
    let mut writer = BufWriter::new(writer_stream);
    while let Ok(raw_req) = read_frame(&mut stream) {
        match RpcEnvelope::decode(&raw_req) {
            Ok(env) => {
                service.handle_request(env, &mut |frame| {
                    let encoded = frame.encode()?;
                    write_frame(&mut writer, &encoded)
                });
            }
            Err(err) => {
                if let Ok(encoded) = RpcResponseFrame::Error(err).encode() {
                    let _ = write_frame(&mut writer, &encoded);
                }
            }
        }
    }
}

struct ServiceObjectBridge<O> {
    store: Arc<O>,
    workspace_id: WorkspaceId,
    scope: ObjectStoreScopeId,
    runtime: Handle,
}

impl<O: ObjectStore> MapObjectStore for ServiceObjectBridge<O> {
    fn proof_scope(&self) -> ObjectStoreScopeId {
        self.scope
    }

    fn get(&self, _id: ObjectId) -> Result<Option<Vec<u8>>, String> {
        Err("typed ObjectRef is required for workspace-scoped object reads".to_owned())
    }

    fn get_ref(&self, reference: ObjectRef) -> Result<Option<Vec<u8>>, String> {
        let store = Arc::clone(&self.store);
        let workspace_id = self.workspace_id;
        self.runtime.block_on(async move {
            match store.get(workspace_id, reference).await {
                Ok(bytes) => Ok(Some(bytes)),
                Err(ObjectStoreError::NotFound { .. }) => Ok(None),
                Err(err) => Err(err.to_string()),
            }
        })
    }

    fn put(&mut self, id: ObjectId, bytes: Vec<u8>) -> Result<(), String> {
        let reference = ObjectRef::from_encoded(&bytes).map_err(|e| e.to_string())?;
        if reference.object_id != id {
            return Err("object ID does not match encoded bytes".to_owned());
        }
        self.put_durable(reference, &bytes)
    }
}

impl<O: ObjectStore> DurableObjectStore for ServiceObjectBridge<O> {
    fn put_durable(&mut self, reference: ObjectRef, encoded: &[u8]) -> Result<(), String> {
        reference.verify(encoded).map_err(|e| e.to_string())?;
        let store = Arc::clone(&self.store);
        let workspace_id = self.workspace_id;
        let encoded = encoded.to_vec();
        self.runtime.block_on(async move {
            let stored = store
                .put(workspace_id, &encoded)
                .await
                .map_err(|e| e.to_string())?;
            if stored.object_ref != reference {
                return Err("uploaded object reference mismatch".to_owned());
            }
            Ok(())
        })
    }
}

fn begin_protected_read<A: MetadataAuthority>(
    control: &WorkspaceControl<A>,
    protection_id: ProtectionId,
) -> Result<(ProtectionScope, MonotonicTime), ServiceError> {
    let scope = control.inspect_active_protection(protection_id)?;
    let expires_at = match &scope {
        ProtectionScope::GenerationRead { expires_at, .. }
        | ProtectionScope::LiveLogRead { expires_at, .. }
        | ProtectionScope::AttachmentBase { expires_at, .. }
        | ProtectionScope::Preparation { expires_at, .. } => *expires_at,
    };
    let read_deadline = expires_at.min(
        control
            .now()
            .saturating_add_nanos(REMOTE_READ_DEADLINE_NANOS),
    );
    Ok((scope, read_deadline))
}

fn finish_protected_read<A: MetadataAuthority>(
    control: &WorkspaceControl<A>,
    protection_id: ProtectionId,
    read_deadline: MonotonicTime,
) -> Result<(), ServiceError> {
    let _ = control.inspect_active_protection(protection_id)?;
    if control.now() > read_deadline {
        return Err(ServiceError::new(
            ServiceErrorCode::LeaseExpired,
            "remote read exceeded bounded protection deadline",
        ));
    }
    Ok(())
}

fn is_object_reachable<A: MetadataAuthority, O: ObjectStore>(
    bridge: &ServiceObjectBridge<O>,
    control: &WorkspaceControl<A>,
    scope: &ProtectionScope,
    target: ObjectRef,
    allow_provenance: bool,
) -> Result<bool, ServiceError> {
    let mut roots: Vec<ObjectRef> = Vec::new();
    match scope {
        ProtectionScope::GenerationRead { generation_id, .. } => {
            roots.push(control.get_generation(*generation_id)?.descriptor);
        }
        ProtectionScope::LiveLogRead { root, .. } => {
            roots.push(*root);
        }
        ProtectionScope::AttachmentBase {
            branch_id,
            generation_id,
            roots: base_roots,
            ..
        } => {
            roots.push(control.get_generation(*generation_id)?.descriptor);
            roots.push(base_roots.volume);
            roots.push(base_roots.logs);
            roots.push(base_roots.artifacts);
            if let Ok(branch) = control.get_branch(*branch_id) {
                roots.push(branch.volume_root);
                roots.push(branch.logs_root);
                roots.push(branch.artifacts_root);
            }
        }
        ProtectionScope::Preparation { dependencies, .. } => {
            roots.extend_from_slice(dependencies);
        }
    }

    let mut visited = HashSet::new();
    let mut stack = roots;
    while let Some(current) = stack.pop() {
        if current == target {
            return Ok(true);
        }
        if !visited.insert(current.object_id) {
            continue;
        }
        if !can_contain(current.object_type, target.object_type) {
            continue;
        }
        let Some(bytes) = bridge
            .get_ref(current)
            .map_err(|msg| ServiceError::new(ServiceErrorCode::BackendError, msg))?
        else {
            continue;
        };
        if current.verify(&bytes).is_err() {
            continue;
        }
        match current.object_type {
            ObjectType::GenerationDescriptor => {
                if let Ok(desc) = decode_generation_descriptor(&bytes) {
                    stack.push(desc.volume_root());
                    stack.push(desc.logs_root());
                    stack.push(desc.artifacts_root());
                    if allow_provenance && let Some(ctx) = desc.context_root() {
                        stack.push(ctx);
                    }
                }
            }
            ObjectType::ProvenanceContext => {
                if allow_provenance && let Ok(ctx) = ProvenanceContext::decode(&bytes) {
                    stack.push(ctx.required_logs_root);
                    stack.push(ctx.required_artifacts_root);
                }
            }
            ObjectType::VolumeRoot => {
                if let Ok(vr) = versionfs_core::format::VolumeRoot::decode(&bytes) {
                    stack.push(vr.extent_map_root);
                }
            }
            ObjectType::VolumeMapNode => {
                let tree = PersistentBTree::from_root(current).map_err(|e| {
                    ServiceError::new(ServiceErrorCode::IntegrityError, e.to_string())
                })?;
                if target.object_type == ObjectType::VolumeMapNode
                    && let Ok(contents) = tree.entries_and_nodes(bridge)
                {
                    if contents.node_refs.contains(&target) {
                        return Ok(true);
                    }
                } else if let Ok(entries) = tree.entries(bridge) {
                    for (_, val) in entries {
                        if val.len() >= 42 {
                            // Volume extent leaf values are canonical ObjectRefs.
                            if let Ok(ext_ref) = decode_extent_leaf(&val)
                                && ext_ref == target
                            {
                                return Ok(true);
                            }
                        }
                    }
                }
            }
            ObjectType::LogMapRoot => {
                if let Ok(lr) = versionfs_core::format::LogMapRoot::decode(&bytes) {
                    stack.push(lr.log_index_root);
                }
            }
            ObjectType::LogMapNode => {
                let tree = PersistentBTree::from_root(current).map_err(|e| {
                    ServiceError::new(ServiceErrorCode::IntegrityError, e.to_string())
                })?;
                if let Ok(contents) = tree.entries_and_nodes(bridge) {
                    if contents.node_refs.contains(&target) {
                        return Ok(true);
                    }
                    for (_, val) in contents.entries {
                        if let Ok(entry) = LogMapEntry::decode(&val) {
                            stack.push(entry.segment_index_root);
                        }
                    }
                }
            }
            ObjectType::LogSegmentIndexNode => {
                let tree = PersistentBTree::from_root(current).map_err(|e| {
                    ServiceError::new(ServiceErrorCode::IntegrityError, e.to_string())
                })?;
                if let Ok(contents) = tree.entries_and_nodes(bridge) {
                    if contents.node_refs.contains(&target) {
                        return Ok(true);
                    }
                    for (_, val) in contents.entries {
                        if let Ok(entry) = versionfs_core::format::SegmentIndexEntry::decode(&val) {
                            stack.push(entry.segment);
                        }
                    }
                }
            }
            ObjectType::LogSegment => {
                if let Ok(seg) = versionfs_core::format::LogSegment::decode(&bytes) {
                    stack.extend(seg.attachments);
                }
            }
            ObjectType::ArtifactMapRoot => {
                if let Ok(ar) = versionfs_core::format::ArtifactMapRoot::decode(&bytes) {
                    stack.push(ar.artifact_index_root);
                }
            }
            ObjectType::ArtifactMapNode => {
                let tree = PersistentBTree::from_root(current).map_err(|e| {
                    ServiceError::new(ServiceErrorCode::IntegrityError, e.to_string())
                })?;
                if let Ok(contents) = tree.entries_and_nodes(bridge) {
                    if contents.node_refs.contains(&target) {
                        return Ok(true);
                    }
                    for (_, val) in contents.entries {
                        if let Ok(desc) = ArtifactDescriptor::decode(&val) {
                            stack.push(desc.content_root);
                            stack.extend(desc.dependencies);
                        }
                    }
                }
            }
            ObjectType::ArtifactIndexNode => {
                let tree = PersistentBTree::from_root(current).map_err(|e| {
                    ServiceError::new(ServiceErrorCode::IntegrityError, e.to_string())
                })?;
                if let Ok(contents) = tree.entries_and_nodes(bridge) {
                    if contents.node_refs.contains(&target) {
                        return Ok(true);
                    }
                    for (_, val) in contents.entries {
                        if let Ok(entry) = versionfs_core::format::ArtifactChunkEntry::decode(&val)
                        {
                            stack.push(entry.chunk);
                        }
                    }
                }
            }
            ObjectType::Extent | ObjectType::ArtifactBlob | ObjectType::ArtifactChunk => {}
        }
    }
    Ok(false)
}

fn can_contain(container: ObjectType, target: ObjectType) -> bool {
    match container {
        ObjectType::GenerationDescriptor => true,
        ObjectType::ProvenanceContext => matches!(
            target,
            ObjectType::LogMapRoot
                | ObjectType::LogMapNode
                | ObjectType::LogSegmentIndexNode
                | ObjectType::LogSegment
                | ObjectType::ArtifactMapRoot
                | ObjectType::ArtifactMapNode
                | ObjectType::ArtifactIndexNode
                | ObjectType::ArtifactBlob
                | ObjectType::ArtifactChunk
        ),
        ObjectType::VolumeRoot => matches!(target, ObjectType::VolumeMapNode | ObjectType::Extent),
        ObjectType::VolumeMapNode => {
            matches!(target, ObjectType::VolumeMapNode | ObjectType::Extent)
        }
        ObjectType::LogMapRoot | ObjectType::LogMapNode => matches!(
            target,
            ObjectType::LogMapNode
                | ObjectType::LogSegmentIndexNode
                | ObjectType::LogSegment
                | ObjectType::ArtifactBlob
                | ObjectType::ArtifactChunk
        ),
        ObjectType::LogSegmentIndexNode => matches!(
            target,
            ObjectType::LogSegmentIndexNode
                | ObjectType::LogSegment
                | ObjectType::ArtifactBlob
                | ObjectType::ArtifactChunk
        ),
        ObjectType::LogSegment => {
            matches!(target, ObjectType::ArtifactBlob | ObjectType::ArtifactChunk)
        }
        ObjectType::ArtifactMapRoot | ObjectType::ArtifactMapNode => matches!(
            target,
            ObjectType::ArtifactMapNode
                | ObjectType::ArtifactIndexNode
                | ObjectType::ArtifactBlob
                | ObjectType::ArtifactChunk
        ),
        ObjectType::ArtifactIndexNode => matches!(
            target,
            ObjectType::ArtifactIndexNode | ObjectType::ArtifactChunk
        ),
        ObjectType::Extent | ObjectType::ArtifactBlob | ObjectType::ArtifactChunk => false,
    }
}

fn decode_extent_leaf(bytes: &[u8]) -> Result<ObjectRef, FormatError> {
    if bytes.len() != 42 {
        return Err(FormatError::ObjectReferenceMismatch);
    }
    let object_type = ObjectType::try_from(u16::from_le_bytes([bytes[0], bytes[1]]))?;
    let object_id = ObjectId::from_bytes(bytes[2..34].try_into().unwrap());
    let encoded_len = u64::from_le_bytes(bytes[34..42].try_into().unwrap());
    if object_type != ObjectType::Extent || encoded_len != u64::from(EXTENT_BYTES) + 16 {
        return Err(FormatError::ObjectReferenceMismatch);
    }
    Ok(ObjectRef {
        object_type,
        object_id,
        encoded_len,
    })
}

fn read_bounded_artifact_slice<A: MetadataAuthority, O: ObjectStore>(
    bridge: &ServiceObjectBridge<O>,
    control: &WorkspaceControl<A>,
    protection_id: ProtectionId,
    read_deadline: MonotonicTime,
    descriptor: &ArtifactDescriptor,
    offset: u64,
    length: u64,
) -> Result<Vec<u8>, ServiceError> {
    let available = descriptor.logical_len.saturating_sub(offset);
    let to_read = length.min(available);
    if to_read == 0 {
        return Ok(Vec::new());
    }
    let to_read_usize = usize::try_from(to_read)
        .map_err(|_| ServiceError::new(ServiceErrorCode::LimitExceeded, "read too large"))?;
    if descriptor.logical_len <= versionfs_core::format::MAX_ARTIFACT_BLOB_BYTES as u64 {
        let bytes = bridge
            .get_ref(descriptor.content_root)
            .map_err(|msg| ServiceError::new(ServiceErrorCode::BackendError, msg))?
            .ok_or_else(|| {
                ServiceError::new(ServiceErrorCode::IntegrityError, "missing artifact blob")
            })?;
        descriptor.content_root.verify(&bytes)?;
        let payload = decode_artifact_blob(&bytes)?;
        if payload.len() as u64 != descriptor.logical_len {
            return Err(ServiceError::new(
                ServiceErrorCode::IntegrityError,
                "artifact blob length mismatch",
            ));
        }
        let start = offset as usize;
        return Ok(payload[start..start + to_read_usize].to_vec());
    }

    let chunk_index =
        ArtifactChunkIndex::from_root(descriptor.content_root).map_err(map_view_err)?;
    let entries = chunk_index.entries(bridge).map_err(map_view_err)?;
    let mut output = Vec::new();
    output
        .try_reserve_exact(to_read_usize)
        .map_err(|_| ServiceError::new(ServiceErrorCode::LimitExceeded, "allocation failed"))?;
    let mut cursor = 0_u64;
    let end = offset + to_read;
    for (expected_idx, (chunk_num, entry)) in entries.into_iter().enumerate() {
        if chunk_num != expected_idx as u64 {
            return Err(ServiceError::new(
                ServiceErrorCode::IntegrityError,
                "non-consecutive artifact chunks",
            ));
        }
        let chunk_start = cursor;
        let chunk_end = cursor + u64::from(entry.actual_len);
        cursor = chunk_end;
        if chunk_end <= offset {
            continue;
        }
        if chunk_start >= end {
            break;
        }
        finish_protected_read(control, protection_id, read_deadline)?;
        let bytes = bridge
            .get_ref(entry.chunk)
            .map_err(|msg| ServiceError::new(ServiceErrorCode::BackendError, msg))?
            .ok_or_else(|| {
                ServiceError::new(ServiceErrorCode::IntegrityError, "missing artifact chunk")
            })?;
        entry.chunk.verify(&bytes)?;
        let payload = decode_artifact_chunk(&bytes)?;
        if payload.len() as u32 != entry.actual_len {
            return Err(ServiceError::new(
                ServiceErrorCode::IntegrityError,
                "artifact chunk length mismatch",
            ));
        }
        let slice_start = offset.saturating_sub(chunk_start) as usize;
        let slice_end = (end.min(chunk_end) - chunk_start) as usize;
        output.extend_from_slice(&payload[slice_start..slice_end]);
    }
    Ok(output)
}

fn create_empty_generation_on_store<O: ObjectStore>(
    bridge: &mut ServiceObjectBridge<O>,
    generation_id: GenerationId,
    boundary_id: versionfs_core::identity::VolumeBoundaryId,
    geometry: VolumeGeometry,
    created_at_unix_ms: u64,
) -> Result<versionfs_core::control::VerifiedGenerationSeed, ServiceError> {
    let mut staging = MemStagingStore::new(bridge.scope);
    let volume_root = VolumeExtentMap::empty(geometry, &mut staging)
        .map_err(map_view_err)?
        .seal(&mut staging)
        .map_err(map_view_err)?;
    let logs_root = LogMap::empty(&mut staging)
        .map_err(map_view_err)?
        .seal(&mut staging)
        .map_err(map_view_err)?;
    let artifacts_root = ArtifactMap::empty(&mut staging)
        .map_err(map_view_err)?
        .seal(&mut staging)
        .map_err(map_view_err)?;
    let descriptor_bytes = versionfs_core::format::GenerationDescriptorV2 {
        generation_id,
        parent_generation_id: None,
        volume_root,
        logs_root,
        artifacts_root,
        provenance: ProvenanceBinding::Unattributed,
        capture_mode: versionfs_core::format::CaptureMode::BlockCut,
        volume_boundary_id: boundary_id,
        created_at_unix_ms,
    }
    .encode()?;
    let descriptor_ref = ObjectRef::from_encoded(&descriptor_bytes)?;
    staging
        .put(descriptor_ref.object_id, descriptor_bytes)
        .map_err(|msg| ServiceError::new(ServiceErrorCode::BackendError, msg))?;
    for (id, bytes) in staging.objects {
        let obj_ref = ObjectRef::from_encoded(&bytes)?;
        if obj_ref.object_id != id {
            return Err(ServiceError::new(
                ServiceErrorCode::IntegrityError,
                "staged object ID mismatch",
            ));
        }
        bridge
            .put_durable(obj_ref, &bytes)
            .map_err(|msg| ServiceError::new(ServiceErrorCode::BackendError, msg))?;
    }
    let proof = verify_generation(bridge, descriptor_ref)
        .map_err(|e| ServiceError::new(ServiceErrorCode::IntegrityError, e.to_string()))?;
    Ok(versionfs_core::control::VerifiedGenerationSeed::from_proof(
        proof,
    ))
}

struct MemStagingStore {
    scope: ObjectStoreScopeId,
    objects: std::collections::HashMap<ObjectId, Vec<u8>>,
}

impl MemStagingStore {
    fn new(scope: ObjectStoreScopeId) -> Self {
        Self {
            scope,
            objects: std::collections::HashMap::new(),
        }
    }
}

impl MapObjectStore for MemStagingStore {
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

fn load_volume_map<O: ObjectStore>(
    bridge: &ServiceObjectBridge<O>,
    volume_root_ref: ObjectRef,
) -> Result<VolumeExtentMap, ServiceError> {
    let bytes = bridge
        .get_ref(volume_root_ref)
        .map_err(|msg| ServiceError::new(ServiceErrorCode::BackendError, msg))?
        .ok_or_else(|| {
            ServiceError::new(
                ServiceErrorCode::IntegrityError,
                format!("missing VolumeRoot {}", volume_root_ref.object_id),
            )
        })?;
    volume_root_ref.verify(&bytes)?;
    let root = VolumeRoot::decode(&bytes)?;
    VolumeExtentMap::from_root(root).map_err(map_view_err)
}

fn map_view_err(err: ViewError) -> ServiceError {
    match err {
        ViewError::Store(msg) => ServiceError::new(ServiceErrorCode::BackendError, msg),
        ViewError::MissingObject(_) => {
            ServiceError::new(ServiceErrorCode::IntegrityError, err.to_string())
        }
        ViewError::Format(f) => f.into(),
        ViewError::Map(m) => ServiceError::new(ServiceErrorCode::IntegrityError, m.to_string()),
    }
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0_u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}
