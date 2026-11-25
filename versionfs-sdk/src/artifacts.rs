//! Native named artifacts and common-cut producer gating for checkpoint capture.
//!
//! Implements RFC-002 Section 4.4 and Section 6.6:
//! - Explicit artifact identities (`ArtifactId`, never inferred from path)
//! - Inline `ArtifactBlob` (`<= 64 KiB`) and 4 MiB `ArtifactChunk` + `ArtifactChunkIndex` (`> 64
//!   KiB`) representations
//! - Gated local staging overlay connected to P7's [`ProducerGate`] so named artifacts and native
//!   logs participate in one common-cut checkpoint protocol without parsing ext4 or recopying
//!   historical log segments.

use std::collections::BTreeMap;
use std::time::Duration;
use thiserror::Error;
use versionfs_core::control::{
    BranchState, ControlError, LiveRoots, NativeRootTransitionMeta, PreparationKind,
};
use versionfs_core::format::{
    ARTIFACT_CHUNK_BYTES, ArtifactChunkEntry, ArtifactDescriptor, FormatError,
    MAX_ARTIFACT_BLOB_BYTES, ObjectRef, ObjectType,
};
use versionfs_core::identity::{ArtifactId, BranchId, GenerationId, OperationId, ProtectionId};
use versionfs_core::persistent_map::{DurableObjectStore, MapObjectStore};
use versionfs_core::views::{
    ArtifactChunkIndex, ArtifactMap, ViewError, decode_artifact_blob, decode_artifact_chunk,
    encode_artifact_blob, encode_artifact_chunk,
};

use crate::barrier::VolumeBarrierControl;
use crate::capture::ProducerGate;
use crate::logs::{
    DEFAULT_NATIVE_RENEWAL_INTERVAL, NativeAdmissionGate, NativeAdmissionGuard, NativeCommitError,
    NativeControl, NativeWriteContext, ReadProtectionGuard, StagingObjectStore,
    commit_native_root_mutation, ensure_store_scope, is_unknown_control_error, push_len_bytes,
    push_object_ref, sha256_domain,
};

impl From<NativeCommitError> for ArtifactError {
    fn from(error: NativeCommitError) -> Self {
        match error {
            NativeCommitError::Control(e) => Self::Control(e),
            NativeCommitError::ObjectStore(msg) => Self::ObjectStore(msg),
        }
    }
}

fn verify_dependency_objects_in_store<S: MapObjectStore>(
    store: &S,
    dependencies: &[ObjectRef],
) -> Result<(), ArtifactError> {
    for dep in dependencies {
        if !matches!(
            dep.object_type,
            ObjectType::ArtifactBlob | ObjectType::ArtifactChunk
        ) {
            return Err(FormatError::InvalidNativeView(
                "artifact dependencies must reference immutable payload objects",
            )
            .into());
        }
        let bytes = store
            .get_ref(*dep)
            .map_err(ArtifactError::ObjectStore)?
            .ok_or(ViewError::MissingObject(dep.object_id))?;
        dep.verify(&bytes)?;
        match dep.object_type {
            ObjectType::ArtifactBlob => {
                decode_artifact_blob(&bytes)?;
            }
            ObjectType::ArtifactChunk => {
                decode_artifact_chunk(&bytes)?;
            }
            _ => unreachable!(),
        }
    }
    Ok(())
}

/// Errors returned by native named-artifact operations.
#[derive(Debug, Error)]
pub enum ArtifactError {
    /// Native artifact admission is closed while a checkpoint capture drains/seals.
    #[error("native artifact mutation admission is closed during capture")]
    AdmissionClosed,
    /// Artifact name was empty.
    #[error("artifact name must not be empty")]
    InvalidName,
    /// The named artifact does not exist in the targeted artifact view.
    #[error("artifact '{0}' not found")]
    NotFound(String),
    /// The stored artifact chunks do not cover the declared `logical_len`.
    #[error("artifact payload length mismatch: declared {declared}, verified {actual}")]
    LengthMismatch {
        /// Declared logical length in [`ArtifactDescriptor`].
        declared: u64,
        /// Actual verified payload bytes read.
        actual: u64,
    },
    /// Canonical format validation failed.
    #[error(transparent)]
    Format(#[from] FormatError),
    /// Persistent-view traversal or sealing failed.
    #[error(transparent)]
    View(#[from] ViewError),
    /// Metadata Authority control error.
    #[error(transparent)]
    Control(#[from] ControlError),
    /// Durable object-store failure.
    #[error("object store error: {0}")]
    ObjectStore(String),
}

/// Prepared immutable payload representation ready to bind into an
/// [`ArtifactDescriptor`] or explicit log attachment edges.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedArtifactPayload {
    /// Exact logical byte length.
    pub logical_len: u64,
    /// `ArtifactBlob` (`<= 64 KiB`) or `ArtifactIndexNode` (`> 64 KiB`).
    pub content_root: ObjectRef,
    /// Explicit immutable payload object refs (`ArtifactBlob` or `ArtifactChunk`s)
    /// suitable for log attachment edges or artifact dependencies.
    pub payload_refs: Vec<ObjectRef>,
}

/// Encodes and durably stores a payload using `ArtifactBlob` (`<= 64 KiB`) or
/// 4 MiB `ArtifactChunk`s indexed by [`ArtifactChunkIndex`] (`> 64 KiB`).
///
/// The returned `payload_refs` can be passed directly as explicit `attachments`
/// to [`crate::logs::LogClient::append`], or bound into an [`ArtifactDescriptor`].
pub fn store_durable_artifact_payload<S: DurableObjectStore>(
    store: &mut S,
    payload: &[u8],
) -> Result<PreparedArtifactPayload, ArtifactError> {
    let (prepared, staged) = {
        let mut staging = StagingObjectStore::new(store);
        let prepared = stage_artifact_payload(&mut staging, payload)?;
        (prepared, staging.into_staged())
    };
    for (reference, encoded) in staged {
        store
            .put_durable(reference, &encoded)
            .map_err(ArtifactError::ObjectStore)?;
    }
    Ok(prepared)
}

/// Encodes a payload into a staging store as either a single `ArtifactBlob`
/// (`<= 64 KiB`) or 4 MiB `ArtifactChunk` objects + [`ArtifactChunkIndex`].
fn stage_artifact_payload<S: MapObjectStore>(
    store: &mut S,
    payload: &[u8],
) -> Result<PreparedArtifactPayload, ArtifactError> {
    let logical_len = payload.len() as u64;
    if payload.len() <= MAX_ARTIFACT_BLOB_BYTES {
        let encoded = encode_artifact_blob(payload)?;
        let blob_ref = ObjectRef::from_encoded(&encoded)?;
        store
            .put(blob_ref.object_id, encoded)
            .map_err(ArtifactError::ObjectStore)?;
        return Ok(PreparedArtifactPayload {
            logical_len,
            content_root: blob_ref,
            payload_refs: vec![blob_ref],
        });
    }

    let mut chunk_index = ArtifactChunkIndex::empty(store)?;
    let mut chunk_refs = Vec::with_capacity(payload.len().div_ceil(ARTIFACT_CHUNK_BYTES));
    for (idx, slice) in payload.chunks(ARTIFACT_CHUNK_BYTES).enumerate() {
        let encoded = encode_artifact_chunk(slice)?;
        let chunk_ref = ObjectRef::from_encoded(&encoded)?;
        store
            .put(chunk_ref.object_id, encoded)
            .map_err(ArtifactError::ObjectStore)?;
        chunk_index = chunk_index.insert(
            store,
            idx as u64,
            ArtifactChunkEntry {
                chunk: chunk_ref,
                actual_len: slice.len() as u32,
            },
        )?;
        chunk_refs.push(chunk_ref);
    }
    Ok(PreparedArtifactPayload {
        logical_len,
        content_root: chunk_index.root(),
        payload_refs: chunk_refs,
    })
}

/// Reads and verifies the full payload of an [`ArtifactDescriptor`] (bounded by
/// `logical_len`), checking every blob or 4 MiB chunk object digest and framing.
pub fn read_artifact_payload<S: MapObjectStore>(
    store: &S,
    descriptor: &ArtifactDescriptor,
) -> Result<Vec<u8>, ArtifactError> {
    read_artifact_payload_with_guard(store, descriptor, &mut || Ok(()))
}

fn read_artifact_payload_with_guard<S: MapObjectStore>(
    store: &S,
    descriptor: &ArtifactDescriptor,
    ensure_protected: &mut dyn FnMut() -> Result<(), ControlError>,
) -> Result<Vec<u8>, ArtifactError> {
    ensure_protected()?;
    if descriptor.logical_len <= MAX_ARTIFACT_BLOB_BYTES as u64 {
        let bytes = store
            .get_ref(descriptor.content_root)
            .map_err(ArtifactError::ObjectStore)?
            .ok_or(ViewError::MissingObject(descriptor.content_root.object_id))?;
        descriptor.content_root.verify(&bytes)?;
        let payload = decode_artifact_blob(&bytes)?;
        if payload.len() as u64 != descriptor.logical_len {
            return Err(ArtifactError::LengthMismatch {
                declared: descriptor.logical_len,
                actual: payload.len() as u64,
            });
        }
        return Ok(payload);
    }

    let chunk_index = ArtifactChunkIndex::from_root(descriptor.content_root)?;
    let entries = chunk_index.entries(store)?;
    // Validate consecutive chunk indices and total indexed length BEFORE
    // reserving memory so a malformed descriptor (e.g. logical_len = 2^63)
    // returns an error instead of panicking in `Vec::with_capacity`.
    let mut indexed_total: u64 = 0;
    for (expected_idx, (chunk_num, entry)) in entries.iter().enumerate() {
        if *chunk_num != expected_idx as u64 {
            return Err(FormatError::InvalidNativeView("non-consecutive artifact chunks").into());
        }
        indexed_total = indexed_total
            .checked_add(u64::from(entry.actual_len))
            .ok_or(ArtifactError::LengthMismatch {
                declared: descriptor.logical_len,
                actual: u64::MAX,
            })?;
    }
    if indexed_total != descriptor.logical_len {
        return Err(ArtifactError::LengthMismatch {
            declared: descriptor.logical_len,
            actual: indexed_total,
        });
    }
    let expected_len =
        usize::try_from(descriptor.logical_len).map_err(|_| ArtifactError::LengthMismatch {
            declared: descriptor.logical_len,
            actual: 0,
        })?;
    let mut output = Vec::new();
    output
        .try_reserve_exact(expected_len)
        .map_err(|_| ArtifactError::LengthMismatch {
            declared: descriptor.logical_len,
            actual: 0,
        })?;
    for (_, entry) in entries {
        ensure_protected()?;
        let bytes = store
            .get_ref(entry.chunk)
            .map_err(ArtifactError::ObjectStore)?
            .ok_or(ViewError::MissingObject(entry.chunk.object_id))?;
        entry.chunk.verify(&bytes)?;
        let payload = decode_artifact_chunk(&bytes)?;
        if payload.len() as u32 != entry.actual_len {
            return Err(ArtifactError::LengthMismatch {
                declared: u64::from(entry.actual_len),
                actual: payload.len() as u64,
            });
        }
        let next_len =
            output
                .len()
                .checked_add(payload.len())
                .ok_or(ArtifactError::LengthMismatch {
                    declared: descriptor.logical_len,
                    actual: u64::MAX,
                })?;
        if next_len > expected_len {
            return Err(ArtifactError::LengthMismatch {
                declared: descriptor.logical_len,
                actual: next_len as u64,
            });
        }
        output.extend_from_slice(&payload);
    }
    if output.len() != expected_len {
        return Err(ArtifactError::LengthMismatch {
            declared: descriptor.logical_len,
            actual: output.len() as u64,
        });
    }
    Ok(output)
}

/// Reads and verifies one explicit log attachment object (`ArtifactBlob` or `ArtifactChunk`).
pub fn read_attachment_object<S: MapObjectStore>(
    store: &S,
    reference: ObjectRef,
) -> Result<Vec<u8>, ArtifactError> {
    let bytes = store
        .get_ref(reference)
        .map_err(ArtifactError::ObjectStore)?
        .ok_or(ViewError::MissingObject(reference.object_id))?;
    reference.verify(&bytes)?;
    match reference.object_type {
        ObjectType::ArtifactBlob => Ok(decode_artifact_blob(&bytes)?),
        ObjectType::ArtifactChunk => Ok(decode_artifact_chunk(&bytes)?),
        _ => Err(FormatError::ObjectReferenceMismatch.into()),
    }
}

/// Reads a named artifact's descriptor and verified payload from an immutable
/// `artifacts_root` without mounting the block volume.
pub fn read_artifact_from_root<S: MapObjectStore>(
    store: &S,
    artifacts_root: ObjectRef,
    name: &str,
) -> Result<(ArtifactDescriptor, Vec<u8>), ArtifactError> {
    if name.is_empty() {
        return Err(ArtifactError::InvalidName);
    }
    let map = ArtifactMap::open(store, artifacts_root)?;
    let Some(descriptor) = map.get(store, name)? else {
        return Err(ArtifactError::NotFound(name.to_owned()));
    };
    let payload = read_artifact_payload(store, &descriptor)?;
    Ok((descriptor, payload))
}

/// Lists all named artifacts in an immutable `artifacts_root` without mounting.
pub fn list_artifacts_from_root<S: MapObjectStore>(
    store: &S,
    artifacts_root: ObjectRef,
) -> Result<Vec<(String, ArtifactDescriptor)>, ArtifactError> {
    let map = ArtifactMap::open(store, artifacts_root)?;
    Ok(map.entries(store)?)
}

/// Client for named-artifact mutations and protected historical reads.
pub struct ArtifactClient {
    admission: Option<NativeAdmissionGate>,
    renewal_interval: Duration,
}

impl Default for ArtifactClient {
    fn default() -> Self {
        Self::new()
    }
}

impl ArtifactClient {
    /// Creates an ungated native artifact client.
    pub const fn new() -> Self {
        Self {
            admission: None,
            renewal_interval: DEFAULT_NATIVE_RENEWAL_INTERVAL,
        }
    }

    /// Creates a native artifact client connected to a shared [`NativeAdmissionGate`].
    pub fn with_admission(admission: NativeAdmissionGate) -> Self {
        Self {
            admission: Some(admission),
            renewal_interval: DEFAULT_NATIVE_RENEWAL_INTERVAL,
        }
    }

    /// Overrides the preparation renewal interval used during long object uploads.
    pub const fn with_renewal_interval(mut self, interval: Duration) -> Self {
        self.renewal_interval = interval;
        self
    }

    fn enter_admission(&self) -> Result<Option<NativeAdmissionGuard>, ArtifactError> {
        match &self.admission {
            Some(gate) => gate
                .try_enter()
                .map(Some)
                .ok_or(ArtifactError::AdmissionClosed),
            None => Ok(None),
        }
    }

    /// Durably stores and commits a named artifact with explicit [`ArtifactId`]
    /// and optional extra payload `dependencies` into the branch's live `artifacts_root`.
    #[allow(clippy::too_many_arguments)]
    pub fn put_artifact<S, C, F>(
        &self,
        store: &mut S,
        control: &C,
        ctx: NativeWriteContext,
        operation_id: OperationId,
        protection_id: ProtectionId,
        name: &str,
        artifact_id: ArtifactId,
        payload: &[u8],
        dependencies: Vec<ObjectRef>,
        next_renewal_id: F,
    ) -> Result<(ArtifactDescriptor, BranchState), ArtifactError>
    where
        S: DurableObjectStore,
        C: VolumeBarrierControl,
        F: FnMut() -> OperationId,
    {
        if name.is_empty() {
            return Err(ArtifactError::InvalidName);
        }
        ensure_store_scope(store, control)?;

        // Compute expected content_root and logical request fingerprint before
        // checking committed replay or requiring mutation admission so exact
        // retries succeed even after admission closes or GC runs, while
        // changed-name or changed-payload retries return `OperationMismatch`.
        let expected_prepared = {
            let mut probe = StagingObjectStore::new(store);
            stage_artifact_payload(&mut probe, payload)?
        };
        let expected_descriptor = ArtifactDescriptor {
            artifact_id,
            logical_len: expected_prepared.logical_len,
            content_root: expected_prepared.content_root,
            dependencies: dependencies.clone(),
        };
        let req_fp = fingerprint_put_artifact(ctx.branch_id, name, &expected_descriptor)?;

        if let Some(transition) = control.get_committed_root_transition(operation_id)? {
            let Some(meta) = &transition.native_meta else {
                return Err(ArtifactError::Control(ControlError::OperationMismatch));
            };
            if transition.kind != PreparationKind::ArtifactMutation
                || transition.branch.branch_id != ctx.branch_id
                || meta.request_fingerprint != req_fp
            {
                return Err(ArtifactError::Control(ControlError::OperationMismatch));
            }
            return Ok((expected_descriptor, transition.branch));
        }

        verify_dependency_objects_in_store(store, &dependencies)?;
        let _guard = self.enter_admission()?;

        let branch = control.get_branch(ctx.branch_id)?;
        let (descriptor, next_artifacts_root, staged, mut prep_deps) = {
            let mut staging = StagingObjectStore::new(store);
            let prepared = stage_artifact_payload(&mut staging, payload)?;
            let descriptor = ArtifactDescriptor {
                artifact_id,
                logical_len: prepared.logical_len,
                content_root: prepared.content_root,
                dependencies: dependencies.clone(),
            };
            let map = ArtifactMap::open(&staging, branch.artifacts_root)?;
            let next_map = map.insert(&mut staging, name, &descriptor)?;
            let root = next_map.seal(&mut staging)?;
            let mut prep_deps = Vec::with_capacity(dependencies.len() + 2);
            prep_deps.push(root);
            prep_deps.push(prepared.content_root);
            prep_deps.extend(dependencies);
            (descriptor, root, staging.into_staged(), prep_deps)
        };
        prep_deps.dedup();

        let next_roots = LiveRoots {
            volume: branch.volume_root,
            logs: branch.logs_root,
            artifacts: next_artifacts_root,
        };
        let committed = commit_native_root_mutation(
            store,
            control,
            ctx,
            operation_id,
            protection_id,
            branch.revision,
            PreparationKind::ArtifactMutation,
            next_roots,
            prep_deps,
            staged,
            Some(NativeRootTransitionMeta {
                request_fingerprint: req_fp,
                receipt_bytes: Vec::new(),
                claim_log_id: None,
            }),
            self.renewal_interval,
            next_renewal_id,
        )?;
        Ok((descriptor, committed))
    }

    /// Removes a named artifact from the branch's live `artifacts_root`.
    #[allow(clippy::too_many_arguments)]
    pub fn delete_artifact<S, C, F>(
        &self,
        store: &mut S,
        control: &C,
        ctx: NativeWriteContext,
        operation_id: OperationId,
        protection_id: ProtectionId,
        name: &str,
        next_renewal_id: F,
    ) -> Result<BranchState, ArtifactError>
    where
        S: DurableObjectStore,
        C: VolumeBarrierControl,
        F: FnMut() -> OperationId,
    {
        if name.is_empty() {
            return Err(ArtifactError::InvalidName);
        }
        ensure_store_scope(store, control)?;
        let req_fp = fingerprint_delete_artifact(ctx.branch_id, name);

        if let Some(transition) = control.get_committed_root_transition(operation_id)? {
            let Some(meta) = &transition.native_meta else {
                return Err(ArtifactError::Control(ControlError::OperationMismatch));
            };
            if transition.kind != PreparationKind::ArtifactMutation
                || transition.branch.branch_id != ctx.branch_id
                || meta.request_fingerprint != req_fp
            {
                return Err(ArtifactError::Control(ControlError::OperationMismatch));
            }
            return Ok(transition.branch);
        }

        let _guard = self.enter_admission()?;

        let branch = control.get_branch(ctx.branch_id)?;
        let (next_artifacts_root, staged) = {
            let mut staging = StagingObjectStore::new(store);
            let map = ArtifactMap::open(&staging, branch.artifacts_root)?;
            if map.get(&staging, name)?.is_none() {
                if let Some(transition) = control.get_committed_root_transition(operation_id)? {
                    let Some(meta) = &transition.native_meta else {
                        return Err(ArtifactError::Control(ControlError::OperationMismatch));
                    };
                    if transition.kind == PreparationKind::ArtifactMutation
                        && transition.branch.branch_id == ctx.branch_id
                        && meta.request_fingerprint == req_fp
                    {
                        return Ok(transition.branch);
                    }
                    return Err(ArtifactError::Control(ControlError::OperationMismatch));
                }
                return Err(ArtifactError::NotFound(name.to_owned()));
            }
            let next_map = map.remove(&mut staging, name)?;
            let root = next_map.seal(&mut staging)?;
            (root, staging.into_staged())
        };

        let next_roots = LiveRoots {
            volume: branch.volume_root,
            logs: branch.logs_root,
            artifacts: next_artifacts_root,
        };
        let committed = commit_native_root_mutation(
            store,
            control,
            ctx,
            operation_id,
            protection_id,
            branch.revision,
            PreparationKind::ArtifactMutation,
            next_roots,
            vec![next_artifacts_root],
            staged,
            Some(NativeRootTransitionMeta {
                request_fingerprint: req_fp,
                receipt_bytes: Vec::new(),
                claim_log_id: None,
            }),
            self.renewal_interval,
            next_renewal_id,
        )?;
        Ok(committed)
    }

    /// Reads a named artifact from a historical Generation under a bounded
    /// Generation read protection, without mounting the block volume.
    #[allow(clippy::too_many_arguments)]
    pub fn read_historical_artifact<S, C, F>(
        &self,
        store: &S,
        control: &C,
        generation_id: GenerationId,
        name: &str,
        open_op_id: OperationId,
        protection_id: ProtectionId,
        close_op_id: OperationId,
        next_renewal_id: F,
    ) -> Result<(ArtifactDescriptor, Vec<u8>), ArtifactError>
    where
        S: MapObjectStore,
        C: NativeControl,
        F: FnMut() -> OperationId,
    {
        if name.is_empty() {
            return Err(ArtifactError::InvalidName);
        }
        ensure_store_scope(store, control)?;
        let deadline = control.open_generation(open_op_id, generation_id, protection_id)?;
        let mut guard = ReadProtectionGuard::new(control, protection_id, deadline, next_renewal_id);
        let read_result = (|| {
            guard.ensure_live()?;
            let seed = control.get_generation(generation_id)?;
            read_artifact_from_root_with_guard(store, seed.roots.artifacts, name, &mut || {
                guard.ensure_live()
            })
        })();
        let _ = control.close_protection(close_op_id, protection_id);
        read_result
    }
}

fn read_artifact_from_root_with_guard<S: MapObjectStore>(
    store: &S,
    artifacts_root: ObjectRef,
    name: &str,
    ensure_protected: &mut dyn FnMut() -> Result<(), ControlError>,
) -> Result<(ArtifactDescriptor, Vec<u8>), ArtifactError> {
    ensure_protected()?;
    let map = ArtifactMap::open(store, artifacts_root)?;
    let Some(descriptor) = map.get(store, name)? else {
        return Err(ArtifactError::NotFound(name.to_owned()));
    };
    let payload = read_artifact_payload_with_guard(store, &descriptor, ensure_protected)?;
    Ok((descriptor, payload))
}

fn fingerprint_put_artifact(
    branch_id: BranchId,
    name: &str,
    descriptor: &ArtifactDescriptor,
) -> Result<[u8; 32], FormatError> {
    let mut buf = Vec::new();
    buf.extend_from_slice(branch_id.as_bytes());
    push_len_bytes(&mut buf, name.as_bytes());
    push_len_bytes(&mut buf, &descriptor.encode()?);
    Ok(sha256_domain(b"versionfs.native.put_artifact.v1", &buf))
}

fn fingerprint_delete_artifact(branch_id: BranchId, name: &str) -> [u8; 32] {
    let mut buf = Vec::new();
    buf.extend_from_slice(branch_id.as_bytes());
    push_len_bytes(&mut buf, name.as_bytes());
    sha256_domain(b"versionfs.native.delete_artifact.v1", &buf)
}

fn fingerprint_staged_flush(
    branch_id: BranchId,
    staged: &BTreeMap<String, StagedArtifactChange>,
) -> Result<[u8; 32], FormatError> {
    let mut buf = Vec::new();
    buf.extend_from_slice(branch_id.as_bytes());
    buf.extend_from_slice(&(staged.len() as u64).to_le_bytes());
    for (name, change) in staged {
        push_len_bytes(&mut buf, name.as_bytes());
        match change {
            StagedArtifactChange::Put {
                artifact_id,
                payload,
                dependencies,
            } => {
                buf.push(1);
                buf.extend_from_slice(artifact_id.as_bytes());
                push_len_bytes(&mut buf, payload);
                buf.extend_from_slice(&(dependencies.len() as u64).to_le_bytes());
                for dep in dependencies {
                    push_object_ref(&mut buf, *dep);
                }
            }
            StagedArtifactChange::Remove => {
                buf.push(0);
            }
        }
    }
    Ok(sha256_domain(b"versionfs.native.staged_flush.v1", &buf))
}

struct PendingStagedFlush {
    operation_id: OperationId,
    protection_id: ProtectionId,
    expected_revision: u64,
    next_roots: LiveRoots,
    prep_deps: Vec<ObjectRef>,
    staged_objects: Vec<(ObjectRef, Vec<u8>)>,
    native_meta: NativeRootTransitionMeta,
    changes: BTreeMap<String, StagedArtifactChange>,
}

/// One locally staged named-artifact mutation waiting to be flushed into the
/// branch's live `artifacts_root` before checkpoint seal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StagedArtifactChange {
    /// Insert or replace a named artifact with explicit [`ArtifactId`].
    Put {
        /// Explicit artifact identity (never inferred from name or path).
        artifact_id: ArtifactId,
        /// Payload bytes (encoded as blob `<= 64 KiB` or 4 MiB chunks `> 64 KiB`).
        payload: Vec<u8>,
        /// Optional explicit payload dependencies.
        dependencies: Vec<ObjectRef>,
    },
    /// Remove a named artifact from the live artifact map.
    Remove,
}

/// Gated local named-artifact overlay and common-cut [`ProducerGate`] connecting
/// both native logs and staged named artifacts to P7's
/// [`crate::capture::CheckpointCoordinator`].
///
/// When P7 closes admission and calls `drain_producers()`, this gate:
/// 1. Waits for any in-flight native log or artifact mutations to finish; and
/// 2. Atomically commits any locally staged named-artifact changes into the branch's
///    `artifacts_root` via `PreparationKind::ArtifactMutation`.
///
/// If a drain-time commit advances the branch revision after P7's pre-drain
/// snapshot, P7's existing revision CAS restarts Step 1 cleanly with a fresh
/// capture ID and sees the updated `logs_root` and `artifacts_root` — no second
/// checkpoint-coordination protocol is needed.
pub struct NativeProducerGate<S, C, ID> {
    admission: NativeAdmissionGate,
    store: S,
    control: C,
    ctx: NativeWriteContext,
    staged: BTreeMap<String, StagedArtifactChange>,
    pending_flush: Option<PendingStagedFlush>,
    renewal_interval: Duration,
    next_ids: ID,
}

impl<S, C, ID> NativeProducerGate<S, C, ID>
where
    S: DurableObjectStore,
    C: VolumeBarrierControl,
    ID: FnMut() -> (OperationId, ProtectionId),
{
    /// Creates a new common-cut producer gate bound to `admission`, `store`,
    /// `control`, and `ctx`.
    pub fn new(
        admission: NativeAdmissionGate,
        store: S,
        control: C,
        ctx: NativeWriteContext,
        next_ids: ID,
    ) -> Self {
        Self {
            admission,
            store,
            control,
            ctx,
            staged: BTreeMap::new(),
            pending_flush: None,
            renewal_interval: DEFAULT_NATIVE_RENEWAL_INTERVAL,
            next_ids,
        }
    }

    /// Updates the writer grant held by this producer gate after a heartbeat renewal.
    pub const fn set_writer_grant(&mut self, grant: versionfs_core::control::WriterGrant) {
        self.ctx.grant = grant;
    }

    /// Returns a handle to the shared [`NativeAdmissionGate`] so [`crate::logs::LogClient`]
    /// and [`ArtifactClient`] share the exact same capture admission gate.
    pub fn admission(&self) -> NativeAdmissionGate {
        self.admission.clone()
    }

    /// Stages a named artifact put in the local overlay (gated by capture admission).
    pub fn stage_put(
        &mut self,
        name: impl Into<String>,
        artifact_id: ArtifactId,
        payload: Vec<u8>,
        dependencies: Vec<ObjectRef>,
    ) -> Result<(), ArtifactError> {
        let name = name.into();
        if name.is_empty() {
            return Err(ArtifactError::InvalidName);
        }
        // Reject invalid dependency object types before mutating the overlay
        // so an invalid call never clobbers a previously staged valid change.
        for dependency in &dependencies {
            if !matches!(
                dependency.object_type,
                ObjectType::ArtifactBlob | ObjectType::ArtifactChunk
            ) {
                return Err(FormatError::ObjectReferenceMismatch.into());
            }
        }
        let _guard = self
            .admission
            .try_enter()
            .ok_or(ArtifactError::AdmissionClosed)?;
        self.staged.insert(
            name,
            StagedArtifactChange::Put {
                artifact_id,
                payload,
                dependencies,
            },
        );
        Ok(())
    }

    /// Stages a named artifact removal in the local overlay (gated by capture admission).
    pub fn stage_remove(&mut self, name: impl Into<String>) -> Result<(), ArtifactError> {
        let name = name.into();
        if name.is_empty() {
            return Err(ArtifactError::InvalidName);
        }
        let _guard = self
            .admission
            .try_enter()
            .ok_or(ArtifactError::AdmissionClosed)?;
        self.staged.insert(name, StagedArtifactChange::Remove);
        Ok(())
    }

    /// Returns the number of locally staged named-artifact changes awaiting flush
    /// (including any in-flight flush awaiting reconciliation after an unknown outcome).
    pub fn staged_len(&self) -> usize {
        self.staged.len()
            + self
                .pending_flush
                .as_ref()
                .map_or(0, |pending| pending.changes.len())
    }

    fn restore_changes_under_staged(&mut self, changes: BTreeMap<String, StagedArtifactChange>) {
        for (name, change) in changes {
            self.staged.entry(name).or_insert(change);
        }
    }

    /// Flushes all locally staged named-artifact changes into the branch's live
    /// `artifacts_root` in one atomic `PreparationKind::ArtifactMutation` commit.
    ///
    /// If a previous flush returned an unknown authority outcome, this method
    /// reconciles that exact original `operation_id` first before retiring its
    /// staged changes or allocating a fresh operation ID for newer edits.
    pub fn flush_staged_artifacts(&mut self) -> Result<Option<BranchState>, ArtifactError> {
        ensure_store_scope(&self.store, &self.control)?;

        let mut last_committed: Option<BranchState> = None;

        // 1. Reconcile any prior flush whose authority outcome was unknown.
        if let Some(pending) = self.pending_flush.take() {
            match self
                .control
                .get_committed_root_transition(pending.operation_id)
            {
                Ok(Some(transition)) => {
                    let matches = transition.kind == PreparationKind::ArtifactMutation
                        && transition.branch.branch_id == self.ctx.branch_id
                        && transition.native_meta.as_ref().is_some_and(|m| {
                            m.request_fingerprint == pending.native_meta.request_fingerprint
                        });
                    if !matches {
                        self.pending_flush = Some(pending);
                        return Err(ArtifactError::Control(ControlError::OperationMismatch));
                    }
                    last_committed = Some(transition.branch);
                }
                Err(ControlError::Aborted) => {
                    self.restore_changes_under_staged(pending.changes);
                }
                Ok(None) => {
                    // Either still `Preparing` or never admitted (`NotFound`):
                    // finish or retry using the exact original operation ID,
                    // protection ID, and staged request.
                    let retry_result = commit_native_root_mutation(
                        &mut self.store,
                        &self.control,
                        self.ctx,
                        pending.operation_id,
                        pending.protection_id,
                        pending.expected_revision,
                        PreparationKind::ArtifactMutation,
                        pending.next_roots,
                        pending.prep_deps.clone(),
                        pending.staged_objects.clone(),
                        Some(pending.native_meta.clone()),
                        self.renewal_interval,
                        || (self.next_ids)().0,
                    );
                    match retry_result {
                        Ok(committed) => {
                            last_committed = Some(committed);
                        }
                        Err(NativeCommitError::Control(err)) if is_unknown_control_error(&err) => {
                            self.pending_flush = Some(pending);
                            return Err(ArtifactError::Control(err));
                        }
                        Err(err) => {
                            // Definite failure: `commit_native_root_mutation`
                            // has aborted any active preparation; restore
                            // un-overridden changes so a subsequent retry can
                            // re-stage against the latest branch revision.
                            self.restore_changes_under_staged(pending.changes);
                            return Err(err.into());
                        }
                    }
                }
                Err(err) => {
                    self.pending_flush = Some(pending);
                    return Err(ArtifactError::Control(err));
                }
            }
        }

        // 2. Flush any remaining locally staged changes.
        if self.staged.is_empty() {
            return Ok(last_committed);
        }
        for change in self.staged.values() {
            if let StagedArtifactChange::Put { dependencies, .. } = change {
                verify_dependency_objects_in_store(&self.store, dependencies)?;
            }
        }
        let branch = self.control.get_branch(self.ctx.branch_id)?;
        let (next_artifacts_root, staged_objects, mut prep_deps) = {
            let mut staging = StagingObjectStore::new(&self.store);
            let mut map = ArtifactMap::open(&staging, branch.artifacts_root)?;
            let mut prep_deps = Vec::new();
            for (name, change) in &self.staged {
                match change {
                    StagedArtifactChange::Put {
                        artifact_id,
                        payload,
                        dependencies,
                    } => {
                        let prepared = stage_artifact_payload(&mut staging, payload)?;
                        let descriptor = ArtifactDescriptor {
                            artifact_id: *artifact_id,
                            logical_len: prepared.logical_len,
                            content_root: prepared.content_root,
                            dependencies: dependencies.clone(),
                        };
                        map = map.insert(&mut staging, name, &descriptor)?;
                        prep_deps.push(prepared.content_root);
                        prep_deps.extend(dependencies.iter().copied());
                    }
                    StagedArtifactChange::Remove => {
                        if map.get(&staging, name)?.is_some() {
                            map = map.remove(&mut staging, name)?;
                        }
                    }
                }
            }
            let root = map.seal(&mut staging)?;
            prep_deps.push(root);
            (root, staging.into_staged(), prep_deps)
        };
        prep_deps.dedup();

        let req_fp = fingerprint_staged_flush(self.ctx.branch_id, &self.staged)?;
        let native_meta = NativeRootTransitionMeta {
            request_fingerprint: req_fp,
            receipt_bytes: Vec::new(),
            claim_log_id: None,
        };
        let (operation_id, protection_id) = (self.next_ids)();
        let next_roots = LiveRoots {
            volume: branch.volume_root,
            logs: branch.logs_root,
            artifacts: next_artifacts_root,
        };
        let changes = std::mem::take(&mut self.staged);
        let pending = PendingStagedFlush {
            operation_id,
            protection_id,
            expected_revision: branch.revision,
            next_roots,
            prep_deps: prep_deps.clone(),
            staged_objects: staged_objects.clone(),
            native_meta: native_meta.clone(),
            changes,
        };
        match commit_native_root_mutation(
            &mut self.store,
            &self.control,
            self.ctx,
            operation_id,
            protection_id,
            branch.revision,
            PreparationKind::ArtifactMutation,
            next_roots,
            prep_deps,
            staged_objects,
            Some(native_meta),
            self.renewal_interval,
            || (self.next_ids)().0,
        ) {
            Ok(committed) => Ok(Some(committed)),
            Err(NativeCommitError::Control(err)) if is_unknown_control_error(&err) => {
                self.pending_flush = Some(pending);
                Err(ArtifactError::Control(err))
            }
            Err(err) => {
                self.restore_changes_under_staged(pending.changes);
                Err(err.into())
            }
        }
    }
}

impl<S, C, ID> ProducerGate for NativeProducerGate<S, C, ID>
where
    S: DurableObjectStore,
    C: VolumeBarrierControl,
    ID: FnMut() -> (OperationId, ProtectionId),
{
    fn close_admission(&mut self) -> Result<(), String> {
        self.admission.close();
        Ok(())
    }

    fn drain_producers(&mut self) -> Result<(), String> {
        self.admission.drain();
        self.flush_staged_artifacts()
            .map_err(|error| error.to_string())?;
        Ok(())
    }

    fn reopen_admission(&mut self) -> Result<(), String> {
        self.admission.reopen();
        Ok(())
    }
}
