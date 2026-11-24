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
use versionfs_core::control::{BranchState, ControlError, LiveRoots, PreparationKind};
use versionfs_core::format::{
    ARTIFACT_CHUNK_BYTES, ArtifactChunkEntry, ArtifactDescriptor, FormatError,
    MAX_ARTIFACT_BLOB_BYTES, ObjectRef, ObjectType,
};
use versionfs_core::identity::{ArtifactId, GenerationId, OperationId, ProtectionId};
use versionfs_core::persistent_map::{DurableObjectStore, MapObjectStore};
use versionfs_core::views::{
    ArtifactChunkIndex, ArtifactMap, ViewError, decode_artifact_blob, decode_artifact_chunk,
    encode_artifact_blob, encode_artifact_chunk,
};

use crate::barrier::VolumeBarrierControl;
use crate::capture::ProducerGate;
use crate::logs::{
    DEFAULT_NATIVE_RENEWAL_INTERVAL, NativeAdmissionGate, NativeAdmissionGuard, NativeCommitError,
    NativeControl, NativeWriteContext, StagingObjectStore, commit_native_root_mutation,
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
    let mut output = Vec::with_capacity(usize::try_from(descriptor.logical_len).map_err(|_| {
        ArtifactError::LengthMismatch {
            declared: descriptor.logical_len,
            actual: 0,
        }
    })?);
    for (expected_idx, (chunk_num, entry)) in entries.into_iter().enumerate() {
        if chunk_num != expected_idx as u64 {
            return Err(FormatError::InvalidNativeView("non-consecutive artifact chunks").into());
        }
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
        output.extend_from_slice(&payload);
    }
    if output.len() as u64 != descriptor.logical_len {
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
        verify_dependency_objects_in_store(store, &dependencies)?;
        let _guard = self.enter_admission()?;

        // Compute expected content_root before checking committed replay so
        // same-length different-payload retries are rejected with OperationMismatch.
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

        if let Some(transition) = control.get_committed_root_transition(operation_id)? {
            if transition.kind != PreparationKind::ArtifactMutation
                || transition.branch.branch_id != ctx.branch_id
            {
                return Err(ArtifactError::Control(ControlError::OperationMismatch));
            }
            let mut replay_probe = StagingObjectStore::new(store);
            let prev_map = ArtifactMap::open(&replay_probe, transition.previous_roots.artifacts)?;
            let expected_next_root = prev_map
                .insert(&mut replay_probe, name, &expected_descriptor)?
                .seal(&mut replay_probe)?;
            if expected_next_root != transition.branch.artifacts_root {
                return Err(ArtifactError::Control(ControlError::OperationMismatch));
            }
            return Ok((expected_descriptor, transition.branch));
        }

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
        let _guard = self.enter_admission()?;

        if let Some(transition) = control.get_committed_root_transition(operation_id)? {
            if transition.kind != PreparationKind::ArtifactMutation
                || transition.branch.branch_id != ctx.branch_id
            {
                return Err(ArtifactError::Control(ControlError::OperationMismatch));
            }
            let mut replay_probe = StagingObjectStore::new(store);
            let prev_map = ArtifactMap::open(&replay_probe, transition.previous_roots.artifacts)?;
            if prev_map.get(&replay_probe, name)?.is_none() {
                return Err(ArtifactError::Control(ControlError::OperationMismatch));
            }
            let expected_next_root = prev_map
                .remove(&mut replay_probe, name)?
                .seal(&mut replay_probe)?;
            if expected_next_root != transition.branch.artifacts_root {
                return Err(ArtifactError::Control(ControlError::OperationMismatch));
            }
            return Ok(transition.branch);
        }

        let branch = control.get_branch(ctx.branch_id)?;
        let (next_artifacts_root, staged) = {
            let mut staging = StagingObjectStore::new(store);
            let map = ArtifactMap::open(&staging, branch.artifacts_root)?;
            if map.get(&staging, name)?.is_none() {
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
            self.renewal_interval,
            next_renewal_id,
        )?;
        Ok(committed)
    }

    /// Reads a named artifact from a historical Generation under a bounded
    /// Generation read protection, without mounting the block volume.
    #[allow(clippy::too_many_arguments)]
    pub fn read_historical_artifact<S, C>(
        &self,
        store: &S,
        control: &C,
        generation_id: GenerationId,
        name: &str,
        open_op_id: OperationId,
        protection_id: ProtectionId,
        close_op_id: OperationId,
    ) -> Result<(ArtifactDescriptor, Vec<u8>), ArtifactError>
    where
        S: MapObjectStore,
        C: NativeControl,
    {
        if name.is_empty() {
            return Err(ArtifactError::InvalidName);
        }
        control.open_generation(open_op_id, generation_id, protection_id)?;
        let read_result = (|| {
            let seed = control.get_generation(generation_id)?;
            read_artifact_from_root(store, seed.roots.artifacts, name)
        })();
        let _ = control.close_protection(close_op_id, protection_id);
        read_result
    }
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
            renewal_interval: DEFAULT_NATIVE_RENEWAL_INTERVAL,
            next_ids,
        }
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

    /// Returns the number of locally staged named-artifact changes awaiting flush.
    pub fn staged_len(&self) -> usize {
        self.staged.len()
    }

    /// Flushes all locally staged named-artifact changes into the branch's live
    /// `artifacts_root` in one atomic `PreparationKind::ArtifactMutation` commit.
    pub fn flush_staged_artifacts(&mut self) -> Result<Option<BranchState>, ArtifactError> {
        if self.staged.is_empty() {
            return Ok(None);
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

        let (operation_id, protection_id) = (self.next_ids)();
        let next_roots = LiveRoots {
            volume: branch.volume_root,
            logs: branch.logs_root,
            artifacts: next_artifacts_root,
        };
        let committed = commit_native_root_mutation(
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
            self.renewal_interval,
            || (self.next_ids)().0,
        )?;
        self.staged.clear();
        Ok(Some(committed))
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
