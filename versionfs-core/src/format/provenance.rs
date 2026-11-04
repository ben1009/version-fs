//! Provenance-capable Generation descriptors and their bounded context object.

use super::{
    CanonicalDecoder, CanonicalEncoder, CaptureMode, EncodedObject, FormatError,
    GENERATION_DESCRIPTOR_FORMAT_VERSION, GenerationDescriptor, MAX_PROVENANCE_CONTEXT_BYTES,
    ObjectRef, ObjectType, decode_object, decode_object_versioned, encode_object,
    encode_object_versioned,
};
use crate::identity::{GenerationId, PrincipalId, VolumeBoundaryId};

const CONTEXT_FIELD_MAX_BYTES: usize = 4096;
const MAX_EXTERNAL_LINKS: usize = 64;

/// Provenance state recorded in a v2 Generation descriptor.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProvenanceBinding {
    /// The Generation has a durable context and required-evidence roots.
    Recorded {
        /// Strong reference to the canonical provenance context object.
        context_root: ObjectRef,
    },
    /// No context was supplied for this Generation.
    Unattributed,
}

impl ProvenanceBinding {
    /// Returns the durable status represented by this binding.
    pub const fn status(self) -> ProvenanceStatus {
        match self {
            Self::Recorded { .. } => ProvenanceStatus::Recorded,
            Self::Unattributed => ProvenanceStatus::Unattributed,
        }
    }

    /// Returns the context object, if this binding is recorded.
    pub const fn context_root(self) -> Option<ObjectRef> {
        match self {
            Self::Recorded { context_root } => Some(context_root),
            Self::Unattributed => None,
        }
    }

    /// Returns the coverage declared by this binding.
    pub const fn coverage(self) -> ProvenanceCoverage {
        match self {
            Self::Recorded { .. } => ProvenanceCoverage::GenerationContext,
            Self::Unattributed => ProvenanceCoverage::None,
        }
    }
}

/// Durable attribution status exposed by upgraded descriptor readers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum ProvenanceStatus {
    /// The Generation has a retained context object.
    Recorded = 1,
    /// No context was supplied, or the descriptor is a legacy v1 descriptor.
    Unattributed = 2,
}

/// Provenance coverage declared by a Generation descriptor.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum ProvenanceCoverage {
    /// No context was recorded.
    None = 0,
    /// One selected context is bound to the Generation.
    GenerationContext = 1,
}

/// The immutable v2 descriptor payload. New Generation writers use this type.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GenerationDescriptorV2 {
    /// Stable Generation identity.
    pub generation_id: GenerationId,
    /// Optional lineage reference; it is not needed to resolve state.
    pub parent_generation_id: Option<GenerationId>,
    /// Complete volume root reference.
    pub volume_root: ObjectRef,
    /// Complete native log root reference.
    pub logs_root: ObjectRef,
    /// Complete native artifact root reference.
    pub artifacts_root: ObjectRef,
    /// Explicit provenance binding; new empty Generations use Unattributed.
    pub provenance: ProvenanceBinding,
    /// Filesystem/block capture protocol actually established.
    pub capture_mode: CaptureMode,
    /// Identity of the captured volume boundary.
    pub volume_boundary_id: VolumeBoundaryId,
    /// Informational Unix timestamp in milliseconds.
    pub created_at_unix_ms: u64,
}

impl GenerationDescriptorV2 {
    /// Encodes the complete descriptor using the v2 envelope discriminator.
    pub fn encode(self) -> Result<Vec<u8>, FormatError> {
        self.validate()?;
        let mut encoder = CanonicalEncoder::new();
        encoder.fixed(self.generation_id.as_bytes());
        encoder.optional_id(
            self.parent_generation_id
                .as_ref()
                .map(GenerationId::as_bytes),
        );
        encoder.object_ref(self.volume_root);
        encoder.object_ref(self.logs_root);
        encoder.object_ref(self.artifacts_root);
        match self.provenance {
            ProvenanceBinding::Unattributed => {
                encoder.u8(ProvenanceStatus::Unattributed as u8);
                encoder.u8(0); // coverage: none
            }
            ProvenanceBinding::Recorded { context_root } => {
                encoder.u8(ProvenanceStatus::Recorded as u8);
                encoder.u8(1); // coverage: generation_context
                encoder.object_ref(context_root);
            }
        }
        encoder.u8(self.capture_mode as u8);
        encoder.fixed(self.volume_boundary_id.as_bytes());
        encoder.u64_le(self.created_at_unix_ms);
        encode_object_versioned(
            GENERATION_DESCRIPTOR_FORMAT_VERSION,
            ObjectType::GenerationDescriptor,
            &encoder.finish(),
        )
    }

    /// Decodes and validates a v2 Generation descriptor.
    pub fn decode(bytes: &[u8]) -> Result<Self, FormatError> {
        let object = decode_object_versioned(bytes)?;
        if object.format_version != GENERATION_DESCRIPTOR_FORMAT_VERSION
            || object.object_type != ObjectType::GenerationDescriptor
        {
            return Err(FormatError::ObjectReferenceMismatch);
        }
        Self::decode_payload(object)
    }

    fn decode_payload(object: EncodedObject<'_>) -> Result<Self, FormatError> {
        let mut decoder = CanonicalDecoder::new(object.payload);
        let generation_id = GenerationId::from_bytes(decoder.fixed()?);
        let parent_generation_id = match decoder.u8()? {
            0 => None,
            1 => Some(GenerationId::from_bytes(decoder.fixed()?)),
            _ => {
                return Err(FormatError::InvalidGenerationDescriptor(
                    "invalid parent presence tag",
                ));
            }
        };
        let volume_root = decoder.object_ref()?;
        let logs_root = decoder.object_ref()?;
        let artifacts_root = decoder.object_ref()?;
        let status = decoder.u8()?;
        let coverage = decoder.u8()?;
        let provenance = match (status, coverage) {
            (1, 1) => {
                let context_root = decoder.object_ref()?;
                ProvenanceBinding::Recorded { context_root }
            }
            (2, 0) => ProvenanceBinding::Unattributed,
            _ => {
                return Err(FormatError::InvalidProvenance(
                    "status and coverage combination is invalid",
                ));
            }
        };
        let capture_mode = CaptureMode::try_from(decoder.u8()?)?;
        let volume_boundary_id = VolumeBoundaryId::from_bytes(decoder.fixed()?);
        let created_at_unix_ms = decoder.u64_le()?;
        decoder.finish()?;
        let descriptor = Self {
            generation_id,
            parent_generation_id,
            volume_root,
            logs_root,
            artifacts_root,
            provenance,
            capture_mode,
            volume_boundary_id,
            created_at_unix_ms,
        };
        descriptor.validate()?;
        Ok(descriptor)
    }

    fn validate(self) -> Result<(), FormatError> {
        if self.volume_root.object_type != ObjectType::VolumeRoot
            || self.logs_root.object_type != ObjectType::LogMapRoot
            || self.artifacts_root.object_type != ObjectType::ArtifactMapRoot
        {
            return Err(FormatError::ObjectReferenceMismatch);
        }
        self.volume_root.validate()?;
        self.logs_root.validate()?;
        self.artifacts_root.validate()?;
        if let ProvenanceBinding::Recorded { context_root } = self.provenance {
            if context_root.object_type != ObjectType::ProvenanceContext {
                return Err(FormatError::InvalidProvenance(
                    "recorded binding must reference a provenance context",
                ));
            }
            if context_root.encoded_len > MAX_PROVENANCE_CONTEXT_BYTES as u64 {
                return Err(FormatError::InvalidProvenance(
                    "provenance context reference exceeds 64 KiB",
                ));
            }
            context_root.validate()?;
        }
        Ok(())
    }
}

/// Wire version reported by an upgraded Generation descriptor reader.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GenerationDescriptorFormat {
    /// Legacy descriptor with no provenance fields.
    V1,
    /// Provenance-capable descriptor.
    V2,
}

/// A Generation descriptor decoded from any supported immutable format.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DecodedGenerationDescriptor {
    /// Legacy v1 descriptor; its provenance is explicitly unattributed.
    V1(GenerationDescriptor),
    /// Provenance-capable descriptor.
    V2(GenerationDescriptorV2),
}

impl DecodedGenerationDescriptor {
    /// Returns the original descriptor format, without rewriting legacy state.
    pub const fn source_format(self) -> GenerationDescriptorFormat {
        match self {
            Self::V1(_) => GenerationDescriptorFormat::V1,
            Self::V2(_) => GenerationDescriptorFormat::V2,
        }
    }

    /// Returns the stable Generation identity.
    pub const fn generation_id(self) -> GenerationId {
        match self {
            Self::V1(descriptor) => descriptor.generation_id,
            Self::V2(descriptor) => descriptor.generation_id,
        }
    }

    /// Returns the optional lineage reference.
    pub const fn parent_generation_id(self) -> Option<GenerationId> {
        match self {
            Self::V1(descriptor) => descriptor.parent_generation_id,
            Self::V2(descriptor) => descriptor.parent_generation_id,
        }
    }

    /// Returns the complete volume root.
    pub const fn volume_root(self) -> ObjectRef {
        match self {
            Self::V1(descriptor) => descriptor.volume_root,
            Self::V2(descriptor) => descriptor.volume_root,
        }
    }

    /// Returns the complete native log root.
    pub const fn logs_root(self) -> ObjectRef {
        match self {
            Self::V1(descriptor) => descriptor.logs_root,
            Self::V2(descriptor) => descriptor.logs_root,
        }
    }

    /// Returns the complete native artifact root.
    pub const fn artifacts_root(self) -> ObjectRef {
        match self {
            Self::V1(descriptor) => descriptor.artifacts_root,
            Self::V2(descriptor) => descriptor.artifacts_root,
        }
    }

    /// Returns the established filesystem/block capture mode.
    pub const fn capture_mode(self) -> CaptureMode {
        match self {
            Self::V1(descriptor) => descriptor.capture_mode,
            Self::V2(descriptor) => descriptor.capture_mode,
        }
    }

    /// Returns the explicit status, treating legacy v1 as unattributed.
    pub const fn provenance_status(self) -> ProvenanceStatus {
        match self {
            Self::V1(_) => ProvenanceStatus::Unattributed,
            Self::V2(descriptor) => descriptor.provenance.status(),
        }
    }

    /// Returns the context reference when the descriptor records attribution.
    pub const fn context_root(self) -> Option<ObjectRef> {
        match self {
            Self::V1(_) => None,
            Self::V2(descriptor) => descriptor.provenance.context_root(),
        }
    }

    /// Returns explicit coverage, treating legacy v1 as none.
    pub const fn provenance_coverage(self) -> ProvenanceCoverage {
        match self {
            Self::V1(_) => ProvenanceCoverage::None,
            Self::V2(descriptor) => descriptor.provenance.coverage(),
        }
    }
}

/// Decodes either a supported legacy v1 or provenance-capable v2 descriptor.
pub fn decode_generation_descriptor(
    bytes: &[u8],
) -> Result<DecodedGenerationDescriptor, FormatError> {
    let object = decode_object_versioned(bytes)?;
    if object.object_type != ObjectType::GenerationDescriptor {
        return Err(FormatError::ObjectReferenceMismatch);
    }
    match object.format_version {
        1 => Ok(DecodedGenerationDescriptor::V1(
            GenerationDescriptor::decode(bytes)?,
        )),
        GENERATION_DESCRIPTOR_FORMAT_VERSION => Ok(DecodedGenerationDescriptor::V2(
            GenerationDescriptorV2::decode_payload(object)?,
        )),
        other => Err(FormatError::UnsupportedFormatVersion(other)),
    }
}

/// Runtime-supplied context and strongly typed required-evidence roots.
///
/// This format does not authenticate the principal or validate the claims;
/// callers must derive/validate the principal from the authenticated request.
/// Raw prompts and conversation bodies have no implicit fields here.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProvenanceContext {
    /// Version FS-validated identity of the recording request.
    pub recording_principal: PrincipalId,
    /// Runtime claim naming the producing actor or agent.
    pub actor: Option<String>,
    /// Runtime claim identifying one execution.
    pub execution_id: Option<String>,
    /// Runtime claim identifying the agent session.
    pub session_id: Option<String>,
    /// Runtime claim identifying a conversation/thread.
    pub thread_id: Option<String>,
    /// Runtime claim identifying the task.
    pub task_id: Option<String>,
    /// Runtime claim identifying the workflow operation.
    pub workflow_operation_id: Option<String>,
    /// Concise runtime-supplied reason for the captured state.
    pub reason: Option<String>,
    /// OpenTelemetry-compatible trace correlation claim.
    pub trace_id: Option<String>,
    /// OpenTelemetry-compatible span correlation claim.
    pub span_id: Option<String>,
    /// Optional parent span correlation claim.
    pub parent_span_id: Option<String>,
    /// Strong root for exact required log evidence ranges.
    pub required_logs_root: ObjectRef,
    /// Strong root for required artifact evidence.
    pub required_artifacts_root: ObjectRef,
    /// Sorted, descriptive external links; these do not retain their targets.
    pub optional_links: Vec<String>,
}

impl ProvenanceContext {
    /// Encodes one canonical v1 context object bounded to 64 KiB total.
    pub fn encode(&self) -> Result<Vec<u8>, FormatError> {
        self.validate()?;
        let mut encoder = CanonicalEncoder::new();
        encoder.fixed(self.recording_principal.as_bytes());
        encode_optional_string(&mut encoder, self.actor.as_deref())?;
        encode_optional_string(&mut encoder, self.execution_id.as_deref())?;
        encode_optional_string(&mut encoder, self.session_id.as_deref())?;
        encode_optional_string(&mut encoder, self.thread_id.as_deref())?;
        encode_optional_string(&mut encoder, self.task_id.as_deref())?;
        encode_optional_string(&mut encoder, self.workflow_operation_id.as_deref())?;
        encode_optional_string(&mut encoder, self.reason.as_deref())?;
        encode_optional_string(&mut encoder, self.trace_id.as_deref())?;
        encode_optional_string(&mut encoder, self.span_id.as_deref())?;
        encode_optional_string(&mut encoder, self.parent_span_id.as_deref())?;
        encoder.object_ref(self.required_logs_root);
        encoder.object_ref(self.required_artifacts_root);
        encoder.u16_le(self.optional_links.len() as u16);
        for link in &self.optional_links {
            encode_string(&mut encoder, link)?;
        }
        let encoded = encode_object(ObjectType::ProvenanceContext, &encoder.finish())?;
        if encoded.len() > MAX_PROVENANCE_CONTEXT_BYTES {
            return Err(FormatError::PayloadTooLarge {
                actual: encoded.len(),
                maximum: MAX_PROVENANCE_CONTEXT_BYTES,
            });
        }
        Ok(encoded)
    }

    /// Decodes and validates one bounded canonical context object.
    pub fn decode(bytes: &[u8]) -> Result<Self, FormatError> {
        if bytes.len() > MAX_PROVENANCE_CONTEXT_BYTES {
            return Err(FormatError::PayloadTooLarge {
                actual: bytes.len(),
                maximum: MAX_PROVENANCE_CONTEXT_BYTES,
            });
        }
        let object = decode_object(bytes)?;
        if object.object_type != ObjectType::ProvenanceContext {
            return Err(FormatError::ObjectReferenceMismatch);
        }
        let mut decoder = CanonicalDecoder::new(object.payload);
        let recording_principal = PrincipalId::from_bytes(decoder.fixed()?);
        let actor = decode_optional_string(&mut decoder)?;
        let execution_id = decode_optional_string(&mut decoder)?;
        let session_id = decode_optional_string(&mut decoder)?;
        let thread_id = decode_optional_string(&mut decoder)?;
        let task_id = decode_optional_string(&mut decoder)?;
        let workflow_operation_id = decode_optional_string(&mut decoder)?;
        let reason = decode_optional_string(&mut decoder)?;
        let trace_id = decode_optional_string(&mut decoder)?;
        let span_id = decode_optional_string(&mut decoder)?;
        let parent_span_id = decode_optional_string(&mut decoder)?;
        let required_logs_root = decoder.object_ref()?;
        let required_artifacts_root = decoder.object_ref()?;
        let link_count = usize::from(decoder.u16_le()?);
        if link_count > MAX_EXTERNAL_LINKS {
            return Err(FormatError::InvalidProvenance(
                "too many optional external links",
            ));
        }
        let mut optional_links = Vec::with_capacity(link_count);
        for _ in 0..link_count {
            optional_links.push(decode_string(&mut decoder)?);
        }
        decoder.finish()?;
        let context = Self {
            recording_principal,
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
            required_logs_root,
            required_artifacts_root,
            optional_links,
        };
        context.validate()?;
        Ok(context)
    }

    fn validate(&self) -> Result<(), FormatError> {
        if self.required_logs_root.object_type != ObjectType::LogMapRoot
            || self.required_artifacts_root.object_type != ObjectType::ArtifactMapRoot
        {
            return Err(FormatError::InvalidProvenance(
                "required evidence roots have invalid object types",
            ));
        }
        self.required_logs_root.validate()?;
        self.required_artifacts_root.validate()?;
        for value in [
            self.actor.as_deref(),
            self.execution_id.as_deref(),
            self.session_id.as_deref(),
            self.thread_id.as_deref(),
            self.task_id.as_deref(),
            self.workflow_operation_id.as_deref(),
            self.reason.as_deref(),
            self.trace_id.as_deref(),
            self.span_id.as_deref(),
            self.parent_span_id.as_deref(),
        ]
        .into_iter()
        .flatten()
        {
            validate_string(value)?;
        }
        if self.optional_links.len() > MAX_EXTERNAL_LINKS {
            return Err(FormatError::InvalidProvenance(
                "too many optional external links",
            ));
        }
        if self
            .optional_links
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
        {
            return Err(FormatError::InvalidProvenance(
                "optional external links must be sorted and unique",
            ));
        }
        for link in &self.optional_links {
            validate_string(link)?;
        }
        Ok(())
    }
}

fn validate_string(value: &str) -> Result<(), FormatError> {
    if value.is_empty() || value.len() > CONTEXT_FIELD_MAX_BYTES {
        return Err(FormatError::InvalidProvenance(
            "context strings must be non-empty and at most 4 KiB",
        ));
    }
    Ok(())
}

fn encode_optional_string(
    encoder: &mut CanonicalEncoder,
    value: Option<&str>,
) -> Result<(), FormatError> {
    match value {
        Some(value) => {
            validate_string(value)?;
            encoder.u8(1);
            encode_string(encoder, value)?;
        }
        None => encoder.u8(0),
    }
    Ok(())
}

fn encode_string(encoder: &mut CanonicalEncoder, value: &str) -> Result<(), FormatError> {
    validate_string(value)?;
    encoder.bytes(value.as_bytes())
}

fn decode_optional_string(
    decoder: &mut CanonicalDecoder<'_>,
) -> Result<Option<String>, FormatError> {
    match decoder.u8()? {
        0 => Ok(None),
        1 => decode_string(decoder).map(Some),
        _ => Err(FormatError::InvalidProvenance(
            "invalid optional context string tag",
        )),
    }
}

fn decode_string(decoder: &mut CanonicalDecoder<'_>) -> Result<String, FormatError> {
    let bytes = decoder.bytes(CONTEXT_FIELD_MAX_BYTES)?;
    let value = std::str::from_utf8(bytes)
        .map_err(|_| FormatError::InvalidProvenance("context string is not UTF-8"))?;
    validate_string(value)?;
    Ok(value.to_owned())
}
