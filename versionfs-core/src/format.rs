//! Canonical Version FS v1 object and metadata envelopes.

use crate::identity::{GenerationId, LogId, ObjectId, VolumeBoundaryId};
use sha2::{Digest, Sha256};
use std::fmt;

/// First durable object encoding version. It is independent of the product MVP version.
pub const FORMAT_VERSION: u16 = 1;
/// First durable Metadata Authority value schema version.
pub const METADATA_SCHEMA_VERSION: u16 = 1;
/// Maximum canonical object payload, matching the largest v0 artifact chunk.
pub const MAX_OBJECT_PAYLOAD_BYTES: usize = 4 * 1024 * 1024;
/// Maximum records in one native-log append batch.
pub const MAX_LOG_RECORDS: usize = 1000;
/// Maximum bytes accepted for one encoded native-log segment record.
pub const MAX_LOG_RECORD_BYTES: usize = 1024 * 1024;
/// Volume logical sector size selected for v0.
pub const LOGICAL_SECTOR_BYTES: u32 = 4 * 1024;
/// Immutable volume extent size selected for v0.
pub const EXTENT_BYTES: u32 = 64 * 1024;

const OBJECT_MAGIC: [u8; 4] = *b"VFSO";
const METADATA_MAGIC: [u8; 4] = *b"VFSM";
const ENVELOPE_HEADER_BYTES: usize = 16;

/// Stable object type codes. Codes are never reused after publication.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[repr(u16)]
pub enum ObjectType {
    /// One 64 KiB immutable volume extent.
    Extent = 1,
    /// A persistent ordered volume extent-map node.
    VolumeMapNode = 2,
    /// A complete volume root.
    VolumeRoot = 3,
    /// One sealed ordered-log append batch.
    LogSegment = 4,
    /// A persistent ordered-log segment index node.
    LogSegmentIndexNode = 5,
    /// A complete native log-map root.
    LogMapRoot = 6,
    /// One artifact payload of at most 64 KiB.
    ArtifactBlob = 7,
    /// One fixed 4 MiB artifact chunk (the final chunk may be shorter).
    ArtifactChunk = 8,
    /// A persistent artifact chunk-index node.
    ArtifactIndexNode = 9,
    /// A complete native artifact-map root.
    ArtifactMapRoot = 10,
    /// A complete immutable Generation descriptor.
    GenerationDescriptor = 11,
}

impl TryFrom<u16> for ObjectType {
    type Error = FormatError;

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Extent),
            2 => Ok(Self::VolumeMapNode),
            3 => Ok(Self::VolumeRoot),
            4 => Ok(Self::LogSegment),
            5 => Ok(Self::LogSegmentIndexNode),
            6 => Ok(Self::LogMapRoot),
            7 => Ok(Self::ArtifactBlob),
            8 => Ok(Self::ArtifactChunk),
            9 => Ok(Self::ArtifactIndexNode),
            10 => Ok(Self::ArtifactMapRoot),
            11 => Ok(Self::GenerationDescriptor),
            other => Err(FormatError::UnknownObjectType(other)),
        }
    }
}

/// Stable Metadata Authority record type codes.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[repr(u16)]
pub enum MetadataRecordType {
    /// Workspace lifecycle and geometry metadata.
    Workspace = 1,
    /// Branch head and durable live-view metadata.
    Branch = 2,
    /// Immutable Generation catalog metadata.
    Generation = 3,
    /// Idempotent mutation outcome metadata.
    OperationOutcome = 4,
    /// Retention and in-progress protection metadata.
    Protection = 5,
}

impl TryFrom<u16> for MetadataRecordType {
    type Error = FormatError;

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Workspace),
            2 => Ok(Self::Branch),
            3 => Ok(Self::Generation),
            4 => Ok(Self::OperationOutcome),
            5 => Ok(Self::Protection),
            other => Err(FormatError::UnknownMetadataRecordType(other)),
        }
    }
}

/// Persisted filesystem-consistency mode. Numeric values are part of format v1.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum CaptureMode {
    /// A block-level cut whose filesystem recovery may be required.
    BlockCut = 1,
    /// A filesystem boundary established with filesystem freeze.
    FilesystemFrozen = 2,
    /// A filesystem boundary established after a clean unmount.
    FilesystemCleanUnmount = 3,
}

impl TryFrom<u8> for CaptureMode {
    type Error = FormatError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::BlockCut),
            2 => Ok(Self::FilesystemFrozen),
            3 => Ok(Self::FilesystemCleanUnmount),
            other => Err(FormatError::UnknownCaptureMode(other)),
        }
    }
}

/// Errors found while encoding or validating canonical data.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FormatError {
    /// The object payload exceeds the format's bounded payload size.
    PayloadTooLarge {
        /// Actual payload length in bytes.
        actual: usize,
        /// Maximum allowed payload length in bytes.
        maximum: usize,
    },
    /// The byte slice is shorter than the canonical header.
    TruncatedEnvelope,
    /// The magic, version, or declared length is invalid.
    InvalidEnvelope(&'static str),
    /// The object format version is not supported by this implementation.
    UnsupportedFormatVersion(u16),
    /// The metadata schema version is not supported by this implementation.
    UnsupportedMetadataSchema(u16),
    /// The object type code is unknown.
    UnknownObjectType(u16),
    /// The metadata record type code is unknown.
    UnknownMetadataRecordType(u16),
    /// The capture mode is unknown.
    UnknownCaptureMode(u8),
    /// The volume capacity or range violates the selected geometry.
    InvalidGeometry(&'static str),
    /// A persistent map has duplicate or non-increasing encoded keys.
    UnorderedKeys,
    /// A referenced object's type, length, or digest does not match its bytes.
    ObjectReferenceMismatch,
    /// A log segment has an invalid or empty sequence range.
    InvalidLogSegment,
}

impl fmt::Display for FormatError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PayloadTooLarge { actual, maximum } => {
                write!(
                    formatter,
                    "object payload is {actual} bytes; maximum is {maximum}"
                )
            }
            Self::TruncatedEnvelope => formatter.write_str("canonical envelope is truncated"),
            Self::InvalidEnvelope(reason) => {
                write!(formatter, "invalid canonical envelope: {reason}")
            }
            Self::UnsupportedFormatVersion(version) => {
                write!(formatter, "unsupported object format version {version}")
            }
            Self::UnsupportedMetadataSchema(version) => {
                write!(formatter, "unsupported metadata schema version {version}")
            }
            Self::UnknownObjectType(object_type) => {
                write!(formatter, "unknown object type {object_type}")
            }
            Self::UnknownMetadataRecordType(record_type) => {
                write!(formatter, "unknown metadata record type {record_type}")
            }
            Self::UnknownCaptureMode(mode) => write!(formatter, "unknown capture mode {mode}"),
            Self::InvalidGeometry(reason) => write!(formatter, "invalid volume geometry: {reason}"),
            Self::UnorderedKeys => formatter.write_str("map keys must be strictly increasing"),
            Self::ObjectReferenceMismatch => {
                formatter.write_str("object reference does not match bytes")
            }
            Self::InvalidLogSegment => {
                formatter.write_str("invalid log segment range or record count")
            }
        }
    }
}

impl std::error::Error for FormatError {}

/// Canonical uncompressed object envelope and payload.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EncodedObject<'a> {
    /// Supported format version.
    pub format_version: u16,
    /// Typed object kind.
    pub object_type: ObjectType,
    /// Canonical type-specific payload bytes.
    pub payload: &'a [u8],
}

/// A strongly typed reference to a complete encoded object.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ObjectRef {
    /// Referenced object's stable type.
    pub object_type: ObjectType,
    /// SHA-256 identity of its complete encoded envelope.
    pub object_id: ObjectId,
    /// Exact number of bytes in its encoded envelope.
    pub encoded_len: u64,
}

impl ObjectRef {
    /// Builds a reference from validated encoded bytes.
    pub fn from_encoded(bytes: &[u8]) -> Result<Self, FormatError> {
        let object = decode_object(bytes)?;
        Ok(Self {
            object_type: object.object_type,
            object_id: object_id(bytes),
            encoded_len: bytes.len() as u64,
        })
    }

    /// Verifies this reference against the complete encoded object bytes.
    pub fn verify<'a>(&self, bytes: &'a [u8]) -> Result<EncodedObject<'a>, FormatError> {
        if bytes.len() as u64 != self.encoded_len || object_id(bytes) != self.object_id {
            return Err(FormatError::ObjectReferenceMismatch);
        }
        let object = decode_object(bytes)?;
        if object.object_type != self.object_type {
            return Err(FormatError::ObjectReferenceMismatch);
        }
        Ok(object)
    }

    fn validate(self) -> Result<(), FormatError> {
        let minimum_len = ENVELOPE_HEADER_BYTES as u64;
        let maximum_len = (ENVELOPE_HEADER_BYTES + MAX_OBJECT_PAYLOAD_BYTES) as u64;
        if !(minimum_len..=maximum_len).contains(&self.encoded_len) {
            return Err(FormatError::ObjectReferenceMismatch);
        }
        Ok(())
    }

    fn encode(self, encoder: &mut CanonicalEncoder) {
        encoder.u16_le(self.object_type as u16);
        encoder.fixed(self.object_id.as_bytes());
        encoder.u64_le(self.encoded_len);
    }
}

/// Encodes an object envelope with a stable type, version, and declared payload length.
pub fn encode_object(object_type: ObjectType, payload: &[u8]) -> Result<Vec<u8>, FormatError> {
    if payload.len() > MAX_OBJECT_PAYLOAD_BYTES {
        return Err(FormatError::PayloadTooLarge {
            actual: payload.len(),
            maximum: MAX_OBJECT_PAYLOAD_BYTES,
        });
    }
    let mut bytes = Vec::with_capacity(ENVELOPE_HEADER_BYTES + payload.len());
    bytes.extend_from_slice(&OBJECT_MAGIC);
    bytes.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
    bytes.extend_from_slice(&(object_type as u16).to_le_bytes());
    bytes.extend_from_slice(&(payload.len() as u64).to_le_bytes());
    bytes.extend_from_slice(payload);
    Ok(bytes)
}

/// Decodes and validates an object envelope, rejecting trailing or missing bytes.
pub fn decode_object(bytes: &[u8]) -> Result<EncodedObject<'_>, FormatError> {
    if bytes.len() < ENVELOPE_HEADER_BYTES {
        return Err(FormatError::TruncatedEnvelope);
    }
    if bytes[..4] != OBJECT_MAGIC {
        return Err(FormatError::InvalidEnvelope("wrong object magic"));
    }
    let version = u16::from_le_bytes([bytes[4], bytes[5]]);
    if version != FORMAT_VERSION {
        return Err(FormatError::UnsupportedFormatVersion(version));
    }
    let object_type = ObjectType::try_from(u16::from_le_bytes([bytes[6], bytes[7]]))?;
    let payload_len = u64::from_le_bytes(bytes[8..16].try_into().expect("fixed header slice"));
    let payload_len = usize::try_from(payload_len)
        .map_err(|_| FormatError::InvalidEnvelope("declared length does not fit this platform"))?;
    if payload_len > MAX_OBJECT_PAYLOAD_BYTES {
        return Err(FormatError::PayloadTooLarge {
            actual: payload_len,
            maximum: MAX_OBJECT_PAYLOAD_BYTES,
        });
    }
    if bytes.len() != ENVELOPE_HEADER_BYTES + payload_len {
        return Err(FormatError::InvalidEnvelope(
            "declared payload length differs from byte length",
        ));
    }
    Ok(EncodedObject {
        format_version: version,
        object_type,
        payload: &bytes[ENVELOPE_HEADER_BYTES..],
    })
}

/// Computes the SHA-256 object identity over the complete encoded envelope.
pub fn object_id(bytes: &[u8]) -> ObjectId {
    ObjectId::from_bytes(Sha256::digest(bytes).into())
}

/// Encodes a metadata value with an explicit schema version and record type.
pub fn encode_metadata_record(
    record_type: MetadataRecordType,
    payload: &[u8],
) -> Result<Vec<u8>, FormatError> {
    if payload.len() > MAX_OBJECT_PAYLOAD_BYTES {
        return Err(FormatError::PayloadTooLarge {
            actual: payload.len(),
            maximum: MAX_OBJECT_PAYLOAD_BYTES,
        });
    }
    let mut bytes = Vec::with_capacity(ENVELOPE_HEADER_BYTES + payload.len());
    bytes.extend_from_slice(&METADATA_MAGIC);
    bytes.extend_from_slice(&METADATA_SCHEMA_VERSION.to_le_bytes());
    bytes.extend_from_slice(&(record_type as u16).to_le_bytes());
    bytes.extend_from_slice(&(payload.len() as u64).to_le_bytes());
    bytes.extend_from_slice(payload);
    Ok(bytes)
}

/// Decodes and validates a versioned Metadata Authority record.
pub fn decode_metadata_record(bytes: &[u8]) -> Result<(MetadataRecordType, &[u8]), FormatError> {
    if bytes.len() < ENVELOPE_HEADER_BYTES {
        return Err(FormatError::TruncatedEnvelope);
    }
    if bytes[..4] != METADATA_MAGIC {
        return Err(FormatError::InvalidEnvelope("wrong metadata magic"));
    }
    let version = u16::from_le_bytes([bytes[4], bytes[5]]);
    if version != METADATA_SCHEMA_VERSION {
        return Err(FormatError::UnsupportedMetadataSchema(version));
    }
    let record_type = MetadataRecordType::try_from(u16::from_le_bytes([bytes[6], bytes[7]]))?;
    let payload_len = u64::from_le_bytes(bytes[8..16].try_into().expect("fixed header slice"));
    let payload_len = usize::try_from(payload_len)
        .map_err(|_| FormatError::InvalidEnvelope("declared length does not fit this platform"))?;
    if payload_len > MAX_OBJECT_PAYLOAD_BYTES {
        return Err(FormatError::PayloadTooLarge {
            actual: payload_len,
            maximum: MAX_OBJECT_PAYLOAD_BYTES,
        });
    }
    if bytes.len() != ENVELOPE_HEADER_BYTES + payload_len {
        return Err(FormatError::InvalidEnvelope(
            "declared payload length differs from byte length",
        ));
    }
    Ok((record_type, &bytes[ENVELOPE_HEADER_BYTES..]))
}

/// Canonical encoder for fixed-order payload fields.
#[derive(Clone, Debug, Default)]
pub struct CanonicalEncoder {
    bytes: Vec<u8>,
}

impl CanonicalEncoder {
    /// Creates an empty encoder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds one byte.
    pub fn u8(&mut self, value: u8) {
        self.bytes.push(value);
    }

    /// Adds a little-endian 16-bit integer.
    pub fn u16_le(&mut self, value: u16) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    /// Adds a little-endian 32-bit integer.
    pub fn u32_le(&mut self, value: u32) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    /// Adds a little-endian 64-bit integer.
    pub fn u64_le(&mut self, value: u64) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    /// Adds fixed-width bytes without a length prefix.
    pub fn fixed(&mut self, value: &[u8]) {
        self.bytes.extend_from_slice(value);
    }

    /// Adds a byte string prefixed by its 32-bit little-endian length.
    pub fn bytes(&mut self, value: &[u8]) -> Result<(), FormatError> {
        let len = u32::try_from(value.len()).map_err(|_| FormatError::PayloadTooLarge {
            actual: value.len(),
            maximum: u32::MAX as usize,
        })?;
        self.u32_le(len);
        self.fixed(value);
        Ok(())
    }

    /// Adds an optional 128-bit ID using a one-byte presence tag.
    pub fn optional_id(&mut self, value: Option<&[u8; 16]>) {
        match value {
            Some(id) => {
                self.u8(1);
                self.fixed(id);
            }
            None => self.u8(0),
        }
    }

    /// Adds a typed object reference in its canonical field order.
    pub fn object_ref(&mut self, value: ObjectRef) {
        value.encode(self);
    }

    /// Returns the encoded payload bytes.
    pub fn finish(self) -> Vec<u8> {
        self.bytes
    }
}

/// Encodes strictly increasing bytewise-sorted key/value entries.
pub fn encode_ordered_entries(entries: &[(&[u8], &[u8])]) -> Result<Vec<u8>, FormatError> {
    let count = u32::try_from(entries.len()).map_err(|_| FormatError::PayloadTooLarge {
        actual: entries.len(),
        maximum: u32::MAX as usize,
    })?;
    if entries.windows(2).any(|window| window[0].0 >= window[1].0) {
        return Err(FormatError::UnorderedKeys);
    }
    let payload_len = entries.iter().try_fold(4_usize, |total, (key, value)| {
        total
            .checked_add(8)
            .and_then(|total| total.checked_add(key.len()))
            .and_then(|total| total.checked_add(value.len()))
            .ok_or(FormatError::InvalidEnvelope(
                "ordered map payload length overflow",
            ))
    })?;
    if payload_len > MAX_OBJECT_PAYLOAD_BYTES {
        return Err(FormatError::PayloadTooLarge {
            actual: payload_len,
            maximum: MAX_OBJECT_PAYLOAD_BYTES,
        });
    }
    let mut encoder = CanonicalEncoder::new();
    encoder.u32_le(count);
    for (key, value) in entries {
        encoder.bytes(key)?;
        encoder.bytes(value)?;
    }
    Ok(encoder.finish())
}

/// Encodes an unsigned logical index as a bytewise-sortable key.
pub const fn ordered_u64_key(value: u64) -> [u8; 8] {
    value.to_be_bytes()
}

/// Fixed v0 volume geometry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VolumeGeometry {
    /// Fixed volume capacity in bytes.
    pub capacity_bytes: u64,
    /// Logical sector size in bytes.
    pub logical_sector_bytes: u32,
    /// Immutable map extent size in bytes.
    pub extent_bytes: u32,
}

impl VolumeGeometry {
    /// Creates and validates a v0 geometry with the fixed sector and extent sizes.
    pub fn new(capacity_bytes: u64) -> Result<Self, FormatError> {
        if capacity_bytes == 0 || !capacity_bytes.is_multiple_of(u64::from(EXTENT_BYTES)) {
            return Err(FormatError::InvalidGeometry(
                "capacity must be a positive extent multiple",
            ));
        }
        Ok(Self {
            capacity_bytes,
            logical_sector_bytes: LOGICAL_SECTOR_BYTES,
            extent_bytes: EXTENT_BYTES,
        })
    }

    /// Validates a sector-aligned byte range inside this volume.
    pub fn validate_range(self, offset: u64, length: u64) -> Result<(), FormatError> {
        if self.logical_sector_bytes != LOGICAL_SECTOR_BYTES || self.extent_bytes != EXTENT_BYTES {
            return Err(FormatError::InvalidGeometry(
                "sector and extent sizes are fixed in v0",
            ));
        }
        if self.capacity_bytes == 0 || !self.capacity_bytes.is_multiple_of(u64::from(EXTENT_BYTES))
        {
            return Err(FormatError::InvalidGeometry(
                "capacity must be a positive extent multiple",
            ));
        }
        let sector = u64::from(self.logical_sector_bytes);
        let end = offset
            .checked_add(length)
            .ok_or(FormatError::InvalidGeometry("range overflow"))?;
        if !offset.is_multiple_of(sector)
            || !length.is_multiple_of(sector)
            || end > self.capacity_bytes
        {
            return Err(FormatError::InvalidGeometry(
                "range is unaligned or outside the volume",
            ));
        }
        Ok(())
    }
}

/// Complete immutable volume root payload.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VolumeRoot {
    /// Fixed volume geometry.
    pub geometry: VolumeGeometry,
    /// Root of the complete persistent extent map.
    pub extent_map_root: ObjectRef,
}

impl VolumeRoot {
    /// Encodes this complete root as a typed immutable object.
    pub fn encode(self) -> Result<Vec<u8>, FormatError> {
        self.geometry.validate_range(0, 0)?;
        if self.extent_map_root.object_type != ObjectType::VolumeMapNode {
            return Err(FormatError::ObjectReferenceMismatch);
        }
        self.extent_map_root.validate()?;
        let mut encoder = CanonicalEncoder::new();
        encoder.u64_le(self.geometry.capacity_bytes);
        encoder.u32_le(self.geometry.logical_sector_bytes);
        encoder.u32_le(self.geometry.extent_bytes);
        encoder.object_ref(self.extent_map_root);
        encode_object(ObjectType::VolumeRoot, &encoder.finish())
    }
}

/// One immutable append batch in a native ordered log.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogSegment {
    /// Stable logical log identity.
    pub log_id: LogId,
    /// Inclusive first sequence number in this batch.
    pub first_seq: u64,
    /// Exclusive next sequence number after this batch.
    pub end_seq: u64,
    /// Record payloads in sequence order.
    pub records: Vec<Vec<u8>>,
    /// Explicit typed attachment references used by records in this batch.
    pub attachments: Vec<ObjectRef>,
}

impl LogSegment {
    /// Encodes and validates a complete sealed segment.
    pub fn encode(&self) -> Result<Vec<u8>, FormatError> {
        if self.records.is_empty()
            || self.end_seq.checked_sub(self.first_seq) != Some(self.records.len() as u64)
            || self.records.len() > MAX_LOG_RECORDS
        {
            return Err(FormatError::InvalidLogSegment);
        }
        let record_bytes = self.records.iter().try_fold(0_usize, |total, record| {
            total
                .checked_add(std::mem::size_of::<u32>())
                .and_then(|total| total.checked_add(record.len()))
                .ok_or(FormatError::InvalidLogSegment)
        })?;
        if record_bytes > MAX_LOG_RECORD_BYTES {
            return Err(FormatError::PayloadTooLarge {
                actual: record_bytes,
                maximum: MAX_LOG_RECORD_BYTES,
            });
        }
        let mut encoder = CanonicalEncoder::new();
        encoder.fixed(self.log_id.as_bytes());
        encoder.u64_le(self.first_seq);
        encoder.u64_le(self.end_seq);
        encoder.u32_le(self.records.len() as u32);
        for record in &self.records {
            encoder.bytes(record)?;
        }
        encoder.u32_le(
            u32::try_from(self.attachments.len()).map_err(|_| FormatError::InvalidLogSegment)?,
        );
        for attachment in &self.attachments {
            attachment.validate()?;
            encoder.object_ref(*attachment);
        }
        encode_object(ObjectType::LogSegment, &encoder.finish())
    }
}

/// Complete immutable Generation descriptor payload.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GenerationDescriptor {
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
    /// Filesystem/block capture protocol actually established.
    pub capture_mode: CaptureMode,
    /// Identity of the captured volume boundary.
    pub volume_boundary_id: VolumeBoundaryId,
    /// Informational Unix timestamp in milliseconds.
    pub created_at_unix_ms: u64,
}

impl GenerationDescriptor {
    /// Encodes the complete descriptor as an immutable object.
    pub fn encode(self) -> Result<Vec<u8>, FormatError> {
        if self.volume_root.object_type != ObjectType::VolumeRoot
            || self.logs_root.object_type != ObjectType::LogMapRoot
            || self.artifacts_root.object_type != ObjectType::ArtifactMapRoot
        {
            return Err(FormatError::ObjectReferenceMismatch);
        }
        self.volume_root.validate()?;
        self.logs_root.validate()?;
        self.artifacts_root.validate()?;
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
        encoder.u8(self.capture_mode as u8);
        encoder.fixed(self.volume_boundary_id.as_bytes());
        encoder.u64_le(self.created_at_unix_ms);
        encode_object(ObjectType::GenerationDescriptor, &encoder.finish())
    }
}
