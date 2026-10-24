//! Canonical Version FS v1 object and metadata envelopes.

use crate::identity::{ArtifactId, GenerationId, LogId, ObjectId, VolumeBoundaryId};
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
/// Maximum encoded size of one persistent B+ tree node, including its envelope.
pub const MAX_PERSISTENT_NODE_BYTES: usize = 64 * 1024;
/// Fixed logical chunk size for large native artifacts.
pub const ARTIFACT_CHUNK_BYTES: usize = 4 * 1024 * 1024;
/// Maximum payload kept in one inline artifact blob before chunk indexing is used.
pub const MAX_ARTIFACT_BLOB_BYTES: usize = 64 * 1024;

const OBJECT_MAGIC: [u8; 4] = *b"VFSO";
const METADATA_MAGIC: [u8; 4] = *b"VFSM";
/// Number of bytes in each canonical object and metadata envelope header.
pub const ENVELOPE_HEADER_BYTES: usize = 16;
/// Largest supported complete encoded object, including its canonical header.
pub const MAX_ENCODED_OBJECT_BYTES: usize = ENVELOPE_HEADER_BYTES + MAX_OBJECT_PAYLOAD_BYTES;
const OBJECT_REFERENCE_BYTES: usize = 42;
const LOG_SEGMENT_FIXED_PAYLOAD_BYTES: usize = 40;

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
    /// A persistent ordered native log-name map node.
    LogMapNode = 12,
    /// A persistent ordered named-artifact map node.
    ArtifactMapNode = 13,
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
            12 => Ok(Self::LogMapNode),
            13 => Ok(Self::ArtifactMapNode),
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
    /// Durable metadata-authority owner incarnation.
    AuthorityOwner = 6,
    /// Branch-name to stable branch-ID binding.
    BranchNameIndex = 7,
    /// Explicit Generation pin metadata.
    Pin = 8,
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
            6 => Ok(Self::AuthorityOwner),
            7 => Ok(Self::BranchNameIndex),
            8 => Ok(Self::Pin),
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
    /// A persistent tree node violates its canonical shape or size limit.
    InvalidPersistentNode(&'static str),
    /// A native log or artifact root/entry is invalid.
    InvalidNativeView(&'static str),
    /// A Generation descriptor is invalid.
    InvalidGenerationDescriptor(&'static str),
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
            Self::InvalidPersistentNode(reason) => {
                write!(formatter, "invalid persistent node: {reason}")
            }
            Self::InvalidNativeView(reason) => {
                write!(formatter, "invalid native view: {reason}")
            }
            Self::InvalidGenerationDescriptor(reason) => {
                write!(formatter, "invalid Generation descriptor: {reason}")
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

    pub(crate) fn validate(self) -> Result<(), FormatError> {
        let minimum_len = ENVELOPE_HEADER_BYTES as u64;
        let maximum_len = MAX_ENCODED_OBJECT_BYTES as u64;
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

/// Strict cursor for canonical fixed-order payloads shared by core decoders.
pub(crate) struct CanonicalDecoder<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> CanonicalDecoder<'a> {
    pub(crate) fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    pub(crate) fn u8(&mut self) -> Result<u8, FormatError> {
        let value = *self
            .bytes
            .get(self.offset)
            .ok_or(FormatError::InvalidEnvelope("truncated payload"))?;
        self.offset += 1;
        Ok(value)
    }

    pub(crate) fn u16_le(&mut self) -> Result<u16, FormatError> {
        Ok(u16::from_le_bytes(self.fixed()?))
    }

    pub(crate) fn u32_le(&mut self) -> Result<u32, FormatError> {
        Ok(u32::from_le_bytes(self.fixed()?))
    }

    pub(crate) fn u64_le(&mut self) -> Result<u64, FormatError> {
        Ok(u64::from_le_bytes(self.fixed()?))
    }

    pub(crate) fn fixed<const N: usize>(&mut self) -> Result<[u8; N], FormatError> {
        let end = self
            .offset
            .checked_add(N)
            .ok_or(FormatError::InvalidEnvelope("payload cursor overflow"))?;
        let slice = self
            .bytes
            .get(self.offset..end)
            .ok_or(FormatError::InvalidEnvelope("truncated payload"))?;
        self.offset = end;
        Ok(slice
            .try_into()
            .expect("fixed-length slice was checked before conversion"))
    }

    pub(crate) fn bytes(&mut self, maximum: usize) -> Result<&'a [u8], FormatError> {
        let len = usize::try_from(self.u32_le()?)
            .map_err(|_| FormatError::InvalidEnvelope("byte string length overflow"))?;
        if len > maximum {
            return Err(FormatError::PayloadTooLarge {
                actual: len,
                maximum,
            });
        }
        let end = self
            .offset
            .checked_add(len)
            .ok_or(FormatError::InvalidEnvelope("byte string length overflow"))?;
        let bytes = self
            .bytes
            .get(self.offset..end)
            .ok_or(FormatError::InvalidEnvelope("truncated byte string"))?;
        self.offset = end;
        Ok(bytes)
    }

    pub(crate) fn object_ref(&mut self) -> Result<ObjectRef, FormatError> {
        let object_type = ObjectType::try_from(self.u16_le()?)?;
        let object_id = ObjectId::from_bytes(self.fixed()?);
        let encoded_len = self.u64_le()?;
        let reference = ObjectRef {
            object_type,
            object_id,
            encoded_len,
        };
        reference.validate()?;
        Ok(reference)
    }

    pub(crate) fn finish(self) -> Result<(), FormatError> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(FormatError::InvalidEnvelope("trailing payload bytes"))
        }
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

    /// Decodes and validates a complete volume root object.
    pub fn decode(bytes: &[u8]) -> Result<Self, FormatError> {
        let object = decode_object(bytes)?;
        if object.object_type != ObjectType::VolumeRoot {
            return Err(FormatError::ObjectReferenceMismatch);
        }
        let mut decoder = CanonicalDecoder::new(object.payload);
        let geometry = VolumeGeometry {
            capacity_bytes: decoder.u64_le()?,
            logical_sector_bytes: decoder.u32_le()?,
            extent_bytes: decoder.u32_le()?,
        };
        geometry.validate_range(0, 0)?;
        let extent_map_root = decoder.object_ref()?;
        decoder.finish()?;
        if extent_map_root.object_type != ObjectType::VolumeMapNode {
            return Err(FormatError::ObjectReferenceMismatch);
        }
        Ok(Self {
            geometry,
            extent_map_root,
        })
    }
}

/// Complete immutable ordered-log map root.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LogMapRoot {
    /// Root node of the log-name to log-state map.
    pub log_index_root: ObjectRef,
    /// Number of visible named logs in the map.
    pub log_count: u64,
}

impl LogMapRoot {
    /// Encodes this complete root as a typed immutable object.
    pub fn encode(self) -> Result<Vec<u8>, FormatError> {
        if self.log_index_root.object_type != ObjectType::LogMapNode {
            return Err(FormatError::ObjectReferenceMismatch);
        }
        self.log_index_root.validate()?;
        let mut encoder = CanonicalEncoder::new();
        encoder.u64_le(self.log_count);
        encoder.object_ref(self.log_index_root);
        encode_object(ObjectType::LogMapRoot, &encoder.finish())
    }

    /// Decodes a complete native log-map root.
    pub fn decode(bytes: &[u8]) -> Result<Self, FormatError> {
        let object = decode_object(bytes)?;
        if object.object_type != ObjectType::LogMapRoot {
            return Err(FormatError::ObjectReferenceMismatch);
        }
        let mut decoder = CanonicalDecoder::new(object.payload);
        let log_count = decoder.u64_le()?;
        let log_index_root = decoder.object_ref()?;
        decoder.finish()?;
        if log_index_root.object_type != ObjectType::LogMapNode {
            return Err(FormatError::ObjectReferenceMismatch);
        }
        Ok(Self {
            log_index_root,
            log_count,
        })
    }
}

/// One named log's complete visible range and segment index.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LogMapEntry {
    /// Stable log identity, independent of its display name.
    pub log_id: LogId,
    /// First visible sequence number, inclusive.
    pub first_seq: u64,
    /// End of the visible sequence range, exclusive.
    pub end_seq: u64,
    /// Root of the persistent segment-range index.
    pub segment_index_root: ObjectRef,
}

impl LogMapEntry {
    /// Encodes a log-map leaf value.
    pub fn encode(self) -> Result<Vec<u8>, FormatError> {
        if self.first_seq > self.end_seq
            || self.segment_index_root.object_type != ObjectType::LogSegmentIndexNode
        {
            return Err(FormatError::InvalidNativeView(
                "invalid log range or index root",
            ));
        }
        self.segment_index_root.validate()?;
        let mut encoder = CanonicalEncoder::new();
        encoder.fixed(self.log_id.as_bytes());
        encoder.u64_le(self.first_seq);
        encoder.u64_le(self.end_seq);
        encoder.object_ref(self.segment_index_root);
        Ok(encoder.finish())
    }

    /// Decodes a log-map leaf value.
    pub fn decode(bytes: &[u8]) -> Result<Self, FormatError> {
        let mut decoder = CanonicalDecoder::new(bytes);
        let log_id = LogId::from_bytes(decoder.fixed()?);
        let first_seq = decoder.u64_le()?;
        let end_seq = decoder.u64_le()?;
        let segment_index_root = decoder.object_ref()?;
        decoder.finish()?;
        if first_seq > end_seq || segment_index_root.object_type != ObjectType::LogSegmentIndexNode
        {
            return Err(FormatError::InvalidNativeView(
                "invalid log range or index root",
            ));
        }
        Ok(Self {
            log_id,
            first_seq,
            end_seq,
            segment_index_root,
        })
    }
}

/// A segment-index leaf value, with an exclusive end sequence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SegmentIndexEntry {
    /// Exclusive end sequence of the referenced sealed segment.
    pub end_seq: u64,
    /// Immutable sealed log segment.
    pub segment: ObjectRef,
}

impl SegmentIndexEntry {
    /// Encodes a segment-index leaf value.
    pub fn encode(self) -> Result<Vec<u8>, FormatError> {
        if self.segment.object_type != ObjectType::LogSegment {
            return Err(FormatError::ObjectReferenceMismatch);
        }
        self.segment.validate()?;
        let mut encoder = CanonicalEncoder::new();
        encoder.u64_le(self.end_seq);
        encoder.object_ref(self.segment);
        Ok(encoder.finish())
    }

    /// Decodes a segment-index leaf value.
    pub fn decode(bytes: &[u8]) -> Result<Self, FormatError> {
        let mut decoder = CanonicalDecoder::new(bytes);
        let end_seq = decoder.u64_le()?;
        let segment = decoder.object_ref()?;
        decoder.finish()?;
        if segment.object_type != ObjectType::LogSegment {
            return Err(FormatError::ObjectReferenceMismatch);
        }
        Ok(Self { end_seq, segment })
    }
}

/// One named immutable artifact and its declared payload dependencies.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArtifactDescriptor {
    /// Stable artifact identity, independent of its name.
    pub artifact_id: ArtifactId,
    /// Exact logical payload length.
    pub logical_len: u64,
    /// Blob reference for small artifacts or chunk-index root for large ones.
    pub content_root: ObjectRef,
    /// Explicit strong references to artifact payload dependencies.
    pub dependencies: Vec<ObjectRef>,
}

impl ArtifactDescriptor {
    /// Encodes an artifact-map leaf value.
    pub fn encode(&self) -> Result<Vec<u8>, FormatError> {
        let expected_type = if self.logical_len <= MAX_ARTIFACT_BLOB_BYTES as u64 {
            ObjectType::ArtifactBlob
        } else {
            ObjectType::ArtifactIndexNode
        };
        if self.content_root.object_type != expected_type {
            return Err(FormatError::InvalidNativeView(
                "artifact length and content representation disagree",
            ));
        }
        if expected_type == ObjectType::ArtifactBlob
            && self.content_root.encoded_len
                != (ENVELOPE_HEADER_BYTES as u64).saturating_add(self.logical_len)
        {
            return Err(FormatError::InvalidNativeView(
                "inline artifact length differs from its blob reference",
            ));
        }
        self.content_root.validate()?;
        let dependency_bytes = self
            .dependencies
            .len()
            .checked_mul(OBJECT_REFERENCE_BYTES)
            .and_then(|size| size.checked_add(4 + 16 + 8 + OBJECT_REFERENCE_BYTES))
            .ok_or(FormatError::InvalidNativeView(
                "artifact dependency count overflow",
            ))?;
        // The descriptor is stored as one leaf value. Reserve the object
        // envelope, leaf count/entry framing, and shortest valid UTF-8 name so
        // every accepted descriptor fits at least one artifact-map node.
        let max_leaf_value_bytes = MAX_PERSISTENT_NODE_BYTES - ENVELOPE_HEADER_BYTES - 4 - 8 - 1;
        if dependency_bytes > max_leaf_value_bytes {
            return Err(FormatError::PayloadTooLarge {
                actual: dependency_bytes,
                maximum: max_leaf_value_bytes,
            });
        }
        let mut encoder = CanonicalEncoder::new();
        encoder.fixed(self.artifact_id.as_bytes());
        encoder.u64_le(self.logical_len);
        encoder.object_ref(self.content_root);
        encoder.u32_le(
            u32::try_from(self.dependencies.len())
                .map_err(|_| FormatError::InvalidNativeView("too many artifact dependencies"))?,
        );
        for dependency in &self.dependencies {
            if !matches!(
                dependency.object_type,
                ObjectType::ArtifactBlob | ObjectType::ArtifactChunk
            ) {
                return Err(FormatError::InvalidNativeView(
                    "artifact dependencies must reference immutable payload objects",
                ));
            }
            dependency.validate()?;
            encoder.object_ref(*dependency);
        }
        Ok(encoder.finish())
    }

    /// Decodes an artifact-map leaf value.
    pub fn decode(bytes: &[u8]) -> Result<Self, FormatError> {
        let mut decoder = CanonicalDecoder::new(bytes);
        let artifact_id = ArtifactId::from_bytes(decoder.fixed()?);
        let logical_len = decoder.u64_le()?;
        let content_root = decoder.object_ref()?;
        let dependency_count = usize::try_from(decoder.u32_le()?)
            .map_err(|_| FormatError::InvalidNativeView("dependency count overflow"))?;
        let dependency_bytes = dependency_count
            .checked_mul(OBJECT_REFERENCE_BYTES)
            .ok_or(FormatError::InvalidNativeView("dependency count overflow"))?;
        let max_leaf_value_bytes = MAX_PERSISTENT_NODE_BYTES - ENVELOPE_HEADER_BYTES - 4 - 8 - 1;
        let descriptor_bytes = dependency_bytes
            .checked_add(4 + 16 + 8 + OBJECT_REFERENCE_BYTES)
            .ok_or(FormatError::InvalidNativeView("dependency count overflow"))?;
        if descriptor_bytes > max_leaf_value_bytes {
            return Err(FormatError::PayloadTooLarge {
                actual: descriptor_bytes,
                maximum: max_leaf_value_bytes,
            });
        }
        if dependency_bytes > decoder.bytes.len().saturating_sub(decoder.offset) {
            return Err(FormatError::InvalidNativeView("truncated dependency list"));
        }
        let mut dependencies = Vec::with_capacity(dependency_count);
        for _ in 0..dependency_count {
            let dependency = decoder.object_ref()?;
            if !matches!(
                dependency.object_type,
                ObjectType::ArtifactBlob | ObjectType::ArtifactChunk
            ) {
                return Err(FormatError::InvalidNativeView(
                    "artifact dependencies must reference immutable payload objects",
                ));
            }
            dependencies.push(dependency);
        }
        decoder.finish()?;
        let descriptor = Self {
            artifact_id,
            logical_len,
            content_root,
            dependencies,
        };
        let expected_type = if logical_len <= MAX_ARTIFACT_BLOB_BYTES as u64 {
            ObjectType::ArtifactBlob
        } else {
            ObjectType::ArtifactIndexNode
        };
        if descriptor.content_root.object_type != expected_type {
            return Err(FormatError::InvalidNativeView(
                "artifact length and content representation disagree",
            ));
        }
        if expected_type == ObjectType::ArtifactBlob
            && descriptor.content_root.encoded_len
                != (ENVELOPE_HEADER_BYTES as u64).saturating_add(logical_len)
        {
            return Err(FormatError::InvalidNativeView(
                "inline artifact length differs from its blob reference",
            ));
        }
        Ok(descriptor)
    }
}

/// One artifact-index leaf value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ArtifactChunkEntry {
    /// Actual payload bytes in this chunk.
    pub actual_len: u32,
    /// Immutable artifact-chunk object reference.
    pub chunk: ObjectRef,
}

impl ArtifactChunkEntry {
    /// Encodes one artifact-chunk index leaf value.
    pub fn encode(self) -> Result<Vec<u8>, FormatError> {
        if self.actual_len == 0
            || self.chunk.object_type != ObjectType::ArtifactChunk
            || self.actual_len as usize > ARTIFACT_CHUNK_BYTES
            || self.chunk.encoded_len != (ENVELOPE_HEADER_BYTES + self.actual_len as usize) as u64
        {
            return Err(FormatError::InvalidNativeView(
                "invalid artifact chunk length",
            ));
        }
        self.chunk.validate()?;
        let mut encoder = CanonicalEncoder::new();
        encoder.u32_le(self.actual_len);
        encoder.object_ref(self.chunk);
        Ok(encoder.finish())
    }

    /// Decodes one artifact-chunk index leaf value.
    pub fn decode(bytes: &[u8]) -> Result<Self, FormatError> {
        let mut decoder = CanonicalDecoder::new(bytes);
        let actual_len = decoder.u32_le()?;
        let chunk = decoder.object_ref()?;
        decoder.finish()?;
        let entry = Self { actual_len, chunk };
        entry.encode()?;
        Ok(entry)
    }
}

/// Complete immutable named-artifact map root.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ArtifactMapRoot {
    /// Root node of the artifact-name map.
    pub artifact_index_root: ObjectRef,
    /// Number of visible named artifacts in the map.
    pub artifact_count: u64,
}

impl ArtifactMapRoot {
    /// Encodes this complete root as a typed immutable object.
    pub fn encode(self) -> Result<Vec<u8>, FormatError> {
        if self.artifact_index_root.object_type != ObjectType::ArtifactMapNode {
            return Err(FormatError::ObjectReferenceMismatch);
        }
        self.artifact_index_root.validate()?;
        let mut encoder = CanonicalEncoder::new();
        encoder.u64_le(self.artifact_count);
        encoder.object_ref(self.artifact_index_root);
        encode_object(ObjectType::ArtifactMapRoot, &encoder.finish())
    }

    /// Decodes a complete native artifact-map root.
    pub fn decode(bytes: &[u8]) -> Result<Self, FormatError> {
        let object = decode_object(bytes)?;
        if object.object_type != ObjectType::ArtifactMapRoot {
            return Err(FormatError::ObjectReferenceMismatch);
        }
        let mut decoder = CanonicalDecoder::new(object.payload);
        let artifact_count = decoder.u64_le()?;
        let artifact_index_root = decoder.object_ref()?;
        decoder.finish()?;
        if artifact_index_root.object_type != ObjectType::ArtifactMapNode {
            return Err(FormatError::ObjectReferenceMismatch);
        }
        Ok(Self {
            artifact_index_root,
            artifact_count,
        })
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
        let payload_len = self
            .attachments
            .len()
            .checked_mul(OBJECT_REFERENCE_BYTES)
            .and_then(|total| total.checked_add(LOG_SEGMENT_FIXED_PAYLOAD_BYTES))
            .and_then(|total| total.checked_add(record_bytes))
            .ok_or(FormatError::InvalidLogSegment)?;
        if payload_len > MAX_OBJECT_PAYLOAD_BYTES {
            return Err(FormatError::PayloadTooLarge {
                actual: payload_len,
                maximum: MAX_OBJECT_PAYLOAD_BYTES,
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

    /// Decodes a complete sealed segment and validates its ordered range.
    pub fn decode(bytes: &[u8]) -> Result<Self, FormatError> {
        let object = decode_object(bytes)?;
        if object.object_type != ObjectType::LogSegment {
            return Err(FormatError::ObjectReferenceMismatch);
        }
        let mut decoder = CanonicalDecoder::new(object.payload);
        let log_id = LogId::from_bytes(decoder.fixed()?);
        let first_seq = decoder.u64_le()?;
        let end_seq = decoder.u64_le()?;
        let record_count =
            usize::try_from(decoder.u32_le()?).map_err(|_| FormatError::InvalidLogSegment)?;
        if record_count == 0 || record_count > MAX_LOG_RECORDS {
            return Err(FormatError::InvalidLogSegment);
        }
        let mut records = Vec::with_capacity(record_count);
        for _ in 0..record_count {
            records.push(decoder.bytes(MAX_LOG_RECORD_BYTES)?.to_vec());
        }
        let attachment_count =
            usize::try_from(decoder.u32_le()?).map_err(|_| FormatError::InvalidLogSegment)?;
        let remaining = decoder.bytes.len().saturating_sub(decoder.offset);
        let attachment_bytes = attachment_count
            .checked_mul(OBJECT_REFERENCE_BYTES)
            .ok_or(FormatError::InvalidLogSegment)?;
        if attachment_bytes > remaining {
            return Err(FormatError::InvalidLogSegment);
        }
        let mut attachments = Vec::with_capacity(attachment_count);
        for _ in 0..attachment_count {
            attachments.push(decoder.object_ref()?);
        }
        decoder.finish()?;
        let segment = Self {
            log_id,
            first_seq,
            end_seq,
            records,
            attachments,
        };
        segment.encode()?;
        Ok(segment)
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

    /// Decodes a complete immutable Generation descriptor.
    pub fn decode(bytes: &[u8]) -> Result<Self, FormatError> {
        let object = decode_object(bytes)?;
        if object.object_type != ObjectType::GenerationDescriptor {
            return Err(FormatError::ObjectReferenceMismatch);
        }
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
            capture_mode,
            volume_boundary_id,
            created_at_unix_ms,
        };
        descriptor.encode()?;
        Ok(descriptor)
    }
}
