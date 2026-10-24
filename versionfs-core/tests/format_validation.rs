//! Payload-budget checks for canonical log segments.

use versionfs_core::format::{
    ArtifactChunkEntry, ArtifactDescriptor, FormatError, LogSegment, MAX_OBJECT_PAYLOAD_BYTES,
    ObjectRef, ObjectType, decode_object, encode_object,
};
use versionfs_core::identity::{ArtifactId, LogId, ObjectId};

// A segment with one one-byte record has 40 fixed bytes and a five-byte record.
const ONE_RECORD_PAYLOAD_BYTES: usize = 45;
const REFERENCE_BYTES: usize = 42;
const MAX_ATTACHMENTS: usize =
    (MAX_OBJECT_PAYLOAD_BYTES - ONE_RECORD_PAYLOAD_BYTES) / REFERENCE_BYTES;

fn segment(attachments: Vec<ObjectRef>) -> LogSegment {
    LogSegment {
        log_id: LogId::from_bytes([1; 16]),
        first_seq: 0,
        end_seq: 1,
        records: vec![b"x".to_vec()],
        attachments,
    }
}

fn attachment() -> ObjectRef {
    ObjectRef::from_encoded(&encode_object(ObjectType::ArtifactBlob, b"x").unwrap()).unwrap()
}

#[test]
fn largest_attachment_list_that_fits_the_payload_budget_is_accepted() {
    let encoded = segment(vec![attachment(); MAX_ATTACHMENTS])
        .encode()
        .unwrap();
    let object = decode_object(&encoded).unwrap();
    assert_eq!(object.object_type, ObjectType::LogSegment);
    assert_eq!(
        object.payload.len(),
        ONE_RECORD_PAYLOAD_BYTES + MAX_ATTACHMENTS * REFERENCE_BYTES
    );
    assert!(object.payload.len() <= MAX_OBJECT_PAYLOAD_BYTES);
}

#[test]
fn oversized_attachment_list_is_rejected_before_attachment_traversal() {
    let mut invalid_attachment = attachment();
    invalid_attachment.encoded_len = 0;
    // A size rejection must precede reference validation and buffer construction.
    let oversized = segment(vec![invalid_attachment; MAX_ATTACHMENTS + 1]);
    assert_eq!(
        oversized.encode(),
        Err(FormatError::PayloadTooLarge {
            actual: ONE_RECORD_PAYLOAD_BYTES + (MAX_ATTACHMENTS + 1) * REFERENCE_BYTES,
            maximum: MAX_OBJECT_PAYLOAD_BYTES,
        })
    );
}

#[test]
fn artifact_dependencies_fit_one_minimal_name_leaf() {
    let reference = |object_type| ObjectRef {
        object_type,
        object_id: ObjectId::from_bytes([1; 32]),
        encoded_len: 16,
    };
    let descriptor = ArtifactDescriptor {
        artifact_id: ArtifactId::from_bytes([2; 16]),
        logical_len: 65_537,
        content_root: reference(ObjectType::ArtifactIndexNode),
        dependencies: vec![reference(ObjectType::ArtifactBlob); 1558],
    };
    let encoded = descriptor.encode().unwrap();
    assert_eq!(encoded.len(), 65_506);

    let oversized = ArtifactDescriptor {
        dependencies: vec![reference(ObjectType::ArtifactBlob); 1559],
        ..descriptor
    };
    assert!(matches!(
        oversized.encode(),
        Err(FormatError::PayloadTooLarge { .. })
    ));
}

#[test]
fn artifact_chunk_entries_reject_zero_length_chunks() {
    let entry = ArtifactChunkEntry {
        actual_len: 0,
        chunk: ObjectRef {
            object_type: ObjectType::ArtifactChunk,
            object_id: ObjectId::from_bytes([3; 32]),
            encoded_len: 16,
        },
    };
    assert!(matches!(
        entry.encode(),
        Err(FormatError::InvalidNativeView(_))
    ));
}
