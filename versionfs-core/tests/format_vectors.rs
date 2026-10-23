//! Checks the canonical on-disk byte vectors shared with non-Rust readers.

use versionfs_core::format::{
    CaptureMode, GenerationDescriptor, LogSegment, MetadataRecordType, ObjectRef, ObjectType,
    VolumeGeometry, VolumeRoot, encode_metadata_record, encode_object, encode_ordered_entries,
    object_id, ordered_u64_key,
};
use versionfs_core::identity::{GenerationId, LogId, ObjectId, VolumeBoundaryId};

const VECTORS: &str = include_str!("../../fixtures/storage-format-v1.tsv");

fn fixture(name: &str) -> (&'static str, &'static str) {
    VECTORS
        .lines()
        .filter(|line| !line.starts_with('#') && !line.trim().is_empty())
        .filter_map(|line| {
            let mut columns = line.split('\t');
            let fixture_name = columns.next()?;
            let encoded = columns.next()?;
            let digest = columns.next()?;
            (fixture_name == name).then_some((encoded, digest))
        })
        .next()
        .unwrap_or_else(|| panic!("missing format vector {name}"))
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn assert_vector(name: &str, encoded: &[u8]) {
    let (expected_bytes, expected_digest) = fixture(name);
    assert_eq!(hex(encoded), expected_bytes, "encoded bytes for {name}");
    assert_eq!(
        object_id(encoded).to_string(),
        expected_digest,
        "digest for {name}"
    );
}

fn reference(object_type: ObjectType, fill: u8, encoded_len: u64) -> ObjectRef {
    ObjectRef {
        object_type,
        object_id: ObjectId::from_bytes([fill; 32]),
        encoded_len,
    }
}

#[test]
fn canonical_envelopes_match_shared_golden_vectors() {
    assert_vector(
        "artifact_chunk_abc",
        &encode_object(ObjectType::ArtifactChunk, b"abc").unwrap(),
    );
    assert_vector(
        "empty_artifact_blob",
        &encode_object(ObjectType::ArtifactBlob, &[]).unwrap(),
    );
    assert_vector(
        "empty_volume_map_entries",
        &encode_object(
            ObjectType::VolumeMapNode,
            &encode_ordered_entries(&[]).unwrap(),
        )
        .unwrap(),
    );
    assert_vector(
        "metadata_branch_v1",
        &encode_metadata_record(MetadataRecordType::Branch, &[1, 2, 3]).unwrap(),
    );
}

#[test]
fn complete_roots_and_ordered_log_segments_match_golden_vectors() {
    let volume = VolumeRoot {
        geometry: VolumeGeometry::new(128 * 1024 * 1024).unwrap(),
        extent_map_root: reference(ObjectType::VolumeMapNode, 0x11, 20),
    };
    assert_vector("volume_root_128_mib", &volume.encode().unwrap());

    let segment = LogSegment {
        log_id: LogId::from_bytes(std::array::from_fn(|index| index as u8)),
        first_seq: 7,
        end_seq: 8,
        records: vec![b"x".to_vec()],
        attachments: vec![],
    };
    assert_vector("log_segment_one_record", &segment.encode().unwrap());

    let descriptor = GenerationDescriptor {
        generation_id: GenerationId::from_bytes(std::array::from_fn(|index| 0x10 + index as u8)),
        parent_generation_id: None,
        volume_root: reference(ObjectType::VolumeRoot, 0x11, 74),
        logs_root: reference(ObjectType::LogMapRoot, 0x22, 58),
        artifacts_root: reference(ObjectType::ArtifactMapRoot, 0x33, 58),
        capture_mode: CaptureMode::BlockCut,
        volume_boundary_id: VolumeBoundaryId::from_bytes(std::array::from_fn(|index| {
            0x40 + index as u8
        })),
        created_at_unix_ms: 123_456_789,
    };
    assert_vector(
        "generation_descriptor_block_cut",
        &descriptor.encode().unwrap(),
    );
}

#[test]
fn numeric_map_keys_sort_in_unsigned_numeric_order_and_capture_modes_are_stable() {
    assert!(ordered_u64_key(1) < ordered_u64_key(256));
    assert_eq!(CaptureMode::BlockCut as u8, 1);
    assert_eq!(CaptureMode::FilesystemFrozen as u8, 2);
    assert_eq!(CaptureMode::FilesystemCleanUnmount as u8, 3);
}
