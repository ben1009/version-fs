//! Compatibility and golden-vector tests for provenance-capable descriptors.

use versionfs_core::format::{
    CaptureMode, DecodedGenerationDescriptor, FormatError, GenerationDescriptor,
    GenerationDescriptorFormat, GenerationDescriptorV2, MAX_PROVENANCE_CONTEXT_BYTES, ObjectRef,
    ObjectType, ProvenanceBinding, ProvenanceContext, ProvenanceCoverage, ProvenanceStatus,
    decode_generation_descriptor, decode_object, decode_object_versioned, object_id,
};
use versionfs_core::identity::{GenerationId, ObjectId, PrincipalId, VolumeBoundaryId};

const VECTORS: &str = include_str!("../../fixtures/storage-format-v2.tsv");
const V1_VECTORS: &str = include_str!("../../fixtures/storage-format-v1.tsv");

fn fixture_in(vectors: &'static str, name: &str) -> (&'static str, &'static str) {
    vectors
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

fn fixture(name: &str) -> (&'static str, &'static str) {
    fixture_in(VECTORS, name)
}

fn decode_hex(value: &str) -> Vec<u8> {
    value
        .as_bytes()
        .chunks(2)
        .map(|pair| {
            let [high, low] = pair else {
                panic!("hex byte must have two digits");
            };
            let high = (*high as char).to_digit(16).expect("hex digit");
            let low = (*low as char).to_digit(16).expect("hex digit");
            ((high << 4) | low) as u8
        })
        .collect()
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

fn context() -> ProvenanceContext {
    ProvenanceContext {
        recording_principal: PrincipalId::from_bytes(std::array::from_fn(|i| i as u8 + 1)),
        actor: Some("codex".to_owned()),
        execution_id: Some("exec-7".to_owned()),
        session_id: Some("session-2".to_owned()),
        thread_id: None,
        task_id: Some("task-99".to_owned()),
        workflow_operation_id: Some("op-3".to_owned()),
        reason: Some("fix recovery".to_owned()),
        trace_id: Some("0123456789abcdef0123456789abcdef".to_owned()),
        span_id: Some("0123456789abcdef".to_owned()),
        parent_span_id: None,
        required_logs_root: reference(ObjectType::LogMapRoot, 0x22, 58),
        required_artifacts_root: reference(ObjectType::ArtifactMapRoot, 0x33, 58),
        optional_links: vec![
            "https://example.test/a".to_owned(),
            "https://example.test/b".to_owned(),
        ],
    }
}

fn descriptor(provenance: ProvenanceBinding) -> GenerationDescriptorV2 {
    GenerationDescriptorV2 {
        generation_id: GenerationId::from_bytes(std::array::from_fn(|i| 0x10 + i as u8)),
        parent_generation_id: None,
        volume_root: reference(ObjectType::VolumeRoot, 0x11, 74),
        logs_root: reference(ObjectType::LogMapRoot, 0x22, 58),
        artifacts_root: reference(ObjectType::ArtifactMapRoot, 0x33, 58),
        provenance,
        capture_mode: CaptureMode::BlockCut,
        volume_boundary_id: VolumeBoundaryId::from_bytes(std::array::from_fn(|i| 0x40 + i as u8)),
        created_at_unix_ms: 123_456_789,
    }
}

#[test]
fn provenance_context_and_v2_descriptors_match_shared_golden_vectors() {
    let context_bytes = context().encode().expect("encode context");
    assert_vector("provenance_context_recorded", &context_bytes);
    assert_eq!(
        ProvenanceContext::decode(&context_bytes).expect("decode context"),
        context()
    );

    assert_vector(
        "generation_descriptor_v2_unattributed",
        &descriptor(ProvenanceBinding::Unattributed)
            .encode()
            .expect("encode unattributed descriptor"),
    );

    let context_ref = ObjectRef::from_encoded(&context_bytes).expect("typed context reference");
    assert_vector(
        "generation_descriptor_v2_recorded",
        &descriptor(ProvenanceBinding::Recorded {
            context_root: context_ref,
        })
        .encode()
        .expect("encode recorded descriptor"),
    );
}

#[test]
fn v1_descriptor_bytes_remain_stable_and_upgrade_as_explicitly_unattributed() {
    let (encoded, _) = fixture_in(V1_VECTORS, "generation_descriptor_block_cut");
    let bytes = decode_hex(encoded);
    let decoded = decode_generation_descriptor(&bytes).expect("decode v1 descriptor");
    let DecodedGenerationDescriptor::V1(descriptor) = decoded else {
        panic!("legacy bytes must retain the v1 source format");
    };
    assert_eq!(descriptor.encode().expect("re-encode v1"), bytes);
    assert_eq!(decoded.source_format(), GenerationDescriptorFormat::V1);
    assert_eq!(decoded.provenance_status(), ProvenanceStatus::Unattributed);
    assert_eq!(decoded.provenance_coverage(), ProvenanceCoverage::None);
    assert_eq!(decoded.context_root(), None);
}

#[test]
fn v2_records_dispatch_and_decode_without_a_payload_version_field() {
    let context_bytes = context().encode().expect("encode context");
    let context_ref = ObjectRef::from_encoded(&context_bytes).expect("context ref");
    let encoded = descriptor(ProvenanceBinding::Recorded {
        context_root: context_ref,
    })
    .encode()
    .expect("encode v2 descriptor");

    let envelope = decode_object_versioned(&encoded).expect("upgraded envelope dispatch");
    assert_eq!(envelope.format_version, 2);
    assert_eq!(envelope.object_type, ObjectType::GenerationDescriptor);
    assert_eq!(
        decode_generation_descriptor(&encoded).expect("decode v2 descriptor"),
        DecodedGenerationDescriptor::V2(
            GenerationDescriptorV2::decode(&encoded).expect("decode typed v2 descriptor")
        )
    );
    assert_eq!(
        decode_generation_descriptor(&encoded)
            .expect("descriptor")
            .provenance_status(),
        ProvenanceStatus::Recorded
    );
    assert_eq!(
        decode_generation_descriptor(&encoded)
            .expect("descriptor")
            .provenance_coverage(),
        ProvenanceCoverage::GenerationContext
    );
    assert_eq!(
        decode_generation_descriptor(&encoded)
            .expect("descriptor")
            .context_root(),
        Some(context_ref)
    );
}

#[test]
fn legacy_decoder_rejects_v2_before_reading_the_payload() {
    let mut bytes = descriptor(ProvenanceBinding::Unattributed)
        .encode()
        .expect("encode v2 descriptor");
    // Make the declared payload length invalid. A v1-only decoder must reject
    // the version/type pair before it reaches payload-length validation.
    bytes[8..16].copy_from_slice(&u64::MAX.to_le_bytes());
    assert_eq!(
        decode_object(&bytes),
        Err(FormatError::UnsupportedObjectVersion {
            version: 2,
            object_type: ObjectType::GenerationDescriptor,
        })
    );
    assert_eq!(
        GenerationDescriptor::decode(&bytes),
        Err(FormatError::UnsupportedObjectVersion {
            version: 2,
            object_type: ObjectType::GenerationDescriptor,
        })
    );
}

#[test]
fn recorded_descriptors_reject_oversized_context_references() {
    let context_root = reference(
        ObjectType::ProvenanceContext,
        0x55,
        MAX_PROVENANCE_CONTEXT_BYTES as u64,
    );
    let at_limit = descriptor(ProvenanceBinding::Recorded { context_root });
    let mut bytes = at_limit.encode().expect("reference at the size limit");
    assert_eq!(
        GenerationDescriptorV2::decode(&bytes).expect("decode reference at the size limit"),
        at_limit
    );

    let oversized_root = ObjectRef {
        encoded_len: context_root.encoded_len + 1,
        ..context_root
    };
    assert!(matches!(
        descriptor(ProvenanceBinding::Recorded {
            context_root: oversized_root,
        })
        .encode(),
        Err(FormatError::InvalidProvenance(_))
    ));

    // Header, generation ID, absent parent, three view roots, status/coverage,
    // then the context reference's type and digest precede its encoded length.
    let context_len_offset = 16 + 16 + 1 + 3 * 42 + 2 + 2 + 32;
    bytes[context_len_offset..context_len_offset + 8]
        .copy_from_slice(&oversized_root.encoded_len.to_le_bytes());
    assert!(matches!(
        GenerationDescriptorV2::decode(&bytes),
        Err(FormatError::InvalidProvenance(_))
    ));
    assert!(matches!(
        decode_generation_descriptor(&bytes),
        Err(FormatError::InvalidProvenance(_))
    ));
}

#[test]
fn malformed_v2_provenance_and_context_are_rejected() {
    let mut bytes = descriptor(ProvenanceBinding::Unattributed)
        .encode()
        .expect("encode v2 descriptor");
    // Header (16), generation ID (16), absent parent tag (1), and three refs.
    let status_offset = 16 + 16 + 1 + 3 * 42;
    bytes[status_offset] = ProvenanceStatus::Recorded as u8;
    assert!(matches!(
        GenerationDescriptorV2::decode(&bytes),
        Err(FormatError::InvalidProvenance(_))
    ));

    let mut invalid_context = context();
    invalid_context.required_logs_root = reference(ObjectType::ArtifactMapRoot, 0x44, 58);
    assert!(matches!(
        invalid_context.encode(),
        Err(FormatError::InvalidProvenance(_))
    ));

    let mut unsorted = context();
    unsorted.optional_links.reverse();
    assert!(matches!(
        unsorted.encode(),
        Err(FormatError::InvalidProvenance(_))
    ));
}

#[test]
fn provenance_context_enforces_the_complete_object_size_limit() {
    let long = "x".repeat(4096);
    let mut oversized = context();
    oversized.actor = Some(long.clone());
    oversized.execution_id = Some(long.clone());
    oversized.session_id = Some(long.clone());
    oversized.thread_id = Some(long.clone());
    oversized.task_id = Some(long.clone());
    oversized.workflow_operation_id = Some(long.clone());
    oversized.reason = Some(long.clone());
    oversized.trace_id = Some(long.clone());
    oversized.span_id = Some(long.clone());
    oversized.parent_span_id = Some(long);
    let link_prefix = "a".repeat(4094);
    oversized.optional_links = (0..8)
        .map(|index| format!("{link_prefix}{index:02}"))
        .collect();
    assert!(matches!(
        oversized.encode(),
        Err(FormatError::PayloadTooLarge {
            maximum: 65_536,
            ..
        })
    ));
}
