//! Opt-in integration test for the real S3 object-store contract.

use std::env;
use std::time::{SystemTime, UNIX_EPOCH};
use versionfs_core::format::{ObjectType, encode_object};
use versionfs_core::identity::WorkspaceId;
use versionfs_service::object_store::{
    ObjectStore, PutDisposition, S3ObjectStore, S3ObjectStoreConfig,
};

/// Exercises the S3 object contract against an explicitly dedicated test bucket.
///
/// Run with `cargo test --locked -p versionfs-service --test s3_conformance
/// -- --ignored --nocapture` after setting `VERSIONFS_S3_CONFORMANCE_BUCKET`,
/// `VERSIONFS_S3_CONFORMANCE_PREFIX`, and the standard AWS credential variables.
/// With `s3-emulator`, set `VERSIONFS_S3_EMULATOR_ENDPOINT` to use local RustFS.
#[tokio::test]
#[ignore = "requires a dedicated S3-compatible Version FS conformance bucket"]
async fn s3_conditional_create_verified_read_and_inventory() {
    let bucket = required_env("VERSIONFS_S3_CONFORMANCE_BUCKET");
    assert!(
        bucket.starts_with("versionfs-test-"),
        "refusing to write outside a bucket named versionfs-test-*"
    );
    let prefix = required_env("VERSIONFS_S3_CONFORMANCE_PREFIX");
    assert!(
        prefix.starts_with("versionfs-conformance/"),
        "the conformance prefix must begin with versionfs-conformance/"
    );

    let store = conformance_store(bucket, prefix);
    let workspace = fresh_workspace_id();
    let encoded = encode_object(ObjectType::ArtifactBlob, b"Version FS S3 conformance")
        .expect("test payload fits the canonical object format");
    let second_encoded = encode_object(ObjectType::ArtifactBlob, b"second Version FS S3 object")
        .expect("test payload fits the canonical object format");

    let created = store
        .put(workspace, &encoded)
        .await
        .expect("S3 conditional create must succeed");
    assert_eq!(created.disposition, PutDisposition::Created);
    let second_created = store
        .put(workspace, &second_encoded)
        .await
        .expect("a second distinct S3 object must be created");
    assert_eq!(second_created.disposition, PutDisposition::Created);
    assert_ne!(
        created.object_ref.object_id,
        second_created.object_ref.object_id
    );

    let reused = store
        .put(workspace, &encoded)
        .await
        .expect("duplicate S3 put must verify and reuse the immutable object");
    assert_eq!(reused.disposition, PutDisposition::Reused);
    assert_eq!(reused.object_ref, created.object_ref);

    assert_eq!(
        store
            .get(workspace, created.object_ref)
            .await
            .expect("S3 GET must return complete verified bytes"),
        encoded
    );

    let first_page = store
        .inventory_page(workspace, None, 1)
        .await
        .expect("first S3 inventory page must return one workspace-scoped object");
    assert_eq!(first_page.entries.len(), 1);
    let cursor = first_page
        .next_cursor
        .as_ref()
        .expect("multiple matching keys must produce a continuation cursor");

    let second_page = store
        .inventory_page(workspace, Some(cursor), 1)
        .await
        .expect("S3 listing must continue from the returned cursor");
    assert_eq!(second_page.entries.len(), 1);
    assert!(second_page.next_cursor.is_none());

    let listed = [first_page.entries[0], second_page.entries[0]];
    assert_ne!(listed[0].object_id, listed[1].object_id);
    for expected in [created.object_ref, second_created.object_ref] {
        assert_eq!(
            listed
                .iter()
                .filter(|entry| {
                    entry.object_id == expected.object_id
                        && entry.object_type == expected.object_type
                        && entry.observed_encoded_len == expected.encoded_len
                })
                .count(),
            1,
            "every created object must appear exactly once across inventory pages"
        );
    }
}

fn conformance_store(bucket: String, prefix: String) -> S3ObjectStore {
    let config = S3ObjectStoreConfig::new(bucket, prefix);
    #[cfg(feature = "s3-emulator")]
    let config = match env::var("VERSIONFS_S3_EMULATOR_ENDPOINT") {
        Ok(endpoint) => config.with_local_emulator_endpoint(endpoint),
        Err(_) => config,
    };
    #[cfg(not(feature = "s3-emulator"))]
    assert!(
        env::var_os("VERSIONFS_S3_EMULATOR_ENDPOINT").is_none(),
        "VERSIONFS_S3_EMULATOR_ENDPOINT requires the s3-emulator feature"
    );

    S3ObjectStore::connect(config).expect("S3 configuration and credential chain must be valid")
}

fn required_env(name: &str) -> String {
    env::var(name).unwrap_or_else(|_| panic!("{name} must be set for the ignored S3 test"))
}

fn fresh_workspace_id() -> WorkspaceId {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is after the Unix epoch")
        .as_nanos()
        .to_be_bytes();
    let process_id = std::process::id().to_be_bytes();
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&timestamp);
    for (index, byte) in process_id.iter().enumerate() {
        bytes[bytes.len() - process_id.len() + index] ^= byte;
    }
    WorkspaceId::from_bytes(bytes)
}
