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
#[tokio::test]
#[ignore = "requires a dedicated Version FS S3 conformance bucket"]
async fn real_s3_conditional_create_verified_read_and_inventory() {
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

    let store = S3ObjectStore::connect(S3ObjectStoreConfig::new(bucket, prefix))
        .expect("S3 configuration and AWS credential chain must be valid");
    let workspace = fresh_workspace_id();
    let encoded = encode_object(ObjectType::ArtifactBlob, b"Version FS S3 conformance")
        .expect("test payload fits the canonical object format");

    let created = store
        .put(workspace, &encoded)
        .await
        .expect("S3 conditional create must succeed");
    assert_eq!(created.disposition, PutDisposition::Created);

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

    let page = store
        .inventory_page(workspace, None, 1)
        .await
        .expect("S3 listing must return the workspace-scoped object");
    assert!(page.entries.iter().any(|entry| {
        entry.object_id == created.object_ref.object_id
            && entry.object_type == created.object_ref.object_type
            && entry.observed_encoded_len == created.object_ref.encoded_len
    }));
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
