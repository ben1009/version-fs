# Real S3 Conformance

The Version FS S3 test is opt-in. Ordinary development and CI do not contact
AWS. Run it only against a dedicated, disposable bucket named `versionfs-test-*`;
the test rejects bucket names outside that pattern and prefixes outside
`versionfs-conformance/`.

The test writes one small object under a fresh workspace ID, verifies conditional
create and duplicate reuse, reads and verifies the complete object, and lists its
workspace-scoped inventory. The Version FS object API has no delete or overwrite
operation, so each test run leaves its object behind. Use a dedicated bucket that
can be discarded after conformance testing. Do not configure lifecycle expiration
for production Version FS prefixes.

## Required AWS setup

Configure a dedicated test bucket with TLS-only access and default or requested
server-side encryption. The default adapter requests SSE-S3; use the Rust
`S3Encryption::SseKms` option when the bucket requires a specific KMS key. Give
the test identity only these capabilities, scoped to the conformance prefix:

- `s3:PutObject` and `s3:GetObject` on
  `arn:aws:s3:::<bucket>/versionfs-conformance/*`.
- `s3:ListBucket` on `arn:aws:s3:::<bucket>` with the
  `s3:prefix` condition restricted to `versionfs-conformance/*`.
- No `s3:DeleteObject`, `s3:DeleteObjectVersion`, or lifecycle-expiration
  permission for the test prefix.

Use the normal AWS credential chain (for example, an isolated AWS profile or a
short-lived role). Never put credentials in repository files, test arguments, or
CI logs.

## Run

```sh
export VERSIONFS_S3_CONFORMANCE_BUCKET=versionfs-test-yourname
export VERSIONFS_S3_CONFORMANCE_PREFIX=versionfs-conformance/manual-run
export AWS_PROFILE=versionfs-conformance
export AWS_REGION=us-east-1
cargo test --locked -p versionfs-service --test s3_conformance -- --ignored --nocapture
```

The test is ignored by default. Its explicit environment checks and bucket-name
guard are required even when running it from a private workstation. Standard
workspace tests use the deterministic in-memory backend and never read AWS
credentials.

To verify the no-delete deployment boundary, inspect the test role policy and
enable S3 data-event logging for the dedicated test bucket. The service-side
object API has no delete operation, and the runtime role must not be able to
delete objects even if another code path is added later.
