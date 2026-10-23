# S3 Object Store Conformance

The Version FS S3 conformance test is opt-in. Ordinary development and CI do
not contact AWS or an emulator. It can target either Amazon S3 or a local
RustFS instance. In both cases, use a dedicated, disposable bucket named
`versionfs-test-*`; the test rejects bucket names outside that pattern and
prefixes outside `versionfs-conformance/`.

The test writes one small object under a fresh workspace ID, verifies conditional
create and duplicate reuse, reads and verifies the complete object, and lists its
workspace-scoped inventory. The Version FS object API has no delete or overwrite
operation, so each test run leaves its object behind. Use a dedicated bucket that
can be discarded after conformance testing. Do not configure lifecycle expiration
for production Version FS prefixes.

## Amazon S3 setup

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

## Run against Amazon S3

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

## Local RustFS setup

RustFS provides an S3-compatible endpoint for local integration testing. See
the [official Docker guide](https://github.com/rustfs/docs.rustfs.com/blob/main/content/en/installation/container/docker.md)
and [S3 compatibility matrix](https://github.com/rustfs/rustfs/blob/main/docs/architecture/s3-compatibility-matrix.md).
This lane exercises the real Rust S3 HTTP client, including conditional create,
GET, and listing, without contacting AWS. It does not replace the Amazon S3
conformance run. Compatibility is feature-specific, so this test checks the
behavior Version FS needs.

The endpoint override is available only with the `s3-emulator` Cargo feature
and accepts only `http://127.0.0.1:<port>`. The normal S3 adapter remains
HTTPS-only. The local emulator path omits SSE request headers because RustFS
SSE-S3 requires KMS configuration; this lane does not qualify encryption.
Amazon S3 conformance continues to exercise the production encryption request.

The following uses Docker and AWS CLI (`aws`), with the
[RustFS 1.0.3 release](https://github.com/rustfs/rustfs/releases/tag/1.0.3).
Pinning the image makes local results reproducible; update the version
deliberately when qualifying a newer RustFS release.

```sh
set -euo pipefail

docker volume create rustfs-data
docker run -d \
  --name rustfs \
  -p 9000:9000 \
  -v rustfs-data:/data \
  -e RUSTFS_ACCESS_KEY=versionfs-local-access \
  -e RUSTFS_SECRET_KEY=versionfs-local-secret-change-me \
  -e RUSTFS_ADDRESS=:9000 \
  rustfs/rustfs:1.0.3 \
  /data

export AWS_ACCESS_KEY_ID=versionfs-local-access
export AWS_SECRET_ACCESS_KEY=versionfs-local-secret-change-me
export AWS_REGION=us-east-1
export VERSIONFS_S3_EMULATOR_ENDPOINT=http://127.0.0.1:9000
export VERSIONFS_S3_CONFORMANCE_BUCKET=versionfs-test-local
export VERSIONFS_S3_CONFORMANCE_PREFIX=versionfs-conformance/rustfs

ready=false
for attempt in $(seq 1 30); do
  if aws --endpoint-url "$VERSIONFS_S3_EMULATOR_ENDPOINT" \
    --region "$AWS_REGION" s3api list-buckets >/dev/null 2>&1; then
    ready=true
    break
  fi
  sleep 1
done
if [ "$ready" != true ]; then
  docker logs rustfs
  exit 1
fi

aws --endpoint-url "$VERSIONFS_S3_EMULATOR_ENDPOINT" \
  --region "$AWS_REGION" \
  s3api create-bucket --bucket "$VERSIONFS_S3_CONFORMANCE_BUCKET"

cargo test --locked -p versionfs-service \
  --features s3-emulator --test s3_conformance -- \
  --ignored --nocapture

docker rm -f rustfs
docker volume rm rustfs-data
```

Keep the endpoint bound to loopback and use throwaway credentials. The test
leaves its object in the bucket, so remove the disposable RustFS volume after
the run.
