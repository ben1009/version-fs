# Rust development

The tooling baseline comes from
[`toy-kv-engine` at `8b1355849bbc42dc0dea7d7bf8ca60097d42dbfe`](https://github.com/ben1009/toy-kv-engine/tree/8b1355849bbc42dc0dea7d7bf8ca60097d42dbfe).
The MVP Metadata Authority adapter pins this same engine revision as a Git
dependency so local and CI qualification use the reviewed transaction, WAL, and
snapshot APIs.
This repository contains four Rust packages matching the
[MVP implementation plan](mvp-implementation-plan.md#2-initial-code-organization).
P0 added canonical-format primitives, bounded shared fixtures, and an opt-in
RAM-backed ublk/ext4 feasibility harness. P1 adds the workspace-scoped S3 object
adapter and deterministic conformance tests. The real-S3 test is explicitly
ignored and requires a dedicated test bucket. P2 adds the toy-kv Metadata
Authority adapter and its conformance tests. P3 adds persistent copy-on-write
maps, typed volume/log/artifact roots, and complete Generation graph verification;
P4 adds the bounded local block overlay, verified workspace object cache,
protected-root range reads, structural extent diff, and the opt-in single-queue
ublk adapter. P5 adds backend-neutral control records and toy-kv-backed
integration tests for operation outcomes, grants, attachments, protections, and
capture preparation. P6 adds a durable barrier coordinator, local recovery
journal, and S3-backed live-root path; this implementation has not yet been
qualified against real S3, ublk, or ext4. `DirtyVolume` remains volatile, while
the generic ublk adapter derives FLUSH/FUA capability from its supplied
`BlockDevice`. The service and SDK lifecycle that uses these protocols remains
in progress.

Workspace cache directories have one owner at a time; drop the prior cache
handle before reopening its directory. An exclusive file lock also prevents
another process from rebuilding or filling that same cache concurrently.
Cache handles identify both their workspace and object-store scope. A cached
map store requires its cache scope to match the backing store's `proof_scope`;
reopening a directory for another scope removes the previous scope's clean
objects before serving reads.
If temporary-file cleanup fails, its capacity stays reserved until reopening
removes the leftover file. Verified backing reads remain available in the meantime.

Validated view handles seal without reading their map nodes. Imported volume
handles can retain the result of `VolumeExtentMap::validate` to check metadata
once before later mutations; named-map opens validate entries and retain counts.
Tooling checks do not qualify any MVP durability gate.

Durable barriers retain the validated candidate map returned by staging, so
retries and completion reuse its metadata validation. Recovery validates the
imported base once and reconstructs the candidate from journaled dirty extents;
it checks that the reconstructed root matches the saved root before publishing.
A different authoritative base root requires fresh metadata validation.

## Setup and tools

Install Rust through rustup. `rust-toolchain.toml` selects the dated nightly and
the rustfmt, Clippy, LLVM tools, and Rust source components. CI reads this file
through `.github/actions/setup-rust`, so ordinary jobs share the local toolchain.
Linux is the reference environment for the planned ublk integration and the
sanitizer jobs.

The ublk binding is pinned to the published `libublk` `0.4.8` crate. Upstream
`0.5.0` is not yet published in the registry. The `ublk-smoke` feature builds
only the disposable P0 platform prototype; `ublk-adapter` builds the single-
queue P4 transport, which can accept a P6 durable block device but is not yet
qualified for durable FLUSH/FUA behavior.

`./dev` runs from the repository root even when invoked from another directory.
It installs cargo-make if missing, then forwards arguments to cargo-make. Individual
tasks install or update their Cargo-managed tools using the versions in
`Makefile.toml`; CI installs the same versions explicitly. Compare `--version`
output with this table when reproducing CI, especially for tools installed
outside Cargo's installation registry.

| Tool | Baseline |
| --- | --- |
| Rust | `nightly-2026-09-23` |
| cargo-make | `0.37.24` |
| cargo-nextest | `0.9.145` |
| cargo-llvm-cov | `0.9.0` |
| cargo-machete | `0.9.2` |
| cargo-sort | `2.1.4` |
| typos-cli | `1.50.1` |

Install a tool at its baseline with `cargo install <tool> --version <version>
--locked`; `cargo binstall` is an optional faster alternative. The wrapper does
not download or execute an unpinned bootstrap script.

## Local commands

| Command | Purpose |
| --- | --- |
| `./dev check` | Formatting, dependency order/use, default/all-feature Clippy, tests, doc tests, documentation, spelling. |
| `./dev build` | Locked workspace build with all features and targets. |
| `./dev fmt` | Apply rustfmt. |
| `./dev sort-deps` | Apply dependency sorting. |
| `./dev test` | Run library, binary, and integration tests with nextest. |
| `./dev test-all-targets` | Also execute benchmark targets using the serialized benchmark group. |
| `./dev test-doc` | Run doc tests separately; nextest does not execute them. |
| `./dev test-cov` | Generate HTML coverage under `target/llvm-cov/html` when runnable tests exist. |
| `./dev test-asan` | Run AddressSanitizer tests on Linux x86-64. |
| `./dev test-lsan` | Run LeakSanitizer tests with the shared suppression file. |
| `cargo test -p versionfs-sdk --features ublk-adapter` | Test the opt-in Linux ublk adapter and local block path. |
| `cargo build -p versionfs-sdk --features ublk-smoke --bin versionfs-ublk-smoke` | Build the opt-in ublk RAM target. |
| `sudo ./scripts/ublk-ext4-smoke.sh` | Format/mount ext4, exercise file I/O and flush/freeze, check unsupported discard, and verify orderly and abrupt-server teardown. |
| `docs/s3-conformance.md` | Dedicated-bucket setup and command for the ignored real-S3 conformance test. |

The ublk smoke requires Linux with `CONFIG_BLK_DEV_UBLK`, `/dev/ublk-control`,
effective `CAP_SYS_ADMIN` (or equivalent device permissions), mount namespaces,
`mkfs.ext4`, `mount`, `umount`, `mountpoint`, `fsfreeze`, and `blkdiscard`. The harness runs the ublk server in a
separate mount namespace from the ext4 consumer to avoid teardown/unmount
self-deadlock. It requires a new FLUSH during sync and verifies file contents
after unmount/remount. Set `VERSIONFS_SMOKE_KEEP=1` to retain the request log. If
cleanup cannot confirm an unmount, it exits with an error and preserves the
mount, server, and logs for recovery; the diagnostic reports their paths and PID.
Unfreeze/unmount that temporary filesystem before stopping its server and
removing the work directory. This volatile RAM device verifies request plumbing,
ext4 flush progress, freeze progress, and kernel device teardown only; the
supplied `DirtyVolume` advertises a volatile cache, so the kernel may emulate
FUA as WRITE followed by FLUSH. The harness does not qualify FUA durability,
S3 durability, Generation publication, or recovery of durable state after server
death. Run it only on a disposable privileged Linux host.

The deterministic object-store tests run with `./dev test`. The real-S3 test is
ignored unless explicitly requested; its dedicated-bucket guard and setup are in
[`docs/s3-conformance.md`](s3-conformance.md). Ordinary CI never contacts AWS.

Checks do not rewrite Rust sources or manifests. Formatting and dependency fixes
have separate commands; spelling failures propagate as failed checks. Build and
test commands use the checked-in lockfile. Add workspace dependencies when an
implementation actually needs them, rather than importing the KV engine's library
dependencies.

Empty test suites currently print a nextest warning instead of failing workspace
bootstrapping. Remove `--no-tests=warn` from each suite when it gains its first
tests. Coverage inspects the test inventory and explicitly skips reporting when
there are no runnable tests; it does not manufacture a coverage percentage.

`.config/nextest.toml` preserves the upstream timeouts, retry/backoff settings,
coverage profile, and serialized benchmark profile. `failpoint_` and `chaos_`
tests have no retries. Name tests that arm process-global failpoints with
`failpoint_`: nextest isolates these tests in separate processes, while the
in-process sanitizer runs skip them.

## Continuous integration

All external actions retain upstream commit-SHA pins. Workflows harden runners,
avoid persisting checkout credentials, and use read-only permissions unless a
specific reporting job needs more.
The tools job builds the pinned `cargo-sort` version from registry source because
its prebuilt release archive fails its published checksum. Other tool downloads
retain checksum verification.

| Workflow | Coverage |
| --- | --- |
| `check.yml` | Formatting, default/all-feature Clippy, dependency sorting/use, spelling, documentation. |
| `test.yml` | Linux unit and binary/integration suites, doc tests, LLVM coverage. |
| `safety.yml` | AddressSanitizer and LeakSanitizer with optimization configured through environment variables. |
| `scheduled.yml` | Rolling nightly with locked dependencies and beta with refreshed dependencies; also runs daily. |
| `dependency-review.yml` | Dependency vulnerability review on PRs. |
| `scorecards.yml` | OpenSSF Scorecard on the default branch and weekly. |

`check`, `test`, and `safety` also support merge queues and manual runs. Dependabot
checks both Cargo and GitHub Actions daily. Patch and minor Cargo updates remain
enabled because the planned workspace includes hosted services and commands.

Coverage is retained as an Actions artifact whenever a report exists. Configure
the repository's `CODECOV_TOKEN` secret to enable Codecov uploads; without it,
local reporting and artifact storage still work. `codecov.yml` consolidates the
upstream coverage policies into one file with workspace-relative exclusions.
Scorecard's code-scanning upload and dependency review require the corresponding
GitHub repository features to be available.

## Adaptations from toy-kv-engine

- Workspace metadata and CI targets refer to Version FS's four packages. Crates
  inherit common metadata and documentation/unsafe-block lints; publishing is
  disabled until a release policy exists.
- Cargo network settings and rustfmt options are retained. Toolchain settings
  use the conventional `rust-toolchain.toml` filename with explicit components.
- Unit/integration selection uses target kinds instead of KV test names. No
  compaction smoke test, `write-perf` profiling job, engine chaos binary, KV
  benchmark, or engine-specific nextest override is claimed to exist here. Add
  Version FS failure campaigns with the protocols they validate.
- The `cfg_aliases` vendored patch belongs to the KV engine's current dependency
  graph. It is not needed by these dependency-free skeletons and is not copied.
  Re-evaluate it when introducing dependencies that require it.
- The upstream leak suppression is retained. Inactive Miri/loom examples are not
  copied; the upstream reason for disabling Miri concerns engine dependencies
  that this repository has not selected.
- Ignore rules cover Rust output, profiles, and local settings; engine database
  directories and duplicate ignore entries are omitted.

The real-S3 conformance lane is introduced with the S3 adapter in P1. It will use
a dedicated test bucket/prefix and explicit credentials; ordinary CI and local
checks never contact AWS. Do not point future conformance runs at a production
bucket.
