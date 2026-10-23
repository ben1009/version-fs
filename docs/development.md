# Rust development

The tooling baseline comes from
[`toy-kv-engine` at `8b1355849bbc42dc0dea7d7bf8ca60097d42dbfe`](https://github.com/ben1009/toy-kv-engine/tree/8b1355849bbc42dc0dea7d7bf8ca60097d42dbfe).
This repository contains four Rust packages matching the
[MVP implementation plan](mvp-implementation-plan.md#2-initial-code-organization).
P0 adds canonical-format primitives, bounded shared fixtures, and an opt-in
RAM-backed ublk/ext4 feasibility harness. The storage protocols, remote object
adapter, service daemon, and user-facing commands are not implemented yet.
Tooling checks do not qualify any MVP storage or durability gate.

## Setup and tools

Install Rust through rustup. `rust-toolchain.toml` selects the dated nightly and
the rustfmt, Clippy, LLVM tools, and Rust source components. CI reads this file
through `.github/actions/setup-rust`, so ordinary jobs share the local toolchain.
Linux is the reference environment for the planned ublk integration and the
sanitizer jobs.

The P0 ublk binding is pinned to the published `libublk` `0.4.8` crate. Upstream
`0.5.0` is not yet published in the registry. Its optional feature is excluded
from normal SDK users: `ublk-smoke` is only a disposable platform prototype.

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
| `cargo build -p versionfs-sdk --features ublk-smoke --bin versionfs-ublk-smoke` | Build the opt-in ublk RAM target. |
| `sudo ./scripts/ublk-ext4-smoke.sh` | Format/mount ext4, exercise file I/O and flush/freeze, check unsupported discard, and verify orderly and abrupt-server teardown. |

The ublk smoke requires Linux with `CONFIG_BLK_DEV_UBLK`, `/dev/ublk-control`,
effective `CAP_SYS_ADMIN` (or equivalent device permissions), mount namespaces,
`mkfs.ext4`, `mount`, `umount`, `mountpoint`, `fsfreeze`, and `blkdiscard`. The harness runs the ublk server in a
separate mount namespace from the ext4 consumer to avoid teardown/unmount
self-deadlock. It requires a new FLUSH during sync and verifies file contents
after unmount/remount. Set `VERSIONFS_SMOKE_KEEP=1` to retain the request log. If
cleanup cannot confirm an unmount, it exits with an error and preserves the
mount, server, and logs for recovery; the diagnostic reports their paths and PID.
Unfreeze/unmount that temporary filesystem before stopping its server and
removing the work directory. This
volatile RAM device verifies request plumbing, ext4 flush progress, freeze
progress, and kernel device teardown only; `libublk` advertises a volatile cache,
so the kernel may emulate FUA as WRITE followed by FLUSH. The harness does not
qualify FUA durability, S3 durability, Generation publication, or recovery of
durable state after server death. Run it only on a disposable privileged Linux
host.

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
