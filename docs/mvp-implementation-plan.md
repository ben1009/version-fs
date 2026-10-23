# Version FS MVP Implementation Plan

| Field | Value |
| --- | --- |
| Status | Proposed; implementation has not started |
| Created | 2026-10-23 |
| Design baseline | [RFC-001](../rfcs/001-version-fs.md) and [RFC-002](../rfcs/002-version-fs-mvp.md), through commit `2465d97` |
| Target | Version FS v0 |

This plan turns RFC-002 into ordered implementation tasks, reviewable changes,
and evidence for completion. RFC-001 defines the system semantics; RFC-002
defines the MVP contract. A task that discovers an incompatible requirement
must resolve it in the RFC before relying on different behavior.

All tasks below are initially unchecked. Completing this document does not
mean an implementation, test campaign, or benchmark has been completed.

## 1. Release outcome

The release must demonstrate this workflow with real S3 and the toy-kv metadata
adapter:

```text
worker A
  create workspace -> import local directory -> checkpoint G1
  fork candidates -> edit files + append logs + stage artifacts
  checkpoint candidate G2 -> conditionally promote G2
  destroy all worker A directories, local records, and caches

worker B
  authenticate -> open retained G2 with an empty cache
  read paths/ranges lazily or explicitly materialize the full namespace
  recover files, captured .git, ordered logs, and artifact bytes
```

The metadata owner uses a separate preserved volume. Restart it against that
volume as part of qualification. Worker-loss recovery must not depend on the
destroyed worker, ancestor descriptors, or replay of checkpoint deltas.

The delivery scope is exactly [RFC-002 Section 2](../rfcs/002-version-fs-mvp.md#2-release-scope).
It includes retention and mark-only reachability reports. Physical object
deletion, a PostgreSQL implementation, FUSE, a Git server, process memory,
application database snapshots, and GTS/global checkpoints remain deferred.

## 2. Initial code organization

Create one Cargo workspace with these packages. Keep subsystems as modules
until an actual dependency or build boundary justifies extracting another crate.

| Package | Responsibility |
| --- | --- |
| `versionfs-core` | Typed identities and errors, canonical formats, immutable graph/tree operations, storage contracts, and backend-independent control protocols. |
| `versionfs-service` | S3 and toy-kv adapters, authentication/authorization, request handlers, and the `versionfsd` executable. |
| `versionfs-sdk` | Client transport, worker-local control records, managed directory handles, mutation admission, heartbeats, and capture/export coordination. |
| `versionfs-cli` | The `versionfs` executable, command composition, stable machine-readable results, and explicit recovery/salvage actions. |

Core modules should cover `format`, `objects`, `trees`, `content`, `authority`,
`operations`, `protections`, `generations`, `branches`, `logs`, `artifacts`,
`reads`, and `reports`. Share immutable fixture data under `fixtures/`.

Keep the following boundaries enforceable from the first implementation:

- Domain transactions use `MetadataAuthority` and point keys; toy-kv engine
  types and commit timestamps stay in the service adapter.
- Object access is workspace scoped. The public API resolves authorized paths,
  log views, or artifact edges rather than exposing arbitrary digest reads.
- The trusted service builder validates object bytes, types, dependencies, and
  durable results. A worker-provided hash or root alone cannot authorize publication.
- The SDK owns worker-local paths. Import does not cause the service to open a
  caller-supplied path on its own host.
- File scans, object transfer, command execution, and local filesystem changes
  happen outside metadata transactions and the authority commit gate.

## 3. Delivery order and dependencies

Task IDs are implementation work packages, each small enough to split into
focused PRs. Dependencies identify required foundations; they are not permission
to postpone a task's own failure cases until the final integration stage.

| Task | Deliverable | Depends on |
| --- | --- | --- |
| P0 | Repository, format decisions, and test foundations | None |
| P1 | Immutable object layer and S3 conformance | P0 |
| P2 | MetadataAuthority contract and toy-kv feasibility | P0 |
| P3 | Persistent maps, chunks, and complete descriptors | P1 |
| P4 | Immutable reads, cache, scanner, export, and diff | P3 |
| P5 | Control schema, operation outcomes, grants, and protections | P2 |
| P6 | Workspace creation, import, and checkpoint publication | P1, P3, P4, P5 |
| P7 | Fork, head movement, rollback, promotion, and deletion | P6 |
| P8 | Durable logs, artifact attachments, and checkpoint integration | P3, P5, P6, P7 |
| P9 | Service transport, complete SDK lifecycle, and CLI workflow | P4, P6, P7, P8 |
| P10 | Retention cleanup and mark-only reachability reports | P5, P7, P8 |
| P11 | Worker/authority recovery and integrated fault campaigns | P9, P10 |
| P12 | Real-backend qualification, benchmarks, and operator handoff | P11 |

Start P1 and P2 independently after P0. P2 is an early feasibility gate: do not
finish a writable control plane around an unverified metadata backend. P3/P4
and P5 can then proceed independently before joining at P6. P9 completes a
transport and SDK skeleton introduced in P0/P6; it does not introduce the first
working capture gate or protection renewal.

The milestones correspond to [RFC-002 Section 15](../rfcs/002-version-fs-mvp.md#15-implementation-sequence):

| Milestone | Tasks | Demonstrable result |
| --- | --- | --- |
| M1: Immutable state and authority feasibility | P0-P4 | Read/export a complete fixture without ancestors; prove sharing and the metadata adapter contract separately. |
| M2: Writable end-to-end workflow | P5-P9 | Use the SDK/CLI to import, checkpoint, fork, append, promote, and restore with durable identified outcomes. |
| M3: Qualified MVP | P10-P12 | Recover from required faults, report reachability without deletion, and publish the required real-backend evidence. |

## 4. Task breakdown

### P0. Repository and test foundation

- [ ] Establish the Rust workspace, reproducible toolchain/dependency lock,
  formatting/lint checks, and package test commands. Add Linux CI and a separate
  opt-in real-S3 profile whose credentials are supplied by the environment.
- [ ] Write the v0 format note before implementing persistent writers. Settle
  integer/length encoding, canonical ordering, type tags, empty objects,
  path keys, descriptor edges, segment encoding, and metadata key/value versions.
  Add golden byte/digest vectors for every persistent object type.
- [ ] Define typed workspace/branch/Generation/operation/stream identities and
  the RFC's definite rejection, conflict, and unknown-outcome errors. Keep
  Generation identity independent of engine timestamps.
- [ ] Establish injectable clocks and failure points around uploads, final
  lease decisions, backend commit, reply delivery, and local control installation.
  Record object accesses, transferred bytes, and authority outcomes in fixtures.
- [ ] Define a transport skeleton for typed requests, bounded streaming data,
  authorization context, and structured errors. Keep the SDK usable through an
  in-process client during early integration.

**Exit evidence:** the empty workspace builds in CI; format vectors are stable;
a fixture can deterministically pause at a named failure boundary and inspect
its recorded outcomes. No current benchmark or durability claim is inferred
from these test doubles.

### P1. Immutable objects and S3 adapter

- [ ] Implement typed SHA-256 identities and length/type/version verification.
  Provide bounded object PUT, verified GET, and prefix inventory operations.
  The v0 object-store interface exposes no physical-delete operation.
- [ ] Implement conditional create. On a key collision, verify complete existing
  bytes before reuse; reconcile an ambiguous PUT by verified GET or retrying
  identical bytes. An upload result never publishes a Generation.
- [ ] Configure workspace prefixes, TLS, encryption at rest, transfer limits,
  and scoped credentials. The release profile has no object-delete permission
  and no lifecycle expiration for the data prefix.
- [ ] Run the same object contract against a deterministic development store
  and real S3. Include corrupt/missing objects, a wrong object at an expected
  key, duplicate upload, lost PUT reply, and unavailable storage.

**Exit evidence:** real S3 accepts/reuses identical verified objects and rejects
bad bytes; request traces show no DELETE. The configured bounded object sizes
work without multipart uploads.

### P2. MetadataAuthority and toy-kv feasibility

- [ ] Pin and record a reviewed toy-kv revision; verify the build, WAL settings,
  durable commit/reopen path, transaction error mapping, and snapshot lifetime.
  Inspect the revision cited by RFC-002 as a starting point, then qualify the
  actual pinned revision with executable conformance cases.
- [ ] Implement `get`, point-only `transact`, read-only `snapshot`, and paged
  `scan_prefix` behind the common contract. Keep engine types and timestamps
  private to `ToyKvMetadataAuthority`.
- [ ] Implement the single-owner authority commit gate from RFC-002 Section 5.1.
  The final transaction body runs under the gate and makes a fresh final lease
  check at its logical serialization point immediately before backend commit.
  A definite conflict reevaluates the body and clock; an unknown commit outcome
  keeps new mutations out until reconciliation. This needs no Version FS hook
  in the general-purpose toy-kv transaction API.
- [ ] Build reusable conformance fixtures for atomic multi-key writes, present
  and absent point-key conflicts, read-your-writes, fresh reads after commit,
  durable restart, explicit uncertain outcomes, and consistent paged snapshots.
  Snapshot loss must fail rather than changing the scan boundary.
- [ ] Document how a later PostgreSQL adapter would satisfy the same contract,
  logical keys, retries, and outcomes. Do not implement that adapter in this task.

Use minimal fixture lease and outcome keys for these adapter tests. P5 supplies
the full domain operation/grant schema, so authority qualification does not depend
on first implementing publication or the complete control plane.

**Exit evidence:** the toy-kv adapter passes conformance on preserved storage,
including process termination around durable commit. Demonstrate expiry before
the final lease decision rejects all writes, and delayed durability acknowledgement
after a valid decision preserves success. If a required backend guarantee cannot
be established, resolve that integration gap before starting P5.

### P3. Persistent maps and complete state

- [ ] Implement canonical ordered B+ tree nodes bounded by encoded size at
  64 KiB, path copying, splits, deletion, lookup, and sorted/prefix iteration.
  Cover changed split boundaries and unchanged subtree identity.
- [ ] Implement blobs up to 64 KiB, fixed 4 MiB file chunks above that threshold,
  persistent chunk indexes, logical lengths, and a defined empty file. Reuse
  unaffected chunks and metadata paths.
- [ ] Instantiate namespace, log-map, segment-index, and artifact-map schemas.
  Namespace roots and empty directories are explicit entries.
- [ ] Implement complete Generation descriptors and typed graph traversal.
  Parent IDs record lineage and are excluded from required state edges.
- [ ] Build new object graphs bottom-up from verified new objects or protected,
  previously validated roots. Keep that provenance available to the publication
  builder; arbitrary client references cannot become trusted edges.

**Exit evidence:** tree behavior matches a simple ordered-map model through
insert/delete/split sequences. A no-change build preserves state roots; a small
overwrite changes only intersecting chunks and affected tree paths. A fixture
resolves from its own descriptor with all ancestor descriptor access denied.

### P4. Reads, cache, directory capture, and export

- [ ] Implement internal descriptor/path/stat/list/range readers. Fetch and
  verify complete intersecting chunks before returning slices. Make access
  counters prove that one path read avoids unrelated file payloads.
- [ ] Add a workspace-scoped verified disk cache: temporary downloads, atomic
  installation, coordinated fills, bounded capacity, and clean-object eviction.
  Keep dirty local files and unresolved capture records outside this cache.
- [ ] Implement the Linux scanner with full content hashing and the RFC's path,
  file-type, metadata, and hard-link restrictions. Store symlink target text
  without following it. Full scan cost remains visible in measurements.
- [ ] Implement export to a fresh staging directory using directory-relative
  operations without following links. Apply supported metadata, record successful
  completion, and only then expose the completed destination.
- [ ] Capture self-contained Git repositories and reject alternates/external
  worktree administration. Implement structural diff with equal-subtree skipping
  and sorted comparison when tree boundaries differ.

**Exit evidence:** an internal immutable fixture round-trips regular files, empty
directories, permissions/mtime, symlinks, large files, and `.git`; unsupported
state is rejected. Corruption never produces a successful partial export or
older file contents. Diff matches a complete comparison after splits, renames,
deletions, and metadata changes. Public reads are exposed only with P5 protections.

### P5. Control records, identified operations, and protection

- [ ] Implement the versioned logical records and point indexes from RFC-002
  Section 5.2. Enforce workspace/caller scope, stable IDs, branch-name uniqueness,
  revisions, retention eligibility, and authority incarnation in transactions.
- [ ] Implement fingerprints and `PREPARING -> COMMITTED/ABORTED` outcomes,
  immutable terminal results, query, and abort tombstones. Matching terminal
  replay precedes new-precondition validation and grants no renewed rights.
  Preserve results for the workspace lifetime.
- [ ] Implement writer acquisition, renewal, release, handover, and restart
  fencing with injected authority time. Increment writer epochs and advance a
  durable owner incarnation before serving after restart.
- [ ] Implement pins, read/live-log protections, managed-workspace base
  protections, and the common preparation lifecycle. Retention eligibility and
  protection acquisition are atomic point transactions from the start.
- [ ] Reuse one preparation mechanism for checkpoint and log create/append/trim/
  delete. Allocate renewal IDs before sending them; reconcile unknown renewals
  without extending a lease twice or releasing uncertain dependencies.

Use the RFC defaults centrally: writer lease 30 seconds / renewal 10 seconds;
base protection five minutes / renewal one minute; renewable preparation ten
minutes / renewal three minutes; Generation read protection five minutes; initial
Generation retention 24 hours. Expired rights require a new eligible operation.

**Exit evidence:** duplicate create/acquire/renew requests return their original
results; changed payloads reject ID reuse; absent-key name claims yield one
binding. A delayed request cannot execute after an abort tombstone. Restart and
handover reject prior grants. Uncertain protection outcomes remain protected.

### P6. First writable checkpoint workflow

- [ ] Implement `create_workspace` after the empty graph is durable, publishing
  G0, initial retention, main branch, and operation result atomically.
- [ ] Implement SDK directory import and `ManagedWorkspaceHandle`. Persist its
  operation and worker/directory binding outside the captured directory; reconcile
  remote base protection with atomic local installation. Import publishes no files.
- [ ] Implement the managed mutation gate and runner quiescence contract. Drain
  admitted writers, descendants, and appends before selecting the revision/log
  root. Reject unsupported background writers and preserve local files on failure.
- [ ] Connect scanner/artifact input to the trusted service graph builder.
  Register protection, upload durable objects, seal one immutable root/descriptor,
  and publish through one final authority transaction.
- [ ] Implement attached and detached publication. Attached success updates the
  head/revision and base protection together, then syncs the SDK's new local base
  before reopening its gate. Detached success retains the Generation independently
  without moving the head or changing the managed baseline.
- [ ] Run writer/base/preparation renewal during long captures. Unknown renewal
  or publication results keep admission closed and pause further scan/uploads
  until the original operation is resolved or definitively aborted.

**Exit evidence:** `create -> acquire -> import -> checkpoint -> open/export`
runs through the toy-kv adapter and real S3. Descriptor-only upload and failed
publication CAS create no visible Generation. Kill the service or lose the reply
at every publication boundary; recover exactly the recorded result even after
later head movement. Surviving local work is available for explicit salvage.

### P7. Branch lifecycle

- [ ] Implement create/fork from a retained Generation using bounded reference
  and protection writes, including a new stable branch identity and no writer grant.
- [ ] Implement adoption/rollback and promotion with stable source/destination
  identities and revision CAS. Promotion selects the published source head.
- [ ] On reset, replace the live log map, advance branch/stream/writer epochs,
  and revoke the old writer. Require explicit `discard_live_logs` when needed.
- [ ] Implement branch deletion and name reuse without reusing IDs or allowing
  a replayed old deletion to affect a replacement branch.

**Exit evidence:** fork traverses no namespace, segments, or per-log history.
Race destination changes, head-away/head-back, source revision changes, name reuse,
and pending old-writer commits. Reset never rewrites a running worker directory;
the SDK reports the need for explicit salvage or rematerialization.

### P8. Ordered logs and artifacts

- [ ] Implement stable log IDs, `(branch_id, stream_epoch, log_id)` identities,
  consecutive `u64` sequences, exact half-open ranges, and persistent indexes.
- [ ] Implement one sealed segment per accepted batch, with 1000-record/1 MiB
  initial limits, overflow/empty-batch rejection, and explicit chunked attachments.
  Share P5's preparation, renewal, fencing, and outcome machinery.
- [ ] Upload the batch, attachments, and index paths before the single conditional
  live-root replacement. Atomically record the exact append receipt/revision;
  retrying an append ID returns that receipt without allocating another range.
- [ ] Implement log create/delete, whole-batch-boundary trim, protected historical
  reads, and polling live tails. Return `StreamReplaced` or `CursorExpired`
  explicitly; recreate a deleted log with a new identity.
- [ ] Implement gated named-artifact staging and descriptor edges. Bind the
  existing immutable log-map root during checkpoint without re-uploading history.
  Exercise fork/reset/promotion against nonempty live and published log views.

**Exit evidence:** append batches are atomic and durable; same-ID replay cannot
append twice. An append racing capture falls entirely on one side of the cut.
Forks share history and append independently; trimming live state preserves pinned
historical ranges and their attachment bytes. Slow uploads, abandoned preparations,
renewal loss, and fencing never produce a false receipt or root update.

### P9. Service, SDK lifecycle, and CLI

- [ ] Complete the typed service/client surface in RFC-002 Section 12 using the
  P0 transport. Enforce authorization for reads, mutations, object reuse, cache
  scope, and report access. Stream payloads and result pages within resource limits.
- [ ] Finish SDK admission and renewal scheduling. Renewal/query/abort work must
  remain available while a checkpoint closes the local gate or a branch preparation
  holds mutation admission. Reconcile a renewal ID before allocating another.
- [ ] Implement protected handle renewal/close, read-deadline admission, managed
  base renewal/close, and worker-local pending-operation reconciliation. Never
  infer success or permission to discard local bytes from the current head alone.
- [ ] Compose CLI commands for import/materialize/run/checkpoint, branch lifecycle,
  logs, artifacts, pins, reads/diff, and operation status/abort. Add report commands
  when P10 is ready. Output stable IDs/revisions and structured errors as JSON.
- [ ] Document and expose the three acknowledgements separately: file write is
  local; append is a durable log range; checkpoint is a durable coherent Generation.
  The managed runner coordinates trusted commands under the worker's OS rights.

**Exit evidence:** the complete candidate/fork/promote/empty-worker restore demo
runs from the CLI against a separate service process. Unauthorized known hashes
give no access. Large streaming requests stay within configured memory/queue
limits, and timeouts retain the original operation ID and uncertain status.

### P10. Retention and mark-only reporting

- [ ] Complete logical retention expiry and protection cleanup, including abandoned
  managed handles and every preparation kind. Serialize expiry/abort against final
  publication so a committed result stays committed and an aborted one cannot publish.
- [ ] Snapshot retained/protected roots and preparation dependencies into durable
  report pages; seal one boundary and release the read-only snapshot before S3 I/O.
  Snapshot loss before sealing aborts that attempt; a new boundary uses a new ID.
- [ ] Traverse typed strong edges, including descriptors, live log roots, chunks,
  segment indexes, Git files, artifacts, and attachments. Use a bounded visited set
  with disk spill and explicit uncertainty for incomplete/unknown preparations.
- [ ] Compare the marked set with an S3 inventory window. Persist bounded candidate
  pages, counts/bytes, completeness, and detail expiry. Missing/corrupt required
  graph metadata makes a report incomplete; expired details return `ReportExpired`.
- [ ] Expose `collect`/report retrieval in the CLI. Keep report work independent
  of mutation admission; reports and candidates authorize no object deletion.

**Exit evidence:** concurrently publish, append, trim, pin/unpin, and open readers
while reporting. Interrupt reports or exhaust their budget without a workspace
outage or misleading complete result. Confirm all retained closures are marked
and no code path issues an S3 DELETE. Expired Generation bytes cannot be reopened
just because storage still contains them; old operation results remain queryable.

### P11. Integrated recovery and failure campaigns

- [ ] Turn the failure cases in RFC-002 Section 16.2 into a tracked campaign matrix.
  Each case records its injected boundary, request/operation IDs, process history,
  expected authority result, persisted roots, and object/retention observations.
- [ ] Destroy worker state after publication and restore each retained fixture on
  a separate empty worker. Deny ancestor descriptor reads in the fixture and verify
  no dependency on ancestry or the original worker remains.
- [ ] Restart the authority from its preserved volume around commit/reply boundaries;
  verify acknowledged results, incarnation fencing, preparation resolution, and
  protection cleanup. Never rebuild the catalog from descriptor listings.
- [ ] Run delayed request, duplicate delivery, lost reply, conflict retry, abort,
  writer handover, heartbeat expiry, append/capture, reader/report, corruption,
  and exhausted-resource campaigns across the integrated modules.
- [ ] Verify unknown outcomes from the authority commit gate can be reconciled
  using authoritative reads or controlled restart without admitting a conflicting
  fresh mutation or depending on another mutation to release the gate.

**Exit evidence:** API outcomes and persisted state agree for every required
campaign. S3/authority outages, full disks/queues, and corrupt graphs never weaken
the promised acknowledgement or discard unresolved protection/local state.

### P12. Qualification and operational handoff

- [ ] Run the full real-S3/toy-kv matrix from RFC-002 Section 14.3, including the
  million-file namespace, 100 GiB file, version-depth and branch-fan-out cases,
  log batching, cold/warm recovery, report races, and lifetime operation metadata.
  Run these as separate experiments rather than one combined Cartesian product.
- [ ] Publish a reproducible baseline with exact service/engine revisions,
  durability settings, hardware/region, fixtures, commands, raw samples, errors,
  latency distributions, transferred/stored bytes, object counts, and resource use.
  Measure full scan work separately from incremental storage and adapter commit
  serialization separately from branch conflicts. Do not invent latency SLOs.
- [ ] Publish operator and developer instructions for persistent metadata volumes,
  ordinary restarts, unknown outcomes, worker salvage, trusted imports/runner use,
  credential scope, no-delete/lifecycle settings, and incomplete/expired reports.
- [ ] Document permanent operation-result growth and unreclaimed S3 bytes,
  capacity rejection, and the metadata-backup boundary. Restoring an older metadata
  backup keeps the authority offline pending a separate reconciliation procedure.
- [ ] Assemble an invariant/acceptance evidence index and the end-to-end demo
  transcript. Record outstanding failures as blockers rather than skipped passes.

**Exit evidence:** every RFC-002 required gate has a reproducible result, the
benchmark baseline and runbooks are published, and a new operator can run the
supported demo and restart procedure without undocumented worker state.

## 5. Validation and review cadence

Each implementation PR includes the tests for its new state transitions and
failure behavior. Fault hooks and access counters land with those transitions;
P11 combines and extends them across processes.

| Validation tier | When | Evidence |
| --- | --- | --- |
| Local and ordinary CI | Every relevant implementation PR | Format vectors, model comparisons, deterministic protocol faults, package checks, and bounded small fixtures. |
| Preserved-storage and real-backend integration | Adapter/protocol changes and scheduled runs | Real toy-kv reopen/process-failure checks, S3 object conformance, and a small complete two-worker workflow. |
| Full release qualification | Before declaring v0 complete | All RFC acceptance/failure cases, real-backend benchmark matrix, invariant evidence, and operator procedures. |

Development emulators and in-memory doubles support iteration. Their successful
runs do not replace WAL/restart evidence or the real S3 release profile. Benchmark
scale is selected explicitly per validation tier; the release report must account
for every RFC-required scenario.

## 6. Acceptance coverage

Use [RFC-002 Section 16](../rfcs/002-version-fs-mvp.md#16-acceptance-criteria)
as the release checklist. This mapping assigns ownership of its functional gates.
The evidence index also lists every Section 16.2 fault case individually.

| RFC-002 functional gate | Owning tasks |
| --- | --- |
| 1: Initial import, filesystem/Git state, artifacts, and logs | P4, P6, P8, P9 |
| 2: Attached/detached publication and invisible failed preparation | P5, P6, P11 |
| 3: Empty-worker and ancestor-independent restore | P3, P4, P9, P11 |
| 4: Shared roots/chunks and cheap fork | P3, P6, P7, P12 |
| 5: History-independent reads and correct structural diff | P3, P4, P12 |
| 6: Conditional branch lifecycle, ABA, and live-log reset policy | P5, P7, P8, P11 |
| 7: Append ranges, stream identities, cursors, trim, and attachments | P5, P8, P11 |
| 8: Workspace authorization and digest isolation | P1, P5, P9 |
| 9: Root closure reports, report races, and no physical deletion | P1, P5, P10, P11 |
| 10: Symlink capture/restore and runner trust boundary | P4, P6, P9 |
| 11: Authority conformance and point-key name uniqueness | P2, P5, P11 |
| 12: Backend-neutral protocol and compatible PostgreSQL mapping | P2, P5, P12 |

## 7. First implementation PRs

Begin with these review units, then split P3-P12 at the task boundaries above:

1. Repository/toolchain/CI skeleton, typed identities/errors, clocks, and shared
   fixture conventions from P0.
2. The v0 format note and golden vectors, before any durable object writer.
3. MetadataAuthority conformance harness and pinned toy-kv feasibility adapter,
   including commit-gate lease decisions and restart/unknown-outcome cases.
4. Immutable object layer and S3 contract, including verified conditional reuse.
5. Persistent ordered maps, then chunk/file representations and complete descriptors.
6. Immutable fixture reads/export/diff and the first authoritative workspace/import/
   checkpoint slice, with protection and crash tests in their respective PRs.

A completed milestone is an integration checkpoint, not permission to omit later
required capabilities. The MVP is ready only after M3 closes the evidence for
the entire RFC-002 scope.
