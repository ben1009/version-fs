# Version FS MVP Implementation Plan

| Field | Value |
| --- | --- |
| Status | P0 merged; P1 object layer implemented, real-S3 run pending; P2 locally qualified |
| Created | 2026-10-23 |
| Design baseline | [RFC-001](../rfcs/001-version-fs.md) and the block-volume revision of [RFC-002](../rfcs/002-version-fs-mvp.md) |
| Target | Version FS v0 |

This plan implements the block-volume MVP from RFC-002. It supersedes the earlier
directory-scanning plan. RFC-001 owns system semantics; RFC-002 owns mechanisms,
durability boundaries, and release criteria. P0 is merged and P1 implementation is in progress. Writing this
plan or building a smoke harness does not establish remote durability or
benchmark evidence.

## 1. Release outcome

```text
worker A
  create fixed volume -> acquire writer -> attach ublk device
  explicitly format/mount ext4 -> files/Git + native logs + artifacts
  checkpoint G1 -> fork candidate -> modify -> checkpoint G2 -> promote
  destroy worker directories, dirty overlay, control records, and caches

worker B
  open retained G2 -> fork/attach a writable volume -> mount ext4
  lazily recover filesystem blocks; read native logs/artifacts by Generation
```

The authority has separate preserved storage. Also demonstrate that acknowledged
FLUSH/FUA boundaries newer than the last Generation survive worker loss via the
branch's live VolumeRoot. A raw block cut, a frozen or clean-unmounted filesystem
checkpoint, and an application-consistent checkpoint are distinct promises;
the MVP provides only the declared block/filesystem boundaries.

The scope is [RFC-002 Section 2](../rfcs/002-version-fs-mvp.md#2-release-scope).
Native logs and artifacts remain first-class views. Native file-tree capture,
pathname/filename diff, resize/discard, physical object deletion, PostgreSQL
implementation, process memory, database participants, and GTS remain deferred.

## 2. Initial code organization

Use a Cargo workspace with four initial packages; keep subsystems as modules
until a real dependency/build boundary justifies another package.

| Package | Responsibility |
| --- | --- |
| `versionfs-core` | Types/errors, formats, persistent maps/volume roots, backend-neutral control and native-view protocols. |
| `versionfs-service` | S3/toy-kv adapters, authority ownership, authorized service handlers, and `versionfsd`. |
| `versionfs-sdk` | Client transport, Linux worker block-server integration, dirty overlay, attachment records, heartbeats, and checkpoint/freeze coordinator. |
| `versionfs-cli` | Device and reference filesystem helpers, version/branch/log/artifact commands, JSON results, and explicit recovery actions. |

Keep these boundaries enforceable:

- ext4 implements filesystem operations; Version FS implements block ordering,
  immutable extent graphs, version identity, publication, and retention.
- Domain transactions depend on MetadataAuthority, not toy-kv timestamps/types
  or SQL constructs. Use point-key preconditions and snapshot-only prefix scans.
- The trusted builder validates bytes, ownership, typed graph edges, and durable
  outcomes. Client-provided digests/roots never prove publication eligibility.
- No S3 I/O, filesystem/device operation, or other external side effect runs
  inside the authority commit gate/transaction. Worker control/cache files live
  outside the exported volume.
- Block devices/handles and native views enforce workspace scope. No raw public
  digest lookup or filesystem UUID substitutes for authorization.

## 3. Delivery order and dependencies

| Task | Deliverable | Depends on |
| --- | --- | --- |
| P0 | Repository, formats, harness, and Linux platform feasibility | None |
| P1 | Immutable object layer and real-S3 conformance | P0 |
| P2 | MetadataAuthority contract and toy-kv qualification | P0 |
| P3 | Persistent extent maps, artifact indexes, and complete roots | P1 |
| P4 | ublk device, dirty overlay, verified reads, and range diff | P3 |
| P5 | Identified operations, grants, attachments, and protections | P2 |
| P6 | Durable volume barriers and live-root recovery | P1, P3, P4, P5 |
| P7 | Filesystem capture, Generation publication, and branches | P6 |
| P8 | Native logs/artifacts and common-cut integration | P3, P5, P7 |
| P9 | Complete service/SDK/CLI lifecycle | P4, P6, P7, P8 |
| P10 | Retention cleanup and mark-only reports | P5, P7, P8 |
| P11 | Integrated recovery/failure campaigns | P9, P10 |
| P12 | Full benchmarks and operational qualification | P11 |

Start P1/P2 independently after P0; P3/P4 and P5 can then proceed independently.
P0's small ublk/ext4 feasibility check must run before investing in the production
adapter. P2 qualifies the metadata contract before writable protocol integration.
Use fixture lease/outcome keys in P2; P5 supplies the full logical schema.

| Milestone | Tasks | Demonstrable result |
| --- | --- | --- |
| M1: Immutable state and feasibility | P0-P4 | Independent volume reads/sharing plus qualified metadata and ublk platform foundations. |
| M2: Writable end-to-end workflow | P5-P9 | Remote-durable barriers, frozen checkpoint/fork/promote, native views, and empty-worker restore through SDK/CLI. |
| M3: Qualified MVP | P10-P12 | Safe report/retention behavior, required fault evidence, real-backend baselines, and runbooks. |

Protections and fault cases ship with their state transitions. P11 integrates
those tests across processes; it is not the first durability or fencing test.

## 4. Task breakdown

### P0. Repository and platform foundation

- [x] Establish reproducible Rust workspace/toolchain/dependency lock, Linux CI,
  formatting/lint/package checks, and bounded shared golden fixtures.
- [x] Establish the opt-in real-S3 conformance lane with the object-store adapter
  in P1; do not run it against a production bucket.
- [x] Pin the Linux ublk userspace binding and add an ext4 tools/configuration
  plus a tiny READ/WRITE/FLUSH mount/freeze smoke harness.
- [x] On a Linux host, verify effective capabilities/privileges,
  device teardown on server death, and unsupported-operation behavior. NBD may
  accelerate exploration but is not the ublk release qualification.
- [x] Write the format note and golden vectors before durable writers: typed
  length-delimited encoding, zeros, extent/sector geometry, ordered keys, complete
  roots, all three capture modes, log segments, artifact chunks, and metadata
  schema versions.
- [x] Define typed identities/errors, injected time, named upload/commit/reply/
  device/freeze failure points, I/O/object counters, and bounded test fixtures.

**Exit evidence:** CI builds the workspace; format vectors are stable; and the
privileged RAM-backed ublk/ext4 smoke passed formatting, file readback after
unmount/remount, fresh sync-phase FLUSH delivery, freeze/thaw,
unsupported-discard handling, orderly teardown, and
server-death teardown on Linux 6.12.71-1-lts. The installed ublk module was
loaded for the run and unloaded afterward. This prototype does not qualify FUA
durability, remote storage, or Generation publication.

### P1. Immutable objects and S3

- [x] Implement scoped SHA-256 typed identities and complete length/hash/type
  verification; provide bounded PUT, verified GET, and inventory, with no DELETE API.
- [x] Implement conditional create and verified reuse. Reconcile ambiguous PUTs
  by verified GET or identical retry; object existence alone is insufficient.
- [x] Require HTTPS and server-side encryption, scope keys by deployment/workspace,
  bound transfer concurrency, expose no overwrite/delete API, and document the
  required no-delete IAM and no-expiration lifecycle policy.
- [x] Exercise duplicates, wrong/corrupt/missing objects, lost upload replies,
  and storage outages against a deterministic backend; add a guarded, opt-in real-S3
  conformance test.
- [ ] Configure the dedicated test identity without object deletion, ensure its
  prefix has no lifecycle expiration, then run the real-S3 conformance test against
  a dedicated `versionfs-test-*` bucket and retain request evidence before claiming
  external-backend qualification.

**Exit evidence:** deterministic tests verify conditional reuse, corrupt/missing
object rejection, ambiguous-upload recovery, bounded paging/transfers, and workspace
scoping. The ignored real-S3 test exercises the same API without running in ordinary
CI; an actual dedicated-bucket run remains pending. The public adapter has no DELETE
or overwrite method, and the real-S3 role must have no object-delete permission.

### P2. MetadataAuthority and toy-kv

- [x] Pin/review the engine revision and verify WAL, durable commit/reopen, errors,
  fresh reads, point transactions, and snapshot paging through one common contract.
- [x] Implement the single-owner authority commit gate around final transaction
  evaluation/commit resolution. Final lease validation defines the logical
  serialization point. A definite no-commit conflict may rerun the complete
  transaction body with fresh metadata and clock reads; unknown commit outcomes
  hold new mutations out until authoritative reconciliation.
- [x] Qualify atomic multi-key writes, absent/stale point conflicts, read-your-writes,
  read-after-commit, process-crash recovery, snapshot loss, and consistent paged scans.
- [x] Document the compatible PostgreSQL mapping without implementing it or adding
  Version FS lease hooks to the general-purpose KV transaction API.

**Exit evidence:** conformance passes against toy-kv-engine revision
`8b1355849bbc42dc0dea7d7bf8ca60097d42dbfe`. WAL-enabled, serializable point
transactions survive abrupt writer-process exit and reopen. Paged scans retain
one snapshot boundary; a live snapshot prevents close/reopen recovery from
silently changing that boundary. The authority gate serializes the full
single-owner mutation path, and injected lease-clock tests reject an expired
decision while preserving a commit whose reply is delayed past expiry. Unknown
commit outcomes block later writes until the operation result key resolves to
committed or not committed. PostgreSQL remains an interface mapping only.

### P3. Persistent volume roots and native indexes

- [ ] Implement canonical ordered B+ trees bounded to 64 KiB nodes, path copying,
  split/delete/iteration, stable unchanged identity, and sorted diff fallback.
- [ ] Implement fixed-capacity VolumeRoot geometry, 4 KiB sectors, immutable 64 KiB
  extents, implicit-zero holes, checked range arithmetic, and complete extent maps.
- [ ] Implement native log-map/segment indexes and artifact blob/chunk indexes.
  Artifact chunks remain distinct from volume extents in format and sizing.
- [ ] Build complete typed Generation descriptors and trusted bottom-up dependency
  proofs. Parent IDs are lineage, not required content edges.

**Exit evidence:** map behavior matches a simple model through splits/deletions;
no-change roots are shared, only affected paths/extents change, and fixtures
resolve with every ancestor descriptor read denied.

### P4. ublk adapter and local data path

- [ ] Implement a fixed-size single-queue device with truthful cache/FUA capability
  negotiation and checked READ/WRITE/FLUSH dispatch. Reject unsupported I/O/geometry.
- [ ] Implement bounded dirty sectors/extents, partial-extent read-modify-write,
  ordered barriers, and immutable sealed upload versions. Normal writes acknowledge
  only local volatile state until P6 provides persistence.
- [ ] Implement verified workspace-scoped cache fills, atomic installation, fill
  coordination, clean eviction, and dirty/control-state separation.
- [ ] Implement protected-root range readers and extent-range diff. Never infer
  zeros for an allocated missing/corrupt extent or label range diff as file diff.

**Exit evidence:** device contents match a byte-array model; dirty tracking needs
no file scan; cold reads fetch only required index/extent working sets. Extent
upload buffers cannot change beneath their hashed version. Do not mount a writable
filesystem claiming reliable barriers until P6 is qualified.

### P5. Control schema and attachment authority

- [ ] Implement operation fingerprints, durable terminal results, query/abort
  tombstones, fixed identities, branch-name indexes, and exact revision/epoch CAS.
- [ ] Implement writer/attachment grants, owner incarnation restart fencing, and
  one writable attachment per branch. Device numbers/filesystem UUIDs are not IDs.
- [ ] Implement retention eligibility guards, pins, read/live-log protections,
  attachment bases, and shared preparation/renewal state from the start.
- [ ] Add checkpoint capture-token admission: block other head/live mutations,
  allow only identified drain-flush children until sealing, and retain independent
  renew/query/abort paths. Definite expiry/fencing resolves token cleanup safely.

Use the RFC's lease/renewal defaults centrally. Every new heartbeat has an operation
ID; replay returns its old deadline. Keep terminal fingerprints/results for the
workspace lifetime and surface capacity exhaustion rather than forgetting IDs.

**Exit evidence:** duplicate creates/acquisitions/renewals return original results;
changed payloads reject replay; absent-key name claims yield one binding; delayed
requests lose to abort tombstones; stale grants/devices cannot mutate authority.

### P6. Durable barriers and live volume recovery

- [ ] Create the zero-volume workspace/initial Generation after its immutable graph
  is durable, then attach under an exact grant and protected base.
- [ ] Record internal barrier ID/boundary/sealed dirty state outside the volume.
  Upload extents/map paths under protected preparation, then atomically CAS the
  live root/revision, receipt, and preparation outcome.
- [ ] Acknowledge FLUSH/FUA only after S3 and authority durability. Honor preflush
  dependency ordering and native FUA or the qualified kernel emulation path.
  Persist the prefix through and including the tagged payload before FUA completion;
  combined preflush/data/FUA persists earlier writes before payload execution.
- [ ] Block later writes through the initial persistence cut; reconcile unknown
  replies with the same ID and preserve dirty bytes/protection. Renew long barriers.
- [ ] Restore the current live root on a new attachment after worker loss, including
  acknowledged barriers beyond the last Generation. Stop fenced/failed old devices.

**Exit evidence:** worker destruction loses only permitted volatile writes;
acknowledged FLUSH/FUA data, including the tagged payload itself, returns from an
empty cache. Combined preflush/data/FUA verifies both persistence boundaries;
ignoring flags or returning success after local I/O cannot pass.

### P7. Checkpoint consistency and branch lifecycle

P7 establishes volume-backed Generation publication and the common-cut framework.
Until P8, `logs_root` and `artifacts_root` reference canonical empty root objects
using P3's formats and normal durability/graph validation, not placeholder digests.
The descriptor and capture protocol already include native-view slots; P8 supplies
their mutation protocols and connects them to this same coordinator.

- [ ] Implement `block_cut`, `filesystem_frozen`, and `filesystem_clean_unmount`
  boundaries, persisting only the mode actually established and rejecting unknown modes.
- [ ] Close local admission, read authoritative revision/live roots together,
  then drain producers and acquire the capture token against those exact pre-drain
  preconditions. Keep the worker's original writer grant, attachment identity,
  and fencing epochs; never adopt a new owner.
  A definite conflict restarts capture with a new operation ID and a fresh drain;
  never refresh roots and reuse the old cut. Reconcile unknown acquisition first.
- [ ] Keep block service alive during freeze or clean unmount; drain permitted
  child barriers, then stop block mutation admission and seal the volume root,
  canonical empty native roots, and final revision through the common-cut framework.
- [ ] Publish attached/detached Generations with retention/outcome/capture-token
  changes atomically. Reconcile/sync the local result before thaw/reopening.
- [ ] Implement metadata-only fork, conditional adoption/rollback/promotion/delete,
  stable identity/revision CAS, volume/stream/writer fencing, and explicit live-state
  discard. Never swap a backing root beneath a mounted filesystem.
- [ ] Coordinate watchdog/restart cleanup of freeze intent outside the mounted
  volume. An aborted capture does not undo a previously acknowledged child flush.

**Exit evidence:** failed freeze/CAS/descriptor upload never publishes a false
checkpoint. Any live mutation between the pre-drain root read and token CAS forces
a fresh capture/drain, including a legitimate drain-time volume barrier. Never
retry by refreshing only the revision/roots. Ownership changes fence the original
worker. All three capture modes have distinct descriptor and recovery tests.
Fork does not walk volume extents; journal replay uses a writable fork;
name reuse and old-device commits cannot affect the new branch state.

### P8. Native logs and artifacts

- [ ] Implement stream identities, consecutive sequences, half-open ranges, one
  sealed batch segment, exact append receipts, conditional tails, and explicit attachments.
- [ ] Reuse preparation/renewal/fencing for append/create/trim/delete. Upload before
  atomic live-root replacement; matching append retries allocate no second range.
- [ ] Implement batch-boundary trim, protected historical reads, polling tails,
  explicit replaced/expired cursors, and independent append after fork/reset.
- [ ] Implement gated named-artifact staging with blob/4 MiB chunk representation.
  Connect native admission/drain/seal hooks to P7's common-cut coordinator and
  replace its empty native roots with captured log/artifact roots. Do not introduce
  a second checkpoint-coordination protocol, parse ext4, or recopy historical segments.

**Exit evidence:** native views can be read without mounting the filesystem;
append races fall wholly on one side of capture; retained log ranges/attachments
survive live trim. An append completing after the pre-drain snapshot invalidates
that attempt and forces a fresh capture/drain under P7's existing protocol.
Native artifact identity is explicit, not inferred from a path.

### P9. SDK, service, and CLI lifecycle

- [ ] Complete typed, authorized, bounded streaming requests and structured errors
  from RFC-002 Section 12. Keep arbitrary digest access private.
- [ ] Complete attachment/read-handle renew/close, pending barrier/capture recovery,
  and freeze-independent heartbeats/outcome queries. Read deadlines honor protection.
- [ ] Compose device attach/detach, explicit ext4 format/mount/unmount helpers,
  checkpoint modes, fork/promote/rollback, native logs/artifacts, pins, diff, and
  operation status/abort. Add report commands when P10 is available.
- [ ] Distinguish local WRITE, durable FLUSH/FUA, durable append, and published
  Generation acknowledgements. Document OS permissions, external dependencies,
  and the absence of application/database/process-memory capture guarantees.

**Exit evidence:** the full two-worker workflow runs through the CLI and separate
service/block-server processes, with clean-cache lazy attach and no hidden dependency
on the old worker. Formatting never happens implicitly during restore.

### P10. Retention and mark-only reports

- [ ] Resolve expired preparations/attachments and capture tokens atomically against
  final commits. Bytes physically present do not revive expired Generation eligibility.
- [ ] Copy/seal retained and live volume/log/artifact roots from one read-only
  authority snapshot; release the snapshot before object I/O.
- [ ] Traverse typed strong edges and uncertain preparations using bounded RAM/disk
  spill; inventory S3 over a recorded window and persist bounded candidate pages.
- [ ] Make corruption, missing graph metadata, snapshot loss, budget exhaustion,
  and detail expiry explicit. Candidates never authorize deletion; report work
  never holds publication admission or the authority gate through object I/O.

**Exit evidence:** races with publication/pins/readers and interrupted reports
produce honest boundaries/completeness without an outage or S3 DELETE. Include
live roots newer than Generation heads and historical attachment/log dependencies.

### P11. Integrated recovery and fault campaigns

- [ ] Track every RFC-002 Section 16.2 case with exact operation IDs, injected
  boundaries, API/kernel outcomes, persisted graph/authority state, and root protection.
- [ ] Kill worker/block server/coordinator during dirty writes, barriers, freeze,
  capture sealing, commit, reply, local baseline sync, and thaw. Reattach safely;
  no unqualified transparent ublk request recovery or mounted-root replacement.
- [ ] Restart the authority from preserved storage around commit; fence old
  incarnations, resolve outcomes without circular dependence on another mutation,
  and never reconstruct publication from uploaded descriptors.
- [ ] Exercise slow preparations/renewals, handover, abort/delayed delivery,
  append/capture, report/read races, corruption, and all resource/backend outages.

**Exit evidence:** API/kernel acknowledgements match persisted state for every
required case. An old immutable Generation never absorbs journal-replay writes;
no failure weakens ordering, durability, retention, or authorization.

### P12. Benchmarks and operator handoff

- [ ] Run all real ublk/ext4/S3/toy-kv scenarios from RFC-002 Section 14.3,
  including sparse/populated capacity, random/sequential I/O, the million-file
  filesystem fixture, 100 GiB content, barriers, history/fan-out, and native logs.
- [ ] Measure application/device/dirty/uploaded bytes separately, filesystem journal
  amplification, map object counts, partial-extent cold reads, barrier latency,
  freeze/upload/publish/thaw duration, cache working set, and authority serialization.
- [ ] Publish exact revisions/kernel/binding/mkfs/mount/WAL settings, budgets,
  hardware/region, fixture commands, raw samples, errors, distributions, and first
  numerical baseline. No invented SLO or unqualified cross-filesystem claims.
- [ ] Publish runbooks for device privilege/lifecycle, failed I/O/freeze recovery,
  retained versus live-root restore, persistent metadata, old-backup offline recovery,
  no-delete/lifecycle settings, report limits, and cumulative metadata/S3 growth.
- [ ] Assemble invariant/acceptance evidence and a reproducible two-worker transcript.

**Exit evidence:** every required gate has evidence and a new operator can repeat
the supported demo/restart without undocumented state. Deferred native filesystem,
PostgreSQL, physical reclamation, and runtime/GTS integrations are not blockers.

## 5. Validation and review cadence

| Tier | When | Evidence |
| --- | --- | --- |
| Local/ordinary CI | Every relevant implementation PR | Golden formats, byte/map models, deterministic authority/device/capture faults, and small fixtures. |
| Privileged Linux and real backend integration | Adapter/protocol changes and scheduled runs | Qualified ublk/ext4 ordering/freeze, real toy-kv process reopen, S3 conformance, and small two-worker workflow. |
| Release qualification | Before declaring v0 complete | Every RFC acceptance/fault case, full benchmark matrix, invariant evidence, and runbooks. |

Emulators cannot replace real backend evidence; ordinary unprivileged CI cannot
prove ublk or freeze behavior. Publish explicit prerequisites and account for every
release case rather than turning unavailable prerequisites into successful tests.

## 6. Acceptance coverage

Use [RFC-002 Section 16](../rfcs/002-version-fs-mvp.md#16-acceptance-criteria)
as the checklist; list each fault campaign individually in the evidence index.

| RFC-002 functional gate | Owning tasks |
| --- | --- |
| 1: Reference filesystem/Git plus native state | P0, P4, P6, P8, P9 |
| 2: Capture modes and publication | P5, P7, P11 |
| 3: Independent lazy restore and safe journal replay | P3, P4, P7, P11 |
| 4: Dirty tracking/sharing/cheap fork and amplification | P3, P4, P7, P12 |
| 5: History-independent reads and extent diff | P3, P4, P12 |
| 6: Branch lifecycle, ABA, and mounted-device fencing | P5, P7, P11 |
| 7: Native log/attachment semantics | P5, P8, P11 |
| 8: Workspace/device/object isolation | P1, P5, P9 |
| 9: Reports and no physical deletion | P1, P5, P10, P11 |
| 10: Durable block barriers and supported I/O | P0, P4, P6, P11 |
| 11: Authority conformance and lease decisions | P2, P5, P11 |
| 12: Backend-neutral protocol and future PG mapping | P2, P5, P12 |

## 7. First implementation PRs

1. Workspace/toolchain/CI, identities/errors, injected clocks, and Linux platform spike.
2. V0 format note/golden vectors before the first persistent writer.
3. MetadataAuthority conformance and pinned toy-kv adapter/commit gate.
4. Immutable objects and verified conditional S3 reuse.
5. Persistent maps, complete VolumeRoot, zero extents, and immutable range reads.
6. Bounded ublk dirty overlay and ordering, then grants/attachments plus remote
   FLUSH/FUA persistence with worker-loss tests.
7. Frozen/raw checkpoint publication and fork/reset, followed by native-view integration.

Later PRs follow P8-P12 and the full acceptance map. A milestone demonstration
cannot substitute for the remaining required MVP capabilities or evidence.
