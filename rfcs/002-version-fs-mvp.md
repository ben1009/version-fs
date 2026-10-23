# RFC-002: Version FS MVP

| Field | Value |
| --- | --- |
| Status | Draft |
| Created | 2026-10-23 |
| Target | Version FS v0 |
| Depends on | [RFC-001: Version FS Architecture](001-version-fs.md) |
| Scope | First implementation, storage formats, protocols, recovery, and release criteria |

## 1. Summary

The MVP proves that a workspace can checkpoint, fork, roll back, and resume on
another worker without copying its complete state for every version. It
implements the Generation semantics and invariants of RFC-001 using:

- Immutable, checksummed objects in Amazon S3 Standard.
- A versioned block-volume view with a persistent copy-on-write extent map.
- A transactional Metadata Authority for publication, references, fencing, and
  retention, with toy-kv-engine as the first backend.
- One active writer per branch.
- A native SDK and CLI, with a Linux ublk block server and ext4 as the reference
  filesystem consumer.
- Native ordered logs using S2 as the semantic reference.
- Explicit retention, pins, reader protection, and mark-only reachability reports.

A successful demonstration is:

```text
worker A: attach volume -> format/mount ext4 -> write + append -> checkpoint G1
                                                               |
                                      fork candidate branches --+
                                                |
                                       checkpoint candidate G2
                                                |
                                      conditionally promote G2
                                                |
worker B: empty cache -> attach G2 on a fork -> mount -> files/logs/artifacts
```

Worker A can be destroyed after successful publication. Worker B needs neither
its disk nor any ancestor Generation to resolve G2.

The block volume is one first-class Version FS view alongside native ordered
logs and artifacts. Version FS remains a Generation-based state system. The
block-volume interface is filesystem-agnostic: ext4 is the reference consumer,
not a required internal format or the definition of Version FS correctness.
Other filesystems or block-aware engines may consume this interface after
qualification. PostgreSQL, SQLite, and RocksDB ordinarily access it through a
filesystem; this RFC provides no raw-block database adapter.

This revision replaces the directory-scanning filesystem adapter with a block
adapter. Native namespace capture, pathname APIs, and filename-level diff are
deferred; the first implementation observes changed block ranges directly.

This RFC selects mechanisms for v0. RFC-001 remains the authority for system
semantics; implementation shortcuts cannot weaken its invariants. Numerical
defaults below are initial Version FS choices, not claims about vendor limits
or achieved performance.

## 2. Release scope

### 2.1 Required capabilities

| Area | MVP behavior |
| --- | --- |
| Objects | Typed SHA-256 identity, verified reads, idempotent uploads, workspace-local sharing. |
| Metadata | Persistent volume-extent map, artifact-chunk indexes, log indexes, and root maps. |
| Authority | Backend-independent transactional contract, implemented by `ToyKvMetadataAuthority` for the MVP. |
| Generations | Complete immutable roots; branch-attached and detached publication; operation outcome lookup. |
| Block volume | One fixed-capacity volume per workspace, sparse zero extents, range reads, dirty-extent tracking, and fenced writable attachment. |
| Filesystem | Linux ublk device with ext4 reference formatting/mounting; filesystem operations and metadata are implemented by Linux. |
| Branches | Create/fork, checkpoint, conditional head movement, rollback, promote, delete, writer fencing. |
| Logs | Atomic durable batch append, conditional tail, receipts, historical reads, live cursors, retention-safe trim. |
| Artifacts | Named immutable payloads and explicit log attachments, sharing the object layer. |
| Git | Preserve self-contained worktree and `.git` bytes inside the volume; no Git protocol or object translation. |
| Recovery | Fresh-worker volume attachment and lazy extent reads; interrupted operations resolved from durable authority. |
| Retention | Recent-Generation retention, explicit pins, protected workspace bases and readers. |
| GC report | Snapshot retained/protected roots, traverse reachability, and report observed unreachable objects without deleting bytes. |
| Tooling | SDK and CLI for the workflow in Section 1; structural extent-range, log-view, and artifact diff. |

### 2.2 Deferred capabilities

The MVP does not implement a native file-tree scanner/materializer, pathname
read/list/stat APIs, semantic filesystem diff, FUSE, virtio-fs, a Git server or smart protocol,
semantic merge, concurrent writers on one branch, physical object deletion,
online reclamation, cross-region writable branches, compression, content-defined
chunking, volume resize, multi-volume atomic checkpoints, discard/TRIM,
automatic autosave, or a remote mutation WAL for unflushed block writes.
The v0 object store only grows. Safe reclamation and deletion fencing belong
in a follow-up RFC, tentatively RFC-003: Safe Object Reclamation.

Process-memory snapshots, application database snapshots, GTS adapters, and
coordinated global checkpoints remain the extension in RFC-001 Section 18.
The MVP neither labels a Generation with a fabricated global timestamp nor
claims a timestamp alone makes file and database state consistent. Its branch
IDs are not a substitute for coordinated `history_id` identities.

Automatic metadata failover and restoration from an older metadata backup are
outside the initial service. Their safety requirements are described where they
affect normal operation. A PostgreSQL adapter is a compatible later backend;
implementing it and operating a replicated metadata service are not MVP gates.

## 3. Components and deployment

```text
                     SDK / CLI / checkpoint coordinator
                                  |
              ext4 consumer -> /dev/ublkbN -> block server
                                  |
                       dirty extents + verified cache
                                  |
                     Version FS service and builders
                       /                       \
                  ObjectStore          MetadataAuthority
                       |                       |
                  S3 Standard          +-------+----------------+
                data + manifests       |                        |
                              ToyKvMetadataAuthority   PgMetadataAuthority
                                   MVP / local          later / hosted
```

The implementation uses Rust for the core library, service, SDK, and CLI.
The domain protocol depends on `ObjectStore` and `MetadataAuthority` interfaces.
One deployment initially owns one metadata namespace and an S3 prefix. Independent
branches can prepare concurrently; bulk transfer is separate from authority
execution. The single-owner MVP adapter may serialize the short metadata commit
region with an authority-level gate, but does not hold it during preparation or
object transfer. The domain protocol does not require one Version FS lock across
preparation. Scaling the API tier and partitioning authority are later work.

The first block adapter uses Linux
[ublk](https://docs.kernel.org/block/ublk.html). Pin and qualify the kernel,
userspace binding, queue parameters, and filesystem tools before release.
NBD may be used for an exploratory prototype, but does not replace the ublk
release gate. Kernel mounts and device administration require an appropriately
configured worker; the block server is not a sandbox or process-memory runtime.

[Fly.io's Sprites Block Device design](https://fly.io/blog/sprites-block-device/)
is a reference for placing ext4 above an object-backed block device. The article
describes NBD, local caching, checksummed remote blocks, and volume checkpoints.
Version FS chooses its own ublk adapter and publication protocol and retains
native log/artifact views; it does not inherit Fly's implementation or performance
guarantees.

### 3.1 MVP metadata backend

`ToyKvMetadataAuthority` adapts
[toy-kv-engine](https://github.com/ben1009/toy-kv-engine) for local development,
integration and fault-injection tests, and the first end-to-end implementation.
It uses serializable OCC point transactions with WAL enabled and a verified
durable commit path, plus read-only snapshots for prefix scans. Section 5.1
defines the adapter's required semantics and release evidence.

The reference deployment has one authority owner and a persistent metadata
volume. Its contents are required durable state, not worker cache. Recovery
tests restart the owner against that volume and replace workspace workers
independently. An optional in-memory test double cannot claim crash durability.

The toy-kv backend is a test/reference implementation. Selecting it does not
promise production availability, replication, multi-node operation, or automatic
failover. Implementing Raft or a new replicated metadata service is outside v0.
Its internal MVCC timestamps are not Generation identities or GTS coordinates.
Using the engine for control metadata does not add application database
snapshots to the MVP.

### 3.2 Compatible PostgreSQL backend

`PgMetadataAuthority` is specified as an interface-compatible future adapter;
its implementation is not required for the initial MVP. It maps the same
logical key/value records and transactions to PostgreSQL
[SERIALIZABLE transactions](https://www.postgresql.org/docs/current/transaction-iso.html),
consistent read-only snapshots, and
[durable commit settings](https://www.postgresql.org/docs/current/runtime-config-wal.html).
Serialization retries and commit uncertainty map to the common authority errors.
SQL locking, constraints, and schema features stay inside that adapter.

This provides a production path using an existing managed or cloud PostgreSQL
deployment, so Version FS operators can reuse hosted metadata infrastructure
before a dedicated replicated authority is justified. The selected deployment
must satisfy the same read, durability, recovery, and fencing contract, including
preserving acknowledged commits through any configured failover. Choosing a
provider alone does not establish those guarantees.

### 3.3 Durability and object storage

The MVP durability boundary survives worker loss and authority-process restart
while its persistent metadata storage remains intact. Keep that storage
independent of disposable workspace disks in the recovery demonstration. Loss
of the only metadata volume is outside this reference backend's guarantee.
The authority and immutable object graph are both necessary for recovery.

S3 is accessed through an object-store interface, but real AWS S3 is the release
backend. An emulator or another S3-compatible service is a development backend
until its behavior passes the same conformance checks. AWS documents strong
consistency for object PUT, GET, and listing in its
[consistency model](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html#ConsistencyModel).
This does not create a transaction spanning S3 and the Metadata Authority.

The deployment initially has one trusted operator and workspace-scoped client
credentials. Clients cannot directly overwrite or delete stored objects. No v0
component, including the report worker, has S3 object-delete permission, and
lifecycle expiration is disabled for the Version FS data prefix. Cross-tenant
deduplication and an identity provider integration are deferred; workspace
authorization is still enforced.

## 4. Immutable formats

### 4.1 Identity and object-store keys

```text
object_id = sha256(format_version || object_type || canonical_payload)

<deployment>/workspaces/<workspace_id>/objects/<type>/<digest>
```

The encoded fields are length-delimited; concatenation must be unambiguous.
Canonical binary encoding fixes integer representation, sorted map order, and
the absence of duplicate keys. Every reference declares its object type,
digest, and encoded length. The format has a version number; unknown versions
are rejected. A format note and golden encoding vectors are required before
implementing the first persistent writer.

Objects are not compressed or packed in v0. S3 encryption at rest and TLS are
required. The digest is computed over the Version FS encoding before S3-managed
encryption and is independent of an S3 ETag.

Uploads use `PutObject` with `If-None-Match: *`; S3 documents the relevant
[conditional-write behavior](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html).
A precondition failure means another object occupies the key. The trusted
object layer verifies its complete bytes and identity before accepting reuse;
an existence response alone is insufficient. An ambiguous upload is resolved
by a verified GET or a retry of the identical bytes. It is never treated as
publication.

All individual objects are bounded by the sizes below, so multipart upload is
unnecessary for v0. Large artifacts use the artifact-chunk representation. Object
listing is useful for inventory and GC, never for discovering published
Generations.

### 4.2 Persistent maps

The MVP uses a persistent ordered B+ tree with nodes of at most 64 KiB. Mutations
copy affected leaves and ancestors; insertions split by encoded size. Unchanged
nodes keep their identities. Rebuilding the full volume map per flush or
checkpoint is prohibited.

| Map | Key | Value |
| --- | --- | --- |
| Volume extents | Logical extent number | Immutable extent reference; an absent entry means zeros. |
| Artifact chunks | Chunk number | Chunk reference and actual length. |
| Logs | Log name | Stable log ID, visible sequence range, and segment-index root. |
| Segment index | Batch starting sequence | Exclusive end and sealed-segment reference. |
| Artifacts | Artifact name | Blob/chunk representation and declared dependencies. |

Diff skips equal identities and compares differing ranges; sorted merge handles
changed tree boundaries. Its volume results are changed logical byte ranges,
not file paths. Native log ranges and named-artifact changes remain inspectable
without interpreting a filesystem image.

### 4.3 Volume representation

v0 has one fixed-capacity volume per workspace, shared geometrically by its
branches. Capacity is selected at creation, is a positive multiple of 64 KiB,
and cannot change in v0. The initial logical sector is 4 KiB; storage extents
are 64 KiB. These are Version FS choices, not ext4 or ublk universal limits.

```text
VolumeRoot {
    format_version,
    capacity_bytes,
    logical_sector_bytes: 4096,
    extent_bytes: 65536,
    extent_map_root,
}
```

Each present extent references exactly 64 KiB of immutable data. Unmapped
extents read as zeros; bytes beyond capacity are rejected. A partial-extent
write reads or reuses the previous verified extent, changes the requested
sectors, and creates a new extent at persistence. Fully zero extents may be
represented by absence. Sharing is by immutable identity, not parent replay.
Filesystem free space does not imply unmapped extents. With discard disabled,
unlinking a file does not reclaim its old blocks or S3 objects; Version FS does
not infer allocation state by parsing the filesystem.

Dirty-sector/extent tracking comes from admitted block writes. No directory
scan, inode discovery, or filesystem parse is needed to discover the storage
delta. A 4 KiB write can still upload 64 KiB plus changed map nodes; filesystem
journals can dirty additional extents. Report application bytes, device bytes,
dirty extents, and uploaded bytes separately.

### 4.4 Native artifact contents

Artifacts remain native objects, independent of the volume's on-disk format.
Payloads up to 64 KiB use one blob; larger payloads use fixed 4 MiB chunks and
a persistent chunk index. Logical length bounds reads. Verify every intersecting
complete object before returning a range. An empty artifact has a defined empty
blob. Log attachments are explicit typed edges, not hashes inside opaque text.

### 4.5 Complete Generation descriptor

```text
GenerationDescriptor {
    format_version,
    generation_id,
    parent_generation_id?,   // lineage, not a required object edge
    volume_root,
    logs_root,
    artifacts_root,
    capture: { mode, volume_boundary_id },
    created_at,              // informational, not a global coordinate
}
```

Generation IDs are opaque UUIDs allocated once per checkpoint. The descriptor
is immutable and checksummed. Its volume, log, and artifact roots resolve
complete declared state without an ancestor descriptor or replay journal.
Filesystem and Git bytes reside inside the volume; no native namespace/Git
index is inferred. Capture mode records the actual protocol used in Section 6.

The authority catalog binds a Generation ID to its descriptor and publication
result. Uploading a descriptor creates no visible Generation. A live durable
volume root is also not automatically a Generation or a coordinated log/artifact
checkpoint.

## 5. Control metadata and operation identity

### 5.1 MetadataAuthority contract

Version FS requires a strongly consistent transactional Metadata Authority.
The interface describes key/value semantics rather than a SQL abstraction.
Conceptually, without fixing a final Rust ABI:

```rust
trait MetadataAuthority {
    fn get(&self, key: Key) -> Result<Option<Value>>;
    fn transact<R>(
        &self,
        body: impl FnMut(&mut dyn MetadataTxn) -> Result<R>,
    ) -> Result<R>;
    fn snapshot(&self) -> Result<SnapshotHandle>;
    fn scan_prefix(
        &self,
        snapshot: &SnapshotHandle,
        prefix: Key,
        cursor: Option<Cursor>,
        limit: usize,
    ) -> Result<Page>;
}

trait MetadataTxn {
    fn get(&mut self, key: Key) -> Result<Option<Value>>;
    fn put(&mut self, key: Key, value: Value) -> Result<()>;
    fn delete(&mut self, key: Key) -> Result<()>;
}
```

| Property | Required semantics |
| --- | --- |
| Atomic multi-key commit | The complete write set commits together, including reference, Generation, retention, and operation-outcome records. No partial commit is visible. |
| Serializable point transactions | Point reads, including missing/deleted keys and read-your-writes, participate in conflict validation. Concurrent read/write preconditions cannot both succeed when no serial execution permits them. |
| Commit-linearized lease checks | Every write authorized by a lease revalidates that lease against the authority owner's clock at the transaction serialization/commit decision. An earlier closure read alone cannot authorize the commit; an expired lease rejects the complete write set. |
| Fresh authoritative reads | New gets, transactions, and snapshot acquisition observe commits completed before they begin. An explicitly held snapshot retains its earlier boundary. Stale caches/replicas are not authoritative reads. |
| Durable commit and recovery | Success waits for the selected backend's durable commit boundary. Reopening its preserved storage recovers every acknowledged complete transaction. Memory-only visibility is insufficient. |
| Consistent prefix scans | Every page uses one read-only snapshot, with bytewise ordered keys and an explicit cursor/end. Losing that snapshot is an error, never a silent switch to current state. |
| Explicit outcomes | A conflict/rejection is known not to commit. A timeout or error after possible commit is an unknown outcome, resolved through the operation record before another logical mutation. |

`transact` may rerun its closure on a definite conflict. The closure performs
metadata reads and buffered mutations, and MAY read the authority owner's
monotonic lease clock. The clock is a trusted local input, not an external side
effect. Lease-dependent preconditions MUST use a fresh clock reading on every
transaction attempt, including retries; a value captured before `transact` or
by an earlier attempt MUST NOT authorize a new mutation. At its serialization/
commit decision, `transact` MUST revalidate each lease-dependent precondition
using a fresh authority-clock reading. The lease check and commit decision form
one logical linearization point: if the lease is expired there, the complete
transaction is rejected; a later durability acknowledgement does not invalidate
a transaction that linearized while the lease was valid. The closure performs
no S3 I/O, command execution, local file mutation, or allocation of a new
logical operation ID on retry. Unknown commit outcomes cannot be retried as
fresh operations.
Deleting a metadata key is unrelated to physical object deletion; v0 still
issues no S3 DELETE.

For the single-owner MVP, the adapter MAY implement this commit-time validation
with an authority-level mutation commit gate; no `MetadataTxn` precommit hook is
required. Every mutating transaction MUST acquire the same gate before its
transaction body begins and retain it through backend commit resolution. Inside
the gated attempt, after reading lease metadata and preparing the buffered write
set, the domain closure takes a fresh clock reading and performs the final lease
checks. This check is the logical serialization point; the adapter submits the
backend commit immediately afterward and prevents another authoritative mutation
from passing its serialization point in between. A definite conflict retries
the complete transaction body and lease check under the gate. If the backend
outcome is unknown, the gate MUST remain closed to new mutations until operation
record reconciliation establishes whether the commit happened. Object uploads
and other preparation remain outside the gate. A multi-owner authority MUST use
an equivalent shared or backend-native serialization mechanism; a process-local
mutex alone is insufficient.

Mutating transactions use point keys. Prefix scans are for read-only snapshots,
listing, and reports; their results cannot act as transactional absence or
range predicates. Name uniqueness reads a deterministic name-index key,
including its absence, and writes it atomically with the new branch. Other
set-based preconditions, including retention eligibility, use explicit point
guards/index records maintained with the membership changes. The protocol
does not depend on SQL unique constraints, foreign keys, or row locks.

The toy-kv source's serializable transaction API currently rejects range/prefix
scans because phantom tracking is absent; its separate read-only snapshot API
provides consistent scans. `ToyKvMetadataAuthority` preserves this separation
instead of silently reducing isolation. Both backends use the same logical
record encoding and domain transaction code. Adapter-specific engine types,
commit timestamps, SQL connections, and native errors stay behind the interface.
The implementation pins a reviewed toy-kv revision and must pass the authority
conformance gates; source inspection alone is not that evidence.

### 5.2 Authoritative records

| Record | Required state |
| --- | --- |
| Workspace | Stable ID, access scope, and retention policy. |
| Authority owner | Durable incarnation used to fence lease grants across owner restart. |
| Branch | Stable ID/name, head Generation, revision, writer epoch/lease, volume and stream epochs, live VolumeRoot/log-map root, active attachment, and capture token. |
| Name / retention indexes | Point-key name bindings and per-Generation eligibility guards, updated atomically with their referenced records. |
| Generation | Descriptor reference, publication operation, retention deadline, lifecycle state. |
| Operation | ID, request fingerprint, phase, allocated identities, prepared dependencies, terminal result. |
| Protection | Pins, reader leases, attachment base roots, and preparation dependencies, including capture/flush children. |
| GC report | Report ID, root snapshot/time, inventory window, completeness, and candidate summary/pages. |

These are logical records encoded as versioned keys and values. Branch IDs,
workspace IDs, and attachment IDs are never reused. Volume and stream epochs
are never reset or reused within their branch. Deleting and recreating a
branch name creates a new branch ID. Every head or live-view mutation increments
its state revision, including durable changes to its live volume or log map. Lease-only
renewal does not change this revision. Writer ownership changes advance the
writer epoch. Comparing only head Generation IDs is insufficient for CAS.

Normal transactions read and validate their branch, owner, operation, and
retention/index keys, then buffer all required changes in one `transact` call.
Retention eligibility and protection acquisition are checked atomically. A
concurrent change to a read precondition causes a conflict, retried with the
same operation ID. Independent branches need no exclusive workspace admission
lock. A GC report uses an authority snapshot without pausing writers or reader
acquisition. No mutation transaction remains open across S3 I/O.

### 5.3 Idempotent mutations

Every authoritative mutating SDK operation carries an opaque `operation_id`,
scoped to its workspace and authenticated caller. An append uses `append_id` as
its operation ID, not as a second deduplication mechanism. The caller durably
records the ID before sending a request.

The block server generates internal operation IDs for durable barriers and records
the attachment, covered I/O boundary, and sealed dirty version outside the exported
volume before sending them. A retried barrier cannot substitute another root.
Ordinary local READ/WRITE requests do not each create permanent remote operation
records; durable root changes, including every FLUSH/FUA commit, do.

```text
ABSENT -> PREPARING -> COMMITTED(result)
                   \-> ABORTED(reason)
```

The request fingerprint binds the operation kind, target identities, declared
preconditions, and logical payload. A protected preparation binds its request
first; checkpoint sealing records the final immutable root once, rejecting a
different root on retry. Matching retries return the recorded result or resume
that preparation. Reusing an ID with another request is `OperationMismatch`.
After matching the fingerprint, terminal-result lookup precedes checking the
preconditions of a new mutation. A replay does not fail merely because the
head, name, or grant has changed since its original success.

Terminal results and request fingerprints remain for the workspace's lifetime.
Compact records may replace large preparation details, but IDs never become
executable again. Results include the created branch ID, writer grant,
Generation, append range, pin, or resulting revision as applicable.

This is a monotonically growing metadata class: every append and other
authoritative mutation, including volume barriers, writer lease, base-protection, and preparation
renewals, keeps an idempotency record even after its log range is trimmed or its
Generation expires. v0 provides replay-safe idempotency for the workspace
lifetime at this storage cost. Bounded deduplication windows and safe result
expiry are deferred; metadata capacity must be provisioned and exhaustion
handled explicitly.

`get_operation(id)` reads authoritative state. A timeout means unknown outcome.
Even `NotFound` does not prove the original request cannot arrive later: retry
the same ID, or atomically record an abort tombstone before abandoning it.
`abort_operation(id)` creates that tombstone if absent, aborts a preparation, or
returns an already committed result. It cannot undo a committed mutation.

An old successful result does not extend a lease or retain its objects forever.
The caller can learn that an operation committed while its returned read lease,
writer grant, or historical Generation has since expired. Obtaining new rights
requires a new operation and current preconditions.

### 5.4 Writer grants

The first implementation permits one active grant per branch. An acquisition
transaction reads the branch and authority incarnation, validates the expected
epoch and ownership policy, advances the epoch, and records a 30-second
renewable grant. Renewal is normally attempted every 10 seconds.

Expiry uses the authority owner's trusted lease clock, with an injectable clock
for deterministic tests; it does not require a database time function or GTS.
Every attempt that authorizes a mutation or renewal reads the clock inside the
transaction body and re-evaluates lease validity using that attempt's metadata.
A retry after expiry cannot reuse an earlier valid time. The body check is an
early rejection only; the Metadata Authority repeats the lease check at the
serialization/commit decision required by Section 5.1. A mutation is authorized
according to that decision point, even if durable acknowledgement arrives later.
Returning an existing terminal operation result still follows Section 5.3 and
grants no new rights.
Clock comparisons cannot regress within an owner incarnation. Before serving
after restart, the owner advances its durable incarnation. Old lease grants
cannot authorize new requests across that boundary; workers reacquire their
grants/protections. Transactions validate the incarnation as a point-read
precondition along with branch epochs. This is restart fencing for one authority
owner, not a multi-node election or availability protocol.

Acquisition has its own operation ID. A lost acquisition reply followed by a
retry returns the same grant and epoch; it must not fence that grant by creating
another one. Retrying an old renewal likewise returns its old deadline. Each
new renewal uses a new operation ID. An expired grant cannot renew itself;
reacquisition advances the epoch.

Every final authoritative mutation validates branch ID, epoch, grant, lease,
authority incarnation, and relevant revision in its transaction. Upload under
a valid grant does not authorize a later commit after handover. An authorized
forced handover advances the epoch before granting the replacement writer. Release,
branch deletion, and head-reset operations revoke the old grant.

The SDK sequences authoritative volume barriers, log mutations, and head changes
for one branch. A registered checkpoint capture token blocks other external
head/live-view mutations; its identified drain-flush children remain permitted
until sealing. Each final transaction checks token ownership and phase. Writer/
base-protection renewals, preparation heartbeats, and outcome query/abort remain
available while capture or a durable barrier holds admission. Explicit ownership
handover can fence the capture; expiry cleanup clears a token only after its
operation is durably aborted or otherwise resolved. CAS/fencing remain required
against clients that violate SDK admission. Different branches prepare independently.

## 6. Versioned block-volume view

### 6.1 Device contract and filesystem boundary

The block server exposes a fixed-capacity Linux ublk device backed by a complete
VolumeRoot plus a mutable dirty overlay. Initial requests are sector-aligned
READ, WRITE, and FLUSH, with correctly negotiated FUA/preflush behavior. Start
with one hardware queue and one ordered mutation stream per writable attachment;
additional queues require an equivalent device-wide barrier protocol.

Reject out-of-range/unaligned requests and unsupported commands. Do not advertise
discard/TRIM, resize, write-zeroes, secure erase, or atomic multi-sector writes.
Ordinary zero-valued writes work normally. Device geometry and limits must match
the qualified kernel/userspace binding; unsupported required features fail setup.

ext4 is the first reference filesystem, with its journal inside this volume.
Linux implements inodes, hard links, symlinks, permissions, xattrs, rename, sparse
files, mmap, and journal recovery under the tested ext4 configuration. Version
FS preserves block bytes and ordering; it does not independently implement or
promise those filesystem semantics for every consumer. Multi-device filesystems,
external journals, and dependencies outside the volume are outside the reference
restore guarantee. A filesystem UUID is not a Version FS branch identity.

A self-contained Git repository works through ordinary filesystem calls. Git
alternates, external worktree state, and database/WAL bytes outside the volume
are external dependencies unless separately captured. A database using ext4 is
a filesystem workload, not a native Version FS database participant.

### 6.2 Attachment, overlay, and fencing

`attach_volume` binds a branch's current live durable VolumeRoot to a new stable
attachment ID under its exact writer grant and volume epoch. One writable
attachment is active per branch. A fork gets an independent writable identity;
reset, detach/replacement, and ownership handover fence the previous attachment.
Every durable root replacement validates that identity and the writer lease.

The worker records the attachment, base root/protection, pending operation IDs,
and capture/freeze intent outside the exported volume. Control files, block-server
state, and heartbeat workers must not depend on the filesystem they freeze.
A base protection is renewed every minute and expires after five minutes.
Replaying a renewal returns its original deadline. Expiry requires explicit
reattachment; an old attachment cannot silently rebind to a newer root.

READ resolves the latest admitted dirty extent, then the immutable base map,
then a verified local/remote object. Ordinary WRITE completion acknowledges the
local volatile overlay only. Dirty bytes are not disposable clean-cache entries
and cannot be evicted before persistence or a definite, explicit discard.
Background uploads may prepare immutable extents, but cannot by themselves
advance authoritative state or justify a durable acknowledgement.

Once fencing, lease/protection expiry, or unrecoverable server failure is known,
the old device stops accepting writes and fails persistence requests. It must
not hot-switch its backing root beneath a mounted filesystem. Preserve available
dirty state for explicit salvage; unmount/stop and create a fresh attachment for
normal use. Transparent ublk server reissue/recovery is deferred in v0.

### 6.3 FLUSH, FUA, and remote durability

The device advertises a volatile write cache honestly. The Linux
[cache-control contract](https://docs.kernel.org/block/writeback_cache_control.html)
requires preflush to persist earlier writes before its dependent write, and
FUA to make its flagged write durable before completion. The adapter must
negotiate native FUA or the kernel's supported flush-based emulation; it must
never ignore either condition or claim a nonvolatile cache while acknowledging
only local dirty bytes.

For this MVP, the persistence boundary for successful FLUSH/FUA is S3 plus the
authority's durable live-volume-root replacement, not the worker disk:

1. Sequence the barrier after the writes it covers. Stop later writes from
   mutating that sealed extent version until the barrier outcome is resolved.
2. Persist an internal operation ID and barrier boundary outside the volume;
   register a protected preparation with the exact attachment, old live root,
   volume epoch, branch revision, and writer grant.
3. Upload changed extents and copied map nodes; verify the complete root through
   trusted builder dependencies. Do no S3 I/O inside a metadata transaction.
4. Atomically CAS the branch's live VolumeRoot, advance its revision, record the
   barrier receipt, and close preparation. Recheck lease validity at Section 5.1's
   logical serialization point. Acknowledge the kernel only after durable success.

A FUA request MUST persist the ordered dirty prefix through and including the
FUA-tagged write before completion. Persisting only writes before the tagged
write does not satisfy FUA. For a combined `REQ_PREFLUSH + WRITE + FUA` request:

1. Remotely persist all earlier completed writes before starting the payload.
2. Execute the payload write.
3. Remotely persist the payload itself, including its authoritative live-root commit.
4. Complete the request only after that durable outcome.

Native FUA and the qualified write-plus-post-flush emulation must provide this
same boundary. These boundaries apply across the device, not merely to one
upload task.

A lost reply is resolved with the same internal operation ID. An unknown commit
is never converted to successful I/O or retried as a new barrier; later dependent
writes remain blocked or fail explicitly. Replay can reveal an already committed
barrier, but does not renew expired rights. A fresh writable attachment resumes
from the current live root even when it is newer than the branch's last Generation.
Older flush roots are not permanent historical snapshots; later writes/reset may
replace them. Only retained Generations/pins provide that historical guarantee.

### 6.4 Checkpoint consistency modes

| Mode | Captured boundary | Guarantee |
| --- | --- | --- |
| `block_cut` | Drain admitted block requests and durably flush one exact device cut. | Complete block state respecting acknowledged ordering; mounted filesystems/applications may need crash recovery. |
| `filesystem_frozen` | Quiesce participating producers, successfully freeze the reference filesystem, drain/flush the device, then capture. A clean unmount is also an explicit supported filesystem boundary. | A stable filesystem image under the qualified freeze/unmount protocol; no automatic application transaction guarantee. |

The mounted ext4 CLI workflow uses `filesystem_frozen`. Freeze support follows
[fsfreeze](https://man7.org/linux/man-pages/man8/fsfreeze.8.html); ext4's
[journal recovery](https://docs.kernel.org/filesystems/ext4/journal.html) remains
the filesystem's responsibility. A block cut alone is not labelled as a frozen
filesystem snapshot. A shared timestamp, flush, or freeze does not create a
consistent application/database/process-memory snapshot.

A database needing a particular transaction/WAL boundary must first use its own
checkpoint/quiescence protocol. v0 records no claim of application consistency
from merely placing database files on ext4. External services, RAM, connections,
and side effects are not captured.

### 6.5 Coordinating the common Generation boundary

The trusted worker coordinator gates application commands, native log mutations,
and named-artifact staging. After draining admitted producers, it reads the
expected branch revision/live roots together from the authority, then registers
the checkpoint's branch capture token/preparation against those exact preconditions.
Completed drain-time appends and volume barriers are included in that tuple.
The worker's original writer grant, attachment identity, and fencing epochs remain
bound throughout; reading current roots must not adopt a replacement owner's identity.
An acquisition conflict invalidates the drained boundary: the coordinator MUST restart
Section 8.2 Step 1; it MUST NOT refresh the revision/roots and continue the same
capture. Other head/live-view mutations are rejected as Busy while the token
is held. The checkpoint's own device-drain flushes are permitted as identified
children until the capture is sealed.

For `filesystem_frozen`, keep block I/O service running while calling freeze:
freeze itself must be able to write back data/journal blocks and issue flushes.
Closing the low-level device gate first would deadlock that operation. Once
freeze succeeds, issue the final durable barrier, close/drain block mutation
admission, and seal the resulting VolumeRoot, exact log-map root, staged
artifacts, and resulting branch revision. Child flushes cannot continue after
sealing. Unmanaged raw writers violate the capture contract.

Hold mutation admission and the filesystem freeze through publication or a
definite abort. Renew writer, attachment-base, and preparation protection on
independent workers. On an unknown renewal, pause further uploads/publication
and reconcile that same renewal ID. Preserve dirty bytes and the freeze intent.

A surviving coordinator/watchdog reconciles a lost publication reply using its
recorded operation ID before thawing. On fencing, stop/unmount the old attachment
rather than resuming it against another root. If the worker is gone, restore
from durable authority on a new worker; do not infer an outcome from a local
freeze flag. A committed child flush stays committed if the later Generation
capture aborts; abort never undoes an acknowledged durability barrier.

The managed runner and coordinator enforce an execution boundary, not a security
sandbox. Commands retain their OS permissions. Effects through external symlinks,
host paths, network services, or other devices are outside this captured state.

### 6.6 Native artifacts and immutable inspection

Named artifacts remain a gated local overlay of explicit payloads/references,
captured into the native artifacts root at checkpoint. Log attachments are
explicit durable object edges at append. Neither requires parsing ext4.

A protected Generation may be exposed as an immutable read-only block device.
Filesystem recovery must never write to that retained root. If a filesystem
needs journal replay, fork a writable volume and replay there; a read-only
inspection that cannot proceed without writes fails explicitly. In particular,
a filesystem mount labelled read-only is not by itself proof that recovery
will issue no block writes.

## 7. Ordered log implementation

### 7.1 Reference model and identities

[S2 streams](https://s2.dev/docs/concepts/streams),
[atomic durable appends](https://s2.dev/docs/concepts/appends), and
[conditional appends](https://s2.dev/docs/concepts/concurrency-control)
are the reference model. Version FS implements its own segment/index path on S3
and the Metadata Authority. S2 is not a required dependency, and Version FS does
not claim wire compatibility or S2's latency and throughput.

Each named log has a stable `log_id` in the immutable log map. Its current
writable identity is:

```text
StreamId = (branch_id, stream_epoch, log_id)
LogView  = { log_id, first_seq, next_seq, segment_index_root }
```

Sequence numbers are consecutive unsigned 64-bit integers, starting at zero;
overflow rejects the append. Visible ranges are half-open `[first_seq, next_seq)`.
Sequence numbers determine order. Optional record timestamps are application
metadata and are neither Generation IDs nor GTS coordinates.

A branch points to a persistent map of its live log views. A fork shares that
map and creates a distinct effective StreamId through its new branch ID.
Rollback or adoption replaces the map and advances `stream_epoch`. This makes
fork and reset independent of the amount of historical log payload and avoids
eagerly recreating every stream record in the authority.

### 7.2 Append protocol

The initial limit is 1000 records and 1 MiB of encoded records per batch, checked
before upload. Larger payloads use explicit chunked artifact attachments.
There is one immutable sealed segment per accepted batch; no shared ingress
WAL or partial open segment needs sealing during checkpoint.

```text
append(stream_id, match_seq_num, append_id, records, attachments)
    -> AppendReceipt { stream_id, append_id, start_seq, end_seq,
                       branch_revision }
```

An append preparation follows the shared protected-preparation lifetime in
Section 8.2. `append_id` is its prepared operation ID. This allows large
attachment or segment uploads to last longer than ten minutes while the
preparation remains renewed, without weakening the atomic tail replacement.

1. Register the operation and a protected preparation against the current branch
   and live log root. Validate the writer grant, stream identity, and expected
   next sequence number.
2. Encode the segment with record boundaries and explicit attachment references.
   Durably upload new attachments, the segment, and changed segment-index and
   log-map nodes. Protect reused dependencies as described in Section 8.
3. In one metadata transaction, revalidate authority, expected branch revision,
   stream identity, and `match_seq_num`. Replace the live log-map root, advance
   the branch revision, and record the exact append receipt. Close preparation
   protection atomically with these changes.
4. Acknowledge only after the object writes and transaction are durable.

The range is `[start_seq, end_seq)` with `end_seq - start_seq` equal to the batch
record count. Empty batches are rejected. Readers see the complete batch or
none of it. Unreferenced uploaded segments are preparation garbage, not accepted
events. Retrying the same `append_id` returns the original receipt even if the
tail has advanced. A different payload or expected position with that ID fails.

This implementation requires both the writer grant and the match condition;
it does not adopt S2's optional cooperative fencing behavior. Independent logs
have no global total record order. v0 additionally serializes mutations within
one branch for a small correctness surface.

### 7.3 Checkpoint, fork, and trim

After the checkpoint coordinator gates and drains native appends, it binds the
existing live log-map root. It does not recopy, reseal, or replay historical segments. Because
each segment is one atomic batch, its captured `next_seq` is a batch boundary.
An append racing checkpoint belongs entirely before the gate's cut or entirely
after it reopens.

Fork descendants share the captured segment graph but have different writable
stream identities. They may assign the same later sequence numbers to different
records. Reading by sequence number therefore always includes a stream identity
or a specific Generation's LogView.

Explicit live trim moves `first_seq` forward to a segment boundary and builds a
new index without the removed prefix. It does not alter any segment. v0 accepts
only whole-batch boundaries and rejects a cut through a batch. Retained
Generations, read protections, and other branches keep their older indexes and
segments reachable. There is no automatic time-based trim in the MVP.

Log creation, trim, and deletion use the same protected preparation and
conditional live-root replacement pattern as append. Deleting a live log
removes its map entry; it cannot erase segments retained by another root. These
operations use the same Section 8.2 preparation deadline and renewal semantics.

Rollback and promotion select one published log view; they do not merge logs or
replay tool calls. Uncheckpointed live events remain separate from published
Generations and are preserved only while their live roots or other protections
exist.

### 7.4 Reads and cursors

Historical reads use a protected Generation handle and its exact LogView. Live
reads acquire protection for a current log root before reading its segments.
Following a live log repeats this protected lookup; notifications are an
optimization, and polling is sufficient for the MVP.

A live cursor includes StreamId and the next sequence number. A changed stream
epoch returns `StreamReplaced`; a cursor before `first_seq` returns
`CursorExpired` with the current retained start. The client must explicitly
choose a new stream/view. It cannot silently join a different branch, skip
trimmed records, or infer exactly-once external processing from an append
receipt. Log deletion and recreation allocate a new log ID.

## 8. Checkpoint preparation and publication

### 8.1 Proving a closed durable graph

The trusted builder hashes and validates object bytes, ownership, type, encoding,
and durable upload results. A client-supplied root is not proof of durability.
Each new node references either verified durable objects from its protected
preparation or shared subtrees from already validated protected roots. Unprotected
reuse requires complete verified reads/uploads. Validation proceeds bottom-up
without walking every unchanged protected subtree.

This applies equally to durable volume barriers, checkpoint descriptors, native
log roots, and artifact attachments. Required dependencies are protected until
commit or a definite abort. No object deletion or GC admission barrier exists
in v0. The parent Generation ID is not a required content edge.

### 8.2 Capture sequence and preparation lifetime

1. Record the checkpoint ID/freeze intent outside the volume, close application/
   log/artifact admission, and drain active producers. Then read the expected
   branch revision/live roots together from the authority. Register the branch
   capture token and protected preparation under those exact revision/root
   preconditions and the worker's original attachment/grant/fencing identities.
   A change of ownership fences this attempt; it must not refresh to a new owner.
   A definite acquisition conflict ends this attempt: restart Step 1 with a new
   checkpoint operation ID, a fresh drain, and a new post-drain revision/root read;
   never silently rebase the old drained cut. An unknown acquisition outcome must
   first be reconciled with the original operation ID before deciding whether to
   proceed or restart.
2. Establish the requested Section 6 boundary. Keep the device serving freeze's
   I/O; permit only the capture's identified drain-flush children. Each child
   advances the live root/revision normally and records its own durable outcome.
3. After the final durable barrier, stop/drain block mutations and seal one
   immutable tuple of volume boundary/root, log root, artifact input, and the
   resulting revision. Track only authorized child revision changes; unrelated
   mutations conflict rather than silently rebasing the capture.
4. Build/upload new artifact/index nodes and a complete Generation descriptor.
   The volume root and historical log segments are reused, not rescanned or recopied.
5. Publish the sealed tuple in one authority transaction, then reconcile the
   result and local baseline before thawing/reopening admission.

Every protected preparation spanning remote object I/O has a renewable ten-minute
inactivity deadline, with a new identified heartbeat every three minutes. This
includes checkpoint, volume FLUSH/FUA barriers, and log create/append/trim/delete.
A capture's internal flush is an authorized child, not a competing external
capture. An operation may explicitly declare a shorter non-renewable lifetime.

`renew_preparation(renewal_operation_id, prepared_operation_id, exact_grant)`
extends only nonterminal preparations under a valid grant/epoch/incarnation.
Upload traffic does not renew protection. Replaying a renewal returns its recorded
expiry; an unknown renewal must be retried/queried under the same ID before another
is allocated. Pause further work while unresolved, keeping the capture freeze or
barrier admission and dependencies protected.

Expiry permits the authority to record an abort; it never proves an unknown
transaction did not commit. Abort/expiry and final commit are ordered atomically.
An aborted operation cannot later publish a Generation or replace a live root;
committed results remain committed. Definite failure preserves checkpoint/dirty
state for salvage and returns no successful append/barrier receipt. Late uploaded
unreferenced bytes remain stored and may appear in mark-only reports.

### 8.3 Publication transaction

Validate authority incarnation, writer/attachment epochs, current lease,
capture token, sealed roots, exact resulting branch revision, preparation state,
and retention guards under Section 5.1. Atomically:

1. Create the Generation catalog/descriptor binding and initial retention.
2. For attached publication, move the head, advance revision, and move the
   attachment's Generation base protection to the new Generation.
3. Record the immutable operation result, release the branch capture token,
   and close preparation protection.

The already committed live volume/log roots must still equal the sealed roots.
Detached publication omits step 2: the Generation is visible and retained by ID
without moving the head or rebasing the attachment's Generation baseline.
Its preceding durable flushes remain ordinary live-root changes.

Before the transaction, no uploaded descriptor is published. After commit, the
Generation is authoritative even if the reply is lost or the head moves again.
The coordinator queries the operation result, syncs its local baseline where
applicable, and only then releases the freeze/admission boundary. A failed CAS
produces no partial Generation/head update and never clears dirty local work.

## 9. Branch lifecycle and reference operations

### 9.1 Create and fork

Workspace creation accepts fixed volume geometry and installs a zero VolumeRoot,
an empty log/artifact view, an empty Generation, and main after the immutable
empty graph is durable. Formatting ext4 is an explicit later consumer operation;
an empty Generation is not falsely presented as a formatted filesystem.

Branch creation takes a retained Generation, allocates a new stable branch ID,
and copies only its volume/log root references and head. It starts without a
writer or attachment. No volume extents, filesystem namespace, or log history
are enumerated. Replay returns the original branch ID despite later name reuse.

### 9.2 Adopt, roll back, and promote

Adoption/rollback selects a retained Generation. Promotion validates source and
destination stable IDs/revisions in one transaction and adopts the source's
published head. Uncheckpointed live volume roots, live log appends, and local
artifact staging are not promoted merely because they belong to that source.

Reset replaces destination live volume and log roots from the selected Generation,
advances branch revision plus volume/stream/writer epochs, and revokes its old
writer/attachment. If durable live volume or log state differs from the destination
head, explicit `discard_live_state` is required. Stable identity/revision CAS
prevents name-reuse and head-away/head-back ABA. An active capture must be resolved
or explicitly fenced before replacement.

Never switch a mounted device to another root. The SDK requires unmount/stop and
fresh attachment before using the selected state; local dirty bytes and staged
artifacts remain available for explicit salvage. Fork and attach a writable clone
for journal recovery instead of modifying a retained Generation.

### 9.3 Delete

Deletion validates current write authority/revision, revokes the attachment/writer,
removes the live name and volume/log branch roots, and records the result atomically.
Historical Generations, pins, readers, and unresolved operations retain their own
protections. A replacement name gets a new branch ID; replay of the old deletion
cannot affect it. Deletion removes no S3 bytes and never forcibly remounts a worker.

## 10. Retention and reachability reports

### 10.1 Roots and protection

The default policy retains each newly published Generation for 24 hours.
Branch heads and explicit pins can retain it longer. Detached Generations have
the same initial protection and may be pinned without branch adoption.
Automatic hourly/daily selection and branch expiration are deferred.

Retained and protected roots include:

- Descriptors and complete graphs of retained Generations and branch heads.
- Current live VolumeRoots and live log-map roots, including durable barriers
  and appends not yet captured in a Generation.
- Explicit pins, attachment-base protections, and reader protections.
- Prepared objects and dependency roots until their operations have definite
  outcomes or have been durably aborted and fenced.

A parent Generation ID is lineage, not a strong edge to its descriptor. Required
Git files, log attachments, segment indexes, and named artifacts are strong
edges. Reachability marking includes each retained descriptor as well as its
complete content graph.

Opening a Generation transactionally verifies publication and current retention
and creates a five-minute renewable read protection. An open writable volume attachment
also protects its selected base roots under the worker SDK's one-minute heartbeat
and five-minute lease. Eligibility and protection acquisition are checked in one
metadata transaction; physically present bytes alone cannot make an expired
Generation eligible again. Expired attachment-base and preparation protections
cease to be roots at their deadlines and are removed by authority cleanup,
without requiring the worker to close normally.
Renewal does not change the protected root. Remote object reads have a
30-second deadline and are admitted only when the handle remains valid through
that deadline. The service drops results arriving after an abort or expiry.
Long exports renew their handles; an expired handle must be opened anew.

Releasing the last pin, deleting a branch, or trimming a log changes logical
retention and visibility; none of these operations deletes object bytes in v0.
An expired Generation cannot be reopened merely because its objects happen to
remain on S3. Operation-result records are not permanent content-retention roots;
they continue to report the original outcome after independent retention ends.

### 10.2 Mark-only report

`collect()` is an operator reporting operation. It calculates reachability and
reports observed unreachable objects; it never issues an object DELETE or
changes a Generation's publication or retention state.

1. Open a read-only authority snapshot. Enumerate retained/protected roots and
   known preparation dependencies through snapshot-bound prefix scans; copy
   them into report pages using separate point transactions. Seal that copied
   root set, record its boundary, and release the snapshot before S3 I/O.
   Retries use the same sealed root set. Losing the snapshot before sealing
   aborts that report attempt; a new boundary requires a new operation ID.
2. Traverse durable roots and typed edges with a visited set that can spill to
   disk. Include protected staged keys; surface incomplete preparations and
   unknown outcomes as uncertainty. Missing, corrupt, or unsupported required
   durable metadata makes the report incomplete, not evidence of unreachability.
3. List the data prefix and compare observed objects with the marked set. Persist
   the inventory window, counts/bytes, uncertainty, and candidate pages within
   a configured report-storage budget. Report details have a configured expiry
   policy; terminal identifiers and summaries retain the operation-result
   contract. Reading expired details returns `ReportExpired`, not a new report.
4. Return the report ID and completeness status. An interrupted or over-budget
   report can resume or fail without changing workspace availability or bytes.

Checkpoint publication, appends, branch changes, and reader acquisition continue
during reporting. The root snapshot has one metadata boundary; object inventory
is observed over a window and may omit concurrent uploads. Candidates mean
unreachable from the captured roots, not safe to delete now. A later publication
may already reference a candidate. Reports do not create retention roots, and
old reports are never deletion authorizations.

The MVP has no collection mode, writer-draining barrier, delete executor, or
unknown-DELETE recovery state. Late PUTs from aborted preparations and bytes
unreferenced after trim or expiry stay on S3. Their storage cost is accepted.

### 10.3 Future reclamation boundary

Physical reclamation requires a separate RFC covering publication/reuse races,
in-progress operations, protected readers, storage-instance identity, deletion
fencing, and uncertain remote outcomes. Enabling deletion is not an operator
switch on a v0 candidate report.

One possible future mechanism is version-specific S3 reclamation: a DELETE with
a specific `versionId` addresses that stored version; an ordinary DELETE in a
versioned bucket creates a delete marker. See
[S3 object-version deletion](https://docs.aws.amazon.com/AmazonS3/latest/userguide/DeletingObjectVersions.html).
Using that mechanism safely also requires binding graph references and reuse
to the intended physical versions. This RFC chooses no deletion protocol and
does not require bucket versioning for the MVP.

## 11. Reads, cache, and recovery

### 11.1 Immutable volume and native-state reads

An authorized protected handle resolves the Generation's volume descriptor,
selected extent-map paths, log views, and artifact references. Reads do not
consult ancestors. Unmapped extents produce zeros; present extents are fetched
and verified in full before slicing. No unauthenticated or arbitrary-digest
object-read endpoint is exposed.

Clean cache keys include workspace, type, format version, and digest. Download
to temporary files, verify hash/length, then install atomically; coordinate fills
and enforce a capacity budget. Corrupt cache entries are replaced from S3.
Missing/corrupt authoritative objects cause an integrity error, never zero-fill
of an allocated extent or fallback to older contents. Dirty extents, freeze
intent, and unresolved barrier/capture records are outside clean-cache eviction.

### 11.2 Worker, device, and authority restart

A fresh worker can open a retained Generation and lazily expose its volume,
without downloading the entire virtual disk. Fork for a writable mount or
filesystem recovery. Native logs/artifacts remain available through their own
Generation handles without mounting ext4. A complete raw-image export is an
explicit O(volume capacity) operation, not the normal attach/read path.

After ordinary worker/device loss, a fresh writable attachment starts from the
branch's latest committed live VolumeRoot. A named retained Generation instead
selects its exact historical root. Successful remote FLUSH/FUA barriers survive
worker loss; later volatile writes may be lost. Filesystem/application recovery
uses the guarantees of that boundary, not a false claim that RAM was restored.

A surviving worker first reconciles pending barrier/checkpoint IDs and freeze
intent. Do not infer the outcome from the current head or automatically discard
dirty bytes. An old mounted device is stopped on server failure/fencing; v0
requires unmount/restart/re-attach rather than in-place root replacement. Journal
replay writes go to a new writable state, never to an immutable root.

The authority reopens its preserved volume, advances its durable incarnation
before serving, and reconciles protections, capture tokens, and terminal results.
Old grants/attachments are fenced. Prepared work may be definitively aborted;
committed volume barriers and Generations remain authoritative. Object listing
cannot reconstruct publication. An unavailable authority fails mutations closed.

### 11.3 Metadata recovery boundary

The reference service survives process restart with its durable metadata volume
preserved; loss of that only volume is outside the MVP guarantee. An older
metadata backup can omit acknowledged live roots, heads, epochs, and outcomes.
Keep the service offline after such a restore until a separate recovery procedure
reconciles authority and fences obsolete writers. S3 bytes alone do not make an
old catalog current. Automatic metadata failover is deferred.

## 12. SDK and CLI surface

These are logical requests/results, not a final Rust or network ABI. Authoritative
mutations use Section 5.3's operation contract. Workspace creation initially scopes
its ID to deployment/caller; later requests use workspace/caller scope.

| Request | Required arguments / result |
| --- | --- |
| `create_workspace` | Operation ID and fixed capacity/geometry; returns workspace, main, and zero-volume Generation. |
| `create_branch` / `get_branch` | Retained base and identified create, or stable ID/name lookup; returns head, live roots, revisions, epochs, and attachment state. |
| `acquire_writer` / `renew_writer` / `release_writer` | Identified request and exact grant/epoch/handover policy; returns original grant/deadline or release result. |
| `attach_volume` | Operation ID, branch ID/revision, exact grant; returns fenced attachment ID, protected base, live VolumeRoot/geometry, and device setup inputs. |
| `renew_base_protection` / `detach_volume` | Operation ID and exact attachment/protection; detach requires unmounted/drained state, resolves pending barriers/capture, and releases protection. It must flush unless explicit discard is requested. |
| `flush_volume` | Internal operation ID, exact attachment/epoch/grant, covered I/O boundary, sealed dirty version, and expected live revision; returns durable volume boundary/root/revision. |
| `checkpoint` | Operation ID, branch/attachment/grant, capture mode, attached/detached policy; captures/seals the exact final revision after authorized drain flushes and returns Generation/optional head revision. |
| `renew_preparation` | Renewal ID, prepared operation ID (including volume barrier or capture child), exact grant; returns its recorded deadline. |
| `move_head` / `promote` | Stable source/destination identities and revisions, retained target/published source head, destination grant, explicit live-state discard policy. |
| `delete_branch` | Operation ID, current stable ID/revision/write authority; revokes attachment and removes live refs. |
| `open_generation` / `renew_handle` / `close_handle` | Identified protected historical view lifecycle; replay does not renew rights. |
| `read_blocks_at` / `attach_readonly` | Protected Generation and byte range, or immutable block attachment; writes fail. Filesystem replay requires a writable fork. |
| `diff` | Two protected Generations; streams changed volume ranges, log-view changes, and native named-artifact changes. It returns no inferred filename diff. |
| `create_log` / `delete_log` / `trim_log` | Identified conditional live-root changes; trim only at batch boundaries. |
| `append` | Exact stream/grant, conditional next sequence, append ID, records/attachments; returns exact durable receipt. |
| `open_live_log` / `read_log_at` / `tail_log` | Protected current/historical roots and exact stream/cursor; retain the Section 7 contracts. |
| `set_artifact` / `remove_artifact` | Gated local staging; native named-artifact changes become Generation state at checkpoint. |
| `pin_generation` / `unpin` | Identified retained-Generation/exact-pin mutation. |
| `get_operation` / `abort_operation` | Original operation ID; recover or make the outcome terminal without undoing a committed barrier/publication. |
| `collect` / `get_gc_report` | Operator mark-only report lifecycle, root boundary/inventory window/completeness and paged candidates; expired details return `ReportExpired`. |

The CLI composes device attachment, reference ext4 format/mount/unmount helpers,
checkpoint/freeze coordination, branch/fork/rollback/promote, log/artifact access,
pins, diff, and recovery/report operations. Formatting is explicitly destructive
to the selected writable volume; it is never implicit on attach/restore.

Block READ/WRITE/FLUSH completion follows Section 6. A kernel write is not a
remote idempotent shell operation; the API promises neither exactly-once command
execution nor restoration of external effects. Mount helpers require the declared
Linux privileges and are not a sandbox.

Errors include `Conflict`, `Fenced`, `LeaseExpired`, `OperationMismatch`,
`InProgress`, `GenerationExpired`, `StreamReplaced`, `CursorExpired`,
`IntegrityError`, `UnsupportedOperation`, `InvalidGeometry`, `FreezeFailed`,
`ReportExpired`, and `Busy`. A transport timeout is an unknown result, not a
known rejection. Kernel-facing persistence errors propagate as failed I/O rather
than a weaker durability acknowledgement. Required consumer features are rejected
explicitly if the adapter cannot support them.

## 13. Failure behavior

| Failure point | Required result and recovery |
| --- | --- |
| Ordinary WRITE completes, worker then dies | Volatile writes after the last durable barrier may be lost; restore the committed live root or a selected retained Generation. |
| FLUSH/FUA fails or its reply is lost | Return no false successful I/O; reconcile the exact barrier ID. Keep dependent writes blocked or fail explicitly. |
| Barrier commits before worker loss | Its live root survives independently of worker disk; a lost kernel reply does not undo the commit. |
| Preflush fails | Do not execute/acknowledge its dependent data write as though the barrier succeeded. |
| External mutation commits after the post-drain root read but before capture-token acquisition | Reject acquisition on the old revision; restart capture and drain with a new operation ID, never refresh roots and reuse the old cut. |
| Extent upload succeeds, metadata commit fails | Old live root remains authoritative; new objects are unreachable preparation data. |
| Descriptor uploaded, publication not committed | No Generation is visible. |
| Barrier/publication reply is lost after commit | Recover the original operation result even if live roots/head later moved. |
| Definite transaction conflict | Retry metadata logic and the fresh final lease decision under the same operation ID; repeat no remote/local side effects inside the closure. |
| Lease expires before the logical commit decision | Reject the entire write set; delayed durability acknowledgement after a valid decision remains valid. |
| Unknown backend commit | Reconcile via authoritative outcome/recovery before admitting conflicting new mutations; do not infer abort. |
| Writer/attachment fenced during upload | Reject root replacement/publication; fail persistence on the stale device and preserve salvageable local bytes. |
| Freeze fails | Do not label the capture filesystem-frozen or publish that requested capture; resolve its preparation and preserve local state. |
| Coordinator dies while filesystem is frozen | Reconcile recorded freeze intent and operation before controlled thaw/stop; do not write local control state through the frozen volume. |
| Child flush commits, parent checkpoint aborts | Preserve the acknowledged live root; no Generation is published and the flush is not rolled back. |
| Preparation/base renewal reply is lost | Replay/query the exact renewal ID; do not extend twice or discard unresolved dependencies. |
| Preparation expires | Durably order abort against final commit; late uploads cannot revive it. Clear capture admission only after a definite outcome. |
| Device server dies | Fail/stop the old device and recover with a fresh attachment; no transparent hot replacement or unqualified request reissue. |
| Append races capture | Capture the whole batch before the declared cut or admit it afterward; never split it. |
| Trim/reset races a cursor | Return explicit expired/replaced errors; retained historical bytes remain unchanged. |
| Report interrupted or roots change | Report the captured boundary/window and uncertainty; never delete bytes or stop normal publication. |
| Missing/corrupt allocated extent or index | Integrity error, never zero-fill or older contents. |
| Queues/disk/metadata/S3 unavailable | Fail or reach a recoverable outcome; no weakened success promise or eviction of dirty/uncertain state. |
| Older metadata backup restored | Keep authority offline until a separate reconciliation/fencing procedure. |

Keep four acknowledgements distinct: ordinary block WRITE is local volatile
state; successful FLUSH/FUA is a durable live volume boundary; append is an exact
durable log range; checkpoint is a published retained Generation binding its
volume/log/artifact views. None implies an application-consistent global snapshot.

## 14. Performance, bounds, and benchmarks

### 14.1 Expected costs and amplification

Let E be mapped volume extents, D changed extents in a persistence cut, S log
segments, V retained Generations, and R the distinct report graph.

| Operation | MVP cost and limitation |
| --- | --- |
| Change discovery | Track admitted dirty block ranges; no O(files + file bytes) directory scan. |
| Volume persistence | Changed 64 KiB extents plus changed map paths; partial writes may require verified read-modify-write. |
| Checkpoint | Establish the requested cut/freeze, flush outstanding dirty extents, bind existing volume/log roots, upload staged artifacts/descriptor, and publish. |
| Fork | Bounded metadata/reference writes, independent of capacity, mapped extents, file count, and ancestry. |
| Attach/read | Fetch root/index paths and demanded extents; independent of V. Filesystem startup may read its own metadata working set. |
| Raw image export | O(capacity); an explicit operation, not a prerequisite for lazy attach. |
| Log append | New batch/attachments plus changed segment/log-map paths and one authority commit. |
| Diff | Skip shared extent subtrees; worst case full mapped-state comparison. No inherent filename semantics. |
| Report | Root snapshot, O(R) marking, inventory, and bounded result storage; no physical reclamation. |

Storage delta is approximately dirty storage extents plus changed Version FS
metadata, not application bytes alone. ext4 journal/metadata writes, repeated
barriers, and 4 KiB-to-64 KiB extent amplification may dominate. Measure these
costs explicitly. Removing file scans does not promise low remote fsync latency:
each durable barrier includes object-store work and authority durability.

### 14.2 Bounded resources and overload

Initial object transfer limits remain eight per workspace and 32 service-wide,
with at most 64 admitted service requests. The initial writable device has one
queue with a configured finite depth and bounded request buffers. One branch
has one ordered persistence/capture stream; renewals/outcome queries remain
available independently. Pin exact ublk limits in the release configuration.

Configure clean-cache, dirty-overlay, temporary-disk, and report budgets separately.
A full dirty budget applies backpressure or fails I/O; it never evicts unwritten
bytes. Seal stable extent versions before upload so a concurrent overwrite cannot
change the bytes associated with an in-flight hash/barrier. The initial flush
path may block later writes through commit to avoid keeping multiple dirty epochs.

Before admission return Busy/capacity errors. After admission preserve identified
outcomes/protection or record a definite abort. Report budget exhaustion does not
block writers. Both unreclaimed S3 objects and workspace-lifetime operation records
(including durability barriers and heartbeats) grow cumulatively in v0. Expiry or
trim cannot be used to erase deduplication records or enable physical deletion.

### 14.3 Benchmark report

Publish reproducible results on real S3, preserved toy-kv storage, Linux ublk,
and the reference ext4 profile. Record exact revisions, WAL settings, kernel and
userspace binding, device geometry/queue configuration, mkfs/mount/journal settings,
hardware, region, network, budgets, sample counts, errors, and raw observations.
Scenarios are separate experiments rather than one Cartesian product.

| Scenario | Required comparison |
| --- | --- |
| Capacity and allocation | 1, 100, and 1000 GiB virtual capacity with sparse and populated maps; attach/fork must not enumerate capacity. |
| Raw writes | 4 KiB random writes versus sequential/coalesced 64 KiB and 1 MiB writes; cold partial-extent read-modify-write versus warm cache. |
| Filesystem workload | 1,000, 100,000, and 1,000,000 files, high fanout, rename/unlink, sparse files, mmap, and a self-contained Git worktree. |
| Large content | 1 GiB and 100 GiB files; small overwrite versus append; compare application/device/dirty/uploaded bytes. |
| Durability | Ordinary writes versus FLUSH/FUA and filesystem fsync; include barrier batching and remote-failure latency. |
| Checkpoint modes | Raw block cut versus reference ext4 freeze/clean unmount; measure quiescence, freeze, upload, publication, and thaw separately. |
| History and fan-out | 10/100/1000 Generations and 1/10/100 branches; no-change checkpoint, independent writes, promote/reset. |
| Logs/artifacts | 100,000 1 KiB records in batches of 1/100/1000; attachments, checkpoint root binding, fork, and trim. |
| Recovery | Empty versus warm cache; first block/mount working set versus full-image export; journal recovery on writable fork. |
| Reporting and lifetime growth | Shared/divergent volume/log roots, abandoned preparations, report races, and permanent barrier/append/heartbeat outcome metadata. |

Report latency distributions/throughput and failed requests, filesystem journal
amplification, extent/map object counts, bytes fetched/uploaded/newly stored, peak
RAM/disk, freeze duration, authority conflicts/serialization, and report overhead.
Separate independent-branch throughput from one hot volume. Structural costs are
release gates; numerical latency is a measured baseline, not an invented SLO.

## 15. Implementation sequence

| Phase | Deliverables | Exit evidence |
| --- | --- | --- |
| 1. Immutable volumes and feasibility | Canonical extent/map formats, S3 adapter/cache, complete root reader/diff, toy-kv conformance, and ublk/ext4 platform spike. | Empty-worker reads need no ancestors; map sharing works; real backend/ublk barrier behavior is demonstrable. |
| 2. Writable views | Grants/attachments, dirty overlay, durable barriers, freeze/capture publication, branch lifecycle, native logs/artifacts, SDK/CLI. | Checkpoint/fork/promote and fresh-worker recovery run through the real adapters with explicit durability/consistency boundaries. |
| 3. Qualification | Retention/protection cleanup, mark-only reports, restart/fault campaigns, benchmarks, and runbooks. | Required gates have evidence; acknowledged barriers and Generations survive their declared failures; reports never delete or pause publication. |

Foundational protections and fault tests ship with their protocols rather than
waiting for Phase 3. The PostgreSQL adapter, physical reclamation, and a native
file-tree view remain outside v0. The implementation plan tracks review-sized
work packages under these phases.

## 16. Acceptance criteria

### 16.1 Functional and structural gates

1. Create a fixed-capacity zero volume, acquire a grant, attach via ublk, explicitly
   format/mount ext4, and use normal filesystem/Git operations. Include hard links,
   symlinks, permissions/xattrs, rename, sparse files, and mmap under the qualified
   profile. Native logs and artifacts remain separate Generation views.
2. Checkpoint in both declared modes. Descriptor uploads and failed CAS are invisible;
   attached/detached publication behaves as specified. Frozen mode records a successful
   filesystem boundary; raw block cuts make no application-consistency claim.
3. Restore retained Generations on an empty worker without ancestor descriptor
   reads or full-image download. Native logs/artifacts resolve without ext4 mount.
   Replay a filesystem journal only on a writable fork, preserving the old root.
4. Verify no-change roots are reused and small writes copy only affected storage
   extents/map paths. Measure sector-to-extent and filesystem-journal amplification.
   Fork copies no payload or complete extent map; dirty discovery performs no file scan.
5. Verify block lookup/request count does not grow with Generation ancestry and
   extent-range diff matches a complete comparison, including zero transitions and
   tree splits. Do not label those results as filename or semantic Git diffs.
6. Fork/checkpoint/promote/rollback/delete and reuse branch names safely. Check
   source/destination revisions, volume/stream epochs, live-state discard, and
   rejection of old attachments. No mounted device is hot-rebased.
7. Validate atomic native append receipts, conditional tails, independent forked
   streams, exact checkpoint prefixes, cursor errors, and trim with historical
   pins and attachment bytes retained.
8. Enforce workspace scope for devices, reads/cache/object reuse, native views,
   and mutations. Knowing a digest, filesystem UUID, or device number grants no rights.
9. Verify report closure includes every retained/live volume/log/artifact root and
   uncertain preparation. Concurrent publication/read protection remains available.
   No report, trim, expiry, reset, or deletion issues an S3 DELETE; lifecycle expiration
   and object-delete permissions stay disabled.
10. Verify Linux READ/WRITE/FLUSH and negotiated preflush/FUA ordering. Successful
    barriers survive worker disk loss via committed live roots, including barriers
    newer than the last Generation. Unsupported requests and out-of-bounds I/O fail.
    A partial ordinary write or block cut is not advertised as application-atomic.
11. Run backend-neutral authority conformance against preserved toy-kv storage:
    atomicity, absent/stale point conflicts, fresh reads, durable recovery, paged
    snapshot consistency, and final lease decisions under the authority commit gate.
12. Verify backend-independent domain code, point-key uniqueness/retention guards,
    snapshot-only report scans, no engine timestamp identities, and a documented
    compatible PostgreSQL mapping. PostgreSQL implementation is not a v0 gate.

### 16.2 Failure-injection gates

Exercise each upload, root replacement, capture step, and local/device control
transition with delayed/duplicate requests, lost replies, injected time, and process
termination. Check API/kernel outcomes and persisted graph state, not only CLI exits.

- Create/acquire/attach/barrier replies lost after commit: replay returns the same
  identity/receipt; never allocate a replacement epoch or apply another barrier.
- Ordinary writes followed by worker loss: recover the last acknowledged durable
  barrier, allowing loss only of subsequent volatile work. Simulate the same with
  a native FUA or the qualified emulation path and combined preflush/data/FUA.
- Write distinct earlier data and FUA-tagged payloads, then destroy the worker
  immediately after successful completion: both must recover, including the
  tagged write itself. For combined preflush/data/FUA, verify earlier writes
  become durable before payload execution and the payload before completion.
- Pause a partial-extent upload and overwrite its buffer: the sealed version/root
  remains stable and later writes cannot contaminate the earlier cut.
- Fail or lose a FLUSH/FUA commit reply: no successful kernel durability completion
  before durable outcome; dependent writes cannot overtake it. A committed child
  barrier remains valid if its parent checkpoint later aborts.
- Keep device I/O available while ext4 freeze flushes its journal; demonstrate no
  close-before-freeze deadlock. Kill the coordinator during freeze, seal, commit,
  local-baseline update, and thaw; reconcile exact operation/freeze intent.
- Freeze failure and raw writers outside the declared protocol: no false frozen
  checkpoint. Test clean-unmount and raw-cut recovery separately.
- Complete an admitted append or fsync/FUA barrier during producer drain. The
  post-drain read must include its revision/live roots, without forcing an
  acquisition conflict merely because the producer completed while draining.
  Handover before that read still fences the worker's original grant/attachment;
  the coordinator must not adopt the new owner's identity.
- Commit an external append or volume barrier after the post-drain root read
  but before capture-token CAS. Acquisition must conflict; no stale-cut Generation
  may be published. A new attempt must drain again and then read fresh preconditions,
  rather than merely refreshing the revision/roots. Resolve lost acquisition
  replies with the original ID before starting another attempt.
- Stale writer/device after handover, reset, deletion, or authority restart: cannot
  replace a live root, publish, append, trim, or resume a mounted device on another root.
- Lease valid in an earlier transaction attempt but expired on retry/final decision:
  reject the complete mutation. A successful logical decision remains committed
  when durability acknowledgement arrives after expiry. No toy-kv precommit hook
  is assumed; test the adapter gate's actual decision boundary.
- Barrier/checkpoint/log preparation beyond ten minutes: renew every three minutes,
  replay a lost renewal without extending twice, pause unresolved work, and reject
  final commit after definite expiry/fencing. Test large slow artifact attachments.
- Abandoned barrier/append/capture or attachment: protection expires, operation is
  definitively aborted/fenced, capture admission is released safely, late uploads
  remain unreferenced, and old handles cannot silently reacquire rights.
- Durable publication/append followed by later head movement/name reuse: original
  result still resolves. An absent-operation abort tombstone wins against delayed
  execution; committed operations cannot be undone by abort.
- Authority crash around commit, retried closure conflicts, and unknown commit
  results: recover complete outcomes, preserve the commit gate's exclusion until
  resolution, and fence prior-incarnation grants before serving.
- Native append racing the common volume cut: entire batches belong on one side;
  native artifacts and log prefixes bind to the same declared capture interval.
- Report snapshot loss, concurrent roots/pins/readers, missing/corrupt metadata,
  interruption, and exhausted budgets: explicit failure/uncertainty without deletion
  authorization, root substitution, or workspace outage.
- Corrupt/missing allocated extents, maps, segments, or descriptors: error instead
  of zero-fill, older contents, or a successful partial filesystem recovery.
- Exhausted dirty/cache/disk/queue/metadata capacity and unavailable S3/authority:
  fail or preserve recoverable state; no dirty eviction, weakened acknowledgement,
  release of uncertain protection, or reversion to a cached authoritative head.

### 16.3 Invariant coverage and completion

| RFC-001 invariants | MVP evidence |
| --- | --- |
| I1-I3 | Immutable complete volume/log/artifact roots, ancestor-independent reads, COW sharing, and structural range diff. |
| I4-I5, I8 | Durable barrier/root publication, attached/detached Generation commits, CAS, trusted graph validation, and lost-reply recovery. |
| I6, I14 | Live/retained graph protection, mark-only report races, historical trim safety, and no physical deletion. |
| I7, I11 | Owner/attachment fencing, strict point preconditions, lease decisions, replay-safe internal/API operation IDs, and abort tombstones. |
| I9 | Destroy worker dirty/control/cache state; recover acknowledged live barriers and retained Generations from durable services. |
| I12-I13 | Workspace/device/object authorization and durable ordered native log ranges/attachments. |
| I10, I15 | Explicit filesystem/application/runtime/DB boundaries; no fabricated coordinated global-checkpoint claim. |

Completion requires every capability and gate above, real ublk/ext4/S3/toy-kv
benchmark evidence, and operator instructions covering device lifecycle, freeze
recovery, failed I/O, volume versus Generation recovery, metadata durability,
unknown outcomes, report limits, and cumulative storage/operation metadata growth.
Other filesystems are not qualified merely because the block interface is generic.

## 17. Selected tradeoffs

| Decision | Reason and accepted cost |
| --- | --- |
| First-class block view plus native logs/artifacts | Linux handles filesystem compatibility while Generation/checkpoint/fork/retention semantics remain shared across views. |
| ext4 over Linux ublk | Reference consumer/adapter; needs a qualified privileged Linux environment and explicit device failure/freeze behavior. NBD is only an optional prototype. |
| Fixed geometry, 4 KiB sectors / 64 KiB immutable extents | Simple COW mapping and direct dirty tracking; partial writes and journals amplify uploaded bytes. No resize/discard or atomic multi-sector promise. |
| Remote-durable FLUSH/FUA | Worker-independent fsync/barrier durability; S3 and metadata commit latency is exposed on the persistence path. |
| Blocking flush/frozen capture | Small correctness surface; later writes may pause through remote commit/publication. |
| Structural block diff | Cheap shared-subtree comparison; filenames, inode changes, and semantic Git diff require a later filesystem-aware view/tool. |
| Transactional MetadataAuthority / toy-kv reference | Backend-neutral protocol exercised locally with preserved storage; no production HA or engine-specific lease hook. |
| Compatible later PostgreSQL | Production path through managed metadata infrastructure; implementation/qualification deferred. |
| Native sealed log batches and artifact edges | Preserve exact ordered views independent of disk format; each batch adds remote objects and permanent outcome metadata. |
| S3 immutable graph and mark-only reporting | Portable recovery and reachability evidence; unreclaimed bytes and operation results grow for the workspace lifetime. |
| Trusted coordinator/runner | Provides capture coordination; OS access and external side effects require a separate sandbox/application protocol. |

Safe physical reclamation, native namespace semantics, optimized extent packing,
transparent device recovery, runtime snapshots, and coordinated GTS/DB integration
need additional proposals rather than hidden extensions of v0's guarantees.

## 18. References

Primary sources consulted on 2026-10-23; Version FS formats, protocols, defaults,
and release gates above are proposals of this RFC, not vendor guarantees.

- [Fly.io: SBD — We Have a Filesystem at Home](https://fly.io/blog/sprites-block-device/)
  is the object-backed block-device/ext4 reference discussed in Section 3. It uses
  NBD; it is not a ublk implementation or a Version FS durability guarantee.
- [Linux ublk](https://docs.kernel.org/block/ublk.html) describes the userspace
  block framework used by the reference adapter.
- [Linux write-cache control](https://docs.kernel.org/block/writeback_cache_control.html),
  [fsfreeze](https://man7.org/linux/man-pages/man8/fsfreeze.8.html), and
  [ext4 journal](https://docs.kernel.org/filesystems/ext4/journal.html) define
  the referenced block ordering and filesystem coordination behavior. Version FS
  remote durability and common Generation capture are requirements of this RFC.
- [RFC-001: Version FS Architecture](001-version-fs.md) defines the inherited
  semantics and invariants, including the deferred global-checkpoint boundary.
- [S2 streams](https://s2.dev/docs/concepts/streams),
  [appends](https://s2.dev/docs/concepts/appends), and
  [concurrency control](https://s2.dev/docs/concepts/concurrency-control) inform
  ordered records, atomic batches, acknowledged ranges, and conditional tails.
- [Amazon S3 consistency](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html#ConsistencyModel)
  and [conditional writes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html)
  inform the immutable object adapter.
- [S3 object-version deletion](https://docs.aws.amazon.com/AmazonS3/latest/userguide/DeletingObjectVersions.html)
  informs only the future reclamation boundary; v0 issues no physical DELETE.
- [toy-kv-engine](https://github.com/ben1009/toy-kv-engine), its
  [point-transaction implementation](https://github.com/ben1009/toy-kv-engine/blob/0e69b0ef7cb2ff29c4bbfd116a2cdbe18a806d8e/kv-engine/src/mvcc/txn.rs),
  and [read-only snapshots](https://github.com/ben1009/toy-kv-engine/blob/0e69b0ef7cb2ff29c4bbfd116a2cdbe18a806d8e/kv-engine/src/mvcc/snapshot.rs)
  inform the reference adapter. The cited revision separates serializable point
  transactions from snapshot scans; the adapter's conformance remains a
  release gate.
- [PostgreSQL transaction isolation](https://www.postgresql.org/docs/current/transaction-iso.html)
  and [WAL settings](https://www.postgresql.org/docs/current/runtime-config-wal.html)
  inform the compatible later adapter, not an initial PostgreSQL requirement.
