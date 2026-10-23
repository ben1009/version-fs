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
- Persistent copy-on-write metadata trees and fixed-size file chunks.
- A transactional Metadata Authority for publication, references, fencing, and
  retention, with toy-kv-engine as the first backend.
- One active writer per branch.
- A native SDK and CLI, with managed local directory materialization.
- Native ordered logs using S2 as the semantic reference.
- Explicit retention, pins, reader protection, and mark-only reachability reports.

A successful demonstration is:

```text
worker A: materialize G0 -> edit files + append events -> checkpoint G1
                                                               |
                                      fork candidate branches --+
                                                |
                                       checkpoint candidate G2
                                                |
                                      conditionally promote G2
                                                |
worker B: empty cache -> open G2 -> read files, logs, and artifacts
```

Worker A can be destroyed after successful publication. Worker B needs neither
its disk nor any ancestor Generation to resolve G2.

This RFC selects mechanisms for v0. RFC-001 remains the authority for system
semantics; implementation shortcuts cannot weaken its invariants. Numerical
defaults below are initial Version FS choices, not claims about vendor limits
or achieved performance.

## 2. Release scope

### 2.1 Required capabilities

| Area | MVP behavior |
| --- | --- |
| Objects | Typed SHA-256 identity, verified reads, idempotent uploads, workspace-local sharing. |
| Metadata | Persistent namespace, file-chunk indexes, log indexes, and root maps. |
| Authority | Backend-independent transactional contract, implemented by `ToyKvMetadataAuthority` for the MVP. |
| Generations | Complete immutable roots; branch-attached and detached publication; operation outcome lookup. |
| Filesystem | Regular files, directories, symlinks, declared metadata, range reads, full local materialization. |
| Branches | Create/fork, checkpoint, conditional head movement, rollback, promote, delete, writer fencing. |
| Logs | Atomic durable batch append, conditional tail, receipts, historical reads, live cursors, retention-safe trim. |
| Artifacts | Named immutable payloads and explicit log attachments, sharing the object layer. |
| Git | Capture and restore a self-contained repository directory, including `.git`. |
| Recovery | Fresh-worker reads and materialization; interrupted operations resolved from durable authority. |
| Retention | Recent-Generation retention, explicit pins, protected workspace bases and readers. |
| GC report | Snapshot retained/protected roots, traverse reachability, and report observed unreachable objects without deleting bytes. |
| Tooling | SDK and CLI for the workflow in Section 1; structural namespace diff. |

### 2.2 Deferred capabilities

The MVP does not implement FUSE, virtio-fs, a Git server or smart protocol,
semantic merge, concurrent writers on one branch, physical object deletion,
online reclamation, cross-region writable branches, compression, content-defined
chunking, automatic autosave, or a remote mutation WAL for local file writes.
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
                     SDK / CLI / managed command runner
                                  |
                   materialized directory + local cache
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
unnecessary for v0. Large artifacts use the file-chunk representation. Object
listing is useful for inventory and GC, never for discovering published
Generations.

### 4.2 Persistent maps

The MVP uses a persistent ordered B+ tree with nodes of at most 64 KiB. Mutations
copy affected leaves and ancestors; insertions split nodes by encoded size.
Rebuilding or repartitioning the entire map on every checkpoint is prohibited.
Stable unchanged nodes retain their object identities.

| Map | Key | Value |
| --- | --- | --- |
| Filesystem namespace | Normalized relative path | Entry type, metadata, and content reference. |
| File chunks | Chunk number | Chunk reference and actual length. |
| Logs | Log name | Stable log ID, visible sequence range, and segment-index root. |
| Segment index | Batch starting sequence number | Exclusive end and sealed-segment reference. |
| Artifacts | Artifact name | File/blob representation and declared dependencies. |

The namespace contains explicit directory entries, including empty directories;
the root directory is explicit as well. Prefix iteration implements directory
listing. A directory rename changes the keys of its descendants and can cost
proportional to that subtree in v0. File-chunk and log indexes are trees, not
ever-growing arrays embedded in one manifest object.

Diff skips equal object identities and compares differing ranges. Node splits
can change tree boundaries; a sorted merge fallback preserves correctness.
The worst case visits the complete compared namespace.

### 4.3 File and artifact contents

| Encoded content size | Representation |
| --- | --- |
| Up to 64 KiB | One immutable blob. |
| Larger | Fixed 4 MiB chunks, a possibly shorter final chunk, and a persistent chunk index. |

A file manifest records its logical length and representation. Empty files
have a defined empty blob. Content beyond the logical end is never exposed.
A range read fetches and verifies each intersecting complete chunk before
returning the requested slice. Partial unverified object reads are not exposed.

A small overwrite in a large file can reuse all unaffected chunks. An insertion
that shifts later fixed boundaries can rewrite the suffix; this tradeoff is
accepted. Sparse holes are materialized as bytes; preserving physical sparseness
is deferred.

### 4.4 Complete Generation descriptor

```text
GenerationDescriptor {
    format_version,
    generation_id,
    parent_generation_id?,   // lineage, not a required object edge
    filesystem_root,
    logs_root,
    artifacts_root,
    git_view: captured_repository_files | none,
    created_at,              // informational, not a global version coordinate
}
```

Generation IDs are opaque UUIDs allocated once per checkpoint operation.
The descriptor is itself an immutable checksummed object. Its root references
resolve complete current state. No descriptor refers to a parent as the source
of missing file entries, chunks, or log records.

A Metadata Authority catalog entry binds the Generation ID to its descriptor
object and publication result. Uploading the descriptor creates no visible
Generation.

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
| Branch | Stable ID, unique live name, head Generation, revision, writer epoch/lease, stream epoch, live log-map root. |
| Name / retention indexes | Point-key name bindings and per-Generation eligibility guards, updated atomically with their referenced records. |
| Generation | Descriptor reference, publication operation, retention deadline, lifecycle state. |
| Operation | ID, request fingerprint, phase, allocated identities, prepared dependencies, terminal result. |
| Protection | Pins, reader leases, workspace bases, and preparation roots. |
| GC report | Report ID, root snapshot/time, inventory window, completeness, and candidate summary/pages. |

These are logical records encoded as versioned keys and values. Branch IDs,
workspace IDs, and stream epochs are never reused. Deleting and recreating a
branch name creates a new branch ID. Every head or live-view mutation increments
its state revision, including durable changes to its live log map. Lease-only
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
authoritative mutation, including writer lease, base-protection, and preparation
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

The SDK serializes data/head mutations for one branch and permits one checkpoint
or log-mutation preparation at a time there. Writer/base-protection renewals,
preparation heartbeats, and operation query/abort remain available during a
long preparation or a closed workspace gate. Server-side CAS and fencing remain
required even if clients violate this convention. Different branches proceed
independently.

## 6. Managed filesystem workspace

### 6.1 Supported state

The materialized adapter initially targets Linux. Paths are case-sensitive,
normalized relative UTF-8 paths; components are at most 255 bytes and paths at
most 4096 bytes. Empty interior components, NUL, `.` and `..` are rejected.
The root has its own distinguished key. The adapter never follows a symlink
while walking a captured tree.

The supported metadata is entry type, file length, permission bits `0777`,
modification time, and symlink target. Restore creates entries under the current
user. Owner IDs, ACLs, extended attributes, special permission bits, devices,
sockets, FIFOs, and hard-linked regular files are unsupported. Capture rejects
such state explicitly instead of silently changing it. Access time and inode
identity are outside the declared view.

Symlink targets are stored as text, not imported dependencies. The materializer
uses directory-relative operations without following existing links and writes
into a fresh staging directory. It never writes through a captured symlink.
An external symlink target remains external after restore. Managed commands
can follow it to host paths permitted by their operating-system credentials;
the no-follow capture/materialization rules do not confine command execution.

### 6.2 Capture boundary

The writable workspace is a local directory associated with a protected base
Generation and a writer grant. Its files are mutable overlay state even when
they were originally materialized from immutable objects.
Local control records and caches live outside the captured directory so grants,
pending-operation state, and cached objects do not become workspace contents.

The managed runner coordinates execution and checkpoint boundaries. It is not
a security sandbox. Managed commands are trusted with the worker filesystem
under their operating-system credentials. Accesses through external symlinks
or absolute host paths, and other external effects, are outside captured
Version FS state. The gate does not provide process or filesystem containment;
a sandboxed runner is a future adapter.

The managed runner admits write commands and SDK file mutations through one
workspace gate. A checkpoint closes that gate, waits for admitted commands and
their write-capable descendants to finish, drains log requests, and prevents
new file mutations and appends until the outcome is resolved. Long-lived
background writers are unsupported. An imported directory requires an explicit
caller quiescence contract; the scanner cannot infer that contract from stable
file sizes or timestamps.

Once all producers are quiesced, the service captures the branch revision and
live log-map root, then scans the directory while the gate remains closed.
Consequently files, artifacts, and the selected log ranges describe the same
interval without admitted mutation. An unsupervised write invalidates the
capture contract; observed changes during the scan cause capture to fail.
A successful scan is not presented as proof that arbitrary external writers
were absent.

v0 keeps the gate closed through upload and publication. It does not implement
concurrent next-overlay writes during checkpoint. On a lost reply it queries or
aborts the original operation before reopening the gate. On rejection the local
directory remains available for explicit salvage; it is never cleared merely
because a lease or CAS check failed.

This boundary is not an application database snapshot. A live database directory
requires that database's export or quiescence protocol before capture. Process
memory, open connections, and external side effects are not captured.

### 6.3 Initial directory import

`import_directory` attaches an existing worker-local directory as a managed
workspace; it does not publish its contents. Its SDK inputs are:

```text
import_directory {
    operation_id,
    branch_id,
    expected_revision,
    grant,
    source_directory,
    quiescence_ack,
}
    -> ManagedWorkspaceHandle { local_handle_id, branch_id, base_generation,
                                base_protection_id }
```

`source_directory` is resolved by the worker SDK, not opened as a path on the
remote service. The caller holds it quiescent through setup and agrees to route
subsequent writers through the managed gate. The SDK verifies the directory
binding, records the operation and worker/directory identity outside that
directory, and obtains a protected base under the current branch revision and
grant. It atomically installs the local control record before returning a
usable handle. A matching retry reconciles the recorded protection/result and
returns the same local handle; it cannot substitute a different worker/directory
identity under the old operation ID. Replay does not renew an expired base
protection.

The worker SDK owns `base_protection_id` while its managed workspace handle is
open. It renews the protection every minute with a five-minute expiry. A renewal
uses its own operation ID; replay returns that renewal's original expiry and
does not extend it again. `close_workspace` quiesces the local gate, resolves or
definitively aborts any active capture, then releases the base protection. It
does not publish or delete the branch. If a worker disappears, the protection
expires. Expired protection is no longer a retention root and the authority
removes it during ordinary cleanup; an abandoned import therefore cannot
retain its Generation indefinitely.

If renewal has an unknown outcome, the SDK retries that same renewal ID or
queries its result while preserving local files. After the protection expires,
the old handle cannot checkpoint against its base. Rebinding requires a new
`import_directory` operation under current branch preconditions. If the branch
head moved, the caller must explicitly choose whether to import onto that head
or salvage the old local work; the SDK never silently changes the base.

The imported directory supplies the complete local filesystem view. Relative
to a nonempty base, absent files are deletions at the next checkpoint. Import
does not reset live logs or the named-artifact view. Its result acknowledges
the local binding and base protection, not remote durability of file bytes:

```text
create_workspace -> empty G0
acquire_writer
import_directory("/repo", quiescence_ack) -> managed local workspace
checkpoint(managed local workspace)      -> durable G1
```

Capture validates the supported state before publication. Host loss before
checkpoint may lose all imported file changes. Alternatively, `materialize`
exports a protected Generation to a fresh directory; `import_directory` binds
that directory to a branch before managed writes. The CLI may compose these
calls.

### 6.4 Change discovery and materialization

The first scanner enumerates all entries and hashes all regular-file content.
It compares the result with the protected base and updates only differing
manifest paths and chunks. Stat information alone is not trusted as evidence
that bytes are unchanged. Thus scan I/O can be proportional to the whole
workspace even when new stored bytes are small.

`open_generation` and native path/range reads are lazy. They fetch the descriptor,
required tree nodes, and demanded chunks. Full directory materialization is an
explicit export: it enumerates and fetches the entire namespace. A local
directory does not acquire lazy paging merely because the SDK supports lazy
reads. FUSE and sparse writable materialization are deferred.

### 6.5 Git and artifacts

The MVP's Git authority is captured repository files. A quiesced, self-contained
`.git` directory is captured with the worktree and restored byte-for-byte under
the filesystem metadata contract. Native Git object/ref translation is absent.
Git commands run normally in the materialized directory between checkpoints.

The adapter rejects Git alternates and external worktree administrative
directories. Submodule gitlinks and other external repositories are not imported
automatically. A declared self-contained repository must include the bytes
required for the Git operations promised by that repository view. No Generation
is automatically converted into a Git commit.

Named artifacts use the same blob/chunk objects as files. Adding or removing a
named artifact is a gated local overlay mutation captured by checkpoint. Log
attachments are explicit object edges in the appended batch; putting a digest
inside opaque log text does not retain that digest's object.

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

After the workspace gate drains appends, a checkpoint binds the existing live
log-map root. It does not recopy, reseal, or replay historical segments. Because
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

The service owns the trusted object builder. It hashes and validates bytes,
checks workspace ownership and encoding, and records durable upload results.
A client assertion that it uploaded a root is not sufficient.

Preparation registers its base Generation, captured live log root, and staged
objects as protected dependencies. A new manifest node is accepted only if
every child is either:

- A verified durable object built in the same protected preparation.
- A shared subtree reachable from an already validated, protected base or log
  root, selected by the trusted builder.

Arbitrary object IDs supplied by a client are not proofs of protected
reachability. Reusing otherwise unprotected content requires a complete verified
read or upload during preparation. Validation proceeds bottom-up over new
nodes; it need not traverse every unchanged protected subtree on each checkpoint.
The prepared root and its validation dependencies are recorded durably before
the final transaction. Immutable keys and the no-deletion backend preserve
verified bytes; preparation protections track the dependencies until commit
or a definite abort. No collection admission barrier is needed in v0.

### 8.2 Capture sequence and preparation lifetime

1. Record the operation ID and capture token locally. Close and drain the
   workspace gate while maintaining the existing base protection and writer
   lease. Bind retries to this same frozen capture; if it is lost, abort it.
2. Register a preparation, allocating its fixed Generation ID and validating
   branch identity, revision, head, and writer epoch. In that transaction,
   protect the base and exact live log root. A stale precondition rejects the
   capture rather than silently changing it.
3. Scan the quiesced directory, collect named artifacts, and build the persistent
   complete state.
4. Wait for durable completion of all new data and metadata objects. Seal the
   prepared root and upload its Generation descriptor.
5. Request attached or detached publication using the recorded preparation.

Every protected preparation that spans remote object I/O has a renewable
ten-minute inactivity deadline. This shared contract covers checkpoint capture
and log mutations, including append, create, trim, and delete. An operation may
declare a shorter non-renewable lifetime; otherwise this default applies. The
SDK sends `renew_preparation` every three minutes for renewable preparations,
with a fresh renewal operation ID, the prepared operation ID, and its exact
writer grant. Object upload traffic alone does not extend the deadline. For
append, the prepared operation ID is `append_id`. The authority extends the
deadline only while the preparation is nonterminal and the grant, lease, branch
epoch, and owner incarnation remain valid. Replaying one renewal ID returns its
original expiry. Renewals cannot revive an aborted operation or a fenced writer.

If renewal has an unknown outcome, the SDK retries the same renewal ID or
queries it before creating another renewal. It pauses further scan and object
uploads and final publication/root replacement while the result is unresolved,
and preserves all preparation protections. A checkpoint keeps the workspace
gate closed; a log mutation keeps its branch admission and protected roots. A
definite expiry, abort, or fencing result prevents the final authoritative
mutation. The SDK resolves the prepared operation and its protections before
release; a checkpoint preserves its local directory for explicit salvage, an
append returns no receipt, and other log mutations return no updated live root.
Newly uploaded objects not referenced by other state remain stored as
preparation garbage. The authority orders expiry/abort and final publication in
its metadata transactions: a committed result remains committed, while an
aborted preparation cannot later publish or replace a live root. A deadline
permits the authority to record an abort; it is not permission to guess an
unknown transaction outcome or release unresolved protection.

### 8.3 Publication transaction

The final transaction reads and validates the branch, authority incarnation,
operation, and retention/index keys under Section 5.1's contract. It checks
exact preconditions, the current grant, sealed root proof, and preparation
state, then atomically:

1. Creates the published Generation catalog entry and descriptor binding.
2. Establishes its initial retention deadline.
3. For attached publication, moves the branch head, advances its revision, and
   moves the managed workspace's base protection to the new Generation.
4. Records the immutable operation result and closes preparation protection.

For detached publication, step 3 is absent. The same captured state becomes
visible and retained by Generation ID while the branch head remains unchanged.
The captured live log map is not reset. Detached publication does not rebase the
local branch baseline; a later head adoption is a separate conditional operation.

Before commit, the old authoritative state remains in force. After commit, the
Generation is published even if the service crashes before replying. A rejected
transaction publishes no Generation and makes no partial head movement.

After an attached success, the worker atomically records and syncs its new base
Generation locally before reopening its mutation gate. Recovery of a lost reply
uses the operation result, not a comparison with the current branch head. The
head may have moved again since the operation committed.

## 9. Branch lifecycle and reference operations

### 9.1 Create and fork

Workspace creation installs an empty Generation and a `main` branch in one
identified operation after the empty descriptor graph is durable. Branch
creation takes a retained base Generation and returns a new stable branch ID.
The transaction verifies retention, copies only the head and live log-root
references, establishes branch protection, and records the result.

The branch starts without a writer grant. Retrying branch creation after a lost
reply returns the original branch ID, even if its name has since changed or
been reused. Branch creation does not enumerate the base namespace, segments,
or named logs.

### 9.2 Adopt, roll back, and promote

`move_head` adopts a retained Generation; rollback uses that same operation to
select older state. Promotion takes a source branch/revision and a destination
branch/revision. In one transaction it validates both references and selects the
source's published head as the destination head. The source's uncheckpointed
live logs and local overlay are not promoted.

Head reset replaces the destination live log-map root with the selected
Generation's log map, advances its branch revision, stream epoch, and writer
epoch, and revokes its old writer grant. If that destination has live log
changes beyond its head, the caller must explicitly set `discard_live_logs`;
otherwise the reset is rejected. CAS includes stable branch identity and
revision, preventing both name-reuse and head-away/head-back ABA.

The caller needs current destination write authority. A running local directory
is never rewritten by a remote reference update. Its old grant cannot publish;
the user must preserve/export its unpublished work or explicitly materialize
the new head and acquire a new grant.

### 9.3 Delete

Deletion validates the expected branch revision and current write authority,
revokes the writer, removes the live name and branch root, and records the
outcome atomically. Historical Generations, pins, and readers retain their own
protection. A new branch with the same name gets a different ID. Retrying the
old delete operation cannot delete that new branch.

## 10. Retention and reachability reports

### 10.1 Roots and protection

The default policy retains each newly published Generation for 24 hours.
Branch heads and explicit pins can retain it longer. Detached Generations have
the same initial protection and may be pinned without branch adoption.
Automatic hourly/daily selection and branch expiration are deferred.

Retained and protected roots include:

- Descriptors and complete graphs of retained Generations and branch heads.
- Current live log-map roots, including appends not yet in a Generation.
- Explicit pins, workspace-base protections, and reader protections.
- Prepared objects and dependency roots until their operations have definite
  outcomes or have been durably aborted and fenced.

A parent Generation ID is lineage, not a strong edge to its descriptor. Required
Git files, log attachments, segment indexes, and named artifacts are strong
edges. Reachability marking includes each retained descriptor as well as its
complete content graph.

Opening a Generation transactionally verifies publication and current retention
and creates a five-minute renewable read protection. An open mutable workspace
also protects its selected base under the worker SDK's one-minute heartbeat and
five-minute lease. Eligibility and protection acquisition are checked in one
metadata transaction; physically present bytes alone cannot make an expired
Generation eligible again. Expired workspace-base and preparation protections
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

### 11.1 Immutable reads

An authorized protected handle resolves the Generation descriptor and selected
tree paths. It does not consult branch history or fetch ancestor descriptors.
The trusted resolver serves only objects reachable from the handle's declared
view; there is no public unauthenticated `GET <digest>` shortcut.

Cache keys include workspace scope, type, format version, and digest. Downloads
land in temporary files and become cache entries only after full hash and length
verification and atomic installation. Concurrent fills share work. The cache
has a configured capacity and can evict any clean durable object.

Dirty directory bytes, unuploaded data, and unresolved capture state are not
cache entries and cannot be evicted to satisfy that capacity. A corrupt cache
entry is discarded and fetched again. A corrupt or missing authoritative object
returns an integrity error; it never becomes an empty file or an older version.

### 11.2 Worker and service restart

A fresh worker authenticates, opens a retained Generation, acquires protection,
and reads or materializes its state with an empty cache. Full materialization
builds a fresh temporary directory and records completion before making it the
workspace directory. A failed export leaves no directory labelled as a complete
restored Generation.

A surviving workspace records its branch ID, base, writer grant, and pending
operation ID. Restart first reconciles that operation. It cannot clear local
work based only on the branch's current head. A different head or epoch requires
explicit salvage or rematerialization, not automatic deletion of local changes.

The service reopens the Metadata Authority's preserved durable state, reconciles
its owner incarnation, and recovers protections and terminal operation results.
Prior-incarnation lease grants are rejected. Prepared work may be durably
aborted; a committed result remains authoritative regardless of restart.
The service never reconstructs publication by listing uploaded Generation
descriptors. If the authority is unavailable or has unresolved recovery errors,
authoritative mutations fail closed; a cached head is not a replacement.

### 11.3 Metadata recovery boundary

The operator must preserve durable metadata and any configured replication
guarantees. Keeping all object bytes does not make an old metadata backup current:
it can omit acknowledged head movements, writer epochs, and operation outcomes.
The MVP runbook keeps the service offline after such a restore until a separate
recovery procedure reconciles authority and fences obsolete writers. It cannot
silently serve the old catalog as current authority.

An older backup is not an automatic failover target. Recovery of acknowledged
data after loss of all durable metadata copies needs a coordinated backup and
object-retention design beyond this release. This does not change the ordinary
worker-loss guarantee.

## 12. SDK and CLI surface

The following specifies logical requests and results, not a final network or
Rust ABI. Each mutation uses Section 5.3's operation contract. Workspace
creation scopes its initial ID to the deployment and caller until the new
workspace ID is known; other operations use the workspace scope.

| Request | Required arguments / result |
| --- | --- |
| `create_workspace` | Operation ID; returns workspace ID and initial main branch/empty Generation. |
| `create_branch` | Operation ID, name, retained base Generation; returns new branch ID/revision. |
| `get_branch` | Branch ID or live name; returns stable ID, revision, head, stream epoch, and writer epoch. |
| `acquire_writer` | Operation ID, branch ID, expected writer epoch, owner, explicit handover policy; returns recorded grant/expiry. |
| `renew_writer` / `release_writer` | Operation ID and exact grant; renewal returns its recorded new expiry. |
| `import_directory` | Operation ID, branch ID/revision, grant, worker-local source directory, explicit quiescence acknowledgement; returns the managed local handle and protected base (Section 6.3). |
| `renew_base_protection` / `close_workspace` | Operation ID and exact managed handle/protection ID; renewal returns the recorded expiry. Close quiesces the gate, resolves capture, and releases the protection. |
| `checkpoint` | Operation ID, branch ID/revision, grant, managed capture, attached/detached mode; returns Generation and optional new branch revision. |
| `renew_preparation` | `renewal_operation_id`, `prepared_operation_id` (checkpoint operation ID, `append_id`, or log create/trim/delete operation ID), exact writer grant; returns the recorded extended preparation deadline. |
| `move_head` | Operation ID, branch ID/revision, grant, retained target, discard policy; returns new revision/epoch. |
| `promote` | Operation ID, source ID/revision, destination ID/revision/grant, discard policy; returns selected Generation and destination revision/epoch. |
| `delete_branch` | Operation ID, branch ID/revision, grant; returns deletion result. |
| `open_generation` | Operation ID and retained Generation ID; returns protected immutable handle and expiry. |
| `renew_handle` / `close_handle` | Operation ID and handle ID; rights are not renewed by replaying a prior operation. |
| `read_at` / `list_at` / `stat_at` | Handle and path, plus byte range or listing cursor where applicable. |
| `materialize` | Protected Generation handle and fresh destination; returns a complete local export, bound for writes separately by `import_directory`. |
| `diff` | Two protected handles; streams added, removed, and changed entries. |
| `create_log` / `delete_log` | Operation ID, branch ID/revision, grant, log name/identity; changes the live log map. |
| `append` | Exact stream, grant, `match_seq_num`, append ID, records and explicit attachments; returns receipt. |
| `open_live_log` | Operation ID and exact stream; returns protected root/range and expiry. |
| `read_log_at` | Protected Generation/log handle and sequence range. |
| `tail_log` | Current stream and cursor; repeats protected current-root acquisition. |
| `trim_log` | Operation ID, stream, branch revision/grant, batch boundary; returns updated range/revision. |
| `set_artifact` / `remove_artifact` | Managed local overlay operation; takes effect in a Generation at checkpoint. |
| `pin_generation` / `unpin` | Operation ID and retained Generation or exact pin ID; returns pin/result. |
| `get_operation` / `abort_operation` | Original operation ID; query or make its outcome terminal. |
| `collect` | Operator operation ID and workspace; returns mark-only report ID, root snapshot/inventory window, completeness, counts/bytes, and candidate pages. No physical deletion. |
| `get_gc_report` | Report ID and optional page cursor; returns the recorded summary/candidate page or `ReportExpired` for expired details. |

Local file mutations and artifact staging use the managed gate. They are not
remote durable mutations; retrying an interrupted local command requires the
application's own contract. The API does not promise exactly-once shell
execution or external actions.

Errors distinguish `Conflict`, `Fenced`, `LeaseExpired`, `OperationMismatch`,
`InProgress`, `GenerationExpired`, `StreamReplaced`, `CursorExpired`,
`IntegrityError`, `UnsupportedState`, `ReportExpired`, and `Busy`. A transport
timeout does not become a definite rejection. A conditional failure returns
the relevant current identity/revision when authorized.

The CLI exposes workspace creation, import/materialize, managed command
execution, checkpoint, branch/fork, rollback, promote, log append/read/tail,
artifact staging, pin/unpin, diff, operation status, and mark-only GC reports.
Machine-readable output includes stable identities and operation IDs. There
are no fake mount or Git-server commands before those adapters exist.

## 13. Failure behavior

| Failure point | Authoritative result and required recovery |
| --- | --- |
| Crash before preparation is registered | No publication. Retry the same ID or record an abort tombstone. |
| Upload fails or its reply is lost | Preparation remains protected; verify/retry identical bytes or abort it. |
| Descriptor uploaded, transaction not committed | Descriptor is invisible preparation data; old head remains authoritative. |
| Commit succeeds, reply is lost | `get_operation` returns the original committed result, even if the branch later moved. |
| Acquire/create reply is lost | Return the original writer grant or branch ID; allocate no replacement on replay. |
| Authority owner restarts | Recover acknowledged metadata atomically, advance the owner incarnation before serving, and reject old lease grants. |
| Authority reports a definite transaction conflict | Retry metadata logic with the same operation ID; repeat no S3 or local side effects inside the closure. |
| Lease expires after closure evaluation | Revalidate at the serialization/commit decision; reject the complete write set with no partial publication, head movement, or live-root change. |
| Commit error may follow durable application | Treat the result as unknown; recover/query the operation record, never assume every backend error means abort. |
| Import setup is interrupted or its reply is lost | Reconcile the recorded base protection and local binding under the same ID; do not claim imported file bytes are remotely durable. |
| Base-protection renewal reply is lost | Retry the same renewal ID or query it; preserve local files until the result is known. If the worker stops renewing, the five-minute protection expires and is removed; the old handle cannot checkpoint until explicitly rebound by a new import under current branch preconditions. |
| Workspace closes with active capture | Quiesce the gate and resolve or definitively abort capture before releasing base protection; close does not publish or delete the branch. |
| Preparation renewal reply is lost | Retry the same renewal ID or query it. Pause further scan/uploads and final publication or live-root replacement; retain the operation's admission boundary and protections. A checkpoint keeps its workspace gate closed. |
| Preparation expires or its writer is fenced | Authority orders expiry/abort against the final commit. Once aborted, publication or live-root replacement fails; late uploads cannot make the operation visible and new bytes remain GC-report candidates. Preserve checkpoint files for explicit salvage, and return no append receipt. |
| Writer replaced during upload | Final commit rejects the old epoch. Local work is preserved; aborted staged bytes remain stored and can appear in a later report. |
| Append commits, receipt is lost | Replay returns its exact original range without another append. |
| Append races checkpoint gate | Its entire batch is captured before the cut, or it waits for the next interval. |
| Trim or reset races a live cursor | Explicit expired/replaced error; retained Generation reads remain unchanged. |
| Roots change during a GC report | The report describes its captured root boundary and inventory window; normal reader/writer operations continue. |
| GC report crashes or exhausts its budget | Resume or fail the report; never delete bytes, change retention, or place the workspace in a recovery mode. |
| Metadata authority is unavailable | No new authoritative outcome is claimed; unknown preparations are not guessed away. |
| Cache is lost or corrupt | Re-fetch and verify from S3; published state remains intact. |
| Required S3 object is missing/corrupt | Report an integrity failure; do not publish or restore partial replacement state. |
| Local host is lost before checkpoint | Uncheckpointed file and named-artifact changes may be lost. Durable accepted log appends survive under their live roots. |
| Old metadata backup is restored | Keep the service offline and reconcile authority before serving its purported published state. |

Local file write completion acknowledges local mutation only. Log append
completion acknowledges the durable log range only. Checkpoint completion
acknowledges the coherent Generation and its publication. These three promises
must remain distinguishable in both SDK results and user documentation.

## 14. Performance, bounds, and benchmarks

### 14.1 Expected costs

Let `N` be namespace entries, `B` total regular-file bytes, `S` log segments,
`V` retained Generations, and `R` the distinct graph reachable in a report.

| Operation | MVP cost and limitation |
| --- | --- |
| Directory capture | Reads `O(N + B)` for discovery; the full pause includes scan, uploads, and commit. |
| Checkpoint storage | New data chunks plus changed persistent nodes and one descriptor; no copy of unchanged payload or metadata subtrees. |
| Fork | Bounded reference/protection writes; no traversal of files, logs, or ancestor Generations. |
| Path/range read | Namespace traversal `O(log N)`, optional chunk-index traversal, and intersecting complete chunks; independent of `V`. |
| Materialize | All entries and selected bytes; storage sharing does not remove export cost. |
| Log append | New batch/attachments, `O(log S)` segment-index path changes, log-map path changes, and one metadata commit. |
| Log checkpoint | Bind the existing log-map root; no historical segment-payload transfer. |
| Diff | Shared-subtree skipping, with complete namespace traversal possible in the worst case. |
| GC report | Root snapshot, `O(R)` distinct-node marking, object inventory, and report storage; no publication pause or physical reclamation. |

Small-file object count, fixed-chunk suffix rewrites, full scans, per-batch S3
latency, reference authority capacity, unreclaimed object bytes, and lifetime
operation-result growth are expected v0 limits.
This release proves correctness and sharing; it does not claim production
throughput for millions of concurrent agents.

Backend-internal commit serialization and durable-write cost are measured
separately from domain-level branch conflicts. The toy-kv reference adapter
does not imply hosted-service throughput or multi-node scalability.

### 14.2 Bounded resources and overload

The default transfer pool permits eight object operations per workspace, with
a service-wide cap of 32. The service queue holds at most 64 admitted requests;
per-branch SDK mutation admission is one active request. These are configurable
initial limits. Tree traversal and scanning stream results instead of retaining
the complete namespace or file in RAM. Cache and temporary-disk budgets are
explicit deployment settings; report marking spills within its disk budget.

Before admission, exhaustion returns `Busy` or an explicit capacity error.
After admission, the operation must reach a recoverable state or a recorded
abort. It cannot drop an accepted append, evict dirty working bytes, release an
unresolved operation's roots, bypass a checksum, or acknowledge a weaker
durability mode. A full metadata store cannot be worked around by forgetting
idempotency results. A report that cannot finish within its resource budget
returns an incomplete/failed outcome without blocking workspace mutations.

These bounds cover working resources and admission, not a bounded lifetime
storage footprint. Both unreclaimed S3 bytes and permanent authority operation
records can grow with cumulative mutations. Trimming logs or expiring
Generations does not remove either class in v0. Capacity limits may reject new
work; they cannot erase deduplication history or enable deletion to make room.

### 14.3 Benchmark report

The release includes a reproducible report on real S3 and
`ToyKvMetadataAuthority` using preserved metadata storage. PostgreSQL benchmarks
are deferred with that adapter. Scenarios below are separate experiments, not
one required Cartesian product. Record backend/engine revision, WAL/durability
settings, hardware, region, network, service limits, format version, retention
settings, cache size, and raw sample counts.

| Scenario | Initial fixture / comparison |
| --- | --- |
| Namespace scale | 1,000, 100,000, and 1,000,000 files, including a high-fanout directory. |
| Large content | 1 GiB and 100 GiB files; overwrite one aligned range versus insert near the beginning. |
| Small change | No changes, one small-file edit, 1% changed files, subtree rename, and mass deletion. |
| Version depth | 10, 100, and 1,000 Generations on a small fixed workspace; compare open/read request counts. |
| Fan-out | 1, 10, and 100 branches sharing one base; fork, independently checkpoint, and drop candidates. |
| Logs | 100,000 1 KiB records; batches of 1, 100, and 1,000 records; capture, fork, and trim. |
| Cold recovery | Empty cache versus warm cache; one-file native read versus full materialization. |
| Reachability report | Shared/divergent retained roots, abandoned preparations, concurrent publication, and interrupted reports. |
| Lifetime metadata | Operation-result keys/bytes after repeated appends, trims, and Generation expiry; verify records remain replay-safe. |
| Authority contention | Independent branches versus one hot branch; measure transaction conflicts, retries, and durable commit latency through the common interface. |

Measure capture pause and total completion separately; bytes scanned, hashed,
uploaded, downloaded, and newly stored; data and manifest object counts;
metadata commits and retained operation keys/bytes; peak RAM/disk; and report
duration/resource use alongside ongoing checkpoint and append latency.
Report latency distributions with sample counts and achieved append/checkpoint
throughput. Single-stream and independent-branch throughput are separate.
Always include errors and saturation, rather than omitting failed requests.

The release gates below require the structural cost properties to hold.
Latency numbers establish the first baseline; this RFC invents no measured
latency SLO. A later performance proposal can set budgets from those results.

## 15. Implementation sequence

| Phase | Deliverables | Exit evidence |
| --- | --- | --- |
| 1. Immutable state | Canonical encoding, object-store adapter, verified cache, persistent maps/chunks, descriptor reader, directory scanner/export, structural diff. | One complete fixture resolves on an empty worker without ancestor reads; unchanged nodes/chunks are physically shared. |
| 2. Authority and writes | MetadataAuthority contract/conformance fixtures, ToyKvMetadataAuthority, common key schema and point transactions, durable outcomes, writer grants, initial import, capture/publication, branch lifecycle, logs, artifact edges. | The first end-to-end workflows run through the toy-kv adapter; conflicts, lost replies, restart, and stale writers have recoverable outcomes without backend-specific protocol calls. |
| 3. Retention and recovery | Pins/read protections, mark-only reports, restart reconciliation, operator runbooks, real-backend fault campaigns, benchmark report. | Reports classify the captured graph without mutating it or pausing writers; a fresh worker restores every retained fixture after worker loss. |

Physical deletion stays outside v0 regardless of these phases' completion.
Development backends can accelerate iteration, but cannot replace real-backend
release evidence. The PostgreSQL mapping must remain compatible with the same
contract, record model, and domain transactions; implementing or benchmarking
that adapter is not an MVP completion requirement.

## 16. Acceptance criteria

### 16.1 Functional and structural gates

1. Execute `create_workspace -> acquire_writer -> import_directory -> checkpoint`
   with supported files, empty directories, symlinks, and a self-contained Git
   repository; stage artifacts and append logs before checkpoint. Capture
   rejects unsupported state. Import replay returns the original local binding
   and never substitutes for durable file capture.
2. Create attached and detached Generations. Neither descriptor upload nor
   failed CAS makes a Generation visible; detached success moves no branch.
3. Restore each retained Generation on another worker with an empty cache.
   Make ancestor descriptors unavailable in the read fixture and verify
   descendant reads still resolve entirely from their own roots. This check
   requires no physical S3 deletion by the MVP.
4. Verify a no-change capture reuses its state roots. A small aligned large-file
   overwrite stores only intersecting chunks and changed tree paths. Measure
   the full scan separately. A fork copies no payload or complete namespace.
5. Verify lookup depth/request count does not grow with Generation ancestry.
   Check namespace diff against a complete comparison, including node splits,
   deletion, directory rename, and metadata-only changes.
6. Fork candidates, checkpoint each independently, promote one with source and
   destination CAS, roll back, delete branches, and reuse names safely. Preserve
   or explicitly discard live log changes under Section 9's policy.
7. Validate append atomicity, exact receipts, conditional tails, forked stream
   identities, exact checkpoint ranges, cursor errors, and trim while an older
   Generation remains pinned. Attached artifact bytes survive with those logs.
8. Enforce workspace authorization for reads, cache use, object reuse, and
   mutations. A known digest does not grant access.
9. Verify reports mark every retained/protected closure, account for uncertain
   preparations, and state their root snapshot and inventory window. Concurrent
   publication and an interrupted report do not block new mutations. No report,
   trim, expiry, or branch deletion issues an S3 object DELETE; deletion
   permissions and lifecycle expiration remain disabled.
10. Verify capture/materialization do not follow restored external symlinks.
    Document that trusted runner commands can access their host targets and
    that those bytes and effects are outside the captured state.
11. Run the backend-neutral authority conformance fixtures against the toy-kv
    adapter: multi-key atomicity, missing-key and stale-read conflicts,
    read-your-writes, read-after-commit, durable crash recovery, and consistent
    multi-page snapshot scans. Concurrent branch-name claims must yield one
    binding through an absent-key check, without SQL-only constraints.
12. Verify Version FS protocol code depends only on the common interface and
    logical schema. Report scans never substitute for transactional predicates;
    toy-kv commit timestamps never become Version FS identities. Document the
    compatible PostgreSQL mapping for the same contract. Every later adapter
    must pass the same conformance suite before use. Implementing the PostgreSQL
    adapter is not a v0 gate.

### 16.2 Failure-injection gates

Exercise each upload and metadata transition with process termination, delayed
requests, duplicate delivery, lost replies, and retried operation IDs. Required
cases include:

- Creation and writer acquisition committing before their replies are lost;
  replay returns the original identities and never grants a second epoch.
- Import setup interrupted after base protection is established but before
  local control installation; a retry reconciles the same binding or requires
  a new import if its worker/directory is lost.
- Keep checkpoint and log-mutation preparations alive beyond ten minutes with
  three-minute heartbeats, including an append with a slow large attachment
  upload. Lose renewal replies and replay the exact operation IDs without
  extending twice. Expiry or fencing prevents Generation publication or live
  log-root replacement, and protections are not released while the outcome is
  unknown.
- Stop heartbeats for an abandoned append after staging objects and advance the
  injected clock past its deadline. Confirm the preparation is aborted, its
  dependencies cease to be protected roots, no live log root or receipt is
  created, and the mark-only report can list new unreferenced bytes.
- Lose a base-protection renewal reply and replay its exact ID; stop the worker
  and advance the injected clock until the protection expires. The old handle
  cannot checkpoint or silently rebase, and an explicit close releases the
  protection immediately after its gate and capture are resolved.
- Publication/append committing before a lost reply, followed by later branch
  movement, reset, or name reuse; outcome recovery still returns the original
  result. An abort tombstone wins against a delayed request or returns the
  already committed result.
- Writer expiry and handover during preparation; a delayed old writer cannot
  publish, append, trim, or move a reference.
- A lease is valid on the first transaction attempt, which conflicts; advance
  the injected clock past expiry before retry. The next attempt must read time
  again and reject a new mutation or renewal under the expired grant.
- Evaluate a lease-dependent mutation while its lease is valid, then advance
  the injected clock past expiry after the closure returns but before the
  serialization/commit decision. The commit-time check rejects the complete
  transaction with no partial writes. Also verify a transaction that linearizes
  before expiry remains committed if its durable acknowledgement is delayed.
- Authority crash before and after durable commit, a conflict during a retried
  transaction closure, and an error after possible commit. Recover one complete
  metadata outcome through the adapter; old-incarnation grants stay fenced.
- Snapshot loss before a report's root set is sealed; report failure is explicit
  and does not silently combine prefix pages from different boundaries.
- Appends racing the capture gate; no batch is split or assigned to the wrong
  checkpoint interval. Local mutation admission stays closed until capture has
  a definite outcome.
- Reader acquisition, pin changes, and publication racing a mark-only report;
  root snapshots and uncertainty are explicit, and current operations proceed.
- Report interruption, missing reachable metadata, or exhausted report storage;
  incomplete results never become deletion authorizations or a workspace outage.
- Missing/corrupt chunks, indexes, and descriptors; no partial successful
  restore or fallback to older contents.
- Exhausted buffers/disk, unavailable S3, and unavailable metadata authority;
  no weaker success acknowledgement and no loss of required protection.

Fault campaigns must check both API outcomes and the persisted state graph;
a CLI exit code alone is not sufficient evidence. Deterministic fault injection
should cover boundaries that are difficult to trigger reliably on real services.

### 16.3 Invariant coverage and completion

| RFC-001 invariants | MVP evidence |
| --- | --- |
| I1-I3: immutable, complete, independent state | Encoding/graph validation, independent restore with ancestor reads denied, sharing and diff fixtures. |
| I4-I5, I8: durability and atomic publication | S3 plus toy-kv authority upload/commit failures, conformance evidence, detached publication, CAS, lost-reply recovery. |
| I6, I14: retention and log safety | Atomic retention/protection, closure marking and report races, trim with retained historical reads, and no physical deletion. |
| I7, I11: authority and retry | Point-transaction conflicts, owner restart/handover/expiry, ABA fixtures, identified mutation retries and abort tombstones. |
| I9: disposable workers | Destroy all worker state after publication and recover on a fresh worker. |
| I12-I13: authorization and durable ordered append | Workspace authorization/data scoping, atomic batches, conditional tails, receipts and attachment retention; no runner sandbox claim. |
| I10, I15: explicit participant/global boundaries | No runtime/DB/global-checkpoint support is claimed; no synthetic timestamp-based consistency assertion. |

The MVP is complete when all required capabilities and gates have evidence,
the real-backend benchmark report is published, and operator instructions cover
backup assumptions, unknown outcomes, trusted runner/import behavior, mark-only
report limits, toy-kv's storage/availability boundary, and cumulative metadata
growth. Physical reclamation, the PostgreSQL implementation, and other deferred
integrations are not release blockers.

## 17. Selected tradeoffs

| Decision | Reason and accepted cost |
| --- | --- |
| Transactional MetadataAuthority | Keep publication, fencing, and retry logic independent of the backend; explicit point-key indexes and a reusable conformance suite are required. |
| Toy-kv reference backend | Exercise OCC, crash recovery, and faults in the first implementation; preserve its metadata volume and make no production HA/replication commitment. |
| Compatible later PostgreSQL backend | Reuse managed database infrastructure through the same contract before building a replicated authority; implementation and production deployment qualification are deferred. |
| S3 immutable object graph | Portable worker recovery and sharing; small objects and each synchronous append incur remote request cost. |
| Persistent ordered trees | Selective reads, range iteration, and metadata sharing; tree encoding and mutation correctness require care. |
| Fixed chunks | Simple, measurable range reuse; insertions can rewrite suffixes. |
| Full quiesced scan | A clear coherent boundary; checkpoint pauses and scan work can follow total workspace size. |
| Captured Git repository files | Normal local Git tooling works without a server adapter; no native Git protocol or object-format translation. |
| One sealed segment per batch | Append durability and checkpoint capture are simple; tiny batches amplify S3 objects and requests. |
| Mark-only GC reports | Validate reachability without a deletion protocol or collection pause; unreferenced object bytes remain stored and storage grows. |
| Workspace-lifetime operation results | Preserve mutation IDs for replay-safe idempotency; every append and other mutation adds permanent metadata even after trim/expiry. High-rate, long-lived workloads need more authority storage; bounded deduplication is deferred. |
| Trusted managed runner | Coordinate capture without building a sandbox; commands retain host-filesystem access under their OS credentials, including through external symlinks. |

Future changes can replace these mechanisms while preserving RFC-001's
semantics. Safe physical reclamation, a multiplexed log WAL, tracked change
discovery, or runtime and GTS integration require additional proposals rather
than hidden extensions to v0's guarantees.

## 18. References

Primary sources consulted on 2026-10-23; Version FS formats, protocols, defaults,
and release gates above are proposals of this RFC, not vendor guarantees.

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
