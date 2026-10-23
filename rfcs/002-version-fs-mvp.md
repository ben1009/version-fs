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
- PostgreSQL transactions for publication, references, fencing, and retention.
- One active writer per branch.
- A native SDK and CLI, with managed local directory materialization.
- Native ordered logs using S2 as the semantic reference.
- Explicit retention and a conservative, blocking mark-and-sweep collector.

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
| Generations | Complete immutable roots; branch-attached and detached publication; operation outcome lookup. |
| Filesystem | Regular files, directories, symlinks, declared metadata, range reads, full local materialization. |
| Branches | Create/fork, checkpoint, conditional head movement, rollback, promote, delete, writer fencing. |
| Logs | Atomic durable batch append, conditional tail, receipts, historical reads, live cursors, retention-safe trim. |
| Artifacts | Named immutable payloads and explicit log attachments, sharing the object layer. |
| Git | Capture and restore a self-contained repository directory, including `.git`. |
| Recovery | Fresh-worker reads and materialization; interrupted operations resolved from durable authority. |
| Retention | Recent-Generation retention, explicit pins, protected workspace bases and readers. |
| GC | Exclusive collection barrier, complete reachability traversal, grace period, conservative interruption behavior. |
| Tooling | SDK and CLI for the workflow in Section 1; structural namespace diff. |

### 2.2 Deferred capabilities

The MVP does not implement FUSE, virtio-fs, a Git server or smart protocol,
semantic merge, concurrent writers on one branch, online GC, cross-region
writable branches, compression, content-defined chunking, automatic autosave,
or a remote mutation WAL for local file writes.

Process-memory snapshots, application database snapshots, GTS adapters, and
coordinated global checkpoints remain the extension in RFC-001 Section 18.
The MVP neither labels a Generation with a fabricated global timestamp nor
claims a timestamp alone makes file and database state consistent. Its branch
IDs are not a substitute for coordinated `history_id` identities.

Automatic metadata failover, restoration from an older metadata backup, and
automatic collector handover are outside the initial service. Their safety
requirements are described where they affect normal operation.

## 3. Components and deployment

```text
                     SDK / CLI / managed command runner
                                  |
                   materialized directory + local cache
                                  |
                     Version FS service and builders
                       /                       \
              immutable object I/O        short transactions
                       |                       |
                  S3 Standard              PostgreSQL
                data + manifests        refs + outcomes + roots
```

The implementation uses Rust for the core library, service, SDK, and CLI.
One service deployment initially owns one metadata database and an S3 prefix.
Multiple independent branches can prepare objects concurrently. Branch-local
transactions serialize that branch; normal operations take no exclusive global
commit lock. Bulk object transfer workers are separate from metadata execution.
Scaling the API tier and partitioning authority are later work.

PostgreSQL is Version FS control metadata, not the application database described
in RFC-001. Its state is as necessary for recovery as the S3 objects.
Authoritative reads use the writable primary, not an asynchronously lagging
replica. All control transactions use durable WAL with `fsync=on` and
`synchronous_commit=on`; see PostgreSQL's
[commit durability settings](https://www.postgresql.org/docs/current/runtime-config-wal.html).
If a deployment enables failover, the promoted database must preserve all
acknowledged commits through synchronous replication and fencing. An
asynchronous replica cannot silently replace that contract.

The supported durability boundary survives loss of workspace workers, service
processes, and their caches. The metadata database must run on storage independent
of those workers. Losing the only durable metadata copy is a disaster, not an
ordinary worker-recovery path.

S3 is accessed through an object-store interface, but real AWS S3 is the release
backend. An emulator or another S3-compatible service is a development backend
until its behavior passes the same conformance checks. AWS documents strong
consistency for object PUT, GET, DELETE, and listing in its
[consistency model](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html#ConsistencyModel).
This does not create a transaction spanning S3 and PostgreSQL.

The deployment initially has one trusted operator and workspace-scoped client
credentials. Clients cannot directly overwrite or delete stored objects. Only
the collector has delete permission. Cross-tenant deduplication and an identity
provider integration are deferred; workspace authorization is still enforced.

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

A SQL catalog entry binds the Generation ID to its descriptor object and
publication result. Uploading the descriptor creates no visible Generation.

## 5. Control metadata and operation identity

### 5.1 Authoritative records

| Record | Required state |
| --- | --- |
| Workspace | Stable ID, access scope, retention policy, collection mode and epoch. |
| Branch | Stable ID, unique live name, head Generation, revision, writer epoch/lease, stream epoch, live log-map root. |
| Generation | Descriptor reference, publication operation, retention deadline, lifecycle state. |
| Operation | ID, request fingerprint, phase, allocated identities, prepared dependencies, terminal result. |
| Protection | Pins, reader leases, workspace bases, and preparation roots. |
| GC epoch | Barrier state, captured roots, candidate and deletion progress. |

These are logical records; the SQL schema may normalize them. Branch IDs,
workspace IDs, and stream epochs are never reused. Deleting and recreating a
branch name creates a new branch ID. Every head or live-view mutation increments
its state revision, including durable changes to its live log map. Lease-only
renewal does not change this revision. Writer ownership changes advance the
writer epoch. Comparing only head Generation IDs is insufficient for CAS.

Normal transactions acquire a shared lock on the workspace admission record,
check its collection mode, and lock affected branch/operation records. Shared
admission locks permit independent branches to proceed concurrently. The
collector takes an exclusive admission lock to change modes. PostgreSQL's
[row locking](https://www.postgresql.org/docs/current/explicit-locking.html)
provides the mechanism; lock ordering is workspace, branch IDs in sorted order,
then operation/protection records. No SQL transaction remains open across S3
I/O. Deadlock or serialization retries preserve the same operation ID.

### 5.2 Idempotent mutations

Every authoritative mutating SDK operation carries an opaque `operation_id`,
scoped to its workspace and authenticated caller. An append uses `append_id` as
its operation ID, not as a second deduplication mechanism. The caller durably
records the ID before sending a request.

```text
ABSENT -> PREPARING -> COMMITTED(result)
                   \-> ABORTED(reason)
```

The request fingerprint binds the operation kind, target identities, declared
preconditions, and logical payload. Checkpoint preparation binds its capture
request first; sealing records the final immutable root once, rejecting a
different root on retry. Matching retries return the recorded result or resume
that preparation. Reusing an ID with another request is `OperationMismatch`.
After matching the fingerprint, terminal-result lookup precedes checking the
preconditions of a new mutation. A replay does not fail merely because the
head, name, or grant has changed since its original success.

Terminal results and request fingerprints remain for the workspace's lifetime.
Compact records may replace large preparation details, but IDs never become
executable again. Results include the created branch ID, writer grant,
Generation, append range, pin, or resulting revision as applicable.

`get_operation(id)` reads authoritative state. A timeout means unknown outcome.
Even `NotFound` does not prove the original request cannot arrive later: retry
the same ID, or atomically record an abort tombstone before abandoning it.
`abort_operation(id)` creates that tombstone if absent, aborts a preparation, or
returns an already committed result. It cannot undo a committed mutation.

An old successful result does not extend a lease or retain its objects forever.
The caller can learn that an operation committed while its returned read lease,
writer grant, or historical Generation has since expired. Obtaining new rights
requires a new operation and current preconditions.

### 5.3 Writer grants

The first implementation permits one active grant per branch. A new acquisition
locks the branch, validates the expected epoch and ownership policy, advances
the epoch, and records a grant with a 30-second renewable lease. Renewal is
normally attempted every 10 seconds. Expiration is evaluated using primary
database time after acquiring the lock, never the worker's clock.

Acquisition has its own operation ID. A lost acquisition reply followed by a
retry returns the same grant and epoch; it must not fence that grant by creating
another one. Retrying an old renewal likewise returns its old deadline. Each
new renewal uses a new operation ID. An expired grant cannot renew itself;
reacquisition advances the epoch.

Every final authoritative mutation validates branch ID, epoch, grant, lease,
and relevant revision under the branch lock. Admission or upload under a valid
grant does not authorize a later commit after handover. An authorized forced
handover advances the epoch before granting the replacement writer. Release,
branch deletion, and head-reset operations revoke the old grant.

The SDK serializes data/head mutations for one branch and permits one checkpoint
or log append preparation at a time there. Lease renewals and operation
query/abort remain available during a long preparation or a closed workspace
gate. Server-side CAS and fencing remain required even if clients violate this
convention. Different branches proceed independently.

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
An external symlink target remains external after restore.

### 6.2 Capture boundary

The writable workspace is a local directory associated with a protected base
Generation and a writer grant. Its files are mutable overlay state even when
they were originally materialized from immutable objects.
Local control records and caches live outside the captured directory so grants,
pending-operation state, and cached objects do not become workspace contents.

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

### 6.3 Change discovery and materialization

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

### 6.4 Git and artifacts

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
and PostgreSQL. S2 is not a required dependency, and Version FS does not claim
wire compatibility or S2's latency and throughput.

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
eagerly recreating every stream record in SQL.

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
removes its map entry; it cannot erase segments retained by another root.

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

Preparation registers and protects its base Generation, captured live log root,
and staged objects before they can be exposed to collection. A new manifest
node is accepted only if every child is either:

- A verified durable object built in the same protected preparation.
- A shared subtree reachable from an already validated, protected base or log
  root, selected by the trusted builder.

Arbitrary object IDs supplied by a client are not proofs of protected
reachability. Reusing otherwise unprotected content requires a complete verified
read or upload during preparation. Validation proceeds bottom-up over new
nodes; it need not traverse every unchanged protected subtree on each checkpoint.
The prepared root and its validation dependencies are recorded durably before
the final transaction. GC admission rules preserve those proofs until commit
or a definite abort.

### 8.2 Capture sequence

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

Preparation has a renewable ten-minute inactivity deadline. Renewals cannot
revive an aborted operation or a fenced writer. A deadline permits the authority
to record an abort; it is not permission for a collector to guess the outcome
of an unreachable metadata transaction.

### 8.3 Publication transaction

The final transaction locks admission, branch, and operation records in the
order defined in Section 5.1 and checks its exact preconditions, current grant,
sealed root proof, and preparation state. It then atomically:

1. Creates the published Generation catalog entry and descriptor binding.
2. Establishes its initial retention deadline.
3. For attached publication, moves the branch head and advances its revision.
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

## 10. Retention and collection

### 10.1 Roots and protection

The default policy retains each newly published Generation for 24 hours.
Branch heads and explicit pins can retain it longer. Detached Generations have
the same initial protection and may be pinned without branch adoption.
Automatic hourly/daily selection and branch expiration are deferred.

GC roots include:

- Descriptors and complete graphs of retained Generations and branch heads.
- Current live log-map roots, including appends not yet in a Generation.
- Explicit pins, workspace-base protections, and reader protections.
- Prepared objects and dependency roots until their operations have definite
  outcomes or have been durably aborted and fenced.

A parent Generation ID is lineage, not a strong edge to its descriptor. Required
Git files, log attachments, segment indexes, and named artifacts are strong
edges. GC preserves each retained descriptor as well as its content graph.

Opening a Generation transactionally verifies publication and current retention
and creates a five-minute renewable read protection. A mutable workspace also
protects its selected base and renews that protection. Acquisition is coordinated
with GC admission; it cannot resurrect state already declared collectible.
Renewal does not change the protected root. Remote object reads have a
30-second deadline and are admitted only when the handle remains valid through
that deadline. The service drops results arriving after an abort or expiry.
Long exports renew their handles; an expired handle must be opened anew.

Releasing the last pin or deleting a branch does not immediately delete bytes.
An expired Generation cannot be reopened merely because its objects happen to
remain on S3. Operation-result records are not permanent content-retention roots;
they continue to report the original outcome after independent retention ends.

### 10.2 Blocking collector

The initial collector is manual or scheduled per workspace and uses a durable
barrier:

```text
RUNNING -> DRAINING -> COLLECTING -> RUNNING
                         |
                  RECOVERY_REQUIRED
```

1. Change admission to `DRAINING`. Block new preparations, reference changes,
   pins, and new reader/workspace protections. Already registered preparations
   may finish under their original authority; required writer/protection
   renewals remain available. Otherwise record definite aborts.
   An unavailable metadata authority or an unresolved commit outcome prevents
   the collector from proceeding.
2. When no active preparation remains, persist `COLLECTING` and snapshot the
   complete authoritative root set. Existing protected readers may continue;
   renewals of unchanged roots are allowed. Roots in this snapshot remain
   protected for the whole collection even if a lease expires during it.
3. Traverse the roots and typed object edges, using a visited set that can spill
   to disk. Abort deletion on missing, corrupt, or unsupported reachable
   metadata. Inventory object keys; listing is not a publication catalog.
4. Mark unretained Generation catalog entries as expired/collected. Select
   unreachable objects older than a one-hour orphan grace period. Recheck their
   eligibility while admission remains closed and persist deletion candidates.
5. Issue bounded deletions, persist results, and reopen admission only after
   every issued deletion has completed and the deletion executor is quiescent.

Root removal can wait until the barrier ends. Local directory edits can
continue, but checkpoint, new remote reads requiring protection, and durable
log appends wait or receive an explicit maintenance response. Existing
immutable reads use the roots already protected. Collection does not run in
the checkpoint transaction or on every checkpoint.

Candidate age is an additional filter, not the proof of safety. The barrier
prevents new references to old candidates while deletion is possible. After it
opens, a publisher cannot reuse an old existence check for unprotected content;
it verifies or uploads that content again.

### 10.3 Interrupted deletion

The barrier never expires automatically. An interrupted collector leaves
admission closed. Recovery must stop the old executor and settle every issued
delete before reopening. A lease check cannot retract a DELETE already sent
to S3, and a timed-out delete is not known to have stopped.

If any delete outcome remains indeterminate, the workspace remains in
`RECOVERY_REQUIRED`; it does not resume writes on a timer. v0 provides no
automatic collector handover. This can cause a prolonged write outage and is
an explicit MVP availability limitation. A replacement must not infer safety
merely from a successful GET, absence of a heartbeat, or another collector's
new epoch.

This restriction matters because object keys are content-addressed and can be
reused. A delayed old DELETE could remove newly reused content after a premature
reopen. A future collector can remove this limitation using storage-instance
identities or another storage-enforced deletion-fencing protocol; that change
requires its own correctness argument.

Late PUTs from definitively aborted preparations may leave garbage. They cannot
publish because their operation is terminal; a later collection can reclaim
them. S3 lifecycle rules must not independently expire Version FS objects.
The release deployment uses an unversioned dedicated data prefix; enabling
bucket versioning requires version-aware reclamation, because a simple DELETE
in a versioned bucket creates a marker rather than reclaiming the version's
bytes, as documented by [S3 DeleteObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObject.html).

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

The service restarts from PostgreSQL, retaining barriers and terminal operation
results. Active preparations can resume while their authority remains valid or
be durably aborted. It never reconstructs publication by listing uploaded
Generation descriptors. If PostgreSQL is unavailable, authoritative mutations
fail closed; a cached branch head is not a replacement authority.

### 11.3 Metadata recovery boundary

The operator must preserve durable metadata and its configured replication
contract. Restoring an older database backup while keeping a later, collected
object namespace can resurrect references to deleted content. The MVP runbook
therefore keeps both writes and GC disabled after such a restore until an
offline reconciliation verifies every state it will expose. It cannot silently
serve the old catalog as current authority.

An older backup is not an automatic failover target. Recovery of acknowledged
data after loss of all durable metadata copies needs a coordinated backup and
object-retention design beyond this release. This does not change the ordinary
worker-loss guarantee.

## 12. SDK and CLI surface

The following specifies logical requests and results, not a final network or
Rust ABI. Each mutation uses Section 5.2's operation contract. Workspace
creation scopes its initial ID to the deployment and caller until the new
workspace ID is known; other operations use the workspace scope.

| Request | Required arguments / result |
| --- | --- |
| `create_workspace` | Operation ID; returns workspace ID and initial main branch/Generation. |
| `create_branch` | Operation ID, name, retained base Generation; returns new branch ID/revision. |
| `get_branch` | Branch ID or live name; returns stable ID, revision, head, stream epoch, and writer epoch. |
| `acquire_writer` | Operation ID, branch ID, expected writer epoch, owner, explicit handover policy; returns recorded grant/expiry. |
| `renew_writer` / `release_writer` | Operation ID and exact grant; renewal returns its recorded new expiry. |
| `checkpoint` | Operation ID, branch ID/revision, grant, managed capture, attached/detached mode; returns Generation and optional new branch revision. |
| `move_head` | Operation ID, branch ID/revision, grant, retained target, discard policy; returns new revision/epoch. |
| `promote` | Operation ID, source ID/revision, destination ID/revision/grant, discard policy; returns selected Generation and destination revision/epoch. |
| `delete_branch` | Operation ID, branch ID/revision, grant; returns deletion result. |
| `open_generation` | Operation ID and retained Generation ID; returns protected immutable handle and expiry. |
| `renew_handle` / `close_handle` | Operation ID and handle ID; rights are not renewed by replaying a prior operation. |
| `read_at` / `list_at` / `stat_at` | Handle and path, plus byte range or listing cursor where applicable. |
| `materialize` | Handle and fresh destination; returns a completed local workspace/export. |
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
| `collect` | Operator operation ID and workspace; returns GC epoch/progress, respecting the durable barrier. |

Local file mutations and artifact staging use the managed gate. They are not
remote durable mutations; retrying an interrupted local command requires the
application's own contract. The API does not promise exactly-once shell
execution or external actions.

Errors distinguish `Conflict`, `Fenced`, `LeaseExpired`, `OperationMismatch`,
`InProgress`, `GenerationExpired`, `StreamReplaced`, `CursorExpired`,
`IntegrityError`, `UnsupportedState`, `Busy`, and `Maintenance`. A transport
timeout does not become a definite rejection. A conditional failure returns
the relevant current identity/revision when authorized.

The CLI exposes workspace creation, import/materialize, managed command
execution, checkpoint, branch/fork, rollback, promote, log append/read/tail,
artifact staging, pin/unpin, diff, operation status, and operator GC status.
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
| Writer replaced during upload | Final commit rejects the old epoch. Local work is preserved; staged garbage is collectible after abort. |
| Append commits, receipt is lost | Replay returns its exact original range without another append. |
| Append races checkpoint gate | Its entire batch is captured before the cut, or it waits for the next interval. |
| Trim or reset races a live cursor | Explicit expired/replaced error; retained Generation reads remain unchanged. |
| Reader open races collection | Admission/protection commits before the snapshot or is blocked; no unprotected successful open. |
| GC crashes or a delete times out | Barrier remains closed; no new publisher can reuse the affected key until deletion is settled. |
| Metadata authority is unavailable | No new authoritative outcome is claimed; unknown preparations are not guessed away. |
| Cache is lost or corrupt | Re-fetch and verify from S3; published state remains intact. |
| Required S3 object is missing/corrupt | Report an integrity failure; do not publish or restore partial replacement state. |
| Local host is lost before checkpoint | Uncheckpointed file and named-artifact changes may be lost. Durable accepted log appends survive under their live roots. |
| Old metadata backup is restored | Disable writes/GC and reconcile before serving its purported published state. |

Local file write completion acknowledges local mutation only. Log append
completion acknowledges the durable log range only. Checkpoint completion
acknowledges the coherent Generation and its publication. These three promises
must remain distinguishable in both SDK results and user documentation.

## 14. Performance, bounds, and benchmarks

### 14.1 Expected costs

Let `N` be namespace entries, `B` total regular-file bytes, `S` log segments,
`V` retained Generations, and `R` the distinct graph reachable during collection.

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
| GC | Root enumeration, `O(R)` distinct-node marking, object inventory, and candidate deletion while admission is blocked. |

Small-file object count, fixed-chunk suffix rewrites, full scans, per-batch S3
latency, the primary metadata database, and blocking GC are expected v0 limits.
This release proves correctness and sharing; it does not claim production
throughput for millions of concurrent agents.

### 14.2 Bounded resources and overload

The default transfer pool permits eight object operations per workspace, with
a service-wide cap of 32. The service queue holds at most 64 admitted requests;
per-branch SDK mutation admission is one active request. These are configurable
initial limits. Tree traversal and scanning stream results instead of retaining
the complete namespace or file in RAM. Cache and temporary-disk budgets are
explicit deployment settings; GC's visited set spills within its disk budget.

Before admission, exhaustion returns `Busy` or an explicit capacity error.
After admission, the operation must reach a recoverable state or a recorded
abort. It cannot drop an accepted append, evict dirty working bytes, release an
unresolved operation's roots, bypass a checksum, or acknowledge a weaker
durability mode. A full metadata store cannot be worked around by forgetting
idempotency results. Collection that cannot finish within its resource budget
stops safely under its barrier.

### 14.3 Benchmark report

The release includes a reproducible report on real S3 and the specified
PostgreSQL deployment. Scenarios below are separate experiments, not one
required Cartesian product. Record hardware, region, network, service limits,
format version, retention settings, cache size, and raw sample counts.

| Scenario | Initial fixture / comparison |
| --- | --- |
| Namespace scale | 1,000, 100,000, and 1,000,000 files, including a high-fanout directory. |
| Large content | 1 GiB and 100 GiB files; overwrite one aligned range versus insert near the beginning. |
| Small change | No changes, one small-file edit, 1% changed files, subtree rename, and mass deletion. |
| Version depth | 10, 100, and 1,000 Generations on a small fixed workspace; compare open/read request counts. |
| Fan-out | 1, 10, and 100 branches sharing one base; fork, independently checkpoint, and drop candidates. |
| Logs | 100,000 1 KiB records; batches of 1, 100, and 1,000 records; capture, fork, and trim. |
| Cold recovery | Empty cache versus warm cache; one-file native read versus full materialization. |
| Collection | Shared and divergent retained roots, abandoned preparations, and interruption before/after deletes. |

Measure capture pause and total completion separately; bytes scanned, hashed,
uploaded, downloaded, and newly stored; data and manifest object counts;
metadata commits; peak RAM/disk; and GC's write-unavailability interval.
Report latency distributions with sample counts and achieved append/checkpoint
throughput. Single-stream and independent-branch throughput are separate.
Always include errors and saturation, rather than omitting failed requests.

The release gates below require the structural cost properties to hold.
Latency numbers establish the first baseline; this RFC invents no measured
latency SLO. A later performance proposal can set budgets from those results.

## 15. Implementation sequence

| Phase | Deliverables | Exit evidence |
| --- | --- | --- |
| 1. Immutable state | Canonical encoding, object-store adapter, verified cache, persistent maps/chunks, descriptor reader, import/export, structural diff. | One complete fixture resolves on an empty worker with no ancestor descriptors; unchanged nodes/chunks are physically shared. |
| 2. Authority and writes | PostgreSQL schema, durable operation outcomes, writer grants, managed capture, attached/detached publication, branch lifecycle, native log append/read/trim, artifact edges. | The end-to-end candidate workflow works; lost replies and stale writers have one recoverable authoritative outcome. |
| 3. Retention and recovery | Pins/read protections, blocking GC, restart reconciliation, operator runbooks, real-backend fault campaigns, benchmark report. | Collection cannot invalidate retained state; a fresh worker restores every retained fixture after worker loss. |

GC deletion remains disabled until protection, publication fencing, and
interruption behavior are demonstrated together. Development backends can
accelerate iteration, but cannot replace real-backend release evidence.

## 16. Acceptance criteria

### 16.1 Functional and structural gates

1. Import supported files, empty directories, symlinks, named artifacts, logs,
   and a self-contained Git repository. Capture rejects unsupported state.
2. Create attached and detached Generations. Neither descriptor upload nor
   failed CAS makes a Generation visible; detached success moves no branch.
3. Restore each retained Generation on another worker with an empty cache.
   Remove unretained ancestor descriptors and verify descendant reads still
   resolve entirely from their own roots.
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

### 16.2 Failure-injection gates

Exercise each upload and metadata transition with process termination, delayed
requests, duplicate delivery, lost replies, and retried operation IDs. Required
cases include:

- Creation and writer acquisition committing before their replies are lost;
  replay returns the original identities and never grants a second epoch.
- Publication/append committing before a lost reply, followed by later branch
  movement, reset, or name reuse; outcome recovery still returns the original
  result. An abort tombstone wins against a delayed request or returns the
  already committed result.
- Writer expiry and handover during preparation; a delayed old writer cannot
  publish, append, trim, or move a reference.
- Appends racing the capture gate; no batch is split or assigned to the wrong
  checkpoint interval. Local mutation admission stays closed until capture has
  a definite outcome.
- Reader acquisition, pin changes, object reuse, publication, and GC racing;
  no live graph is deleted. A stale existence result cannot justify reuse.
- Collector interruption and delayed/unknown deletion outcomes; the barrier
  persists, and no automatic timeout or new collector epoch reopens admission.
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
| I1-I3: immutable, complete, independent state | Encoding/graph validation, independent restore, ancestor removal, sharing and diff fixtures. |
| I4-I5, I8: durability and atomic publication | Real-backend upload/commit failures, detached publication, CAS, lost-reply recovery. |
| I6, I14: retention and log safety | Pin/read/GC races, complete closure marking, trim with retained historical reads. |
| I7, I11: authority and retry | Handover/expiry, ABA fixtures, identified create/acquire/append/publication retries and abort tombstones. |
| I9: disposable workers | Destroy all worker state after publication and recover on a fresh worker. |
| I12-I13: authorization and durable ordered append | Workspace isolation, atomic batches, conditional tails, receipts and attachment retention. |
| I10, I15: explicit participant/global boundaries | No runtime/DB/global-checkpoint support is claimed; no synthetic timestamp-based consistency assertion. |

The MVP is complete when all required capabilities and gates have evidence,
the real-backend benchmark report is published, and operator instructions cover
backup assumptions, unknown outcomes, safe collection, and an interrupted GC
barrier. Unsupported adapters or future integrations are not release blockers.

## 17. Selected tradeoffs

| Decision | Reason and accepted cost |
| --- | --- |
| PostgreSQL authority | Reuse durable multi-record transactions; adds an independent durable service and one primary capacity boundary. |
| S3 immutable object graph | Portable worker recovery and sharing; small objects and each synchronous append incur remote request cost. |
| Persistent ordered trees | Selective reads, range iteration, and metadata sharing; tree encoding and mutation correctness require care. |
| Fixed chunks | Simple, measurable range reuse; insertions can rewrite suffixes. |
| Full quiesced scan | A clear coherent boundary; checkpoint pauses and scan work can follow total workspace size. |
| Captured Git repository files | Normal local Git tooling works without a server adapter; no native Git protocol or object-format translation. |
| One sealed segment per batch | Append durability and checkpoint capture are simple; tiny batches amplify S3 objects and requests. |
| Blocking GC without automatic handover | Smaller concurrent deletion surface; collection and uncertain delete recovery reduce write availability. |

Future changes can replace these mechanisms while preserving RFC-001's
semantics. Online GC, a multiplexed log WAL, tracked change discovery, or runtime
and GTS integration require additional proposals rather than hidden extensions
to v0's guarantees.

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
  inform the immutable object adapter. [DeleteObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObject.html)
  distinguishes deletion from versioned-bucket delete markers.
- [PostgreSQL WAL settings](https://www.postgresql.org/docs/current/runtime-config-wal.html)
  and [explicit locking](https://www.postgresql.org/docs/current/explicit-locking.html)
  inform durable metadata acknowledgement and transaction-level coordination.
