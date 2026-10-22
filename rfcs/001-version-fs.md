# RFC-001: Version FS — Versioned Storage for Files, Git, and Logs

| Field | Value |
| --- | --- |
| Status | Draft |
| Created | 2026-10-22 |
| Target | Version FS v0 |
| Scope | Storage model, consistency, durability, and implementation boundaries |

## 1. Summary

Version FS provides durable versioned state for agents and other workloads that
create many short-lived workspaces. Files, source-control state, append-only
logs, and artifacts share an immutable object graph and a common checkpoint
boundary.

The fundamental abstraction is a **Generation**: an immutable, complete logical
state rooted at a stable manifest. A retained Generation can be read or restored
without replaying earlier Generations. Its physical representation shares
unchanged data and metadata with other Generations.

```text
                      Generation
                           |
                 immutable root manifest
                  /        |        \
                 FS       Git       Logs / artifacts
```

A running workspace has a published Generation, a mutable filesystem overlay,
live log streams, and a local cache. A checkpoint captures their common boundary,
makes its object graph durable, and atomically advances a branch to a new
Generation. Fork, rollback, and promotion operate primarily on metadata.

Version FS does not replace a transactional database. A database retains its own
transactions, indexes, concurrency control, and recovery machinery. A future
coordinator can publish a small global checkpoint that references both systems
after they durably capture a common logical boundary.

The design principle is: **unify version semantics, not storage engines.**

## 2. Motivation

An agent may start on ephemeral compute, create thousands of files, produce
large build outputs, fork several candidates, and retain only one successful
result. The same session may need to resume on another host or inspect a state
that existed before a failed tool call.

```text
                         base state
                         /   |   \
                        A    B    C
                        |    |    |
                     modify test modify
                        |    |    |
                    checkpoint rollback checkpoint
                        |         |
                      discard   promote
```

Copying a complete workspace for every checkpoint or candidate makes storage,
startup time, and cleanup proportional to the base workspace. Keeping the only
durable copy on the execution host also makes host failure a state-loss event.

Version FS separates these concerns:

```text
ephemeral compute + local working state + durable versioned storage
```

The desired cost of a checkpoint is the data and manifest paths that changed,
plus bounded publication metadata. The desired cost of a fork is a metadata
transaction. These are architectural objectives, not measured performance claims.

Filesystems, Git, and backup systems each address parts of this problem. Version
FS makes retained historical state directly readable, branchable, and usable as
an execution base across machines. Git remains an important interface, while
generated files, logs, and artifacts can be checkpointed independently of
human-facing Git commits.

## 3. Requirements and scope

The terms **MUST**, **MUST NOT**, **SHOULD**, and **MAY** express requirements of
this proposal. Implementation choices described as proposed defaults remain
subject to review while this RFC is a draft.

### 3.1 Required v0 behavior

Version FS v0 MUST provide:

1. Immutable, complete, independently readable retained Generations.
2. Sharing of unchanged data and manifest nodes across Generations.
3. Durable immutable object storage and content integrity verification.
4. A mutable workspace overlay and explicit, synchronous durable checkpoints.
5. One active writer per branch, with server-enforced fencing.
6. Atomic branch updates, fork, rollback, deletion, and conditional promotion.
7. Files, directories, supported metadata, and durable record streams with
   atomic batch appends, sequence numbers, replay, and live tailing.
8. Lazy reads through a native API and materialization into a local directory.
9. Recovery on a new host without the previous host's cache or overlay.
10. Retention, explicit pins, and conservative reachability-based collection.
11. A basic structural diff that skips identical manifest subtrees.
12. Explicit publication recovery and retry semantics.

Git repositories MAY be captured as ordinary filesystem state in v0. Native
Git protocol hosting is a later interface; it is not a v0 acceptance criterion.

### 3.2 Deferred capabilities

Later versions MAY add FUSE or virtio-fs mounts, optimized sparse workspaces,
native Git object/ref APIs, Git smart protocol, remote mutation journaling,
autosave, improved chunking, online GC, distributed caches, and coordinated
database checkpoints.

The initial implementation does not require general merge, multiple independent
writers within one branch, cross-region synchronous writing, database
integration, or a global Timestamp Oracle (TSO).

### 3.3 Non-goals

Version FS does not implement SQL, relational constraints, query optimization,
row-level MVCC, or arbitrary database transactions. An implementation may use a
database internally for its small coordination service; this does not make
Version FS an application database.

A Generation captures persisted logical state. It does not capture process
memory, open network connections, external service state, or the effects of
already-issued emails, payments, or other external actions. Restoring a
Generation does not promise exactly-once execution of those actions.

Full POSIX compatibility, automatic semantic merge, and Git's internal storage
format are also outside the v0 contract.

## 4. Terminology

| Term | Meaning |
| --- | --- |
| Object | Immutable stored content, such as a file chunk, manifest node, or log segment. |
| Manifest | Immutable metadata that resolves names and logical ranges to objects. |
| Root manifest | The entry point for the state represented by a Generation. |
| Generation | A published, immutable logical state with a stable identity. |
| Workspace | An authorized namespace containing branches and retained Generations. |
| Branch | A mutable named reference to a Generation and its writer coordination state. |
| Overlay | Unpublished mutable changes above a workspace's base Generation. |
| Checkpoint | Capture and durable publication of an overlay boundary as a Generation. |
| Pin | A durable retention reference that keeps a Generation and its object graph live. |
| Read lease | A temporary retention reference protecting an active reader. |
| Writer epoch | A monotonically increasing fencing value checked at publication. |
| Revision | A monotonically increasing branch-head version used by compare-and-swap. |
| Publication session | An identified, fenced checkpoint attempt tracked by the coordinator. |
| TSO | A service allocating timestamps in a shared logical ordering domain. |

Generation identity, object identity, branch revision, and TSO timestamp are
different values. None is inferred from a wall-clock timestamp.

## 5. Architecture and authority

```text
                       Agent runtime
                            |
            SDK / local directory / log API
                            |
             base Generation + overlay + live logs
                            |
                checkpoint / append builders
                      /           \
                     /             \
          immutable data plane       coordination service
          - file objects             - branch heads / revisions
          - manifest nodes           - writer epochs / leases
          - log segments             - live log tails / append receipts
          - artifact objects         - publication outcomes
          - Generation descriptors   - retention roots / pins
                     |               - GC coordination
                 object store                  |
                     |                durable metadata backend
               local object cache
```

The data plane stores immutable bytes. Files, manifest nodes, log segments, and
Generation descriptors may use the same object-store backend and cache. Separate
logical APIs do not require separate physical storage engines.

The coordination service is the authority for published Generations, branch
heads, live log positions, ownership, and retention. Its accepted updates MUST
be durable and linearizable. Transactions that publish a Generation MUST
atomically update the relevant branch, Generation catalog entry, retention
state, and operation result.

The exact metadata backend is left to the implementation RFC. A transactional
store or an atomic update of an aggregate metadata record can satisfy the
contract. A development-only coordinator on a single local disk cannot claim
the production host-loss guarantee unless that metadata has equivalent remote
durability and a recovery procedure.

The data plane and control plane are both required to recover the service.
Losing a worker's NVMe cache is safe. Losing the authoritative branch and
retention metadata is a separate disaster-recovery problem. Object-store listing
alone MUST NOT be used to infer which uploaded descriptors were published.

## 6. Generation model

### 6.1 Descriptor and root

The following structures are conceptual, not a wire-format commitment:

```text
GenerationDescriptor {
    format_version,
    workspace_id,
    generation_id,
    parent_generation_id?,
    root_manifest_id,
    checkpoint_boundary,
    checkpoint_tso?,
    created_at,
    metadata,
}

RootManifest {
    format_version,
    filesystem_root,
    log_roots,
    artifact_root?,
    git_root?,
}
```

`parent_generation_id` records lineage. It MUST NOT be needed to resolve file
contents, directory metadata, log contents, or artifact references. Retaining a
child does not automatically require retaining its parent descriptor.

```text
generation_id -> descriptor -> root manifest -> complete reachable state
```

A Generation ID identifies one descriptor within a workspace. It need not be
globally ordered or content-derived. Repeated checkpoints with identical
contents MAY have distinct Generation IDs while sharing the same root manifest.

Uploading a descriptor does not publish a Generation. Direct lookup by
Generation ID succeeds only for a published, authorized, retained Generation or
a reader holding an applicable lease. Expired state can return
`GenerationExpired` even if some of its objects have not yet been collected.

### 6.2 Logical completeness

Every required state edge MUST be reachable from the Generation's root graph.
External databases, package registries, and services are outside that graph
unless their needed bytes have been explicitly imported as artifacts.

The read path may traverse many shared immutable nodes. Independent readability
does not imply a single object, a single request, or no internal tree traversal.
It means there is no need to reconstruct state from a chain of earlier
Generations or replay a mutation journal.

## 7. Immutable objects and manifests

### 7.1 Content identity

Objects SHOULD use a strong content digest with explicit domain separation:

```text
object_id = Hash(format_version || object_type || canonical_payload)
```

An initial implementation SHOULD use SHA-256 unless the object-format RFC
justifies another digest. Lengths and type fields MUST be unambiguous. Canonical
encoding is required for metadata; raw file bytes have their natural byte
encoding. Compression and encryption are physical representations of logical
content and MUST have versioned decoding metadata.

Object creation is idempotent. A key MUST NOT be overwritten with different
logical content. Reusing an existing object requires checking its type, length,
and content identity according to the backend contract. A conflicting object at
an expected identity is corruption, not a last-write-wins update.

Physical keys MUST be scoped by tenant or encryption domain. An object hash is
an identifier, not an authorization capability. Cross-tenant deduplication is
not required.

### 7.2 Persistent metadata

Manifest nodes MUST support structural sharing. Updating a file rewrites the
necessary metadata paths and reuses unchanged subtrees:

```text
          root G100                    root G101
          /       \                    /       \
      shared A     B               shared A     B'
                 / \                           / \
                C   D                         C'  D
```

Large directories and file-extent maps MUST also use persistent structures;
copying one complete directory listing per small edit would defeat the goal.
Candidate structures include persistent B-trees, radix trees, and HAMTs. This RFC
requires their observable properties rather than selecting a final layout.

Directory entries refer to immutable file or directory descriptions. Deletions
are resolved while building the new tree; a published Generation MUST NOT need
to consult tombstones in an older Generation.

Metadata work includes the changed entries, paths to them, and any node splits
or rebalancing. It is not literally constant per modified file. Choosing node
size, directory layout, and encoding requires measurements in the implementation
RFC.

### 7.3 Files and ranges

Small files MAY occupy one object. Large files SHOULD use an immutable extent
or chunk index so an overwrite can reuse unchanged ranges:

```text
G100: A B C  D
G101: A B C' D
```

Fixed-size chunks are the proposed initial policy. The threshold and chunk size
remain implementation decisions. Fixed-size chunking bounds an overwrite's
storage amplification; inserting bytes near the start of a file can still
change many later chunks. Content-defined chunking and extent editing are later
optimizations.

Range reads fetch the necessary chunks. Integrity MUST still be verified. With
a digest for each complete chunk, v0 can fetch and verify the whole overlapping
chunk; verifying an arbitrary subrange requires additional authenticated block
metadata. Compression SHOULD be independent per chunk when enabled.

### 7.4 Object-store requirements

The backend MUST provide complete-object reads, durable successful writes,
immutable creation or an equivalent verified put-if-absent operation, and
explicit deletion. An incomplete multipart upload is not a durable object.

Amazon S3 documents strong consistency and atomic updates to one key, while
updates across keys are not atomic. It also supports conditional writes. These
properties support immutable object creation, but the Version FS publication
transaction still belongs in the coordination service. See [S3's consistency
model](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html#ConsistencyModel)
and [conditional writes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html).

Version FS MUST NOT assume every S3-compatible backend has the same contract.
Backend support requires validating these semantics. An ETag used for a
conditional request is an opaque concurrency token; it MUST NOT substitute for
the Version FS content digest.

## 8. Filesystem view and overlay

### 8.1 Supported namespace

v0 MUST represent regular files, directories including empty directories,
symbolic links, file length, permission bits including the executable bit, and
modification time. Ownership handling must be documented as a portable policy.

Unsupported features such as hard-link identity, extended attributes, special
devices, or sparse allocation metadata MUST be rejected or explicitly documented
as an opt-in conversion. A successful exact checkpoint MUST NOT silently discard
required filesystem semantics. Capturing a live database directory requires
that database's own snapshot or quiescence protocol.

Paths are resolved within the workspace. Materialization MUST validate entry
names and symbolic-link handling so captured content cannot cause writes outside
the selected destination.

### 8.2 Overlay reads and mutations

```text
read(path)
    -> current overlay entry, including deletion
    -> base Generation on overlay miss
    -> cache or remote immutable object
```

Mutations update a workspace-local overlay. Multiple processes MAY use the same
writer session, provided the adapter sequences their operations. Independent
writer sessions on the same branch are excluded in v0.

The adapter assigns a monotonic local mutation sequence. A checkpoint boundary
selects a coherent prefix across file contents, namespace metadata, captured Git
state, and log records. Completed supported filesystem operations before the
boundary are included; operations after it remain in a subsequent overlay.

This is a filesystem consistency boundary. Applications requiring an atomic
change across several file operations must establish their own barrier before
requesting a checkpoint.

### 8.3 Native API and materialization

v0 MUST support lazy immutable reads through the native API and explicit export
of a Generation or selected subtree into a local directory. Export downloads
the selected bytes; an ordinary exported directory does not gain lazy loading
without a mount adapter.

Importing changes from an ordinary local directory requires quiescing all
processes that can modify the captured tree and draining in-flight mutations
before capture, or using a proven local snapshot mechanism. The tree must remain
quiescent for the duration of a directory capture. A directory walk racing
arbitrary writes MUST NOT claim to produce a consistent checkpoint.

A future FUSE or virtio-fs adapter can provide transparent lazy filesystem reads
and coordinate overlay boundaries internally. Memory-mapped writes and open-file
behavior need an explicit contract before that adapter claims support.

## 9. Log and artifact views

### 9.1 S2 as the log reference

The log interface takes its reference model from [S2 streams](https://s2.dev/docs/concepts/streams):
named, ordered records with atomic append batches, replay from a cursor, and
live following. This RFC proposes Version FS semantics around that model;
deploying S2 is not a v0 requirement.

Following [S2's record model](https://s2.dev/docs/concepts/records), an application
record has a byte body and optional byte-valued headers. Version FS adds its own
stream identity and checkpoint integration:

```text
LogRecord {
    seq_num,
    timestamp,
    headers: [(bytes, bytes)],
    body: bytes,
}
```

Sequence numbers are assigned by the log service and identify consecutive
records within one stream incarnation. They are record positions, not byte
offsets. A newly created empty log starts at zero. Arrival timestamps support
inspection and seeking; they do not establish order across streams or supply a
global TSO boundary.

The **stream head** is its first retained sequence number. The **stream tail**
is the next sequence number to assign. Neither is the branch head, which points
to a Generation.

### 9.2 Immutable storage and captured bounds

Records are stored in immutable, sealed segments indexed by persistent metadata.
Published segments are never extended in place. A Generation captures:

```text
LogHead {
    log_id,
    stream_id,
    segment_index_root,
    first_seq_num,
    next_seq_num,
}
```

Its visible records are exactly `[first_seq_num, next_seq_num)`. The segment
index MUST resolve that interval independently of earlier Generations and of
the current live stream. Framing and checksums MUST detect incomplete or corrupt
records. The physical segment format is independent of the public record API.

### 9.3 Atomic durable append

[S2 appends](https://s2.dev/docs/concepts/appends) acknowledge a batch after
durability and report the range it occupies. Version FS adopts that boundary
for its log API: v0 `append_log` success MUST mean the complete batch is remotely
durable and ordered. An append batch occupies one contiguous sequence range and
is visible atomically. It must not expose a partial prefix on failure.

```text
AppendReceipt {
    stream_id,
    append_id,
    start_seq_num,
    end_seq_num_exclusive,
    durable_tail_seq_num,
}
```

Two records appended at sequence 42 occupy `[42, 44)`. The receipt's tail is the
observed durable tail and may be later than that batch's end. The receipt does
not publish a Generation or establish filesystem durability.

For a native v0 backend, the append path creates sealed segment objects and
updated index nodes, then atomically advances the durable live tail, its
retention root, and the append outcome in coordinated metadata. It validates
the writer epoch at that update. Required objects MUST be durable before the
tail becomes visible. Active append sessions participate in GC protection.

Batch limits, buffering, and backpressure are implementation decisions. A local
enqueue operation MAY exist, but it returns a pending result rather than an
`AppendReceipt`. Server-side ordering is required even if clients pipeline
requests; independent concurrent requests must not infer order from arrival at
the caller.

### 9.4 Conditional writes, fencing, and retries

The API accepts an expected next sequence number, analogous to S2's
`match_seq_num`, along with a stable append ID. A new append succeeds only at
that position and under the current branch writer epoch. See [S2 concurrency
control](https://s2.dev/docs/concepts/concurrency-control).

v0 retains one logical writer per branch. Every new append MUST carry current
writer authority. S2's fencing token is optional and cooperative; a Version FS
adapter cannot rely on it while also allowing callers to bypass the adapter
with unfenced writes. A future S2 backend must define token handover and access
control alongside Version FS epochs.

The service binds `(stream_id, append_id)` to the request contents, expected
position, and recorded outcome. A timeout leaves the result unknown. Retrying
the same request returns its recorded receipt or resolves the original attempt;
changing its payload or expected position is an error. A recorded outcome may
be returned after writer handover, but a new mutation under the old epoch is
rejected.

An advanced tail alone does not prove which request wrote the records. The
client must resolve the append ID or verify the intended range. If outcome
records have expired, it must reconcile instead of blindly appending again.
These requirements make duplicate suppression explicit without promising
exactly-once external effects. S2 also distinguishes conditional writes from
unconditional retries in its [retry and duplicate discussion](https://s2.dev/blog/patterns).

### 9.5 Replay, following, and resumable cursors

[S2 reads](https://s2.dev/docs/concepts/reads) provide the reference for bounded
scans and live following. Version FS readers use a cursor containing a stream
incarnation ID and the next sequence number to consume. A consumer persists that
cursor after processing records so it can resume after disconnecting.

`read_log_at` is bounded by the Generation's captured interval and never follows
the current live tail. A separate live-read API returns only durably appended
records, can wait at the tail, and supports record/byte limits and bounded
buffering. A slow reader may disconnect and resume without blocking writers.
Read leases protect the selected content during access.

A live reader's protection starts at its requested cursor and advances as the
reader makes progress. Trim cannot reclaim its protected unread range while
the lease remains valid. The consumer is responsible for persisting its own
processing cursor; delivery alone does not acknowledge application processing.

`check_log_tail` is a linearizable observation of the durable live tail: it
reflects completed prior appends, not locally queued records. It is a freshness
or catch-up boundary, not proof of a consistent filesystem checkpoint. Timestamp
and tail-relative selectors MAY help interactive inspection; stored replay
cursors use exact sequence positions.

A cursor for a replaced stream returns `StreamReplaced`. A live cursor before
the retained head returns `CursorExpired`; the API MUST NOT silently skip the
missing records.

### 9.6 Checkpoints, forks, and rollback

A checkpoint selects the log prefix belonging to its logical mutation boundary
and waits for included batches to become durable. It records the corresponding
immutable index and exact exclusive end. A checkpoint MUST NOT split an atomic
batch or include later records merely because a tail query observed them.
Appends after the boundary can remain visible to live readers while being absent
from that Generation.

Forks share the captured prefix and create a distinct live stream incarnation
for further appends, continuing after the inherited end. Rollback and promotion
similarly require a successor live incarnation when replacing a writable
history. Existing Generations keep their captured identities and bytes. The
same numeric position on two divergent incarnations can contain different
records, so sequence numbers alone are insufficient cursors.

Promotion selects the source Generation's log view. It does not interleave the
two branches' records or rewind a shared mutable stream.

### 9.7 Trimming and retention

Following S2's distinction between [trimming](https://s2.dev/docs/concepts/trimming)
and retention, `trim_log(before_seq_num)` advances a live stream's logical head
and releases that live view's reference to the prefix. Sequence numbers are
never renumbered. Logical trim does not immediately delete physical segments.

Retained Generations, forks, pins, active readers, and unresolved appends still
protect their ranges. Trimming a live stream MUST NOT remove records required
by any of those roots. Only GC may reclaim segments after their final reference
is gone. Stream retention or an external TTL cannot override Generation
retention.

[S2's snapshot-and-follow pattern](https://s2.dev/docs/concepts/snapshots) informs
a future mutation journal: a checkpoint stores the first sequence number not
folded into state, and recovery follows from that cursor. Published Generation
state remains independently readable. Trimming a journal additionally requires
checking every retained recovery base that still needs its suffix.

An external S2 stream URI and cursor alone are insufficient as a permanent
Generation log view if that service can trim the required records. An adapter
must preserve captured bytes in the immutable graph or establish an equivalent
verified retention contract before publication.

### 9.8 Storage pipeline and artifacts

[S2's architecture](https://s2.dev/docs/platform/architecture) separates durable
ingress chunks from later per-stream segment batching. Version FS may adopt a
similar pipeline for scale. v0 may begin with one sealed segment per append
batch and an indexed live tail; a multiplexed ingress WAL is a later
optimization. Any WAL-to-segment handoff must preserve acknowledged records and
their reader protections. A Generation must resolve immutable log bytes without
replaying an inter-Generation mutation journal.

Large payloads SHOULD be stored as artifacts with small log records referencing
their immutable identity. Required artifact edges MUST appear in the manifest's
reachability graph; a digest embedded only in an opaque record body is not a GC
root. Artifacts stored under ordinary filesystem paths or an artifact root use
the same integrity and retention rules.

A storage mutation journal is a distinct log purpose. A future implementation
may share segment encoding and transport with event logs, but recovery ordering,
acknowledgement, and retention rules remain explicit for each use.

## 10. Branches, leases, and fencing

### 10.1 Branch state

```text
BranchState {
    workspace_id,
    branch_id,
    name,
    head_generation_id,
    head_revision,
    writer_epoch,
    lease_owner?,
    lease_expires_at?,
    fork_base_generation_id?,
}
```

A branch name may be reused after deletion; `branch_id` MUST change so stale
handles cannot address a newly created branch. Head revisions increase on every
head movement, including rollback. Writer epochs increase when ownership is
replaced or an administrative head change invalidates the old writer.

Comparing only a Generation ID is insufficient. A branch can move from G100 to
G101 and back to G100. Checking the head revision prevents a stale operation from
mistaking that branch for the original state.

### 10.2 Writer acquisition and publication

The coordination service grants a lease for one branch and returns a fencing
epoch. A checkpoint publication validates all of:

```text
branch_id is current
head_revision == expected_revision
head_generation_id == expected_head
writer_epoch == supplied_epoch
lease_owner == supplied_owner
lease is valid according to the metadata authority
publication session is active
```

The checks and head advancement MUST occur in the same atomic metadata update.
Checking a lease only on the worker is insufficient. Expiration decisions use
the authority's lease rules, not an untrusted client wall clock.

A stale writer may finish uploading immutable objects. It MUST NOT advance a
branch or live log tail, publish a Generation, or modify ownership after it has
been fenced. If a worker loses lease renewal, it must stop accepting mutations
before its local lease deadline and reconnect or recover under a new epoch.

During a coordination-service outage, pinned immutable reads may continue while
their retention leases remain valid. Writer acquisition and publication fail
closed.

## 11. Checkpoint and publication protocol

### 11.1 Capture and upload

At most one checkpoint is in flight per branch writer in v0.

1. Allocate a stable operation ID and Generation ID. Register a publication
   session with the expected branch identity, revision, head, and writer epoch.
   Its registration participates in GC coordination.
2. Establish the logical checkpoint boundary. Freeze the selected overlay and
   its exact log heads. Retain access to the base Generation.
3. Build new immutable file objects and manifest nodes, and select the durable
   log segments captured by the boundary. Reuse protected base subtrees and
   create or verify other referenced objects.
4. Wait for successful durable completion of every required object upload,
   including manifest nodes and the root manifest.
5. Write the immutable Generation descriptor. Its presence is preparation data,
   not publication evidence.
6. Submit the publication transaction to the coordination service.

The builder need not re-upload or recursively reverify every protected base
object. The base's retention reference protects its existing closure. Reuse of
an otherwise unprotected object requires a valid existence/integrity check or
idempotent upload during the protected publication session.

The trusted publication path MUST establish that the new graph is closed over
durable, authorized objects. It may combine verified uploads with already
validated, retained subtrees rather than traverse the entire base. A worker's
assertion that uploads finished, or a successful read of the root object alone,
is insufficient evidence that every required child is durable. The
implementation RFC must specify how backend acknowledgements and manifest
references are validated before the metadata commit.

Writes after the frozen boundary MAY continue in a new overlay. An
implementation may instead pause mutation through checkpoint completion for a
simpler first implementation.

### 11.2 Atomic visibility

The publication transaction MUST atomically:

1. Check branch CAS, lease, fencing, session, and idempotency preconditions.
2. Register the descriptor as a published Generation.
3. Establish its retention policy and update branch-root retention.
4. Advance the branch head and increment its revision.
5. Persist the successful operation result and close the publication session.

This transaction is the logical visibility boundary. After it commits, the
Generation is authoritative even if a worker crashes before receiving the reply
or writing an auxiliary `COMMIT` record. A `PREPARE` or `COMMIT` marker stored by
the worker is never the sole publication authority.

A successful `checkpoint()` returns only after the required objects and metadata
transaction are durable. The worker then discards the committed frozen overlay
and rebases any later overlay on the new Generation. It MUST preserve later
writes. On fencing or CAS failure, unpublished changes are preserved for explicit
recovery or export; the worker must not silently clear them.

### 11.3 Retry and unknown outcomes

The operation ID identifies one request payload. Retrying it MUST return the
recorded outcome or resume the same attempt. Reusing it with different
preconditions or contents is an error.

A timeout is not proof that publication failed. Recovery queries the durable
operation result, including the committed Generation and resulting branch
revision. Inspecting only the current head is insufficient because the branch
may have advanced or rolled back after the original commit.

Unresolved publication outcomes protect their prepared state from GC. The
coordinator must retain operation records or compact tombstones long enough to
prevent a delayed retry from applying the same logical mutation twice. A client
outside the supported retry horizon receives an explicit unknown/expired
outcome and must reconcile; it cannot blindly repeat the mutation.

## 12. Fork, rollback, promotion, and deletion

| Operation | Preconditions | Effect |
| --- | --- | --- |
| Fork | Authorized retained base Generation. | Create a new branch ID and retention reference to the same Generation. |
| Rollback | Expected branch ID/revision/head; authority to replace the head. | Point to an earlier retained Generation; increase revision and fence prior writer state. |
| Promote | Expected source and destination states; destination still at the source's recorded fork base. | Point the destination to the selected source Generation in one metadata transaction. |
| Delete branch | Expected branch identity/revision; authorization. | Fence the writer and remove the branch's retention reference. |

Promotion selects an exact source Generation and validates the source revision
in the transaction. If either checked branch changed, the operation fails with a
conflict. Requiring the destination to remain at the fork base makes v0 promotion
a conditional selection of validated speculative work; it does not merge
independent destination edits.

Rollback, promotion, and deletion must be coordinated with active runtimes. An
existing writable overlay is based on a particular head and epoch. After an
external head replacement, that runtime must reconnect and explicitly reconcile
or discard its local overlay before publishing again. A control-plane pointer
change alone does not rewrite the files in a running process's local directory.

These operations do not rewrite immutable data. Deleting a branch releases a
root; it does not immediately erase objects shared by another branch, a pin, a
reader, or a retained historical Generation.

General three-way or semantic merge is deferred. A future merge can produce
another complete Generation using domain-specific file, Git, and log rules.

## 13. Git compatibility

Git commits and native Generations serve different histories. Machine
checkpoints may be frequent while user-visible source commits remain selective:

```text
Generations: G100 -> G101 -> G102 -> G103 -> G104
Git commits:   C1 --------------------------> C2
```

In v0, a local Git repository can be captured through the filesystem view.
Checkpoint capture MUST include a coherent `.git` state by quiescing Git
operations along with other directory writers. This preserves Git's existing
format and tools, while Git maintenance can cause substantial file/chunk changes.
It does not implement a remote Git service.

A later native adapter may store Git blobs, trees, commits, tags, and refs in the
common object substrate. It MUST preserve Git object encoding and Git object
identity. A native Object ID is not automatically a Git SHA, and a filesystem
tree containing logs and arbitrary metadata is not automatically a Git tree.
See [Git's object model](https://git-scm.com/book/en/v2/Git-Internals-Git-Objects).

For a given Generation, there MUST be one declared authoritative representation
of Git state. v0 uses filesystem contents; the optional native `git_root` is
absent. An adapter must define a coherent transition before a native Git root
can replace that representation. Divergent `.git` files and native refs cannot
both claim authority.

Future clone, fetch, push, checkout, commit, and branch support must define
atomic ref updates and retention of the reachable Git object graph. Neither
every Generation becoming a Git commit nor a one-to-one correspondence between
Git commits and Generations is required.

## 14. Durability and recovery

### 14.1 Acknowledgement boundaries

The following modes describe filesystem overlay mutations:

| Mode | Acknowledged mutation | Host-loss recovery | Scope |
| --- | --- | --- | --- |
| Ephemeral overlay | Mutation accepted locally. | Latest successful durable checkpoint. | Required in v0. |
| Logged overlay | Mutation accepted locally; a separate watermark reports asynchronously durable records. | Published Generation plus the confirmed durable journal prefix. | Deferred. |
| Durable mutations | Selected mutation acknowledged after remote journal/data durability. | All mutations covered by the documented acknowledgement contract. | Deferred. |

Log `append_log` has the separate remote durability boundary defined in Section
9, including in v0. A host can lose uncheckpointed filesystem writes while its
acknowledged live log records survive. Those records do not establish a coherent
filesystem/log state until a Generation captures the common boundary. Optional
local log buffering must report pending rather than durable append completion.

Checkpoint is always a synchronous durability boundary, including in ephemeral
mode. A future asynchronous API must return a pending operation and separate
durable completion; it must not report checkpoint success before durability.

In v0, `fsync` of an ordinary local materialization protects local filesystem
state according to that filesystem. It is not a remote checkpoint. A future
mount must document whether `fsync` is local or remote before offering stronger
durability guarantees.

### 14.2 Recovery on a new worker

1. Resolve the authorized branch or retained Generation through the coordinator.
2. Acquire a read lease or writer session protecting the selected base.
3. Fetch and verify the descriptor and root manifest.
4. Open the namespace and exact recorded log heads.
5. Fetch and verify data objects as paths and ranges are accessed.

This path requires no previous worker disk and no ancestor replay. v0 makes no
promise to recover uncheckpointed overlay writes after host loss.

Durable live records after the Generation's captured end may be read separately
from a protected live stream. They cannot reconstruct the filesystem overlay
unless a future explicit mutation-journal protocol gives them that meaning.

A corrupt cache entry is evicted and fetched again. A corrupt or missing
required remote object is reported as a storage integrity failure. The service
MUST NOT silently replace it with empty content, an older version, or a partial
Generation.

### 14.3 Failure outcomes

| Failure point | Authoritative outcome | Recovery action |
| --- | --- | --- |
| Before or during uploads | Previous branch head. | Resume the same session or abort; partial uploads are not published. |
| After descriptor upload, before metadata commit | Previous branch head. | Resolve session; collect abandoned data after protection expires. |
| Metadata commit times out | Unknown to the caller. | Query the durable operation result before retrying or cleaning up. |
| Metadata commit succeeds, reply is lost | New Generation is published. | Return the recorded successful outcome on retry. |
| Live append reply is lost | The complete batch may be durable; no partial batch is accepted. | Resolve the append ID or retry the identical conditional request. |
| Old writer uploads after fencing | Objects may exist; branch and live log tails remain unchanged by that writer. | Reject its publication/append commits and expire orphaned preparation data. |
| Worker/cache disappears after checkpoint success | Published state remains durable. | Recover from the metadata authority and object store. |
| Coordinator is unavailable | No new authoritative head changes. | Retry after recovery; respect existing read-lease limits. |
| GC crashes during deletion | Published reachable data remains protected; the GC barrier stays closed. | Fence the old deletion executor and resolve pending deletes before reopening publication. |

The coordination service itself needs durable log/backup recovery. Restoring an
old metadata snapshot without reconciling later accepted publications and GC
state is unsafe. Its recovery protocol MUST prevent resurrecting references to
objects legitimately collected after that snapshot.

## 15. Cache and lazy reads

Cache keys include the authorization/encryption domain, object identity, and
relevant format version. A verified immutable object never requires coherence
invalidation because of a later Generation.

The cache requires capacity limits, eviction, integrity checks, and coordination
of concurrent downloads. A fill must not expose an incomplete object as a valid
cache entry. Any object known to be durably stored may be evicted.

Dirty overlay bytes and unuploaded checkpoint objects are not disposable cache
entries. They follow the selected mutation durability policy.

Lazy access fetches the descriptor, necessary manifest nodes, and demanded data.
It can reduce startup transfer toward the active working set. Full directory
materialization still transfers all selected content, and metadata traversal,
request latency, and chunk size remain part of cold-start cost.

## 16. Retention and garbage collection

### 16.1 Roots and leases

Retention roots include active branch heads, retained historical Generations,
live stream indexes and their retained ranges, explicit pins/tags, active
workspace bases, read leases, unresolved append/publication sessions, and future
global-checkpoint references.

Example policy:

| Class | Illustrative retention |
| --- | --- |
| All recent Generations | 24 hours |
| Hourly selections | 7 days |
| Daily selections | 30 days |
| Tagged Generation | Until unpinned |
| Deleted speculative branch history | A short configured grace period |

These are examples rather than v0 defaults. A branch remains a root until it is
explicitly deleted or expires under an authorized branch-lifecycle policy.

Opening a Generation MUST atomically verify retention and acquire protection
against collection. A read lease is a retention mechanism in addition to an
authorization check. A reader that cannot renew it must stop issuing unprotected
remote reads before expiry.

Lineage links are not state reachability edges. GC traces a Generation's manifest
graph, including all visible log segments and any native Git history required by
its view. It need not retain parent Generation descriptors merely because their
IDs appear in history metadata.

### 16.2 Conservative v0 collector

v0 SHOULD start with coordinated mark-and-sweep. Candidate age and a grace period
reduce risk but do not, by themselves, prevent a publisher from referencing an
old object between a reachability check and deletion.

The initial collector MUST use an exclusive coordination barrier:

1. Block new append/publication sessions, root additions, and new read
   protections that could change the live root set. Drain or resolve in-flight
   appends and publications.
2. Abort and fence abandoned sessions before excluding them from protection.
   An unresolved metadata outcome blocks deletion of the affected data.
3. Snapshot the authoritative retained roots, including existing reader and
   workspace-base protections, and traverse their complete object graphs.
4. Select old, unreachable candidates. Recheck eligibility while the barrier
   still excludes new references, then delete eligible objects.
5. Wait for all issued deletion requests to finish, record completion of the GC
   epoch, and release the barrier.

Existing protected reads can continue because their roots are included in the
mark. Local overlay work can continue, but new durable log appends and checkpoint
publication wait.
If the collector loses contact with the metadata authority, it MUST stop issuing
deletes. The barrier is durable state and MUST NOT automatically expire to admit
new references. Recovery must fence the old deletion executor and settle all
in-flight deletion requests before reopening publication. An unresolved delete
outcome keeps the barrier closed. Checking a lease before issuing a remote
delete does not fence a request already sent to the object store.

An implementation that needs automatic GC handover must additionally fence
actual deletions, for example through a storage protocol that distinguishes
physical object instances and cannot delete a newly reused instance. That
protocol needs its own correctness argument before replacing the conservative
recovery rule.

After the barrier, appenders and publishers cannot rely on an old existence
result for an unprotected object; they must ensure it again. Retained base subtrees remain
safe through their roots. Late uploads from a fenced abandoned session may
create garbage, but cannot publish references to it.

This blocking collector trades availability during collection for a smaller
correctness surface. Its cost must be measured. Online collection requires a
separate protocol for publication roots, object reuse, concurrent readers, and
root-set changes before it replaces the barrier.

Deleting a candidate is idempotent. Failed deletions are retried. Object-store
lifecycle policies MUST NOT independently delete live Version FS content.

## 17. Integrity, authorization, and encryption

All fetched manifest and data objects MUST be checked against their expected
identity before being used. Parsers also validate lengths, types, offsets, and
format versions. Metadata limits must prevent malformed graphs from exhausting
resources during reads or collection.

Authorization is evaluated at tenant, workspace, branch, and Generation
boundaries. Knowing an object digest does not grant access to the object.
Direct object-store URLs MUST be scoped to authorized content and time limited.
Workers must not receive unrestricted write access to the metadata authority.

The deployment SHOULD encrypt remote storage at rest and transport in transit.
Per-tenant or per-workspace envelope encryption MAY be added. Key versions and
physical object references must permit immutable storage and controlled key
rotation without silently changing logical content identity. Cache isolation
must follow the same tenant and key boundaries.

Lease fencing, content hashing, and authorization address separate concerns.
Correct content bytes do not authorize a caller, and an authorized caller still
needs a current writer epoch to publish.

## 18. Database boundary and global checkpoints

This section specifies an extension boundary rather than a v0 implementation.

```text
                     logical checkpoint coordinator
                                  |
                                 TSO
                          /               \
                    database           Version FS
                    snapshot           Generation
                          \               /
                           AgentCheckpoint
```

The database continues to manage its own MVCC, WAL, pages, indexes, transactions,
and snapshots. Version FS manages its immutable state graph and branches. A
database image imported as an artifact is still created and validated by that
database's own checkpoint API.

### 18.1 A timestamp does not select a consistent cut

A shared TSO provides ordering. To coordinate a checkpoint at T, the runtime must
establish an application boundary that assigns relevant work consistently,
drains operations included at or before T, and prevents later operations from
entering that captured state. Each participant must support the required
snapshot boundary or remain quiesced while it is captured.

Taking two unrelated snapshots and attaching the same timestamp does not
establish consistency. A filesystem write caused by a database transaction,
for example, must be assigned to the same side of the boundary as required by
the application's recovery contract.

If participants expose **contiguous** durable-prefix watermarks in the same
timestamp domain and scope, then:

```text
DB stable_ts  = 120
VFS stable_ts = 117

global_stable_ts = min(DB stable_ts, VFS stable_ts) = 117
```

Here `stable_ts` means all required operations through that value are resolved
and durable, with no holes. A maximum observed timestamp is insufficient. Even
a valid minimum watermark only bounds eligible checkpoints; the coordinator
still needs actual snapshots representing the requested common boundary.

### 18.2 Global publication

```text
AgentCheckpoint {
    checkpoint_id,
    workspace_or_agent_id,
    tso,
    database_snapshot_id,
    versionfs_generation_id,
    participant_retention_references,
}
```

The coordinator establishes the boundary, prepares both snapshots, waits for
durability, acquires durable retention references in both systems, and then
atomically publishes the global manifest in its authoritative catalog.

The global record is the visibility boundary for coordinated restore. If both
participants finish but the coordinator crashes before global publication,
their local snapshots remain valid without constituting a published global
checkpoint. Recovery resolves the coordinator operation before releasing
participant pins. Cleanup uses an idempotent recovery protocol; a timeout alone
is not permission to release state that a global record may reference.

This avoids rolling back a participant merely because its peer failed. It also
requires a durable coordinator record and explicit cross-system retention;
there is no implicit atomic transaction across the two engines.

Forking an AgentCheckpoint creates a branch or equivalent snapshot-derived
state in each participant. The coordinator publishes the new agent identity
only after both references are ready. A partially created fork can be retried
or cleaned up by operation ID.

### 18.3 Timestamped logs

Events MAY contain TSO values for correlation. Log sequence numbers establish
stream order; neither an arrival timestamp nor an S2-assigned timestamp is a
TSO. A Generation's exact log head remains authoritative for reproducibility.
If application timestamps can arrive out of order, filtering through T requires
a completeness watermark and cannot simply stop at the first event with a
larger timestamp.

## 19. API sketch

The API is conceptual. Omitted authentication, workspace scope, and request IDs
must be explicit in the eventual public interface.

```text
create_workspace(name) -> Workspace
delete_workspace(workspace, expected_revision)

get_generation(generation) -> Descriptor + ReadLease
pin_generation(generation, pin_name) -> Pin
unpin_generation(pin)

create_branch(name, retained_base) -> BranchState
get_branch(branch) -> BranchState
acquire_writer(branch, owner) -> WriterToken
renew_writer(token) -> WriterToken
checkpoint(token, expected_head, expected_revision, operation_id) -> Generation
get_operation(operation_id) -> Pending | Succeeded | Failed | Unknown

rollback(branch, expected_head, expected_revision, target, operation_id)
promote(source, expected_source_revision,
        destination, expected_destination_head,
        expected_destination_revision, operation_id)
delete_branch(branch, expected_revision, operation_id)

read_at(read_lease, path, range?) -> Bytes
list_at(read_lease, path, cursor?) -> Entries
stat_at(read_lease, path) -> Metadata

write(writer, path, data)
write_range(writer, path, offset, data)
remove(writer, path)
mkdir(writer, path)
rename(writer, source, destination)

open_log(branch, log) -> LiveLogLease + StreamBounds
append_log(writer, stream, match_seq_num, append_id, records) -> AppendReceipt
get_append_result(stream, append_id) -> Pending | Succeeded | Failed | Unknown
check_log_tail(live_log_lease) -> DurableTail
read_log(live_log_lease, cursor, limits, wait?) -> Records + NextCursor
read_log_at(read_lease, log, from_seq_num, limits) -> Records + NextCursor
trim_log(writer, stream, before_seq_num, operation_id)

diff(generation_a, generation_b, cursor?) -> Changes
materialize(read_lease, destination, selected_paths?)
capture_quiesced_directory(writer, source_directory)
```

Expected errors include `Conflict`, `StaleWriter`, `LeaseExpired`,
`GenerationExpired`, `StreamReplaced`, `CursorExpired`, `UnsupportedFeature`,
`IntegrityError`, `Unavailable`, and an explicitly unresolved operation outcome.
Error responses must distinguish
a rejected operation from an operation whose commit result is unknown.

`append_log` success returns a remotely durable batch receipt; local enqueue
returns a pending result. `checkpoint` success always follows the durable
Generation publication contract.

## 20. Invariants

| ID | Invariant |
| --- | --- |
| I1 | Published Generation descriptors and referenced logical objects never change. |
| I2 | A Generation's root resolves its complete declared state. |
| I3 | Resolving that state requires neither ancestor Generation replay nor an inter-Generation journal. |
| I4 | Every required object is durable before a Generation is published. |
| I5 | Branch head, publication catalog state, retention changes, and successful operation outcome move atomically. |
| I6 | Shared content remains durable while any live retention root or protected operation can reference it. |
| I7 | A stale writer epoch cannot publish after ownership changes. |
| I8 | Uploads and auxiliary markers alone cannot make partial state visible. |
| I9 | Losing worker compute or its cache cannot destroy successfully checkpointed state. |
| I10 | Database integration relies on explicit snapshot and retention APIs rather than database implementation details. |
| I11 | A retry cannot silently apply the same identified operation twice. |
| I12 | Hash identity never grants authorization. |
| I13 | A successful log append is a durable atomic batch with an identified sequence range. |
| I14 | Live stream trimming cannot change the log bytes visible in a retained Generation. |

These are release requirements. Performance optimizations may not weaken them.

## 21. Cost model and observability

Let N be namespace size, K the changed entries, D tree depth, and ΔB the new
data bytes after chunking. The intended behavior is:

| Operation | Intended work |
| --- | --- |
| Resolve one path | Traverse its manifest path and fetch demanded data. |
| Checkpoint | Upload ΔB plus changed manifest paths and bounded publication metadata. |
| Log append | Upload the batch and changed index paths, then atomically advance its durable tail and receipt. |
| Fork | One coordinated branch/root update; no data copying. |
| Rollback / promote | Coordinated pointer and retention updates; no data copying. |
| Diff | Traverse unequal nodes and emit changed entries; skip identical subtrees. |
| Full materialization | Read the selected namespace and all selected file bytes. |
| Initial GC | Traverse all live roots and candidate inventory under the barrier. |

Manifest work may be proportional to K times tree-path length, with shared paths
reducing duplication. Worst-case diff is linear in the affected namespace. A
directory import without a reliable change journal may scan O(N) metadata, and
hashing a changed file may read more bytes than its physical delta. Object
packing, compression, and compaction introduce additional amplification.

The implementation SHOULD report checkpoint capture/upload/publication latency,
new versus reused data and manifest bytes, append durability latency, tail/read
lag, cache hit rate, remote request count, lease/fencing failures, operation
outcomes awaiting resolution, retained bytes, GC pause duration, and deletion
failures.

Benchmarks must distinguish CPU work, bytes read locally, bytes uploaded,
object-store request counts, and metadata coordination latency. No fixed latency,
deduplication ratio, or million-workspace capacity is claimed by this RFC.

## 22. Alternatives and tradeoffs

| Alternative | Useful properties | Reason for the proposed model |
| --- | --- | --- |
| Complete workspace copies | Simple independent restore. | Copy cost grows with the base; persistent manifests preserve independence while sharing content. |
| Chains of Generation deltas | Small initial change records. | Recovery depth grows and ancestor retention becomes mandatory; state roots avoid that dependency. |
| Git as the entire storage model | Existing tooling and immutable source history. | Native Generations also cover logs, artifacts, frequent machine checkpoints, and filesystem metadata. |
| Only local filesystem snapshots | Fast local capture. | They can assist capture but do not define portable remote publication or branch coordination. |
| Multiple writers on one branch | Shared mutable namespace. | v0 uses independent branches to keep publication and recovery simpler. |
| Reference counting for every object | Potentially incremental reclaim. | Correct accounting across publication failures and shared graphs is deferred; v0 uses explicit roots and marking. |
| Online GC immediately | Fewer pauses. | Publication, object reuse, and reader races require a separate protocol before removing the initial barrier. |
| One engine for files and application DB state | One apparent checkpoint endpoint. | Separate engines can retain their appropriate execution models and coordinate through explicit snapshots. |

The main v0 tradeoffs are coordination-service availability, collection pauses,
filesystem capture barriers, and object-store overhead for small objects. They
are deliberate implementation constraints that must be measured before broader
deployment.

## 23. Implementation sequence and acceptance criteria

### Phase 1: Object graph and immutable reads

Implement versioned object encoding, checksums, persistent manifests, file/chunk
references, an object-store interface, and a bounded verified cache. A local
backend MAY support development, but it must not be presented as remote durable
storage.

Demonstrate two Generations sharing unchanged data and metadata. Retain the
newer root, remove unneeded lineage metadata, clear the cache, and still read all
of its declared state.

### Phase 2: Writable branches and publication

Implement overlays, capture boundaries, durable metadata transactions, epochs,
CAS revisions, operation identities, fork, rollback, and promotion. Add local
directory capture/materialization, durable batch appends, resumable log reads,
and sealed log segments.

Demonstrate preservation of writes after a checkpoint boundary, rejection of a
previous writer epoch, rejection of a stale revision after rollback, and correct
recovery after a successful publication whose reply was lost.

### Phase 3: Remote recovery, retention, and GC

Implement the S3 backend, reader/base protections, pins, retention policies, and
collection under the exclusive barrier. Recover a Generation on a fresh worker
without downloading unrelated file content.

Demonstrate that removing one branch does not delete shared content; active
readers and uncertain publication outcomes remain protected; and collecting
abandoned uploads never damages a retained Generation.

### Release validation

Before v0 claims these guarantees, fault-injection coverage MUST exercise upload
failure, incomplete multipart upload, metadata failure before/after commit,
lost replies, writer handover, rollback/promotion races, cache corruption, worker
loss, and GC interruption. Validation must include the publication/GC race and
an old operation retried after the branch has moved again.

Collection failure validation MUST include a collector that loses ownership
with remote deletion requests still in flight. Publication must remain blocked
until those requests can no longer remove newly referenced content.

Log validation MUST cover lost append replies, duplicate retries, stale writer
epochs, all-or-nothing batch visibility, appends racing checkpoint capture,
retained reads after live trim, and cursors across fork or rollback.

Performance evaluation SHOULD include a large mostly unchanged namespace, many
small files, large-file range edits, long segmented logs, repeated forks, and
many discarded branches. Results must disclose capture mode, storage backend,
chunk/node sizes, cache state, and durability settings.

These are proposed implementation and validation requirements. This RFC does
not assert that an implementation or its checks already exist.

## 24. Open decisions and follow-up RFCs

| Area | Decision required |
| --- | --- |
| Object and manifest format | Canonical encoding, hash domain, limits, version negotiation, compression. |
| Persistent tree | B-tree, radix tree, or HAMT; per-directory layout and target node size. |
| Large files | Chunk size/threshold, extent representation, sparse data, later content-defined chunking. |
| Metadata authority | Durable backend, atomic record boundaries, scale/partitioning, backup and recovery. |
| Writer sessions | Lease duration, renewal margins, failure handling, operation-result retention. |
| Local integration | SDK details, reliable directory capture, eventual mount semantics and supported metadata. |
| Logs | Record/batch limits, segment/index size, append outcomes, live read backpressure, and retention-safe trim. |
| Collection | Candidate grace period, barrier budget and recovery, deletion fencing, online collection protocol. |
| Durability | Optional remote WAL, autosave cadence, `fsync` contract, storage classes. |
| Git | Native Git object identity mapping, refs, protocol hosting, Generation associations. |
| Global checkpoints | TSO scope, application barriers, participant watermarks, durable coordinator and pin lifecycle. |

The storage-class choice must follow the promised failure model and measured
latency/cost. This RFC does not assume S3 Express is necessary or interchangeable
with S3 Standard.

An indicative roadmap is v1 for native Git integration, improved mounts and
diff tooling, autosave/journaling, and retention automation; v2 for coordinated
database checkpoints and broader execution integration. Semantic merge,
provenance indexes, and cross-region writable branches remain later proposals.
The ordering can change after v0 measurements.

## 25. Related work and references

The following sources informed the proposal. Their implementation choices and
performance claims are not Version FS guarantees. The design implications below
are this RFC's interpretation. Sources were consulted on 2026-10-22.

- [toy-kv-engine](https://github.com/ben1009/toy-kv-engine) provides an adjacent
  database design with independent checkpoints and an incremental backup
  repository. Its object sharing and publication/recovery boundaries are useful
  reference points. Application database integration should use its explicit
  checkpoint API rather than treating live database files as ordinary copies.
- [Tensorlake's repository](https://github.com/tensorlakeai/tensorlake) and
  [filesystem/Git product overview](https://www.tensorlake.ai/) describe agent
  sandboxes, versioned file trees, snapshots, forks, and Git interfaces. They
  motivate a shared storage substrate with separate execution and source-control
  views. Version FS's exact guarantees are specified independently here.
- [LayerFS](https://github.com/Ephemeral-AI-Lab/layerfs) explores writable agent
  workspaces over shared history and checkpointed changes. Its documented release
  profile also illustrates why sharing and durability must be evaluated
  separately; remote durable publication is an explicit Version FS requirement.
- [Code Storage](https://code.storage/) presents machine-oriented Git storage
  and APIs. It informs the need for programmatic access alongside compatibility
  with existing source-control tools, without requiring Git to represent every
  kind of Version FS state.
- [S2](https://s2.dev/docs/intro) supplies the reference for the log interface:
  ordered record streams, durable atomic batch appends, sequence cursors,
  conditional writes, replay, and live following. Its
  [architecture](https://s2.dev/docs/platform/architecture) also informs the
  separation between durable ingress and later segment batching. Version FS
  adds Generation-bound log views, branch writer enforcement, and retention
  rules that protect captured ranges from live-stream trimming.
- [Turso's durability architecture](https://turso.tech/blog/how-does-the-turso-cloud-keep-your-data-durable-and-safe)
  describes shared database segments, remote WAL durability, and lazy reads.
  [Turso's database overview](https://turso.tech/what-is-turso) supplies the
  database context. These are useful examples of separating local execution
  from remote durability while leaving database semantics inside the database.
- [GitHub's agent-scale infrastructure article](https://github.blog/engineering/architecture-optimization/the-bridge-to-agent-scale-git-infrastructure/)
  distinguishes immutable object storage from the smaller reference state that
  requires ordering. This supports the proposed separation of data transfer from
  branch coordination.
- [Anthropic's source-control redesign](https://www.anthropic.com/research/redesigning-source-control)
  discusses sparse clients, content-addressed storage, and inexpensive changes
  for agent workloads. Version FS explicitly chooses independently resolvable
  Generation roots; it does not adopt changeset replay as its state-read model.
- [chDB's durable layer](https://clickhouse.com/blog/chdb-durable-layer-for-agent-memory)
  describes explicit remote durability boundaries and single-writer recovery for
  an embedded analytical database. It informs the ownership and acknowledgement
  questions while reinforcing the need to state the database boundary.
- [Git Internals: Git Objects](https://git-scm.com/book/en/v2/Git-Internals-Git-Objects)
  documents Git's content-addressed object model. Native Version FS identity and
  Git identity must remain distinct unless the exact Git encoding is preserved.
- [Sapling](https://sapling-scm.com/) and
  [Mononoke](https://github.com/facebook/sapling/tree/main/eden/mononoke) provide
  additional source-control client/server designs and large-repository storage
  reference points.
- [Amazon S3 consistency](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html#ConsistencyModel)
  and [conditional write semantics](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html)
  define the initial remote backend's relevant primitives. They do not provide
  Version FS's multi-record publication or retention transaction.

The proposal succeeds if retained Generations become ordinary execution inputs:
cheap to create, share, inspect, fork, and restore after the worker is gone.
