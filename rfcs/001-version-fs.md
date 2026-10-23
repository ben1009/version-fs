# RFC-001: Version FS Architecture

| Field | Value |
| --- | --- |
| Status | Draft |
| Created | 2026-10-22 |
| Target | Version FS architecture |
| Scope | System model, state semantics, correctness, performance, scalability, and integration boundaries |

## 1. Summary

Version FS is a durable state system for agents and other workloads that create
many short-lived workspaces. Block volumes, filesystem state, Git state, ordered
logs, and artifacts participate in common version, checkpoint, fork, and retention
semantics.

The fundamental abstraction is a **Generation**: an immutable, complete logical
state rooted at a stable manifest. A retained Generation is independently
readable and restorable. It may share almost all physical data and metadata with
other Generations.

```text
                           Version FS
                       immutable Generation
                                |
           +----------+---------+---------+------------+
           |          |         |         |            |
         Block        FS       Git    Ordered logs  Artifacts
           |          |         |         |            |
           +----------+---------+---------+------------+
                                |
                     immutable object graph
                                |
              +-----------------+-----------------+
              |                                   |
          data plane                         control plane
       immutable objects               branches / refs / publication
```

The data plane stores immutable state. The control plane establishes which
state is published, which references may move, and which objects remain live.
The views can use different internal representations and storage engines while
participating in the same Generation boundary.

Version FS does not replace a transactional database or a process runtime. A
Global Timestamp Service (GTS) can provide common version coordinates across
process memory, database state, and Version FS. A future global checkpoint binds
their snapshots and Generation at a common boundary through explicit participant
interfaces.

The design principle is: **unify version semantics, not storage engines.**

This RFC defines the system and its correctness contract across releases.
Release scope, implementation mechanisms, sequencing, and acceptance criteria
belong in separate implementation proposals.

## 2. Motivation

An agent may start on ephemeral compute, create thousands of files, produce
large build outputs, fork several candidates, and retain only one successful
result. A session may need to resume on another host or inspect the state before
a failed tool call.

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

Copying a complete workspace for every checkpoint makes storage and startup
cost proportional to the workspace. Keeping its only durable copy on the worker
makes host failure a state-loss event.

The execution model should instead be:

```text
ephemeral compute + local working state + durable versioned state
```

Historical state becomes an ordinary execution input: it can be read, forked,
reviewed, restored, or promoted. A backup is one use of a Generation; a
Generation also serves as a live execution base.

The desired checkpoint cost follows changed data and metadata. Fork should
primarily create references. These are architectural objectives, not measured
performance claims.

## 3. Goals and system properties

The terms **MUST**, **MUST NOT**, **SHOULD**, and **MAY** describe this
architecture's contract. They do not select a release's feature set or a
particular implementation mechanism.

### 3.1 Core properties

1. Generations are immutable, complete, and independently readable.
2. Unchanged data and metadata can be structurally shared.
3. A checkpoint atomically publishes one coherent logical state.
4. Fork and rollback primarily manipulate references rather than copy state.
5. Published state and acknowledged durable live state survive loss of worker
   compute and local caches under their declared retention and durability contracts.
6. Files, block volumes, Git, ordered logs, and artifacts share version, checkpoint,
   fork, and retention semantics.
7. Storage state and application databases remain separate systems.
8. Retention protects the complete reachable state of every retained Generation.
9. Concurrent or stale operations cannot overwrite newer authoritative state.
10. Content integrity and authorization are independently enforced.

The performance and scalability objectives in Section 21 describe how these
properties should remain practical as state, histories, and concurrency grow.

### 3.2 Interfaces and views

| View | Logical state | Relationship to a Generation |
| --- | --- | --- |
| Filesystem | Namespace, file contents, and declared metadata. | A complete immutable namespace view. |
| Block volume | Logical byte ranges, geometry, and declared I/O ordering/durability. | A complete immutable volume root, independent of its consumer's filesystem format. |
| Git | Source objects, refs, and source-control history. | A coherent declared Git view; machine checkpoints need not become Git commits. |
| Ordered logs | Durable ordered records. | An exact immutable captured range for each included log. |
| Artifacts | Named or referenced immutable payloads and outputs. | Explicit reachable content governed by Generation retention. |

Each view defines its own domain operations. Their common contract concerns
version identity, coherent capture, publication, branching, and retention.
One shared physical format or storage engine is not required.

A block-volume view may host a filesystem or a block-aware engine. Filesystems
are consumers of that interface; native ordered logs and artifacts remain
independent first-class views. A release may realize filesystem access through
such a consumer without implementing a native file-tree adapter.

A Generation MUST declare the authoritative representation of each logical view.
When filesystem or Git state is realized through a block volume, its consumer
view does not automatically create a second independent authoritative root.
Native representations and derived views must agree with that declaration;
divergent representations cannot both claim authority for the same logical state.

### 3.3 Non-goals

Version FS does not implement SQL, relational constraints, query optimization,
row-level MVCC, or application database transactions. A metadata service may use
a database internally without making Version FS an application database.

A Generation captures persisted Version FS state. A process runtime owns capture
and restore of process memory and execution context; its snapshot can participate
in the shared version described in Section 18. External connections and service
state require their own explicit recovery contracts. Restore does not promise
exactly-once execution of already-issued external actions.

Git's internal storage format does not define native Version FS storage.
Full POSIX compatibility and automatic semantic merge are not prerequisites for
the Generation model; adapters must declare the semantics they actually support.

### 3.4 Extension boundaries

Release RFCs choose supported adapters, concurrency policies, storage backends,
and durability modes. Mounts, sparse access, mutation journals, multi-writer
coordination, semantic merge, and remote execution can extend this model without
weakening its invariants.

Coordinated process and database checkpoints are a separate extension. The
architecture supports a shared global timestamp domain for their logical
versions. Their protocol needs its own specification of participant boundaries,
ordering, and retention.

## 4. Terminology

| Term | Meaning |
| --- | --- |
| Object | Immutable stored content or metadata. |
| Manifest | Metadata resolving names and logical ranges to immutable state. |
| Root manifest | The entry point for one Generation's complete state graph. |
| Generation | A published, immutable, independently readable logical state. |
| Workspace | An authorized namespace of branches and retained Generations. |
| Branch | A named, independently advancing authority domain with a published Generation head and optional durable live-view state. |
| Branch head | The branch's movable reference to a published immutable Generation. |
| Live view state | Authoritative durable view roots between Generations, recoverable independently of worker-local changes but not themselves a Generation. |
| History (agent history) | A stable identity for one coordinated version lineage; forking a coordinated version creates a distinct descendant history identity. |
| Overlay | Mutable worker-local changes above selected Generation or live-view state; acknowledgement and recovery guarantees are declared by the adapter. |
| Block volume | A logical address space whose complete bytes are resolved by a versioned immutable root. |
| Checkpoint | Capture and durable publication of a coherent state boundary. |
| Publication | The authoritative transition making a prepared Generation visible. |
| Pin | A retention reference keeping a Generation's reachable state live. |
| Log position | A location within one ordered log history. |
| Global Timestamp Service (GTS) | A global time authority providing comparable version coordinates under a declared ordering contract. |
| GlobalTimestamp | An abstract global version coordinate T, interpreted in its declared timestamp domain. |
| Coordinated version | An immutable binding of a GlobalTimestamp to captured participant state, scoped by timestamp domain and stable history ID. |
| Runtime snapshot | Captured process memory and execution context, possibly including its containing sandbox or VM. |

Generation identity, object identity, log position, branch concurrency state,
and optional global version coordinates serve different purposes. A coordinated
version can select a retained Generation or participant snapshot without
replacing its native identity. A local wall-clock reading alone does not
establish a coordinated version.

## 5. Architecture and authority

```text
                        Agent runtime
                              |
                adapters and domain interfaces
                   /                    \
       immutable state access       mutable working state
                   \                    /
                    capture and publication
                      /               \
                 data plane        control plane
                 immutable         authoritative refs,
                 object graph      visibility, retention
```

The data plane holds durable immutable objects, manifests, and Generation roots.
Different subsystems may store different kinds of objects and use separate
physical engines. Every required edge in a published Generation must nevertheless
resolve under that Generation's retention and authorization contract.

The control plane determines authoritative branch heads and durable live-view
roots, published Generations, write authority, and retention. Mutations that
establish publication or replace an authoritative reference MUST have one atomic,
durable outcome. Their related visibility and retention effects must agree with
that outcome.

The system requires both planes for recovery. Worker caches are disposable;
authoritative reference and retention metadata are durable system state.
Finding an uploaded descriptor in object storage is not evidence of publication.

The architecture does not require a particular metadata backend, transaction
layout, writer count, or lease protocol. Each implementation must explain how
its chosen mechanisms satisfy the publication, concurrency, and retention
invariants.

## 6. Generation model

### 6.1 Identity, root, and lineage

Conceptually, a Generation identifies its immutable root, declared views,
capture boundary, and lineage. These are logical relationships rather than a
wire format:

```text
Generation G102
    +-- parent_generation_id: G101    (lineage only)
    +-- root (declares authoritative representations)
          +-- block volume root, when included
          |     +-- consumer filesystem / Git views, when represented here
          +-- native filesystem / Git roots, when separately declared
          +-- exact log views
          +-- artifacts
```

`parent_generation_id` describes history. It MUST NOT be required to resolve
the child's file contents, metadata, Git view, logs, or artifacts. Retaining a
child does not by itself require retaining its parent descriptor.

```text
G102 -> root -> complete state
```

State resolution MUST NOT require:

```text
G100 + delta101 + delta102 -> reconstructed G102
```

Distinct Generations may describe identical state and share the same root.
Generation identity does not imply a timestamp, a Git commit, or a content hash.
A Generation MAY also be addressed through a scoped global version coordinate
that binds to its immutable identity and root, as described in Section 18.

A published Generation MAY be detached, with no branch pointing to it. Its
visibility and retention protection are established independently of branch
references. It is readable by identity while retained and authorized, and may
later be selected by a branch or referenced by a future global checkpoint.

### 6.2 Logical completeness and independent restore

Every required state edge MUST be reachable through the Generation's declared
roots. Shared immutable nodes may be reached directly from many Generations.
Resolution can traverse a tree or graph; it cannot require replaying Generation
ancestry or an inter-Generation mutation journal.

Independent restore means a fresh authorized worker can recover the retained
Generation using its root graph and the durable services protecting it. It does
not require eagerly downloading all content or packaging the state into one
object.

External package registries and services remain outside the captured state
unless their required bytes are imported. A view must declare which dependencies
belong to the Generation and which remain external.

## 7. Immutable objects and persistent manifests

Objects contain immutable data or metadata. Their identities support stable
references, integrity verification, and reuse. Content addressing is useful for
deduplication; its digest, encoding, and physical key policy are implementation
decisions.

Persistent manifests share unchanged subtrees. Updating one leaf creates the
changed path while retaining other nodes:

```text
          root G100                    root G101
          /       \                    /       \
      shared A     B               shared A     B'
                 / \                           / \
                C   D                         C'  D
```

Logical completeness does not require a metadata copy for every Generation.
Large directories, file ranges, and log indexes should also permit structural
sharing. The exact tree structure, node sizes, chunking policy, and packing
strategy belong in implementation RFCs.

Deletions are represented in the new complete state. A reader must not need to
consult an ancestor's tombstones. A large file may reuse unchanged ranges,
without committing the architecture to a particular chunk size or algorithm.

## 8. Filesystem view and overlay

The filesystem view exposes a namespace of files, directories, and declared
metadata. An adapter must state its supported operations and metadata. It MUST
NOT silently discard required semantics while claiming an exact checkpoint.

Mutable working state may be represented as an overlay over a selected
Generation or the branch's durable live-view state:

```text
read(path) -> overlay entry, including deletion
           -> selected Generation / live-view base on overlay miss
           -> verified local or remote object
```

A checkpoint chooses a coherent boundary across supported namespace operations,
file contents, and other included views. Operations after that boundary belong
to later state. An application requiring atomicity across several operations
must establish the necessary application boundary.

The architecture permits a native SDK, materialized directories, FUSE,
virtio-fs, or other adapters. Every capture method must explain how it obtains a
coherent view; a directory walk racing arbitrary writes cannot claim that
property. Capture of a live database image requires that database's own
snapshot or quiescence contract.

A block adapter must declare its flush, ordering, durability, and capture cut.
Generation immutability applies to the captured volume root. A filesystem freeze
or recovery protocol belongs to the filesystem consumer; application consistency
requires the application's own coordination. A block snapshot or shared timestamp
alone cannot establish that boundary. Recovery must not modify a retained root.

## 9. Log and artifact views

### 9.1 Ordered logs

A log is a first-class ordered state view. [S2 streams](https://s2.dev/docs/concepts/streams)
and [durable appends](https://s2.dev/docs/concepts/appends) are the reference
model for durable ordered records. S2 is an architectural reference rather than
a required service dependency.

A successful log append establishes a durable, ordered, atomic set of records.
It does not by itself publish a Generation or make filesystem changes durable.
Log order, Generation identity, and optional global timestamp ordering remain
distinct.

### 9.2 Generation-bound log semantics

A Generation includes an exact immutable log view: a historical prefix, or the
declared retained range within that prefix. It must resolve those records
without depending on the current writable stream or replaying earlier
Generations. Later appends cannot change what that Generation exposes.

Checkpoint capture must select the records belonging to its common logical
boundary. A captured range cannot cut an atomic append in half or include later
records simply because they are already visible to live readers.

A fork shares its historical prefix; descendants append independently. Rollback
selects an earlier immutable view. Promotion selects the source's captured log
view. None of these operations implicitly interleaves divergent histories.

Trimming a live log MUST NOT invalidate records visible in any retained
Generation. An external stream reference alone is insufficient if its retention
can remove required bytes. The implementation must preserve those bytes or
establish an equivalent retention guarantee.

### 9.3 Artifacts

Artifacts are immutable payloads such as build outputs, tool results, or large
log attachments. They can be named in the filesystem or through an artifact
view. Their required references participate in the Generation's reachable graph
and retention contract.

An application-visible hash inside an opaque payload does not automatically
create a protected graph edge. A view that requires an artifact must declare
that dependency so restore and collection preserve it.

## 10. Branches and write authority

A branch represents an independently advancing history without requiring a
complete physical copy of its data. Its head references a published immutable
Generation. It MAY also own authoritative durable live-view state between
Generations, such as a block-volume root or a live log root newer than its head:

```text
branch
    +-- head Generation: G10
    +-- durable live volume root: V15
    +-- durable live log root: L18

worker-local mutations
    -> acknowledged durable live-view boundary
    -> coherent captured and published Generation
```

These are distinct durability and visibility boundaries. A durable live update
does not by itself publish a Generation or promise a common cut across views.
The branch head continues to select its published Generation until an explicit
head update. Worker-local overlay state is not authoritative durable live state
merely because its bytes exist on the worker.

Current durable live roots and their required dependencies MUST remain protected
and recoverable under their acknowledgement contract, even without a Generation
capturing them. Replacing a live root does not automatically retain the previous
live boundary as a historical version; retained Generations provide that guarantee.

Authoritative updates must validate current write authority and the state on
which the operation depends. A stale writer MUST NOT overwrite newer
authoritative state or append to a history after its authority has been
replaced. Reference replacement must also account for deletion and recreation,
or movement away from and back to the same Generation.

Writer counts, lease policies, epochs, and concurrency tokens are mechanism
choices. A release may serialize each branch's writers or implement a broader
coordination policy while preserving the same authority contract.

## 11. Checkpoint and publication semantics

### 11.1 Preparation and visibility

```text
select coherent state boundary
              |
prepare immutable objects and complete root
              |
establish durability of all required state
              |
atomic authoritative publication
              |
Generation becomes visible
```

**Upload != publication.** Data, manifests, or descriptors may exist before the
Generation becomes visible. Prepared state alone is not a published Generation.
Publication must establish that its complete required graph is durable and
protected, including references to reused state.

The publication boundary atomically establishes the Generation's authoritative
visibility, required retention protection, and operation outcome. Publication
does not require moving a branch reference.

An operation MAY combine publication with a conditional branch update. When it
does, publication and that reference update MUST succeed or fail atomically.
A later, separate branch operation can also select an already published
Generation. Detached publication supports independently retained snapshots and
future global checkpoints without changing a workspace branch.

A successful checkpoint means both state and publication are durable. A pending
asynchronous capture cannot report the same successful completion before those
conditions hold.

### 11.2 Failure and retry

Before Generation publication, a failed capture leaves the prior published
Generation and branch head in place. Separately committed durable live-view
updates remain authoritative according to their own outcomes; capture failure
does not undo them. After publication, the new Generation remains authoritative
even if the worker crashes or loses the reply. Auxiliary preparation or commit
markers do not override the authoritative outcome.

A timeout can leave the outcome unknown to the caller. Recovery must distinguish
an unknown result from a rejected operation before retrying or releasing
protection. Retrying an identified operation must not silently perform its
logical mutation twice.

Publishing one boundary must not discard writes outside that boundary. A
failed or stale attempt must preserve unpublished work for explicit recovery
under the adapter's contract.

## 12. Fork, rollback, promotion, and deletion

| Operation | Meaning |
| --- | --- |
| Fork | Create a new branch from a retained Generation, sharing existing state. |
| Rollback | Move the branch head to an earlier retained Generation. |
| Promote | Conditionally select a source Generation as another branch's head. |
| Delete branch | Remove its authoritative head/live references and release its retention interest under the declared live-state policy. |

Rollback moves a reference; it does not rewrite history. Promotion must reject
an update when the destination or other declared preconditions have changed.
Selecting a speculative result does not imply a semantic merge of independent
edits.

Rollback, adoption, promotion, and deletion MUST define whether divergent durable
live state is preserved, rejected, or explicitly discarded. They MUST NOT discard
it implicitly. Promotion of a published source Generation does not implicitly
promote that source's newer live state. A release must state how selected heads,
live views, and worker-local state are reconciled.

An external reference change does not automatically rewrite a running process's
mounted volume or local namespace. Its unpublished work must be reconciled before
it can become authoritative. Deletion does not immediately erase shared content
protected by other branches, retained Generations, pins, or readers.

A future merge may construct another complete Generation using rules specific
to files, Git, logs, and artifacts.

## 13. Git view

Git commits and native Generations serve different histories. Machine
checkpoints may be frequent while user-facing source commits remain selective:

```text
Generations: G100 -> G101 -> G102 -> G103 -> G104
Git commits:   C1 --------------------------> C2
```

The Git view represents coherent source-control state. It may use captured Git
repository files or a native adapter for Git objects and refs. A Generation must
declare one authoritative representation; divergent repository files and native
refs cannot both claim authority.

Git compatibility must preserve Git's object encoding, identity, and ref
semantics. A native Version FS object identity is not automatically a Git object
ID, and an arbitrary filesystem tree is not automatically a Git tree. See
[Git's object model](https://git-scm.com/book/en/v2/Git-Internals-Git-Objects).

Clone, fetch, push, checkout, commit, and branch are interfaces over the relevant
state. Neither every Generation becoming a Git commit nor a one-to-one mapping
between commits and Generations is required.

## 14. Durability and recovery

Successfully checkpointed state must survive loss of the worker, overlay, and
cache. Durability of published state requires both its immutable graph and the
authoritative metadata establishing publication and retention.

Working mutations may have weaker durability than checkpoints. Each adapter
must state what acknowledgement protects and what host loss can discard.
A durable live log append and a coherent filesystem checkpoint are separate
boundaries; surviving event records do not implicitly reconstruct filesystem
changes.

An adapter may also acknowledge durable live state newer than the branch head.
Recovery of the current branch resumes from those authoritative live roots,
whereas opening a retained Generation selects that Generation's exact roots.
Losing a worker cannot silently roll acknowledged live state back to its last
Generation; later authorized replacement or explicit discard follows Section 12.

Restore resolves a retained Generation, obtains protection for access, opens its
root graph, and fetches required content. It requires no prior worker disk and
no replay of ancestor Generations. Optional recovery journals protect work
between checkpoints without becoming dependencies of published Generation reads.

Missing or corrupt required content is an integrity failure. Recovery MUST NOT
silently substitute an older Generation, empty content, or a partial state.
Recovery of the control plane must also avoid resurrecting references to content
already legitimately collected.

## 15. Cache and lazy access

Immutable content can be cached under stable object identity within its
authorization and encryption domain. Later Generations do not require coherence
invalidation for already verified immutable bytes.

A local cache is disposable. Partially downloaded content cannot be treated as
valid, and dirty working bytes are not equivalent to durable cached objects.

Lazy access resolves required metadata and fetches data as demanded. It lets
startup transfer follow the active working set rather than the full workspace.
A complete export still transfers all selected bytes. Request latency and
metadata traversal remain real costs.

## 16. Retention and garbage collection

Retained Generations, branch heads and current durable live roots, pins, protected
readers, and protected operations define liveness. Shared content remains live
while any such reference can require it. Retention applies to all views, including
log ranges and required artifact dependencies.

A retained coordinated version protects its Version FS Generation even when no
branch points to it. Other participants protect their own captured state under
the shared retention contract described in Section 18.

Lineage references do not automatically retain ancestor Generation descriptors.
State reachability follows each retained root's complete declared graph.

Opening retained state must acquire protection consistently with collection.
Publication and collection must not race in a way that deletes content being
made authoritative. Unknown operation outcomes must be resolved before releasing
protection that a successful operation could require.

Collection may reclaim content only after it is no longer required by any live
root or protected operation. Age or a grace period alone is not proof of that
condition. The collector must also remain safe through interruption, ownership
changes, and delayed deletion outcomes.

The architecture does not choose reference accounting, mark-and-sweep, blocking
collection, or online collection. Each mechanism must preserve independent
readability of every retained Generation.

## 17. Integrity, authorization, and encryption

Manifest and data content must be verified against the expected integrity
identity before use. Parsing must reject malformed types, lengths, references,
and unsupported formats rather than silently changing the declared state.

Authorization is separate from object identity. Knowing a digest MUST NOT grant
access to content. Workspace, branch, Generation, and direct object access must
respect the applicable authorization and retention boundaries.

Encryption and cache isolation must preserve those boundaries. Digest
algorithms, key management, signed access mechanisms, and encoding versions are
selected by implementation and deployment specifications.

## 18. Global timestamps across process memory, databases, and Version FS

### 18.1 Global time authority and required semantics

A coordinated checkpoint MAY use a shared Global Timestamp Service (GTS), also
called a global time authority, to provide a common version coordinate. Version
FS requires the following semantics from that service and its time adapter:

| Property | Required semantics |
| --- | --- |
| Global comparability | Participants compare coordinates in the same declared timestamp domain using one ordering contract. |
| Strict history ordering | Finalized coordinated versions within one history MUST receive distinct, strictly increasing coordinates, including across service handover and recovery. |

`GlobalTimestamp` denotes the resulting abstract version coordinate. Its encoding,
selection protocol, and treatment of uncertainty belong to the provider adapter;
the architecture fixes no integer width or timestamp-allocation API. Values from
different time authorities are not automatically comparable.

The coordinator and participants establish the common checkpoint boundary using
their snapshot and visibility contracts. GTS supplies coordinates and ordering;
it does not establish the consistency of captured participant state.

**Version FS depends on timestamp semantics, not on a particular timestamp provider.**

### 18.2 Provider adaptation examples

The following are possible provider models, with different native semantics:

| Provider model | Native capability | Adaptation to GlobalTimestamp |
| --- | --- | --- |
| [TiDB / PD TSO](https://docs.pingcap.com/tidb/stable/tso/) | Single-valued increasing timestamps combining physical milliseconds and a logical counter. | Preserve the oracle's domain and ordering guarantees when exposing T. |
| [TrueTime-style service](https://research.google.com/archive/spanner-osdi2012.pdf) | Time uncertainty intervals such as `[earliest, latest]`. | Select a coordinate and satisfy the provider's uncertainty and ordering rules before finalizing it. |
| [AWS Time Sync](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/set-time.html) plus an ordering layer | Synchronized UTC clock readings. | Add a timestamp allocator or an HLC-based protocol with sufficient coordination and fencing to meet the GTS contract. |

Spanner combines TrueTime with its transaction protocol, including
[commit wait](https://docs.cloud.google.com/spanner/docs/whitepapers/life-of-reads-and-writes),
to provide external consistency. A TrueTime interval must not be reduced to an
arbitrary scalar while discarding the uncertainty on which ordering depends.

AWS Time Sync is clock infrastructure; it does not by itself establish this
RFC's timestamp-oracle contract. An adapter must demonstrate ordering across
participants and through failover; local clock or local HLC monotonicity alone
is insufficient. These are integration requirements of this RFC. Another global
time service with equivalent guarantees can also satisfy them.

### 18.3 Shared version bindings

A `GlobalTimestamp` T MAY serve as the logical version of process memory,
database state, and a Version FS Generation. The published version binds T to
their native snapshot and storage identities. That binding MUST be immutable
and unambiguous within its timestamp domain and agent history, including forks.
Selecting a historical version does not rewind the time authority.

Branch names may refer to histories but do not define their identities. A
history ID MUST NOT be reassigned to another lineage, including after branch
deletion or name reuse. A coordinated version is conceptually identified by:

```text
GlobalVersionKey {
    timestamp_domain,
    history_id,
    timestamp,
}
```

Strict history ordering makes this key unique for each finalized coordinated
version. Different histories MAY use the same timestamp. Retrying the same
finalized version preserves its existing key and immutable binding.

```text
                  Global Timestamp Service
                             |
                   GlobalTimestamp T
                             |
           AgentCheckpoint(domain, history_id, T)
                   /         |         \
          Process @ T      DB @ T     Version FS @ T
          snapshot M      snapshot D   Generation G
                                           |
                               Block / FS / Git / Logs / Artifacts
```

Within its timestamp domain and agent history, a checkpoint including all three
participants conceptually binds:

```text
Checkpoint {
    timestamp: T,
    runtime_snapshot: M,
    db_snapshot: D,
    generation: G,
}
```

T is the common version coordinate. Lookup MUST use its timestamp domain, stable
history ID, and timestamp to resolve M, D, and G through the immutable binding.
An unscoped T is not a globally unique checkpoint identifier. Participants may
expose T as their logical snapshot version under the same scoped contract. This
is an optional integration capability; Generations can exist without a global
timestamp. These structures describe semantic identities and relationships;
they do not prescribe a wire format.

### 18.4 Participant state and ownership

| Participant | Meaning of version T | Owner of capture and restore |
| --- | --- | --- |
| Process runtime | A consistent snapshot of process memory and required execution context at the boundary. | Runtime, sandbox, or VM checkpoint adapter. |
| Database | An engine-confirmed snapshot at the boundary, using a supported MVCC read version or a snapshot bound to T. | Database engine. |
| Version FS | An exact immutable Generation at the boundary, including its declared log ranges and artifacts. | Version FS. |

The runtime declares which memory, threads, registers, and resources its snapshot
can restore, along with environment compatibility. Its file mappings and handles
must agree with the selected Version FS state. If a database runs inside the
captured process, its memory and durable snapshot must also form an
engine-consistent restore boundary. A filesystem checkpoint alone does not
capture this execution state.

Runtime snapshot images MAY be stored as Version FS artifacts. Their bytes then
follow Generation integrity and retention rules; the runtime still owns their
meaning and restore procedure. The database continues to own its transactions,
indexes, snapshots, and recovery. Sharing T does not require a shared engine.

### 18.5 Tensorlake as the process-memory reference

[Tensorlake's Snapshot and Clone model](https://docs.tensorlake.ai/sandboxes/snapshots)
provides the reference for process-memory semantics. It distinguishes filesystem
snapshots, restored by cold boot, from memory snapshots that restore filesystem,
memory, and running processes. A snapshot can outlive its source sandbox and be
restored into new sandboxes. Its [lifecycle model](https://docs.tensorlake.ai/sandboxes/lifecycle)
separately supports suspending and resuming the same sandbox.

For Version FS integration, this motivates the following semantics:

| Capability | Relationship to versioned state |
| --- | --- |
| Suspend / resume | Pause and continue one runtime identity; it need not publish a reusable coordinated version. |
| Durable memory checkpoint | Bind a reusable runtime snapshot to the captured filesystem Generation and optional database snapshot at T. |
| Restore / fork | Start independent runtime descendants from a retained version while preserving the source snapshot. |

The runtime participant MAY capture the containing sandbox or VM rather than
serialize each process separately. If its image also includes filesystem state,
the adapter MUST bind that state to the selected Generation at T; two divergent
filesystem representations cannot both be authoritative during restore.

Tensorlake also describes [incremental memory and disk snapshots](https://www.tensorlake.ai/blog/multi-cloud-scheduling).
This informs sharing of unchanged checkpoint data. Each retained coordinated
version must protect the complete dependency closure needed by its runtime
snapshot, including shared data and required base images. This preserves
recoverability without prescribing a runtime's physical representation.

These are Version FS design implications. The global timestamp binding, database
coordination, and retention contract in this RFC are not claims about Tensorlake's
APIs.

### 18.6 Consistency, durability, and retention

**A shared timestamp alone does not create a consistent snapshot.** The
participants must establish the same application boundary, including causally
related memory changes, database transactions, filesystem writes, and log records.
Allocating T or attaching it to unrelated snapshots does not establish that cut.

A checkpoint declares its participants and can claim durable coordinated restore
only after their captured states and retention bindings are durable. A volatile
memory image or an unprotected MVCC read timestamp alone is insufficient. A
checkpoint without a runtime snapshot can restore declared data state but cannot
claim to resume the process from that version.

Opening a retained coordinated version resolves its scoped timestamp binding.
Unavailable or expired participant state must be reported rather than silently
substituting another version. Forking that version derives an independent history
from those states while preserving the original version's identity and protections.

Participant snapshots remain valid on their own if global publication fails;
they are not automatically a published coordinated checkpoint. A Version FS
Generation can be published detached for this purpose without moving a branch.
Provider adaptation, completeness watermarks, runtime capture mechanisms, and
coordinator recovery require a separate integration protocol.

## 19. Conceptual operations

These names express capabilities and their semantics, not function signatures,
request parameters, or a release checklist:

| Operation | Meaning |
| --- | --- |
| Checkpoint | Publish one coherent durable Generation. |
| Open Generation | Access retained immutable state under protection. |
| Fork | Start an independent branch from retained state. |
| Rollback | Select an earlier retained state for a branch. |
| Promote | Conditionally select a branch's captured result. |
| Read / write | Access a declared view or mutate working state. |
| Append / read log | Durably extend a log or read its live/captured view. |
| Pin / unpin | Add or release explicit retention interest. |
| Open coordinated version | Resolve participant state using its timestamp domain, stable history ID, and GlobalTimestamp. |

Conflict detection, retry identity, cursor formats, and concrete API parameters
are defined by implementation RFCs. Coordinated version access belongs to the
optional integration described in Section 18.

## 20. Invariants

| ID | Invariant |
| --- | --- |
| I1 | Published Generations and their referenced logical objects never change. |
| I2 | A Generation's roots resolve its complete declared state. |
| I3 | State resolution requires neither ancestor Generation replay nor an inter-Generation journal. |
| I4 | Every required object is durable before a Generation is published. |
| I5 | Publication atomically establishes visibility, required retention protection, the operation outcome, and any reference updates included in that operation; a branch update is optional. |
| I6 | Shared content remains durable while any live retention root or protected operation can require it. |
| I7 | A stale writer cannot mutate authoritative state after its authority has been replaced. |
| I8 | Uploads and auxiliary markers alone cannot make partial state visible. |
| I9 | Loss of worker compute or its cache cannot destroy retained Generations or current acknowledged durable live state under their declared contracts. |
| I10 | Runtime and database integration use explicit participant contracts and preserve each system's capture and recovery responsibilities. |
| I11 | A retry cannot silently apply the same identified logical operation twice. |
| I12 | Content identity never grants authorization. |
| I13 | A successful log append atomically establishes an identified ordered range of durable records. |
| I14 | Live trimming cannot change log bytes visible in a retained Generation. |
| I15 | A published coordinated global version binds its declared participant states at one common boundary and protects all state required for the promised restore. |

These are architecture requirements across releases. Implementation choices and
performance optimizations must preserve them. Release specifications must
define the evidence that demonstrates these guarantees.

## 21. Performance and scalability

### 21.1 Workload dimensions

Version FS targets frequent checkpoints over large shared bases and many
independent agent histories. Capacity and performance have several dimensions:

| Dimension | Sources of growth |
| --- | --- |
| Bytes | Files, artifacts, runtime images, and retained changed content. |
| Metadata | Namespace entries, directory fanout, manifest nodes, and log indexes. |
| Versions | Branches, retained Generations, pins, and coordinated checkpoints. |
| Concurrency | Readers, writers, simultaneous checkpoints, and fork/restore bursts. |
| Churn | Log ingestion, small writes, branch lifecycle, and abandoned preparations. |

Large data capacity, large metadata capacity, and high operation concurrency
are distinct objectives. Scaling independent histories also differs from
increasing throughput for one heavily contended history.

### 21.2 Architectural cost model

Let `delta_data` be new encoded content at the selected storage granularity,
`delta_metadata` the immutable metadata rewritten for a capture, and
`working_set` the metadata and content demanded by an execution.

| Operation | Architectural cost objective |
| --- | --- |
| Checkpoint | New storage follows `delta_data + delta_metadata` plus Generation/publication metadata. |
| Fork | Create references and retention state without copying or traversing the complete base. |
| Rollback / promote | Move authoritative references without rewriting immutable history. |
| Open / read Generation | Resolve roots and access the `working_set` through selective metadata traversal. |
| Diff | Skip shared identical subgraphs and visit changed state and reported differences. |
| Append / capture log | Store new records and index changes; capture binds existing immutable ranges. |

Structural sharing does not automatically make change discovery incremental.
A capture from a materialized directory can scan the entire namespace, and a small
edit can rewrite a whole file when its representation has no range sharing.
The cost model must distinguish application changes from bytes scanned,
transferred, or newly stored, including metadata and request amplification.

Checkpoint completion includes coherent capture, preparation, durability, and
atomic publication. Capture pause and total completion time are distinct.
Independent preparation should not serialize unrelated publications, while
later writes must remain outside the captured boundary.

In the worst case, diff visits the complete compared namespace and requires content
reads. Full materialization or export reads all selected state. Structural
sharing and lazy access do not remove those costs.

### 21.3 Read and metadata scalability

Opening a retained Generation SHOULD require only its roots and relevant
metadata, without enumerating the complete namespace. Lookup depth SHOULD
follow the selected namespace/index structure rather than the number of earlier
checkpoints; reads must not replay a checkpoint chain.

After obtaining an authorized, protected Generation handle, immutable data and
metadata reads SHOULD proceed independently of branch reference serialization.
Resolving a current mutable head remains an authoritative operation. Cached
immutable content does not establish that a branch reference is current.

Data access and bulk transfer capacity SHOULD scale independently of authority
capacity. Large payloads should not be forced through one global metadata
leader. Namespace and range access should remain selective as metadata grows,
while preserving integrity, authorization, and retention.

### 21.4 Coordination scalability

Authority SHOULD scale across workspaces and independent reference domains.
Independent branches SHOULD advance without a global commit lock. A hot branch
or log remains an ordering domain; adding capacity elsewhere does not remove
its serialization requirements.

Operations spanning authority partitions MUST preserve their declared atomicity,
including publication's reference and retention effects. Partitioning and
authority migration must preserve fencing and authoritative retry outcomes.

Independent logs require no global total record order. Each log history retains
its declared order and atomic append boundaries. Generation capture SHOULD bind
existing immutable ranges without recopying the historical payload through the
Generation publisher.

GTS is optional for the coordinated versions in Section 18. Ordinary immutable
reads, local mutations, and uncoordinated Generations need not allocate a global
timestamp. Coordinated checkpoint cost includes the chosen time provider and
all required runtime, database, and Version FS participants. Preparation can
proceed independently only within the common boundary contract, and completion
still depends on every required participant becoming durable.

### 21.5 Retention and overload principles

Shared history should remain cheap to retain and fork. Divergent content,
retained roots, and protected operations still consume capacity; current
workspace size alone does not describe the retained state graph.

Normal checkpoint publication SHOULD NOT require work proportional to all
retained historical state. Garbage collection and background work MUST preserve
retention, independent readability, and publication correctness as the number
of Generations and shared dependencies grows.

Implementations MUST provide bounded resource use and explicit overload
behavior. Resource exhaustion or overload MUST NOT weaken durability, retention,
isolation, or publication correctness. Protection required for an unknown
operation outcome remains in force until that outcome is resolved.

Numerical service objectives, benchmark procedures, and concrete resource
management policies belong in implementation proposals.

## 22. Alternatives and architectural tradeoffs

| Alternative | Architectural tradeoff |
| --- | --- |
| Complete workspace copies | Independent restore is simple; data and metadata copying grow with the base. |
| Chains of Generation deltas | Small change records create replay and ancestor-retention dependencies. |
| Git as the entire state model | Source-control tools are useful, while filesystem metadata, logs, artifacts, and machine checkpoints need broader semantics. |
| Only local snapshots | Local capture can be fast; portable durability, publication, and reference authority still require a system contract. |
| One engine for application DB and files | Engines become coupled despite different transaction and state-access requirements. |

Persistent immutable roots combine independent reads with physical sharing.
Their cost is explicit publication, retention, integrity, and authority
coordination. Implementation proposals must explain their concrete tradeoffs.

## 23. Related work

The following sources informed the architecture. Their implementation choices
and performance claims are not Version FS guarantees. The implications below
are this RFC's interpretation; sources were consulted on 2026-10-22.

- [toy-kv-engine](https://github.com/ben1009/toy-kv-engine) informs the separate
  database snapshot and recovery boundary.
- [Tensorlake](https://github.com/tensorlakeai/tensorlake), its
  [snapshot and clone guide](https://docs.tensorlake.ai/sandboxes/snapshots), and
  [sandbox lifecycle](https://docs.tensorlake.ai/sandboxes/lifecycle) are the
  process-memory references: reusable memory/filesystem snapshots, runtime
  suspend/resume, and restoration into independent descendants. Its
  [snapshot architecture](https://www.tensorlake.ai/blog/multi-cloud-scheduling)
  informs incremental state capture and physical sharing. Section 18 defines
  Version FS's own coordination and retention requirements around that model.
- [LayerFS](https://github.com/Ephemeral-AI-Lab/layerfs) is a reference for writable
  workspaces over shared history; Version FS defines remote publication
  separately from physical sharing.
- [Code Storage](https://code.storage/) informs programmatic source-state access.
- [S2 streams](https://s2.dev/docs/concepts/streams) and
  [appends](https://s2.dev/docs/concepts/appends) provide the durable ordered-log
  reference. Version FS adds Generation-bound views and retention semantics.
- [Turso's durability architecture](https://turso.tech/blog/how-does-the-turso-cloud-keep-your-data-durable-and-safe)
  and [database overview](https://turso.tech/what-is-turso) inform the separation
  of local execution, remote durability, and database semantics.
- [GitHub's agent-scale infrastructure article](https://github.blog/engineering/architecture-optimization/the-bridge-to-agent-scale-git-infrastructure/)
  informs the distinction between immutable objects and ordered references.
- [Anthropic's source-control redesign](https://www.anthropic.com/research/redesigning-source-control)
  provides context for sparse clients and agent workloads; Version FS requires
  complete state roots without changeset replay.
- [chDB's durable layer](https://clickhouse.com/blog/chdb-durable-layer-for-agent-memory)
  informs explicit remote durability boundaries while retaining database ownership
  of database state.
- [TiDB / PD TSO](https://docs.pingcap.com/tidb/stable/tso/) is one reference for
  an increasing timestamp oracle. Its
  [historical snapshot reads](https://docs.pingcap.com/tidb/v7.5/read-historical-data/)
  inform timestamp-addressed database versions.
- [The Spanner paper](https://research.google.com/archive/spanner-osdi2012.pdf),
  [TrueTime and external consistency](https://docs.cloud.google.com/spanner/docs/true-time-external-consistency),
  and [Spanner's commit protocol](https://docs.cloud.google.com/spanner/docs/whitepapers/life-of-reads-and-writes)
  inform uncertainty-aware timestamp selection and ordering. External consistency
  relies on the surrounding protocol as well as the time authority.
- [Amazon Time Sync Service](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/set-time.html)
  supplies a synchronized UTC clock reference. Section 18 requires an ordering
  adapter before such clock infrastructure can serve as a GTS for this design.
- [Git Internals](https://git-scm.com/book/en/v2/Git-Internals-Git-Objects),
  [Sapling](https://sapling-scm.com/), and
  [Mononoke](https://github.com/facebook/sapling/tree/main/eden/mononoke) provide
  source-control object and client/server reference points.

The design succeeds when a retained Generation is an ordinary durable execution
input: complete and independently recoverable, with unchanged physical state
shared across histories.
