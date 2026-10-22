# RFC-001: Version FS Architecture

| Field | Value |
| --- | --- |
| Status | Draft |
| Created | 2026-10-22 |
| Target | Version FS architecture |
| Scope | System model, state semantics, correctness, and integration boundaries |

## 1. Summary

Version FS is a durable state system for agents and other workloads that create
many short-lived workspaces. Filesystem state, Git state, ordered logs, and
artifacts participate in common version, checkpoint, fork, and retention
semantics.

The fundamental abstraction is a **Generation**: an immutable, complete logical
state rooted at a stable manifest. A retained Generation is independently
readable and restorable. It may share almost all physical data and metadata with
other Generations.

```text
                           Version FS
                       immutable Generation
                                |
              +-----------+-----+------+----------+
              |           |            |          |
              FS         Git      Ordered logs  Artifacts
              |           |            |          |
              +-----------+-----+------+----------+
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
shared TSO can provide logical versions across process memory, database state,
and Version FS. A future global checkpoint binds their snapshots and Generation
at a common boundary through explicit participant interfaces.

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
5. Published state survives loss of worker compute and local caches.
6. Files, Git, ordered logs, and artifacts share version, checkpoint, fork, and
   retention semantics.
7. Storage state and application databases remain separate systems.
8. Retention protects the complete reachable state of every retained Generation.
9. Concurrent or stale operations cannot overwrite newer authoritative state.
10. Content integrity and authorization are independently enforced.

### 3.2 Interfaces and views

| View | Logical state | Relationship to a Generation |
| --- | --- | --- |
| Filesystem | Namespace, file contents, and declared metadata. | A complete immutable namespace view. |
| Git | Source objects, refs, and source-control history. | A coherent declared Git view; machine checkpoints need not become Git commits. |
| Ordered logs | Durable ordered records. | An exact immutable captured range for each included log. |
| Artifacts | Named or referenced immutable payloads and outputs. | Explicit reachable content governed by Generation retention. |

Each view defines its own domain operations. Their common contract concerns
version identity, coherent capture, publication, branching, and retention.
One shared physical format or storage engine is not required.

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
architecture supports TSO as their shared logical version domain. Their protocol
needs its own specification of participant boundaries, ordering, and retention.

## 4. Terminology

| Term | Meaning |
| --- | --- |
| Object | Immutable stored content or metadata. |
| Manifest | Metadata resolving names and logical ranges to immutable state. |
| Root manifest | The entry point for one Generation's complete state graph. |
| Generation | A published, immutable, independently readable logical state. |
| Workspace | An authorized namespace of branches and retained Generations. |
| Branch | A mutable named reference representing a line of history. |
| Overlay | Unpublished mutable changes above a base Generation. |
| Checkpoint | Capture and durable publication of a coherent state boundary. |
| Publication | The authoritative transition making a prepared Generation visible. |
| Pin | A retention reference keeping a Generation's reachable state live. |
| Log position | A location within one ordered log history. |
| TSO | A service allocating timestamps in a shared logical ordering domain. |
| TSO version | A scoped logical version binding a TSO value to captured participant state. |
| Runtime snapshot | Captured process memory and the execution context declared necessary for resume. |

Generation identity, object identity, log position, branch concurrency state,
and optional TSO versions serve different purposes. A TSO version can select a
retained Generation or participant snapshot without replacing its native
identity. Wall-clock time alone does not establish any of them.

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

The control plane determines authoritative branch heads, published Generations,
write authority, and retention. Mutations that establish publication or replace
an authoritative reference MUST have one atomic, durable outcome. Their related
visibility and retention effects must agree with that outcome.

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
    +-- root
          +-- filesystem state
          +-- Git state
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
A Generation MAY also be addressed through a TSO logical version that binds to
its immutable identity and root, as described in Section 18.

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

Mutable working state may be represented as an overlay over a Generation:

```text
read(path) -> overlay entry, including deletion
           -> base Generation on overlay miss
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

## 9. Log and artifact views

### 9.1 Ordered logs

A log is a first-class ordered state view. [S2 streams](https://s2.dev/docs/concepts/streams)
and [durable appends](https://s2.dev/docs/concepts/appends) are the reference
model for durable ordered records. S2 is an architectural reference rather than
a required service dependency.

A successful log append establishes a durable, ordered, atomic set of records.
It does not by itself publish a Generation or make filesystem changes durable.
Log order, Generation identity, and optional TSO ordering remain distinct.

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

A branch is a movable reference to an immutable Generation. It represents an
independently advancing history and does not require a complete physical copy
of that history's data.

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

Before publication, a failed attempt leaves the prior authoritative state in
place. After publication, the new Generation remains authoritative even if the
worker crashes or loses the reply. Auxiliary preparation or commit markers do
not override the authoritative outcome.

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
| Rollback | Move a branch reference to an earlier retained Generation. |
| Promote | Conditionally select a source Generation as another branch's head. |
| Delete branch | Remove its authoritative reference and release its retention interest. |

Rollback moves a reference; it does not rewrite history. Promotion must reject
an update when the destination or other declared preconditions have changed.
Selecting a speculative result does not imply a semantic merge of independent
edits.

An external reference change does not automatically rewrite a running process's
local directory. Its unpublished work must be reconciled before it can become
authoritative. Deletion does not immediately erase shared content protected by
other branches, retained Generations, pins, or readers.

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

Retained Generations, branch references, pins, protected readers, and protected
operations define liveness. Shared content remains live while any such reference
can require it. Retention applies to all views, including log ranges and required
artifact dependencies.

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

## 18. TSO versions across process memory, databases, and Version FS

### 18.1 Shared logical version

A TSO value T MAY serve as the logical version of process memory, database state,
and a Version FS Generation. T identifies a captured boundary within a declared
TSO domain and agent history; it does not replace each participant's native
snapshot or storage identity. The binding of a published version to those
identities MUST be immutable. Version lookup must identify the domain and
history, including forks, so a number alone cannot select an unrelated state.

```text
                         shared TSO
                             |
                    logical version T
                             |
                   AgentCheckpoint(T)
                   /         |         \
          Process @ T      DB @ T     Version FS @ T
          snapshot M      snapshot D   Generation G
                                           |
                                   FS / Git / Logs / Artifacts
```

The coordinator can expose T as the common version selector while resolving M,
D, and G through the published checkpoint. Participants may also expose T as
their logical snapshot version under the same binding contract. This is an
optional integration capability; Generations can exist without a TSO version.

### 18.2 Participant state and ownership

| Participant | Meaning of version T | Owner of capture and restore |
| --- | --- | --- |
| Process runtime | A consistent snapshot of process memory and required execution context at the boundary. | Runtime or process checkpoint adapter. |
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

### 18.3 Consistency, durability, and retention

**A shared timestamp alone does not create a consistent snapshot.** The
participants must establish the same application boundary, including causally
related memory changes, database transactions, filesystem writes, and log records.
Allocating T or attaching it to unrelated snapshots does not establish that cut.

A checkpoint declares its participants and can claim durable coordinated restore
only after their captured states and retention bindings are durable. A volatile
memory image or an unprotected MVCC read timestamp alone is insufficient. A
checkpoint without a runtime snapshot can restore declared data state but cannot
claim to resume the process from that version.

Opening a retained coordinated version T resolves its exact participant bindings.
Unavailable or expired participant state must be reported rather than silently
substituting another version. Forking T derives an independent history from
those states while preserving the original version's identity and protections.

Participant snapshots remain valid on their own if global publication fails;
they are not automatically a published coordinated checkpoint. A Version FS
Generation can be published detached for this purpose without moving a branch.
TSO allocation, completeness watermarks, runtime capture mechanisms, and
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
| Open coordinated version | Resolve the declared process, database, and Version FS state bound to a TSO version. |

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
| I9 | Loss of worker compute or its cache cannot destroy successfully checkpointed state. |
| I10 | Runtime and database integration use explicit participant contracts and preserve each system's capture and recovery responsibilities. |
| I11 | A retry cannot silently apply the same identified logical operation twice. |
| I12 | Content identity never grants authorization. |
| I13 | A successful log append atomically establishes an identified ordered range of durable records. |
| I14 | Live trimming cannot change log bytes visible in a retained Generation. |
| I15 | A published coordinated TSO version binds its declared participant states at one common boundary and protects all state required for the promised restore. |

These are architecture requirements across releases. Implementation choices and
performance optimizations must preserve them. Release specifications must
define the evidence that demonstrates these guarantees.

## 21. Expected cost properties

| Operation | Architectural objective |
| --- | --- |
| Checkpoint | Persist changed content, affected manifest paths, and publication metadata. |
| Fork | Add references without copying the complete state. |
| Rollback / promote | Move authoritative references without rewriting immutable history. |
| Read | Resolve relevant metadata and fetch demanded content. |
| Diff | Skip shared identical subgraphs and visit changed state. |

Logical completeness must not force a full physical copy. Tree depth, indexing,
capture method, packing, and remote requests still affect actual cost. Worst-case
diff can visit the affected namespace, and a full export reads the selected data.
This RFC makes no fixed latency, storage ratio, or capacity claim.

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
- [Tensorlake](https://github.com/tensorlakeai/tensorlake) and its
  [product overview](https://www.tensorlake.ai/) motivate agent workspaces,
  snapshots, forks, and source-control interfaces.
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
- [CRIU's checkpoint/restore design](https://criu.org/Checkpoint/Restore) provides
  a reference for process memory and execution-state capture; selecting a runtime
  checkpoint mechanism is a separate integration decision.
- [TiDB's TSO model](https://docs.pingcap.com/tidb/v7.1/tso/) and
  [historical snapshot reads](https://docs.pingcap.com/tidb/v7.5/read-historical-data/)
  provide references for timestamp-based database versions. This RFC defines its
  own cross-participant binding and publication contract.
- [Git Internals](https://git-scm.com/book/en/v2/Git-Internals-Git-Objects),
  [Sapling](https://sapling-scm.com/), and
  [Mononoke](https://github.com/facebook/sapling/tree/main/eden/mononoke) provide
  source-control object and client/server reference points.

The design succeeds when a retained Generation is an ordinary durable execution
input: complete and independently recoverable, with unchanged physical state
shared across histories.
