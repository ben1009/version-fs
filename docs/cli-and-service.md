# CLI, Service, and SDK Lifecycle (P9)

This document summarizes the Version FS service (`versionfsd`), SDK (`versionfs-sdk`),
and CLI (`versionfs`) operational contracts implemented in P9.

## Acknowledgement Levels

Version FS distinguishes four explicit durability and retention boundaries:

1. **`local_volatile_only` (`WriteAck`)**: Normal block `WRITE` requests accepted into
   the worker-local dirty overlay are volatile in worker memory/cache only. They are
   not durable on the Metadata Authority or Object Store until a `FLUSH`/`FUA`
   barrier completes.
2. **`durable_live_volume` (`FlushAck`)**: A block `FLUSH`/`FUA` barrier commits a
   new authoritative live `VolumeRoot` on the branch (`revision` advances). This
   persists the live volume state across worker replacement, but does **not** create
   a retained Generation checkpoint.
3. **`durable_log_range` (`AppendAck`)**: A native log append commits a durable
   contiguous sequence range on the branch's live `logs_root` (`end_seq` advances).
   This is durable on the live branch, not a retained Generation checkpoint.
4. **`published_generation` (`CheckpointAck`)**: A checkpoint capture publishes an
   immutable `GenerationDescriptorV2` binding the volume, native logs, native
   artifacts, and explicit provenance status (`recorded` or `unattributed`) under the
   established `capture_mode` (`block_cut` or `fs_consistent`).

## OS Permissions and External Dependencies

- **Unprivileged local/service mode**: `versionfsd` (backed by `ToyKvMetadataAuthority`
  and `DirectoryObjectStore`), `versionfs volume serve`, and all metadata/log/artifact
  CLI commands run unprivileged over TCP (`tcp://...`) or Unix sockets (`unix://...`).
- **Linux block-device & ext4 helpers (`CAP_SYS_ADMIN` / root)**:
  - Creating/attaching kernel `ublk` devices (`versionfs-sdk/src/ublk.rs`) requires
    Linux `ublk` support and `CAP_SYS_ADMIN` (or root).
  - `versionfs fs format`, `mount`, `unmount`, and `LinuxFilesystemBoundary` freeze
    helpers invoke external system binaries: `mkfs.ext4`, `mount`, `umount`, and
    `fsfreeze`.
  - `mkfs.ext4` is strictly explicit (`--confirm-destructive` /
    `confirm_destructive == true`) and refuses mounted devices. Formatting is never
    performed implicitly during attach, fork, rollback, or restore.
  - `mount` and `umount` never pass lazy (`-l`) or forced (`-f`) flags.

## Protection, Deadlines, and Provenance

- Arbitrary content-digest reads are not exposed publicly. `FetchProtectedObject`
  requires an active, unexpired `ProtectionId` whose Generation/preparation graph
  reaches the requested `ObjectRef`.
- Remote read handles (`ProtectedReadHandle`) enforce bounded read deadlines
  (`min(protection_deadline, now + 30s)`).
- Attachment heartbeats (`AttachmentHeartbeat`) renew writer grants and attachment-base
  protection using control-plane RPCs only, independent of any frozen filesystem
  boundary.
- Provenance inspection (`versionfs generation inspect-provenance`) preserves legacy
  V1 descriptors as `unattributed` (`coverage = none`), returns `Unauthorized` when
  a caller is denied context reads (never relabeling `recorded` provenance as
  `unattributed`), and requires opt-in + sanitization (`sanitize_provenance_text`)
  before ingesting raw payload text.

## Capture Boundaries and Non-Guarantees

Version FS captures **block-device (`block_cut`) or filesystem-quiesced
(`fs_consistent`) storage boundaries** together with native logs, native artifacts,
and explicitly supplied provenance metadata. Version FS does **not** capture or
guarantee consistency of:

- Application-internal in-memory state or uncommitted buffers;
- Database WAL/transaction state that has not been flushed to the filesystem/block
  device before freeze/barrier;
- Process memory, open file descriptors, or live OS processes.

## Mark-only retention reports (P10)

`versionfs report collect` and `versionfs report get` (backed by
`ServiceClient::collect_report` / `ServiceClient::get_gc_report` and
`WorkspaceService`) run bounded, mark-only retention reporting:

- Roots from retained Generations, live branches, active protections, and active
  preparations are sealed from one read-only `MetadataAuthority` snapshot, and
  the snapshot is released before any object-store I/O.
- Graph traversal follows typed strong edges (including recorded
  `ProvenanceContext` required evidence roots `required_logs_root` and
  `required_artifacts_root`) using a disk-spill visited set
  (`DiskSpillVisitedSet`), while excluding `optional_links` and descriptive IDs.
- Object-store inventory runs over a recorded Unix-ms window (`inventory_page`)
  and persists bounded `GcCandidatePage`s without issuing any object deletions.
- Missing/corrupt objects, missing metadata, uncertain `Preparing` operations,
  and candidate budget exhaustion are recorded explicitly in
  `GcReportSummary::completeness`; backend inventory listing errors abort the
  collection call, and expired detail pages return `ReportExpired` while
  preserving the terminal summary.
