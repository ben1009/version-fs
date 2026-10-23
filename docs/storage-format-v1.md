# Version FS Durable Format v1

**Status:** Initial implementation format for the Version FS v0 MVP  
**Format version:** `1`  
**Metadata schema version:** `1`

This note fixes the byte-level envelope and root encodings before the first
durable writer. The shared golden vectors are in
[`fixtures/storage-format-v1.tsv`](../fixtures/storage-format-v1.tsv) and are
checked by `versionfs-core`.

## 1. Primitive Encoding Rules

- Unsigned integers are little-endian unless a field explicitly says otherwise.
- Fixed-width IDs are raw bytes, with no text or length prefix.
- Variable byte strings are prefixed with a 32-bit little-endian byte length.
- Optional 128-bit IDs use a one-byte tag: `0` means absent; `1` means present,
  followed by exactly 16 bytes. Other tags are invalid.
- Maps use strictly increasing bytewise key order. Duplicate keys are invalid.
- Numeric map keys use fixed-width big-endian encoding so lexicographic byte
  order matches unsigned numeric order. For example, logical extent `1` sorts
  before extent `256`.
- A reader rejects unknown versions/types, invalid lengths, trailing bytes,
  malformed references, and invalid enum values. It must not guess a format.

## 2. Immutable Object Envelope

Every immutable object is encoded as:

| Offset | Size | Field |
| --- | ---: | --- |
| 0 | 4 | ASCII magic `VFSO` |
| 4 | 2 | format version (`1`, little-endian) |
| 6 | 2 | object type code (little-endian) |
| 8 | 8 | payload byte length (little-endian) |
| 16 | variable | canonical type-specific payload |

The maximum payload is 4 MiB. No object compression or packing is used in v0.
Object identity is SHA-256 of the complete encoded envelope, including the
magic, version, type, and length. A reference carries the object type, 32-byte
digest, and exact encoded length. Object-store keys use the typed digest path
from RFC-002; the digest is never an authorization token.

Object type codes are fixed:

| Code | Type |
| ---: | --- |
| 1 | `Extent` |
| 2 | `VolumeMapNode` |
| 3 | `VolumeRoot` |
| 4 | `LogSegment` |
| 5 | `LogSegmentIndexNode` |
| 6 | `LogMapRoot` |
| 7 | `ArtifactBlob` |
| 8 | `ArtifactChunk` |
| 9 | `ArtifactIndexNode` |
| 10 | `ArtifactMapRoot` |
| 11 | `GenerationDescriptor` |

Codes are not reused. New formats need an explicit version or new type code.
The tree-node layout and split policy are fixed with the persistent-map
implementation before its first persistent write; the common byte and key rules
here already apply to those nodes.

## 3. Object References and Complete Roots

An object reference is exactly 42 bytes, in this order:

```text
object_type:  u16 little-endian
object_id:    32 raw SHA-256 bytes
encoded_len:  u64 little-endian
```

`VolumeRoot` payload fields are fixed-order:

```text
capacity_bytes:       u64 little-endian
logical_sector_bytes: u32 little-endian, exactly 4096 in v0
extent_bytes:         u32 little-endian, exactly 65536 in v0
extent_map_root:      ObjectRef of type VolumeMapNode
```

Capacity is positive and a multiple of 64 KiB. Read/write ranges are aligned to
4 KiB logical sectors and remain within capacity. Each present extent is exactly
64 KiB. An absent extent-map entry reads as all zeroes. A fully zero extent is
represented by absence, never by a special zero-data object.

`GenerationDescriptor` payload fields are fixed-order:

```text
generation_id:        16 bytes
parent_id:            optional-ID encoding
volume_root:          ObjectRef of type VolumeRoot
logs_root:            ObjectRef of type LogMapRoot
artifacts_root:       ObjectRef of type ArtifactMapRoot
capture_mode:         u8
volume_boundary_id:   16 bytes
created_at_unix_ms:   u64 little-endian, informational only
```

The descriptor binds a complete root for each native view. Its parent ID is
lineage metadata and is not required to resolve the Generation. Root references
must have the declared types; readers then verify encoded length and digest
against the referenced bytes.

Capture mode values are stable:

| Value | Mode |
| ---: | --- |
| 1 | `block_cut` |
| 2 | `filesystem_frozen` |
| 3 | `filesystem_clean_unmount` |

Clean unmount is not serialized as freeze. Unknown values are rejected.

## 4. Ordered Log Segments

One `LogSegment` object represents one complete accepted append batch. Payload
fields are:

```text
log_id:             16 bytes
first_seq:          u64 little-endian, inclusive
end_seq:            u64 little-endian, exclusive
record_count:       u32 little-endian
records:            record_count repetitions of u32 length + bytes
attachment_count:   u32 little-endian
attachments:        attachment_count repetitions of ObjectRef
```

The record count is nonzero and equals `end_seq - first_seq`; sequence ranges
are half-open. Records remain in sequence order. Attachments are explicit typed
references, not hashes inferred from opaque record bytes. Empty append batches
are invalid. The MVP append API limits each batch to 1000 records and 1 MiB of
encoded record payload; object-envelope maximums also apply to the complete
segment.

## 5. Artifact Payloads

An artifact of at most 64 KiB is one `ArtifactBlob`; an empty artifact uses the
empty payload. Larger artifacts use `ArtifactChunk` objects with up to 4 MiB of
raw payload bytes each, with a shorter final chunk permitted. Chunk object IDs
are independent of artifact names and chunk positions, allowing identical bytes
to share storage. The persistent artifact index declares logical length and
chunk order.

## 6. Metadata Authority Values

Metadata values use a separate envelope:

| Offset | Size | Field |
| --- | ---: | --- |
| 0 | 4 | ASCII magic `VFSM` |
| 4 | 2 | metadata schema version (`1`, little-endian) |
| 6 | 2 | metadata record type code (little-endian) |
| 8 | 8 | payload byte length (little-endian) |
| 16 | variable | canonical record payload |

Record type codes are `Workspace=1`, `Branch=2`, `Generation=3`,
`OperationOutcome=4`, and `Protection=5`. Metadata keys remain point-addressed
by the authority protocol; a prefix scan is not a transactional predicate.
Unknown schema versions and record types are rejected. A migration must add
explicit reader/writer support before data is stored under the new version.

## 7. Golden Vectors

`fixtures/storage-format-v1.tsv` contains stable encoded-byte and SHA-256 vectors
for artifact chunks, the empty artifact, the empty extent map, a 128 MiB volume
root, a one-record log segment, a complete Generation descriptor, and a
versioned Metadata Authority record. Vectors use deliberately fixed IDs for
references so they test canonical encoding independent of object-store state.

The empty-volume-map vector has zero entries; the absent extent key in that map
therefore denotes the all-zero 64 KiB extent. Tests also pin numeric key order
and all three capture-mode values.
