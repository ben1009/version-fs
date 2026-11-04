# Version FS Provenance Format v2

**Status:** Implemented in `versionfs-core`; capture-time provenance publication
and protected inspection remain pending.

This note defines the v2 Generation descriptor and its bounded provenance
context. All referenced data, maps, log segments, and artifact payloads retain
their existing v1 object encodings and identities. See
[`storage-format-v1.md`](storage-format-v1.md) for those object layouts.

## 1. Envelope dispatch

The Generation descriptor uses the ordinary `VFSO` envelope with format version
`2` and object type `11`. The payload has no repeated format-version field.
Upgraded readers dispatch on `(version, type)` before decoding the payload:

| Version | Type | Meaning |
| ---: | ---: | --- |
| 1 | 11 | Legacy Generation descriptor; explicitly unattributed. |
| 2 | 11 | Provenance-capable Generation descriptor. |
| 1 | 14 | Provenance context. |

All other known objects remain on v1. The legacy `decode_object` entry point is
v1-only and rejects `(2, 11)` before payload validation. The upgraded
`decode_object_versioned` and `decode_generation_descriptor` entry points
accept the supported pairs above and reject unknown versions or pairs.

`GenerationDescriptor::encode` and `GenerationDescriptor::decode` remain legacy
v1 codecs. General readers use `decode_generation_descriptor`, which preserves
the source version in its decoded result; new writers use
`GenerationDescriptorV2`.

Deploy v2-capable readers before allowing new SDK writers to publish v2
descriptors. Once v2 objects exist, a binary rollback must retain v2 reader
support. This MVP does not support mixed-version rolling upgrades with v1-only
readers, and retained descriptors must not be rewritten for a downgrade.

## 2. Generation descriptor v2

The v2 descriptor payload uses this exact field order:

| Field | Encoding |
| --- | --- |
| `generation_id` | 16 bytes |
| `parent_generation_id` | Optional-ID encoding from v1 |
| `volume_root` | 42-byte `ObjectRef`, type `VolumeRoot` |
| `logs_root` | 42-byte `ObjectRef`, type `LogMapRoot` |
| `artifacts_root` | 42-byte `ObjectRef`, type `ArtifactMapRoot` |
| `provenance_status` | `u8`: `1` recorded, `2` unattributed |
| `provenance_coverage` | `u8`: `1` generation context, `0` none |
| `context_root` | Present only for recorded; 42-byte `ObjectRef`, type `ProvenanceContext` |
| `capture_mode` | Existing v1 `u8` capture mode |
| `volume_boundary_id` | 16 bytes |
| `created_at_unix_ms` | `u64` little-endian |

Only `(recorded, generation_context, context_root present)` and
`(unattributed, none, context_root absent)` are valid. Unknown tags and every
other combination are rejected. V1 descriptors decode as source format v1,
unattributed, coverage none, and no context root. Decoding never rewrites their
bytes, digests, or publication binding.

A recorded binding's context reference must declare an encoded length no larger
than the complete context-object limit of 64 KiB. Descriptor validation rejects
an oversized reference before graph verification fetches its target.

## 3. Provenance context object

A context is a v1 `VFSO` object of type `14`, bounded to 64 KiB including its
envelope. Its payload uses the following fixed field order:

1. `recording_principal`: 16 raw bytes.
2. Ten optional UTF-8 strings, in order: `actor`, `execution_id`, `session_id`,
   `thread_id`, `task_id`, `workflow_operation_id`, `reason`, `trace_id`,
   `span_id`, and `parent_span_id`.
3. `required_logs_root`: 42-byte `ObjectRef`, type `LogMapRoot`.
4. `required_artifacts_root`: 42-byte `ObjectRef`, type `ArtifactMapRoot`.
5. `optional_link_count`: `u16` little-endian, followed by that many
   length-prefixed UTF-8 link strings.

An optional string is a one-byte tag (`0` absent, `1` present); a present value
uses a `u32` little-endian byte length followed by its UTF-8 bytes. Strings must
be non-empty and no longer than 4096 bytes. Optional links are limited to 64,
must be sorted and unique by UTF-8 bytes, and use the same length-prefixed
encoding. Links are descriptive and do not retain their targets.

The context records a principal identity and runtime attribution claims. The
format does not authenticate the principal; the service must derive it from or
validate it against the authenticated request. Actor, task, execution, reason,
and trace fields remain untrusted claims. Raw prompts, conversations, and tool
output have no implicit field and are not copied by default.

## 4. Strong evidence edges

The Generation descriptor's context reference and the context's required log and
artifact roots are strong dependency edges. Graph verification must resolve and
validate both complete native roots, even when they are different from the
Generation's ordinary `logs_root` or `artifacts_root`. Missing or corrupt
required evidence invalidates the proof; readers must not downgrade the
Generation to unattributed. Optional links and opaque IDs create no retention
edges.

The format and graph proof do not implement capture-time binding, operation
fingerprinting, publication, retention catalog updates, or protected context
inspection. Those protocols remain separate P7/P9/P10 work.

