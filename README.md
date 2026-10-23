# Version FS

Version FS is a proposed versioned storage substrate for files, Git state, logs,
and artifacts produced by agents and other automated workloads.

Its central abstraction is a **Generation**: an immutable, independently readable
logical state. Generations share unchanged objects and metadata, allowing
workspaces to checkpoint, fork, roll back, and promote state without copying a
complete workspace.

Published state lives in durable storage. Compute nodes keep a mutable overlay
and a disposable cache. Transactional databases remain separate systems.

## Design

- [RFC-001: Version FS Architecture](rfcs/001-version-fs.md)
- [RFC-002: Version FS MVP](rfcs/002-version-fs-mvp.md)
- [MVP Implementation Plan](docs/mvp-implementation-plan.md)

The project is at the design stage. RFC-001 defines the architecture and core
semantics; RFC-002 proposes the first implementation and release criteria. Both
are drafts. The implementation plan translates RFC-002 into ordered work packages
and completion evidence. This repository does not yet contain an implementation.
