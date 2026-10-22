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

- [RFC-001: Version FS — Versioned Storage for Files, Git, and Logs](rfcs/001-version-fs.md)

The project is at the design stage. RFC-001 is a draft proposal for v0; this
repository does not yet contain an implementation.
