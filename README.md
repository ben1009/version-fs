# Version FS

Version FS is a proposed versioned storage substrate for block volumes, files,
Git state, logs, and artifacts produced by agents and other automated workloads.

Its central abstraction is a **Generation**: an immutable, independently readable
logical state. Generations share unchanged objects and metadata, allowing
workspaces to checkpoint, fork, roll back, and promote state without copying a
complete workspace.

Published Generations and acknowledged durable live state live in durable
storage. Compute nodes may additionally keep volatile mutable state and disposable
caches. Transactional databases remain separate systems.

The proposed MVP exposes a versioned block volume through Linux ublk, with ext4
as the reference filesystem consumer, alongside native ordered logs and artifacts.
Generation, branch, and checkpoint semantics apply across these views.

## Design

- [RFC-001: Version FS Architecture](rfcs/001-version-fs.md)
- [RFC-002: Version FS MVP](rfcs/002-version-fs-mvp.md)
- [MVP Implementation Plan](docs/mvp-implementation-plan.md)

The project is at the design stage. RFC-001 defines the architecture and core
semantics; RFC-002 proposes the first implementation and release criteria. Both
are drafts. The implementation plan translates RFC-002 into ordered work packages
and completion evidence. The Rust workspace and development tooling are in place;
storage protocols, services, and device integration are not implemented yet.

## Development

Install [rustup](https://rustup.rs/), then run:

```sh
./dev             # List development tasks; bootstrap cargo-make if necessary
./dev check       # Formatting, dependencies, Clippy, tests, docs, and spelling
./dev build       # Build every workspace target
./dev test-cov    # Generate HTML coverage when tests are available
```

The repository pins the same Rust toolchain and development tools as
[toy-kv-engine](https://github.com/ben1009/toy-kv-engine). See the
[development guide](docs/development.md) for CI, sanitizer commands, tool versions,
and the changes made while porting them.
