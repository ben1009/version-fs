# Version FS development

Read `README.md`, `docs/development.md`, and the relevant RFC before changing a
subsystem. The current Rust packages are tooling skeletons, not an implemented MVP.

## Workspace

- `versionfs-core`: formats, persistent maps, backend-neutral protocols.
- `versionfs-service`: object-store adapters, metadata authority, service handlers.
- `versionfs-sdk`: transport, Linux worker integration, capture coordination.
- `versionfs-cli`: command support; executable commands will be added with their
  implementations.

Keep core protocols independent of toy-kv and PostgreSQL. Add dependencies only
when they are used, inherit shared metadata/lints, and keep dependency declarations
sorted. The dated nightly and tool versions are documented in `docs/development.md`.

## Validation

Run `./dev check` before opening a PR. Use `./dev fmt` and `./dev sort-deps` for
explicit fixes; check tasks must not rewrite source files. Nextest is the preferred
test runner; run doc tests separately. Name process-global failpoint tests with
`failpoint_` so in-process sanitizer runs can exclude them. Keep protocol failure
tests with the state machines they validate.

Do not describe successful skeleton builds or empty test suites as storage,
durability, real-S3, or ublk qualification. The remaining P0 work and later gates
are tracked in `docs/mvp-implementation-plan.md`.
