# Examples (Cookbook)

This directory is a growing collection of copy/pasteable examples for common Gonimbus workflows.

The goal is to keep these examples:

- **Operator-friendly** (CLI-first)
- **Provider-aware** (S3/S3-compatible now; more later)
- **Performance-aware** (prefix-first listing; avoid accidental full-bucket scans)
- **Composable** (examples combine cleanly across filters, sharding, outputs)

This area is expected to expand as we add providers (GCS, etc.), multi-region/site-to-site workflows, and new operations beyond crawl/inspect.

## Safety

When dogfooding against real buckets, consider running with the global safety latch enabled:

- `--readonly` (or `GONIMBUS_READONLY=1`) disables provider-side mutations, including `transfer` execution and `write-probe` preflight.

## Index

- Advanced filtering (size/date/regex): `docs/user-guide/examples/advanced-filtering.md`
- Transfer operations (copy/move): `docs/user-guide/transfer.md`

## Design Notes

- **Stable paths matter**: these docs are intended to be linked from the CLI help and from `README.md`.
- **Future: embedded docs**: we plan to ship key docs inside the `gonimbus` binary using the same `//go:embed` pattern used for schemas.
  - See `docs/architecture/adr/ADR-0001-embedded-assets-over-directory-walking.md` for the embedded-assets approach.
  - See `docs/development/accessing-crucible-docs-via-gofulmen.md` for how gofulmen exposes embedded SSOT docs.
