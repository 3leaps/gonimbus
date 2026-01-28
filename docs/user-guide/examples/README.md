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

## Guides

- Local index (large buckets): `docs/user-guide/index.md`
- Content streaming (metadata/content access): `docs/user-guide/streaming.md`
- Tree (prefix summary): `docs/user-guide/examples/tree.md`
- Advanced filtering (size/date/regex): `docs/user-guide/examples/advanced-filtering.md`
- Transfer operations (copy/move/reflow): `docs/user-guide/transfer.md`

## Common Workflows

### Reorganize by Content Date

Files often arrive organized by processing date but need reorganization by business date embedded in content:

```bash
# 1. List objects
gonimbus inspect 's3://bucket/arrivals/' --json > objects.jsonl

# 2. Extract business date from XML content
jq -r '"s3://bucket/" + .key' objects.jsonl | \
  gonimbus content probe --stdin --config probe.yaml --emit reflow-input \
  > probe.jsonl

# 3. Reorganize by extracted date
gonimbus transfer reflow --stdin \
  --dest 's3://bucket/by-business-date/' \
  --rewrite-from 'arrivals/{store}/{arrival_date}/{file}' \
  --rewrite-to '{business_date}/{store}/{file}' \
  --dry-run < probe.jsonl
```

### Download and Reorganize Locally

```bash
gonimbus transfer reflow --stdin \
  --dest 'file:///data/reorganized/' \
  --src-profile prod-readonly \
  --rewrite-from '...' \
  --rewrite-to '...' < probe.jsonl
```

## Design Notes

- **Stable paths matter**: these docs are intended to be linked from the CLI help and from `README.md`.
- **Future: embedded docs**: we plan to ship key docs inside the `gonimbus` binary using the same `//go:embed` pattern used for schemas.
  - See `docs/architecture/adr/ADR-0001-embedded-assets-over-directory-walking.md` for the embedded-assets approach.
  - See `docs/development/accessing-crucible-docs-via-gofulmen.md` for how gofulmen exposes embedded SSOT docs.
