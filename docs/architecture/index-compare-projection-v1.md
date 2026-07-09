# Index Compare Projection v1

`gonimbus.index.compare_projection.v1` is the parity projection for comparing
the SQLite index format with the durable-v2 index format (the v0.4.0 default)
over one observed crawl. Dual-format builds (`index build --format both`) emit
a `gonimbus.index.compare_result.v1` report that includes this projection's
`projection_semantics` block.

## Scope

The projection covers active LIST-derived current rows only. It is not a
point-in-time history format and does not compare durable coverage attestation,
segment metadata, hub metadata, local file paths, run bookkeeping, or boundary
rendering fields.

Rows are ordered by `rel_key`. Null and empty string values normalize to an
empty value for optional string fields. Timestamps normalize to UTC
`RFC3339Nano`.

Included row fields:

- `rel_key`
- `size_bytes`
- `last_modified`
- `storage_class`

Excluded from the row digest:

- provider ETag or equivalent object-version token
- `first_seen_*`, `last_seen_*`, and `last_changed_*`
- HEAD-derived enrichment fields
- `deleted_at`
- durable coverage, segment, and reachability metadata

Provider ETag is excluded from the row digest because it is not a portable
content hash. The Slice A comparator still performs a separate keyed
`provider_etag_equivalence` check so excluded content identity is not
unchecked.

## Result Semantics

The comparator emits `gonimbus.index.compare_result.v1` with:

- projection and comparator versions
- a `projection_semantics` block that machine-carries what the result
  certifies and what it does not certify
- observation/run identity
- SQLite and durable artifact identities
- materialization booleans for SQLite, durable publication, comparison run, and
  parity pass
- row counts and projection SHA-256 digests for each side
- bounded first-N mismatch details
- separate content-identity semantics and mismatch count

Unknown projection versions must be rejected explicitly.

## Reading A Green Result

`parity_passed: true` means the SQLite and durable indexes agree on the
LIST-derived projection for the same crawl: `rel_key`, `size_bytes`,
`last_modified`, and `storage_class`. The provider ETag check is a
same-provider equivalence guard and is not a portable content hash.

A green result does not mean the durable index is reflow-ready. HEAD-derived
enrichment metadata is outside projection v1 and requires a separate
`enrich-with-head` pass plus a future enriched-run projection. Run-scoped
temporal fields, coverage attestations, and physical format metadata are also
outside this projection.
