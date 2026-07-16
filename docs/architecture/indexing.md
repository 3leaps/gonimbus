# Indexing Architecture

This document describes the architectural contracts for building and querying local indexes.

## Core Concepts

### IndexSet

An **IndexSet** is a stable identity for an indexable dataset.

An IndexSet is defined by:

- `base_uri` (a normalized prefix URI; always ends in `/`)
- provider identity (e.g. `storage_provider`, `cloud_provider`, region/endpoint identity)
- build parameters that affect what is included (match predicates, build-time filters, and future scope configuration)

Operational consequence: if any of these inputs change, the IndexSet identity changes and a different on-disk index directory is used.

### IndexRun

An **IndexRun** is a single execution that writes a snapshot into an IndexSet database.

- A run may finish as `success`, `partial`, or `failed`.
- **Soft-delete is only safe on successful runs**.

## Provider Capability Contract

Index building is designed around a small set of provider capabilities.

### Required for crawl-backed indexing

- **Prefix listing**: list objects under a literal `prefix` with pagination.
  - Must return at least: object `key`, `size`, `last_modified`.
  - Should preserve provider-native LIST fields when available. S3-family
    providers carry `StorageClass` through `provider.ObjectSummary` into
    `objects_current.storage_class`; providers without a LIST-time storage
    class leave it unset, which persists as SQL `NULL` and is omitted from
    JSONL output.

### Required for path-based scope compilation

- **Delimiter listing**: list _common prefixes_ under a literal `prefix` and delimiter (default `/`).
  - Used to discover segment values (e.g., discover device IDs under a store prefix).

### Optional

- **HEAD enrichment**: `gonimbus index enrich-with-head` reconstructs a
  provider from index metadata plus runtime credentials/config flags and calls
  `Head` only for post-filter candidate rows. v1 supports S3-family indexes and
  rejects unsupported providers explicitly.

### Error classification

Providers should normalize common error classes so indexing can remain resilient and correct:

- `access denied` → prefix can be skipped; run becomes partial.
- `not found` (bucket/prefix) → prefix can be skipped; run becomes partial.
- `throttled` → prefix can be skipped; run becomes partial.
- `provider unavailable` (service/network) → prefix can be skipped; run becomes partial.

## Index Build: Listing vs Ingesting

Index builds separate concerns:

- **Scoping** decides _what we list_ (provider-cost lever).
- **Matching/filtering** decides _what we ingest_ (index size / relevance lever).
- **Ingest mapping** carries LIST-derived object attributes into
  `objects_current` without issuing HEAD requests.

### Build match (ingest predicates)

`build.match` includes:

- `includes` / `excludes` patterns (doublestar)
- `include_hidden`
- build-time metadata filters (`size`, `modified`, `key_regex`)

These reduce ingest volume and query cost, but do not inherently reduce provider listing cost.

### LIST-derived storage class

`storage_class` is a LIST-derived scalar. It is stored exactly as the provider
returns it, with no normalization. Missing or empty provider values are stored
as SQL `NULL`; query JSONL omits the field rather than emitting `""` or
explicit `null`.

`index query --storage-class` performs exact, case-sensitive matching against
non-null values. The filter is pushed into SQL using bound parameters, including
canonical-by-ETag mode where filtering happens before ETag grouping.

### Current-state forward deltas

`index query --since-run` is a current-state forward-delta query. It returns
rows whose `first_seen_run_id` or `last_changed_run_id` resolves to an
`index_runs.started_at` after the supplied successful boundary run in the same
IndexSet. Run ordering is resolved through `index_runs`; run ID lexical order is
not a contract.

The current SQLite schema stores latest object state in `objects_current`, not
per-run object snapshots. Therefore `--since-run` can answer "what current rows
were added or meaningfully changed after run X", but it cannot reconstruct
"what rows existed at run X". Full point-in-time history belongs to a future
segment or snapshot-backed index format.

### Durable Snapshot Manifests

As of v0.4.0, durable-v2 is the **default** index build format. The durable path
writes immutable segment snapshots behind an internal manifest. Internal
manifests may expose exact segment shape metadata needed for engine validation,
including segment key ranges, tombstone counts, and artifact-level ETag counts.
SQLite remains an explicit compatibility path (`--format sqlite`); `--format
both` publishes durable and builds a run-scoped SQLite projection only as
per-run parity verification.

Durable snapshots store segment files in a shared immutable segment namespace.
Phase 1 manifests carry parent manifest references and an explicit reachability
policy: retained manifests, their parent chains, and latest pointers are the
roots of segment reachability. Mutable refcount files are not required for
correctness. A future `index compact` command may derive refcounts as an audit
and delete plan before applying retention policy, but those derived counts are
not primary truth.

Boundary manifests are a separate future render mode for publication across a
data boundary. Phase 1 does not publish boundary manifests: invoking boundary
rendering fails with a hard not-implemented error before any artifact is
created. This prevents partial or half-hardened boundary output from being
mistaken for a safe publication format.

When boundary rendering is implemented, it must use tokens minted in a separate
namespace rather than segment identifiers or hashes of restricted values. It
must coarsen or omit restricted-axis shape metadata such as exact per-segment
row counts, tombstone counts, byte sizes, and segment counts by restricted
axis. It must omit boundary `distinct_etags` and suppress restricted-column
min/max statistics, bloom filters, and dictionary surfaces in boundary segment
variants.

Boundary mode is not a row-key de-identification layer. A recipient that is not
authorized to see row-level keys needs a different representation, not a
boundary manifest over the same keys.

`last_changed_*` advances on the same LIST-derived change predicate used by the
incremental build delta report: size, ETag, storage class, or `last_modified`
changes, plus reappearance from soft-delete. `last_seen_*` alone does not
advance `last_changed_*`.

Indexes migrated from schemas that predate delta tracking record an
`object_delta_baselines` row per legacy IndexSet. Query boundaries before that
baseline are rejected because the migration backfills current rows from
`last_seen_*` and cannot recover earlier first-seen or changed history. Deletion
history is not tracked, so `--since-run` is incompatible with
`--include-deleted`.

### HEAD-derived enrichment

`archive_status`, `restore_state`, `restore_expiry`, `content_type`, and
`head_enriched_at` are HEAD-derived fields. They are written only by
`index enrich-with-head`; normal LIST-backed `index build` does not populate
them.

Mutation scope is intentionally narrow: enrichment updates only the
HEAD-derived columns and `head_enriched_at`. It must not overwrite
LIST-derived `storage_class`, size, ETag, `last_modified`, `last_seen_run_id`,
or `deleted_at`.

For S3-family providers, `ArchiveStatus` is stored as the raw provider value
(`ARCHIVE_ACCESS` or `DEEP_ARCHIVE_ACCESS`) when present. Restore headers are
normalized to `ongoing`, `completed`, `expired`, or `unknown`; absent values
persist as SQL `NULL` and are omitted from query JSONL. HEAD-observed
`StorageClass` is not persisted in v1.

Candidate filters (`--storage-class`, `--pattern`, `--key-regex`,
`--min-size`, `--max-size`, `--include-deleted`) are applied before HEAD calls.
Storage-class filtering uses the same exact, case-sensitive, parameterized SQL
semantics as `index query`.

### Build scope (provider-cost lever)

`build.scope` is a scoper that compiles into an explicit prefix plan. This is the primary provider-cost lever for huge buckets where data is partitioned in the key path (e.g., date folders).

At a high level:

- `build.scope` constrains what the provider lists (prefix plan)
- `build.match` constrains what is ingested (predicates/filters)

Initial scope types (v0.1.4):

- `prefix_list`: explicit prefixes (no wildcards)
- `date_partitions`: discover variable segments + expand a date range
- `union`: combine child scopes

Schema source of truth:

- `internal/assets/schemas/index-manifest.schema.json`

The intent is to support datasets where a recent window is desired (e.g. last 30 days) but the history is huge.

Example layout (generic):

- `s3://bucket/data/{store_id}/{device_id}/{YYYY-MM-DD}/{file}`

A scope compiler can:

1. constrain stores (allowlist)
2. discover device IDs via delimiter listing
3. expand an explicit date range to concrete `YYYY-MM-DD/` prefixes
4. crawl only those prefixes

## Determinism and Canonicalization

Any configuration that changes the effective scope or ingest predicate MUST be included in the IndexSet identity in canonical form:

- lists: trim, drop empties, dedupe, sort
- timestamps/dates: parse and normalize to UTC, serialize stably
- regex/glob strings: trim only (do not rewrite)

This prevents “same config, different YAML ordering” from producing different IndexSets.

## Notes on Multi-Cloud

The architecture is intentionally provider-agnostic:

- S3-compatible stores, GCS, and Azure Blob all support prefix + delimiter semantics.
- Inventory ingestion is provider-specific and can be added as a separate IndexSource type without changing the index schema.

See also:

- `docs/architecture/adr/ADR-0003-index-build-provider-capabilities.md`
