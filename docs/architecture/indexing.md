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

### Required for path-based scope compilation

- **Delimiter listing**: list _common prefixes_ under a literal `prefix` and delimiter (default `/`).
  - Used to discover segment values (e.g., discover device IDs under a store prefix).

### Optional

- **Enrichment** (`Head` / metadata) for content-type/tags.
  - Deferred until an enrichment stage exists.

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

### Build match (ingest predicates)

`build.match` includes:

- `includes` / `excludes` patterns (doublestar)
- `include_hidden`
- build-time metadata filters (`size`, `modified`, `key_regex`)

These reduce ingest volume and query cost, but do not inherently reduce provider listing cost.

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
