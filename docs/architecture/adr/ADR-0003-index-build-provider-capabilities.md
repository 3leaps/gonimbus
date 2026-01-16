# ADR-0003: Index Build Provider Capabilities (Prefix + Delimiter)

**Status:** Accepted
**Date:** 2026-01-16
**Decision Makers:** @3leapsdave

## Context

Gonimbus indexing must scale to 100M+ object buckets across multiple providers.

A key constraint is that object stores are prefix-indexed. To reduce provider listing cost, index builds must execute against an explicit set of prefixes.

Different providers (S3-compatible, GCS, Azure Blob) expose different SDKs and error shapes, but share common semantics:

- list objects by prefix
- list common prefixes (“directories”) by prefix + delimiter

We need a provider capability contract that lets indexing and scope compilation remain provider-agnostic.

## Decision

Define the indexing capability contract in terms of:

1. **Prefix listing**: list objects under a literal prefix with pagination.
2. **Delimiter listing**: list common prefixes under a literal prefix and delimiter.
3. **Error classification**: normalize error classes important to index correctness.

Index build code and schemas will be written against these capabilities.

### Prefix listing requirements

Providers used for crawl-backed index builds must support:

- `List(prefix, continuation_token)` returning:
  - `key` (string)
  - `size` (int64)
  - `last_modified` (timestamp)
  - `etag` (optional)

### Delimiter listing requirements

Providers used for path-based scope compilation must support:

- `ListCommonPrefixes(prefix, delimiter, continuation_token)` returning:
  - `prefixes[]` (each ending in delimiter)

Delimiter listing is used to discover variable path segments (e.g. device IDs) before expanding deterministic date partitions.

### Error classification requirements

To preserve correctness and safe deletes:

- Access denied, not found, throttled, and provider-unavailable conditions MUST be classifiable.
- These conditions are treated as non-fatal *per-prefix* and result in `partial` runs.

Soft-delete is only executed on `success` runs.

## Consequences

### Positive

- Scope partitions can be implemented once and reused across providers.
- QA can validate provider conformance against a shared checklist.
- Provider-specific SDK differences stay contained in provider implementations.

### Negative

- Some providers may not reliably support delimiter listing or may behave inconsistently at scale.
- Error normalization requires continuous hardening for S3-compatible endpoints.

## Follow-ups

- Add a provider conformance checklist to QA runbooks (outside of this ADR).
- Implement `build.scope` schema and compiler (v0.1.4).

## Related

- `docs/architecture/indexing.md`
- `docs/architecture.md` (match prefix derivation)
