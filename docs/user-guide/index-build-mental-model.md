# Index Build Mental Model

This note explains what `gonimbus index build` does during large object-store
enumerations, what it stores, and what a filtered index does not contain.

Object stores such as S3, S3-compatible systems, GCS, and Azure Blob are
prefix-indexed. To know what exists under a prefix, a client must list that
prefix page by page. Gonimbus cannot make a broad prefix listing disappear, but
it can make the listing intentional, bounded, reusable, and queryable.

## The Pipeline

An index build is a streaming crawl-to-index pipeline:

1. Compile the crawl plan.
   - Without `build.scope`, Gonimbus derives list prefixes from
     `build.match.includes`.
   - With `build.scope`, Gonimbus compiles an explicit prefix plan and uses that
     instead.
2. List every object under each crawl prefix.
3. Apply glob matching and hidden-file rules.
4. Apply build-time metadata filters.
5. Store only matched objects in the index database.

The build is streaming and batched. It does not keep the full provider listing
in memory.

## Listing Scope Versus Ingest Filtering

There are two separate levers:

| Lever            | Config                                              | Controls                           | Cost effect                                        |
| ---------------- | --------------------------------------------------- | ---------------------------------- | -------------------------------------------------- |
| Listing scope    | `base_uri`, derived include prefixes, `build.scope` | Which provider prefixes are listed | Reduces provider list calls and elapsed crawl work |
| Ingest filtering | glob/exclude/hidden rules, `build.match.filters`    | Which listed objects are stored    | Reduces index size and downstream query volume     |

Filtering is not a provider-list shortcut. If the crawl prefix contains 100M
objects, a filtered build still receives those 100M object summaries from the
provider. The value of filtering is that Gonimbus does not store or follow up on
objects that fail the ingest predicates.

`index build --since` uses both levers. On a date-partitioned
`build.scope`, it narrows the date prefix plan before LIST and then applies a
last-modified ingest filter. On non-date layouts, it cannot reduce enumeration;
the run reports that boundary and falls back to full enumeration plus the
last-modified ingest filter.

Use `gonimbus index build --job <manifest> --dry-run` before large builds. The
dry run prints the derived prefixes or scope plan, which is the best early signal
for whether the job will enumerate a bounded partition or a very broad prefix.

## What Gets Stored

For each object that passes matching and filtering, the current index stores:

- relative object key
- size in bytes
- provider ETag when returned by listing
- last-modified timestamp when returned by listing
- last-seen run ID and timestamp
- soft-delete timestamp when a successful full-coverage run later misses the
  object

The index also stores index-set identity, run history, and structured run events
for partial/error cases.

The current crawl-backed index build does not fetch object bodies and does not
issue per-object `HEAD` calls for enrichment. That means it does not store:

- object creation date
- a content hash computed by Gonimbus
- content type
- user metadata
- tags
- arbitrary provider-specific metadata

The stored ETag is the provider's list-time ETag. For S3-like systems, treat it
as a provider version/fingerprint hint, not as a universal content hash.

## What Happens To Filtered-Out Objects

Objects that are listed but fail the include/exclude/hidden rules or
`build.match.filters` are not inserted into `objects_current`.

Operationally, this means:

- A filtered index cannot answer later queries about objects it chose not to
  ingest.
- The path of a filtered-out object is not retained as an index row.
- Progress output can show `objects_found` greater than `objects_matched`, but
  that is build telemetry, not a durable per-object inventory of rejected paths.
- If downstream orchestration needs those objects later, build a different index
  set whose match/filter/scope configuration includes them.

This behavior is intentional. The index is a reusable cache of the ingested
working set, not a hidden full-bucket inventory plus a query-time filter.

## Rebuild Cadence

Index builds are designed to be reused by downstream orchestrators instead of
re-enumerating the same large prefixes repeatedly. Keep manifests stable for the
same operational shard so repeated builds land in the same index set.

For current or mutable partitions, rebuild on a cadence that matches expected
late arrivals and corrections. For closed partitions, freeze the index unless an
audit or incident requires a rebuild.

Scoped builds are not full-coverage audits. They intentionally skip
soft-delete by default because objects outside the scope were not checked. Use a
full-coverage audit build when deletion detection across the whole index set is
required.

Since builds are also not full-coverage audits. `--since auto` reads the latest
successful Gonimbus run for the same IndexSet and uses that run's start time as
the lower bound. If the watermark cannot be resolved safely, Gonimbus falls back
to full enumeration and warns instead of fabricating a timestamp or using an
empty scope.

## Practical Guidance For Orchestrators

- Do not reimplement listing when an index already covers the working set.
  Query the index and feed those rows into downstream stages.
- Treat `build.scope` and concrete include prefixes as the provider-cost control.
  Broad patterns such as `**/*.xml` under a broad base URI still require broad
  listing.
- Treat `build.match.filters` as the index-size control. It is useful, but it
  cannot avoid the provider list operation for objects under the selected
  prefixes.
- Build separate index sets for operational shards that need different coverage,
  such as site, month, or source family.
- Export validated index runs to an index hub so other agents and pipeline
  stages can hydrate and query without rebuilding.

Related docs:

- [Index user guide](index.md)
- [Steady-State Index Operations](steady-state-index-operations.md)
- [Indexing Architecture](../architecture/indexing.md)
- [ADR-0003: Index Build Provider Capabilities](../architecture/adr/ADR-0003-index-build-provider-capabilities.md)
