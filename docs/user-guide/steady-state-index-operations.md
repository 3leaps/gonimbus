# Steady-State Index Operations

Gonimbus indexes are designed for repeated operational builds. A build records
one run inside an index set, and queries read the latest object state for that
index set.

This page is for operators running recurring `index build` jobs against large
or growing buckets, where each build should add to an index set's run history
rather than create a new index set. It describes the repeat-build pattern and
the `index build --since` mode for incremental top-ups.

## Mental Model

An index set is the stable identity for an indexed source. The same manifest
continues to use the same index set when these identity inputs stay unchanged:

- Base URI
- Provider identity, including provider type, region, and endpoint
- Match and filter configuration
- `build.scope` configuration

Each successful repeat build appends a new run to that index set. The run is a
record of one traversal, and the index set's current object table
(`objects_current`) is updated to the latest indexed state. `index query` reads
that current table, so query results answer "what is current for this index
set?" rather than "what did run N see?"

Changing the base URI, provider identity, match filters, or `build.scope` can
produce a different index set. The identity is derived from those inputs; it is
not a durable human-selected name. Use `index list`, `index stats`, or
`index doctor` to confirm which `idx_*` identity a manifest is using before
comparing runs.

## Cadence Guidance

Choose rebuild cadence based on how likely a partition is to receive new,
changed, or corrected objects.

| Data window    | Suggested cadence                      | Reason                                      |
| -------------- | -------------------------------------- | ------------------------------------------- |
| Current period | Nightly or more often                  | New objects and corrections are expected    |
| Recent periods | Weekly during the close window         | Late arrivals still happen                  |
| Closed periods | Frozen except audit or incident work   | Avoid relisting stable data unnecessarily   |
| Audit pass     | Monthly, or after major source changes | Validate deletion state and source coverage |

The close window is an operational decision. A useful default is a rolling
14-day close window after the end of a period: keep the current period hot,
continue rebuilding the just-ended period during the grace window, then freeze
older periods unless an audit or incident requires another pass. Adjust the
window for sources with longer delivery lag, such as late-arriving objects from
upstream producers.

When you need a quick operator check, `index list` shows whether repeated
builds are landing in the same index set and how many runs that set has
accumulated. A run-count increase after the scheduled build is the first signal
that the repeat-build path is using the same manifest identity.

## Scoped Builds and Soft-Delete

Scoped builds reduce provider listing work by limiting the prefix plan. They are
appropriate for date-partitioned or shard-oriented operations, but they are not
full-coverage audits. This distinction matters most for deletion detection.

Soft-delete behavior depends on run coverage:

- A successful full-coverage build can mark previously seen objects as deleted
  when they are missing from the latest traversal.
- A scoped build skips soft-delete by default because objects outside the scoped
  prefix plan were not checked.
- A partial or interrupted run is not authoritative for deletion detection.

This means a scoped index can remain stale for deleted objects outside the
latest prefix plan until a full-coverage audit build is run. That tradeoff is
intentional for recurring operational builds: Gonimbus avoids interpreting
"not listed in this scope" as "deleted from the source."

`index build --since` follows the same rule. A since build is not a
full-coverage audit, even when it exits successfully, so it skips soft-delete.
Objects deleted outside the since listing plan can remain visible until a
full-coverage audit build runs.

## Incremental Top-Ups with `--since`

Use `--since <timestamp>` when a recurring build should ingest objects modified
at or after a known lower bound:

```bash
gonimbus index build --job manifests/current.yaml --since 2026-07-02T00:00:00Z
```

Use `--since auto` to let Gonimbus read the latest successful run for the same
IndexSet and use that run's start time as the lower bound:

```bash
gonimbus index build --job manifests/current.yaml --since auto
```

Bare `--since` is accepted as shorthand for `--since auto`.

The watermark comes only from Gonimbus-written run metadata for the same
IndexSet. If the prior successful run is missing, unreadable, ambiguous, or in
the future, Gonimbus fails closed to full enumeration and prints a warning. It
does not fabricate `now`, use source-object metadata as authority, or produce
an empty listing scope.

Dry-run plans do not open the local index database. `--dry-run --since auto`
therefore shows the fail-closed full-enumeration preview; use an explicit
timestamp with dry-run to inspect the narrowed date-partition prefix plan.

For manifests with `build.scope.type: date_partitions`, `--since` narrows the
date range before provider LIST calls. This is the mode that reduces
enumeration cost for date-partitioned layouts. Mixed `union` scopes can report
`enumeration_reduction: partial` when only some child scopes can be narrowed;
the remaining child scopes still use full enumeration with a last-modified
ingest filter. For non-date-ordered layouts, Gonimbus cannot infer a cheaper
listing plan from a timestamp; it falls back to full enumeration with that same
filter and reports that enumeration reduction was not applied.

Each since run prints a small delta report by effective crawl prefix:
`added`, `changed`, and `unchanged`. The report is emitted on the same
operator-controlled output surfaces as the normal run summary.

### Confirming the reduction signal

Treat the since-plan signal as required run evidence. A successful `--since`
build means the index was updated; it does not automatically mean provider
enumeration was reduced.

Check the run output before treating a top-up as the cheaper path:

- `enumeration_reduction: yes` means the listing plan was narrowed before
  provider LIST.
- `enumeration_reduction: partial` means only part of a mixed scope could be
  narrowed; the remaining scope still used full enumeration with the
  last-modified ingest filter.
- `enumeration_reduction: no` means Gonimbus could not derive a cheaper
  prefix plan and used full enumeration with the ingest filter.

The stored `since_plan` event also includes
`enumeration_reduction_applied` and `enumeration_reduction_partial` booleans
for scripts that should not parse display text.

The fallback is deliberately visible. If you expected reduction and the run
reported `no`, check whether the manifest has a `date_partitions` scope, the
manifest identity still matches the prior IndexSet, and the resolved watermark
is plausible for the operational shard.

### Downstream deltas with `--since-run`

After a successful top-up, downstream consumers can read current objects added
or changed after a completed boundary run:

```bash
gonimbus index query \
  --index-set idx_da038d8171b4a9ba \
  --since-run run_1783087200000000000
```

This is useful for "process only new or changed objects" flows. For example, a
consumer can feed the JSONL output into a reorganization or validation stage
without relisting the source.

The boundary run must be known, successful, and in the same IndexSet. Gonimbus
compares stored run ordering metadata rather than sorting run ID strings. For
indexes migrated from older schemas, precise added/changed classification
starts at the migration baseline; older boundary runs are rejected.

`--since-run` is latest-state delta query. It does not reconstruct historical
snapshots and does not report deletion deltas. `--include-deleted --since-run`
is rejected rather than implying deletion history.

## Recommended Pattern

1. Create one manifest per operational shard, such as a prefix, collection, or
   time window.
2. Keep the manifest identity stable for repeat builds of the same shard.
3. Run frequent scoped builds for current or otherwise active shards.
4. Continue rebuilding recent shards through the close window.
5. Use `--since auto` for steady-state top-ups when the manifest identity stays
   stable.
6. Confirm the since-plan signal reports the expected enumeration reduction.
7. Use `index query --since-run <run_id>` for downstream current-state deltas.
8. Freeze closed shards unless an audit or incident response requires a rebuild.
9. Schedule periodic full-coverage audit builds when deletion detection matters.
10. Compare run counts and stats after each scheduled build before treating the
    run as ready for downstream queries.
11. Export validated runs to an index hub so other operators can hydrate the
    current run without rebuilding.

## Useful Commands

Build the same manifest repeatedly:

```bash
gonimbus index build --job manifests/current-month.yaml
```

List index sets and run counts:

```bash
gonimbus index list
```

The same manifest should continue to report the same `idx_*` identity with an
increasing run count. If a scheduled build creates a new index set, compare the
manifest's base URI, provider identity, match filters, and `build.scope`.

Commands that accept an index set ID also accept an unambiguous short prefix.
Use the full `idx_*` value in automation, and use short prefixes only for
interactive inspection.

Inspect one index set and its identity:

```bash
gonimbus index doctor idx_1234abcd --detail
```

Review object counts, run history, and prefix stats:

```bash
gonimbus index stats s3://bucket/data/ --runs --prefixes
```

Publish a validated run to a hub:

```bash
gonimbus index export \
  --hub s3://ops-bucket/index-hub/ \
  --index-set idx_1234abcd5678ef90 \
  --hub-profile hub-admin
```

Hydrate the latest published run:

```bash
gonimbus index hydrate \
  --hub s3://ops-bucket/index-hub/ \
  --index-set idx_1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef \
  --dest /tmp/gonimbus-indexes/
```

## Current Limits

`--since` reduces enumeration only when the manifest can map the watermark to a
narrower listing plan, such as `date_partitions`. It is not a general
provider-side modified-time query for arbitrary key layouts.

Gonimbus does not yet provide:

- query-time historical object views over prior runs
- `index query --at-run` for historical run snapshots

For query-time deltas, `index query --since-run <run_id>` emits the current
active rows first seen or meaningfully changed after a successful run in the
same IndexSet. It is a forward delta over latest state, not point-in-time
history. For indexes migrated from older schemas, precise `added` / `changed`
classification begins at the migration baseline, and boundary runs before that
baseline are rejected.

Use a full-coverage audit build when you need deletion detection.
`--since-run` does not track deletion history, so it rejects
`--include-deleted --since-run` rather than implying a deletion delta. Use
stable manifest identities when you need comparable run history.
