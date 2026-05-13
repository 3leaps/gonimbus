# Steady-State Index Operations

Gonimbus indexes are designed for repeated operational builds. A build records
one run inside an index set, and queries read the latest object state for that
index set.

This page describes today's operating pattern for recurring builds. It does not
describe automatic incremental listing: `index build` does not currently have a
`--since` mode, a built-in delta report, or historical query flags.

## Mental Model

An index set is the stable identity for an indexed source. The same manifest
continues to use the same index set when these identity inputs stay unchanged:

- Base URI
- Provider identity, including provider type, region, and endpoint
- Match and filter configuration
- `build.scope` configuration

Each successful repeat build appends a new run to that index set. The run is a
record of one traversal, and the index set's current object table is updated to
the latest indexed state. `index query` reads that current table, so query
results answer "what is current for this index set?" rather than "what did run
N see?"

Changing the base URI, provider identity, match filters, or `build.scope` can
produce a different index set. The identity is derived from those inputs; it is
not a durable human-selected name. Use `index list`, `index stats`, or
`index doctor` to confirm which `idx_*` identity a manifest is using before
comparing runs.

## Cadence Guidance

Choose rebuild cadence based on how likely a partition is to receive new,
changed, or corrected objects.

| Data window    | Suggested cadence                    | Reason                                      |
| -------------- | ------------------------------------ | ------------------------------------------- |
| Current period | Nightly or more often                | New objects and corrections are expected    |
| Recent periods | Weekly during the close window       | Late arrivals still happen                  |
| Closed periods | Frozen except audit or incident work | Avoid relisting stable data unnecessarily   |
| Audit pass     | Monthly or release-gated full build  | Validate deletion state and source coverage |

The close window is an operational decision. A useful default is a rolling
14-day close window after the end of a period: keep the current period hot,
continue rebuilding the just-ended period during the grace window, then freeze
older periods unless an audit or incident requires another pass. Adjust the
window for sources with longer delivery lag.

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
intentional for recurring operational builds: gonimbus avoids interpreting
"not listed in this scope" as "deleted from the source."

## Recommended Pattern

1. Create one manifest per operational shard, such as a prefix, collection, or
   time window.
2. Keep the manifest identity stable for repeat builds of the same shard.
3. Run frequent scoped builds for current or otherwise active shards.
4. Continue rebuilding recent shards through the close window.
5. Freeze closed shards unless an audit or incident response requires a rebuild.
6. Schedule periodic full-coverage audit builds when deletion detection matters.
7. Compare run counts and stats after each scheduled build before publishing.
8. Export validated runs to an index hub so other operators can hydrate the
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

Today, recurring builds are repeat listings over the manifest's effective
scope. They are the supported pattern for steady-state operations, but they are
not automatic delta builds. Gonimbus does not yet provide:

- `index build --since` for provider-side delta listing
- A native "new since last run" report
- `index query --at-run` for historical run snapshots
- `index query --since-run` for query-time deltas

For now, treat each successful build as the latest state of the same index set.
Use a full-coverage audit build when you need deletion detection, and use
stable manifest identities when you need comparable run history.
