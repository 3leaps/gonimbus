# Steady-State Index Operations

Gonimbus indexes are designed for repeated operational builds. A build records
one run inside an index set, and queries read the latest object state for that
index set.

This page describes today's operating pattern for recurring builds. It does not
describe automatic incremental listing: `index build` does not currently have a
`--since` mode or a built-in delta report.

## Mental Model

An index set is the stable identity for an indexed source. The same manifest
continues to use the same index set when these identity inputs stay unchanged:

- Base URI
- Provider identity, including provider type, region, and endpoint
- Match and filter configuration
- `build.scope` configuration

Each successful repeat build appends a new run to that index set and updates
`objects_current`. `index query` reads `objects_current`, so results reflect the
latest indexed state rather than a historical run snapshot.

Changing the base URI, provider identity, match filters, or `build.scope` can
produce a different index set. Use `index list`, `index stats`, or
`index doctor` to confirm which `idx_*` identity a manifest is using before
comparing runs.

## Cadence Guidance

Choose rebuild cadence based on how likely a partition is to receive new,
changed, or corrected objects.

| Data window    | Suggested cadence              | Reason                                      |
| -------------- | ------------------------------ | ------------------------------------------- |
| Current period | Frequent rebuilds              | New objects and corrections are expected    |
| Recent periods | Less frequent rebuilds         | Late arrivals still happen                  |
| Closed periods | Frozen except audit/incident   | Avoid relisting stable data unnecessarily   |
| Audit pass     | Periodic full-coverage rebuild | Validate deletion state and source coverage |

The close window is an operational decision. A common pattern is to keep the
current period hot, rebuild the prior period for a fixed grace window after it
ends, and freeze older periods unless an audit or incident requires another
pass.

## Scoped Builds and Soft-Delete

Scoped builds reduce provider listing work by limiting the prefix plan. They are
appropriate for date-partitioned or shard-oriented operations, but they are not
full-coverage audits.

Soft-delete behavior depends on run coverage:

- A successful full-coverage build can mark previously seen objects as deleted
  when they are missing from the latest traversal.
- A scoped build skips soft-delete by default because objects outside the scoped
  prefix plan were not checked.
- A partial or interrupted run is not authoritative for deletion detection.

This means scoped indexes can become stale for deleted objects until a
full-coverage audit build is run. That tradeoff is usually correct for recurring
operational builds because it avoids interpreting "not listed in this scope" as
"deleted from the source."

## Recommended Pattern

1. Create one manifest per operational shard, such as a prefix, collection, or
   time window.
2. Keep the manifest identity stable for repeat builds of the same shard.
3. Run frequent scoped builds for active shards.
4. Run less frequent builds for recent shards that still receive late arrivals.
5. Freeze closed shards unless an audit or incident response requires a rebuild.
6. Schedule periodic full-coverage audit builds when deletion detection matters.
7. Export validated runs to an index hub so other operators can hydrate the
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

Inspect one index set and its identity:

```bash
gonimbus index doctor idx_1234abcd --detail
```

Review run history, prefix stats, and object counts:

```bash
gonimbus index stats s3://bucket/data/
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
  --index-set idx_1234abcd5678ef90 \
  --dest /tmp/gonimbus-indexes/
```

## Current Limits

Today, recurring builds are repeat listings over the manifest's effective
scope. Gonimbus does not yet provide:

- `index build --since` for provider-side delta listing
- A native "new since last run" report
- `index query --at-run` for historical run snapshots
- `index query --since-run` for query-time deltas

For now, treat each successful build as the latest state of the same index set.
Use a full-coverage audit build when you need deletion detection, and use
stable manifest identities when you need comparable run history.
