# Workspace Pattern

A gonimbus workspace is a git repository that tracks the configuration, manifests, and runbooks for an indexing and reflow pipeline. Large artifacts (index databases, logs) live in object storage via an index hub — not in git.

## When to Use a Workspace

| Scenario                        | Workspace? | Why                                       |
| ------------------------------- | ---------- | ----------------------------------------- |
| One-off inspection              | No         | Use `inspect` or `tree` directly          |
| Recurring index builds          | Yes        | Track manifests, automate builds          |
| Cross-account reflow            | Yes        | Track rewrite templates, hub config       |
| Multi-shard production pipeline | Yes        | Track shard strategy, retention, runbooks |

## Layout

```
my-pipeline/
├── workspace.yaml           # Hub roots, provider defaults, path conventions
├── manifests/               # Index build manifests (.yaml)
│   ├── store-01001-dec.yaml
│   └── store-01001-jan.yaml
├── runbook/                 # Operator docs, incident playbooks
│   └── build-publish.md
├── scripts/                 # Thin wrappers around gonimbus commands (optional)
│   └── build-and-publish.sh
└── .gitignore               # Ignore index.db, logs, checkpoints
```

### .gitignore

```gitignore
# Index artifacts (stored in hub, not git)
**/runs/**/index.db
*.db-wal
*.db-shm

# Job logs and checkpoints
**/jobs/
*.log
```

## workspace.yaml

This file is a convention — gonimbus does not parse it. Scripts and runbooks reference it for consistency.

```yaml
# workspace.yaml
project:
  name: retail-acquisition
  description: POS transaction data acquisition pipeline

source:
  bucket: s3://source-bucket
  base_uri: s3://source-bucket/production/data/
  profile: source-readonly

destination:
  bucket: s3://dest-landing-zone
  root: project-data/
  profile: dest-admin
  region: us-west-2

paths:
  data: data/ # Reflowed objects (clean, query-friendly)
  ops: ops/ # Operational artifacts
  index_hub: ops/index-hub/ # Hub root for index export/hydrate
  selections: ops/selections/ # Query result staging (input to reflow)
  logs: ops/logs/ # Run logs

hub:
  uri: s3://dest-landing-zone/project-data/ops/index-hub/
  profile: dest-admin
  region: us-west-2

shard_strategy:
  type: per-site-month # See "Shard Strategies" below
  site_segment_index: 0 # Position of site ID in path
  date_segment_index: 2 # Position of date in path
```

## Destination Path Convention

Separate reflowed data from operational artifacts:

```
s3://<dest-bucket>/<project-root>/
├── data/              <- Reflowed objects (clean lakehouse paths)
│   └── <rewrite-to template output>
└── ops/               <- Operational artifacts (not user-facing)
    ├── index-hub/     <- Hub root for index export/hydrate
    ├── selections/    <- Query result JSONL files (reflow input)
    └── logs/          <- Run logs, checkpoint summaries
```

**Why separate?**

- `data/` is the lakehouse-ready output. Downstream consumers (Spark, Athena, DuckDB) crawl `data/` without encountering operational artifacts.
- `ops/` contains pipeline metadata. Separate IAM policies can restrict write access to `ops/` while `data/` is read-only for consumers.

## Shard Strategies

Design principles:

- Prefer many bounded shards over one giant index
- Shard keys should align with stable path segments + time windows
- Each shard must be independently rebuildable and publishable
- Queries should target the smallest shard set possible

| Strategy               | Shard Key                | Use Case                                  |
| ---------------------- | ------------------------ | ----------------------------------------- |
| Per-site x month       | `{site}-{YYYY-MM}`       | Site-partitioned data with monthly cycles |
| Per-site x ISO-week    | `{site}-{YYYY-Www}`      | Weekly operational cycles                 |
| Per-collection x month | `{collection}-{YYYY-MM}` | Grouped path fragments                    |
| Rolling window         | `latest-{N}d`            | Operational dashboards, recent data       |

### Blast Radius

Shard granularity controls the blast radius of failures:

| Granularity              | Lock Blast Radius      | Recovery Scope    |
| ------------------------ | ---------------------- | ----------------- |
| One index per bucket     | Entire bucket unusable | Full rebuild      |
| One index per site       | Single site affected   | Site rebuild only |
| One index per site-month | One month of one site  | Minimal rebuild   |

## Operational Flows

### Build + Publish

```bash
# Build index from manifest
gonimbus index build --job manifests/store-01001-dec.yaml

# Export to hub
gonimbus index export \
  --hub s3://dest-bucket/project/ops/index-hub/ \
  --index-set idx_da038d8171b4a9ba... \
  --hub-profile dest-admin
```

### Hydrate + Query

```bash
# Hydrate latest run to local disk
gonimbus index hydrate \
  --hub s3://dest-bucket/project/ops/index-hub/ \
  --index-set idx_da038d8171b4a9ba... \
  --dest /tmp/hydrated/ \
  --hub-profile dest-admin

# Query the hydrated index
gonimbus index query s3://source-bucket/production/data/ \
  --index-set idx_da038d8171b4a9ba... \
  --pattern '**/report-*.xml' \
  --after 2025-12-01 --before 2026-01-01
```

### Extract + Reflow

```bash
# Query selection to file
gonimbus index query s3://source-bucket/production/data/ \
  --index-set idx_da038d8171b4a9ba... \
  --pattern '01001/**/RecordTypeAlpha*.xml' \
  --output s3://dest-bucket/project/ops/selections/dec-01001.jsonl \
  --output-profile dest-admin

# Reflow with key rewriting
gonimbus transfer reflow \
  --stdin < selections/dec-01001.jsonl \
  --src-profile source-readonly \
  --dest s3://dest-bucket/project/data/ \
  --dest-profile dest-admin \
  --rewrite-from 'production/data/{site}/{device}/{date}/{filename}' \
  --rewrite-to '{site}/{device}/{filename}'
```

### Hub Maintenance

```bash
# List what's in the hub
gonimbus index hub ls --hub s3://dest-bucket/project/ops/index-hub/ \
  --hub-profile dest-admin

# Show runs for an index set
gonimbus index hub show --hub s3://dest-bucket/project/ops/index-hub/ \
  --index-set idx_da038d8171b4a9ba... --hub-profile dest-admin

# Prune old runs (keep 3 most recent)
gonimbus index hub gc --hub s3://dest-bucket/project/ops/index-hub/ \
  --keep 3 --hub-profile dest-admin

# Repoint latest to a specific run
gonimbus index hub set-latest --hub s3://dest-bucket/project/ops/index-hub/ \
  --index-set idx_da038d8171b4a9ba... \
  --run-id run_1709654400000000000 \
  --hub-profile dest-admin
```

## Rewrite Template Guidance

The `--rewrite-from` template must match the **full S3 key**, not the `rel_key` from the index. This means the template includes any bucket-level prefix segments.

Example: if the source bucket has objects at `production/data/{site}/{device}/{date}/{file}`:

```bash
--rewrite-from 'production/data/{site}/{device}/{date}/{filename}'
--rewrite-to '{site}/{device}/{filename}'
```

The `--dest` flag provides the base prefix. The final destination key is `<dest-base>/<rewrite-to output>`.

Document rewrite templates alongside index build manifests in the workspace so the full pipeline is reproducible.

## Scheduling

Gonimbus does not include a scheduler. Use native scheduling tools:

| Environment | Tool                     | Example                                             |
| ----------- | ------------------------ | --------------------------------------------------- |
| Linux/macOS | cron                     | `0 2 * * * /workspace/scripts/build-and-publish.sh` |
| Kubernetes  | CronJob                  | Standard k8s CronJob spec                           |
| AWS         | EventBridge + ECS/Lambda | Scheduled rule triggering container task            |

A typical scheduled script:

```bash
#!/bin/bash
set -euo pipefail

# Build
gonimbus index build --job manifests/nightly.yaml

# Export to hub
gonimbus index export \
  --hub s3://hub-bucket/index-hub/ \
  --index-set "$INDEX_SET_ID" \
  --hub-profile hub-admin

# Prune old runs
gonimbus index hub gc \
  --hub s3://hub-bucket/index-hub/ \
  --keep 7 \
  --hub-profile hub-admin
```

## Getting Started

1. Create a workspace repository
2. Add `workspace.yaml` with your source/destination conventions
3. Create index build manifests in `manifests/`
4. Initialize the hub: `gonimbus index hub init --hub <hub-uri>`
5. Build, export, and query — see flows above
6. Add scheduling when the manual workflow is validated
