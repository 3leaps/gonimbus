# Local Index

The local index enables fast repeated queries against large buckets without re-enumerating via the provider API.

For the operator mental model behind listing, ingest filtering, stored metadata,
and what filtered indexes do not retain, see
[Index Build Mental Model](index-build-mental-model.md).

For recurring production builds, see
[Steady-State Index Operations](steady-state-index-operations.md) for cadence,
run-history, and soft-delete guidance.

For content-addressed post-pass artifacts over completed index runs, see
[Atlas Artifacts](atlas.md).

For the v0.4.0 durable-default operator map (parity, hub round-trip, SQLite
escape hatches, boundary framing), see
[Durable Index Format](durable-index.md).

## When to Use the Index

Gonimbus supports two workflows based on bucket scale:

| Workflow      | Scale       | Commands                     | Index? |
| ------------- | ----------- | ---------------------------- | ------ |
| **Explore**   | <1M objects | `tree`, `inspect`, `crawl`   | No     |
| **Inventory** | 1M+ objects | `index build`, `index query` | Yes    |

**Use the Explore workflow** when:

- You need to understand bucket structure (`tree`)
- You need quick spot-checks (`inspect`)
- You need a one-time full enumeration (`crawl`)

**Use the Index workflow** when:

- Live enumeration takes too long (minutes to hours)
- You need to run the same query multiple times
- You need instant pattern matching on large datasets

## Quick Start

```bash
# Create an index manifest (see below)
# Then build the index (default format: durable)
gonimbus index build --job index-manifest.yaml

# SQLite when you need SQLite-only surfaces (gc, --since-run, --resume-run, …)
gonimbus index build --job index-manifest.yaml --format sqlite

# Format-aware query works on durable or SQLite sets
gonimbus index query 's3://my-bucket/data/' --pattern '**/report-*.xml' --count
```

### Artifact formats

| Format                | Build flag         | What it produces                                           | Local consumers today                                                                |
| --------------------- | ------------------ | ---------------------------------------------------------- | ------------------------------------------------------------------------------------ |
| **durable** (default) | `--format durable` | Segment-backed durable-v2 snapshot under the segment cache | `query`, `list`, `stats`, `doctor`, `enrich-with-head`, export/hydrate/compare       |
| **sqlite**            | `--format sqlite`  | Classic `index.db` under `indexes/idx_*/`                  | All local consumers including **`gc`**, `--since-run`, full `--resume-run` lifecycle |
| **both**              | `--format both`    | Dual-build + parity report for migration validation        | Both surfaces for the same crawl (SQLite preferred when `index.db` exists)           |

Durable is now the default index artifact format. SQLite remains an explicit
compatibility/transition mode. Durable hydrate restores `manifest.json` +
segments, **not** `index.db`. Format-aware local consumers work on durable-only
sets; keep `--format sqlite` or `both` when you still need **`gc`**,
**`query --since-run`**, **`stats --prefixes`**, or full **`--resume-run`**
checkpoint recovery.

See [Durable Index Format](durable-index.md) for the full migration map, green
parity semantics, hub round-trip, segment packing defaults, and internal-render
boundary framing.

## Index Manifest

Index builds require a manifest with explicit provider identity:

```yaml
version: "1.0"

connection:
  provider: s3
  bucket: my-bucket
  region: us-east-1
  base_uri: s3://my-bucket/data/

identity:
  storage_provider: aws_s3
  cloud_provider: aws
  region_kind: aws
  region: us-east-1

build:
  match:
    includes:
      - "**/*"
  crawl:
    concurrency: 16

output:
  destination: stdout
```

### S3-Compatible Providers

For Wasabi, Cloudflare R2, or other S3-compatible stores:

```yaml
connection:
  provider: s3
  bucket: my-bucket
  region: us-east-2
  endpoint: https://s3.us-east-2.wasabisys.com
  base_uri: s3://my-bucket/data/

identity:
  storage_provider: wasabi
  cloud_provider: other
  region_kind: aws
  region: us-east-2
  endpoint_host: s3.us-east-2.wasabisys.com
```

### Scoping with Include Patterns

To index only a subset of the bucket, use include patterns:

```yaml
build:
  match:
    includes:
      - "2025/**" # Only 2025 data
      - "production/**" # Only production prefix
```

Include patterns support doublestar globs. The index will enumerate only the prefixes that can be derived from these patterns.

### Scoping with `build.scope`

Use `build.scope` to generate an explicit prefix plan before listing. This is the primary lever for reducing provider listing work on date-partitioned layouts.

```yaml
build:
  scope:
    type: date_partitions
    base_prefix: "data/"
    discover:
      segments:
        - index: 0 # discover store IDs under data/
    date:
      segment_index: 1
      format: "2006-01-02"
      range:
        after: "2025-12-01" # inclusive
        before: "2026-01-01" # exclusive
```

Notes:

- Supported scope types: `prefix_list`, `date_partitions`, `union`.
- Scope controls **what is listed**. Match filters control **what is ingested**.
- `build.scope` is included in the index identity; changing scope produces a new index.
- Scoped builds skip soft-delete by default because the prefix plan is not full coverage.
- `gonimbus index build --dry-run` prints the scope plan so you can audit prefix counts.
- Prefix-shaped SQLite-era `match.includes` (literal prefix + terminal `/**`) can be
  audited/converted with `gonimbus index migrate-match-scope` into
  `build.scope` `prefix_list` for durable builds. See
  [Durable Index — match→scope migration](durable-index.md#prefix-shaped-match--scope-migration-g11-subset).
  Remaining match controls (excludes, non-prefix globs, filters) stay open.

### Incremental builds with `--since`

Use `index build --since <timestamp>` for recurring top-ups that should ingest
objects modified at or after a lower bound:

```bash
gonimbus index build --job index-manifest.yaml --since 2026-07-02T00:00:00Z
```

Use `--since auto` to read the latest successful run for the same IndexSet and
use that run's start time as the watermark:

```bash
gonimbus index build --job index-manifest.yaml --since auto
```

Bare `--since` is accepted as shorthand for `--since auto`.

When the manifest uses `build.scope.type: date_partitions`, `--since` narrows
the date partition range before LIST, reducing enumeration for date-partitioned
layouts. Mixed `union` scopes can report `enumeration_reduction: partial` when
only the date-partitioned branches can be narrowed; non-date branches still use
full enumeration with a last-modified ingest filter. If the layout is not
date-partitioned, Gonimbus reports that enumeration reduction was not applied
and falls back to full enumeration with that same filter.

Since builds are not full-coverage audits. They skip soft-delete and can leave
deletion state stale until a full-coverage build runs. Each since run prints a
per-prefix delta report with `added`, `changed`, and `unchanged` counts.
Also check the since-plan reduction signal: `enumeration_reduction: yes` means
the listing plan was narrowed before provider LIST, `partial` means only part
of a mixed scope was narrowed, and `no` means the run used full enumeration
with the last-modified ingest filter. The stored `since_plan` event also
includes `enumeration_reduction_applied` and
`enumeration_reduction_partial` booleans for scripts.

Dry-run plans do not open the local index database. `--dry-run --since auto`
therefore previews the fail-closed path rather than resolving a prior run; use
`--dry-run --since <timestamp>` to inspect concrete date-partition narrowing.

## Commands

### `doctor`

Run local environment diagnostics. S3 provider diagnostics accept explicit
endpoint and region overrides so you can check S3-compatible targets without
mutating shell-wide AWS environment variables.

```bash
# General diagnostics
gonimbus doctor

# S3 credential/config diagnostics
gonimbus doctor --provider s3 --profile archive

# S3-compatible endpoint diagnostics
gonimbus doctor --provider s3 \
  --profile archive \
  --endpoint https://s3.us-east-2.wasabisys.com \
  --region us-east-2
```

`--endpoint`, `--region`, and `--probe-uri` are valid only with
`--provider s3`. When both CLI flags and environment variables are present,
the CLI flag wins.

#### Opt-In S3 Probe

By default, `doctor --provider s3` checks configuration and credentials only.
It does not make bucket/object calls unless you pass `--probe-uri`.

```bash
# Bucket-level read probe
gonimbus doctor --provider s3 --probe-uri s3://bucket

# Prefix-scoped read probe
gonimbus doctor --provider s3 --probe-uri s3://bucket/some/prefix/

# Exact-key read probe
gonimbus doctor --provider s3 --probe-uri s3://bucket/path/to/object.xml
```

| URI shape                 | Probe operation                    |
| ------------------------- | ---------------------------------- |
| `s3://bucket`             | `ListObjectsV2(MaxKeys=1)`         |
| `s3://bucket/prefix/`     | `ListObjectsV2(Prefix, MaxKeys=1)` |
| `s3://bucket/path/to/key` | `HeadObject`                       |

Probe URIs must be precise targets. Glob patterns such as
`s3://bucket/prefix/**/*.xml` or `s3://bucket/foo?bar` are rejected; use a
bucket, trailing-slash prefix, or exact key.

The probe is read-only and never uses `HeadBucket`, `PutObject`, or
`DeleteObject`. `HeadObject` and `ListObjectsV2(MaxKeys=1)` are low-cost
interactive checks, but do not run `doctor` inside high-volume loops.

#### Operational Data Root

Gonimbus stores local indexes, index-build job records, and resumable operation
checkpoints under one app-wide data root. By default, the root follows the
platform app-data location; if `XDG_DATA_HOME` is set, the default is derived
from that location.

Use `GONIMBUS_DATA_DIR` for a one-process override:

```bash
GONIMBUS_DATA_DIR=/mnt/gonimbus-data gonimbus index build --job index-manifest.yaml
```

Use `data_root` in the user config for a persistent override:

```yaml
data_root: /mnt/gonimbus-data
```

Precedence is:

1. `GONIMBUS_DATA_DIR` (`GONIMBUS_DATA_ROOT` is accepted as an environment alias)
2. `data_root` in config (`data_dir` is accepted as an alias)
3. `XDG_DATA_HOME`
4. Platform default

The override is an app-wide root, not an index-only setting. Keep indexes,
index-build job records, reflow state, and operation checkpoints under the
same root so resume and diagnostics stay coherent.

Gonimbus does not auto-migrate existing local data when the root changes. To
relocate, stop active jobs, copy the existing data-root contents to the new
directory, set the override, then run `gonimbus doctor` and an index command
such as `gonimbus index doctor` before resuming scheduled work.

Do not place the data root inside a git working tree. Gonimbus rejects any
resolved data root that lands there, including roots derived from `XDG_DATA_HOME`
or through symlinked components. Prefer a host-local directory with owner-only
permissions; avoid cloud-synced, backed-up, or network-shared folders unless
your local data-at-rest controls explicitly cover that location.

### `index init`

Initialize the local index database. Run once before building indexes.

```bash
gonimbus index init
```

### `index build`

Build an index from a crawl manifest.

```bash
gonimbus index build --job index-manifest.yaml
```

Progress is streamed to stdout. Build time scales linearly with object count (~3,000 objects/sec).

If a long-running build stops on a resumable fatal interruption such as an
expired refreshable credential or operator cancellation, Gonimbus preserves the
partial run as `failed-resumable`, writes a redacted
`gonimbus.operation.error.v1` JSON record to stdout, and prints a short stderr
summary with `run_id`, `status`, `error_class`, progress counters, and a
`gonimbus index build --resume-run <run_id>` command. Runtime failures do not
print command help; argument errors still do.

Resume is explicit:

```bash
gonimbus index build --resume-run <run_id>
```

The resume path validates checkpoint identity and fresh credentials before any
data-plane call or run-state mutation. Recovery from a crashed resume records
`resume_recovered`; normal resume attempts record `resume_started`, then
`resume_completed` on success.

### `index list`

List all local indexes.

```bash
gonimbus index list
```

Output shows base URI, provider, object count, size, latest status, latest run
ID, and identity health. If the latest index run is `failed-resumable`, list
output includes a `gonimbus index ... --resume-run <run_id>` hint for the
operation that wrote the checkpoint.

**Format-aware:** `index list` discovers durable segment sets and SQLite
`index.db` directories. Dual-format sets list once (preferred identity). See
[Durable Index Format](durable-index.md).

### `index query`

Query indexed objects by pattern and filters.

```bash
# Pattern matching
gonimbus index query 's3://bucket/prefix/' --pattern '**/data/*.parquet'

# With metadata filters
gonimbus index query 's3://bucket/prefix/' \
  --after 2025-12-01 \
  --before 2026-01-01 \
  --min-size 1KB

# With LIST-derived storage class filters
gonimbus index query 's3://bucket/prefix/' \
  --storage-class GLACIER,DEEP_ARCHIVE

# Count only (no output records)
gonimbus index query 's3://bucket/prefix/' --pattern '**/*.json' --count

# Emit current objects first seen or changed after a completed run
gonimbus index query 's3://bucket/prefix/' \
  --since-run run_1783087200000000000

# Emit one canonical object per non-empty ETag group
gonimbus index query 's3://bucket/prefix/' --canonical-by-etag

# Include non-canonical ETag-equivalent rows for audit flows
gonimbus index query 's3://bucket/prefix/' \
  --canonical-by-etag \
  --include-alternates

# Write results to a local file
gonimbus index query 's3://bucket/prefix/' --pattern '**/*.xml' \
  --output file:///tmp/results.jsonl

# Write results to S3
gonimbus index query 's3://bucket/prefix/' --pattern '**/*.xml' \
  --output s3://output-bucket/queries/results.jsonl

# Write results to S3 with cross-account credentials
gonimbus index query 's3://bucket/prefix/' --pattern '**/*.xml' \
  --output s3://other-account-bucket/results.jsonl \
  --output-profile other-account
```

When `--output` is set, stdout is silent and results are written to the destination. Summary output stays on stderr.

If the latest run for the selected index is `failed-resumable`, `index query`
still allows inspection of the local partial index but prints a stderr warning
with the resumable run ID. Treat those query results as checkpoint-state
inspection, not as a validated completed snapshot.

Canonical local SQLite reads are strict and non-mutating: query, stats, list,
atlas, export, doctor, and GC do not enable WAL or migrate a legacy schema as a
side effect. General readers hold the same stable whole-set authority as GC for
their full snapshot lifetime and reject existing or newly appearing WAL/SHM,
rollback, master, and statement journals rather than ignoring transactionally
current state. A live writer or GC therefore causes loud contention/refusal,
never an immutable read of stale base bytes. Canonical reads require a valid
`identity.json`, exactly one database index set, and equality between the
marker, authority, and database IDs; missing, corrupt, mismatched, or ambiguous
identity fails closed. A legacy database fails with an explicit schema error.
There is no supported in-place command that reconstructs or adopts a missing
canonical identity marker from `index.db`. `index init` migrates schema; it does
not repair identity.

The refusal applies to every build format that can publish canonical identity.
That includes the default durable-only build: although it does not create
`index.db`, it will not write `identity.json` beside an existing markerless
database. SQLite and dual-format builds enforce the same boundary whether the
canonical database is selected by default or supplied explicitly with `--db`.
Typing the canonical path does not downgrade it to an external database.

An explicit local `--db` proven outside the configured `indexes/` root remains
an operator-owned compatibility target. Gonimbus does not publish canonical
identity beside it; the caller must exclude other writers and maintenance for
the full operation. A path inside the canonical root, or a path that resolves
into it through a link, is never treated as external.

The supported migration for a pre-marker canonical SQLite directory is a full
source rebuild:

1. Stop every Gonimbus process using that data root. Preserve the entire
   affected `indexes/idx_*` directory by **moving the whole directory** to a
   backup location outside the configured data root. A backup copy is not
   sufficient: SQLite/both builds refuse an existing canonical `index.db`
   whose valid matching marker is absent. Do not copy or open only `index.db`,
   and do not discard transaction sidecars from an artifact that may have been
   active.
2. Rerun the original index manifest against the source with the required
   format, for example
   `gonimbus index build --job index-manifest.yaml --format sqlite` (or
   `--format both`). The build path takes stable whole-set authority and
   publishes a new database and `identity.json`; this is a rebuild, not an
   adoption of the old database.
3. Require `gonimbus index list --json` to report `"identity_status": "ok"`,
   then run `gonimbus index doctor` before retiring the preserved backup under
   the operator's retention policy.

If the original manifest/scope cannot be recovered, keep the legacy artifact
retained and fail closed; do not synthesize an identity marker. Upgrade old
schemas separately through a guarded path such as
`index init --db <canonical-index.db>` or a subsequent build.

`--since-run <run_id>` emits the current active rows first seen or meaningfully
changed after a successful run in the same IndexSet. **SQLite-only today**
(durable query fails closed). It is a forward delta over latest index state,
intended for "only process new or changed objects" flows. It is not
point-in-time history: the current SQLite index does not retain object
snapshots for older runs, so it cannot reconstruct "state as of run X".

Delta tracking uses Gonimbus-written run metadata and compares run boundaries by
stored run timestamps, not by run ID string sorting. Unknown, non-successful, or
cross-IndexSet run IDs fail closed. For indexes migrated from older schemas,
precise `added` / `changed` classification begins at the migration baseline run;
older boundary runs are rejected rather than returning a confident but
under-specified delta.

`--since-run` output keeps the existing `gonimbus.index.object.v1` record type
and adds optional fields such as `change_kind`, `first_seen_run_id`, and
`last_changed_run_id`. `change_kind` is `added` when the object first appeared
after the boundary, and `changed` when an existing or reappeared object changed
after the boundary. Deletion history is not tracked in this index format, so
`--include-deleted --since-run` is rejected instead of implying a deletion
delta.

`--canonical-by-etag` groups query results by non-empty ETag and emits one
`gonimbus.index.object.canonical.v1` record per group. Rows with empty or
missing ETag are emitted unchanged as standard `gonimbus.index.object.v1`
records, so consumers should branch on the JSONL `type` field when using this
mode. Existing filters apply before grouping: a row excluded by `--pattern`,
`--key-regex`, size, date, or `--storage-class` filters cannot become
canonical and cannot appear as an alternate.

Canonical selection defaults to `--canonical-tie-break min-key`, which chooses
the lexicographically smallest `rel_key`. `min-modified` and `max-modified`
choose by `last_modified` with `rel_key` as the deterministic secondary
tie-break. `--include-alternates` adds an `alternates[]` array for audit and
verification flows; `alternates_count` is always emitted on canonical records.
Canonical records include `canonical.size_bytes`, and alternate entries include
`alternates[].size_bytes` so dedup audit consumers can calculate skipped bytes
without issuing a second query.
`--count` and `--limit` operate on output records after grouping, counting
canonical records and empty-ETag passthrough records together.

ETag is a provider version/fingerprint hint, not a universal content hash. In
particular, multipart-uploaded S3 objects can have ETags that differ across
uploads of the same bytes. See [Index Build Mental Model](index-build-mental-model.md)
for the indexing caveats behind this mode.

### `index stats`

View detailed statistics for an index.

```bash
gonimbus index stats 's3://bucket/prefix/'
```

Use `--runs` to include run IDs, statuses, and resume hints for
`failed-resumable` runs. The run summary counts failed-resumable runs
separately from hard failed runs.

### `index doctor`

Validate index integrity and identity.

```bash
# Summary view
gonimbus index doctor

# Detailed diagnostics for one local index
gonimbus index doctor idx_1234abcd --detail
```

### `index show`

Alias for `index doctor`.

```bash
gonimbus index show idx_1234abcd
```

### `index gc`

Audit or reclaim old local index sets across SQLite and durable storage. The
JSON dry-run plan includes a stable plan digest and exact identity, segment-set,
and journal targets. Rerunning the same policy without `--dry-run` takes
per-set exclusion, recomputes and matches that plan under the held leases, and
revalidates active state plus every target digest immediately before mutation.
Unproven or active artifacts are retained with warnings.

```bash
# Plan all but the last 3 sets for each base URI
gonimbus index gc --keep-last 3 --dry-run

# Plan sets older than 30 days
gonimbus index gc --max-age 30d --dry-run --json

# Preview what would be removed
gonimbus index gc --keep-last 1 --dry-run

# Execute the same retention policy
gonimbus index gc --keep-last 1
```

Execution first completes any interrupted GC transaction recorded beneath the
operator data root. Targets are quarantined within their canonical artifact
root before recursive removal, while the intent/receipt remains outside every
deletion target. GC holds the same stable per-set authority used by public
build and enrich libraries from pre-quarantine revalidation through recovery;
the lock is outside the renameable set root, so a library call cannot re-create
the canonical root while deletion is in flight. A crash after any target move
or removal is safe to retry.
`--json` emits `gonimbus.index.gc.result.v1` with the executed plan digest,
transaction ID, verified removed bytes, and recovery status.

This command removes whole proven index sets only. It does not prune individual
durable runs, journals, or segments within a retained set. Unknown legacy
layouts, symlinks/path aliases, changed target bytes, active jobs/checkpoints,
and durable sets without a proven writer-lock artifact fail closed.

### `index compare`

Compare index artifacts.

**Dual-format parity** is produced by the build itself:

```bash
gonimbus index build --job index-manifest.yaml --format both
```

That single crawl materializes SQLite and durable artifacts and emits a
`gonimbus.index.compare_result.v1` report. Green parity certifies LIST-projection
fidelity only — not reflow readiness. See
[Durable Index Format](durable-index.md#dual-format-parity) and
[Index Compare Projection v1](../architecture/index-compare-projection-v1.md).

**Temporal durable delta** compares two durable snapshots:

```bash
gonimbus index compare durable-delta \
  --before-manifest /path/to/before/manifest.json \
  --before-segments /path/to/before/segments \
  --after-manifest /path/to/after/manifest.json \
  --after-segments /path/to/after/segments
```

## Job Management

For long-running builds (hours on large buckets), gonimbus provides managed job execution with durable state and background operation.

### Starting Background Builds

```bash
# Start a managed background build (returns job id immediately)
gonimbus index build --background --job index-manifest.yaml

# With a human-friendly name
gonimbus index build --background --job index-manifest.yaml --name nightly-sweep

# Prevent duplicate running jobs for the same effective invocation
gonimbus index build --background --job index-manifest.yaml --dedupe
```

The `--background` flag supports `--format sqlite`, `--format durable`, and
`--format both`. It spawns a managed child process and returns immediately with
a job ID. The child verifies a versioned effective invocation and the exact
manifest content accepted by the parent before building. `--dedupe` collapses
only active jobs with the same effective format, manifest digest, incremental
input, identity overrides, and other behavior-affecting options; changing any
of those inputs starts a distinct job.

### Local Control Plane API

`gonimbus serve` exposes the same managed index job machinery over local HTTP
routes when running on a loopback bind such as `localhost` or `127.0.0.1`.
Because this Phase 1 API is unauthenticated, `serve` rejects non-loopback hosts
while the local job control API is enabled. Phase 1 supports `index.build` jobs
from local manifest paths only; remote manifest URIs, webhooks, queue
consumers, and multi-worker scheduling are intentionally deferred.

```bash
# Start the local control plane
gonimbus serve

# Submit an index build job
curl -X POST http://localhost:8080/api/v1/jobs \
  -H 'Content-Type: application/json' \
  -d '{"type":"index.build","manifest_path":"/absolute/path/index-manifest.yaml","name":"nightly-sweep","since":"auto"}'

# List jobs
curl 'http://localhost:8080/api/v1/jobs?status=running&type=index.build'

# Check or cancel a job
curl http://localhost:8080/api/v1/jobs/<job_id>
curl -X DELETE http://localhost:8080/api/v1/jobs/<job_id>
```

### Monitoring Jobs

```bash
# List all running and recent jobs
gonimbus index jobs list

# JSON output for scripting
gonimbus index jobs list --json

# Check status of a specific job (supports short ID prefixes)
gonimbus index jobs status <job_id>
gonimbus index jobs status abc1  # short prefix if unambiguous
```

### Streaming Logs

```bash
# View job logs
gonimbus index jobs logs <job_id>

# Follow logs in real-time
gonimbus index jobs logs <job_id> --follow

# Tail recent lines
gonimbus index jobs logs <job_id> --tail 100
```

### Stopping Jobs

```bash
# Graceful stop (SIGTERM -> context cancellation)
gonimbus index jobs stop <job_id>

# Force stop (SIGKILL) - use as last resort
gonimbus index jobs stop <job_id> --signal kill
```

Graceful cancellation produces a `partial` run status and preserves index integrity.

### Cleaning Up Job Records

```bash
# Remove job records older than 7 days
gonimbus index jobs gc --max-age 168h

# Preview what would be removed
gonimbus index jobs gc --max-age 168h --dry-run
```

### Job States

| State      | Meaning                               |
| ---------- | ------------------------------------- |
| `queued`   | Job created, not yet started          |
| `running`  | Build in progress                     |
| `stopping` | Graceful shutdown in progress         |
| `stopped`  | Cancelled by user                     |
| `success`  | Build completed successfully          |
| `partial`  | Build completed with skipped prefixes |
| `failed`   | Build failed with error               |

### On-Disk Layout

Job records are stored under the app data directory:

```
<data_dir>/jobs/index-build/<job_id>/
├── job.json      # Job metadata and state
├── stdout.log    # Captured stdout
└── stderr.log    # Captured stderr
```

## Query Filters

Index queries support pattern, metadata, storage-class, and output filters:

| Filter         | Flag               | Example                            |
| -------------- | ------------------ | ---------------------------------- |
| Pattern        | `--pattern`        | `**/data/*.parquet`                |
| Min size       | `--min-size`       | `1KB`, `100MiB`                    |
| Max size       | `--max-size`       | `1GB`                              |
| After date     | `--after`          | `2025-12-01`                       |
| Before date    | `--before`         | `2026-01-01`                       |
| Storage class  | `--storage-class`  | `STANDARD`, `GLACIER,DEEP_ARCHIVE` |
| Enriched after | `--enriched-after` | `2026-05-25T00:00:00Z`             |
| Since run      | `--since-run`      | `run_1783087200000000000`          |
| Count only     | `--count`          | Returns count instead of records   |

`--storage-class` matches the raw provider value captured from LIST responses.
The flag is repeatable and accepts comma-separated values. Matching is exact and
case-sensitive. Objects whose provider did not return a storage class have no
`storage_class` JSONL field and are not matched by `--storage-class`.

## HEAD Enrichment

Use `index enrich-with-head` to cache expensive HEAD-only metadata on an
existing index. The command is **format-aware**: durable-only sets append a new
enrich-only snapshot (OS write lease + parent CAS); sets with `index.db` use the
SQLite in-place path. See [Durable Index Format](durable-index.md).

```bash
gonimbus index enrich-with-head idx_da038d8171b4a9ba \
  --storage-class GLACIER,DEEP_ARCHIVE \
  --pattern "**/*.xml" \
  --parallel 32 \
  --state-out enrich-state.jsonl
```

Supported v1 candidate filters are `--storage-class`, `--pattern`,
`--key-regex`, `--min-size`, `--max-size`, and `--include-deleted`. Filters are
applied before HEAD calls on both substrates, so rows excluded by storage class
or key/size filters do not incur provider HEAD cost.

Provider reconstruction uses index metadata plus runtime inputs such as
`--profile`, `--region`, `--endpoint`, and the normal SDK credential chain.
Credentials are never stored in the index. v1 supports S3-family indexes and
rejects unsupported providers.

The command writes only HEAD-derived fields:

- `archive_status`
- `restore_state`
- `restore_expiry`
- `content_type`
- `head_enriched_at`

It does not overwrite LIST-derived `storage_class`, size, ETag,
`last_modified`, `last_seen_run_id`, or `deleted_at`. `--resume` skips rows
whose `head_enriched_at` is already set. A full run re-HEADs all candidate rows
and overwrites the HEAD-derived fields only on successful HEAD responses.

`--state-out` is audit/debug JSONL for post-filter candidate rows, including
rows skipped by `--resume`. Durable state lives in the index. The command exits
non-zero on any permanent per-object failure, unsupported provider, invalid
filter, provider reconstruction failure, or interruption.

Resumable fatal interruptions use the operation-level run ID, distinct from the
row-skipping `--resume` flag. **`--resume-run` is SQLite-only today** (operation
checkpoint lifecycle). Durable-only sets reject `--resume-run` with a clear
error; use row-level `--resume` to skip already-enriched keys, or re-run after
fixing the failure cause. SQLite path:

```bash
gonimbus index enrich-with-head --resume-run <run_id>   # SQLite / dual with index.db
```

When a failed SQLite enrichment run can be resumed, stdout includes a redacted
`gonimbus.operation.error.v1` record and stderr includes `run_id`, `status`,
`error_class`, progress counters, and the safe resume command. The
`failed-resumable`, `resume_recovered`, `resume_started`, and
`resume_completed` lifecycle is the same as `index build`.

| Canonical Option   | Flag                    | Behavior                                       |
| ------------------ | ----------------------- | ---------------------------------------------- |
| ETag grouping      | `--canonical-by-etag`   | One canonical record per non-empty ETag group  |
| Tie-break          | `--canonical-tie-break` | `min-key`, `min-modified`, or `max-modified`   |
| Include alternates | `--include-alternates`  | Populate alternates for canonical ETag records |

| Output Option   | Flag                | Example                                         |
| --------------- | ------------------- | ----------------------------------------------- |
| Output dest     | `--output`          | `s3://bucket/key.jsonl`, `file:///path/f.jsonl` |
| Output profile  | `--output-profile`  | AWS profile for output destination              |
| Output region   | `--output-region`   | AWS region for output destination               |
| Output endpoint | `--output-endpoint` | Custom S3 endpoint for output destination       |

## Index Identity

Each index is uniquely identified by:

- **base_uri**: The prefix being indexed (e.g., `s3://bucket/data/`)
- **provider_identity**: Provider, region, and endpoint
- **build_params_hash**: Include patterns and configuration
- **scope config**: `build.scope` values

Different scopes produce different indexes:

- `s3://bucket/` is distinct from `s3://bucket/data/`
- Same URI with different include patterns produces different indexes

## Workflow Examples

### Explore First, Then Index

For unfamiliar buckets, explore before deciding to index:

```bash
# Understand structure
gonimbus tree s3://bucket/ --depth 2 --output table

# Spot-check a prefix
gonimbus inspect s3://bucket/data/ --limit 100

# If it's large and you'll query repeatedly, build an index
gonimbus index build --job index-manifest.yaml
```

### Scheduled Index Builds

For operational use, schedule index builds and query on demand:

```bash
# Build (e.g., nightly via cron)
gonimbus index build --job production-index.yaml

# Query anytime (instant)
gonimbus index query 's3://bucket/prod/' --pattern '**/*.json' --count

# Clean up old builds
gonimbus index gc --keep-last 7
```

For repeated builds of long-lived shards, see
[Steady-State Index Operations](steady-state-index-operations.md).

### Multi-Bucket Inventory

Build separate indexes for different buckets or prefixes:

```bash
gonimbus index build --job bucket-a-index.yaml
gonimbus index build --job bucket-b-index.yaml

# List all indexes
gonimbus index list
```

## Enterprise Indexing Workflow

For large-scale deployments (10M+ objects), gonimbus provides a tiered approach to control both **provider API costs** (listing calls) and **index size** (storage/query efficiency).

### The Three-Tier Model

| Tier                    | Mechanism                           | Controls                | When to Use                                        |
| ----------------------- | ----------------------------------- | ----------------------- | -------------------------------------------------- |
| **1. Prefix Sharding**  | `base_uri` + `build.match.includes` | Which prefixes to list  | Always – reduces listing scope                     |
| **2. Ingest Filtering** | `build.match.filters`               | Which objects to store  | When you need a subset (recent, large files, etc.) |
| **3. Path Scoping**     | `build.scope`                       | Prefix plan compilation | Date-partitioned data, deep path structures        |

### Tier 1: Prefix Sharding (Reduce Listing Scope)

Include patterns with a concrete prefix avoid full-bucket enumeration:

```yaml
build:
  match:
    includes:
      - "store-001/**" # Lists only s3://bucket/store-001/
      - "store-002/**" # Lists only s3://bucket/store-002/
```

The crawler derives the strongest possible list prefix from each pattern. Use `--dry-run` to see the derived prefixes before building.

### Tier 2: Ingest Filtering (Reduce Index Size)

Filters apply **after** listing but **before** storage – you still pay the list cost, but the index stays small:

```yaml
build:
  match:
    includes:
      - "**/*"
    filters:
      modified:
        after: "2025-12-01" # Only recent objects
      size:
        min: 1KB # Skip tiny files
```

### Tier 3: Path Scoping (Reduce Provider Costs)

For date-partitioned layouts where the date is deep in the path, path scoping compiles a prefix plan without listing everything. See [Scoping with build.scope](#scoping-with-buildscope) above.

**Impact**: In testing with date-partitioned enterprise data:

| Metric         | Without Scope | With Scope | Improvement     |
| -------------- | ------------- | ---------- | --------------- |
| Objects found  | 16M           | 78K        | **99.5% less**  |
| Build time     | ~3 min        | ~20 sec    | **~10x faster** |
| Wasted listing | 99%           | 0%         | Eliminated      |

The key insight: with scope, `objects_found ≈ objects_matched` because you only list what you need.

### Verifying Your Strategy

Before running a large build, validate the plan:

```bash
# Preview what prefixes will be listed
gonimbus index build --job index-manifest.yaml --dry-run

# Output shows:
# - Derived crawl prefixes (or scope plan)
# - Prefix count
# - Any warnings about broad patterns
```

## Discoverability and Debugging

### Previewing Builds (`--dry-run`)

The `--dry-run` flag validates the manifest and shows the crawl plan without executing:

```bash
gonimbus index build --job index-manifest.yaml --dry-run
```

This displays:

- Manifest validation status
- Derived crawl prefixes (or scope plan if `build.scope` is set)
- Identity hash that will be used
- Any configuration warnings

### Inspecting Indexes (`index doctor` / `index show`)

`index doctor` (aliased as `index show`) maps index directories to human-readable identities:

```bash
# Summary of all local indexes
gonimbus index doctor

# Summary for a specific local index by ID or prefix
gonimbus index doctor idx_1234abcd

# Detailed JSON for a specific index directory
gonimbus index doctor /path/to/indexes/idx_1234abcd/ --detail

# Include object counts (expensive on large indexes)
gonimbus index doctor --stats
```

The `--detail` flag shows:

- Full identity payload (base URI, provider, region, endpoint)
- Original manifest configuration (if preserved)
- Build parameters hash
- Run history summary

### Understanding Index Directories

Index directories use a hash-based naming scheme:

```
~/.local/share/gonimbus/indexes/
├── idx_1234abcd5678ef90/
│   ├── index.db          # SQLite database
│   └── identity.json     # Human-readable identity
└── idx_9876fedc5432ba10/
    ├── index.db
    └── identity.json
```

Use `index doctor` to decode which `idx_*` directory corresponds to which bucket/prefix.

## Performance

### Query Performance

| Operation          | Live Crawl | Index Query | Improvement |
| ------------------ | ---------- | ----------- | ----------- |
| Count 100K objects | ~30s       | <1s         | 100x        |
| Pattern query      | minutes    | <1s         | 100-1000x   |

### Build Throughput

| Mode           | Throughput       | Notes                            |
| -------------- | ---------------- | -------------------------------- |
| Unscoped build | ~90K objects/sec | Listing throughput (all objects) |
| Scoped build   | N/A              | Only lists what's needed         |
| Ingest rate    | ~3K objects/sec  | Writing to index DB              |

### Tested Scale

- 32M objects enumerated in ~3 minutes (unscoped)
- 150K-350K objects indexed per build (with filters)
- 10x build time reduction with `build.scope` on date-partitioned data

## Troubleshooting

### "Index not found"

The base URI must match exactly. Use `index list` to see available indexes.

### Build takes too long

Use include patterns to scope the build:

```yaml
build:
  match:
    includes:
      - "recent-data/**"
```

### Query returns no results

Check that the pattern matches relative to the base URI:

- Base URI: `s3://bucket/data/`
- Pattern matches keys like `reports/file.json` (not `data/reports/file.json`)

### Stale index

Rebuild to capture recent changes:

```bash
gonimbus index build --job index-manifest.yaml
```

## Index Hub

For team and production use, indexes can be published to a shared hub (S3 or local filesystem) and hydrated on demand.
v0.4.0 hubs are format-aware: runs carry a `sqlite-v1` or `durable-v2` marker.
Operator detail: [Durable Index Format — Hub export and hydrate](durable-index.md#hub-export-and-hydrate).

```bash
# Export to hub (default --format auto: durable if local durable snapshot exists, else sqlite)
gonimbus index export --hub s3://bucket/index-hub/ --index-set idx_da038d...

# Force durable or sqlite export
gonimbus index export --hub s3://bucket/index-hub/ \
  --index-set idx_da038d... --format durable
gonimbus index export --hub s3://bucket/index-hub/ \
  --index-set idx_da038d... --format sqlite

# Hydrate from hub
gonimbus index hydrate --hub s3://bucket/index-hub/ \
  --index-set idx_da038d8171b4a9ba... --dest /tmp/hydrated/

# Browse hub contents
gonimbus index hub ls --hub s3://bucket/index-hub/
gonimbus index hub show --hub s3://bucket/index-hub/ --index-set idx_da038d...

# Maintenance
gonimbus index hub gc --hub s3://bucket/index-hub/ --keep 3
gonimbus index hub set-latest --hub s3://bucket/index-hub/ \
  --index-set idx_da038d... --run-id run_1709654400000000000
```

`index export` and `index hub set-latest` update `latest.json` with
conditional compare-and-swap by default. If another writer advances the pointer
first, gonimbus re-reads the current pointer and either retries, yields to the
newer run, or fails closed with manual reconciliation guidance. Use
`--latest-write-mode unconditional` only for explicit recovery after you have
verified hub state.

Hub runs carry an explicit format marker. `index export --format auto` (default)
selects `durable-v2` when a local durable complete marker is present for the
target run, otherwise `sqlite-v1`. Explicit `--format durable` resolves from the
local durable snapshot (complete marker + segments) and does not require
`index.db`. Explicit `--format sqlite` publishes `index.db` and optional
`identity.json` as before. `index hydrate` reads the marker, rejects unknown
formats, and for durable runs verifies the manifest and every referenced segment
by digest before writing them under the destination directory. A durable hydrate
does not create an `index.db`; downstream commands must explicitly support
durable manifests before using that hydrated output.

`index hub ls` and `index hub show` display hub run formats so mixed
`sqlite-v1` / `durable-v2` hubs are legible. JSON output includes format counts
at the index-set level and per-run artifact summaries. `index hub gc --dry-run
--json` includes the same per-run format and artifact summary for removal
candidates, so operators can see when retention would remove a durable manifest
and its segment set rather than a single SQLite database artifact.

Durable hub artifacts are a full-fidelity **internal render** for trusted
operators and pipelines — not a reduced-trust third-party share format. See
[boundary framing](durable-index.md#internal-render-framing-mandatory).

### Large Hub Exports

There are two different scale stories depending on format:

**Durable (default).** A durable export publishes multiple segment objects plus
a small manifest and markers. The largest individual PUT is a segment (typically
tens of megabytes under default packing), which is how durable clears the
historical single-object PUT wall that multi-gigabyte SQLite inventories hit.

**SQLite compatibility.** Large `index.db` files can still exceed a provider's
single-object PUT limit. For S3-compatible hubs, `index export` automatically
uses multipart upload through the shared transfer uploader when an artifact
crosses the default multipart threshold (64 MiB). The default part size is 8 MiB;
very large known-size artifacts increase part size automatically when needed to
stay within provider part-count limits.

In both cases the export still follows the same commit order: upload immutable
artifacts, write `complete.json`, then advance `latest.json`.

Plan local disk with these facts in mind:

- For **sqlite** export, the local `index.db` already exists before upload and
  remains the source file. Multipart does not require a second full-size copy of
  that database just to cross the PUT wall.
- For **durable** export, segments already exist under the segment cache before
  hub upload; export does not re-pack the inventory.
- Small commit-marker files may use temporary files, and provider/retry paths may
  use system temp space. Keep the system temp directory outside the repository
  working tree and leave headroom for normal OS and checkpoint activity.

Provider cleanup still matters for multipart paths. Gonimbus aborts multipart
uploads on failure paths it controls, but a killed process, host failure, or
provider-side partial state can leave incomplete uploads behind. Configure the
destination bucket's lifecycle cleanup for incomplete multipart uploads, such as
an S3 `AbortIncompleteMultipartUpload` lifecycle rule, before relying on repeated
large SQLite exports.

Multipart ETags are provider-specific and should not be treated as universal
content hashes. Trust the hub's `complete.json` artifact sizes and SHA-256 values
for export integrity.

See [Workspace Pattern](workspace.md) for production workspace layout, shard strategies, and operational flows.
