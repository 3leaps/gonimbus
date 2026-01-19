# Local Index

The local index enables fast repeated queries against large buckets without re-enumerating via the provider API.

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
# Initialize the index database (one-time)
gonimbus index init

# Create an index manifest (see below)
# Then build the index
gonimbus index build --job index-manifest.yaml

# Query the index
gonimbus index query 's3://my-bucket/data/' --pattern '**/report-*.xml' --count
```

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

## Commands

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

### `index list`

List all local indexes.

```bash
gonimbus index list
```

Output shows base URI, provider, object count, size, and status.

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

# Count only (no output records)
gonimbus index query 's3://bucket/prefix/' --pattern '**/*.json' --count
```

### `index stats`

View detailed statistics for an index.

```bash
gonimbus index stats 's3://bucket/prefix/'
```

### `index doctor`

Validate index integrity and identity.

```bash
# Summary view
gonimbus index doctor

# Detailed diagnostics
gonimbus index doctor --detail
```

### `index show`

Display manifest provenance for an index.

```bash
gonimbus index show 's3://bucket/prefix/'
```

### `index gc`

Clean up old indexes.

```bash
# Keep only the last 3 runs per index
gonimbus index gc --keep-last 3

# Remove indexes older than 30 days
gonimbus index gc --max-age 30d

# Preview what would be removed
gonimbus index gc --keep-last 1 --dry-run
```

## Job Management

For long-running builds (hours on large buckets), gonimbus provides managed job execution with durable state and background operation.

### Starting Background Builds

```bash
# Start a managed background build (returns job id immediately)
gonimbus index build --background --job index-manifest.yaml

# With a human-friendly name
gonimbus index build --background --job index-manifest.yaml --name nightly-sweep

# Prevent duplicate running jobs for the same manifest
gonimbus index build --background --job index-manifest.yaml --dedupe
```

The `--background` flag spawns a managed child process and returns immediately with a job ID.

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

Index queries support the same filters as `inspect`:

| Filter      | Flag         | Example                          |
| ----------- | ------------ | -------------------------------- |
| Pattern     | `--pattern`  | `**/data/*.parquet`              |
| Min size    | `--min-size` | `1KB`, `100MiB`                  |
| Max size    | `--max-size` | `1GB`                            |
| After date  | `--after`    | `2025-12-01`                     |
| Before date | `--before`   | `2026-01-01`                     |
| Count only  | `--count`    | Returns count instead of records |

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

# Detailed JSON for a specific index
gonimbus index doctor --db ~/.local/share/gonimbus/indexes/idx_1234abcd/ --detail

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
