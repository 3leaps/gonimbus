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

## Performance

| Operation          | Live Crawl | Index Query | Improvement |
| ------------------ | ---------- | ----------- | ----------- |
| Count 100K objects | ~30s       | <1s         | 100x        |
| Pattern query      | minutes    | <1s         | 100-1000x   |

Build throughput scales linearly at ~3,000 objects/sec.

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
