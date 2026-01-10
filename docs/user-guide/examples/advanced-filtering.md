# Advanced Filtering (Size, Date, Key Regex)

Advanced filtering lets you refine results beyond glob matching.

Filters are applied with **AND semantics**:

1. Gonimbus lists by prefix (derived from the glob when possible)
2. The **glob matcher** is applied to each key
3. Optional **metadata filters** (size/date) and **key regex** are applied

## Semantics

- **Size**
  - Raw bytes: `1024`
  - Base-10 (SI): `1KB`, `100MB`, `1GB` (1KB = 1000 bytes)
  - Base-2 (IEC): `1KiB`, `100MiB`, `1GiB` (1KiB = 1024 bytes)
  - Bounds are **inclusive** (`min <= size <= max`)

- **Date** (`--after` / `--before`)
  - ISO 8601 date: `2024-01-15` (interpreted as start of day UTC)
  - ISO 8601 datetime: `2024-01-15T10:30:00Z` (or with offsets)
  - Bounds are **exclusive**:
    - `after`: requires `LastModified > after`
    - `before`: requires `LastModified < before`

- **Key regex** (`--key-regex` / `filters.key_regex`)
  - Standard Go `regexp` syntax
  - Compiled early (invalid regex fails fast)
  - Applied to the full object key after glob matching

## CLI (inspect) Examples

### S3-compatible endpoint (Wasabi/MinIO)

```bash
# Baseline listing
# Note: set --limit high enough to see all matches in a prefix.

gonimbus inspect s3://my-bucket/prefix/ \
  --endpoint https://s3.us-east-2.wasabisys.com \
  --region us-east-2 \
  --limit 10000
```

### Skip zero-byte marker files

```bash
gonimbus inspect s3://my-bucket/prefix/ --min-size 1
```

### Size range

```bash
gonimbus inspect s3://my-bucket/prefix/ \
  --min-size 1KB \
  --max-size 100KB
```

### Date range

```bash
gonimbus inspect s3://my-bucket/prefix/ \
  --after 2024-01-01 \
  --before 2024-06-30
```

### Regex (keys)

```bash
# JSON files only

gonimbus inspect s3://my-bucket/prefix/ --key-regex '\\.json$'

# Transaction-like keys

gonimbus inspect s3://my-bucket/prefix/ --key-regex 'TXN-\\d{8}'
```

### Glob + filters

Use a glob to narrow enumeration to a specific subtree, then filters to refine:

```bash
gonimbus inspect 's3://my-bucket/archives/**/*.json' \
  --min-size 50KB \
  --limit 10000
```

## Manifest (crawl) Examples

Filters can also be configured in a crawl manifest under `match.filters`.

```yaml
version: "1.0"
connection:
  provider: s3
  bucket: my-bucket
  region: us-east-2
  # endpoint: https://s3.us-east-2.wasabisys.com
  # profile: my-sso-profile
match:
  includes:
    - "fixtures/v0.1.2/tier-b/**"
  filters:
    size:
      min: 1KB
      max: 100MB
    modified:
      after: 2026-01-01
      before: 2026-02-01
    key_regex: "\\.parquet$"
crawl:
  concurrency: 8
output:
  destination: stdout
```

Run it:

```bash
gonimbus crawl --job my-filtered-crawl.yaml
```

## Performance Guidance

- Prefer **prefix-first include patterns** (e.g. `data/2026/**`) rather than `**` at bucket root.
- Remember: filters refine _after listing_. If your include patterns force full-bucket listing, filters cannot avoid that provider work.
- Start with `gonimbus crawl --job ... --dry-run` to validate the manifest and see the intended scope.

## Troubleshooting

- Invalid regex: fix the pattern (Go `regexp` syntax), then rerun.
- Invalid date: ensure ISO 8601 (`YYYY-MM-DD` or full RFC3339 timestamp).
- Invalid sizes: check units and ordering (`min` must be <= `max`).

## Not Yet Supported

- `content_type` filtering requires an enrichment/HEAD stage and is not implemented yet.
