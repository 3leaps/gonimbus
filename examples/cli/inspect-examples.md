# gonimbus inspect Examples

The `inspect` command provides quick, ad-hoc inspection of cloud storage without a manifest file.

## Basic Usage

```bash
# List objects in a bucket prefix
gonimbus inspect s3://my-bucket/data/

# Limit results
gonimbus inspect s3://my-bucket/data/ --limit 10

# JSON output (JSONL format)
gonimbus inspect s3://my-bucket/data/ --json
```

## Pattern Matching

```bash
# All parquet files (recursive)
gonimbus inspect 's3://my-bucket/**/*.parquet'

# Files in specific year
gonimbus inspect 's3://my-bucket/data/2024/**'

# Single directory level (no recursion)
gonimbus inspect 's3://my-bucket/data/*'

# Multiple levels with wildcard
gonimbus inspect 's3://my-bucket/data/*/01/*.parquet'
```

**Note:** Quote patterns containing `*` to prevent shell expansion.

## S3-Compatible Providers

```bash
# Wasabi
gonimbus inspect s3://my-bucket/ \
    --endpoint https://s3.us-east-2.wasabisys.com \
    --region us-east-2

# Cloudflare R2
gonimbus inspect s3://my-bucket/ \
    --endpoint https://ACCOUNT_ID.r2.cloudflarestorage.com \
    --region auto

# DigitalOcean Spaces
gonimbus inspect s3://my-bucket/ \
    --endpoint https://nyc3.digitaloceanspaces.com \
    --region nyc3
```

## AWS Profiles

```bash
# Use specific AWS profile
gonimbus inspect s3://my-bucket/ --profile production

# With pattern
gonimbus inspect 's3://my-bucket/**/*.json' --profile staging
```

## Output Formats

### Table (default)
```
KEY                          SIZE  MODIFIED
data/2024/01/sales.parquet   1 MB  2025-01-14 08:00:00
data/2024/01/inventory.parquet 2 MB  2025-01-14 09:30:00

Found 2 object(s) (3 MB total)
```

### JSON (--json)
```jsonl
{"key":"data/2024/01/sales.parquet","size":1048576,"last_modified":"2025-01-14T08:00:00Z","etag":"..."}
{"key":"data/2024/01/inventory.parquet","size":2097152,"last_modified":"2025-01-14T09:30:00Z","etag":"..."}
```

## Common Patterns

```bash
# Count objects matching pattern
gonimbus inspect 's3://bucket/**/*.parquet' --json | wc -l

# Find largest files
gonimbus inspect s3://bucket/ --json | jq -s 'sort_by(.size) | reverse | .[0:10]'

# List all file extensions
gonimbus inspect s3://bucket/ --json | jq -r '.key | split(".") | .[-1]' | sort | uniq -c
```
