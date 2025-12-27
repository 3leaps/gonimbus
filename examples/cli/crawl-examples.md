# gonimbus crawl Examples

The `crawl` command runs structured crawl jobs defined in YAML manifests.

## Basic Usage

```bash
# Run a crawl job
gonimbus crawl --job manifest.yaml

# Dry-run (validate without executing)
gonimbus crawl --job manifest.yaml --dry-run

# Suppress progress records
gonimbus crawl --job manifest.yaml --quiet

# Override output destination
gonimbus crawl --job manifest.yaml --output results.jsonl
```

## Dry-Run Output

```bash
$ gonimbus crawl --job crawl.yaml --dry-run

=== Crawl Plan (dry-run) ===

Provider:    s3
Bucket:      my-data-bucket
Region:      us-east-1

Patterns:
  Include:
    - **/*.parquet
  Exclude:
    - **/_temporary/**

Concurrency: 8
Output:      stdout
Progress:    true

Manifest validated successfully. Remove --dry-run to execute.
```

## Output Processing

```bash
# Save to file
gonimbus crawl --job manifest.yaml > results.jsonl

# Filter object records only
gonimbus crawl --job manifest.yaml | grep '"type":"gonimbus.object.v1"'

# Count matched objects
gonimbus crawl --job manifest.yaml --quiet | grep object.v1 | wc -l

# Extract keys only
gonimbus crawl --job manifest.yaml --quiet | \
    jq -r 'select(.type == "gonimbus.object.v1") | .data.key'

# Calculate total size
gonimbus crawl --job manifest.yaml --quiet | \
    jq -s '[.[] | select(.type == "gonimbus.object.v1") | .data.size] | add'
```

## Pipeline Examples

```bash
# Index to DuckDB (future feature)
gonimbus crawl --job manifest.yaml | gonimbus index --db inventory.duckdb

# Stream to S3 (using aws cli)
gonimbus crawl --job manifest.yaml | \
    aws s3 cp - s3://results-bucket/crawl-$(date +%Y%m%d).jsonl

# Parallel processing with GNU parallel
gonimbus crawl --job manifest.yaml --quiet | \
    jq -r 'select(.type == "gonimbus.object.v1") | .data.key' | \
    parallel -j 10 'aws s3 cp s3://bucket/{} /local/backup/{}'
```

## Manifest Validation

Common validation errors:

```bash
# Wrong version
Error: /version: value must be "1.0"

# Wrong field names
Error: /match: missing properties: 'includes'
Error: /match: additionalProperties 'include' not allowed

# Use 'includes' not 'include'
# Use 'excludes' not 'exclude'  
# Use 'include_hidden' not 'includeHidden'
```

## Performance Tuning

```yaml
# High-throughput crawl
crawl:
  concurrency: 20      # More parallel list operations
  progress_every: 1000 # Less frequent progress updates

# Rate-limited crawl
crawl:
  concurrency: 4
  rate_limit: 100      # Max 100 requests/second
```
