# Gonimbus Examples

This directory contains example manifests, sample outputs, and CLI usage patterns.

## Contents

### manifests/

Example job manifests for common use cases. These are templates - replace placeholder values with your actual configuration.

| File | Description |
|------|-------------|
| `basic-crawl.yaml` | Minimal crawl configuration |
| `parquet-filter.yaml` | Filter by file extension |
| `multi-pattern.yaml` | Multiple include/exclude patterns |
| `s3-compatible.yaml` | S3-compatible providers (Wasabi, R2, Spaces) |

### output-samples/

Example JSONL output records showing the structure of each record type.

| File | Description |
|------|-------------|
| `object-records.jsonl` | Object metadata records |
| `progress-records.jsonl` | Progress update records |
| `summary-record.jsonl` | Final summary record |

### cli/

Command-line usage examples and patterns.

| File | Description |
|------|-------------|
| `inspect-examples.md` | `gonimbus inspect` usage patterns |
| `crawl-examples.md` | `gonimbus crawl` usage patterns |

## Quick Start

```bash
# Inspect a bucket prefix
gonimbus inspect s3://my-bucket/data/ --limit 10

# Inspect with pattern matching
gonimbus inspect 's3://my-bucket/data/**/*.parquet'

# Run a crawl job
gonimbus crawl --job examples/manifests/basic-crawl.yaml

# Dry-run to validate manifest
gonimbus crawl --job examples/manifests/basic-crawl.yaml --dry-run
```

## Provider Configuration

All examples use environment variables or AWS CLI profiles for credentials. Never commit credentials to manifests.

### AWS S3
```bash
# Uses default credential chain (env vars, ~/.aws/credentials, IAM role)
gonimbus inspect s3://my-bucket/
```

### S3-Compatible (Wasabi, R2, Spaces)
```bash
export AWS_ACCESS_KEY_ID="your-key"
export AWS_SECRET_ACCESS_KEY="your-secret"
gonimbus inspect s3://my-bucket/ \
    --endpoint https://s3.us-east-2.wasabisys.com \
    --region us-east-2
```

## Output Format

All crawl output is JSONL (JSON Lines) format. Each line is a complete JSON object:

```jsonl
{"type":"gonimbus.object.v1","ts":"...","job_id":"...","data":{...}}
{"type":"gonimbus.progress.v1","ts":"...","job_id":"...","data":{...}}
{"type":"gonimbus.summary.v1","ts":"...","job_id":"...","data":{...}}
```

See `output-samples/` for complete examples of each record type.
