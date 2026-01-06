# Transfer Operations

The `gonimbus transfer` command copies or moves objects between S3-compatible buckets with optional path transformation.

## Quick Start

```bash
# Validate manifest syntax
gonimbus transfer --job transfer.yaml --plan

# Run preflight checks without transferring
gonimbus transfer --job transfer.yaml --dry-run

# Execute transfer
gonimbus transfer --job transfer.yaml
```

## Transfer Manifest

Transfer operations are defined in YAML manifests:

```yaml
version: "1.0"

source:
  provider: s3
  bucket: source-bucket
  region: us-east-1

target:
  provider: s3
  bucket: target-bucket
  region: us-west-2

match:
  includes:
    - "data/**/*.parquet"
  excludes:
    - "**/_temporary/**"

transfer:
  mode: copy # copy or move
  concurrency: 16 # parallel workers
  on_exists: skip # skip, overwrite, or fail
  path_template: "" # optional path transformation
  dedup:
    enabled: true
    strategy: etag # etag, key, or none
  preflight:
    mode: write-probe # plan-only, read-safe, or write-probe
    probe_strategy: multipart-abort

output:
  destination: stdout # or file:/path/to/output.jsonl
```

## Transfer Modes

| Mode   | Description                                       |
| ------ | ------------------------------------------------- |
| `copy` | Copy objects, keep source intact (default)        |
| `move` | Copy objects, delete source after successful copy |

## On-Exists Behavior

| Setting     | Description                     |
| ----------- | ------------------------------- |
| `skip`      | Skip if target exists (default) |
| `overwrite` | Replace existing target objects |
| `fail`      | Error if target exists          |

## Deduplication

When `on_exists: skip`, deduplication prevents redundant transfers:

| Strategy | Comparison    | Use Case                         |
| -------- | ------------- | -------------------------------- |
| `etag`   | Compare ETags | Same content detection (default) |
| `key`    | Key exists    | Skip if target key exists        |
| `none`   | Disabled      | Always transfer                  |

**Note:** ETag comparison may not work reliably for multipart uploads or server-side encrypted objects.

## Path Transformation

The `path_template` field transforms source keys to target keys during transfer.

### Supported Placeholders

| Placeholder  | Description             | Example Input               | Example Output |
| ------------ | ----------------------- | --------------------------- | -------------- |
| `{filename}` | Final path segment      | `a/b/c.txt`                 | `c.txt`        |
| `{dir[n]}`   | Nth directory (0-based) | `a/b/c.txt` with `{dir[0]}` | `a`            |
| `{key}`      | Full source key         | `a/b/c.txt`                 | `a/b/c.txt`    |

### Examples

**Flatten structure:**

```yaml
# Source: region/store/device/2024/01/file.json
# Target: file.json
transfer:
  path_template: "{filename}"
```

**Date-first reorganization:**

```yaml
# Source: region/store/device/2024/01/file.json
# Target: 2024/01/region/file.json
transfer:
  path_template: "{dir[3]}/{dir[4]}/{dir[0]}/{filename}"
```

**Add archive prefix:**

```yaml
# Source: data/file.json
# Target: archive/2024/data/file.json
transfer:
  path_template: "archive/2024/{key}"
```

**Remove middle layers:**

```yaml
# Source: vendor/raw/unprocessed/data/file.json
# Target: data/file.json
transfer:
  path_template: "{dir[3]}/{filename}"
```

### Validation

Templates are validated before any transfers occur. Invalid templates (unsupported placeholders, out-of-range directory indices) cause immediate failure with a clear error message.

## Preflight Checks

Transfer operations run preflight checks before enumeration to fail fast on permission issues.

### Preflight Modes

| Mode          | Provider Calls      | Use Case                      |
| ------------- | ------------------- | ----------------------------- |
| `plan-only`   | None                | Validate manifest syntax only |
| `read-safe`   | List, Head, Get     | Validate read permissions     |
| `write-probe` | Above + write probe | Validate write permissions    |

### Capability Checks

Preflight validates these capabilities in fail-fast order:

1. **target.write** (write-probe mode only): Can we write to the target?
2. **source.list**: Can we list the source prefix?
3. **source.read**: Can we read objects from source?
4. **target.head**: Can we check target existence? (when `on_exists != overwrite`)

### Write Probe Strategies

| Strategy          | Operation                       | Side Effects       |
| ----------------- | ------------------------------- | ------------------ |
| `multipart-abort` | Create + abort multipart upload | None (preferred)   |
| `put-delete`      | Put 0-byte object + delete      | Minimal (fallback) |

## Output Records

Transfer emits JSONL records for each operation:

### Transfer Record

```json
{
  "type": "gonimbus.transfer.v1",
  "ts": "2024-01-15T10:00:00Z",
  "job_id": "abc123",
  "provider": "s3",
  "data": {
    "source_key": "data/input/file.json",
    "target_key": "archive/2024/file.json",
    "bytes": 1048576
  }
}
```

### Skip Record

```json
{
  "type": "gonimbus.skip.v1",
  "ts": "2024-01-15T10:00:01Z",
  "job_id": "abc123",
  "provider": "s3",
  "data": {
    "source_key": "data/input/existing.json",
    "target_key": "archive/2024/existing.json",
    "reason": "dedup.etag"
  }
}
```

### Summary Record

```json
{
  "type": "gonimbus.summary.v1",
  "ts": "2024-01-15T10:01:00Z",
  "job_id": "abc123",
  "provider": "s3",
  "data": {
    "objects_found": 1000,
    "objects_matched": 850,
    "bytes_total": 1073741824,
    "duration_ns": 60000000000,
    "duration": "1m0s",
    "errors": 2
  }
}
```

## Cross-Account Transfers

For transfers between different AWS accounts, use separate credentials:

```yaml
source:
  provider: s3
  bucket: source-bucket
  profile: source-account # Uses ~/.aws/credentials [source-account]

target:
  provider: s3
  bucket: target-bucket
  profile: target-account # Uses ~/.aws/credentials [target-account]
```

Or use environment variables with role assumption.

## S3-Compatible Storage

Transfer works with any S3-compatible storage:

```yaml
# Wasabi to AWS S3
source:
  provider: s3
  bucket: wasabi-bucket
  endpoint: https://s3.us-east-2.wasabisys.com
  region: us-east-2

target:
  provider: s3
  bucket: aws-bucket
  region: us-east-1

transfer:
  preflight:
    mode: write-probe
    probe_strategy: put-delete # Use fallback for Wasabi
```

## Best Practices

1. **Always run preflight first**: Use `--dry-run` to validate permissions before large transfers
2. **Use write-probe for production**: Validates write permissions before wasting time on enumeration
3. **Start with small batches**: Test path_template with a narrow include pattern first
4. **Monitor with JSONL output**: Pipe output to analysis tools for large transfers
5. **Use dedup for reruns**: `on_exists: skip` with `strategy: etag` prevents redundant copies

## See Also

- [Storage Provider Configuration](../appnotes/storage-providers.md)
- [Preflight Permission Probes](../appnotes/storage-providers.md#preflight-permission-probes)
