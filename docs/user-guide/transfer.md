# Transfer Operations

The `gonimbus transfer` command copies or moves objects between S3-compatible buckets with optional path transformation.

## Quick Start

```bash
# Validate manifest syntax
gonimbus transfer --job transfer.yaml --plan

# Safety latch (recommended when dogfooding): disable all provider-side mutations
# This blocks both transfers and write-probe preflight.
export GONIMBUS_READONLY=1

# Run preflight checks without transferring
gonimbus transfer --job transfer.yaml --dry-run

# Execute transfer (requires readonly disabled)
unset GONIMBUS_READONLY
# or: gonimbus transfer --job transfer.yaml --readonly=false

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

**Readonly safety latch:** If `--readonly` (or `GONIMBUS_READONLY=1`) is set, Gonimbus refuses `write-probe` preflight and refuses to execute transfers.

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

## Transfer Reflow

The `transfer reflow` command reorganizes objects by rewriting keys based on templates. Unlike manifest-based transfers, reflow operates directly from URIs or probe output, making it ideal for:

- **Reorganizing misfiled data** - files placed by arrival date but need organization by business date
- **Structural migrations** - flattening hierarchies or adding prefix layers
- **Content-aware routing** - using fields extracted from file content to determine destination paths

### Quick Start

```bash
# Path-based reflow (extract variables from path segments)
gonimbus transfer reflow 's3://source/data/' \
  --dest 's3://dest/reorganized/' \
  --rewrite-from '{region}/{store}/{date}/{file}' \
  --rewrite-to '{date}/{region}/{store}/{file}' \
  --dry-run

# Content-aware reflow (using probe-derived variables)
gonimbus content probe --stdin --config probe.yaml < uris.txt | \
  gonimbus transfer reflow --stdin \
    --dest 's3://dest/by-business-date/' \
    --rewrite-from '{_}/{store}/{device}/{folder_date}/{file}' \
    --rewrite-to '{business_date}/{store}/{file}'
```

### Template Variables

Templates use `{variable}` placeholders that are extracted from source paths or probe output:

| Variable    | Source                     | Example                           |
| ----------- | -------------------------- | --------------------------------- |
| `{segment}` | Path segment by name       | `{store}`, `{date}`, `{file}`     |
| `{_}`       | Ignored segment (wildcard) | Matches any segment, not captured |
| Probe vars  | From `content probe`       | `{business_date}`, `{version}`    |

### Path-Based Reflow

Extract variables directly from source path structure:

```bash
# Source: data/us-east/store-123/2024-01-15/receipt.xml
# Template: data/{region}/{store}/{date}/{file}
# Variables: region=us-east, store=store-123, date=2024-01-15, file=receipt.xml

gonimbus transfer reflow 's3://bucket/data/' \
  --dest 's3://bucket/reorganized/' \
  --rewrite-from 'data/{region}/{store}/{date}/{file}' \
  --rewrite-to '{date}/{region}/{store}/{file}' \
  --dry-run
```

### Content-Aware Reflow

When the destination path depends on content inside files (not just the path), use `content probe` to extract variables first:

```bash
# 1. Create probe config
cat > probe.yaml << 'EOF'
extract:
  - name: business_date
    type: xml_xpath
    xpath: //BusinessDate
EOF

# 2. List and probe objects
gonimbus inspect 's3://source/prefix/' --json | \
  jq -r '"s3://source/" + .key' | \
  gonimbus content probe --stdin --config probe.yaml --emit reflow-input \
  > probe-output.jsonl

# 3. Reflow using extracted business_date
gonimbus transfer reflow --stdin \
  --dest 's3://dest/by-date/' \
  --rewrite-from 'prefix/{store}/{device}/{folder_date}/{file}' \
  --rewrite-to '{business_date}/{store}/{file}' \
  < probe-output.jsonl
```

### Destination Providers

Reflow supports multiple destination types:

| Destination | Example                            | Use Case                 |
| ----------- | ---------------------------------- | ------------------------ |
| S3          | `s3://bucket/prefix/`              | Cloud-to-cloud migration |
| S3-compat   | `s3://bucket/` + `--dest-endpoint` | Wasabi, R2, MinIO        |
| Local       | `file:///tmp/output/`              | Download and reorganize  |

### Collision Handling

Control behavior when destination objects already exist:

| Option                         | Behavior                              |
| ------------------------------ | ------------------------------------- |
| `--on-collision log` (default) | Log conflict, fail operation          |
| `--on-collision fail`          | Fail immediately on first conflict    |
| `--on-collision overwrite`     | Replace (requires `--overwrite` flag) |

### Checkpoint and Resume

For large reflow jobs, use checkpointing to enable resume after interruption:

```bash
# Start with checkpoint
gonimbus transfer reflow 's3://source/' \
  --dest 's3://dest/' \
  --rewrite-from '...' \
  --rewrite-to '...' \
  --checkpoint /tmp/reflow-state.db

# Resume after interruption
gonimbus transfer reflow 's3://source/' \
  --dest 's3://dest/' \
  --checkpoint /tmp/reflow-state.db \
  --resume
```

### Dry Run

Always preview before executing:

```bash
gonimbus transfer reflow 's3://source/' \
  --dest 's3://dest/' \
  --rewrite-from '{region}/{date}/{file}' \
  --rewrite-to '{date}/{region}/{file}' \
  --dry-run
```

Dry run output shows planned mappings without writing:

```json
{
  "type": "gonimbus.reflow.v1",
  "data": {
    "source_uri": "s3://source/us-east/2024-01-15/file.xml",
    "dest_uri": "s3://dest/2024-01-15/us-east/file.xml",
    "status": "planned"
  }
}
```

## See Also

- [Content Access (streaming.md)](streaming.md) - `content probe` for derived field extraction
- [Storage Provider Configuration](../appnotes/storage-providers.md)
- [Preflight Permission Probes](../appnotes/storage-providers.md#preflight-permission-probes)
