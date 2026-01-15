# Release Notes

This file contains release notes for the three most recent releases in reverse chronological order. For the complete release history, see the [CHANGELOG](CHANGELOG.md) or the [docs/releases/](docs/releases/) directory.

---

## v0.1.3 (2026-01-15)

**Local Index for Large-Scale Bucket Inventory**

This release adds a local index store for offline bucket inventory. For buckets with millions of objects, the index enables fast repeated queries without re-enumerating via the provider API.

### When to Use the Index

Gonimbus supports two workflows:

| Workflow      | Scale       | Commands                     | Index? |
| ------------- | ----------- | ---------------------------- | ------ |
| **Explore**   | <1M objects | `tree`, `inspect`, `crawl`   | No     |
| **Inventory** | 1M+ objects | `index build`, `index query` | Yes    |

For smaller buckets, `tree` and `inspect` work well for exploration. For larger buckets where live enumeration takes minutes or hours, build an index once and query it repeatedly.

### Index Commands

```bash
# Initialize local index database
gonimbus index init

# Build index from a crawl manifest
gonimbus index build --job index-manifest.yaml

# List local indexes
gonimbus index list

# Query indexed objects by pattern
gonimbus index query 's3://bucket/prefix/' --pattern '**/data/*.parquet'

# Query with filters
gonimbus index query 's3://bucket/prefix/' --after 2025-12-01 --min-size 1KB --count

# View index statistics
gonimbus index stats 's3://bucket/prefix/'

# Validate index integrity
gonimbus index doctor

# Clean up old indexes
gonimbus index gc --keep-last 3
```

### Index Manifest Example

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

### Performance

Index queries are significantly faster than live crawl for repeated access:

| Operation        | Live Crawl | Index Query |
| ---------------- | ---------- | ----------- |
| Count objects    | ~30s       | <1s         |
| Pattern + filter | minutes    | <1s         |

Build throughput scales linearly at approximately 3,000 objects/sec.

### Documentation

- Index commands: `gonimbus index --help`
- See [docs/releases/v0.1.3.md](docs/releases/v0.1.3.md) for complete release notes

---

## v0.1.2 (2026-01-11)

**Transfer Engine, Tree Command, and Advanced Filtering**

This release adds comprehensive transfer operations with preflight probing, parallel prefix discovery (14x speedup), a new tree command for prefix summaries, advanced metadata filtering, and a global readonly safety latch.

### Transfer Workflow

```bash
# Validate manifest and check permissions
gonimbus preflight --job transfer.yaml --dry-run

# Execute transfer (copy or move)
gonimbus transfer --job transfer.yaml
```

Features:

- Copy/move objects between buckets with path transformation
- Same-bucket, cross-account, and cross-provider (AWS → R2/Wasabi) transfers
- Preflight permission probing: verify read/write/delete before enumeration
- Parallel prefix discovery with 14x speedup for large buckets
- Deduplication: skip by ETag, key, or always transfer

### Tree Workflow

```bash
# Direct prefix summary (non-recursive)
gonimbus tree s3://my-bucket/data/

# Depth-limited traversal with safety limits
gonimbus tree s3://my-bucket/production/ --depth 2 --timeout 5m

# Include/exclude patterns for traversal scope
gonimbus tree s3://my-bucket/ --depth 4 --include 'production/**' --exclude '**/_temporary/**'
```

Features:

- Directory-like summaries with table or JSONL output
- Depth-limited traversal with bounded safety limits
- Partial results on timeout/max limits (streamed JSONL)
- Include/exclude patterns for pathfinder-style scope control

### Inspect Workflow (Advanced Filtering)

```bash
# Size range filtering (supports KB/KiB, MB/MiB, GB/GiB)
gonimbus inspect s3://my-bucket/data/ --min-size 1KB --max-size 100MB

# Date range filtering (ISO 8601)
gonimbus inspect s3://my-bucket/data/ --after 2024-01-01 --before 2024-06-30

# Key regex filtering
gonimbus inspect s3://my-bucket/data/ --key-regex '\.json$'
```

### Safety Features

**Global Readonly Safety Latch:**

```bash
export GONIMBUS_READONLY=1
```

Blocks provider-side mutations:

- Refuses transfer jobs
- Refuses write-probe preflight
- Intended for dogfooding and lower-trust automation

### Performance Benchmarks

**Parallel Prefix Discovery (Sharding):**

Multi-level prefix trees (4K prefixes, scales to millions):

| Configuration         | Time  | Speedup |
| --------------------- | ----- | ------- |
| Sequential            | 21.2s | 1.0x    |
| Parallel (8 workers)  | 3.2s  | 6.6x    |
| Parallel (16 workers) | 2.2s  | 9.5x    |
| Parallel (32 workers) | 1.5s  | **14x** |

**Tree Parallel Sweep (Depth Traversal):**

| Configuration | Result              |
| ------------- | ------------------- |
| `parallel=1`  | Timeout (10m limit) |
| `parallel=32` | 25s completion      |

### Documentation

- [Transfer Operations](docs/user-guide/transfer.md) - Full transfer guide with examples
- [Preflight Probing](docs/appnotes/preflight.md) - Permission verification contract
- [Tree Command Examples](docs/user-guide/examples/tree.md) - Prefix summary recipes
- [Advanced Filtering](docs/user-guide/examples/advanced-filtering.md) - Size/date/regex filtering

### Bug Fixes

- Fixed "failed to rewind transport stream for retry" errors during transfer
- Fixed missing duration in tree summary records
- Fixed table output serialization for timeout/partial results; timeout now emits clean `error.v1` + `summary.v1` (was FATAL)

See [docs/releases/v0.1.2.md](docs/releases/v0.1.2.md) for complete release notes.

---

## v0.1.1 (2026-01-05)

**Enterprise Authentication & Test Infrastructure**

This release adds enterprise AWS SSO support with improved diagnostics, plus comprehensive cloud integration tests that bring S3 provider coverage from 49% to 97%.

### Highlights

- **AWS Profile & SSO Support**: `doctor --profile` flag for enterprise SSO diagnostics
- **Credential Expiry Warnings**: Proactive alerts when SSO tokens expire within 1 hour
- **Cloud Integration Tests**: S3 provider and CLI tests using moto (AWS mock server)
- **Faster Doctor**: IMDS timeout eliminated when profile/env credentials available

### New Commands

```bash
# Check SSO profile credentials
gonimbus doctor --provider s3 --profile my-sso-profile

# Run cloud integration tests (for contributors)
make moto-start && make test-cloud
```

### For Enterprise Users

AWS SSO (Identity Center) users can now validate their configuration:

```bash
# Login to SSO
aws sso login --profile my-sso-profile

# Verify credentials work with gonimbus
gonimbus doctor --provider s3 --profile my-sso-profile

# Run inspection
gonimbus inspect s3://bucket/ --profile my-sso-profile
```

See [docs/auth/aws-profiles.md](docs/auth/aws-profiles.md) for multi-account SSO patterns.

### For Contributors

Cloud integration tests now run in CI using moto as a service container. To run locally:

```bash
make moto-start    # Start moto on port 5555
make test-cloud    # Run cloud integration tests
make moto-stop     # Clean up
```

See [docs/development/testing.md](docs/development/testing.md) for testing philosophy and coverage approach.

See [docs/releases/v0.1.1.md](docs/releases/v0.1.1.md) for full release notes.
