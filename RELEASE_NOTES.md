# Release Notes

This file contains release notes for the three most recent releases in reverse chronological order. For the complete release history, see the [CHANGELOG](CHANGELOG.md) or the [docs/releases/](docs/releases/) directory.

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

---

## v0.1.0 (2026-01-03)

**Initial Public Release**

Gonimbus is a Go-first library + CLI + server for large-scale inspection and crawl of cloud object storage (100K-1M+ objects). This release delivers S3 support with prefix-first listing and JSONL output.

### Highlights

- **S3 & S3-Compatible Support**: AWS S3, Wasabi, Cloudflare R2, DigitalOcean Spaces
- **Pattern Matching**: Doublestar globs with intelligent prefix derivation for scale
- **Streaming Output**: JSONL records with typed envelopes for objects, errors, and progress
- **Bounded Pipeline**: Configurable concurrency with backpressure and rate limiting
- **Schema-Validated Manifests**: YAML/JSON job manifests with strict validation

### CLI Commands

```bash
# Quick inspection
gonimbus inspect s3://bucket/prefix/

# Run a crawl job
gonimbus crawl --job manifest.yaml

# Check environment
gonimbus doctor
```

### Getting Started

```bash
go install github.com/3leaps/gonimbus/cmd/gonimbus@v0.1.0
gonimbus version
```

See [docs/releases/v0.1.0.md](docs/releases/v0.1.0.md) for full release notes.
