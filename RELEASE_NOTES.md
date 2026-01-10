# Release Notes

This file contains release notes for the three most recent releases in reverse chronological order. For the complete release history, see the [CHANGELOG](CHANGELOG.md) or the [docs/releases/](docs/releases/) directory.

---

## v0.1.2 (2026-01-XX) - In Progress

**Transfer Engine & Parallel Discovery**

This release adds a full transfer engine for copy/move operations between S3 buckets, preflight permission probing, and parallel prefix discovery with up to 14x speedup for large-scale enumeration.

### Highlights

- **Transfer Operations**: Copy and move objects between buckets with manifest-driven configuration
- **Preflight Probing**: Verify read/write/delete permissions before transfer with zero-side-effect probes
- **Parallel Discovery**: 14x faster prefix enumeration with bounded concurrency (scales to millions of objects)
- **Cross-Provider Support**: Same-bucket, cross-account, and cross-provider (AWS → R2/Wasabi) transfers
- **Retry Resilience**: Fixed stream retry issues with seekable buffering for PUT operations

### New Commands

```bash
# Run a transfer job (copy objects between buckets)
gonimbus transfer --job transfer-manifest.yaml

# Check permissions before transfer (dry-run)
gonimbus preflight --job transfer-manifest.yaml

# Quick copy with sharding for large buckets
gonimbus transfer --job manifest.yaml  # with sharding.enabled: true
```

### Transfer Manifest Example

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

transfer:
  mode: copy
  concurrency: 32
  on_exists: skip
  path_template: "archive/{key}"
  sharding:
    enabled: true
    depth: 2
    list_concurrency: 16
```

### Performance Benchmarks

Parallel prefix discovery on multi-level prefix trees (tested with 100K objects, 4K prefixes):

| Configuration         | Time  | Speedup |
| --------------------- | ----- | ------- |
| Sequential            | 21.2s | 1.0x    |
| Parallel (8 workers)  | 3.2s  | 6.6x    |
| Parallel (16 workers) | 2.2s  | 9.5x    |
| Parallel (32 workers) | 1.5s  | **14x** |

Designed for buckets with millions of objects - speedup increases with prefix count.

### For Operators

- Use `sharding.enabled: true` with appropriate `depth` for buckets with millions of objects
- `list_concurrency: 16` is the recommended default; 32 for very large workloads
- `on_exists: overwrite` is fastest for initial migrations; `skip` for incremental sync
- Preflight probes use `multipart-abort` by default (zero storage side effects)

### Bug Fixes

- Fixed "failed to rewind transport stream for retry" errors during transfer
- Small objects now properly buffered for SDK retry support

See [docs/user-guide/transfer.md](docs/user-guide/transfer.md) for complete transfer documentation.

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
