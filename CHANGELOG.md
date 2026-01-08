# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.2] - 2026-01-XX (in progress)

### Added

- **Transfer Engine** (`pkg/transfer/`, `internal/cmd/transfer.go`)
  - Manifest-driven copy/move operations between S3 buckets
  - `gonimbus transfer --job manifest.yaml` CLI command
  - Support for same-bucket, cross-account, and cross-provider transfers
  - Configurable concurrency and `on_exists` behavior (skip, overwrite, fail)
  - Path templates for destination key transformation (`{key}`, `{basename}`, `{ext}`)

- **Preflight Permission Probing** (`pkg/preflight/`, `internal/cmd/preflight.go`)
  - Pre-transfer capability verification (read, write, delete permissions)
  - `gonimbus preflight --job manifest.yaml` standalone command
  - Zero-side-effect probes: `multipart-abort` (preferred) and `put-delete` strategies
  - Detailed JSONL preflight records with per-capability results
  - Documentation: `docs/appnotes/preflight.md`

- **Prefix Sharding for Parallel Enumeration** (`pkg/shard/`)
  - `sharding.enabled`, `sharding.depth`, `sharding.list_concurrency` manifest options
  - Parallel prefix discovery using delimiter listing
  - Bounded concurrency with configurable worker pools
  - Up to 14x speedup for multi-level prefix trees (tested with 4K prefixes, scales to millions)
  - Live benchmark test: `pkg/shard/discovery_benchmark_test.go`

- **Documentation**
  - Transfer operations user guide: `docs/user-guide/transfer.md`
  - Preflight permission probe app note: `docs/appnotes/preflight.md`

### Changed

- Preflight probe ordering: write probes now run before read probes for faster fail-fast
- Transfer manifest schema extended with `sharding` and `path_template` fields

### Fixed

- **Retryable PUT Bodies** (`pkg/transfer/`)
  - Fixed "failed to rewind transport stream for retry" errors on transient failures
  - Small objects now buffered with seekable wrapper for SDK retry support

### Performance

- **Parallel Prefix Discovery**: 14x speedup at 32 concurrency for multi-level prefix trees
  - Sequential: 21.2s → Parallel: 1.5s (tested with 4K prefixes, designed for millions)
  - Recommended: `list_concurrency: 16-32` for large buckets

## [0.1.1] - 2026-01-05

### Added

- **AWS Profile Authentication** (`internal/cmd/doctor.go`)
  - `--profile` flag on `doctor` command for enterprise SSO diagnostics
  - Credential expiry check with warning when < 1 hour remaining
  - IMDS timeout optimization when profile/env credentials available
  - SSO-aware help text (`aws sso login` guidance)
  - Documentation: `docs/auth/aws-profiles.md`

- **Cloud Integration Tests** (`test/cloudtest/`, `pkg/provider/s3/`, `internal/cmd/`)
  - S3 provider integration tests using moto (AWS mock server)
  - CLI inspect command end-to-end tests
  - Test helpers for bucket creation, object upload, and isolation
  - Makefile targets: `test-cloud`, `moto-start`, `moto-stop`, `moto-status`
  - CI workflow with moto service container
  - Documentation: `docs/development/testing.md`

### Changed

- S3 provider test coverage increased from 49% to 97% with cloud integration tests
- `ec2/imds` promoted from indirect to direct dependency (IMDS timeout control)

### Fixed

- `make install` now correctly installs binary to `~/.local/bin`

## [0.1.0] - 2026-01-03

Initial public release of Gonimbus - a Go-first library + CLI + server for large-scale inspection and crawl of cloud object storage.

### Added

- **Provider Interface & S3 Implementation** (`pkg/provider/`)
  - Abstract provider interface with `List`, `Head`, and `Close` methods
  - S3 provider using AWS SDK v2 with default credential chain
  - Support for S3-compatible stores (Wasabi, Cloudflare R2, DigitalOcean Spaces)
  - Custom endpoint and explicit credential configuration

- **Pattern Matching Layer** (`pkg/match/`)
  - Doublestar glob pattern matching for cloud object keys
  - Prefix derivation algorithm for efficient listing at scale
  - Include/exclude pattern support
  - Hidden file detection and filtering

- **JSONL Output Layer** (`pkg/output/`)
  - Typed record envelopes: `gonimbus.object.v1`, `gonimbus.error.v1`, `gonimbus.progress.v1`
  - Stream-friendly JSONL writer with atomic line writes
  - Configurable progress emission

- **Crawl Engine** (`pkg/crawler/`)
  - Bounded streaming pipeline: lister → matcher → writer
  - Configurable concurrency and rate limiting
  - Backpressure via bounded channels
  - Context cancellation and graceful shutdown
  - Progress tracking and summary statistics

- **Job Manifest Schema** (`pkg/manifest/`)
  - JSON Schema validated job manifests (YAML/JSON)
  - Connection, match, crawl, and output configuration
  - Strict validation with clear error messages

- **CLI Commands** (`internal/cmd/`)
  - `gonimbus crawl` - Run crawl jobs from manifest files
  - `gonimbus inspect` - Quick inspection of objects or prefixes
  - `gonimbus doctor` - Environment and credential diagnostics
  - `gonimbus serve` - HTTP server with health endpoints
  - `gonimbus version` - Version and build information

- **Server Skeleton** (`internal/server/`)
  - Chi-based HTTP router with middleware stack
  - Health check endpoints (`/health`, `/health/live`, `/health/ready`, `/health/startup`)
  - Prometheus metrics endpoint (`/metrics`)
  - Version endpoint (`/version`)
  - Panic recovery and error handling middleware

- **Documentation**
  - Storage provider configuration guide (`docs/appnotes/storage-providers.md`)
  - Example manifests for common use cases (`examples/manifests/`)
  - CLI usage examples (`examples/cli/`)

### Infrastructure

- Makefile with quality gates (`make check-all`, `make prepush`)
- License-audit target with dependency cooling policy (`.goneat/dependencies.yaml`)
- golangci-lint integrated via goneat assess
- Release signing workflow (minisign + optional PGP)
- Embedded app identity via `.fulmen/app.yaml`
- gofulmen v0.2.1 / Crucible v0.3.0 integration
- ADR-0001: Embedded assets over directory walking
- ADR-0002: Pathfinder boundary constraints in tests

[Unreleased]: https://github.com/3leaps/gonimbus/compare/v0.1.2...HEAD
[0.1.2]: https://github.com/3leaps/gonimbus/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/3leaps/gonimbus/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/3leaps/gonimbus/releases/tag/v0.1.0
