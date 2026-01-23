# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.5] - 2026-01-23

### Added

#### Content Streaming Commands (`stream get`, `stream head`)

- **Stream Get Command** (`internal/cmd/stream_get.go`)
  - `gonimbus stream get <uri>` streams object content with JSONL framing
  - Mixed-framing output: JSONL headers + raw bytes for efficient large payload handling
  - `gonimbus.stream.open.v1` with uri, size, etag, last_modified, content_type
  - `gonimbus.stream.chunk.v1` with seq, nbytes followed by raw bytes
  - `gonimbus.stream.close.v1` with status, chunks, bytes
  - Size validation: HEAD size vs GetObject size mismatch detection (stale key semantics)
  - Errors emitted to stdout as `gonimbus.error.v1` (streaming mode contract)

- **Stream Head Command** (`internal/cmd/stream_head.go`)
  - `gonimbus stream head <uri>` retrieves object metadata without content
  - Returns `gonimbus.object.v1` with full metadata including custom S3 user metadata
  - Errors emitted to stdout as `gonimbus.error.v1` (consistent with streaming mode)

- **Stream Package** (`pkg/stream/`)
  - `Writer` for producing mixed-framing streams (JSONL + raw bytes)
  - `Decoder` for consuming streams with truncation detection (`io.ErrUnexpectedEOF`)
  - Exact byte reconstruction verified via SHA256/MD5 round-trip testing

#### Transfer Size Validation

- **validate=size** (`pkg/transfer/`)
  - Compares enumerated size (from list/index) vs GetObject content-length
  - Catches stale index/list metadata before deep pipeline processing
  - Size mismatch mapped to `NOT_FOUND` error code (stale key semantics)
  - `SizeMismatchError` type with key, expected, and got fields

#### Documentation

- ADR-0004: Language-neutral content stream contract (`docs/architecture/adr/`)
- Streaming contract specification (`docs/development/streaming/`)
- QA checklist and helper replication guidance

### Fixed

- Cloud integration test credentials for stream writer tests (`pkg/stream/writer_cloudintegration_test.go`)

## [0.1.4] - 2026-01-19

### Added

#### Path-Scoped Index Builds (`build.scope`)

- **Scope Types** (`pkg/manifest/`, `internal/assets/schemas/`)
  - `prefix_list`: Explicit prefixes for deterministic crawl scope
  - `date_partitions`: Dynamic prefix generation from date ranges with segment discovery
  - `union`: Combine multiple scope definitions

- **Scope Compiler** (`pkg/scope/`)
  - Compiles `build.scope` configuration into explicit prefix plans
  - Delimiter listing for segment discovery (e.g., device IDs under store prefixes)
  - Date range expansion to concrete `YYYY-MM-DD/` prefixes
  - `--dry-run` flag previews scope plan before execution

- **Scope Guardrails** (`pkg/scope/`)
  - Warning threshold for large prefix expansions
  - Soft-delete skipped by default for scoped builds (partial coverage)
  - Scope config included in IndexSet identity hash

- **Provider Capability Contract** (`docs/architecture/adr/ADR-0003-*.md`)
  - ADR-0003: Defines prefix listing and delimiter listing requirements
  - Error classification for partial run handling
  - Provider-agnostic scope compilation contract

#### Index Job Management

- **Job Registry** (`pkg/jobregistry/`)
  - Durable on-disk job records under the app data dir (`jobs/index-build/<job_id>/job.json`)
  - Captures identity/run metadata, PID, heartbeat timestamps, and log file paths

- **Managed Background Builds** (`internal/cmd/index_build.go`, `pkg/jobregistry/executor.go`)
  - `gonimbus index build --background` spawns a managed child process and returns a job id
  - Captures stdout/stderr to per-job log files
  - Safe cancellation via SIGTERM -> context cancellation; SIGKILL fallback

- **Job CLI** (`internal/cmd/index_jobs*.go`)
  - `gonimbus index jobs list/status` with JSON output support
  - `jobs status` supports short id prefix resolution when unambiguous
  - `gonimbus index jobs stop/logs/gc` for operational control
  - `--dedupe` prevents starting duplicate running jobs for the same manifest

#### Documentation

- Enterprise indexing workflow guide with three-tier model (`docs/user-guide/index.md`)
- Indexing architecture with scope concepts (`docs/architecture/indexing.md`)
- ADR-0003: Index build provider capabilities (`docs/architecture/adr/`)

### Changed

- `--after` filter is now inclusive (was exclusive) for consistency with date range semantics
- Soft-delete skipped by default for scoped builds (partial coverage assumption)
- Index identity now includes scope configuration hash for isolation

### Fixed

- Tree traversal callback is now safe under parallel execution (`internal/cmd/tree.go`)

### Performance

- **99.5% reduction** in objects listed with `build.scope.date_partitions` on date-partitioned data
- **~10x faster** build times (3 min → 30 sec for 15-store scoped builds)
- Zero wasted enumeration: `objects_found ≈ objects_matched` with scope

## [0.1.3] - 2026-01-15

### Added

#### Index Workflow

- **Local Index Store** (`pkg/indexstore/`)
  - SQLite-based local index for offline bucket inventory
  - Per-index database isolation (hash-based identity)
  - Streaming batch ingestion from crawl results
  - Soft-delete handling for removed objects
  - Schema version tracking for upgrades

- **Index CLI Commands** (`internal/cmd/index*.go`)
  - `gonimbus index init` - Initialize local index database
  - `gonimbus index build --job <manifest>` - Build index from crawl
  - `gonimbus index list` - List local indexes with stats
  - `gonimbus index query <uri>` - Query indexed objects by pattern
  - `gonimbus index stats <uri>` - Detailed index statistics
  - `gonimbus index gc` - Garbage collect old indexes
  - `gonimbus index doctor` - Validate index integrity and identity
  - `gonimbus index show` - Display manifest provenance

- **Index Build Features**
  - Build-time include patterns for scope control
  - Derived prefix display during builds
  - Explicit identity validation (provider, region, endpoint)
  - Tolerates provider outages via SDK retry

- **Index Query Features**
  - Pattern matching with doublestar globs
  - Metadata filters: `--min-size`, `--max-size`, `--after`, `--before`
  - Count mode: `--count` for quick totals
  - JSONL output for integration with other tools

- **Index Manifest Schema** (`internal/assets/schemas/index-manifest.schema.json`)
  - Connection, identity, build, and output configuration
  - Build-time scope with include patterns
  - Provider identity for multi-cloud support

### Changed

- Index set identity now includes provider identity hash for isolation
- Index runs track partial/failed status for operational visibility

### Performance

- **Query Speedup**: 100-1000x faster than live crawl for repeated queries
- **Build Throughput**: ~3,000 objects/sec ingestion rate
- **Tested Scale**: 16M objects enumerated, 150K indexed (with filters)

## [0.1.2] - 2026-01-11

### Added

#### Transfer Workflow

- **Transfer Engine** (`pkg/transfer/`, `internal/cmd/transfer.go`)
  - Manifest-driven copy/move operations between S3 buckets
  - `gonimbus transfer --job manifest.yaml` CLI command
  - Support for same-bucket, cross-account, and cross-provider transfers
  - Configurable concurrency and `on_exists` behavior (skip, overwrite, fail)
  - Path templates for destination key transformation (`{filename}`, `{dir[n]}`, `{key}`)
  - Deduplication strategies: `etag` (default), `key`, or `none`

- **Prefix Sharding for Parallel Enumeration** (`pkg/shard/`)
  - `sharding.enabled`, `sharding.depth`, `sharding.list_concurrency` manifest options
  - Parallel prefix discovery using delimiter listing
  - Bounded concurrency with configurable worker pools
  - Up to 14x speedup for multi-level prefix trees (tested with 4K prefixes, scales to millions)
  - Live benchmark test: `pkg/shard/discovery_benchmark_test.go`

- **Preflight Permission Probing** (`pkg/preflight/`, `internal/cmd/preflight.go`)
  - Pre-transfer capability verification (read, write, delete permissions)
  - `gonimbus preflight --job manifest.yaml` standalone command
  - Three modes: `plan-only` (no calls), `read-safe` (List/Head/Get), `write-probe` (with probes)
  - Zero-side-effect probes: `multipart-abort` (preferred) and `put-delete` strategies
  - Detailed JSONL preflight records with per-capability results
  - Documentation: `docs/appnotes/preflight.md`

#### Tree Workflow

- **Tree Command for Prefix Summaries** (`internal/cmd/tree.go`)
  - `gonimbus tree <uri>` CLI command for directory-like summaries
  - Direct-only (non-recursive) operation by default
  - Depth-limited traversal with `--depth N` flag
  - Safety limits: `--timeout`, `--max-prefixes`, `--max-objects`, `--max-pages`
  - Include/exclude patterns for traversal scope (pathfinder-style)
  - Table output with formatted sizes and counts
  - JSONL output for streaming and partial results

#### Inspect Workflow

- **Advanced Metadata Filtering** (`pkg/match/filter.go`)
  - Size filtering: `min_size`, `max_size` with KB/KiB/MB/MiB/GB/GiB units
  - Date filtering: `after`, `before` with ISO 8601 dates/datetimes
  - Key regex filtering: `key_regex` with Go regexp syntax
  - CLI flags: `--min-size`, `--max-size`, `--after`, `--before`, `--key-regex`
  - Manifest configuration: `match.filters.size`, `match.filters.modified`, `match.filters.key_regex`

#### General & Safety

- **Global Readonly Safety Latch** (`internal/cmd/root.go`)
  - `--readonly` flag and `GONIMBUS_READONLY=1` environment variable
  - Blocks provider-side mutations (transfers, write-probe preflight)
  - Intended for dogfooding and lower-trust automation
  - Readonly tests: `internal/cmd/readonly_test.go`

#### Documentation

- Transfer operations user guide: `docs/user-guide/transfer.md`
- Preflight permission probe app note: `docs/appnotes/preflight.md`
- Examples cookbook: `docs/user-guide/examples/README.md`
- Tree command examples: `docs/user-guide/examples/tree.md`
- Advanced filtering examples: `docs/user-guide/examples/advanced-filtering.md`

### Changed

- Preflight probe ordering: write probes now run before read probes for faster fail-fast
- Transfer manifest schema extended with `sharding`, `path_template`, `dedup` fields
- Job manifest schema extended with `preflight` and `filters` fields

### Fixed

- **Retryable PUT Bodies** (`pkg/transfer/`)
  - Fixed "failed to rewind transport stream for retry" errors on transient failures
  - Small objects now buffered with seekable wrapper for SDK retry support

- **Tree Command** (`internal/cmd/tree.go`)
  - Fixed missing duration field in summary records
  - Fixed table output serialization for timeout/partial results
  - Ensure summary is emitted even when timeout occurs
  - Fixed timeout producing FATAL instead of clean partial output with `error.v1` + `summary.v1`

### Performance

- **Parallel Prefix Discovery**: 14x speedup at 32 concurrency for multi-level prefix trees
  - Sequential: 21.2s → Parallel: 1.5s (tested with 4K prefixes, designed for millions)
  - Recommended: `list_concurrency: 16` default, 32 for very large workloads

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

[Unreleased]: https://github.com/3leaps/gonimbus/compare/v0.1.5...HEAD
[0.1.5]: https://github.com/3leaps/gonimbus/compare/v0.1.4...v0.1.5
[0.1.4]: https://github.com/3leaps/gonimbus/compare/v0.1.3...v0.1.4
[0.1.3]: https://github.com/3leaps/gonimbus/compare/v0.1.2...v0.1.3
[0.1.2]: https://github.com/3leaps/gonimbus/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/3leaps/gonimbus/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/3leaps/gonimbus/releases/tag/v0.1.0
