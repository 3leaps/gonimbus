# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.8] - 2026-05-05

### Added

#### Index Hub (`index hub` + `index export` + `index hydrate`)

- **Index Hub CRUD** (`internal/cmd/index_hub.go`)
  - `gonimbus index hub init` — create a new hub root with marker file
  - `gonimbus index hub ls` — list index sets and their runs at a hub
  - `gonimbus index hub show` — show details for a specific index set or run
  - `gonimbus index hub set-latest` — advance the `latest.json` pointer for an index set (requires committed run)
  - `gonimbus index hub rm-run` — remove a specific run; protects `latest` unless `--force`
  - `gonimbus index hub gc` — garbage-collect runs by `--keep N` or `--before DATE`; supports `--dry-run` and `--json`

- **Index Export** (`internal/cmd/index_export.go`)
  - `gonimbus index export` publishes an index run to a file or S3 hub
  - Atomic publish sequence: `index.db` → `identity.json` → `complete.json` (commit marker) → `latest.json`
  - SHA-256 + size integrity manifest in `complete.json`
  - `latest.json` is best-effort last-writer-wins for v0.1.x; CAS / fail-closed semantics tracked for v0.2.x

- **Index Hydrate** (`internal/cmd/index_hydrate.go`)
  - `gonimbus index hydrate` downloads a published index run from a hub
  - Resolves run via `latest.json` pointer or explicit `--run-id`
  - SHA-256 + size verification for `index.db` and `identity.json`
  - Rejects uncommitted runs (no `complete.json`)
  - Saves `complete.json` to destination for provenance

- **Hub JSON Schemas** (`schemas/gonimbus/v1.0.0/`)
  - `index-hub.schema.json` — hub marker
  - `index-hub-complete.schema.json` — run commit marker with integrity manifest
  - `index-hub-latest.schema.json` — index-set latest pointer
  - `index-hub-identity.schema.json` — index set identity descriptor

#### Index Query Flags

- **`--index-set <id>`** (`internal/cmd/index_query.go`) — explicit index-set selection when multiple sets share a base URI; resolves prefix or full `idx_<64hex>` form
- **`--output <uri>`** — stream query results to S3 or `file://` destinations (in addition to stdout)

#### Workspace Pattern

- **Workspace convention** (`docs/user-guide/workspace.md`)
  - `workspace.yaml` schema and layout convention
  - Documented shard strategies for date-partitioned data
  - Operational flows: build+publish, hydrate+query, extract+reflow, hub maintenance
  - Rewrite template guidance and scheduling patterns

#### Role Catalog

- **Dataeng role** (`config/agentic/roles/dataeng.yaml`) — pipeline operations, manifests, integration testing; updated for v0.1.8 hub/workspace operations
- **Attribution policy** (`AGENTS.md`) — strengthened to mandate `noreply@3leaps.net` and reject model-provider domains

### Changed

- **Pre-push hook** (`.goneat/hooks.yaml`) — assess gate scoped to `--new-issues-only --new-issues-base origin/main` so unrelated changes don't pay for legacy lint debt
- **AGENTS.md** — `.plans/` references retired; planning artifacts now live in the productbook (private) and the OOB workspace (client-confidential, private)
- **AGENTS.md DO NOT list** — replaced narrow `.plans/` rule with broader prohibition on referencing client data, paths, or identifiers in repo content

### Fixed

- **`gonimbus index hub gc --json`** silently no-oped deletions (`internal/cmd/index_hub.go`) — fixed to honor `--dry-run` correctly and emit per-run outcomes (artifacts deleted, errors) in the JSON envelope; regression test added
- Five gosec G115 / G703 findings annotated with rationale (provably bounded conversions; user-supplied CLI paths)
- One golangci-lint QF1012 (`fmt.Sprintf` → `fmt.Fprintf`) in `pkg/manifest/validate.go`

### Removed

- **Guardian browser-intercept hooks** (`.goneat/hooks/pre-commit`, `.goneat/hooks/pre-push`) — regenerated without `--with-guardian` for the larger team feature-branch workflow

## [0.1.7] - 2026-01-28

### Added

#### Transfer Reflow (`transfer reflow`)

- **Transfer Reflow Command** (`internal/cmd/transfer_reflow.go`)
  - `gonimbus transfer reflow <source-uri>` copies objects while rewriting keys
  - Template-based path variable extraction and substitution
  - Supports probe-derived variables (e.g., `{business_date}` from content)
  - Parallel copy with configurable workers (`--parallel`, default 16)
  - Checkpoint/resume with SQLite state (`--checkpoint`, `--resume`)
  - Dry-run mode for planning (`--dry-run`)
  - Collision detection and handling (`--on-collision log|fail|overwrite`)

- **Reflow Package** (`pkg/transfer/`)
  - `ReflowRewrite` for template parsing and key transformation
  - Path segment variable extraction (`{program}`, `{site}`, `{date}`, etc.)
  - Support for probe-derived variables via `ApplyWithVars`
  - Wildcard segments (`{_}`) for ignored path components

#### Content Probe (`content probe`)

- **Content Probe Command** (`internal/cmd/content_probe.go`)
  - `gonimbus content probe <uri>` extracts derived fields from content
  - Config-driven extraction rules (`--config probe.yaml`)
  - Bulk processing via `--stdin`
  - Output modes: `--emit probe|reflow-input|both`
  - Parallel probing with `--concurrency` (default 16)

- **Probe Package** (`pkg/probe/`)
  - XPath extractor for XML content (`//TagName`, `/a/b/c`)
  - Regex extractor with named/numbered capture groups
  - JSON path extractor (`$.a.b[0].id`)
  - Configurable byte window (`--bytes`, default 4096)

#### file:// Provider

- **Local Filesystem Support** (`pkg/provider/file/`)
  - `file://` URIs as transfer reflow destinations
  - Automatic directory creation
  - Collision detection for existing files
  - Overwrite support (`--overwrite --on-collision overwrite`)

#### Bulk Input Support

- **Bulk Content Head** (`internal/cmd/content_head.go`)
  - `gonimbus content head --stdin` for parallel multi-object inspection
  - JSONL input from inspect or index query output
  - Configurable concurrency (`--concurrency`)

### Changed

- Content commands consistently emit `gonimbus.error.v1` for errors
- Transfer reflow accepts `gonimbus.reflow.input.v1` records from probe

## [0.1.6] - 2026-01-25

### Added

#### Content Inspection Commands (`content head`)

- **Content Head Command** (`internal/cmd/content_head.go`)
  - `gonimbus content head <uri>` reads the first N bytes of an object
  - Uses HTTP Range requests when provider supports them (falls back to GetObject)
  - Output is JSONL-only (`gonimbus.content.head.v1`) with base64-encoded content
  - No mixed framing - suitable for simple inspection pipelines
  - Includes full metadata (etag, size, last_modified, content_type)

- **Content Package** (`pkg/content/`)
  - `HeadBytes(ctx, provider, key, n)` - read first N bytes with metadata
  - `HeadBytesMulti` - parallel multi-key content head operations
  - Automatic fallback: GetRange → GetObject (provider capability detection)

#### Provider Range Requests

- **ObjectRanger Interface** (`pkg/provider/capabilities.go`)
  - `GetRange(ctx, key, start, endInclusive)` for byte-range reads
  - HTTP Range semantics (inclusive start/end offsets)
  - S3 provider implementation with range header support

- **S3 Range Support** (`pkg/provider/s3/provider.go`)
  - Implements `ObjectRanger` interface for S3 and S3-compatible stores
  - Cloud integration tests for range request behavior

#### Documentation

- User guide: streaming vs content command mental model
- Content inspection examples

### Changed

- Provider capability detection uses interface type assertions for optional features
- Content commands emit errors to stdout as `gonimbus.error.v1` (consistent with stream commands)

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

[Unreleased]: https://github.com/3leaps/gonimbus/compare/v0.1.7...HEAD
[0.1.7]: https://github.com/3leaps/gonimbus/compare/v0.1.6...v0.1.7
[0.1.6]: https://github.com/3leaps/gonimbus/compare/v0.1.5...v0.1.6
[0.1.5]: https://github.com/3leaps/gonimbus/compare/v0.1.4...v0.1.5
[0.1.4]: https://github.com/3leaps/gonimbus/compare/v0.1.3...v0.1.4
[0.1.3]: https://github.com/3leaps/gonimbus/compare/v0.1.2...v0.1.3
[0.1.2]: https://github.com/3leaps/gonimbus/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/3leaps/gonimbus/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/3leaps/gonimbus/releases/tag/v0.1.0
