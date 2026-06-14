# Gonimbus

[![CI](https://github.com/3leaps/gonimbus/actions/workflows/ci.yml/badge.svg)](https://github.com/3leaps/gonimbus/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/3leaps/gonimbus)](https://goreportcard.com/report/github.com/3leaps/gonimbus)
[![Go Reference](https://pkg.go.dev/badge/github.com/3leaps/gonimbus.svg)](https://pkg.go.dev/github.com/3leaps/gonimbus)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

> High-scale cloud object storage inspection and crawl engine

Gonimbus is a Go-first **library + CLI + server** for large-scale inspection and crawl of cloud object storage. It produces machine-friendly outputs (JSONL baseline) and favors **prefix-first listing** with doublestar matching to stay fast and predictable.

**Scale**: Tested with 32M+ object buckets. Path-scoped index builds reduce listing costs by 99%+ on date-partitioned data.

## Modes

- **CLI**: Run validated crawl/inspect jobs from manifests; stream JSONL to stdout/files or index sinks
- **Server**: Long-running runner with streaming results; intended to live near the data and accept remote job submissions
- **Library**: Embeddable components (matcher, crawler, outputs, provider backends) for Go apps. See [docs/library-consumers.md](docs/library-consumers.md) for the supported import surface (`pkg/uri`, `pkg/match`, `pkg/provider`, `pkg/provider/s3`, `pkg/provider/file`) and the embedding contract

## Quick Start

### Prerequisites

- Go 1.25+ ([install](https://go.dev/doc/install))
- golangci-lint ([install](https://golangci-lint.run/welcome/install/))

### Install

```bash
go install github.com/3leaps/gonimbus/cmd/gonimbus@latest
gonimbus version
```

`go install`-built binaries report the correct version via `runtime/debug.ReadBuildInfo` + an embedded `VERSION` file (no `-ldflags` injection required).

### Run

```bash
# Quick inspection of an S3 prefix
gonimbus inspect s3://my-bucket/path/to/data/

# Run a crawl job from manifest
gonimbus crawl --job crawl-manifest.yaml

# Check environment and auth
gonimbus doctor

# Check with specific AWS profile
gonimbus doctor --provider s3 --profile my-sso-profile

# Start server mode
gonimbus serve
```

### Build from Source

```bash
git clone https://github.com/3leaps/gonimbus.git
cd gonimbus
make bootstrap
make build
./bin/gonimbus version
```

## Core Capabilities

### Providers

- **S3/S3-compatible**: First-class support with access key/secret
- **AWS profiles**: Assume-role chains, SSO, cached tokens
- **Embedded S3 auth controls**: Anonymous public reads and injected AWS SDK
  credential providers for Go library consumers
- **GCS**: Roadmap (placeholder stub today; full provider tracked for a later release)

### Authentication

Uses SDK default auth chains - no reinventing the wheel:

- AWS: env vars, shared config/credentials, profiles, SSO, web identity/IRSA
- Enterprise SSO: `--profile` flag with `aws sso login` workflow
- Raw keys supported as explicit fallback (Wasabi, DigitalOcean Spaces)
- Library consumers can opt into unsigned public reads with
  `s3.Config{Anonymous: true}` or inject caller-managed AWS SDK credentials
  with `s3.Config.CredentialsProvider`

See [docs/auth/aws-profiles.md](docs/auth/aws-profiles.md) for enterprise authentication patterns.

### Matching

- Doublestar semantics over normalized keys
- Derives strongest possible list prefix per pattern (critical for scale)
- Include/exclude pattern support
- Path-scoped index builds for date-partitioned data (see [docs/user-guide/index.md](docs/user-guide/index.md))

### Reflow and Index Operations

`transfer reflow` can write explicit destination user metadata, preserve source content type, set or propagate destination storage class, and derive per-object destination metadata from named source metadata fields or source system fields.

Metadata writes are opt-in and disclosure-sensitive: destination metadata is durable, visible to destination readers, and not redacted at destination. Use `--metadata-policy clear` plus explicit `--metadata-set`, `--metadata-set-from-source-key`, and `--metadata-set-from-source-derived` allow-lists when source metadata may contain sensitive values. See [docs/user-guide/transfer.md](docs/user-guide/transfer.md#destination-metadata) and [docs/releases/v0.2.1.md](docs/releases/v0.2.1.md) for the full operator posture, including the destination-system-metadata non-goal.

Local directory sources are supported with `transfer reflow file://...`; hidden
files and dot-directories are skipped by default unless `--hidden=include` is
set. Indexes retain LIST-derived storage class and can be enriched with
HEAD-derived archive/restore/content-type metadata via `index enrich-with-head`.
Local directory trees can also be backed up into object storage by piping
`crawl --emit reflow-input` into `transfer reflow --stdin`, which preserves
nested paths without rewrite templates. Long-running job-backed `index build`,
`index enrich-with-head`, and `transfer reflow` runs are now failed-resumable:
an interrupted run can be safely continued with `--resume-run <run_id>`.
(Stdin-streamed reflow is not `--resume-run`-resumable.)
`stream put` can upload raw stdin or framed `stream get` batches, reflow can
use `overwrite-if-source-newer` for freshness-based collision handling, and
`inspect-pair` can verify terminal reflow write claims against destination HEAD
results. See [docs/releases/v0.3.1.md](docs/releases/v0.3.1.md) for the current
operator notes.

### Outputs

Stream-friendly JSONL records:

```json
{"type":"gonimbus.object.v1","ts":"2025-01-15T10:30:00.000Z","job_id":"abc123","provider":"s3","data":{...}}
```

Two output modes for content access:

- **Content inspection** (JSONL-only): `content head` reads first N bytes with base64 encoding. See [docs/releases/v0.1.6.md](docs/releases/v0.1.6.md).
- **Content streaming** (mixed framing): `stream get` delivers full content with JSONL headers + raw bytes. See [docs/releases/v0.1.5.md](docs/releases/v0.1.5.md).

Optional DuckDB sink for local indexing.

## Examples

See `docs/user-guide/examples/README.md` for copy/paste recipes (advanced filtering, s3-compatible endpoints, and more as this project grows).

For automated workflow testing and validation, see [fulseed](https://github.com/fulmenhq/fulseed) - a companion tool for building reproducible test scenarios.

## CLI Commands

```bash
# Explore workflow (no index required)
gonimbus tree <uri>            # Prefix summary (directory-like view)
gonimbus inspect <uri>         # Quick inspection with filters
gonimbus crawl --job <path>    # Full crawl to JSONL
gonimbus atlas build --from-index <id> --output <dir> # Build content-addressed atlas artifacts

# Index workflow (for large buckets)
gonimbus index init            # Initialize local index database
gonimbus index build --job <path>  # Build index from crawl
gonimbus index build --background --job <path>  # Background build with job tracking
gonimbus index query <uri>     # Query indexed objects by pattern/storage class
gonimbus index enrich-with-head <index-set-id>  # Cache HEAD-derived archive/restore metadata
gonimbus index list            # List local indexes
gonimbus index stats           # Show index statistics and resumable run state
gonimbus index doctor          # Validate index integrity
gonimbus index gc              # Clean up old indexes
gonimbus index export          # Export an index run to a hub
gonimbus index hydrate         # Download an index run from a hub
gonimbus index hub             # Manage index hubs

# Job management (for long-running builds)
gonimbus index jobs list       # List running and recent jobs
gonimbus index jobs status <id>  # Check job state and progress
gonimbus index jobs logs <id>  # Stream job logs
gonimbus index jobs stop <id>  # Safe cancellation
gonimbus index jobs gc         # Clean up old job records

# Content inspection (JSONL-only, for routing decisions)
gonimbus content head <uri>    # Read first N bytes (base64 in JSONL)

# Content streaming (for pipeline integration)
gonimbus stream head <uri>     # Object metadata (JSONL)
gonimbus stream get <uri>      # Stream full content (JSONL + raw bytes)
gonimbus stream put <uri>      # Upload raw/framed stdin, multipart for large objects

# Operations
gonimbus transfer --job <path> # Copy/move objects between buckets
gonimbus transfer reflow <source> --dest <uri> # Copy objects to a new key layout
gonimbus inspect-pair --from-reflow <path> --expected-dest-prefix <uri> # Verify reflow writes
gonimbus preflight --job <path> # Verify permissions before transfer
gonimbus doctor                # Environment/auth checks
gonimbus envinfo               # Environment summary for support/debugging
gonimbus health                # Self-health check
gonimbus serve                 # Run server mode
gonimbus version               # Version info

# Safety latch: hard-disable provider-side mutations
# gonimbus --readonly <command>
```

## Configuration

Gonimbus uses three-layer configuration via [gofulmen](https://github.com/fulmenhq/gofulmen):

1. **Template Defaults**: `config/gonimbus/v1.0.0/gonimbus-defaults.yaml`
2. **User Overrides**: `~/.config/3leaps/gonimbus.yaml`
3. **Runtime**: Environment variables (`GONIMBUS_*`) and CLI flags

### Environment Variables

```bash
GONIMBUS_PORT=8080              # Server port
GONIMBUS_HOST=localhost         # Server host
GONIMBUS_LOG_LEVEL=info         # Log level (trace/debug/info/warn/error)
GONIMBUS_METRICS_PORT=9090      # Metrics port
GONIMBUS_READONLY=1             # Disable provider-side mutations
```

Copy `.env.example` to `.env` for local development.

## Server Endpoints

When running in server mode:

- `GET /health/*` - Liveness/readiness probes
- `GET /version` - Full version info with SSOT versions
- `GET /metrics` - Prometheus metrics

## Non-Goals

- Mounts, sync engines, FUSE/desktop UX
- "List everything by default" for broad patterns (scale requires explicit sharding)
- Pinning/offline queues

## Development

```bash
make help          # Show all targets
make bootstrap     # Install dependencies
make build         # Build binary
make test          # Run unit tests
make test-cloud    # Run cloud integration tests (requires moto)
make lint          # Run linting
make check-all     # Lint + test
```

### Cloud Integration Tests

Cloud integration tests run against a local S3-compatible endpoint (moto):

```bash
make moto-start    # Start moto server (Docker)
make test-cloud    # Run cloud integration tests
make moto-stop     # Stop moto server
```

See [docs/development/](docs/development/) for detailed development guides including [testing strategy](docs/development/testing.md).

### Contributing

Contributions are welcome — see [`AGENTS.md`](AGENTS.md) for the role model, commit-attribution format, and quality gates. Gonimbus is open source and conforms to the [3 Leaps OSS policies](https://github.com/3leaps/oss-policies): keep the repository free of proprietary or client-specific material (client names, proprietary product or brand names, account or bucket identifiers, internal paths), and keep any sensitive local data outside the repository tree per [ADR-0005](docs/architecture/adr/ADR-0005-sensitive-local-data-policy-conformance.md).

## Architecture

See [docs/architecture.md](docs/architecture.md) for component design:

- Provider Layer
- Match Layer (Cloud-Doublestar)
- Crawl Engine
- Job Manifest schemas
- Output formats

## Fulmen Ecosystem

Gonimbus is part of the [Fulmen ecosystem](https://fulmenhq.dev):

```
Level 4: Production Apps (Gonimbus)
Level 3: DX Tools (goneat, fulward)
Level 2: Templates (forge-workhorse-*)
Level 1: Libraries (gofulmen, pyfulmen)
Level 0: Crucible (SSOT - schemas, standards)
```

### Dependencies

- **gofulmen** - Config path API, three-layer config, schema validation, Crucible shim
- **AWS SDK v2** - Default configuration loading

## License

Licensed under the Apache License 2.0. See [LICENSE](LICENSE) for details.

**Trademarks**: "Fulmen" and "3 Leaps" are trademarks of 3 Leaps, LLC.

---

<div align="center">

**Built with lightning by the 3 Leaps team**
**Part of the [Fulmen Ecosystem](https://fulmenhq.dev)**

</div>
