# Gonimbus

> High-scale cloud object storage inspection and crawl engine

Gonimbus is a Go-first **library + CLI + server** for large-scale inspection and crawl of cloud object storage (100K-1M+ objects). It produces machine-friendly outputs (JSONL baseline) and favors **prefix-first listing** with doublestar matching to stay fast and predictable.

## Modes

- **CLI**: Run validated crawl/inspect jobs from manifests; stream JSONL to stdout/files or index sinks
- **Server**: Long-running runner with streaming results; intended to live near the data and accept remote job submissions
- **Library**: Embeddable components (matcher, crawler, outputs, provider backends) for Go apps

## Quick Start

### Prerequisites

- Go 1.25+ ([install](https://go.dev/doc/install))
- golangci-lint ([install](https://golangci-lint.run/welcome/install/))

### Install

```bash
go install github.com/3leaps/gonimbus/cmd/gonimbus@latest
```

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
- **GCS**: Fast-follow (v0.2.x)

### Authentication

Uses SDK default auth chains - no reinventing the wheel:

- AWS: env vars, shared config/credentials, profiles, SSO, web identity/IRSA
- Enterprise SSO: `--profile` flag with `aws sso login` workflow
- Raw keys supported as explicit fallback (Wasabi, DigitalOcean Spaces)

See [docs/auth/aws-profiles.md](docs/auth/aws-profiles.md) for enterprise authentication patterns.

### Matching

- Doublestar semantics over normalized keys
- Derives strongest possible list prefix per pattern (critical for scale)
- Include/exclude pattern support

### Outputs

Stream-friendly JSONL records:

```json
{"type":"gonimbus.object.v1","ts":"2025-01-15T10:30:00.000Z","job_id":"abc123","provider":"s3","data":{...}}
```

Optional DuckDB sink for local indexing.

## CLI Commands

```bash
gonimbus crawl --job <path>    # Run crawl job (prints JSONL)
gonimbus inspect <uri>         # Quick single-object or prefix inspection
gonimbus doctor                # Environment/auth checks
gonimbus serve                 # Run server mode
gonimbus version               # Version info
gonimbus envinfo               # Dump config/env/SSOT info
gonimbus health                # Self-check
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
```

Copy `.env.example` to `.env` for local development.

## Server Endpoints

When running in server mode:

- `GET /health/*` - Liveness/readiness probes
- `GET /version` - Full version info with SSOT versions
- `GET /metrics` - Prometheus metrics

## Non-Goals

- Mounts, sync engines, FUSE/desktop UX (see [NimbusNest](https://github.com/3leaps/nimbusnest))
- "List everything by default" for broad patterns (scale requires explicit sharding)
- Pinning/offline queues

## Relationship to NimbusNest

NimbusNest is the mount/sync/UX client. Gonimbus provides the crawl/match/auth/backends that NimbusNest can reuse, but scopes stay separate.

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
