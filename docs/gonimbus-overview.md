# Gonimbus Overview (Vision Draft)

## What It Is

Gonimbus is a Go-first **library + CLI + optional server** for large-scale inspection and crawl of cloud object storage (100K–1M+ objects). It produces machine-friendly outputs (JSONL baseline) and favors **prefix-first listing** with doublestar matching to stay fast and predictable.

## Modes

- **CLI**: run validated crawl/inspect jobs from manifests; stream JSONL to stdout/files or index sinks.
- **Server**: long-running runner with streaming results; intended to live near the data and accept remote job submissions.
- **Library**: embeddable components (matcher, crawler, outputs, provider backends) for Go apps and future thin clients.

## Core Capabilities

- **Providers**: S3/S3-compatible first; GCS fast-follow.
- **Auth**: provider-default chains as first-class citizens (profiles/roles/SSO for AWS; ADC/device flow for GCP). Raw keys remain an explicit fallback.
- **Matching**: doublestar semantics over normalized keys; derive the strongest list prefix per pattern and enforce sharding when patterns have no prefix.
- **Crawl Engine**: bounded pipelines (lister → matcher → optional enricher → writer) with backpressure, rate limiting, and cancellation.
- **Outputs**: JSONL envelopes (object/error/progress). Includes a DuckDB sink for local indexing; JSONL remains the contract.
- **Doctoring**: environment/auth checks plus `whoami`-style visibility for active identities.

## Non-Goals

- No mounts, sync engines, FUSE/desktop UX, or pinning/offline queues.
- No "list everything by default" for broad patterns; scale requires explicit sharding or inventory ingestion.

## Near-Term Roadmap (v0.1.x)

- S3-compatible support with access key/secret; AWS profiles/assume-role; GCP ADC/device flow.
- Prefix-first crawler with doublestar matcher and JSONL writer; DuckDB sink.
- Server skeleton with streaming responses and local endpoints exercised via forge-workhorse-groningen.
- Schema-validated job manifests and outputs (Crucible-aligned).
