# Gonimbus Architecture (Draft)

This document describes the intended internal architecture for `gonimbus` as a hybrid **library + CLI + optional server** focused on **inspection/crawl/inventory** of cloud object storage.

## High-Level Components

### 1) Provider Layer

Provider clients implement a small, stable surface area:

- `List(prefix, continuationToken)` → page of object summaries
- `Head(key)` / `GetMetadata(key)` (optional enrichment)

Provider implementations must use the provider SDK default auth chains.

### 2) Match Layer (Cloud-Doublestar)

Responsible for:

- normalizing object keys into a stable “path-like” form (always `/`)
- evaluating doublestar patterns against keys
- deriving the **strongest possible list prefix** for each include pattern

Prefix derivation is core to scale:

- list by prefix on the provider
- match patterns client-side

### 3) Crawl Engine

The engine streams results through a bounded pipeline:

- lister: pages through provider list results per prefix
- matcher: filters by include/exclude
- enricher (optional): performs `Head`/tag calls with a bounded worker pool
- writer: emits JSONL records (single writer goroutine to preserve record atomicity)

Key concerns:

- backpressure (bounded channels)
- rate limiting (token bucket per provider/account)
- cancellation (context propagation)
- stable correlation IDs for errors and server streaming

### 4) Job Manifest (Schemas-First)

Jobs must be representable as a schema-validated manifest.

At minimum:

- `connection`: provider + scope (e.g., S3 bucket, optional endpoint for S3-compatible)
- `match`: include/exclude patterns, includeHidden
- `crawl`: enrichment level, concurrency, rate limits, retries
- `output`: JSONL destination (stdout/file), plus optional compression (future)

### 5) Outputs (JSONL Baseline)

Outputs are stream-friendly and machine-consumable.

Recommended record envelope fields (draft):

- `type` (e.g., `gonimbus.object.v1`, `gonimbus.error.v1`, `gonimbus.progress.v1`)
- `ts` (RFC3339Nano)
- `job_id`
- `provider`
- `data` (type-specific payload)

## Auth Strategy (Must-Have)

### AWS / S3

Use AWS SDK v2 default configuration loading so we get (without reinventing):

- env vars (`AWS_*`)
- shared config/credentials files (`~/.aws/config`, `~/.aws/credentials`)
- profiles and assume-role chains
- SSO and cached tokens
- web identity / IRSA / workload identity (where applicable)

Raw access keys remain supported as an explicit option (useful for Wasabi/DO), but should never be the only path.

### GCP / GCS (Fast Follow)

Use Application Default Credentials patterns:

- `gcloud auth application-default`
- service account JSON (explicit path when required)
- workload identity / metadata server when running in GCP

## CLI Surface (Draft)

- `gonimbus crawl --job <path>`: run a crawl job (prints JSONL)
- `gonimbus inspect <uri>`: quick single-object or prefix inspection (prints JSON)
- `gonimbus doctor`: environment/auth checks; prints actionable guidance
- `gonimbus serve`: run server mode for long-running jobs and streaming results

## Server Mode (Draft)

Server mode is optional but planned. It should:

- accept job submissions (validated)
- execute jobs with resumability policies (later)
- stream JSONL back to clients
- expose basic endpoints: `/health/*`, `/version`, `/jobs`, `/jobs/{id}/stream`

Initial security posture can be “local single-user”; enterprise auth can be layered later.

## Package Structure (Draft)

Conceptual module map (actual layout follows workhorse forge structure):

- `pkg/provider/` - Provider interface and implementations
  - `pkg/provider/s3/` - S3/S3-compatible provider
  - `pkg/provider/gcs/` - GCS provider (v0.2.x)
- `pkg/match/` - Doublestar matcher + prefix derivation
- `pkg/crawler/` - Crawl engine with bounded pipeline
- `pkg/output/` - JSONL writer + record types
- `internal/cmd/` - CLI commands
- `internal/server/` - HTTP server implementation

## Scale Notes

- Never default to `ListAllObjects` for broad patterns; require explicit opt-in or enforce sharding.
- Prefer prefix-first listing; dedupe prefixes across patterns.
- Keep memory bounded: stream, don't accumulate 1M objects unless explicitly requested.

## Boundaries

For explicit non-goals and scope boundaries, see [non-goals.md](non-goals.md).

Key boundary: if a requirement is about "local filesystem semantics" (mounts, sync, caching), it belongs in NimbusNest or another app, not in gonimbus core.
