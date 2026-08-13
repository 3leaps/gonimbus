# Preflight & Probing (Developer/QA Standard)

This document defines the **preflight contract** for Gonimbus multi-step cloud operations (crawl, sharding discovery, enrichment, transfer). The goal is to fail fast on missing permissions **before** expensive enumeration or transfer.

## Motivation

Cloud object stores do not generally provide a reliable, universal "tell me if I can do X" API. The only robust approach is staged probing.

Without preflight:

- A job can spend minutes listing 1M+ objects and only then discover it cannot `HeadObject` (enrichment) or cannot write to the target bucket (transfer).

## Modes

Preflight modes are schema-backed and appear in manifests as `crawl.preflight` (crawl jobs) and `transfer.preflight` (transfer jobs).

- `plan-only`
  - No provider calls.
  - Used for static planning and validation.

- `read-safe`
  - Provider calls allowed.
  - Must not write/delete.
  - Intended to validate: credentials, connectivity, bucket existence, list/head permissions.

- `write-probe`
  - Explicit opt-in minimal side effects.
  - Must isolate probes to a dedicated prefix (default `_gonimbus/probe/`).
  - Intended to validate: target write/delete permissions early.

## Safety Latch (Readonly)

Gonimbus also supports a global safety latch:

- `--readonly` (or `GONIMBUS_READONLY=1`) disables provider-side mutations.
- In readonly mode, Gonimbus refuses `write-probe` preflight and refuses to execute transfer jobs.

This is intended for dogfooding on real buckets and for lower-trust automation/agents.

## Probe Strategy

Provider implementations must choose the lowest-impact probe available.

- `multipart-abort`
  - Preferred when available.
  - Example (S3): create multipart upload then abort.
  - Goal: no durable objects created.

- `put-delete`
  - Fallback.
  - Writes a 0-byte object under probe prefix and deletes it (best effort).
  - Note: versioned buckets may create delete markers; object lock may prevent deletes.

## Output Contract

Preflight results must be emitted as JSONL using `gonimbus.preflight.v1`.

At minimum, records must include capability name, allowed bool, and method string.

## Required Capabilities (v0.1.x)

- Crawl/shard discovery:
  - `source.list`

- Enrichment (HEAD stage):
  - `source.head`

- Transfer (copy/move):
  - `source.list`
  - `source.read`
  - `target.write`
  - `target.head` (dedup)
  - `target.delete` (move and some probes)

## QA / CI Expectations

Tests should assert:

- `plan-only` performs no provider calls.
- `read-safe` performs only non-mutating calls.
- `write-probe` is explicit opt-in and uses only probe prefix.
- Preflight records are emitted before long-running phases.

For S3 cloud integration tests (moto):

- Add allow/deny policy fixtures to ensure preflight properly detects `ACCESS_DENIED`.
- Include at least one failure test where preflight fails before listing.

## Operational Guidance

- Run preflight before sharding discovery or transfer enumeration.
- For write-probe mode, recommend bucket lifecycle rules to expire probe prefixes.
- Never log credentials; probe keys must not include secrets.
