# ADR-0004: Language-Neutral Content Stream Contract (No Rust Bindings)

**Status:** Accepted
**Date:** 2026-01-20
**Decision Makers:** @3leapsdave

## Context

Gonimbus is Go-first, but we want “helper” integrations that work across multiple languages (Go, Python, TypeScript/Node, and later C#) without forcing consumers to embed Go code.

A near-term v0.1.5 requirement is a **streamable content output** that:

- can be decoded incrementally into an `io.Reader` in Go (no full-buffer)
- includes idempotency and revalidation fields (at least `uri`, `etag`, `size`, `last_modified`)
- keeps progress/errors structured and out of the content payload

We discussed whether to plan for Rust helpers.

Rust could be valuable for high-throughput stream processors and predictable memory behavior, but introducing Rust in v0.1.5 would likely imply cross-language bindings and a packaging/CI matrix (build artifacts per platform) that is not justified without a concrete consumer.

## Decision

### 1) Prefer a stable, language-neutral wire contract

Define a **JSONL record contract** for streaming content that is usable from any runtime.

Helpers are implemented as thin adapters over this contract.

### 2) Go is the reference implementation for v0.1.5

Implement the first helper(s) in Go (library-first), targeting `pkg/stream` / `pkg/content`.

Python/Node helpers follow the same contract; they do not require bindings.

### 3) Do not plan Rust bindings for v0.1.5

If Rust becomes necessary later, the preferred path is:

- implement the same JSONL content stream contract in Rust natively, and/or
- ship a small Rust CLI/sidecar that speaks the same contract

We explicitly avoid “bind Rust to Go” or “bind Go to Rust” as the primary strategy unless a concrete in-process embedding requirement forces it.

## Contract: Stream Records (Draft v1)

This contract uses a JSONL envelope for control-plane records and a raw byte framing for the data-plane.

### Control-plane: JSONL envelope

All JSON records use the existing Gonimbus JSONL envelope:

- `type` (string)
- `ts` (RFC3339Nano)
- `job_id` (string)
- `provider` (string)
- `data` (type-specific object)

### Data-plane: raw bytes

Binary payload is emitted as raw bytes, preceded by a JSON header record that declares exactly how many bytes follow.

This means a helper is required for most consumers (Go/Python/Node/C#) to interleave “read one JSON line” with “read N raw bytes” safely.

### `gonimbus.stream.open.v1`

Opens a stream.

Required `data` fields:

- `stream_id` (string; unique within the output stream)
- `uri` (string; canonical object URI, e.g. `s3://bucket/key`)

Optional `data` fields:

- `etag` (string)
- `size` (int64)
- `last_modified` (RFC3339)
- `content_type` (string)
- `content_encoding` (string)
- `range` (object: `{start,end}`) when streaming byte ranges

### `gonimbus.stream.chunk.v1`

Declares a single chunk whose bytes follow immediately after the JSON record's trailing newline.

Required `data` fields:

- `stream_id` (string)
- `seq` (int64; 0-based monotonic sequence)
- `nbytes` (int64; exact byte length of the raw chunk that follows)

Recommended `data` fields:

- `offset` (int64; byte offset from start of object)

Notes:

- Consumers MUST read exactly `nbytes` bytes after parsing the header line, then resume JSONL parsing.
- Chunk size is implementation-defined (recommended default: 64KiB).

### `gonimbus.stream.close.v1`

Closes a stream.

Required `data` fields:

- `stream_id` (string)
- `status` (string; one of: `success`, `error`, `cancelled`)
- `chunks` (int64)
- `bytes` (int64)

Optional `data` fields:

- `duration_ns` (int64)

### Errors and Progress

Progress and errors remain **separate control-plane records**.

- Existing `gonimbus.progress.v1` MAY include `stream_id` as an optional field in `data` when progress refers to a content stream.
- Content-stream-specific failures SHOULD be emitted as `gonimbus.error.v1` with enough context to correlate to a `stream_id` and/or `uri`.

## Consequences

### Positive

- All languages can implement helpers without in-process bindings.
- Streaming decode is straightforward (line-at-a-time JSONL parsing).
- Contract supports later C# naturally (`Stream`) and Node (`Readable`).
- Rust remains an option without committing to a packaging matrix now.

### Negative

- Mixed framing (JSON + raw bytes) requires helpers; not easily consumable with plain `jq` pipelines.
- JSON headers add overhead vs pure binary protocols.

### Mitigations

- Keep chunks reasonably sized; helpers decode incrementally.
- If/when throughput demands it, add a v2 contract (or alternate framing) rather than retrofitting v1.

## Alternatives Considered

### 1) Rust-first library with bindings

Rejected for v0.1.5 due to packaging complexity and no immediate adopter.

### 2) Protobuf/gRPC streaming

Rejected for v0.1.5 because it introduces heavier runtime coupling and does not align with the existing JSONL baseline.

### 3) Mixed framing (JSON headers + raw byte blocks)

Accepted for v0.1.5: avoids base64 overhead and is the most efficient for large payloads, at the cost of requiring a helper to parse the interleaved stream.

## Related

- `pkg/output/record.go` (existing JSONL envelope)
- `docs/architecture.md` (outputs and streaming pipeline)
