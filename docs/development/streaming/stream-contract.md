# Streaming Contract (v0.1.5)

This document defines the canonical Gonimbus mixed-framing stream contract used by helpers (Go first, then Python and TypeScript).

## Goals

- Streamable: decode without buffering full objects.
- Language-neutral: implementable in Go, Python, Node, and later C#.
- Predictable error handling: detect truncated streams and corrupt framing early.

## Output Model

The output stream is a sequence of:

1. JSONL "control-plane" records (one JSON object per line)
2. raw byte blocks that immediately follow `gonimbus.stream.chunk.v1` header lines

In streaming mode, operational errors are emitted on stdout as JSONL `gonimbus.error.v1` records.
This is intentional: it allows downstream consumers to handle failures without scraping stderr.

Control-plane records use the standard Gonimbus envelope:

- `type` (string)
- `ts` (RFC3339Nano)
- `job_id` (string)
- `provider` (string)
- `data` (object)

## Record Types

### `gonimbus.stream.open.v1`

Declares a new stream.

Required `data` fields:

- `stream_id` (string)
- `uri` (string)

Optional `data` fields:

- `etag` (string)
- `size` (int64)
- `last_modified` (RFC3339)
- `content_type` (string)
- `content_encoding` (string)
- `range` (`{start,end}`)

### `gonimbus.stream.chunk.v1`

Declares a chunk whose raw bytes follow immediately after the JSON line terminator (`\n`).

Required `data` fields:

- `stream_id` (string)
- `seq` (int64, 0-based)
- `nbytes` (int64; exact count of bytes that follow)

Recommended `data` fields:

- `offset` (int64)

### `gonimbus.stream.close.v1`

Declares the end of the stream.

Required `data` fields:

- `stream_id` (string)
- `status` (`success|error|cancelled`)
- `chunks` (int64)
- `bytes` (int64)

## Decoder State Machine

Consumers MUST implement:

- Read one full JSON line.
- If `type != gonimbus.stream.chunk.v1`: process the record, then continue.
- If `type == gonimbus.stream.chunk.v1`:
  - parse `data.nbytes`
  - read exactly `nbytes` raw bytes from the underlying stream
  - resume reading the next JSON line

Truncated raw data MUST be treated as an error (`unexpected EOF`).

## Canonical Go Reference

- Decoder: `pkg/stream/decoder.go`
- Writer: `pkg/stream/writer.go`
