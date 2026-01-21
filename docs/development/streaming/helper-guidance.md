# Helper Implementation Guidance (Python / TypeScript)

This document is a practical checklist for implementing Gonimbus mixed-framing stream helpers in other languages.

## Key Requirements

- Line parsing: JSON records are JSONL; treat each `\n` as the end of a JSON header.
- Chunk parsing: when a `gonimbus.stream.chunk.v1` record is encountered, read exactly `nbytes` raw bytes next.
- No implicit text decoding: chunk bytes are binary and may contain any values.

## Error Handling

- If the underlying stream ends before `nbytes` are read, raise a "truncated stream" error.
- If JSON parsing fails, stop and surface the error; do not attempt recovery.
- In streaming mode, errors can be surfaced as `gonimbus.error.v1` records on stdout. Helpers should treat these as part of the normal stream.
- If the consumer stops reading mid-chunk, helpers should drain the remaining bytes for that chunk if they plan to continue parsing subsequent JSON records.

## Buffering

- Use bounded buffers; do not accumulate the entire stream in memory.
- Prefer reading chunk bytes in moderate blocks (e.g., 64KiB) and yielding/writing them as you go.

## Recommended APIs

Python:

- Provide an iterator/generator yielding events (`record` or `chunk`).
- Provide an adapter that returns a file-like object (`read()`), backed by chunk events.

TypeScript (Node):

- Provide an async iterator yielding events.
- Provide a `Readable` stream adapter for chunk payloads.

## Canonical Go Behavior

Match the Go reference exactly:

- `pkg/stream/decoder.go`: chunk short reads surface as unexpected EOF.
- `pkg/stream/decoder.go`: if a chunk body is not fully consumed, closing it drains remaining bytes.
