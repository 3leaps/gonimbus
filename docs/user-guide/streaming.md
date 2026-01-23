# Content Streaming

The `stream` commands provide structured access to object metadata and content, enabling gonimbus to serve as a data plane for downstream consumers.

## When to Use Streaming

| Use Case                       | Command       | Notes                            |
| ------------------------------ | ------------- | -------------------------------- |
| Check metadata before download | `stream head` | Size, type, custom metadata      |
| Route based on content type    | `stream head` | Read content_type field          |
| Process content in pipelines   | `stream get`  | Pipe to decoders/processors      |
| Verify content integrity       | `stream get`  | Compute hash on streamed content |

## Commands

### `stream head`

Retrieves object metadata without downloading content.

```bash
gonimbus stream head s3://bucket/path/to/file.xml --profile my-profile
```

Output is a single `gonimbus.object.v1` JSONL record:

```json
{
  "type": "gonimbus.object.v1",
  "ts": "2026-01-23T12:28:17.349Z",
  "job_id": "...",
  "provider": "s3",
  "data": {
    "key": "path/to/file.xml",
    "size": 3729736,
    "etag": "60eda68512f8238bd2ba9abac0de63d7",
    "last_modified": "2025-12-15T20:53:44Z",
    "content_type": "application/xml",
    "metadata": { "custom": "user-metadata" }
  }
}
```

### `stream get`

Streams object content with JSONL framing (mixed-framing output).

```bash
gonimbus stream get s3://bucket/path/to/file.xml --profile my-profile
```

Output sequence:

1. `gonimbus.stream.open.v1` - metadata (uri, size, etag, last_modified)
2. `gonimbus.stream.chunk.v1` + raw bytes - repeated for each chunk
3. `gonimbus.stream.close.v1` - completion status

Example output structure:

```
{"type":"gonimbus.stream.open.v1",...,"data":{"stream_id":"...","uri":"s3://...","size":3729736,...}}
{"type":"gonimbus.stream.chunk.v1",...,"data":{"stream_id":"...","seq":0,"nbytes":65536}}
<65536 raw bytes>
{"type":"gonimbus.stream.chunk.v1",...,"data":{"stream_id":"...","seq":1,"nbytes":65536}}
<65536 raw bytes>
...
{"type":"gonimbus.stream.close.v1",...,"data":{"stream_id":"...","status":"success","chunks":58,"bytes":3729736}}
```

## Stream Contract

The streaming output follows a language-neutral contract (ADR-0004):

| Record Type       | Required Fields                  | Notes                                            |
| ----------------- | -------------------------------- | ------------------------------------------------ |
| `stream.open.v1`  | stream_id, uri                   | size, etag, last_modified, content_type optional |
| `stream.chunk.v1` | stream_id, seq, nbytes           | Raw bytes follow immediately after the JSON line |
| `stream.close.v1` | stream_id, status, chunks, bytes | status: success/error/cancelled                  |

### Why Mixed Framing?

- **No base64 overhead**: Raw bytes are emitted directly after chunk headers
- **Incremental decode**: Consumers read one JSON line, then N bytes, repeat
- **Any language**: Contract is implementable in Go, Python, Node, Rust, C#

## Error Handling

Errors are emitted to **stdout** as `gonimbus.error.v1` records:

```json
{
  "type": "gonimbus.error.v1",
  "data": {
    "code": "NOT_FOUND",
    "message": "...",
    "key": "...",
    "details": { "mode": "streaming" }
  }
}
```

This enables consumers to rely on structured output without scraping stderr.

**Exit codes**:

- `0` - Success
- `1` - Error (with error record on stdout)

## Decoder Package (Go)

The `pkg/stream` package provides Go helpers:

```go
import "github.com/3leaps/gonimbus/pkg/stream"

d := stream.NewDecoder(r)
for {
    ev, err := d.Next()
    if err == io.EOF {
        break
    }
    if ev.Kind == stream.EventChunk {
        // ev.Chunk.Body is an io.ReadCloser for the raw bytes
        io.Copy(dst, ev.Chunk.Body)
        ev.Chunk.Body.Close()
    }
}
```

Truncation is detected: if the stream is cut mid-chunk, `Decoder` returns `io.ErrUnexpectedEOF`.

## Use Cases

### Metadata-Based Routing

Check file type before processing:

```bash
gonimbus stream head s3://bucket/data/file.xml --profile prod | jq '.data.content_type'
```

### Content Pipeline Integration

Pipe to downstream processors:

```bash
gonimbus stream get s3://bucket/data/file.xml --profile prod | ./my-decoder | ./processor
```

### Integrity Verification

Compare streamed content against expected hash:

```bash
# Stream, extract raw bytes, compute MD5
gonimbus stream get s3://bucket/file --profile prod | ./extract-stream | md5
```

### Extract and Process XML

Extract specific fields from streamed XML:

```bash
gonimbus stream get s3://bucket/data.xml --profile prod | ./extract-stream | grep -o '<BusinessDate>[^<]*'
```

## Size Validation

Both `stream get` and transfer operations validate that enumerated size matches actual content-length:

```
Enumerated size (from list/index): 3729736
GetObject content-length:          3729736  ✓
```

When sizes don't match (stale index/list):

- Error emitted with `NOT_FOUND` code (stale key semantics)
- No deep pipeline processing - fails early
- Clear error message: `source size mismatch for <key>: expected=N got=M`

## Performance

Stream commands add minimal overhead to raw provider operations:

| Operation   | Overhead         | Notes                     |
| ----------- | ---------------- | ------------------------- |
| stream head | ~1 HEAD request  | Metadata only             |
| stream get  | ~1 HEAD + 1 GET  | Size validation adds HEAD |
| Chunking    | ~200 bytes/chunk | JSONL headers             |

Tested throughput:

| File Size | Chunks | Throughput |
| --------- | ------ | ---------- |
| 466 B     | 1      | instant    |
| 3.3 MB    | 403    | ~1.5 MB/s  |
| 3.7 MB    | ~58    | ~2 MB/s    |

## Writing a Decoder (Other Languages)

The stream contract is language-neutral. To implement a decoder:

1. Read one line (JSONL record)
2. Parse JSON to get record type
3. If `stream.chunk.v1`, read exactly `nbytes` raw bytes after the newline
4. Repeat until `stream.close.v1` or EOF
5. Handle truncation: if EOF before expected bytes, report error

See `docs/development/streaming/helper-guidance.md` for detailed implementation guidance.
