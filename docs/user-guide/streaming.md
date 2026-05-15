# Content Access

Gonimbus provides two command families for accessing object content:

- **`stream`** commands - for **delivery** (getting full content to processors)
- **`content`** commands - for **inspection** (examining headers without full download)

## Command Taxonomy

| Command         | Output Format               | Bytes Delivered      | Use Case                           |
| --------------- | --------------------------- | -------------------- | ---------------------------------- |
| `stream head`   | JSONL only                  | None (metadata only) | Routing decisions, size checks     |
| `stream get`    | Mixed framing (JSONL + raw) | Full object          | Content download for processing    |
| `stream put`    | JSONL only                  | Raw stdin writes     | Upload pipeline output             |
| `content head`  | JSONL only (base64)         | First N bytes        | Header inspection, magic bytes     |
| `content probe` | JSONL only                  | First N bytes        | Extract derived fields for routing |

**Key insight**: Use `stream` when you need the actual bytes delivered to a processor. Use `content` when you need to inspect headers to make a decision. Use `content probe` when you need to extract structured fields (dates, IDs, versions) from content for downstream routing.

## When to Use Each Command

| Use Case                       | Command        | Notes                            |
| ------------------------------ | -------------- | -------------------------------- |
| Check metadata before download | `stream head`  | Size, type, custom metadata      |
| Route based on content type    | `stream head`  | Read content_type field          |
| Inspect magic bytes            | `content head` | First 4-16 bytes for file type   |
| Check XML declaration          | `content head` | First 256 bytes for encoding     |
| Process content in pipelines   | `stream get`   | Pipe to decoders/processors      |
| Write pipeline output          | `stream put`   | Raw stdin to one exact object    |
| Verify content integrity       | `stream get`   | Compute hash on streamed content |

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

### `stream put`

Writes stdin to one exact destination object.

```bash
cat output.xml | gonimbus stream put file:///tmp/output.xml
cat output.xml | gonimbus stream put s3://bucket/path/to/output.xml --profile my-profile
```

It can also consume one framed `stream get` object and write it to an explicit
destination:

```bash
gonimbus stream get s3://bucket/path/to/input.xml --profile my-profile \
  | gonimbus stream put --framing jsonl file:///tmp/input.xml
```

`stream put` refuses to replace an existing object by default. For `file://`
and S3-compatible destinations this default uses provider-level conditional
create semantics, so concurrent writers cannot race through a separate
existence check. If a destination provider cannot enforce that precondition,
the command fails closed rather than falling back to a non-atomic write. Use
`--overwrite` when replacement is intentional:

```bash
cat output.xml | gonimbus stream put s3://bucket/path/to/output.xml --overwrite
```

Output is a `gonimbus.stream.put.v1` JSONL record on success:

```json
{
  "type": "gonimbus.stream.put.v1",
  "data": {
    "dest_uri": "s3://bucket/path/to/output.xml",
    "dest_key": "path/to/output.xml",
    "bytes": 3729736,
    "status": "success"
  }
}
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

## Content Inspection Commands

The `content` commands provide JSONL-only output for inspection operations, making them easier to integrate with tools like `jq`.

### `content head`

Reads the first N bytes of an object using HTTP Range requests:

```bash
# Read first 4KB (default)
gonimbus content head s3://bucket/path/to/file.xml --profile my-profile

# Read first 256 bytes (magic bytes, headers)
gonimbus content head s3://bucket/path/to/file.xml --bytes 256 --profile my-profile
```

Output is a single `gonimbus.content.head.v1` JSONL record:

```json
{
  "type": "gonimbus.content.head.v1",
  "ts": "2026-01-25T12:00:00Z",
  "job_id": "...",
  "provider": "s3",
  "data": {
    "uri": "s3://bucket/path/to/file.xml",
    "key": "path/to/file.xml",
    "bytes_requested": 4096,
    "bytes_returned": 4096,
    "content_b64": "PD94bWwgdmVyc2lvbj0iMS4wIi4uLg==",
    "etag": "60eda68512f8238bd2ba9abac0de63d7",
    "size": 3729736,
    "last_modified": "2025-12-15T20:53:44Z",
    "content_type": "application/xml"
  }
}
```

### Content Inspection Use Cases

**File type detection** (magic bytes):

```bash
gonimbus content head s3://bucket/data/file --bytes 16 --profile prod | \
  jq -r '.data.content_b64' | base64 -d | xxd
```

**XML declaration extraction**:

```bash
gonimbus content head s3://bucket/data/doc.xml --bytes 256 --profile prod | \
  jq -r '.data.content_b64' | base64 -d | head -1
```

**Content-aware routing**:

```bash
header=$(gonimbus content head s3://bucket/file --bytes 64 --profile prod | \
  jq -r '.data.content_b64' | base64 -d)

case "$header" in
  *"<?xml"*) echo "XML document" ;;
  *"PK"*) echo "ZIP archive" ;;
  *) echo "Unknown format" ;;
esac
```

### Why Base64?

`content head` encodes bytes as base64 in JSONL for:

- **JSONL-only output**: No mixed framing, easy to parse with standard tools
- **Binary safety**: Works for any content type
- **Small payloads**: First N bytes are typically small (4KB default)

For full content delivery, use `stream get` to avoid base64 overhead.

### `content probe`

Extracts derived fields from object content using configurable extractors. This is the key command for content-aware routing - extracting business dates, schema versions, or other fields embedded in file content.

```bash
# Probe single object
gonimbus content probe s3://bucket/path/to/file.xml \
  --config probe.yaml --profile my-profile

# Bulk probe via stdin
gonimbus content probe --stdin --config probe.yaml --profile my-profile < uris.txt

# Output ready for transfer reflow
gonimbus content probe --stdin --config probe.yaml --emit reflow-input < uris.txt
```

#### Probe Configuration

Create a `probe.yaml` file defining extraction rules:

```yaml
read_strategy:
  mode: fixed_window

extract:
  - name: business_date
    type: xml_xpath
    xpath: //BusinessDate

  - name: schema_version
    type: json_path
    json_path: $.metadata.version

  - name: record_id
    type: regex
    pattern: "ID=([A-Z0-9]+)"
    group: 1
```

#### Extractor Types

| Type        | Use Case               | Example                    |
| ----------- | ---------------------- | -------------------------- |
| `xml_xpath` | XML element extraction | `//BusinessDate`           |
| `json_path` | JSON field extraction  | `$.data.timestamp`         |
| `regex`     | Pattern matching       | `date=(\d{4}-\d{2}-\d{2})` |

#### Read Strategy

`content probe` defaults to `fixed_window`, which reads the first `--bytes`
bytes (default 4096, maximum 10 MB) and applies every extractor to that
buffer. This works well when routing fields sit in the document header —
most JSON metadata and small XML records resolve inside the first 4–16 KB.

For larger structured documents — especially XML where the routing element
may sit kilobytes or megabytes past the prologue — switch to
`until_resolved` to read incrementally only as far as needed:

```yaml
read_strategy:
  mode: until_resolved
  max_bytes: 16MB
  chunk_bytes: 64KB
quarantine_prefix: "_unresolved/"
extract:
  - name: business_date
    type: xml_xpath
    xpath: //BusinessDate
    required: true
    on_missing: quarantine
```

`until_resolved` reads monotonic byte ranges (`[0, chunk_bytes)`,
`[chunk_bytes, 2*chunk_bytes)`, ...) until every required extractor
resolves, `max_bytes` is reached, or the stream is exhausted. The probe
stops at the first chunk that satisfies every required extractor, so
documents whose target field arrives early still complete in one or two
GETs.

MVP streaming extractor support is `xml_xpath` and `regex`; `json_path`
continues to work under `fixed_window` and is rejected under
`until_resolved`.

##### `on_missing`: fail vs. quarantine

When a required extractor does not resolve before the read budget is spent,
`on_missing` controls what the probe does next:

| Setting      | What it does                                                       | When to use                                                                                  |
| ------------ | ------------------------------------------------------------------ | -------------------------------------------------------------------------------------------- |
| `fail`       | Emit `gonimbus.error.v1`; nothing forwarded to reflow              | Strict pipelines where unresolved objects must be reviewed before any reorganization happens |
| `quarantine` | Emit `gonimbus.reflow.input.v1` with `routing_class: "quarantine"` | Bulk pipelines where anomalies should be moved aside deterministically without halting work  |

Quarantined records carry the configured `quarantine_prefix` and any
unresolved required vars set to `"_unresolved"`. `transfer reflow` writes
quarantined objects to `<dest>/<quarantine_prefix>/<source-key>`, bypassing
`--rewrite-from` and `--rewrite-to` entirely — operators get a
deterministic parallel landing zone for anomalies without disrupting the
normal-routing flow.

`quarantine_prefix` is required when any extractor uses `on_missing:
quarantine`. It sits at the **top level** of the probe config (sibling of
`read_strategy` and `extract`), not nested under `read_strategy`. Trailing
slashes are normalized: `"_unresolved"` and `"_unresolved/"` are equivalent
and both emit as `"_unresolved/"`.

##### Probe Audit Block

Under `until_resolved`, every probe output carries a `probe` audit block
recording what was actually read and which extractors resolved:

```json
"probe": {
  "bytes_read": 65536,
  "termination_reason": "all_required_resolved",
  "extractors": [
    {
      "name": "business_date",
      "type": "xml_xpath",
      "resolved": true,
      "required": true,
      "on_missing": "quarantine",
      "bytes_at_resolution": 65536
    }
  ]
}
```

`termination_reason` is one of:

| Value                   | Meaning                                                                          |
| ----------------------- | -------------------------------------------------------------------------------- |
| `all_required_resolved` | Every required extractor matched within the read budget; the probe stopped early |
| `max_bytes_reached`     | The probe exhausted `max_bytes` before every required extractor resolved         |
| `stream_exhausted`      | The object ended before every required extractor resolved                        |
| `parse_error`           | A terminal parse failure prevented further extraction                            |

`bytes_at_resolution` is the cumulative byte offset (from object start) at
the first chunk boundary where the extractor resolved — it is chunk-aligned,
not the byte-precise position of the matching element. A `null` value means
the extractor never resolved.

Two error shapes to be aware of when consuming probe output:

- `probe` block is **`null`** — extraction never started, typically a
  charset or framing problem that prevented the extractor loop from
  running. Look in `error.details` for the underlying cause.
- `probe` block is **present with `resolved: false` entries** — extraction
  ran but did not satisfy the required set within the read budget. Inspect
  `bytes_read` and `termination_reason` to decide whether to widen
  `max_bytes`, adjust the extractor, or accept the quarantine outcome.

#### Output Modes

| Mode           | Description                         | Use Case           |
| -------------- | ----------------------------------- | ------------------ |
| `reflow-input` | Ready for `transfer reflow --stdin` | Pipeline to reflow |
| `probe`        | Raw probe results                   | Analysis/debugging |
| `both`         | Both formats (one record each)      | Auditing           |

#### Probe Output

```json
{
  "type": "gonimbus.content.probe.v1",
  "data": {
    "uri": "s3://bucket/path/to/file.xml",
    "key": "path/to/file.xml",
    "bytes_requested": 4096,
    "bytes_returned": 2069,
    "vars": {
      "business_date": "2025-12-25",
      "schema_version": "2.1"
    },
    "etag": "...",
    "size": 2069,
    "probe": {
      "bytes_read": 2069,
      "termination_reason": "all_required_resolved",
      "extractors": [
        {
          "name": "business_date",
          "type": "xml_xpath",
          "resolved": true,
          "required": true,
          "on_missing": "fail",
          "bytes_at_resolution": 2069
        }
      ]
    }
  }
}
```

`probe.termination_reason` is one of `all_required_resolved`,
`max_bytes_reached`, `stream_exhausted`, or `parse_error`.

### Bulk Input (`--stdin`)

Both `content head` and `content probe` support bulk processing via stdin:

```bash
# List objects, then probe in parallel
gonimbus inspect 's3://bucket/prefix/' --json --profile prod | \
  jq -r 'select(.key?) | "s3://bucket/" + .key' | \
  gonimbus content probe --stdin --config probe.yaml --profile prod

# Or from a file of URIs
gonimbus content probe --stdin --config probe.yaml --profile prod < uris.txt
```

**Performance**: Bulk operations run with configurable parallelism (`--concurrency`, default 16), making them efficient for large-scale inspection.

## Writing a Decoder (Other Languages)

The stream contract is language-neutral. To implement a decoder:

1. Read one line (JSONL record)
2. Parse JSON to get record type
3. If `stream.chunk.v1`, read exactly `nbytes` raw bytes after the newline
4. Repeat until `stream.close.v1` or EOF
5. Handle truncation: if EOF before expected bytes, report error

See `docs/development/streaming/helper-guidance.md` for detailed implementation guidance.
