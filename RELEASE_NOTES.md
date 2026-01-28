# Release Notes

This file contains release notes for up to the three most recent releases in reverse chronological order. For the complete release history, see the [CHANGELOG](CHANGELOG.md) or the [docs/releases/](docs/releases/) directory.

---

## v0.1.7 (2026-01-28)

**Transfer Reflow with Content-Aware Routing**

This release delivers the complete transfer reflow pipeline, enabling content-aware data reorganization across cloud storage providers and local filesystems.

### Transfer Reflow Command

Copy objects while rewriting keys based on templates:

```bash
# Path-based reflow
gonimbus transfer reflow 's3://source/prefix/' \
  --dest 's3://dest/base/' \
  --rewrite-from '{program}/{site}/{date}/{file}' \
  --rewrite-to '{date}/{program}/{site}/{file}'

# Content-aware reflow (with probe-derived variables)
gonimbus transfer reflow --stdin \
  --dest 's3://dest/base/' \
  --rewrite-from '{_}/{store}/{device}/{date}/{file}' \
  --rewrite-to '{business_date}/{store}/{file}' < probe.jsonl

# Bucket to local filesystem
gonimbus transfer reflow --stdin \
  --dest 'file:///tmp/output/' \
  --rewrite-from '...' \
  --rewrite-to '...' < probe.jsonl
```

#### Features

- Template variables from path segments or probe-derived fields
- Parallel copy with configurable workers (`--parallel`)
- Checkpoint/resume for large jobs (`--checkpoint`, `--resume`)
- Collision handling (`--on-collision log|fail|overwrite`)
- Dry-run mode (`--dry-run`)

### Content Probe Command

Extract derived fields from object content:

```bash
# Probe single object
gonimbus content probe 's3://bucket/file.xml' --config probe.yaml

# Bulk probe via stdin
gonimbus content probe --stdin --config probe.yaml < uris.txt
```

#### probe.yaml Example

```yaml
extract:
  - name: business_date
    type: xml_xpath
    xpath: //BusinessDate
  - name: schema_version
    type: json_path
    path: $.metadata.version
```

#### Extractor Types

| Type        | Use Case               | Example                    |
| ----------- | ---------------------- | -------------------------- |
| `xml_xpath` | XML element extraction | `//BusinessDate`           |
| `regex`     | Pattern matching       | `date=(\d{4}-\d{2}-\d{2})` |
| `json_path` | JSON field extraction  | `$.data.timestamp`         |

### file:// Provider

Transfer reflow now supports local filesystem destinations:

```bash
gonimbus transfer reflow --stdin \
  --dest 'file:///tmp/reflow-out/' \
  --src-profile my-aws-profile \
  --rewrite-from '...' \
  --rewrite-to '...' < probe.jsonl
```

### Collision Handling

| Mode                           | Behavior                                  |
| ------------------------------ | ----------------------------------------- |
| `--on-collision log` (default) | Log conflict, fail operation              |
| `--on-collision fail`          | Fail immediately on first conflict        |
| `--on-collision overwrite`     | Replace existing (requires `--overwrite`) |

### Documentation

- See [docs/releases/v0.1.7.md](docs/releases/v0.1.7.md) for complete release notes

---

## v0.1.6 (2026-01-25)

**Content Inspection with Range Requests**

This release introduces content inspection commands that read object headers without downloading entire files, using HTTP Range requests for efficiency.

### Content Inspection Commands

New `content` subcommands provide JSONL-only inspection operations:

```bash
# Read the first 4KB of an object (default)
gonimbus content head s3://bucket/path/to/file.xml --profile my-profile

# Read the first 256 bytes (magic bytes, file headers)
gonimbus content head s3://bucket/path/to/file.xml --bytes 256 --profile my-profile
```

#### Output Format

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

### stream vs content Commands

| Command        | Output                            | Use Case                       |
| -------------- | --------------------------------- | ------------------------------ |
| `stream head`  | JSONL (metadata only)             | Routing decisions, size checks |
| `stream get`   | Mixed framing (JSONL + raw bytes) | Full content download          |
| `content head` | JSONL (base64 content)            | Header inspection, magic bytes |

**Key difference**: `content` commands are JSONL-only with no mixed framing, making them easier to integrate with tools like `jq`.

### Provider Range Requests

The S3 provider now supports HTTP Range requests via the `ObjectRanger` interface:

- **Efficient partial reads**: Only downloads requested bytes
- **Automatic fallback**: Falls back to GetObject if provider doesn't support ranges
- **Standard semantics**: Uses HTTP Range header with inclusive byte offsets

### Use Cases

#### File Type Detection

Inspect magic bytes without downloading entire files:

```bash
# Read first 16 bytes for magic number detection
gonimbus content head s3://bucket/data/file --bytes 16 --profile prod | \
  jq -r '.data.content_b64' | base64 -d | xxd
```

#### XML Declaration Extraction

Extract XML version and encoding from document headers:

```bash
# Read first 256 bytes for XML declaration
gonimbus content head s3://bucket/data/doc.xml --bytes 256 --profile prod | \
  jq -r '.data.content_b64' | base64 -d | head -1
```

#### Content-Aware Routing

Make routing decisions based on file headers without full download:

```bash
# Check if file starts with expected header
header=$(gonimbus content head s3://bucket/file --bytes 64 --profile prod | \
  jq -r '.data.content_b64' | base64 -d)
if [[ "$header" == *"expected-pattern"* ]]; then
  # Route to processor A
fi
```

### Documentation

- See [docs/releases/v0.1.6.md](docs/releases/v0.1.6.md) for complete release notes

---

## v0.1.5 (2026-01-23)

**Content Streaming + validate=size for Consumer Integration**

This release introduces content streaming commands and validation, enabling Gonimbus to serve as a data plane for downstream consumers (Go, Python, Node) that need to process object content without managing provider SDKs directly.

### Content Streaming Commands

New `stream` subcommands provide structured access to object metadata and content:

```bash
# Get object metadata (JSONL output)
gonimbus stream head s3://bucket/key --profile my-profile

# Stream object content (mixed JSONL + raw bytes)
gonimbus stream get s3://bucket/key --profile my-profile
```

#### Stream Contract

The streaming output uses a mixed-framing format (ADR-0004):

| Record Type                | Purpose                                               |
| -------------------------- | ----------------------------------------------------- |
| `gonimbus.stream.open.v1`  | Stream metadata (uri, size, etag, last_modified)      |
| `gonimbus.stream.chunk.v1` | Chunk header (seq, nbytes) + raw bytes                |
| `gonimbus.stream.close.v1` | Completion status (success/error, total chunks/bytes) |

Errors are emitted to **stdout** as `gonimbus.error.v1` records (streaming mode contract), enabling consumers to rely on structured output without scraping stderr.

#### Decoder Package

The `pkg/stream` package provides Go helpers for producing and consuming streams:

- `Writer`: Produces mixed-framing output
- `Decoder`: Parses streams with truncation detection (`io.ErrUnexpectedEOF`)
- Byte-exact reconstruction verified via MD5/SHA256 round-trip

### validate=size (Stale Index Mitigation)

Both `stream get` and transfer operations now validate that enumerated size matches GetObject content-length:

- Catches stale index/list metadata before deep pipeline processing
- Size mismatch mapped to `NOT_FOUND` error code (stale key semantics)
- Fails early, avoiding wasted buffering and retries

### Documentation

- ADR-0004: Language-neutral content stream contract
- Streaming contract spec and helper guidance (`docs/development/streaming/`)
- See [docs/releases/v0.1.5.md](docs/releases/v0.1.5.md) for complete release notes
