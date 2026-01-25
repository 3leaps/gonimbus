# Release Notes

This file contains release notes for up to the three most recent releases in reverse chronological order. For the complete release history, see the [CHANGELOG](CHANGELOG.md) or the [docs/releases/](docs/releases/) directory.

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

---

## v0.1.4 (2026-01-19)

**Path-Scoped Index Builds + Managed Jobs for Enterprise Scale**

This release delivers two major capabilities for enterprise-scale index operations:

1. **Path-Scoped Index Builds** (`build.scope`) - the primary lever for reducing provider listing costs on date-partitioned buckets
2. **Managed Index Build Jobs** - durable job tracking and background execution for long-running builds

Together, these features make Gonimbus practical for multi-hour index builds on huge buckets, with 99%+ reduction in wasted enumeration and full operational visibility.

---

### Path-Scoped Index Builds

#### The Problem

Large enterprise buckets contain years of date-partitioned data. When operators only need the last 30 days, traditional crawl approaches still enumerate the entire history:

- **Without scope**: List 32M objects, match 350K (99% wasted)
- **With scope**: List 185K objects, match 185K (0% wasted)

#### The Solution

Add a `build.scope` block to generate an explicit prefix plan before listing:

```yaml
build:
  scope:
    type: date_partitions
    discover:
      segments:
        - index: 0 # discover store IDs
        - index: 1 # discover device IDs
    date:
      segment_index: 2
      format: "2006-01-02"
      range:
        after: "2025-12-15" # inclusive
        before: "2026-01-01" # exclusive
```

#### Scope Types

| Type              | Use Case                                       |
| ----------------- | ---------------------------------------------- |
| `prefix_list`     | Explicit prefixes when you know what to list   |
| `date_partitions` | Dynamic expansion from date ranges + discovery |
| `union`           | Combine multiple scopes                        |

#### Performance

| Configuration       | Objects Found | Build Time | Improvement     |
| ------------------- | ------------- | ---------- | --------------- |
| 15-store full month | 32M           | ~3 min     | baseline        |
| 15-store scoped 17d | 185K          | ~30 sec    | **99.5% / 10x** |

Key insight: with scope, `objects_found ≈ objects_matched` because you only list what you need.

#### Key Features

- **Dry-run preview**: `gonimbus index build --dry-run` shows prefix plan before execution
- **Guardrails**: Warnings for large prefix expansions
- **Identity isolation**: Scope config hashed into IndexSet identity
- **Soft-delete safety**: Skipped by default for scoped builds (partial coverage)

---

### Managed Index Build Jobs

#### The Problem

Index builds on enterprise buckets can run for hours. Managing them with shell primitives (`&`, `nohup`, `screen`) is brittle for:

- Multi-hour or multi-day jobs
- Multiple concurrent builds (multi-store fanout)
- AI agents that need deterministic state and cancellation

#### The Solution

First-class job management with durable state and background execution:

```bash
# Start a managed background build (returns job id immediately)
gonimbus index build --background --job index.yaml --name nightly-sweep

# Monitor running and recent jobs
gonimbus index jobs list
gonimbus index jobs status <job_id>

# Stream logs from a running job
gonimbus index jobs logs <job_id> --follow

# Safe cancellation
gonimbus index jobs stop <job_id>

# Clean up old job records
gonimbus index jobs gc --max-age 168h
```

#### Key Features

- **Durable job registry**: Jobs persist under app data dir (`jobs/index-build/<job_id>/`)
- **Background execution**: `--background` spawns a managed child process
- **Safe cancellation**: SIGTERM triggers graceful context cancellation; SIGKILL fallback
- **Log capture**: stdout/stderr streamed to per-job files
- **Deduplication**: `--dedupe` prevents duplicate running jobs for the same manifest
- **JSON output**: All job commands support `--json` for machine-friendly output

#### Job States

`queued` → `running` → `success` | `partial` | `failed`

Jobs can also be `stopping` (graceful shutdown in progress) or `stopped` (cancelled).

---

### Documentation

- User guide: [docs/user-guide/index.md](docs/user-guide/index.md)
- Architecture: [docs/architecture/indexing.md](docs/architecture/indexing.md)
- See [docs/releases/v0.1.4.md](docs/releases/v0.1.4.md) for complete release notes
