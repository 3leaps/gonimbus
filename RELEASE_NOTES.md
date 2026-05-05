# Release Notes

This file contains release notes for up to the three most recent releases in reverse chronological order. For the complete release history, see the [CHANGELOG](CHANGELOG.md) or the [docs/releases/](docs/releases/) directory.

---

## v0.1.8 (2026-05-05)

**Index Hub + Workspace Pattern + DX Hardening — Final v0.1.x Release**

This release closes out the v0.1.x line by delivering the publishable / consumable index lifecycle: build an index locally, **publish** it to a hub, **consume** it on another host, and manage hub contents over time. Paired with a documented workspace convention and DX hardening, this is the operational toolchain that production data-acquisition pipelines need.

### Index Hub

Publish (`export`) and consume (`hydrate`) index runs against `file://` and `s3://` hubs, with full CRUD:

```bash
# Initialize a hub root
gonimbus index hub init --hub s3://my-hub/

# Publish a run
gonimbus index hub init --hub s3://my-hub/
gonimbus index export --index-set idx_<sha256> --hub s3://my-hub/

# Consume on another host
gonimbus index hydrate --index-set idx_<sha256> --hub s3://my-hub/

# Manage hub contents
gonimbus index hub ls --hub s3://my-hub/
gonimbus index hub show --hub s3://my-hub/ --index-set idx_<sha256>
gonimbus index hub set-latest --hub s3://my-hub/ --index-set idx_<sha256> --run-id run_<id>
gonimbus index hub rm-run --hub s3://my-hub/ --index-set idx_<sha256> --run-id run_<id>
gonimbus index hub gc --hub s3://my-hub/ --keep 5 --json
```

#### Publish Sequence (atomic-ish)

`index.db` → `identity.json` → `complete.json` (commit marker) → `latest.json`. Hydrate verifies SHA-256 + size against the integrity manifest in `complete.json` and rejects uncommitted runs.

#### latest.json Pointer

`latest.json` updates use plain `PutObject` — best-effort, last-writer-wins. CAS / fail-closed semantics (If-Match / If-None-Match, etag plumbing) are tracked for v0.2.x.

### Index Query Flags

```bash
# Explicit index-set selection (resolves prefix or full idx_<64hex>)
gonimbus index query 's3://bucket/prefix/' --index-set idx_da038d8

# Stream results to S3 / file destinations
gonimbus index query 's3://bucket/prefix/' --output 's3://results/query.jsonl'
gonimbus index query 's3://bucket/prefix/' --output 'file:///tmp/query.jsonl'
```

### Workspace Pattern

`workspace.yaml` convention with documented layout, shard strategies, and operational flows:

- Build + publish (crawl → index → export)
- Hydrate + query (consume on remote host)
- Extract + reflow (probe → transfer reflow with content-aware routing)
- Hub maintenance (set-latest, rm-run, gc)

See [`docs/user-guide/workspace.md`](docs/user-guide/workspace.md) for the full pattern.

### DX Hardening

- Pre-push hook scoped to `--new-issues-only --new-issues-base origin/main` so unrelated changes don't pay for legacy lint debt
- Pre-existing high-severity gosec / golangci-lint findings annotated with rationale or fixed
- Guardian browser-intercept hooks removed; the team is on a feature-branch workflow

### Bug Fixes

- `gonimbus index hub gc --json` (without `--dry-run`) silently no-oped deletions; fixed to honor `--dry-run` correctly and emit per-run outcomes in the JSON envelope

### Upgrade Notes

No breaking changes from v0.1.7. Upgrade with:

```bash
go install github.com/3leaps/gonimbus/cmd/gonimbus@v0.1.8
```

### What's Next

v0.1.x is complete. v0.2.x will introduce control-plane capabilities: managed runner, queue consumer, job lifecycle, and conditional-update (CAS / fail-closed) semantics for `latest.json`. GCS provider also lands in v0.2.x.

See [docs/releases/v0.1.8.md](docs/releases/v0.1.8.md) for complete release notes.

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
