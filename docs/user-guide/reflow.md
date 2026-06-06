# Reflow: Content-Aware Data Reorganization

## The Problem

Large-scale cloud storage grows organically. Systems accumulate millions of objects in structures that made sense when the bucket was small but break down at scale:

- **Date buried under entity**: A telemetry bucket organized as `{device_id}/{date}/data.json` — querying "all data for January 2025" requires listing every device
- **Arrival date vs actual date**: Files land in folders based on _when they arrived_, not _what they represent_ — late transmissions, retries, and network delays cause systematic misfiling
- **Duplicate files**: The same data retransmitted across multiple days, inflating storage and confusing queries
- **Mixed schemas**: Multiple data sources writing to the same bucket with different formats and naming conventions

These aren't hypothetical problems. They're the natural consequence of real-world systems operating at scale — devices with intermittent connectivity, batch jobs that retry on failure, collection systems designed for convenience rather than query patterns.

**The core issue**: The path structure tells you _where_ the data landed, not _what_ the data actually is. And at 10M+ objects, you can't afford to enumerate everything to find what you need.

## The Solution: Index → Probe → Reflow

Gonimbus addresses this with a three-stage pipeline:

```
┌─────────┐     ┌─────────┐     ┌─────────┐     ┌─────────────┐
│  Index   │ ──► │  Probe  │ ──► │ Reflow  │ ──► │ Destination │
│  Build   │     │ Content │     │ Transfer│     │  (correct   │
│          │     │         │     │         │     │  structure) │
└─────────┘     └─────────┘     └─────────┘     └─────────────┘
  Catalog         Extract         Copy with        Organized
  source          routing         key rewrite      for queries
  objects         fields
```

Each stage addresses a specific challenge:

| Stage      | Challenge                                       | What It Does                                         |
| ---------- | ----------------------------------------------- | ---------------------------------------------------- |
| **Index**  | "I can't afford to list 30M objects every time" | Build a searchable catalog with scoped enumeration   |
| **Probe**  | "The path says Dec 3 but the file says Nov 30"  | Extract the _real_ values from file content          |
| **Reflow** | "I need date-first paths, not entity-first"     | Copy to a corrected structure using extracted values |

## When You Need Reflow

Not every data movement requires the full pipeline. Here's how to decide:

| Situation                                           | Solution                                                     |
| --------------------------------------------------- | ------------------------------------------------------------ |
| Simple copy between buckets                         | `gonimbus transfer` with path template                       |
| Reorganize paths using path segments only           | `gonimbus transfer reflow` (no probe needed)                 |
| Path structure is wrong AND path values are correct | `transfer reflow` with `--rewrite-from` / `--rewrite-to`     |
| Path values are wrong — need data from inside files | Full pipeline: `index` → `content probe` → `transfer reflow` |

The full Index → Probe → Reflow pipeline is for the hardest case: **the data you need to route on is inside the files, not in the paths**.

## Stage 1: Index Build

Before probing or reflowing, you need to know _what's there_. For large buckets, a scoped index build avoids enumerating the entire bucket.

### Why Not Just List Everything?

At scale, listing is expensive:

| Objects | Approximate List Time | API Calls (1000/page) |
| ------- | --------------------- | --------------------- |
| 100K    | ~10 seconds           | 100                   |
| 1M      | ~2 minutes            | 1,000                 |
| 10M     | ~20 minutes           | 10,000                |
| 100M    | ~3 hours              | 100,000               |

If you only need December's data across 50 entities, listing all 100M objects wastes 99%+ of the work.

### Scoped Builds

Path scoping tells the index builder exactly which prefixes to enumerate:

```yaml
version: "1.0"

connection:
  provider: s3
  bucket: telemetry-data
  region: us-east-1
  base_uri: s3://telemetry-data/collection/

identity:
  storage_provider: aws_s3
  cloud_provider: aws
  region_kind: aws
  region: us-east-1

build:
  scope:
    type: date_partitions
    discover:
      segments:
        - index: 0 # Entity IDs — discover all
    date:
      segment_index: 1 # Date is second segment
      format: "2006-01-02"
      range:
        after: "2025-12-01"
        before: "2026-01-01"

  match:
    includes:
      - "**/*.json"
```

This builds an index of only December 2025 data. With scoping, a 30M-object bucket might yield 150K indexed objects in 30 seconds instead of 3 minutes.

For more on index strategy and scoped builds, see [Local Index](index.md).

### Query the Index

Once built, queries are instant:

```bash
# Count objects matching a pattern
gonimbus index query 's3://telemetry-data/collection/' \
  --pattern '**/*.json' --count

# Get full JSONL records
gonimbus index query 's3://telemetry-data/collection/' \
  --pattern '**/readings-*.json'
```

## Stage 2: Content Probe

This is the stage that makes reflow "content-aware." The probe reads the first N bytes of each object and extracts structured fields using configurable extractors.

### Why Inspect Content?

Because the path lies.

Consider a telemetry system where devices upload daily readings. Device 42 has connectivity issues — its December 8 reading doesn't arrive until December 15 and lands in the `2025-12-15/` folder. The filename says `readings-20251208.json`, and the JSON inside confirms `"date": "2025-12-08"`. But the path says December 15.

If you reorganize using path-based date alone, you'll file December 8's data under December 15. Content probing catches this:

```yaml
# probe-config.yaml
extract:
  - name: actual_date
    type: json_path
    path: $.date
```

```bash
# Extract the real date from each file
gonimbus content probe --stdin \
  --config probe-config.yaml \
  --emit reflow-input \
  < uris.txt > probe-output.jsonl
```

### Extraction Types

| Type        | Content Format | Config                              | Example                           |
| ----------- | -------------- | ----------------------------------- | --------------------------------- |
| `json_path` | JSON           | `path: $.data.timestamp`            | Extract nested JSON fields        |
| `xml_xpath` | XML            | `xpath: //Header/Date`              | Extract XML elements              |
| `regex`     | Any text       | `pattern: date=(\d{4}-\d{2}-\d{2})` | Pattern match with capture groups |

### Byte Window

The probe reads only the first N bytes (default 4096, max 10MB). For most structured data, the routing fields appear in headers or early in the document. This avoids downloading entire large files just to read a date.

```bash
# Read first 8KB (enough for most XML/JSON headers)
gonimbus content probe --stdin --config probe.yaml --bytes 8192 --emit reflow-input < uris.txt
```

### When Required Fields Sit Deeper

Some structured documents put the routing field well past the prologue — large XML invoices, settlement files, or any format whose target element sits behind a long header. The 10 MB fixed-window cap protects against accidentally downloading whole files, but it also means a probe configured with `fixed_window` may not see the field at all.

For these cases, opt in to bounded incremental reads with `read_strategy.mode: until_resolved`. The probe issues monotonic range reads and stops at the first chunk that resolves every required extractor:

```yaml
# probe-config.yaml — reach deeper into large structured docs
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

Two operator choices matter here:

- **`required: true`** marks the field as load-bearing — if the probe cannot resolve it within `max_bytes`, the record is anomalous.
- **`on_missing`** decides what to do with anomalies. `fail` halts the record (emits `gonimbus.error.v1`, forwards nothing). `quarantine` keeps the pipeline moving: the probe emits a reflow input with `routing_class: "quarantine"`, and reflow files it under a parallel location — see [Quarantine Routing](#quarantine-routing) below.

`until_resolved` currently supports `xml_xpath` and `regex` extractors. `json_path` remains valid under `fixed_window`; combining it with `until_resolved` is rejected at config-load.

Probe output under `until_resolved` gains a `probe` audit block capturing `bytes_read`, the per-extractor resolution state and `bytes_at_resolution`, and `termination_reason` (`all_required_resolved`, `max_bytes_reached`, `stream_exhausted`, or `parse_error`). The audit block is the operator's record of how much was actually read — useful for tuning `max_bytes` and `chunk_bytes` on subsequent runs.

### Bulk Processing

For pipelines with thousands of objects, use concurrency:

```bash
gonimbus content probe --stdin \
  --config probe.yaml \
  --concurrency 32 \
  --emit reflow-input \
  < uris.txt > probe-output.jsonl
```

### Output Modes

| Mode          | Flag                  | Use Case                                     |
| ------------- | --------------------- | -------------------------------------------- |
| Probe results | `--emit probe`        | See extracted values (debugging, validation) |
| Reflow input  | `--emit reflow-input` | Feed directly to `transfer reflow --stdin`   |
| Both          | `--emit both`         | Debugging + pipeline in one pass             |

## Stage 3: Transfer Reflow

The final stage copies objects from source to destination, rewriting keys using template variables that come from either path segments or probe-derived fields.

### Path-Only Reflow

When the path structure is correct but needs reorganization:

```bash
# Source:  collection/{device}/{date}/{filename}
# Target:  by-date/{date}/{device}/{filename}

gonimbus transfer reflow 's3://telemetry/collection/' \
  --dest 's3://telemetry-lake/by-date/' \
  --rewrite-from '{device}/{date}/{file}' \
  --rewrite-to '{date}/{device}/{file}' \
  --dry-run
```

### Content-Aware Reflow

When path values are unreliable and the real value comes from content:

```bash
# Probe output provides {actual_date} from file content
# Source path: collection/{device}/{arrival_date}/{filename}
# Target path: by-date/{actual_date}/{device}/{filename}

gonimbus transfer reflow --stdin \
  --dest 's3://telemetry-lake/by-date/' \
  --rewrite-from 'collection/{device}/{_}/{file}' \
  --rewrite-to '{actual_date}/{device}/{file}' \
  < probe-output.jsonl
```

Note the `{_}` wildcard — it matches the arrival date segment but discards it. The destination uses `{actual_date}` from the probe instead.

### Cross-Account Transfers

When source and destination are in different cloud accounts:

```bash
gonimbus transfer reflow --stdin \
  --dest 's3://dest-bucket/landing/' \
  --src-profile source-account-readonly \
  --dest-profile dest-account-admin \
  --dest-region us-east-1 \
  --rewrite-from '{entity}/{date}/{file}' \
  --rewrite-to '{actual_date}/{entity}/{file}' \
  < probe-output.jsonl
```

Cross-account means no server-side copy optimization — objects are downloaded and re-uploaded. This is inherent to the problem, not a gonimbus limitation.

### Quarantine Routing

When `content probe` emits a record with `routing_class: "quarantine"` (because a required extractor was not resolved within `max_bytes`), `transfer reflow` files it deterministically at:

```
<dest>/<quarantine_prefix>/<source-key>
```

`<quarantine_prefix>` is the relative path configured in the probe config (for example `_unresolved/`). `--rewrite-from` and `--rewrite-to` are bypassed entirely for quarantined records, and the source key is preserved end-to-end. That lets an operator re-probe the original objects later — with a wider `max_bytes`, a different chunk size, or an additional extractor — without losing identity.

A single reflow command can land both normal and quarantined records in one pass: normal records flow through the rewrite template, quarantined records land at the parallel quarantine prefix. Operators stay in the bulk flow without an out-of-band handling step.

Records without `routing_class` (or with `routing_class: "normal"`) continue to follow the normal `--rewrite-from` / `--rewrite-to` path as before — quarantine routing is opt-in at the probe layer.

### Deduplication

Real-world data has duplicates. The same file may appear in multiple folders due to retransmissions or retry logic. When destination objects already exist:

- Default (`--on-collision skip-if-duplicate`): atomically write only if absent, then skip byte-identical duplicates and fail non-identical conflicts
- Fail: `--on-collision fail` — mark any existing-destination collision as failed after duplicate/conflict classification
- Quarantine: `--on-collision quarantine --collision-quarantine-prefix _conflict/` — route non-identical conflicts aside and leave the original intact
- Overwrite: `--on-collision overwrite --overwrite` — replace existing

`--on-collision log` is a deprecated alias for `skip-if-duplicate`. For pipelines where duplicates are expected, `skip-if-duplicate` is the safest default.

### Checkpoint and Resume

Large reflow jobs (100K+ objects) benefit from checkpointing:

```bash
# Start with checkpoint
gonimbus transfer reflow --stdin \
  --dest 's3://dest/landing/' \
  --rewrite-from '...' --rewrite-to '...' \
  --checkpoint ./reflow-state.db \
  < probe-output.jsonl

# If interrupted (network issue, auth expiry, etc.), resume:
gonimbus transfer reflow --stdin \
  --dest 's3://dest/landing/' \
  --checkpoint ./reflow-state.db \
  --resume \
  < probe-output.jsonl
```

The checkpoint database tracks which objects have been successfully copied. Resume skips completed objects and picks up where it left off.

If a fatal interruption is resumable, Gonimbus also emits a
`gonimbus.operation.error.v1` record with `run_id`, `error_class`, progress
counters, and a `gonimbus transfer reflow --resume-run <run_id>` hint. The
`--resume-run` form resumes from checkpointed config and does not require
repeating the original source or destination arguments. Runtime failure stderr
uses the same redacted summary fields and does not print command help.

## End-to-End Example

Here's a complete pipeline for reorganizing activity log data from entity-first to date-first structure.

### Source Structure (Problem)

```
s3://activity-logs/collection/
├── device-001/
│   ├── 2025-12-15/           ← arrival date (not always correct)
│   │   ├── log-20251208.json ← actual date is Dec 8
│   │   ├── log-20251209.json
│   │   └── log-20251210.json
│   └── 2025-12-16/
│       └── log-20251211.json
├── device-002/
│   └── ...
└── device-003/
    └── ...
```

### Target Structure (Goal)

```
s3://activity-lake/by-date/
├── 2025-12-08/
│   └── device-001/
│       └── log-20251208.json
├── 2025-12-09/
│   └── device-001/
│       └── log-20251209.json
└── ...
```

### Pipeline Commands

```bash
# 1. Build scoped index (December 2025 only)
gonimbus index build --job index-dec-2025.yaml

# 2. Query index for target files
gonimbus index query 's3://activity-logs/collection/' \
  --pattern '**/log-*.json' \
  | jq -r '"s3://activity-logs/" + .data.key' \
  > uris.txt

# 3. Probe content for actual date
cat > probe.yaml << 'EOF'
extract:
  - name: actual_date
    type: json_path
    path: $.date
EOF

gonimbus content probe --stdin \
  --config probe.yaml \
  --emit reflow-input \
  --concurrency 16 \
  < uris.txt > probe-output.jsonl

# 4. Reflow to date-first structure
gonimbus transfer reflow --stdin \
  --dest 's3://activity-lake/by-date/' \
  --rewrite-from 'collection/{device}/{_}/{file}' \
  --rewrite-to '{actual_date}/{device}/{file}' \
  --checkpoint ./checkpoint.db \
  --dry-run \
  < probe-output.jsonl

# 5. Review dry-run output, then execute for real
gonimbus transfer reflow --stdin \
  --dest 's3://activity-lake/by-date/' \
  --rewrite-from 'collection/{device}/{_}/{file}' \
  --rewrite-to '{actual_date}/{device}/{file}' \
  --checkpoint ./checkpoint.db \
  < probe-output.jsonl
```

### Validation

After reflow, verify the results:

```bash
# Check destination structure
gonimbus tree 's3://activity-lake/by-date/' --depth 2

# Count objects
gonimbus inspect 's3://activity-lake/by-date/' --count

# Spot-check a specific date
gonimbus inspect 's3://activity-lake/by-date/2025-12-08/' --json | head -5
```

## Common Patterns

### Pattern: Filename-Based Routing

When the correct date is embedded in the filename rather than the path, and is faster than reading file content:

```yaml
# probe.yaml — extract date from filename pattern
extract:
  - name: actual_date
    type: regex
    source: key # Match against the object key, not content
    pattern: 'log-(\d{4})(\d{2})(\d{2})'
    format: "{1}-{2}-{3}" # Reformat capture groups
```

This avoids downloading any content at all — the probe reads the key from the index record.

### Pattern: Tiered Extraction

Try fast extraction first, fall back to slower methods:

1. **Filename regex** — instant, no download
2. **Content probe (small window)** — 4KB read, covers most headers
3. **Content probe (large window)** — full header parsing if needed

### Pattern: Incremental Reflow

For ongoing data acquisition, run reflow on a schedule:

```bash
# Build rolling index (current month)
gonimbus index build --job current-month.yaml

# Query for new files since last run
gonimbus index query 's3://source/' \
  --after "$(cat last-run-date.txt)" \
  | jq -r '"s3://source/" + .data.key' \
  | gonimbus content probe --stdin --config probe.yaml --emit reflow-input \
  | gonimbus transfer reflow --stdin \
      --dest 's3://dest/' \
      --rewrite-from '...' --rewrite-to '...' \
      --on-collision skip-if-duplicate

# Record current timestamp
date -u +%Y-%m-%dT%H:%M:%SZ > last-run-date.txt
```

### Pattern: Local Staging

For validation or when the destination isn't ready, reflow to local filesystem first:

```bash
gonimbus transfer reflow --stdin \
  --dest 'file:///tmp/staging/' \
  --rewrite-from '{entity}/{date}/{file}' \
  --rewrite-to '{actual_date}/{entity}/{file}' \
  < probe-output.jsonl

# Inspect local results before uploading
find /tmp/staging -type f | head -20
```

## Performance Considerations

### Index Build

- Scoped builds are dramatically faster (10-100x) for date-partitioned data
- Build time is dominated by provider API calls, not local processing
- Use `--background` for builds exceeding a few minutes

### Content Probe

- Default 4KB window is sufficient for most XML/JSON headers
- Concurrency (`--concurrency`) is the primary throughput lever
- Each probe requires one GetObject/GetRange call

### Transfer Reflow

- Throughput is network-bound (download + upload per object)
- Parallel workers (`--parallel`, default 16) overlap I/O
- Cross-account transfers can't use server-side copy — plan for 2x bandwidth
- Checkpoint/resume is essential for jobs exceeding 10K objects

### Pipeline Composition

The Unix pipeline pattern (`probe | reflow`) streams records — no intermediate file needed for small/medium jobs:

```bash
gonimbus content probe --stdin --config probe.yaml --emit reflow-input < uris.txt \
  | gonimbus transfer reflow --stdin --dest 's3://dest/' --rewrite-from '...' --rewrite-to '...'
```

For large jobs (100K+ objects), write intermediate JSONL to a file for checkpoint/resume capability.

## See Also

- [Local Index](index.md) — scoped builds, query filters, enterprise indexing
- [Transfer Operations](transfer.md) — manifest-based transfers, preflight, dedup
- [Content Access](streaming.md) — stream commands, content probe, extraction config
