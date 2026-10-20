# Release Notes

This file contains release notes for up to the three most recent releases in reverse chronological order. For the complete release history, see the [CHANGELOG](CHANGELOG.md) or the [docs/releases/](docs/releases/) directory.

---

## [0.4.4] - 2026-10-20

**Write receipts, probe outcomes, and listing integrity**

v0.4.4 makes copy and probe pipelines report what actually happened, and makes
index builds refuse a listing that ended early.

- **Terminal write receipts.** Terminal reflow records can carry the
  destination's acknowledgement of each write: payload SHA-256, ETag, revision,
  and known source last-modified. These are transfer receipts, not independent
  read-back verification.
- **Probe summary and exit codes.** `content probe` ends with one
  `gonimbus.content.probe.summary.v1` record. A completed run with object data
  errors exits **60**, previously **32**; provider failures remain 32.
- **Listing integrity.** A page that reports more results without a usable
  continuation token fails the listing, so an index build never seals a
  partial inventory as complete.
- **Durable include refusals** name the alternative (`index
migrate-match-scope`, `build.scope`, or `--format sqlite`), with a worked
  `date_partitions` example for a wildcard in a middle path segment.
- **Release provenance.** A read-only tag workflow builds the release; a
  signed maintainer ceremony publishes it.

### Upgrade

```bash
go install github.com/3leaps/gonimbus/cmd/gonimbus@v0.4.4
```

Scripts that match exit code 32 for a completed probe with object data errors
must handle 60. Upgrade probe producers and reflow consumers together, or filter
the summary record for an older consumer.

See [docs/releases/v0.4.4.md](docs/releases/v0.4.4.md) for the complete
release notes.

---

## [0.4.3] - 2026-09-11

**Reuse a finished durable inventory**

Two operator stories, in this order: reuse a finished durable inventory
instead of listing a very large bucket again; then prove which inventory
you used and when that snapshot actually finished.

v0.4.3 takes exact custody of an already-complete durable run and lets you
query it under current acquire and receipt contracts **without listing the
live object store**. Recrawl remains available; it is not required.
Snapshot completion time is when the snapshot finished, distinct from later
hub commit time.

### Operator quick path

```bash
gonimbus index hub bridge-durable \
  --source-hub-read-handle archive-read \
  --target-hub-publish-handle custody-publish \
  --index-set idx_<64-lowercase-hex> \
  --run-id run_<exact-id> \
  --identity-file /path/to/identity.json \
  --output-format custody-bridge-receipt-jsonl-v1

gonimbus index acquire \
  --hub-read-handle archive-read \
  --index-set idx_<64-lowercase-hex> \
  --run-id run_<exact-id> \
  --dest /var/lib/gonimbus/acquired/that-run

gonimbus index query \
  --snapshot-dir /var/lib/gonimbus/acquired/that-run \
  --count --output-format receipt-jsonl-v1
```

### Also in this cut

- **Query receipts** (`receipt-jsonl-v1`) and pinned durable deltas
  (`--proof-through-run`).
- **Truthful snapshot completion time.**
- **`index doctor --snapshot-dir`** inspects that acquired dest, not ambient
  cache.
- Directory-form coverage prefixes (one terminal `/`) accepted in bridge
  **validation only**.

### Hygiene

- grpc v1.83.2; goneat runner `v0.5.6` / contributor `v0.6.0`.

### Boundary framing

Durable-v2 remains a full-fidelity **internal render** — not a reduced-trust
publication format. Acquire never follows `latest.json`. Do not fall back to
a live bucket walk when a receipt is missing.

### Upgrade

```bash
go install github.com/3leaps/gonimbus/cmd/gonimbus@v0.4.3
```

See [docs/releases/v0.4.3.md](docs/releases/v0.4.3.md) for the complete
release notes.

---

## [0.4.2] - 2026-08-13

**Library Reflow Data Plane + Independent Source/Dest Admission**

v0.4.2 makes the library record-stream engine (`pkg/reflow`) the execution home
for live copies: concurrent workers, memory admission, collision and durability
handling, provenance sidecars, S3-compatible positional sources, and budgeted
partitioned work. The CLI is an adapter. A standing dual-path parity gate holds
engine and CLI-pool behavior together. Stdin `gonimbus.reflow.input.v1` copies
with `s3://` sources and an object-store destination dispatch to the engine
again; file destinations, file-tree sources, GCS positional sources,
`index.object.v1` stdin, quarantine collision mode, `--preserve-mode`, and
non-default `--on-source-failure` still report `execution_path: cli-pool`.

Object-store and heterogeneous copies now admit source-read and dest-write
independently (#191, #192). Dest admission tracks twice adaptive `current`
(source stays at `current`), hard-clamped to a recorded, pool-realizable dest
ceiling of twice the effective ceiling (default multiplier 2). Honesty checks
assert that recorded dest policy. Dest occupancy may exceed `--parallel` up
to that recorded ceiling; `--parallel` remains the operator request. Memory,
file-descriptor, IfAbsent, and throttle clamps still bind.

### Operator quick path

```bash
# Live reflow — engine path for reflow.input.v1 + s3:// source; --parallel is a requested ceiling
gonimbus transfer reflow --stdin --dest 's3://dest-bucket/prefix/' \
  --rewrite-from '{key}' --rewrite-to '{key}' --parallel 32

# Optional memory bound for retry buffers + concurrency sizing
gonimbus transfer reflow --stdin --dest 's3://dest-bucket/prefix/' \
  --memory-budget 8GiB --parallel 32

# Plan / apply stalled managed-index recovery (recover is confirm-gated)
gonimbus index jobs plan-stalled job_...
gonimbus index jobs recover-stalled job_... --confirm
```

### Also in this cut

- **`--memory-budget` and a memory admission ledger** — lowest detected memory
  candidate binds; copy buffers reserve before any provider action.
- **Connection pools follow admitted N** via Stable `ResolveConnectionPool`
  (not a field-throughput claim).
- **Parallel durable crawl journal lanes** (#182; multi-entry crawl plans)
  and index set-authority lease reclaim.
- **`plan-stalled` / `recover-stalled`** (#185) — recover is `--confirm` gated.
- **Atomic file IfAbsent publish** (#190) — no visible truncated final after a
  hard interrupt mid-write. Experimental savepoint elision stays default off.

### Boundary framing

Durable-v2 remains a full-fidelity **internal render** for trusted operators and
pipelines — not a reduced-trust, de-identified, or third-party publication
format. Content digests are **content-integrity / tamper-evidence** checksums,
not statements of author authenticity or provenance. SQLite remains a
first-class compatibility path (`--format sqlite` / `--format both`).

### Upgrade

```bash
go install github.com/3leaps/gonimbus/cmd/gonimbus@v0.4.2
```

No format or schema break; existing durable and SQLite artifacts are read as-is.
See [docs/releases/v0.4.2.md](docs/releases/v0.4.2.md) for the complete release
notes.
