# Release Notes

This file contains release notes for up to the three most recent releases in reverse chronological order. For the complete release history, see the [CHANGELOG](CHANGELOG.md) or the [docs/releases/](docs/releases/) directory.

---

## [0.4.5] - 2026-10-21

**Go 1.26.10 minimum, quieter stderr, single-source release tag**

v0.4.5 raises the minimum Go toolchain, keeps stderr for real problems, and
tightens how the release tooling selects a tag.

- **Go 1.26.10 minimum.** gonimbus builds with Go toolchain `go1.26.10`, to
  ensure secure operation.
- **Quieter stderr.** Commands against AWS S3 no longer print an SDK checksum
  debug line, and runtime failures no longer append the usage block. Usage
  still prints for invocation errors.
- **Release tag from one variable.** Release tooling reads the tag only from
  `GONIMBUS_RELEASE_TAG` and checks it against `VERSION` before any
  tag-consuming step.
- **Docs.** What `dest_etag` holds per destination provider, and the shape of
  the `index build --json` receipt.

No commands, flags, record schemas or exit codes change.

### Upgrade

```bash
go install github.com/3leaps/gonimbus/cmd/gonimbus@v0.4.5
```

See [docs/releases/v0.4.5.md](docs/releases/v0.4.5.md) for the complete
release notes.

---

## [0.4.4] - 2026-10-20

**Write receipts, probe outcomes, and listing integrity**

v0.4.4 makes copy and probe pipelines report what each write acknowledged and
how each probe ended, and makes index builds refuse a listing that ended early.

- **Terminal write receipts.** Terminal reflow records can carry the
  destination's acknowledgement of each write: payload SHA-256, ETag, revision,
  and known source last-modified. These are transfer receipts, not independent
  read-back verification.
- **Probe summary and exit codes.** `content probe` ends with one
  `gonimbus.content.probe.summary.v1` record. A completed run with object data
  errors exits **60**, previously **32**; provider failures remain 32.
- **Listing integrity.** A page that reports more results without a usable
  continuation token fails the listing, so an index build fails before
  sealing instead of attesting a partial listing.
- **Durable include refusals** name the alternative
  (`index migrate-match-scope`, `build.scope`, or `--format sqlite`), with a
  worked `date_partitions` example for a wildcard in a middle path segment.
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
