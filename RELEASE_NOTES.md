# Release Notes

This file contains release notes for up to the three most recent releases in reverse chronological order. For the complete release history, see the [CHANGELOG](CHANGELOG.md) or the [docs/releases/](docs/releases/) directory.

---

## v0.4.1 (2026-07-18)

**SQLite Independence + Streaming Durable Publication**

v0.4.0 made the durable index the default build format. v0.4.1 completes the
operator story: a durable-only default build no longer needs an `index.db` for
everyday local work, and `index build` publishes durable snapshots through a
streaming path that never materializes the full row set in memory.

SQLite remains a first-class compatibility path (`--format sqlite` /
`--format both`) for `query --since-run`, `stats --prefixes`, and full
`--resume-run` recovery.

### SQLite-independent local surfaces

`query`, `list`, `stats`, `doctor`, and `enrich-with-head` now read the durable
substrate natively (no `index.db`, no ephemeral SQLite bridge). A machine-stable
`index build --json` receipt (`gonimbus.index.build_result.v1`) and a pinned
durable query (`--index-set` + `--run-id`) let automation hand off the exact
committed snapshot without list heuristics.

```bash
# Default durable build — now queryable and diagnosable natively
gonimbus index build --job index.yaml

# Machine-stable receipt for handoff
gonimbus index build --job index.yaml --json

# Query the durable snapshot directly (streaming verified JSONL)
gonimbus index query --index-set idx_... --prefix data/

# Dry-run whole-set reclamation plan
gonimbus index gc --dry-run --keep-last 3 --json
```

### Also in this cut

- **Streaming durable publication** with operator-tunable capacity budgets
  (`--spill-workspace-max` / `--spill-record-max` / `--spill-root`; 16 GiB
  workspace / 16 MiB record defaults; flag > env > config > default). Invalid
  budgets refuse before any side effect; exhaustion refuses fail-closed with no
  `latest` advance. Segment packing stays 500k rows/segment (engine lever, not a
  setting). Operator guide:
  [durable-index capacity budgets](docs/user-guide/durable-index.md#streaming-capacity-budgets-operator-tunable).
- **Managed background builds** (`index build --background`) with dedup, leases,
  recovery, and terminal receipts.
- **Whole-set GC** — `index gc --dry-run` (`--max-age` / `--keep-last` /
  `--json`), leased execution, canonical SQLite authority.
- **`index migrate-match-scope`** — fail-closed migration of prefix-shaped
  `match.includes` to explicit `build.scope`.
- **`transfer reflow` stdin `--parallel` restored** (a v0.3.5–v0.4.0
  regression).

### Boundary framing

Durable-v2 remains a full-fidelity **internal render** for trusted operators and
pipelines — not a reduced-trust, de-identified, or third-party publication
format. Content digests are **content-integrity / tamper-evidence** checksums,
not statements of author authenticity or provenance.

### Upgrade

```bash
go install github.com/3leaps/gonimbus/cmd/gonimbus@v0.4.1
```

No format or schema break; existing durable and SQLite artifacts are read as-is.
See [docs/releases/v0.4.1.md](docs/releases/v0.4.1.md) for the complete release
notes.

---

## v0.4.0 (2026-07-09)

**Durable Index Format Is the Default**

v0.4.0 is the index-substrate epoch. `index build` defaults to durable-v2
snapshots (immutable Snappy-Parquet segments + internal manifest + hub markers)
instead of centering the operator workflow on a single SQLite `index.db`.

SQLite remains a first-class compatibility path via `--format sqlite` or dual
`--format both`. Existing `index.db` files are not rewritten. Format-aware local
consumers include `query`, `list`, `stats`, `doctor`, and `enrich-with-head`.
**`index gc`** still needs an `index.db` today; other SQLite-only surfaces are
narrowed in the durable-index operator guide (`query --since-run`,
`stats --prefixes`, full `--resume-run` recovery).

### Why this matters

Large indexes hit single-object export ceilings when published as one database
file. Durable packing splits the snapshot into segment objects (plus a small
manifest), so hub export and hydrate scale past the old monolith wall while
keeping row-level LIST-projection parity against SQLite on validated field runs.
The largest individual hub PUT becomes a segment, not the whole inventory.

### Operator quick path

```bash
# Default durable build
gonimbus index build --job index.yaml

# SQLite when you still need gc / --since-run / full --resume-run recovery
gonimbus index build --job index.yaml --format sqlite

# Dual-format parity + SQLite-only consumers from one crawl
gonimbus index build --job index.yaml --format both

# Export auto-selects durable when a local durable complete marker exists
gonimbus index export --hub s3://bucket/index-hub/ --index-set idx_...

# Temporal compare between two durable snapshots
gonimbus index compare durable-delta \
  --before-manifest /path/to/before/manifest.json \
  --before-segments /path/to/before/segments \
  --after-manifest /path/to/after/manifest.json \
  --after-segments /path/to/after/segments
```

### Also in this cut

- Format-aware hub export/hydrate (`sqlite-v1` / `durable-v2`) with digest
  verification on durable artifacts
- Scoped durable and `--format both` with fail-closed coverage equal to the
  crawl prefix plan
- stderr progress for durable crawl and segmenting tails
- Compare result `projection_semantics` (green parity = LIST fidelity, not
  reflow readiness)
- Default segment packing of 500k rows (engine lever; not operator-configurable
  in this cut)

### Boundary framing

Durable-v2 here is a full-fidelity **internal render** for trusted operators and
pipelines — not a reduced-trust third-party publication format. A durable hub
export is not a disclosure-controlled share format.

### Upgrade

```bash
go install github.com/3leaps/gonimbus/cmd/gonimbus@v0.4.0
```

See [docs/releases/v0.4.0.md](docs/releases/v0.4.0.md) for the complete release
notes and [docs/user-guide/durable-index.md](docs/user-guide/durable-index.md)
for the operator migration map.

---

## v0.3.7 (2026-07-05)

**Operational Data Root Overrides**

v0.3.7 gives deployments a single supported data-root control for local
operational state. `GONIMBUS_DATA_DIR` sets a one-process root, `data_root` sets
the same root in config, and Gonimbus now routes index databases, index-build job
records, server job defaults, and operation checkpoints through the shared
resolver.

`GONIMBUS_DATA_ROOT` and `data_dir` remain supported aliases, but new automation
should prefer `GONIMBUS_DATA_DIR` and `data_root`.

### Relocating operational state

Use an environment override for a single process:

```bash
GONIMBUS_DATA_DIR=/mnt/gonimbus-data gonimbus index build --job index.yaml
```

Use config for a persistent relocation:

```yaml
data_root: /mnt/gonimbus-data
```

`gonimbus doctor` now reports the resolved data root, the source that selected
it, and whether Gonimbus can write or create it.

### Guardrails

Resolved data roots inside a git working tree are rejected before state is
created, regardless of source, including symlink-resolved paths. App-data
directories and runtime state files are created with owner-only permissions
where Gonimbus controls them.

Changing the root does not migrate existing state. Move existing indexes, job
records, and checkpoints deliberately, then verify the new root with
`gonimbus doctor` before resuming production jobs.

### Upgrade

```bash
go install github.com/3leaps/gonimbus/cmd/gonimbus@v0.3.7
```

Existing command flags, manifest formats, and default platform app-data behavior
remain compatible with v0.3.6.

See [docs/releases/v0.3.7.md](docs/releases/v0.3.7.md) for the complete release
notes.
