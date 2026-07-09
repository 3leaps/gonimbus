# Release Notes

This file contains release notes for up to the three most recent releases in reverse chronological order. For the complete release history, see the [CHANGELOG](CHANGELOG.md) or the [docs/releases/](docs/releases/) directory.

---

## v0.4.0 (2026-07-09)

**Durable Index Format Is the Default**

v0.4.0 is the index-substrate epoch. `index build` defaults to durable-v2
snapshots (immutable Snappy-Parquet segments + internal manifest + hub markers)
instead of centering the operator workflow on a single SQLite `index.db`.

SQLite remains a first-class compatibility path via `--format sqlite` or dual
`--format both`. Existing `index.db` files are not rewritten. Local query,
enrich-head, stats, most doctor paths, **`index list`**, and **`index gc`**
still need an `index.db` today — durable-only sets are not yet listed by
`list` / `gc`.

### Why this matters

Large indexes hit single-object export ceilings when published as one database
file. Durable packing splits the snapshot into segment objects (plus a small
manifest), so hub export and hydrate scale past the old monolith wall while
keeping row-level LIST-projection parity against SQLite on validated field runs.

### Operator quick path

```bash
# Default durable build
gonimbus index build --job index.yaml

# SQLite compatibility (query / enrich-head / stats / list / gc)
gonimbus index build --job index.yaml --format sqlite

# Dual-format parity + local inventory visibility from one crawl
gonimbus index build --job index.yaml --format both

# Export auto-selects durable when a local durable complete marker exists
gonimbus index export --hub s3://bucket/index-hub/ --index-set idx_...
```

### Also in this cut

- Format-aware hub export/hydrate (`sqlite-v1` / `durable-v2`) with digest
  verification on durable artifacts
- Scoped durable and `--format both` with fail-closed coverage equal to the
  crawl prefix plan
- stderr progress for durable crawl and segmenting tails
- Compare result `projection_semantics` (green parity = LIST fidelity, not
  reflow readiness)

### Boundary framing

Durable-v2 here is a full-fidelity **internal render** for trusted operators and
pipelines — not a reduced-trust third-party publication format. That publication
path is a separate future surface.

### Upgrade

```bash
go install github.com/3leaps/gonimbus/cmd/gonimbus@v0.4.0
```

See [docs/releases/v0.4.0.md](docs/releases/v0.4.0.md) for the complete release
notes.

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

---

## v0.3.6 (2026-07-04)

**Incremental Index Top-Ups and Query-Time Deltas**

v0.3.6 makes recurring index operations cheaper and easier to feed into
downstream workflows. `index build --since <timestamp>|auto` can narrow
date-partitioned listing plans before provider LIST calls, and it reports
whether that reduction actually applied. `index query --since-run <run_id>`
then emits the current objects first seen or meaningfully changed after a
successful run, giving consumers a forward delta without rebuilding or
relisting the source.

### Incremental top-ups with `--since`

Use `--since <timestamp>` when a recurring build has a known lower bound, or
`--since auto` to derive the watermark from the latest successful run in the
same IndexSet.

On date-partitioned scopes, Gonimbus narrows the listing plan before provider
LIST when possible, then applies a last-modified ingest filter. Builds that
cannot safely reduce enumeration still run and report that reduction was
unavailable rather than silently paying full cost without a signal.

### Query-time deltas with `--since-run`

`index query --since-run <run_id>` emits current active rows first seen or
meaningfully changed after a successful boundary run in the same IndexSet.
Output keeps the existing object record shape and adds optional delta fields
for change classification and run identity.

### Upgrade

```bash
go install github.com/3leaps/gonimbus/cmd/gonimbus@v0.3.6
```

See [docs/releases/v0.3.6.md](docs/releases/v0.3.6.md) for the complete release
notes.
