# Durable Index Format (v0.4.0 Default)

v0.4.0 makes the **durable index format** the default artifact of
`gonimbus index build`. This page is the operator-facing map of what that
means, how to keep SQLite workflows during the transition, and how to validate
a migration with dual-format parity.

For the full index command surface, see [Local Index](index.md). For LIST vs
ingest mental model, see [Index Build Mental Model](index-build-mental-model.md).
Architecture notes for the parity projection live in
[Index Compare Projection v1](../architecture/index-compare-projection-v1.md).

## Why durable exists

A classic SQLite `index.db` is convenient for local query, but at multi-million
object scale it becomes a **single multi-gigabyte file**. Publishing that file
to an object-storage hub hits provider **single-object PUT ceilings**, and
hydrate has to move the entire monolith before anything is usable.

Durable-v2 replaces the monolith export shape with:

| Piece                             | Role                                            |
| --------------------------------- | ----------------------------------------------- |
| Crawl journals                    | Sealed observation log for the run              |
| Immutable Snappy-Parquet segments | Row data packed for hub-friendly object sizes   |
| Internal manifest                 | Segment inventory, digests, reachability roots  |
| Complete / latest markers         | Hub commit points (`sqlite-v1` or `durable-v2`) |

The largest individual hub PUT becomes a **segment** (typically tens of
megabytes under default packing), not the whole inventory database. Field
validation at multi-million object scale showed LIST-projection parity against
SQLite and faithful coverage for scoped dual-format builds.

## What changes in the operator workflow

```bash
# Default: durable snapshot (no index.db)
gonimbus index build --job index.yaml

# Still need local query / enrich-head / stats / list / gc?
gonimbus index build --job index.yaml --format sqlite
# or dual-format for migration confidence + SQLite consumers
gonimbus index build --job index.yaml --format both
```

| Need                                                            | Format choice                                        |
| --------------------------------------------------------------- | ---------------------------------------------------- |
| Hub-scale export/hydrate, default new builds                    | `durable` (default)                                  |
| Local `index query` (native segment scan; no `index.db` needed) | `durable`, `sqlite`, or `both`                       |
| Local `enrich-with-head`, `stats`, most `doctor`                | `sqlite` or `both`                                   |
| Local inventory (`index list` / `index gc`)                     | `sqlite` or `both` (dirs with `index.db` only today) |
| Migration confidence (one crawl, two artifacts + parity report) | `both`                                               |

**Existing `index.db` files are not rewritten or invalidated.** SQLite remains a
first-class compatibility path. Resume printed as
`gonimbus index build --resume-run <run_id>` still works under the durable
default when `--format` is omitted (SQLite checkpoint lifecycle).

### Artifact layout (conceptual)

```text
indexes/idx_<hash>/
  identity.json              # always for the index set
  index.db                   # only when format is sqlite or both

cache/segments/<index_set_id>/runs/<run_id>/
  journals/ ...
  segments/ ...
  manifest.json
  complete.json              # (or complete marker for the run)
```

Exact on-disk paths follow the app data root (`GONIMBUS_DATA_DIR` / `data_root`);
see [Operational Data Root](index.md#operational-data-root) in Local Index.

### Progress on long durable builds

Multi-minute durable builds emit best-effort `progress:` lines on **stderr**:

- Crawl cadence: listing progress by prefix / object counts
- Publish tail: `phase=segmenting segment=k/N rows=…`

Progress is observational only — it does not change artifact bytes. A quiet
terminal with no progress for a multi-minute durable build is unexpected on
v0.4.0; check that you are running the new binary.

### Segment packing

Default target packing is **500,000 rows per segment**. That target is an
engine packing lever, **not operator-facing configuration** in this cut. Do not
plan automation around a custom segment-size flag.

## Dual-format parity

`--format both` runs **one crawl** into both SQLite and durable writers, then
emits a machine-readable `gonimbus.index.compare_result.v1` report (including a
`projection_semantics` block).

```bash
gonimbus index build --job index.yaml --format both
```

Use this as the **migration confidence gate** before switching consumers off
SQLite for new hub traffic.

### What green parity certifies

A green result (`parity_passed: true`) means the SQLite and durable artifacts
agree on the **LIST-derived projection** for the same crawl:

- `rel_key`
- `size_bytes`
- `last_modified`
- `storage_class`

Provider ETag is checked separately as same-provider equivalence, not as a
portable content hash. See
[Index Compare Projection v1](../architecture/index-compare-projection-v1.md).

### What green parity does **not** certify

- Reflow-input readiness
- HEAD-enrichment parity (`index enrich-with-head` remains SQLite-bound)
- Coverage attestation structure, hub metadata, or physical segment shape
- Reduced-trust / third-party publication safety (see boundary framing below)

### Scoped dual-format builds

`build.scope` is allowed under durable and `--format both`. Coverage is
**fail-closed set-equality** against the crawl prefix plan: every planned
prefix must be attested, with no silent roll-up. That is what makes a
date-partitioned cohort safe to dual-build at scale.

Some dual-format combinations remain intentionally closed in this cut (for
example `--format both` with `--since`, `--background`, or non-default match
filters). Prefer a representative scoped dual-format job for parity, then
return to your production cadence flags on single-format builds.

## Temporal durable compare

After you have two durable snapshots for the same index set, compare them
temporally:

```bash
gonimbus index compare durable-delta \
  --before-manifest /path/to/before/manifest.json \
  --before-segments /path/to/before/segments \
  --after-manifest /path/to/after/manifest.json \
  --after-segments /path/to/after/segments
```

The report summarizes added / changed / tombstoned rows with fail-closed
coverage attribution. This is a **snapshot-to-snapshot** tool, not a
replacement for `index query --since-run`. Forward object deltas via
`--since-run` still require a SQLite-backed index (`--format sqlite` or
`both`); durable-only snapshots do not support `--since-run` yet.

## Hub export and hydrate

```bash
# Auto: durable-v2 when a local durable complete marker exists, else sqlite-v1
gonimbus index export --hub s3://bucket/index-hub/ --index-set idx_...

# Force a format
gonimbus index export --hub s3://bucket/index-hub/ --index-set idx_... --format durable
gonimbus index export --hub s3://bucket/index-hub/ --index-set idx_... --format sqlite

# Hydrate reads the hub marker; no --format required
gonimbus index hydrate --hub s3://bucket/index-hub/ \
  --index-set idx_... --dest /tmp/hydrated/
```

- Unknown hub formats are **rejected**.
- Durable hydrate verifies the manifest and **each segment digest** before
  trust, then restores `manifest.json` + segments — **not** `index.db`.
- `index hub ls` / `show` surface per-run formats so mixed hubs stay legible.

Large **SQLite** hub exports still use multipart upload when `index.db` crosses
the default threshold. Durable export naturally stays under single-PUT walls by
publishing segment objects; multipart remains available for large individual
artifacts when needed.

## SQLite-bound commands during the transition

`index query` is format-aware: it opens `sqlite-v1` when `index.db` is present,
otherwise a verified durable-v2 latest snapshot. Plain query streams verified
segment rows (emit-as-arrived; later-segment failure returns non-zero). Result
order matches SQLite (`rel_key`). Canonical-by-ETag is intentionally
non-constant-memory (`O(matched rows)` before grouping; `O(distinct non-empty
ETags)` for selection/output).

**`--since-run` requires SQLite today.** Against a durable-only index the
command fails closed; use `--format sqlite` or `both` when you need forward
deltas from a successful run boundary.

These local workflows still require an `index.db` today:

- `index enrich-with-head`
- `index stats`
- most `index doctor` paths
- **`index list`** and **`index gc`** — they enumerate directories that contain
  `index.db`. A durable-only default build produces **no** `index.db`, so those
  commands report as if no indexes exist until you build with `--format sqlite`
  or `both`.

Plan local inventory and remaining stats/doctor automation accordingly, or dual-build while you
migrate.

## Internal-render framing (mandatory)

Durable-v2 in this release is a full-fidelity **internal render** for trusted
operators and pipelines. It is **not**:

- a reduced-trust or de-identified share format
- a third-party publication format
- a disclosure-controlled boundary product

Do not treat a durable hub export as safe to hand across a trust boundary
without the future reduced-trust boundary-render path. That path is separate
work (column suppression, opaque tokens, coverage/statistics redaction) and is
**not** part of v0.4.0.

## Recommended migration path

1. Install v0.4.0 and confirm `gonimbus version`.
2. On a representative unit, run `--format both` and confirm green LIST parity.
3. Point hub export/hydrate at format-aware paths; prefer durable for new hub
   traffic.
4. Keep `--format sqlite` or `both` for any automation that still calls query,
   enrich-head, stats, doctor, list, or gc.
5. Treat durable hub artifacts as internal pipeline inputs only until a later
   boundary-render release lands.

## See also

- [Local Index](index.md) — full command reference
- [Steady-State Index Operations](steady-state-index-operations.md) — recurring builds
- [v0.4.0 release notes](../releases/v0.4.0.md)
- [Library consumers](../library-consumers.md) — `pkg/indexbuild` embedding notes
- [API stability](../api-stability.md) — Experimental tier for the durable engine
