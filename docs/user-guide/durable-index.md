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

# SQLite when you still need SQLite-only surfaces (gc, --since-run, …)
gonimbus index build --job index.yaml --format sqlite
# Dual-format for migration confidence + SQLite-only consumers
gonimbus index build --job index.yaml --format both
```

| Need                                                            | Format choice                                      |
| --------------------------------------------------------------- | -------------------------------------------------- |
| Hub-scale export/hydrate, default new builds                    | `durable` (default)                                |
| Local `index query` / `list` / `stats` / `doctor`               | `durable`, `sqlite`, or `both` (format-aware seam) |
| Local `enrich-with-head`                                        | `durable`, `sqlite`, or `both` (format-aware)      |
| Local inventory GC (`index gc --dry-run`)                       | Format-aware immutable plan; execution gated       |
| Migration confidence (one crawl, two artifacts + parity report) | `both`                                             |

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
- HEAD-enrichment **substrate** parity (SQLite in-place vs durable append/new-run
  are different write models; filters/`--resume` candidate selection align)
- Coverage attestation structure, hub metadata, or physical segment shape
- Reduced-trust / third-party publication safety (see boundary framing below)

### Scoped dual-format builds

`build.scope` is allowed under durable and `--format both`. Coverage is
**fail-closed set-equality** against the crawl prefix plan: every planned
prefix must be attested, with no silent roll-up. That is what makes a
date-partitioned cohort safe to dual-build at scale.

Some dual-format combinations remain intentionally closed in this cut (for
example `--format both` with `--since` or non-default match filters).
Background execution is supported for `sqlite`, `durable`, and `both`; the
managed child verifies the exact effective invocation and manifest content
selected by its parent before building.

## Prefix-shaped match → scope migration (G11 subset)

Durable builds still reject non-default `build.match.includes` at the faithful
coverage gate. Some SQLite-era manifests use **prefix-shaped** includes only
(literal non-root prefix + terminal `/**`, no excludes/filters/hidden deviation).
Those are expressible as an explicit `build.scope` `prefix_list`.

### Audit / convert

```bash
# Machine plan (no provider, no authority, no marker writes)
gonimbus index migrate-match-scope --job legacy.yaml --json

# Emit a proposed durable-compatible manifest (exclusive create)
gonimbus index migrate-match-scope --job legacy.yaml \
  --emit-manifest proposed.yaml
```

Accepted includes are converted to `build.scope.type: prefix_list` with default
match (`includes: ["**"]`). Ambiguous globs, residual predicates, and an
existing `build.scope` combined with residual includes **fail closed**.

Default sole `**` is already durable-compatible (`already_compatible`).
Re-running against an emitted proposed form returns `already_migrated`.

Post-date entity shapes such as `date/day/entity=<id>/**` are emitted as
**exact** `prefix_list` entries. Do not assume
`scope.date_partitions.discover.segments` proves equivalence for segments
after the date index.

### Parallel cutover (new identity)

Changing match/scope changes index-set identity. There is **no in-place
lineage**. Operator sequence:

1. `migrate-match-scope` audit + emit proposed manifest
2. `index build --job proposed.yaml` (new independent set via existing library path)
3. Compare projections (LIST plan digests + row/byte projection); do **not**
   treat `--format both` as legacy-vs-migrated proof (it fans one new crawl)
4. Pin / select the **new** receipt for consumers
5. Keep the old set through a validation window (rollback = switch pin back)
6. Reclaim the old set with existing whole-set `index gc` (G5a) when ready

Migration never rewrites identity on the old set, never adopts old DB/segments
under a new scope hash, and never synthesizes parent linkage.

### Still open (remaining G11)

Excludes, suffix/non-prefix globs, metadata filters, and non-default
`include_hidden` are **not** retired by this migration. They remain rejected
on durable builds until later control-by-control work.

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

## Format-aware local commands

`index query`, `index list`, `index stats`, and `index doctor` share a
format-aware local reader seam:

- **sqlite-v1** when `index.db` is present (preferred when both formats exist
  for the same set)
- **durable-v2** when a verified latest → complete → manifest chain is present
  (including durable-only default builds with no `index.db`)

`index query` streams verified segment rows on durable (emit-as-arrived;
later-segment failure returns non-zero). Result order matches SQLite
(`rel_key`). Canonical-by-ETag is intentionally non-constant-memory
(`O(matched rows)` before grouping; `O(distinct non-empty ETags)` for
selection/output).

Durable-v2 limitations (fail closed or narrowed):

- **`index query --since-run`** requires SQLite today.
- **`index stats --prefixes`** is sqlite-only (`prefix_stats` table).
- **`index stats --runs`** on durable lists published complete markers only
  (not the full SQLite run lifecycle / failed-resumable).
- Durable size in stats/list is the sum of published **segment file sizes**,
  not `SUM(objects_current.size_bytes)`.
- **`index gc --dry-run`** inventories SQLite and durable sets by authoritative
  markers and emits one immutable plan for identity, segment-set, and journal
  roots. Rerun the same retention policy without `--dry-run` to execute it.
  Execution takes the same stable whole-set authority used by public build and
  enrich libraries, plus the narrower durable publish lock, recomputes the plan
  under exclusion, and requires the exact plan digest to match. The authority
  lock lives outside the renameable set root and remains held through recovery,
  so quarantine cannot detach it from the path used by a new writer. Each target
  is re-hashed immediately before it is moved to a transaction quarantine. An
  owner-only operation record outside the deletion roots makes every move/delete
  boundary idempotently recoverable; the next GC invocation finishes an
  interrupted transaction before planning new work.
  Corrupt, aliased, symlinked, active, or legacy durable sets without a proven
  writer-lock artifact remain retained with warnings.
- **Full run/checkpoint lifecycle** (`--resume-run` for build/enrich
  operation recovery) remains SQLite/opcheckpoint-oriented; durable enrich
  rejects `--resume-run` (row-level `--resume` is supported).
- **`index enrich-with-head`** is format-aware via library workhorse
  `pkg/indexenrich`: durable-only sets take the stable **OS-level whole-set
  authority** (Unix flock / Windows LockFileEx; shared with build and GC) and
  the inner durable publish lease,
  open **one** verified parent snapshot, HEAD-filter candidates from that
  parent, seal an enrich-only journal, and publish a **new** internal-render
  snapshot that advances `latest.json` only if the held lease is still valid
  and the live parent CAS (set/run/manifest + coverage digest) still matches
  (append/new-run; prior segments immutable). Unobserved keys are never
  tombstoned (`enrich-only` publication mode). Dual-format sets with
  `index.db` prefer SQLite enrich. Publication is all-or-nothing for
  pre-latest failures; post-latest report failures still surface committed
  identity when stdout cannot. Enrichment is internal-render-only — not a
  boundary-safe share format.
  **Scale bound:** refuses prior snapshots with validated manifest
  `counts.rows` **> 2,000,000** before row materialization (descriptor sum and
  non-negative counts enforced). Larger sets need a stream/spill engine.

Use exact build receipts (`index build --json`) plus `--index-set` / `--run-id`
for automation handoff; do not rediscover just-built durable sets via list
ordering alone when multiple scopes share a base URI.

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
4. Default durable builds support format-aware `query`, `list`, `stats`,
   `doctor`, and `enrich-with-head`. Keep `--format sqlite` or `both` only for
   **SQLite-only** surfaces: `query --since-run`,
   `stats --prefixes`, and full `--resume-run` checkpoint recovery.
5. Treat durable hub artifacts as internal pipeline inputs only until a later
   boundary-render release lands.

## See also

- [Local Index](index.md) — full command reference
- [Steady-State Index Operations](steady-state-index-operations.md) — recurring builds
- [v0.4.0 release notes](../releases/v0.4.0.md)
- [Library consumers](../library-consumers.md) — `pkg/indexbuild` embedding notes
- [API stability](../api-stability.md) — Experimental tier for the durable engine
