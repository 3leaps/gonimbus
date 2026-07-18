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
# Durable publication + per-run SQLite parity verification
gonimbus index build --job index.yaml --format both
```

| Need                                                             | Format choice                                |
| ---------------------------------------------------------------- | -------------------------------------------- |
| Hub-scale export/hydrate, default new builds                     | `durable` (default)                          |
| Local `index query` / `list` / `stats` / `doctor`                | `durable` or `sqlite` (format-aware seam)    |
| Local `enrich-with-head`                                         | `durable` or `sqlite` (format-aware)         |
| Local inventory GC (`index gc --dry-run`)                        | Format-aware immutable plan; execution gated |
| Canonical SQLite consumer artifact (`index.db`)                  | `sqlite` only                                |
| Migration confidence (durable publication + per-run parity gate) | `both`                                       |

**Existing `index.db` files are not rewritten or invalidated.** SQLite remains a
first-class compatibility path. Resume printed as
`gonimbus index build --resume-run <run_id>` still works under the durable
default when `--format` is omitted (SQLite checkpoint lifecycle).

### Artifact layout (conceptual)

```text
indexes/idx_<hash>/
  identity.json              # always for the index set
  index.db                   # only when format is sqlite (canonical consumer artifact)

cache/segments/<index_set_id>/runs/<run_id>/
  journals/ ...
  segments/ ...
  manifest.json
  complete.json              # (or complete marker for the run)

cache/segments/<index_set_id>/verification/run_<nano>/
  index.db                   # --format both only: run-scoped parity-verification
                             # projection; never a reader-selectable consumer DB
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

### Sizing the merge workspace (successive builds at scale)

A **successive** durable build (a second or later build of the same index set)
stages the **full prior current-state** into an on-disk scratch workspace before
merging in the new observations. Peak workspace therefore scales roughly with
corpus size, not with the change set. A first build has no prior state to stage
and is unaffected.

The workspace has a **finite ceiling** (`MaxWorkspaceBytes`). Crossing it fails
the build **closed** — a typed error, the prior published run and `latest`
untouched — rather than growing without bound. The default ceiling is **16 GiB**.
Field peak runs ~1.2–1.35 KiB/row (bounded, flattening with scale), so this
carries to ~13.6M objects — validated with ~20% headroom at ~10.9M. It is a floor
for convenience, **not a guarantee** for larger sets, which must raise it
explicitly.

A second budget, the **max single journal-record size** (`MaxRecordBytes`,
default **16 MiB**), bounds one journal line. The journal header carries the
crawl-prefix plan, so a very wide/dense scope (tens of thousands of prefixes) can
push the header past the default and fail the build closed at the journal phase.
Raise it the same way when a very fine scope needs it.

Size the ceiling (and, if needed, point the scratch at a roomier disk) via — in
precedence order — the CLI flag, an environment variable, or application config:

```bash
# CLI flag (foreground builds)
gonimbus index build --job index.yaml --spill-workspace-max 24GiB --spill-record-max 32MiB --spill-root /mnt/scratch

# Environment (also inherited by --background jobs)
export GONIMBUS_SPILL_WORKSPACE_MAX=24GiB
export GONIMBUS_SPILL_RECORD_MAX=32MiB
export GONIMBUS_SPILL_ROOT=/mnt/scratch
```

```yaml
# Application config (XDG config file — never the index job manifest)
index:
  spill:
    workspace_max: 24GiB # 24GiB/16GB/raw bytes; explicit 0/negative/unlimited is refused
    record_max: 32MiB # max single journal-record size
    root: /mnt/scratch # absolute, real, non-symlink, operator-exclusive
```

Notes:

- **Precedence:** CLI flag > environment > application config > 8 GiB default.
- The value is a **ceiling, not a reservation**, and a self-protection control
  against runaway scratch — keep it a finite positive size; there is no
  "unlimited" spelling.
- `--spill-workspace-max` / `--spill-root` are **not forwarded to `--background`
  jobs** (the managed child is reconstructed from a fingerprinted invocation).
  Use the environment or config keys, which the managed child inherits.
- The scratch **root is host/operator configuration**, never index identity — it
  never enters manifests, receipts, or committed digests, and is never echoed
  into artifacts or sanitized errors (diagnostics show its source, not the path).
- Each durable build prints the effective ceiling and its source on **stderr**.

## Dual-format parity

`--format both` runs **one crawl** and publishes the **durable index as the
canonical artifact**. The SQLite side is a **run-scoped parity-verification
projection**: it is written to a fresh per-run path, compared against the
durable output, and reported — it is never a reader-selectable consumer
`index.db`, never adopted by a later run, and never a lineage parent. Use
`--format sqlite` when a consumer needs the canonical `index.db`.

Each run emits a machine-readable `gonimbus.index.compare_result.v1` report
(including a `projection_semantics` block), and under `--json` a terminal
`gonimbus.index.build_result.v1` receipt with `formats_committed:
["durable-v2"]` plus a `verification` block (projection materialized/closed,
the producing run binding, parity status, projection rows and digest). The
receipt never claims a committed consumer SQLite artifact; "verification
succeeded" and "consumer artifact committed" are separate facts.

```bash
gonimbus index build --job index.yaml --format both
```

Use this as the **migration confidence gate** before switching consumers off
SQLite for new hub traffic. Successive `both` builds of the same set extend
durable lineage continuously; each run's verification projection starts clean,
so runs never contend for a shared SQLite path.

If durable publication succeeds but the verification projection or parity
comparison fails, the durable snapshot stays authoritative and visible, no
successful `both` receipt is emitted, and the failed projection is never
selected by readers.

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

Repeated builds of the same scoped identity merge coverage against the
verified parent: prior rows outside the current run's attested plan are
retained verbatim (including their first-seen lineage, HEAD enrichment, and
existing tombstones), and deletes are inferred only for keys inside the
current confirmed-complete attestation. The published coverage still lists
exactly the crawled plan — retained rows carry no fresh observation claim.

Some dual-format combinations remain intentionally closed in this cut (for
example `--format both` with `--since` or non-default match filters).
Background execution is supported for `sqlite`, `durable`, and `both`; the
managed child verifies the exact effective invocation and manifest content
selected by its parent before building.

## Prefix-shaped match → scope migration

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
6. Reclaim the old set with existing whole-set `index gc` when ready

Migration never rewrites identity on the old set, never adopts old DB/segments
under a new scope hash, and never synthesizes parent linkage.

### Still open (non-prefix match controls)

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
`--since-run` still require a canonical SQLite index (`--format sqlite`);
durable snapshots do not support `--since-run` yet.

### Lineage and continuity

Ordinary durable builds emit a continuous lineage contract on the manifest
(`run_started_at`, digest-bound `state_parent`, `lineage`) and load prior state
from the verified latest snapshot of the same index set. See
[durable lineage](../architecture/durable-lineage.md).

- A **first publication** is a baseline (generation 1, no state parent).
- A build over a **pre-continuity** (no-lineage) parent — for example an
  enriched snapshot — publishes a baseline bound to that parent as a verified
  state source; that parent is still **not** a delta boundary.
- A build over a **continuous** parent extends it (generation + 1) after
  validating the parent's bounded ancestry; any ancestry defect fails closed
  without advancing latest.
- **Legacy manifests** (fields absent) remain readable as a **verified
  current-state** source. Legacy latest is **not** a trustworthy `--since-run` /
  forward-delta boundary.
- `run_started_at` is a non-zero **UTC** authoritative run start (not
  `created_at` or journal time).
- Durable `--since` / `--since-run` remains unsupported: forward object deltas
  still require a SQLite-backed index.

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

- **durable-v2** when a verified latest → complete → manifest chain is present
  (preferred when both formats exist for the same set — a set-root `index.db`
  beside a verified durable latest may be a stale artifact from an earlier run
  and is surfaced diagnostically, never silently selected)
- **sqlite-v1** when `index.db` is present with no verified durable latest
  (SQLite-only sets keep their existing selection)

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
   `doctor`, and `enrich-with-head`. Keep `--format sqlite` only for
   **SQLite-only** surfaces: `query --since-run`, `stats --prefixes`, and full
   `--resume-run` checkpoint recovery. (`both` no longer produces a canonical
   `index.db`; its SQLite side is per-run parity verification.)
5. Treat durable hub artifacts as internal pipeline inputs only until a later
   boundary-render release lands.

## See also

- [Local Index](index.md) — full command reference
- [Steady-State Index Operations](steady-state-index-operations.md) — recurring builds
- [v0.4.0 release notes](../releases/v0.4.0.md)
- [Library consumers](../library-consumers.md) — `pkg/indexbuild` embedding notes
- [API stability](../api-stability.md) — Experimental tier for the durable engine
