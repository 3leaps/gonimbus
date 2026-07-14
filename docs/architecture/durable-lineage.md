# Durable lineage schema (dark)

**Status**: additive schema + dark ancestry readers only

**Does not**: activate continuous-state publication, prior-run load for ordinary
builds, durable `--since` / `--since-run`, spill/merge, or streaming writers

## Purpose

This document freezes the **additive** durable-manifest contract for:

1. authoritative `run_started_at`
2. exact single `state_parent` (set / run / manifest digest)
3. all-or-nothing `lineage` generation/baseline record
4. bounded, digest-verifying **ancestry readers**

Production builds do **not** emit continuity edges today. Legacy latest remains
a **verified current-state source**, not a trustworthy delta boundary.

## Wire fields (`gonimbus.index.manifest.v1`)

`index_schema_version` is **not** bumped. New fields are optional JSON members.

| Field            | Required?                                           | Meaning                                                                                                                                                     |
| ---------------- | --------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `run_started_at` | required when `lineage` present; otherwise optional | Authoritative run start (RFC3339 **UTC**, non-zero). **Not** an alias of `created_at`, journal start, or complete-marker time. Non-UTC offsets are refused. |
| `state_parent`   | optional                                            | Exact parent binding: `index_set_id`, `run_id`, required lowercase `manifest_sha256`.                                                                       |
| `lineage`        | optional                                            | `{ "version": 1, "generation": N, "baseline": bool }`. Absence = pre-continuity.                                                                            |

`parent_manifests` remains the reachability / enrich heritage list (digest
optional). Do **not** treat it as the continuous-state parent and do **not**
auto-translate it into `state_parent`.

### Presence rules (fail closed)

| Shape                                                     | Result                                                                                                                                                                                                                                                                  |
| --------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| No `lineage`, no `state_parent`                           | Legacy / pre-continuity — current-state open OK                                                                                                                                                                                                                         |
| `state_parent` without `lineage`                          | Refuse (`lineage_partial`)                                                                                                                                                                                                                                              |
| `lineage` without `run_started_at`                        | Refuse (`lineage_invalid_time`)                                                                                                                                                                                                                                         |
| Non-UTC `run_started_at`                                  | Refuse (`lineage_invalid_time`)                                                                                                                                                                                                                                         |
| `lineage.version` ≠ 1                                     | Refuse (`lineage_unknown_version`)                                                                                                                                                                                                                                      |
| `baseline:true`                                           | `generation` must be `1`; own set/run identity must be safe path components; optional **0 or 1** exact `state_parent` as a verified **pre-continuity state source** (not a delta boundary; parent **must** have `Lineage == nil` or refuse `lineage_baseline_conflict`) |
| `baseline:false`                                          | Requires full continuous `state_parent`; `generation >= 2`; parent must itself carry continuous lineage when walked                                                                                                                                                     |
| Cross-set `state_parent`                                  | Refuse (`lineage_cross_set`)                                                                                                                                                                                                                                            |
| Empty / uppercase digest                                  | Refuse (`lineage_invalid_digest`)                                                                                                                                                                                                                                       |
| Unsafe own `index_set_id` / `run_id` when lineage present | Refuse (`lineage_malformed`)                                                                                                                                                                                                                                            |

Never invent lineage from directory order, timestamps, or empty
`parent_manifests`. Structural validity does **not** depend on observer
wall-clock time.

## Reader API

Library-owned in `internal/indexsubstrate`:

- `ValidateManifestLineageStructure` — structural checks when fields present
- `ResolveAncestry` — digest-bound same-set walk via injected complete-path lookup
  and the same-bytes complete→manifest open seam

`DeriveReachabilityPlan` is **not** lineage verification (optional digests, no
byte hash, no depth budget).

### Trust and budgets

- Continuous roots **require** `CompletePath` and are re-opened under budget.
  Self-asserted `Accounted*Bytes` without a complete path are not provenance.
- Default budgets: depth 64, nodes 64, aggregate marker+manifest bytes 256 MiB,
  plus per-file marker/manifest bounds.
- Aggregate **remaining** bytes are carried into each open: marker then manifest
  reads are capped by `min(per-file bound, remaining)` so over-budget opens do
  not fully read large parents.
- Continuous and optional baseline→legacy node/depth budgets are checked
  **before** parent lookup/open (legacy parent is a graph node/edge for budgets
  even though it is omitted from the continuous `Chain`).
- Trusted **delta** ancestry stops at `baseline:true` (even when a pre-continuity
  state parent was verified).
- Continuous (non-baseline) parents must carry lineage; a baseline’s optional
  state-source parent must be pre-continuity (`Lineage == nil`).
- Parent identity already present in the walk is refused as `lineage_cycle`
  **before** parent I/O. Stable reason codes from parent structural validation
  are preserved across hops.

## Darkness (current product behavior)

| Path                                       | Behavior                                                                                             |
| ------------------------------------------ | ---------------------------------------------------------------------------------------------------- |
| Production `PublishSnapshot`               | Does not emit `run_started_at` / `state_parent` / `lineage`                                          |
| Durable/`both` build adapter               | Does not load prior-run state for ordinary builds                                                    |
| Durable `--since-run`                      | Unsupported                                                                                          |
| Canonical authority / whole-set GC execute | Untouched by this schema                                                                             |
| Spill / streaming writer                   | Spill/merge dark primitive is separate (see durable-spill-merge.md); not activated by lineage schema |

`SegmentWriterConfig` accepts optional lineage fields for tests and future
activation plumbing only. The writer validates the caller-supplied
`run_started_at` **before** any UTC normalization, so decoder and emitter
enforce one contract (non-UTC offsets refuse as `lineage_invalid_time` and do
not create segment artifacts).

## Explicit non-goals

- Emitting continuous lineage from production publish
- Enabling durable `--since` / `--since auto` / `--since-run`
- Loading prior-run state / coverage merge / continuity activation
- Reachability delete driven by new edges
- Backfilling history onto legacy artifacts

## Compatibility

- Existing durable manifests without the new fields continue to open for
  query/export/hydrate/current-state.
- Absence of lineage is pre-continuity; readers do not invent baselines.

## Related

- Operator guide: `docs/user-guide/durable-index.md`
- Dark spill/merge row source: `docs/architecture/durable-spill-merge.md`
- ADR-0006 (CLI as adapter), ADR-0007 (canonical authority — not reopened here)
