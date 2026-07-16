# Durable lineage

**Status**: active — ordinary durable builds emit lineage / `state_parent` and
load verified parent state

**Does not**: enable durable `--since` / `--since-run` (timestamp-scoped
reduction), merge coverage for scope-reduced builds, raise enrich scale
ceilings, or make the SQLite index a lineage authority

## Purpose

This document freezes the **additive** durable-manifest contract for:

1. authoritative `run_started_at`
2. exact single `state_parent` (set / run / manifest digest)
3. all-or-nothing `lineage` generation/baseline record
4. bounded, digest-verifying **ancestry readers**

Ordinary durable builds emit continuity edges: parent rows and continuity
metadata derive from a single verified same-set capture of latest, under the
three-way baseline/generation rule, with bounded ancestry validated before any
continuous extension. Legacy (pre-continuity) latest remains a **verified
current-state source**, not a trustworthy delta boundary.

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

## Activation status (current product behavior)

| Path                                       | Behavior                                                                                                                                                                                                                                                    |
| ------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Production `PublishSnapshot`               | **Active** — persists `run_started_at`, digest-bound `state_parent`, and `lineage` supplied by the durable build path                                                                                                                                       |
| Durable/`both` build adapter               | **Active** — ordinary builds stream the verified same-set parent's rows from a single lease-held capture (caller `PriorRows` refused) and derive the three-way baseline/generation rule; `both` derives durable lineage independently of the SQLite sidecar |
| Ancestry validation                        | **Active** — a continuous parent's bounded ancestry is verified before extension and before a same-run recovery re-publish; defects fail closed without advancing latest                                                                                    |
| Durable `--since` / `--since-run`          | Unsupported (timestamp-scoped reduction is not activated)                                                                                                                                                                                                   |
| Scope-reduced coverage merge               | Not activated — a scope-reduced build does not yet retain out-of-scope prior rows under merged coverage                                                                                                                                                     |
| Enrich publish                             | Pre-continuity (no lineage emission on the enrich path); enrich scale ceiling unchanged                                                                                                                                                                     |
| Canonical authority / whole-set GC execute | Untouched by this schema                                                                                                                                                                                                                                    |

`SegmentWriterConfig` carries the lineage fields the publish path supplies. The
writer validates the caller-supplied `run_started_at` **before** any UTC
normalization, so decoder and emitter enforce one contract (non-UTC offsets
refuse as `lineage_invalid_time` and do not create segment artifacts).

## Explicit non-goals

- Enabling durable `--since` / `--since auto` / `--since-run`
- Merging coverage for scope-reduced builds (retaining out-of-scope prior rows)
- Reachability delete driven by new edges
- Backfilling history onto legacy artifacts
- Treating the SQLite index as a lineage authority

## Compatibility

- Existing durable manifests without the new fields continue to open for
  query/export/hydrate/current-state.
- Absence of lineage is pre-continuity; readers do not invent baselines.

## Related

- Operator guide: `docs/user-guide/durable-index.md`
- Spill/merge row source: `docs/architecture/durable-spill-merge.md`
- ADR-0006 (CLI as adapter), ADR-0007 (canonical authority — not reopened here)
