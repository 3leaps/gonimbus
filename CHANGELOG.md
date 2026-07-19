# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Library API Section Convention

Use a `### Library API` subsection under `## [Unreleased]` only when a release
changes the Stable embedded library API listed in `docs/api-stability.md`.
For a Stable break, include the break, the migration path, and the
advance-notice status. Omit the subsection when there are no Stable API
changes.

## [Unreleased]

### Added

- **Concurrent execution in the library reflow engine.** The `pkg/reflow`
  record-stream runner now executes objects on a bounded worker pool honoring
  the resolved concurrency ceiling, with per-destination-key arbitration moved
  into the engine (concurrent same-key workers serialize; each established
  destination key gets exactly one durable observed-mark and one conditional
  create). Live stdin record-stream copies dispatch to the engine again — the
  v0.4.1 dispatch narrowing (#159) is removed, behind a standing behavioral
  parity gate: a same-input dual-path harness (copy-heavy and skip-heavy),
  interruption/resume evidence measuring exactly-one-land per object against a
  real checkpoint store, and a flag-coverage matrix proving every flag's
  disposition on each execution path.
- **Dispatch transparency.** Run and summary records carry `execution_path`
  (`engine` | `cli-pool`) on both paths; requested (`parallel`), resolved
  (`concurrency_ceiling_effective`), and observed (`concurrency_max_active`)
  concurrency remain separate fields, and no run reports a requested value the
  selected path does not honor.

### Changed

- **Concurrency configurations normalize to one invariant**
  (`1 ≤ floor ≤ initial ≤ effective ≤ requested`) across the CLI, the library
  runner (including the documented zero-value config), and direct limiter
  construction: pool size, limiter behavior, and records derive from the same
  normalized config; throttle recovery can never exceed the reported effective
  ceiling; fixed (non-adaptive) partial configs run at the ceiling they report.
- **Strict terminal checkpoint acknowledgement on both execution paths.** A
  completed copy or collision-skip decision is never acknowledged with its
  success status when the checkpoint store cannot durably record it: the
  object reports `failed` (reason `checkpoint.write_failed`) and the run exits
  non-zero; resume against a healthy store converges without double-landing.
  Auxiliary arbitration-state write failures warn (typed
  `REFLOW_ARBITRATION_STATE_WRITE_FAILED`) and continue on both paths. The
  CLI-only collision modes (`overwrite-if-source-newer`, `quarantine`) retain
  their historical warn-and-continue terminal behavior until they migrate to
  the engine.

### Known limits (stated, not claimed)

- Reflow interruption guarantees cover in-process cancellation with in-flight
  work; hard process-kill crash windows and concurrent multi-process runs
  sharing one checkpoint root are not claimed (run one transfer per checkpoint
  at a time).

## [0.4.1] - 2026-07-18

**SQLite independence for the durable index, and memory-bounded streaming
publication.**

v0.4.0 made the durable index the default build format. v0.4.1 completes the
operator story around it: the durable-only default no longer needs an
`index.db` for everyday local work, and `index build` publishes durable
snapshots through a streaming path that never materializes the full row set in
memory. SQLite remains a first-class compatibility path (`--format sqlite` /
`--format both`) for the surfaces that still require it.

As in v0.4.0, the durable-v2 format is a full-fidelity **internal render** for
trusted operator and pipeline use — **not** a reduced-trust, de-identified, or
third-party publication format. Content digests carried by durable manifests
and segments are **content-integrity / tamper-evidence** checksums, not
statements of author authenticity or provenance.

### Added

- **Durable-native `index query`** — format-aware library reader seam
  (`pkg/indexreader`, Experimental) scans durable segments natively with no
  ephemeral SQLite bridge; plain query streams verified JSONL as rows arrive.
  Marker-authoritative `sqlite-v1` / `durable-v2` dispatch with same-open /
  same-bytes digest verification. `query --since-run` fails closed on
  `durable-v2` (use `--format sqlite` or `both`).
- **Machine-stable build receipt and pinned-run handoff** — `index build --json`
  emits a terminal `gonimbus.index.build_result.v1` receipt (status, requested
  and committed formats, full set/run/scope identity, counts, durable
  `manifest_sha256`) so automation hands off the exact committed snapshot
  without list heuristics. Pinned durable query via `--index-set` + `--run-id`
  opens the run complete marker and never consults `latest.json`.
- **Format-aware `index list`, `stats`, and `doctor`** — local inventory and
  diagnostics work for durable-only sets with no `index.db`. `doctor` inspects
  the SQLite and durable substrates independently so a broken durable marker
  stays discoverable and reports unhealthy.
- **Durable `index enrich-with-head`** — HEAD enrichment for durable-only sets
  under an OS-level exclusive write lease; prior segments stay immutable and
  unobserved keys are never tombstoned.
- **Managed durable build execution** — `index build --background` runs a
  managed durable build via canonical effective invocation and exact
  child-process replay, with deduplication, leases, recovery, terminal
  receipts, and job log/API surfaces. Credential-bearing, signed, and metadata
  invocation material is rejected before persistence.
- **Whole-set garbage collection** — `index gc --dry-run` (`--max-age`,
  `--keep-last`, optional `--json`) produces a deterministic, format-aware plan
  that groups SQLite and durable artifacts by full index-set identity and
  retains conservatively when safety cannot be proven. Leased execution and a
  canonical SQLite authority binding land alongside the planner (ADR-0007).
- **`index migrate-match-scope`** — fail-closed migration of prefix-shaped
  `build.match.includes` (literal prefix + terminal `/**`) into explicit
  `build.scope` `prefix_list`, with LIST-plan equality and an independent
  LIST-reachable projection proof. Exclusive emit publishes via hard-link-only
  (no in-place replacement); `--force` uses atomic rename.
- **Streaming durable publication** — `index build` publishes durable snapshots
  by compacting sealed journals against the verified parent through a
  spill/merge current-state source and a streaming segment-set writer, so the
  full row set is never materialized in memory. The row / artifact / digest
  contract is identical to the prior materialized path. Merge-workspace and
  single-record ceilings are finite, operator-tunable budgets (CLI flag > env >
  app config > built-in default; 16 GiB workspace / 16 MiB record defaults);
  invalid budgets refuse with typed errors before any crawl or side effect, and
  exhaustion refuses fail-closed with no authority advance. `--format both`
  publishes durable as canonical with a per-run SQLite parity-verification
  projection. Sealed journals carry a tamper-evident content digest and the
  canonical crawl-prefix plan; `latest` advance is CAS-guarded. Streaming
  segment progress and workspace peak/ceiling diagnostics render on stderr
  (counts and sources only; best-effort, never fails a build).

### Changed

- `index build` on the durable path now executes through the streaming
  publication engine instead of materializing the compacted row set. Emitted
  rows, segment artifacts, manifests, and digests are unchanged; the difference
  is bounded memory during publication.

### Fixed

- **`transfer reflow` stdin live copies honor `--parallel` again:** for stdin
  `gonimbus.reflow.input.v1` streams with a cloud destination and default
  collision handling, live (non-dry-run) execution routes through the CLI
  worker pool so object copies run concurrently. From v0.3.5 through v0.4.0
  that shape used the library record-stream engine, which executed objects
  serially while the run record still reported the requested `--parallel`
  value (materially slower multi-object runs). Dry-run continues on the
  library engine. Library engine concurrency is a follow-on.

### Development

- Documented a finding-driven gosec G101 triage and inline `#nosec` convention
  in `docs/development/ci.md` (no Go-source suppressions, no scanner-config
  allowlists).
- Added an on-demand, measurement-only reflow throughput/concurrency harness
  (`make test-reflow-throughput`, non-CI) with supervised smoke, saturation,
  checkpoint, and full-pipe profiles, strict telemetry/parity/sterility
  assertions, and a small-scale relative gate (absolute-envelope validation
  remains a deferred BYO-cloud follow-on).

## [0.4.0] - 2026-07-09

**Durable index format is now the default.**

v0.4.0 is the index-substrate epoch: segmented immutable durable indexes
(journals → Snappy-Parquet segments → internal manifests → hub markers) become
the default `index build` artifact, with SQLite retained as an explicit
compatibility path. The release closes the multi-release durable substrate and
operator-usability arc, including dual-format parity, hub export/hydrate for
`durable-v2`, hub visibility, scoped dual-format builds with faithful coverage,
and stderr progress for multi-minute durable builds. See
[`docs/releases/v0.4.0.md`](docs/releases/v0.4.0.md) for the narrative
walkthrough.

### Added

- **Durable index substrate:** crawl journals, compaction, immutable segment
  writer, publication gate (sealed journal → coverage → segments → manifest →
  complete → latest), durable build engine (`pkg/indexbuild`), boundary render
  guard, and parent-chain reachability for internal manifests.
- **Dual-format index builds:** `index build --format durable|sqlite|both` with
  shared observation for dual-format parity comparison.
- **Parity and delta comparators:** old-vs-new projection-v1 compare (LIST
  fidelity) and durable snapshot-to-snapshot temporal delta; compare results
  carry an explicit `projection_semantics` block stating what green parity
  certifies and does not certify.
- **Format-aware hub export and hydrate:** hub commit markers distinguish
  `sqlite-v1` and `durable-v2`; unknown formats reject; durable paths verify
  manifest and per-segment digests before trust.
- **Hub run format visibility:** `index hub` surfaces per-run formats; latest
  pointer reads are bounded and semantically validated for delete authority.
- **Scoped durable / both builds:** `build.scope` (explicit LIST prefix plan)
  is allowed under durable and `--format both`, with fail-closed set-equality
  between crawl-plan prefixes and coverage attestations (no silent roll-up).
- **Durable build progress:** multi-minute durable builds emit crawl `progress:`
  lines and a segmenting-tail `phase=segmenting segment=k/N rows=…` on stderr
  without changing artifact bytes.

### Changed

- **Default build format is durable:** `index build` defaults to durable-v2
  snapshots under the segment cache. SQLite remains available via
  `--format sqlite` or dual-format `--format both`.
- **Export default is auto:** `index export --format auto` prefers a local
  durable snapshot when present, otherwise sqlite-v1, without requiring
  `index.db` for durable export.
- **Default segment packing:** target rows per segment is 500k (packing lever;
  not operator-facing configuration in this cut).
- **Resume remains SQLite lifecycle:** printed `index build --resume-run <id>`
  still reaches the checkpoint resume path under the durable default when
  `--format` is omitted.

### Compatibility

- Existing SQLite `index.db` files are not rewritten, migrated, or invalidated.
- Local SQLite-bound consumers and inventory: `index query`, `enrich-head`,
  `stats`, most `doctor` paths, **`index list`**, and **`index gc`** still
  require an `index.db`. Under the durable default a plain build produces no
  `index.db`, so durable-only index sets are not yet visible to `list` / `gc`
  ("No indexes found"). Build with `--format sqlite` or `both` during the
  transition if you need those local workflows or local enumeration. Durable
  hydrate restores manifest + segments, not `index.db`.
- Durable-v2 artifacts in this release are a full-fidelity **internal render**
  for trusted operator workflows. They are not a reduced-trust / de-identified
  publication format, and a durable hub export is not a disclosure-controlled
  share format.

### Documentation

- Added this v0.4.0 release page and refreshed rolling release notes.
- Updated the current-release README pointer for the durable default.
- Added operator guide [`docs/user-guide/durable-index.md`](docs/user-guide/durable-index.md)
  covering durable-default workflow, dual-format parity semantics, durable-delta
  compare, hub round-trip, SQLite-bound `list`/`gc`, segment packing defaults,
  and internal-render boundary framing.
- Expanded Local Index user guide (list/gc caveats, compare commands, hub export
  dual-format scale story) and index-build mental model for multi-format output.
- Expanded library-consumer guidance for `pkg/indexbuild`, hub markers, and
  consumers that assumed every build produces `index.db`.
- Refreshed architecture notes for durable-default (no longer experimental-only)
  and compare projection semantics.

## [0.3.7] - 2026-07-05

**Operational data root overrides and repository-local state guardrails.**

v0.3.7 gives operators a single supported way to relocate Gonimbus operational
state away from the platform default app-data directory. `GONIMBUS_DATA_DIR`
sets a one-process data root, `data_root` sets the same root in config, and the
existing operational-state surfaces now resolve through that shared root instead
of each choosing their own local path. The release also fails closed when a
resolved data root would live inside a git working tree, regardless of source.
See [`docs/releases/v0.3.7.md`](docs/releases/v0.3.7.md) for the narrative
walkthrough.

### Added

- **App-wide data root override:** `GONIMBUS_DATA_DIR` controls the operational
  data root for index databases, index-build job records, server job defaults,
  and operation checkpoints. `GONIMBUS_DATA_ROOT` remains accepted as an
  environment alias.
- **Persistent config key:** `data_root` can be set in Gonimbus config for a
  durable relocation. `data_dir` remains accepted as a compatibility alias.
- **Doctor visibility:** `gonimbus doctor` reports the resolved data root,
  source, and writability/creatability status so operators can verify a runtime
  environment before a crawl, index build, or transfer.

### Changed

- **Single resolver for operational state:** indexes, index-build jobs, server
  job defaults, and operation checkpoints now use the same data-root resolver.
- **Platform defaults remain automatic:** when no override is set, Gonimbus uses
  the platform app-data location, honoring `XDG_DATA_HOME` where the platform
  config layer does.
- **Local-state permissions tightened:** app-data directories and runtime state
  files are created with owner-only permissions where Gonimbus controls the
  path.

### Fixed

- **Repository-local state rejection:** resolved data roots inside a git working
  tree are rejected before state is created, regardless of source, including
  paths that enter the repository through symlinks.

### Documentation

- Added this v0.3.7 release page and refreshed rolling release notes.
- Documented data-root precedence, no-auto-migration behavior, off-host storage
  caution, and repository-local path rejection in the index user guide.
- Updated README environment-variable examples and the current-release pointer.

## [0.3.6] - 2026-07-04

**Incremental index top-ups and query-time forward deltas.**

v0.3.6 focuses on steady-state indexing for large object stores. `index build
--since <timestamp>|auto` lets recurring builds narrow provider enumeration
when the manifest has a date-partitioned scope, and `index query --since-run
<run_id>` lets downstream consumers read the current objects first seen or
changed after a completed run. The release also hardens index timestamp storage
and schema-v8 migration behavior so the new delta surface fails closed instead
of guessing across older run history. See
[`docs/releases/v0.3.6.md`](docs/releases/v0.3.6.md) for the narrative walkthrough.

### Added

- **Incremental index builds:** `index build --since <timestamp>|auto` narrows
  date-partition scope before provider LIST when possible, then applies a
  last-modified ingest filter. `--since auto` derives its watermark from the
  latest successful run in the same IndexSet and falls back to full enumeration
  with a warning when the watermark cannot be resolved safely.
- **Since-plan telemetry:** since builds emit structured planning output that
  tells operators whether enumeration reduction was applied, partially applied,
  or unavailable, plus per-prefix `added` / `changed` / `unchanged` counts.
- **Query-time forward deltas:** `index query --since-run <run_id>` emits the
  current active rows first seen or meaningfully changed after a successful run
  in the same IndexSet. The output keeps the existing
  `gonimbus.index.object.v1` record shape and adds optional delta fields such
  as `change_kind`, `first_seen_run_id`, and `last_changed_run_id`.
- **Index schema v8:** object rows now persist `first_seen_run_id` and
  `last_changed_run_id` alongside the existing run and timestamp metadata.

### Changed

- **Scoped since builds remain not-full-coverage:** since builds skip
  soft-delete because they do not prove absence outside the narrowed listing
  plan. Periodic full-coverage audit builds remain the deletion-detection path.
- **Timestamp storage compatibility:** index database timestamp storage is now
  normalized to a fixed-width UTC text form that is preserved by both the
  default SQLite driver and the optional `gonimbus_libsql` build. CLI and JSON
  output timestamps remain unchanged, but users who query `index.db` directly
  may see internal timestamp fields stored with a `+0000 UTC` suffix instead of
  `Z`. Existing RFC3339/RFC3339Nano stored values remain readable and are
  migrated automatically.

### Fixed

- **Since-run guardrails:** `--since-run` validates that the boundary run is
  known, successful, in the same IndexSet, and comparable by stored run
  ordering rather than by lexicographic run ID sorting.
- **Migration honesty:** indexes migrated from earlier schemas reject
  pre-baseline `--since-run` boundaries instead of returning a confident delta
  that cannot distinguish added from changed rows.
- **Change-predicate parity:** SQL persistence of `last_changed_*` is guarded
  against drift from the Go object-row change predicate, including timestamp
  equivalence cases.
- **libsql parity:** optional `gonimbus_libsql` builds preserve the same
  timestamp storage and migration behavior as the default pure-Go SQLite path.

### Documentation

- Added this v0.3.6 release page and refreshed rolling release notes.
- Updated steady-state index guidance for `--since`, `--since auto`,
  enumeration-reduction signals, scoped-build deletion limits, steady-state
  cadence, `--since-run` downstream delta flows, and the deliberate absence of
  `--at-run` point-in-time queries.
- Updated the current-release README pointer for incremental index operations.

## [0.3.5] - 2026-07-02

**Multipart export and reflow for large objects, the migrated Experimental
reflow engine, and hardened probe/error paths.**

v0.3.5 makes the large-object path practical across the workflows operators use
for reorganization: `index export` and `transfer reflow` now share the same
multipart upload primitive and can cross the >5 GiB single-PUT boundary. The
release also completes the v0.3.4 `pkg/reflow` migration for the stdin reflow
subset, so that CLI path now runs through the Experimental embeddable engine
instead of a CLI-only implementation. Error handling is tightened around
content-probe terminal records and throttled provider probes. See
[`docs/releases/v0.3.5.md`](docs/releases/v0.3.5.md) for the narrative walkthrough.

### Library API

- **Changed (Experimental):** `pkg/reflow` now owns the migrated stdin reflow
  execution subset, including metadata planning, dry-run planning, record-stream
  copy execution, collision decisions, adaptive concurrency, and typed run /
  summary records. The package remains **Experimental** per
  [`docs/api-stability.md`](docs/api-stability.md); there are no Stable library
  API breaks in this release.

### Added

- **Multipart upload primitive shared by reflow and index export:** large
  provider writes now use multipart upload after the default threshold, with
  bounded part sizing, abort-on-failure cleanup, and conditional completion for
  IfAbsent-capable destinations.
- **`index export` large-artifact support:** exported hub artifacts, including a
  large `index.db`, can be written to S3-compatible hubs through multipart upload
  instead of failing at the provider's single-PUT limit.
- **`transfer reflow` large-object support:** reflowed objects can use multipart
  upload through the shared transfer primitive, while retaining collision policy,
  metadata, storage-class, and provenance behavior.
- **GCS capability-matrix coverage:** the reflow engine now has GCS-shaped
  capability fixtures proving that provider-agnostic gating handles GCS
  destination semantics.

### Changed

- **CLI-as-adapter reflow path:** stdin `transfer reflow` records for the
  migrated subset now route through `pkg/reflow`, while unsupported forms stay on
  the legacy path until later migration work.
- **Multipart dedup posture:** multipart-form ETags are never treated as blind
  byte equality for collision decisions; size/ETag verification records keep the
  comparison explicit.

### Fixed

- **Content-probe terminal error preservation:** `content probe` now recognizes
  `gonimbus.error.v1` input records as already-terminal records, re-emits them
  without minting an `INTERNAL` wrapper, and preserves retryable `TRANSIENT`
  codes.
- **Content-probe error sanitization:** content-probe error messages now use the
  same provider-error sanitizer as reflow, and parser diagnostics no longer
  carry raw input JSONL lines forward in `details.input`.
- **Throttled probe retry:** standalone reflow probe operations now retry
  throttled HEAD/body-compare attempts through the adaptive limiter before
  falling back to the existing per-object failure behavior when the bounded retry
  budget is exhausted. The checkpoint/resume classifier boundary is unchanged.

### Documentation

- Added this v0.3.5 release page and refreshed rolling release notes.
- Added operator guidance for large `index export` and `transfer reflow`
  multipart writes, including scratch/local-disk planning, multipart ETag
  posture, and provider lifecycle cleanup for incomplete multipart uploads.
- Updated the current-release README pointer for multipart and embeddable-engine
  operator notes.
- Clarified that manifest `path_template` supports a full-source-key
  placeholder, distinct from `transfer reflow` rewrite templates.

## [0.3.4] - 2026-06-27

**Google Cloud Storage as a first-class source and reflow destination, plus the
library-exposure foundation for the embeddable reflow engine.**

v0.3.4 widens the provider matrix beyond S3 and `file://` for the first time: GCS
(`gs://`) is now a first-class crawl/inspect source and `transfer reflow`
destination, riding the existing provider-dispatch seam and the same adaptive-
concurrency and IfAbsent-capability model documented for S3. This release also
lands the structural groundwork for exposing the reflow engine as an embeddable
library, with the CLI behavior unchanged. See
[`docs/releases/v0.3.4.md`](docs/releases/v0.3.4.md) for the narrative walkthrough.

### Library API

- **Added (Experimental):** `pkg/reflow` now exposes the reflow engine's
  lower-level building blocks — typed JSONL records (`Record`, `RunRecord`,
  `SummaryRecord`, `SourceRunRecord`, `Warning`, `CollisionInfo`, `ProvenanceRef`,
  and the run-config types), the adaptive-concurrency substrate
  (`ConcurrencyLimiter`, `ConcurrencyConfig`, `ResolveConcurrency`,
  `DefaultResourceProbe`), and provider-error redaction helpers
  (`SanitizeOperationCauseMessage`, `FormatErrorMessage`, `NewPathError`). These
  are **Experimental** per [`docs/api-stability.md`](docs/api-stability.md): the
  `transfer reflow` CLI is unchanged and continues to drive the internal path;
  the data/decision-plane migration onto `pkg/reflow` completes in a later release.
- **Added (Experimental):** `pkg/provider/gcs` — a Google Cloud Storage provider
  implementing the read (List/Head/Get/range) and reflow-destination
  (Put + conditional IfAbsent) contracts, mapping `429` and
  `403`+`RESOURCE_EXHAUSTED` to `provider.ErrThrottled` and `5xx` (incl. `503`)
  to `provider.ErrProviderUnavailable`.

### Added

- **Google Cloud Storage provider:** `gs://` sources and reflow destinations
  across inspect/index/tree/stream/content/doctor and `transfer reflow`. GCS
  reports the same IfAbsent honored/probe-status summary fields as S3 and plugs
  into the existing adaptive-concurrency model. Authentication follows Application
  Default Credentials and service-account keys under the established
  credential-source discipline (no URI- or manifest-sourced credential
  filepaths); `STORAGE_EMULATOR_HOST` is honored for hermetic testing only. GCS
  conditional writes support IfAbsent (skip-if-duplicate); ETag-based `If-Match`
  preconditions are not supported (GCS uses generation preconditions), so
  `--on-collision overwrite-if-source-newer` is unavailable on GCS destinations
  and fails closed.
- **Opt-in real-GCS test lane:** an opt-in integration lane validates the provider
  against a real GCS bucket (bring-your-own; see
  [`docs/development/testing.md`](docs/development/testing.md)), alongside the
  hermetic `fake-gcs-server` lane that runs on every PR.

### Changed

- **Reflow probe operations are now bounded by adaptive concurrency.** Standalone
  provider probes (source/destination HEADs and body-compare reads) acquire the
  same adaptive-concurrency slots as copies, so backoff under provider throttling
  reduces probe pressure as well as copy pressure. Acquisitions are sequential and
  never nested (deadlock-free even at the concurrency floor).
- **Dependencies:** `golang.org/x/net` upgraded to v0.56.0.

### Fixed

- **Checkpoint resume across versions:** the op-checkpoint `no_adaptive` config
  field is now omitted when unset, restoring `--resume-run` for resumable
  checkpoints written by pre-v0.3.3 builds (previously failed closed with an
  identity mismatch). A cross-version fingerprint compatibility test guards the
  boundary.

### Documentation

- New GCS quick-start in the README and release notes; README provider matrix
  updated to show GCS as supported.
- Corrected broken `transfer reflow` rewrite examples (`--rewrite-from '{key}'`)
  in the shipped v0.2.1 / v0.2.3 release notes.

## [0.3.3] - 2026-06-17

**Adaptive transfer reflow concurrency, safer reflow error surfaces, priority
probe fallbacks, and release-package docs.**

v0.3.3 makes large `transfer reflow` runs faster to right-size and safer to
trust, while tightening reflow collision, probe fallback, and error-output
surfaces. See [`docs/releases/v0.3.3.md`](docs/releases/v0.3.3.md) for the
narrative walkthrough.

### Library API

- **Added:** `pkg/provider/s3.Config` now exposes optional
  `MaxIdleConnsPerHost` and `MaxConnsPerHost` HTTP transport sizing fields.
  Zero values preserve the AWS SDK defaults; transfer reflow uses the fields to
  opt into resource-capped S3 client tuning without changing unrelated
  provider construction paths.

### Added

- **Priority XML XPath probe extraction:** `xml_xpath` extractors now support
  `xpath_priority` for ordered fallback tags, with audit fields for
  `resolved_priority`, `resolved_xpath`, `truncated_fallback`, and
  `truncated_fallback_count`.
- **Adaptive transfer reflow concurrency:** `transfer reflow --parallel` now
  acts as a requested ceiling by default. The effective ceiling is bounded by
  resource caps, adaptive mode backs off on throttling and freezes ramp-up
  during connection-error streaks, and run/summary output includes additive
  `concurrency_*` fields for operator audit.
- **Capability-aware reflow collision fallback:** non-overwrite collision modes
  on S3-compatible destinations that do not honor `If-None-Match: *`, or whose
  semantic IfAbsent probe is inconclusive, now fail closed to a HEAD/compare
  fallback before writing. Reflow emits a structured warning and terminal
  `gonimbus.reflow.summary.v1` fields for the IfAbsent probe status, fallback
  activation, and degraded-path object count.

### Fixed

- **Truncated priority fallbacks fail closed:** lower-priority XPath values
  observed before `max_bytes` or a fixed-window boundary now route to
  quarantine by default instead of being treated as final normal-routing
  matches.
- **Reflow abort and per-object errors are classified and sanitized:** resumable
  transfer-reflow aborts now emit a compact classified cause on the operation
  error record, and published reflow error/warning messages redact provider URL
  credential material.
- **S3 signing coverage:** profile-based and SDK default-chain environment
  credential paths now have hermetic SigV4 signing regressions alongside the
  existing static-key coverage.

## [0.3.2] - 2026-06-15

**Package-manager distribution plumbing and release build matrix updates.**

v0.3.2 prepares the release pipeline for Homebrew and Scoop publishing and
updates the release build matrix to match those distribution targets. See
[`docs/releases/v0.3.2.md`](docs/releases/v0.3.2.md) for the narrative
walkthrough.

### Added

- **Package-manager release evidence:** the release upload ceremony now prints
  the GitHub download URL and SHA256 for the Homebrew and Scoop assets consumed
  by downstream package-manager manifests.

### Changed

- **Release build matrix narrowed to package-manager targets:** release builds
  publish Linux AMD64, Linux ARM64, macOS ARM64, Windows AMD64, and Windows
  ARM64 artifacts. The native Intel Mac artifact,
  `gonimbus-darwin-amd64`, is no longer published.

## [0.3.1] - 2026-06-14

**Embedded S3 auth controls, toolchain pinning, and dependency refresh.**

v0.3.1 adds explicit embedded S3 credential modes for unsigned public reads and
caller-managed AWS credential providers, pins the module toolchain directive to
the release-lane Go patch version, and refreshes the past-cooling AWS SDK,
smithy, routing, and platform dependencies. See
[`docs/releases/v0.3.1.md`](docs/releases/v0.3.1.md) for the narrative
walkthrough.

### Library API

- **Added:** `pkg/provider/s3.Config` now supports `Anonymous` unsigned
  read-only construction and caller-injected `CredentialsProvider` values.
  Anonymous reads send no `Authorization` header and never fall back to ambient
  credentials; S3 write methods fail closed with `provider.ErrAnonymousReadOnly`
  joined with `provider.ErrAccessDenied`. `pkg/provider` also adds the stable
  `ErrAnonymousReadOnly` sentinel and `IsAnonymousReadOnly` helper. Credential
  precedence is now explicit: injected provider, static keys, profile, then the
  AWS SDK default chain. `Profile` is ignored when a higher-priority credential
  source is configured; pass region and endpoint fields directly in that shape.

### Changed

- **Go toolchain directive pinned:** `go.mod` now declares `toolchain go1.26.4`
  to match the CI, dependency-security, and release workflow Go pins used for
  v0.3.1 dependency evidence.
- **AWS SDK family updated:** AWS SDK for Go v2 packages now track the latest
  past-cooling v1.41.12 root SDK, S3 service v1.102.2, and smithy-go v1.27.2
  family for the v0.3.1 dependency refresh.
- **Routing and platform packages updated:** `github.com/go-chi/chi/v5` and
  `golang.org/x/sys` now track past-cooling dependency versions for the v0.3.1
  package refresh.

### Fixed

- **Probe quarantine routing preserved for derived fields with missing
  required sources:** when a required `derived` field depends on an extractor
  configured with `on_missing: quarantine` and that source is missing, the
  record now routes to quarantine instead of rendering with an unresolved
  field. Adds prober and until-resolved regressions for date-derived partition
  fields.

### Security

- **Spoofable forwarded-IP trust removed from the default server stack:**
  the server no longer installs `chi/middleware.RealIP`, so the default
  request path does not rewrite `RemoteAddr` from caller-supplied
  `X-Forwarded-For` or `X-Real-IP` headers.
- **Anonymous S3 mode fails closed for writes:** S3 write methods return
  `provider.ErrAnonymousReadOnly` joined with `provider.ErrAccessDenied`
  before issuing provider requests when a provider is configured for unsigned
  public reads.

## [0.3.0] - 2026-06-12

**Pure-Go index defaults, resumable long-running operations, provider-dispatch
reflow, and release-gate hardening.**

v0.3.0 moves the default local index store to pure-Go SQLite, adds resumable
failure handling for long-running index and reflow operations, expands
provider-dispatched transfer reflow so local file trees can be copied into cloud
object stores, and tightens release security gates. See
[`docs/releases/v0.3.0.md`](docs/releases/v0.3.0.md) for the narrative
walkthrough.

### Library API

- **Added:** `pkg/provider` now exposes `ErrCredentialsRefreshFailed` and
  `IsCredentialsRefreshFailed` so provider adapters can mark credential-cache
  refresh failures with a stable sentinel instead of requiring command-layer
  string matching. This is additive; existing provider implementations do not
  need changes unless they want to surface refresh failures through the new
  helper.
- **Added:** `pkg/output` now exposes `ErrCodeTransient` for temporary network
  and transport failures such as DNS errors, connection resets, mid-transfer
  EOFs, I/O timeouts, and TLS handshake timeouts. Transfer and reflow JSONL
  errors use this class so callers can distinguish retryable transport failures
  from fatal or internal errors.
- **No Stable break from provider dispatch:** the provider-dispatch work lives
  under `internal/providerdispatch`. The Stable `pkg/uri` and
  `pkg/provider/s3` surfaces are unchanged in v0.3.0.

### Added

- **Pure-Go index-store default:** default builds now use the pure-Go
  `modernc.org/sqlite` index-store driver, which simplifies static,
  cross-compiled, and container builds (no C toolchain required). The
  libsql/Turso driver remains available behind the explicit `gonimbus_libsql`
  build tag.
- **Sensitive local operation checkpoints:** `pkg/opcheckpoint` adds the
  operation-checkpoint substrate for failed-resumable runs, including
  credential-material scanning, lease files, and identity validation before
  resume.
- **Failed-resumable index discovery:** `index list` and `index stats --runs`
  now expose the latest run ID and a safe `--resume-run` command for
  failed-resumable index runs; `index stats` also reports a separate
  failed-resumable run count, and `index query` warns when the latest run is a
  partial failed-resumable checkpoint.
- **Resumable runtime failure summaries:** `index build`,
  `index enrich-with-head`, and `transfer reflow` now print short redacted
  stderr summaries for failed-resumable runtime interruptions, including
  `run_id`, `status`, `error_class`, progress counters, and the safe
  `--resume-run` command. Runtime failures no longer need command-help dumps
  for operator classification; argument errors still show usage.
- **Resume lease heartbeat:** `--resume-run` operations now renew their
  operation-checkpoint lease while they are running, so long index build,
  enrich-with-head, and transfer reflow resumes do not outlive the fixed lease
  claimed at startup.
- **Provider-dispatched reflow:** command code now routes source and destination
  construction through provider dispatch, allowing `transfer reflow` to copy
  from `file://` local directory sources to S3 or S3-compatible destinations
  with the same rewrite, metadata, collision, dry-run, checkpoint, and audit
  machinery used for object-store sources.
- **Transfer/reflow failure taxonomy:** transfer and reflow error records now
  classify temporary network and transport failures as `TRANSIENT` and emit the
  reflow reason `transient.network`.
- **Dependency security workflow:** high-and-critical dependency enforcement
  now runs in CI on pull requests, `main` pushes, a daily schedule, and manual
  dispatch.

### Changed

- **Version bumped to `0.3.0`:** version stamping continues through `VERSION`,
  `.fulmen/app.yaml`, the embedded app identity mirror, and
  `internal/buildinfo/VERSION`.
- **Local pre-push stays scoped:** `make prepush` and the installed pre-push hook
  now run changed-file scoped format, lint, and security checks without the
  unscopable dependency category. Dependency enforcement is centralized in the
  CI workflow where clean-room scans are authoritative.
- **CI and release toolchains use Go `1.26.4`:** release-lane workflows now use
  the patched Go toolchain for the v0.3.0 build and dependency gates.

### Fixed

- **Resume collision false-conflicts:** transfer reflow no longer treats an
  in-flight-at-interruption duplicate collision as a fatal conflict when the
  duplicate source content is byte-identical to already completed destination
  data.
- **No more `[unknown]` for transient transport failures:** failed transfer and
  reflow chunks now carry the machine-actionable `TRANSIENT` class for known
  temporary network conditions instead of falling through to internal or unknown
  surfaces.

### Security

- **Provider read confinement:** local file provider reads stay confined to the
  resolved source root, and symlinks remain skipped by default unless
  `--symlinks=follow` is explicitly selected for reflow.
- **No high-or-critical dependency violations at the release gate:** the direct
  `golang.org/x/net` dependency is updated to `v0.55.0`, CI/release builds use
  Go `1.26.4`, and the dependency policy records no new suppressions.

## [0.2.3] - 2026-05-31

**Stream put completion, reflow freshness arbitration, and release-surface guardrails.**

v0.2.3 completes the stream write path, adds destination verification for
reflow outputs, adds a newest-wins collision mode for mirror-style reflow, and
formalizes Stable API and public-repository hygiene guardrails. See
[`docs/releases/v0.2.3.md`](docs/releases/v0.2.3.md) for the narrative
walkthrough.

### Library API

- **Breaking:** `provider.MultipartUploader` now requires implementers to add
  `UploadPart` and `CompleteMultipartUpload`, and the package adds `PartETag`
  plus optional conditional multipart completion for streaming writes.
  Migration path: provider implementations that advertise multipart capability
  must implement the full create/upload-part/complete/abort lifecycle, or stop
  advertising `MultipartUploader` until they can support it. The bundled S3
  provider implements the expanded interface and conditional completion used to
  preserve no-overwrite semantics. Advance-notice status: documented for
  v0.2.3; known embedders must be notified before release per
  `docs/api-stability.md`.

### Added

#### Stream writes

- **`stream put` upload path** — `gonimbus stream put` now uploads stdin to a
  destination object, including both raw single-object mode and framed JSONL
  batches produced by `stream get`.
- **Destination authority for framed uploads** — the CLI destination remains
  authoritative. Exact destinations write one framed object; trailing-slash
  destinations act as roots for multi-object batches. Frame `dest_key` values
  are ignored unless `--dest-from-frame` is explicitly enabled.
- **Multipart streaming writes** — S3 uploads switch to multipart at the
  configured threshold, emit `gonimbus.stream.progress.v1` progress records,
  and avoid full-object buffering once multipart upload begins.

#### Reflow and verification

- **Freshness-based collision mode** —
  `transfer reflow --on-collision overwrite-if-source-newer` overwrites an
  existing destination only when the observed source `LastModified` is newer, or
  when timestamps are equal but sizes differ. Destination overwrite is guarded
  with the observed destination ETag so concurrent mutation yields a
  deterministic skipped record.
- **Pair verification command** — `gonimbus inspect-pair` reads reflow JSONL and
  verifies terminal write claims against destination HEAD results, emitting
  per-object `gonimbus.inspect.pair.v1` records plus a summary record.

#### API and repository guardrails

- **Stable API manifest and soft diff gate** — `docs/api-stability.md`,
  `docs/development/api-stability.md`, and `make api-stability` now define and
  check the Stable embedded library surface before release.
- **Public-surface policy conformance** — repository-facing agent and
  contributor guidance now points to the canonical 3leaps OSS policy and keeps
  sensitive local data structurally outside the repository working tree.

### Changed

- **Bumped version to `0.2.3`** for this release. Version stamping continues
  through `VERSION`, `.fulmen/app.yaml`, the embedded app identity mirror, and
  `internal/buildinfo/VERSION`.

### Security

- **Framed destination keys are constrained under the CLI destination root.**
  `stream put --dest-from-frame` rejects absolute paths, URI-like prefixes,
  root-anchored paths, and `..` traversal before writing destination objects.
- **Conditional multipart completion preserves no-overwrite semantics** for S3
  destinations that support the expanded multipart interface.
- **Sensitive local data policy is explicit.** Public docs state the principle
  that sensitive or proprietary data belongs outside repository working trees,
  rather than relying on `.gitignore` as a security boundary.

## [0.2.2] - 2026-05-26

**Index archive operations and local-tree reflow — richer indexed state with safer local-source defaults.**

v0.2.2 improves the operator path for two common workflows: migrating local
trees through `transfer reflow`, and using indexes to plan work around object
storage class, archive, restore, and content-type state. It also moves CI and
release builds to Go `1.26.3` so release evidence uses the current patched
toolchain. See [`docs/releases/v0.2.2.md`](docs/releases/v0.2.2.md) for the
narrative walkthrough.

### Added

#### Local-tree reflow

- **File source provider parity** — `transfer reflow` now accepts
  `file:///absolute/source-root/` sources and routes them through the same
  rewrite, collision, metadata, checkpoint, dry-run, and audit machinery used
  for object-store sources (GON-035).
- **Safe hidden-path default** — local-tree reflow skips hidden files and
  dot-directories by default. Use `--hidden=include` only after reviewing
  `--dry-run` output and adding explicit excludes for non-hidden generated
  paths that should not be copied (GON-035).
- **Local-source disclosure controls** — per-object output uses
  `file://local/<relative-path>` rather than absolute source roots; checkpoint
  files and run metadata remain local operational artifacts and should be
  treated accordingly (GON-035).

#### Index metadata

- **LIST-derived storage class** — index builds now persist provider storage
  class values returned during listing and emit `storage_class` in query JSONL
  when present (GON-036).
- **Storage-class query filter** — `index query --storage-class` filters exact,
  case-sensitive storage class values. The flag is repeatable and accepts
  comma-separated values, including canonical-by-ETag mode (GON-036).
- **HEAD enrichment command** — `index enrich-with-head <index-set-id>` caches
  HEAD-derived archive status, restore state, restore expiry, content type, and
  `head_enriched_at` for filtered candidate rows in an existing index
  (GON-037).
- **Enrichment-aware query output** — `index query` can emit the enriched
  archive/restore/content-type fields and filter by `--enriched-after`
  (GON-037).

### Changed

- **Bumped version to `0.2.2`** for this release. Version stamping continues
  through `VERSION`, `.fulmen/app.yaml`, the embedded app identity mirror, and
  `internal/buildinfo/VERSION`.
- **CI and release workflows pin Go `1.26.3`** and use
  `golangci-lint-action` v2.11.2 so builds, scans, and release artifacts run
  on a patched Go 1.26 lane (PR #53).
- **Provider dispatch docs expanded** to document file source URI handling and
  future provider extension points for reflow (GON-035).

### Security

- **Local hidden paths are excluded unless requested.** This reduces accidental
  publication of dotfiles such as `.env`, `.git/*`, `.DS_Store`, and cache
  directories when using a local directory as a reflow source. The hidden
  filter is intentionally not gitignore-aware; use explicit `--exclude` rules
  for generated non-hidden paths.
- **HEAD enrichment stores only selected metadata.** Credentials are never
  stored in the index; provider access is reconstructed from index identity and
  runtime flags or the normal SDK credential chain. Enrichment mutates only
  HEAD-derived fields and does not overwrite LIST-derived object identity.

## [0.2.1] - 2026-05-22

**Reflow destination metadata — explicit operator control with explicit disclosure discipline.**

v0.2.1 is a focused release for reflow operators who need destination objects to carry durable, queryable metadata while preserving gonimbus's safety posture: metadata writes are opt-in, per-object derivation is allow-listed, failure paths redact source values, and destination system metadata remains owned by the object store. See [`docs/releases/v0.2.1.md`](docs/releases/v0.2.1.md) for the narrative walkthrough.

### Added

#### Reflow destination metadata

- **Caller-controlled destination user metadata** — `transfer reflow` can write explicit destination user metadata with repeatable `--metadata-set key=value`. Keys are normalized to lower case and repeated keys use last-value-wins semantics (GON-033).
- **Source metadata policies** — `--metadata-policy clear|preserve|merge` controls whether source user metadata is omitted, copied, or copied then overridden by `--metadata-set`. Preserve/merge reject source metadata key collisions after lower-case normalization instead of guessing (GON-033).
- **Content-Type and storage-class controls** — `--preserve-content-type` copies the source content type, and `--destination-storage-class <class>|propagate` writes an explicit destination storage class or propagates the source class when it is a valid PUT target (GON-033).
- **Metadata-aware provider writes** — S3 and file providers implement metadata-aware PUT paths, including conditional PUT variants, so destination metadata composes with the default `skip-if-duplicate` reflow path. Local file destinations store metadata in cleartext `.gnb-meta.json` sidecars (GON-033).

#### Per-object metadata derivation

- **Per-object source-key projection** — `--metadata-set-from-source-key dest=src` copies one named source user-metadata key into one named destination user-metadata key per object (GON-034).
- **Per-object expression derivation** — `--metadata-set-from-source-derived dest=expr` supports the v1 expression set: `meta.<key>.<subfield>`, `urldecode(meta.<key>).<subfield>`, `system.etag`, `system.last_modified`, `system.content_length`, `system.content_type`, `system.storage_class`, and one string-literal concatenation such as `system.etag + "-src"` (GON-034).
- **Missing-source routing** — `--metadata-on-missing-source skip|fail|empty` controls missing source keys, invalid JSON, URL-decode failures, null values, and unsupported non-scalar JSON results. `fail` emits a redacted `gonimbus.error.v1`; with `--on-collision=quarantine`, failed derivations route to the quarantine prefix (GON-034).

### Changed

- **Bumped version to `0.2.1`** for this release. Version stamping continues through `VERSION`, `.fulmen/app.yaml`, the embedded app identity mirror, and `internal/buildinfo/VERSION`.
- **CI and release workflows pin Go `1.25.10`** so vulnerability scans, SBOMs, and release artifacts are attributable to an exact patched toolchain (PR #46).

### Fixed

- **SQLite open-ordering WAL flake reduced** — index-store setup now applies `busy_timeout` before switching WAL mode, reducing intermittent lock failures when opening root index databases under test and local operations (PR #46).
- **YAML formatting drift pinned** — `.yamlfmt` now pins indentation/line endings and `.yamllint` disables mandatory document starts so local `make fmt` and CI format checks agree (PR #46).

### Security

- **Destination metadata disclosure is explicit operator surface.** Values supplied by `--metadata-set`, copied or derived from source metadata, preserved content type, propagated storage class, and file-provider metadata sidecars are durable destination metadata. They are visible to callers with destination HEAD/GET or filesystem access and are not redacted at destination. Use `--metadata-policy clear` plus explicit `--metadata-set*` allow-lists when source metadata might contain credential URIs, tokens, or other sensitive values.
- **Per-object derivation is an allow-list, not projection.** Gonimbus rejects wildcard destination keys and wildcard subfield projection. v1 derivation accepts only scalar JSON outputs (string, number, bool); null, arrays, and objects route through `--metadata-on-missing-source` so a nested object cannot be serialized wholesale by accident.
- **Destination system metadata remains a non-goal.** `system.<field>` expressions read source-side system fields as derivation inputs, but the new flags write only destination user metadata (`x-amz-meta-*` for S3-compatible stores). Gonimbus does not set destination ETag, destination LastModified, destination ContentLength, or other object-store-generated system fields. Content-Type and StorageClass are writable only through their explicit GON-033 flags.
- **Derivation failures redact source values.** Invalid JSON, invalid URL escapes, decoded-invalid JSON, non-scalar fail mode, and source canonical-collision failures name the destination key, expression, and reason kind without echoing raw source metadata values in error JSONL, checkpoints, or default stderr.

### Internal

- **Stable dependency evidence artifacts** — `make dependencies` now writes stable SBOM and vulnerability report outputs under `sbom/`, with an explicit goneat vulnerability policy for release-lane scans (PR #46).
- **CI hardening lane documented** — `docs/development/ci.md` records the exact Go pin, glibc runner rationale, workspace-relative GOPATH, bash-shell requirement, and SBOM/vulnerability report behavior.

## [0.2.0] - 2026-05-20

**Library-enabling and scaling — far more complex reflow patterns, the same predictable engine.**

v0.2.0 grows gonimbus along three axes simultaneously: stable library surface for Go consumers, deeper content-aware reflow patterns (derived vars, mixed segments, lookups, mirrored sidecars, Hive partitions, canonical-by-ETag dedup), and correctness primitives that keep behavior right at scale (atlas, conditional CAS, parallel-race arbitration). See [`docs/releases/v0.2.0.md`](docs/releases/v0.2.0.md) for the narrative walkthrough.

### Added

#### Library enablement

- **Public URI parser package** ([`pkg/uri`](pkg/uri)) — Go library consumers can use the CLI's existing S3 URI parsing behavior without importing internal command code. Promoted from `internal/uri` per the library-consumer roadmap (GON-021).
- **Library-consumer config contract documentation** ([`docs/library-consumers.md`](docs/library-consumers.md)) — credential resolution, env-var precedence, hermetic-embedder posture, dep-tree boundary, and provider-construction patterns for downstream Go modules (GON-022).
- **`internal/buildinfo` package** — version resolution chain that gives downstream `go install`-built binaries a reliable version string (see Fixed: #6 below).

#### Reflow + probe sophistication

- **Probe derived variables** — `content probe` computes declared `derived` vars from extracted values via `substring`, `regex_capture`, `format`, `pad`, `lowercase`, `uppercase` transforms. Pair with `transfer reflow`'s mixed-segment rendering (below) to build destination keys without a separate transform step (GON-024).
- **Mixed rewrite segments** — `transfer reflow` renders one placeholder with literal prefix/suffix inside a single path segment, e.g. `year={year}` or `events/{tenant}-{date}/`. Earlier releases required a whole segment per placeholder (GON-024).
- **Probe lookup derived variables** — new `lookup` transform with `regex`, `prefix`, and `exact` match modes for table-driven derivations. Pairs with `content probe --rewrite-from` so probe recipes can derive values from source-key captures before `transfer reflow` renders destination keys (GON-031).
- **Reflow provenance sidecars** — `transfer reflow` writes a `.gnb.json` sidecar next to each destination object recording source URI, derived fields, and rewrite path. Sibling default; sidecar-suppression flag also available (GON-019).
- **Mirrored provenance sidecar placement** — `--provenance-sidecar-root` for `transfer reflow` allows sidecars to be written under a separate same-bucket root while preserving the sibling default and the provider-relative `provenance.key` contract (GON-025).
- **Reflow collision modes refined** — `--on-collision skip-if-duplicate` is the clearer default name; new `--on-collision quarantine` with `--collision-quarantine-prefix` for explicit isolation of collisions; nested `collision` metadata on collision records (GON-020).
- **Hive-style partition emission pattern** — `docs/user-guide/examples/` documents the recommended pattern for emitting Hive-style partitions (`category={x}/date={y}/`) by composing `extract` + `derived` + mixed-segment rewrites without per-pattern engine changes (GON-027).
- **Index query canonical-by-ETag mode** — `gonimbus index query --canonical-by-etag` emits one canonical object per non-empty ETag group, with deterministic tie-breaks, mixed empty-ETag passthrough records, output-record-count `--count` / `--limit` semantics, and optional `--include-alternates` for audit detail. Solves content-fingerprint deduplication at query time (GON-032).

#### Scaling correctness

- **Local atlas phase A** — `gonimbus atlas build` produces a derived view across completed indexes for cross-run analytics. Phase A scope: local-only build, single-index input; cross-index aggregation tracked for later (GON-012).
- **Conditional `latest.json` CAS** — `gonimbus index export` uses If-Match / If-None-Match on `latest.json` writes for fail-closed publish semantics on substrates that support it. Best-effort fallback preserved for v0.1.x-compatible hubs (GON-013).
- **Atomic conditional puts** — provider-level `If-None-Match: *` for objects requiring race-safe creation; opt-in via provider capability detection (GON-014).
- **Opt-in build summary** — `gonimbus index build --summary` emits a per-run JSONL summary (object counts, byte totals, prefix breakdown) without changing default streaming behavior (GON-011).
- **Until-resolved probe reads** — `content probe` reads until a configurable resolution condition is met (rather than fixed byte ranges), reducing wasted reads on variable-length headers (GON-017).
- **`doctor` S3 endpoint + region flags** — `gonimbus doctor --endpoint URL --region NAME` for explicit probe target overrides during cross-account / cross-endpoint validation (GON-016).

#### Server + stream

- **Stream put** — `gonimbus stream put` accepts raw stdin (`feat(stream): add raw stdin put command`) and framed input (`feat(stream): accept framed stream put input`) for bulk-load patterns (GON-015).
- **Local job control API** — `gonimbus serve` exposes a local job control API (start/list/cancel) for scripted orchestration against a single runner (phase A of the control-plane work).

### Changed

- **(Breaking) Reflow collision Phase B — flat fields removed**. The legacy flat collision JSONL fields (`collision_kind`, `collision_etag`, `collision_size_bytes`) are removed from `gonimbus.reflow.v1` records after the GON-020 Phase A warning window. Audit tooling must read the nested `collision` object as the sole current collision representation (GON-026).
- **`--on-collision log` is deprecated** as an alias for `skip-if-duplicate`; the new name is the default. Will be removed in a later release (GON-020).
- **Bumped version to `0.2.0`** for this release. Version stamping respects the `internal/buildinfo` chain so `go install ...@v0.2.0` reports the correct version (see Fixed: #6 below).
- **Bumped bounded dependency set** ([PR #42](https://github.com/3leaps/gonimbus/pull/42)) — gofulmen v0.3.2 → v0.3.5 (pulls crucible v0.4.9 → v0.4.12, doublestar v4.9.1 → v4.10.0, zap v1.27 → v1.28); AWS SDK family coherent settle (`aws-sdk-go-v2` v1.41.0 → v1.41.7, `s3` v1.95.0 → v1.101.0, plus credentials, config, IMDS, smithy, service-internal modules); `chi/v5` v5.2.3 → v5.2.5; `mapstructure/v2` v2.4.0 → v2.5.0; `cobra` v1.10.1 → v1.10.2; `golang.org/x/net` v0.48.0 → v0.54.0; `golang.org/x/time` v0.14.0 → v0.15.0. `modernc.org/sqlite` + libsql substrate updates deferred to GON-023's delivery PR.
- **Steady-state index operations docs clarified** — `docs/user-guide/steady-state-index-operations.md` documents the operator-side lifecycle for index sets after the initial build (refresh, snapshot, gc cadence).
- **Index build mental model docs** ([PR #39](https://github.com/3leaps/gonimbus/pull/39)) — `docs/user-guide/index-build-mental-model.md` walks through the prefix-first listing approach and where index-build cost sits in the larger pipeline.

### Fixed

- **Parallel content-aware reflow now preserves the default `skip-if-duplicate` contract** (GON-030). When `transfer reflow --parallel N>1` routes multiple source objects to the same destination key, a single gonimbus process now arbitrates per destination key before conditional writes. The first object lands; byte-identical race losers emit `status=skipped reason=collision.duplicate` instead of redundant `complete collision=no` records. This protects the single-run operator path even on S3-compatible substrates that accept but do not enforce `If-None-Match: *`; cross-process and cross-operator races still depend on substrate-level conditional-write support. Active in-memory gates are bounded to in-flight destination keys; per-run observed destination keys are tracked in the checkpoint database. For local file destinations, same-size collision losers fall back to a byte comparison because file version tokens are not source-provider ETags.
- **`go install`-built binaries now report the correct version** ([#6](https://github.com/3leaps/gonimbus/issues/6)). The Makefile's `-ldflags -X main.version=…` injection is no longer the only source of the version string — a new `internal/buildinfo` package resolves the version from a three-tier chain: ldflags overrides → `runtime/debug.ReadBuildInfo` (covers `go install module@vX.Y.Z`) → embedded `VERSION` file (covers `go install ./cmd/...` from a working tree). The repo-root `VERSION` is mirrored into `internal/buildinfo/VERSION` by `make sync-app-version`. Regression guard in `test/integration/standalone_binary_test.go` builds the binary with no ldflags and asserts the reported version matches `VERSION`.
- **Pinned yamlfmt config** ([#3](https://github.com/3leaps/gonimbus/issues/3)). Added `.yamlfmt` at repo root with `pad_line_comments: 2` so that local `make fmt` (via goneat) and CI's `yamlfmt -lint .` agree on inline-comment spacing. Without this pin, `make check-all` could rewrite YAML files to a different convention than CI expected, producing unrelated format-check failures.
- **`content probe` decodes declared XML charsets** ([#28](https://github.com/3leaps/gonimbus/pull/28)) — previously assumed UTF-8 and produced extraction errors on POS XML with declared non-UTF-8 encodings. Now honors the XML declaration's `encoding` attribute (GON-018).
- **`inspect` reports capped output explicitly** ([#8](https://github.com/3leaps/gonimbus/pull/8)) — when the result set hits the configured `--limit`, output now includes a clear truncation marker rather than silently capping. Caught after a recurring "where are the rest of my objects?" pattern.
- **`index doctor` honors positional targets** ([#10](https://github.com/3leaps/gonimbus/pull/10)) — `gonimbus index doctor <hub-uri>` was being ignored in favor of `--hub`; both forms now work.
- **`index` accepts positional hub URIs** ([#14](https://github.com/3leaps/gonimbus/pull/14)) — subcommand uniformity.
- **`index` hints expired SSO during dry-run** ([#15](https://github.com/3leaps/gonimbus/pull/15)) — clear "your AWS SSO session has expired, run `aws sso login --profile X`" message instead of an opaque SDK error.
- **Suppress S3-compatible checksum warning noise** ([#16](https://github.com/3leaps/gonimbus/pull/16)) — non-AWS S3-compatible endpoints (Wasabi, etc.) no longer flood stderr with checksum-algo-not-supported warnings.
- **Clean diagnostic command output** ([#9](https://github.com/3leaps/gonimbus/pull/9)) — `gonimbus doctor` and related commands emit cleaner JSON without redundant log preamble interfering with downstream `jq` pipelines.
- **Reject `transfer reflow` stdin positional args** ([#11](https://github.com/3leaps/gonimbus/pull/11)) — explicit error rather than silently ignoring the args.
- **CLI root help template refresh** ([#13](https://github.com/3leaps/gonimbus/pull/13)) — replaced an outdated copy of cobra's root help template with the upstream baseline.

### Internal

- **CI runner image bumped to `goneat-tools-runner-glibc:v0.4.2`** ([PR #43](https://github.com/3leaps/gonimbus/pull/43)) — the glibc variant is required because gonimbus's cgo path pulls in libsql's glibc-compiled static library. Workflow adjustments to support the new image: workspace-relative `GOPATH` (v0.3+ images run as non-root and don't expose a writable `/opt/gopath`), explicit `defaults.run.shell: bash` (the `-glibc` image doesn't surface bash as GHA's default). All three findings captured in [`docs/development/ci.md`](docs/development/ci.md) for the next contributor.
- **Dataeng role updated for parallel operations** — `config/agentic/roles/dataeng.yaml` documents the parallel-ops considerations dataeng should apply when running large reflow / index workloads.

## [0.1.8] - 2026-05-05

### Added

#### Index Hub (`index hub` + `index export` + `index hydrate`)

- **Index Hub CRUD** (`internal/cmd/index_hub.go`)
  - `gonimbus index hub init` — create a new hub root with marker file
  - `gonimbus index hub ls` — list index sets and their runs at a hub
  - `gonimbus index hub show` — show details for a specific index set or run
  - `gonimbus index hub set-latest` — advance the `latest.json` pointer for an index set (requires committed run)
  - `gonimbus index hub rm-run` — remove a specific run; protects `latest` unless `--force`
  - `gonimbus index hub gc` — garbage-collect runs by `--keep N` or `--before DATE`; supports `--dry-run` and `--json`

- **Index Export** (`internal/cmd/index_export.go`)
  - `gonimbus index export` publishes an index run to a file or S3 hub
  - Atomic publish sequence: `index.db` → `identity.json` → `complete.json` (commit marker) → `latest.json`
  - SHA-256 + size integrity manifest in `complete.json`
  - `latest.json` is best-effort last-writer-wins for v0.1.x; CAS / fail-closed semantics tracked for v0.2.x

- **Index Hydrate** (`internal/cmd/index_hydrate.go`)
  - `gonimbus index hydrate` downloads a published index run from a hub
  - Resolves run via `latest.json` pointer or explicit `--run-id`
  - SHA-256 + size verification for `index.db` and `identity.json`
  - Rejects uncommitted runs (no `complete.json`)
  - Saves `complete.json` to destination for provenance

- **Hub JSON Schemas** (`schemas/gonimbus/v1.0.0/`)
  - `index-hub.schema.json` — hub marker
  - `index-hub-complete.schema.json` — run commit marker with integrity manifest
  - `index-hub-latest.schema.json` — index-set latest pointer
  - `index-hub-identity.schema.json` — index set identity descriptor

#### Index Query Flags

- **`--index-set <id>`** (`internal/cmd/index_query.go`) — explicit index-set selection when multiple sets share a base URI; resolves prefix or full `idx_<64hex>` form
- **`--output <uri>`** — stream query results to S3 or `file://` destinations (in addition to stdout)

#### Workspace Pattern

- **Workspace convention** (`docs/user-guide/workspace.md`)
  - `workspace.yaml` schema and layout convention
  - Documented shard strategies for date-partitioned data
  - Operational flows: build+publish, hydrate+query, extract+reflow, hub maintenance
  - Rewrite template guidance and scheduling patterns

#### Role Catalog

- **Dataeng role** (`config/agentic/roles/dataeng.yaml`) — pipeline operations, manifests, integration testing; updated for v0.1.8 hub/workspace operations
- **Attribution policy** (`AGENTS.md`) — strengthened to mandate `noreply@3leaps.net` and reject model-provider domains

### Changed

- **Pre-push hook** (`.goneat/hooks.yaml`) — assess gate scoped to `--new-issues-only --new-issues-base origin/main` so unrelated changes don't pay for legacy lint debt
- **AGENTS.md** — repository-local workflow references retired; sensitive
  local context now stays outside this public repository
- **AGENTS.md DO NOT list** — replaced narrow local-workflow guidance with a
  broader prohibition on referencing client data, paths, or identifiers in repo
  content

### Fixed

- **`gonimbus index hub gc --json`** silently no-oped deletions (`internal/cmd/index_hub.go`) — fixed to honor `--dry-run` correctly and emit per-run outcomes (artifacts deleted, errors) in the JSON envelope; regression test added
- Five gosec G115 / G703 findings annotated with rationale (provably bounded conversions; user-supplied CLI paths)
- One golangci-lint QF1012 (`fmt.Sprintf` → `fmt.Fprintf`) in `pkg/manifest/validate.go`

### Removed

- **Guardian browser-intercept hooks** (`.goneat/hooks/pre-commit`, `.goneat/hooks/pre-push`) — regenerated without `--with-guardian` for the larger team feature-branch workflow

## [0.1.7] - 2026-01-28

### Added

#### Transfer Reflow (`transfer reflow`)

- **Transfer Reflow Command** (`internal/cmd/transfer_reflow.go`)
  - `gonimbus transfer reflow <source-uri>` copies objects while rewriting keys
  - Template-based path variable extraction and substitution
  - Supports probe-derived variables (e.g., `{business_date}` from content)
  - Parallel copy with configurable workers (`--parallel`, default 16)
  - Checkpoint/resume with SQLite state (`--checkpoint`, `--resume`)
  - Dry-run mode for previewing writes (`--dry-run`)
  - Collision detection and handling (`--on-collision log|fail|overwrite`)

- **Reflow Package** (`pkg/transfer/`)
  - `ReflowRewrite` for template parsing and key transformation
  - Path segment variable extraction (`{program}`, `{site}`, `{date}`, etc.)
  - Support for probe-derived variables via `ApplyWithVars`
  - Wildcard segments (`{_}`) for ignored path components

#### Content Probe (`content probe`)

- **Content Probe Command** (`internal/cmd/content_probe.go`)
  - `gonimbus content probe <uri>` extracts derived fields from content
  - Config-driven extraction rules (`--config probe.yaml`)
  - Bulk processing via `--stdin`
  - Output modes: `--emit probe|reflow-input|both`
  - Parallel probing with `--concurrency` (default 16)

- **Probe Package** (`pkg/probe/`)
  - XPath extractor for XML content (`//TagName`, `/a/b/c`)
  - Regex extractor with named/numbered capture groups
  - JSON path extractor (`$.a.b[0].id`)
  - Configurable byte window (`--bytes`, default 4096)

#### file:// Provider

- **Local Filesystem Support** (`pkg/provider/file/`)
  - `file://` URIs as transfer reflow destinations
  - Automatic directory creation
  - Collision detection for existing files
  - Overwrite support (`--overwrite --on-collision overwrite`)

#### Bulk Input Support

- **Bulk Content Head** (`internal/cmd/content_head.go`)
  - `gonimbus content head --stdin` for parallel multi-object inspection
  - JSONL input from inspect or index query output
  - Configurable concurrency (`--concurrency`)

### Changed

- Content commands consistently emit `gonimbus.error.v1` for errors
- Transfer reflow accepts `gonimbus.reflow.input.v1` records from probe

## [0.1.6] - 2026-01-25

### Added

#### Content Inspection Commands (`content head`)

- **Content Head Command** (`internal/cmd/content_head.go`)
  - `gonimbus content head <uri>` reads the first N bytes of an object
  - Uses HTTP Range requests when provider supports them (falls back to GetObject)
  - Output is JSONL-only (`gonimbus.content.head.v1`) with base64-encoded content
  - No mixed framing - suitable for simple inspection pipelines
  - Includes full metadata (etag, size, last_modified, content_type)

- **Content Package** (`pkg/content/`)
  - `HeadBytes(ctx, provider, key, n)` - read first N bytes with metadata
  - `HeadBytesMulti` - parallel multi-key content head operations
  - Automatic fallback: GetRange → GetObject (provider capability detection)

#### Provider Range Requests

- **ObjectRanger Interface** (`pkg/provider/capabilities.go`)
  - `GetRange(ctx, key, start, endInclusive)` for byte-range reads
  - HTTP Range semantics (inclusive start/end offsets)
  - S3 provider implementation with range header support

- **S3 Range Support** (`pkg/provider/s3/provider.go`)
  - Implements `ObjectRanger` interface for S3 and S3-compatible stores
  - Cloud integration tests for range request behavior

#### Documentation

- User guide: streaming vs content command mental model
- Content inspection examples

### Changed

- Provider capability detection uses interface type assertions for optional features
- Content commands emit errors to stdout as `gonimbus.error.v1` (consistent with stream commands)

## [0.1.5] - 2026-01-23

### Added

#### Content Streaming Commands (`stream get`, `stream head`)

- **Stream Get Command** (`internal/cmd/stream_get.go`)
  - `gonimbus stream get <uri>` streams object content with JSONL framing
  - Mixed-framing output: JSONL headers + raw bytes for efficient large payload handling
  - `gonimbus.stream.open.v1` with uri, size, etag, last_modified, content_type
  - `gonimbus.stream.chunk.v1` with seq, nbytes followed by raw bytes
  - `gonimbus.stream.close.v1` with status, chunks, bytes
  - Size validation: HEAD size vs GetObject size mismatch detection (stale key semantics)
  - Errors emitted to stdout as `gonimbus.error.v1` (streaming mode contract)

- **Stream Head Command** (`internal/cmd/stream_head.go`)
  - `gonimbus stream head <uri>` retrieves object metadata without content
  - Returns `gonimbus.object.v1` with full metadata including custom S3 user metadata
  - Errors emitted to stdout as `gonimbus.error.v1` (consistent with streaming mode)

- **Stream Package** (`pkg/stream/`)
  - `Writer` for producing mixed-framing streams (JSONL + raw bytes)
  - `Decoder` for consuming streams with truncation detection (`io.ErrUnexpectedEOF`)
  - Exact byte reconstruction verified via SHA256/MD5 round-trip testing

#### Transfer Size Validation

- **validate=size** (`pkg/transfer/`)
  - Compares enumerated size (from list/index) vs GetObject content-length
  - Catches stale index/list metadata before deep pipeline processing
  - Size mismatch mapped to `NOT_FOUND` error code (stale key semantics)
  - `SizeMismatchError` type with key, expected, and got fields

#### Documentation

- ADR-0004: Language-neutral content stream contract (`docs/architecture/adr/`)
- Streaming contract specification (`docs/development/streaming/`)
- QA checklist and helper replication guidance

### Fixed

- Cloud integration test credentials for stream writer tests (`pkg/stream/writer_cloudintegration_test.go`)

## [0.1.4] - 2026-01-19

### Added

#### Path-Scoped Index Builds (`build.scope`)

- **Scope Types** (`pkg/manifest/`, `internal/assets/schemas/`)
  - `prefix_list`: Explicit prefixes for deterministic crawl scope
  - `date_partitions`: Dynamic prefix generation from date ranges with segment discovery
  - `union`: Combine multiple scope definitions

- **Scope Compiler** (`pkg/scope/`)
  - Compiles `build.scope` configuration into explicit prefix plans
  - Delimiter listing for segment discovery (e.g., device IDs under store prefixes)
  - Date range expansion to concrete `YYYY-MM-DD/` prefixes
  - `--dry-run` flag previews scope plan before execution

- **Scope Guardrails** (`pkg/scope/`)
  - Warning threshold for large prefix expansions
  - Soft-delete skipped by default for scoped builds (partial coverage)
  - Scope config included in IndexSet identity hash

- **Provider Capability Contract** (`docs/architecture/adr/ADR-0003-*.md`)
  - ADR-0003: Defines prefix listing and delimiter listing requirements
  - Error classification for partial run handling
  - Provider-agnostic scope compilation contract

#### Index Job Management

- **Job Registry** (`pkg/jobregistry/`)
  - Durable on-disk job records under the app data dir (`jobs/index-build/<job_id>/job.json`)
  - Captures identity/run metadata, PID, heartbeat timestamps, and log file paths

- **Managed Background Builds** (`internal/cmd/index_build.go`, `pkg/jobregistry/executor.go`)
  - `gonimbus index build --background` spawns a managed child process and returns a job id
  - Captures stdout/stderr to per-job log files
  - Safe cancellation via SIGTERM -> context cancellation; SIGKILL fallback

- **Job CLI** (`internal/cmd/index_jobs*.go`)
  - `gonimbus index jobs list/status` with JSON output support
  - `jobs status` supports short id prefix resolution when unambiguous
  - `gonimbus index jobs stop/logs/gc` for operational control
  - `--dedupe` prevents starting duplicate running jobs for the same manifest

#### Documentation

- Enterprise indexing workflow guide with three-tier model (`docs/user-guide/index.md`)
- Indexing architecture with scope concepts (`docs/architecture/indexing.md`)
- ADR-0003: Index build provider capabilities (`docs/architecture/adr/`)

### Changed

- `--after` filter is now inclusive (was exclusive) for consistency with date range semantics
- Soft-delete skipped by default for scoped builds (partial coverage assumption)
- Index identity now includes scope configuration hash for isolation

### Fixed

- Tree traversal callback is now safe under parallel execution (`internal/cmd/tree.go`)

### Performance

- **99.5% reduction** in objects listed with `build.scope.date_partitions` on date-partitioned data
- **~10x faster** build times (3 min → 30 sec for 15-store scoped builds)
- Zero wasted enumeration: `objects_found ≈ objects_matched` with scope

## [0.1.3] - 2026-01-15

### Added

#### Index Workflow

- **Local Index Store** (`pkg/indexstore/`)
  - SQLite-based local index for offline bucket inventory
  - Per-index database isolation (hash-based identity)
  - Streaming batch ingestion from crawl results
  - Soft-delete handling for removed objects
  - Schema version tracking for upgrades

- **Index CLI Commands** (`internal/cmd/index*.go`)
  - `gonimbus index init` - Initialize local index database
  - `gonimbus index build --job <manifest>` - Build index from crawl
  - `gonimbus index list` - List local indexes with stats
  - `gonimbus index query <uri>` - Query indexed objects by pattern
  - `gonimbus index stats <uri>` - Detailed index statistics
  - `gonimbus index gc` - Garbage collect old indexes
  - `gonimbus index doctor` - Validate index integrity and identity
  - `gonimbus index show` - Display manifest provenance

- **Index Build Features**
  - Build-time include patterns for scope control
  - Derived prefix display during builds
  - Explicit identity validation (provider, region, endpoint)
  - Tolerates provider outages via SDK retry

- **Index Query Features**
  - Pattern matching with doublestar globs
  - Metadata filters: `--min-size`, `--max-size`, `--after`, `--before`
  - Count mode: `--count` for quick totals
  - JSONL output for integration with other tools

- **Index Manifest Schema** (`internal/assets/schemas/index-manifest.schema.json`)
  - Connection, identity, build, and output configuration
  - Build-time scope with include patterns
  - Provider identity for multi-cloud support

### Changed

- Index set identity now includes provider identity hash for isolation
- Index runs track partial/failed status for operational visibility

### Performance

- **Query Speedup**: 100-1000x faster than live crawl for repeated queries
- **Build Throughput**: ~3,000 objects/sec ingestion rate
- **Tested Scale**: 16M objects enumerated, 150K indexed (with filters)

## [0.1.2] - 2026-01-11

### Added

#### Transfer Workflow

- **Transfer Engine** (`pkg/transfer/`, `internal/cmd/transfer.go`)
  - Manifest-driven copy/move operations between S3 buckets
  - `gonimbus transfer --job manifest.yaml` CLI command
  - Support for same-bucket, cross-account, and cross-provider transfers
  - Configurable concurrency and `on_exists` behavior (skip, overwrite, fail)
  - Path templates for destination key transformation (`{filename}`, `{dir[n]}`, `{key}`)
  - Deduplication strategies: `etag` (default), `key`, or `none`

- **Prefix Sharding for Parallel Enumeration** (`pkg/shard/`)
  - `sharding.enabled`, `sharding.depth`, `sharding.list_concurrency` manifest options
  - Parallel prefix discovery using delimiter listing
  - Bounded concurrency with configurable worker pools
  - Up to 14x speedup for multi-level prefix trees (tested with 4K prefixes, scales to millions)
  - Live benchmark test: `pkg/shard/discovery_benchmark_test.go`

- **Preflight Permission Probing** (`pkg/preflight/`, `internal/cmd/preflight.go`)
  - Pre-transfer capability verification (read, write, delete permissions)
  - `gonimbus preflight --job manifest.yaml` standalone command
  - Three modes: `plan-only` (no calls), `read-safe` (List/Head/Get), `write-probe` (with probes)
  - Zero-side-effect probes: `multipart-abort` (preferred) and `put-delete` strategies
  - Detailed JSONL preflight records with per-capability results
  - Documentation: `docs/appnotes/preflight.md`

#### Tree Workflow

- **Tree Command for Prefix Summaries** (`internal/cmd/tree.go`)
  - `gonimbus tree <uri>` CLI command for directory-like summaries
  - Direct-only (non-recursive) operation by default
  - Depth-limited traversal with `--depth N` flag
  - Safety limits: `--timeout`, `--max-prefixes`, `--max-objects`, `--max-pages`
  - Include/exclude patterns for traversal scope (pathfinder-style)
  - Table output with formatted sizes and counts
  - JSONL output for streaming and partial results

#### Inspect Workflow

- **Advanced Metadata Filtering** (`pkg/match/filter.go`)
  - Size filtering: `min_size`, `max_size` with KB/KiB/MB/MiB/GB/GiB units
  - Date filtering: `after`, `before` with ISO 8601 dates/datetimes
  - Key regex filtering: `key_regex` with Go regexp syntax
  - CLI flags: `--min-size`, `--max-size`, `--after`, `--before`, `--key-regex`
  - Manifest configuration: `match.filters.size`, `match.filters.modified`, `match.filters.key_regex`

#### General & Safety

- **Global Readonly Safety Latch** (`internal/cmd/root.go`)
  - `--readonly` flag and `GONIMBUS_READONLY=1` environment variable
  - Blocks provider-side mutations (transfers, write-probe preflight)
  - Intended for dogfooding and lower-trust automation
  - Readonly tests: `internal/cmd/readonly_test.go`

#### Documentation

- Transfer operations user guide: `docs/user-guide/transfer.md`
- Preflight permission probe app note: `docs/appnotes/preflight.md`
- Examples cookbook: `docs/user-guide/examples/README.md`
- Tree command examples: `docs/user-guide/examples/tree.md`
- Advanced filtering examples: `docs/user-guide/examples/advanced-filtering.md`

### Changed

- Preflight probe ordering: write probes now run before read probes for faster fail-fast
- Transfer manifest schema extended with `sharding`, `path_template`, `dedup` fields
- Job manifest schema extended with `preflight` and `filters` fields

### Fixed

- **Retryable PUT Bodies** (`pkg/transfer/`)
  - Fixed "failed to rewind transport stream for retry" errors on transient failures
  - Small objects now buffered with seekable wrapper for SDK retry support

- **Tree Command** (`internal/cmd/tree.go`)
  - Fixed missing duration field in summary records
  - Fixed table output serialization for timeout/partial results
  - Ensure summary is emitted even when timeout occurs
  - Fixed timeout producing FATAL instead of clean partial output with `error.v1` + `summary.v1`

### Performance

- **Parallel Prefix Discovery**: 14x speedup at 32 concurrency for multi-level prefix trees
  - Sequential: 21.2s → Parallel: 1.5s (tested with 4K prefixes, designed for millions)
  - Recommended: `list_concurrency: 16` default, 32 for very large workloads

## [0.1.1] - 2026-01-05

### Added

- **AWS Profile Authentication** (`internal/cmd/doctor.go`)
  - `--profile` flag on `doctor` command for enterprise SSO diagnostics
  - Credential expiry check with warning when < 1 hour remaining
  - IMDS timeout optimization when profile/env credentials available
  - SSO-aware help text (`aws sso login` guidance)
  - Documentation: `docs/auth/aws-profiles.md`

- **Cloud Integration Tests** (`test/cloudtest/`, `pkg/provider/s3/`, `internal/cmd/`)
  - S3 provider integration tests using moto (AWS mock server)
  - CLI inspect command end-to-end tests
  - Test helpers for bucket creation, object upload, and isolation
  - Makefile targets: `test-cloud`, `moto-start`, `moto-stop`, `moto-status`
  - CI workflow with moto service container
  - Documentation: `docs/development/testing.md`

### Changed

- S3 provider test coverage increased from 49% to 97% with cloud integration tests
- `ec2/imds` promoted from indirect to direct dependency (IMDS timeout control)

### Fixed

- `make install` now correctly installs binary to `~/.local/bin`

## [0.1.0] - 2026-01-03

Initial public release of Gonimbus - a Go-first library + CLI + server for large-scale inspection and crawl of cloud object storage.

### Added

- **Provider Interface & S3 Implementation** (`pkg/provider/`)
  - Abstract provider interface with `List`, `Head`, and `Close` methods
  - S3 provider using AWS SDK v2 with default credential chain
  - Support for S3-compatible stores (Wasabi, Cloudflare R2, DigitalOcean Spaces)
  - Custom endpoint and explicit credential configuration

- **Pattern Matching Layer** (`pkg/match/`)
  - Doublestar glob pattern matching for cloud object keys
  - Prefix derivation algorithm for efficient listing at scale
  - Include/exclude pattern support
  - Hidden file detection and filtering

- **JSONL Output Layer** (`pkg/output/`)
  - Typed record envelopes: `gonimbus.object.v1`, `gonimbus.error.v1`, `gonimbus.progress.v1`
  - Stream-friendly JSONL writer with atomic line writes
  - Configurable progress emission

- **Crawl Engine** (`pkg/crawler/`)
  - Bounded streaming pipeline: lister → matcher → writer
  - Configurable concurrency and rate limiting
  - Backpressure via bounded channels
  - Context cancellation and graceful shutdown
  - Progress tracking and summary statistics

- **Job Manifest Schema** (`pkg/manifest/`)
  - JSON Schema validated job manifests (YAML/JSON)
  - Connection, match, crawl, and output configuration
  - Strict validation with clear error messages

- **CLI Commands** (`internal/cmd/`)
  - `gonimbus crawl` - Run crawl jobs from manifest files
  - `gonimbus inspect` - Quick inspection of objects or prefixes
  - `gonimbus doctor` - Environment and credential diagnostics
  - `gonimbus serve` - HTTP server with health endpoints
  - `gonimbus version` - Version and build information

- **Server Skeleton** (`internal/server/`)
  - Chi-based HTTP router with middleware stack
  - Health check endpoints (`/health`, `/health/live`, `/health/ready`, `/health/startup`)
  - Prometheus metrics endpoint (`/metrics`)
  - Version endpoint (`/version`)
  - Panic recovery and error handling middleware

- **Documentation**
  - Storage provider configuration guide (`docs/appnotes/storage-providers.md`)
  - Example manifests for common use cases (`examples/manifests/`)
  - CLI usage examples (`examples/cli/`)

### Infrastructure

- Makefile with quality gates (`make check-all`, `make prepush`)
- License-audit target with dependency cooling policy (`.goneat/dependencies.yaml`)
- golangci-lint integrated via goneat assess
- Release signing workflow (minisign + optional PGP)
- Embedded app identity via `.fulmen/app.yaml`
- gofulmen v0.2.1 / Crucible v0.3.0 integration
- ADR-0001: Embedded assets over directory walking
- ADR-0002: Pathfinder boundary constraints in tests

[Unreleased]: https://github.com/3leaps/gonimbus/compare/v0.4.0...HEAD
[0.4.0]: https://github.com/3leaps/gonimbus/compare/v0.3.7...v0.4.0
[0.3.7]: https://github.com/3leaps/gonimbus/compare/v0.3.6...v0.3.7
[0.3.6]: https://github.com/3leaps/gonimbus/compare/v0.3.5...v0.3.6
[0.3.5]: https://github.com/3leaps/gonimbus/compare/v0.3.4...v0.3.5
[0.3.4]: https://github.com/3leaps/gonimbus/compare/v0.3.3...v0.3.4
[0.3.3]: https://github.com/3leaps/gonimbus/compare/v0.3.2...v0.3.3
[0.3.2]: https://github.com/3leaps/gonimbus/compare/v0.3.1...v0.3.2
[0.3.1]: https://github.com/3leaps/gonimbus/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/3leaps/gonimbus/compare/v0.2.3...v0.3.0
[0.2.3]: https://github.com/3leaps/gonimbus/compare/v0.2.2...v0.2.3
[0.2.2]: https://github.com/3leaps/gonimbus/compare/v0.2.1...v0.2.2
[0.2.1]: https://github.com/3leaps/gonimbus/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/3leaps/gonimbus/compare/v0.1.8...v0.2.0
[0.1.8]: https://github.com/3leaps/gonimbus/compare/v0.1.7...v0.1.8
[0.1.7]: https://github.com/3leaps/gonimbus/compare/v0.1.6...v0.1.7
[0.1.6]: https://github.com/3leaps/gonimbus/compare/v0.1.5...v0.1.6
[0.1.5]: https://github.com/3leaps/gonimbus/compare/v0.1.4...v0.1.5
[0.1.4]: https://github.com/3leaps/gonimbus/compare/v0.1.3...v0.1.4
[0.1.3]: https://github.com/3leaps/gonimbus/compare/v0.1.2...v0.1.3
[0.1.2]: https://github.com/3leaps/gonimbus/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/3leaps/gonimbus/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/3leaps/gonimbus/releases/tag/v0.1.0
