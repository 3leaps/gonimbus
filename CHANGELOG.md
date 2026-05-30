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
- **AGENTS.md** — local planning-directory references retired; planning artifacts now live outside this public repository
- **AGENTS.md DO NOT list** — replaced narrow planning-directory guidance with broader prohibition on referencing client data, paths, or identifiers in repo content

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
  - Dry-run mode for planning (`--dry-run`)
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

[Unreleased]: https://github.com/3leaps/gonimbus/compare/v0.2.2...HEAD
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
