# Release Notes

This file contains release notes for up to the three most recent releases in reverse chronological order. For the complete release history, see the [CHANGELOG](CHANGELOG.md) or the [docs/releases/](docs/releases/) directory.

---

## [0.5.1] - 2026-11-20

**Opt-in `--require-source-match` source-read binding and input `source_version_id` native version pinning for `transfer reflow`, additive `provider.RevisionHeader` (`HeadRevision`), Go `1.26.11` security and dependency updates, and retirement of `-tags gonimbus_libsql`**

v0.5.1 adds opt-in strict source-read binding (`--require-source-match`) and input-side native version pinning (`source_version_id`) for `transfer reflow` across both `pkg/reflow` and the CLI pool so copied bytes match the admitted source revision before destination writes, adds `provider.RevisionHeader` (`HeadRevision`) on S3 and GCS providers, updates the Go toolchain pin to `go1.26.11` (`GO-2026-6859`) with cooled Go module refreshes, and retires the opt-in `-tags gonimbus_libsql` build tag.

- **Strict source-read binding (`--require-source-match`).** Binds `transfer reflow` source reads to the admitted ETag or native object version via `provider.RevisionGetter` across copy, conditional copy, quarantine copy, and duplicate body comparisons. Refuses non-`RevisionGetter` sources at preflight (exit code `40`, `reason: "replay_unverifiable"`), fails closed on missing expected tokens (`reason: "source_token_missing"`), separates `source_changed` from `not_found` without misclassifying source `412` responses as destination collisions, and records read-bound `source_etag`, optional `source_version_id`, and `source_match` on pinned terminal records while preserving byte-identical unpinned output.
- **Native input version pinning (`source_version_id`) and `provider.RevisionHeader`.** Honors optional `source_version_id` on `gonimbus.reflow.input.v1` via S3 `VersionId` and GCS generation-pinned reads (including historical versions after overwrite or behind S3 delete markers), performs dual-token ETag verification before writing destination bytes, routes mandatory metadata and `overwrite-if-source-newer` timestamp lookups through `HeadRevision` rather than unversioned `Head`, and fails closed (`replay_unverifiable`) on blank/null tokens, S3 `"null"`, non-positive GCS generations, and non-versioned (`file://`) providers.
  - **Compatibility guard:** If you rely on source binding, including `source_version_id`, pass `--require-source-match`. gonimbus 0.5.0 and earlier reject the flag, so the run fails closed. Without the flag, 0.5.0 ignores `source_version_id` and copies the current object.
- **Go `1.26.11` toolchain (`GO-2026-6859`) and cooled dependency updates.** Updates the Go toolchain pin to `go1.26.11` (keeping the `go 1.26.0` directive), refreshes cooled Go module dependencies (`aws-sdk-go-v2`, `cloud.google.com/go/storage`, `google.golang.org/api`, `modernc.org/sqlite`, `doublestar`, `chi`, `pflag`, `golang.org/x/*`, `grpc`), and bumps CI `golangci-lint` to `v2.15.0`.
- **Retirement of opt-in `-tags gonimbus_libsql`.** Removes the opt-in `-tags gonimbus_libsql` build tag and `github.com/tursodatabase/go-libsql` dependency because Go `1.26.11` rejects precompiled LTO bitcode sections in `#cgo LDFLAGS` static archives. Local SQLite indexes continue using pure-Go `modernc.org/sqlite`; remote `libsql://` and `https://` index URLs fail closed with `"remote libsql is not supported in this release"`.

### Upgrade

```bash
brew install 3leaps/tap/gonimbus
go install github.com/3leaps/gonimbus/cmd/gonimbus@v0.5.1
```

See [docs/releases/v0.5.1.md](docs/releases/v0.5.1.md) for the complete release notes.

---

## [0.5.0] - 2026-11-11

**`durable-index/v0` contract adoption and plan safeguards, `parquet-inventory/v0` ZSTD segments with legacy dual-read and portable relative markers, truthful dot-segment policy and per-run engine provenance, discovered `segments` build scopes and offline `migrate-match-scope`, and Gen-2.1 release ceremony refinements**

v0.5.0 adopts the `durable-index/v0` contract (`ADR-0017`, `DDR-0004`) across
durable index planning, segment emission, and verification; enforces fail-closed
`scope_loss` admission on successor durable builds unless an explicit pinned
acknowledgment is supplied; switches durable segment writes to
`parquet-inventory/v0` with `ZSTD` compression (~3.2× smaller segments) while
preserving full dual-read compatibility with existing `0.4.x` (`durable-v2`)
indexes and adding portable relative marker paths; records truthful
`include_hidden: true` durable set identity, `dot_segment_policy`,
`dot_segment_rows`, and per-run `gonimbus_version` / `gonimbus_commit` /
`gonimbus_vcs_modified` provenance; introduces `build.scope.type: segments`
(`ADR-0009`) and offline discovered-scope proposals in
`index migrate-match-scope`; and refines the release ceremony to Gen-2.1.

- **`durable-index/v0` plan canonicalization, `plan_digest`, and `scope_loss`
  safeguards.** Vendors and pins the upstream `durable-index/v0` contract
  closure (`3leaps/crucible` `v0.2.9`) under
  `internal/contracts/durableindex/v0/upstream/` (`make
contract-fixture-integrity`). Compiles canonical `unicode-prefix/v0` prefix
  plans, computes the length-framed `durable-index/v0/plan\0` SHA-256
  `plan_digest`, compares proposed plans against the verified parent publication
  by raw UTF-8 byte-prefix containment, and refuses unexplained prefix coverage
  reductions (`scope_loss`) on `--format durable` and `--format both` (warning
  on standalone `--format sqlite`) unless all four
  `--acknowledge-scope-loss-{parent,plan,by,at}` flags binding
  `(parent_manifest_sha256, proposed_plan_digest, actor, at)` are supplied and
  persisted with `counts.retained_unobserved_rows`.
- **`parquet-inventory/v0` ZSTD segment emission, legacy `0.4.x` dual-read, and
  portable relative markers.** Emits `DDR-0004` `parquet-inventory/v0` ZSTD
  segments (nested `revision`, `first_seen`/`last_seen`/`last_changed`, optional
  `deleted`, 3-level revision-bound `enrichments` LIST, and 9-digit
  `.000000000Z` UTC timestamps) while retaining same-FD dual-read compatibility
  with `0.4.x` (`durable-v2`) flat Snappy segments (`128 MiB` whole-member
  uncompressed byte ceiling for `parquet-inventory/v0` vs `1 GiB` whole-segment
  ceiling for legacy segments). Adds `manifest_rel_path`, `segment_rel_dir`
  (`"."` in the CLI run layout or a layout-dependent relative directory), and
  `complete_rel_path` alongside primary marker paths so `v0.5.0+` readers
  resolve copied or relocated index directories locally while `<= v0.4.7`
  readers continue reading via the primary paths.
- **Truthful dot-segment policy (`include_hidden: true`), `dot_segment_rows`,
  and per-run engine provenance.** Confirmed-complete durable index builds
  (`--format durable` and `--format both`) ingest all enumerated keys in scope,
  including keys with `.`-prefixed path segments (`dot_segment_policy:
"include"`), and record `"include_hidden": true` in `identity.json` (deriving
  a distinct `v0.5.0` `index_set_id` without modifying existing `0.4.x` sets).
  Reports `dot_segment_policy` and `dot_segment_rows` in
  `gonimbus.index.manifest.v1` (`counts.dot_segment_rows`) and
  `gonimbus.index.build_result.v1`, and records `gonimbus_version`,
  `gonimbus_commit`, and `gonimbus_vcs_modified` across `manifest.json`,
  `complete.json`, and `gonimbus.index.build_result.v1`.
- **Discovered `segments` build scope (`ADR-0009`) and offline
  `index migrate-match-scope`.** Ratifies `ADR-0008`, `ADR-0009`, and `DDR-0001`
  (`Accepted`). Adds `build.scope.type: segments` (`pkg/scope`) for partitioned
  layouts with literal or discovered middle segments and a terminal `**` subtree
  without changing existing `date_partitions` identity hashes, and extends
  `gonimbus index migrate-match-scope` (`convertible`, `already_compatible`,
  `already_migrated`, `refused`) with an optional `discovered_scope_proposal`
  audit field and `--propose-discovered-scope`.
- **Gen-2.1 release ceremony refinements and pinned `0.4` minor-line packages.**
  Updates `scripts/release-publish.sh` to the verified 1+1 download pattern,
  adds staged-tag verification, `noclobber` + expected tag-object/commit anchor
  binding, `<tag>.txt` + 3-line attribution footer validation, worktree-aware
  `GONIMBUS_GPG_HOMEDIR` containment, and offline portability/write-guard/recovery
  test suites (`make release-tooling-test`), pins `goneat v0.6.8` and
  `goneat-tools-runner-glibc:v0.5.15`, and preserves `v0.4.7` as a pinned
  minor-series package (`3leaps/tap/gonimbus@0.4` and `3leaps/gonimbus-0.4`).

### Upgrade

```bash
brew install 3leaps/tap/gonimbus
go install github.com/3leaps/gonimbus/cmd/gonimbus@v0.5.0
```

See [docs/releases/v0.5.0.md](docs/releases/v0.5.0.md) for the complete
release notes.

---

## [0.4.7] - 2026-11-09

**Machine-readable `version --json`, throttled probe retries, Gen-2 release provenance and registry verification, durable storage architecture ratification, and digest-pinned runner contracts**

v0.4.7 adds `gonimbus version --json [--extended]` (and allows CLI startup in
scrubbed environments without `HOME`), retries throttled standalone provider
probe operations (`HEAD` and body-compare `GET`) during `transfer reflow` with
AIMD-aware concurrency slot release, upgrades release provenance to Gen-2
(exact signing-subkey enforcement, workflow SHA-256 pinning, clean-room
published-release staging, post-publication Go proxy / Homebrew / Scoop
registry verification, and public-surface hygiene scanning), ratifies the
durable storage architecture (`ADR-0007`, `ADR-0008`, `DDR-0001`), and codifies
the raw-asset byte-for-byte packaging and `receipt-jsonl-v1` segment-pruning
contracts for digest-pinned automation runners.

- **`gonimbus version --json [--extended]` and scrubbed-environment startup.**
  `gonimbus version --json` emits a single-line JSON object (`name`, `version`,
  `commit`, `built_at`, `go`, `platform`), and `--json --extended` (`-e`) adds
  `os`, `arch`, `gofulmen`, `crucible`, and Go module build info (`module_path`,
  `vcs_revision`, `vcs_time`, `vcs_modified`). Human `gonimbus version
[--extended]` output is unchanged, extra positional arguments are rejected,
  and CLI startup no longer exits when `HOME` and `XDG_CONFIG_HOME` are unset
  (`env -i`) and no explicit `--config` path is given.
- **AIMD-aware retry for throttled reflow probes.** Standalone provider `HEAD`
  and body-compare `GET` probe calls in `pkg/reflow` and `transfer reflow`
  retry `provider.ErrThrottled` up to `RetryMax` (default 3 retries / 4 total
  attempts), releasing the adaptive concurrency limiter slot before sleeping
  between attempts and feeding each attempt outcome through
  `ObserveProviderResult`. Optional pre-transfer source `HEAD` on CLI reflow
  runs also routes fatal non-404 errors through `RecordFatalError`.
- **Gen-2 release provenance, registry verification, and surface hygiene.**
  Enforces the exact OpenPGP signing subkey (`config/release/signing-subkey.txt`)
  across tag, manifest signature, and post-tag selector checks; pins
  `.github/workflows/*.yml` digests in `config/release/release-workflow.sha256`;
  attaches `expected-fingerprints.txt` and `expected-fingerprints.ndjson`
  alongside the public keys on each immutable GitHub Release; adds `make
release-stage-published`, `make release-verify-registries`, and `make
surface-scan`.
- **Durable storage architecture ratification.** Ratifies `ADR-0007` (Accepted,
  `2026-09-30`), adds `ADR-0008` and `DDR-0001`, and publishes
  `docs/architecture/durable-storage.md` and
  `docs/architecture/canonical-state-conformance.md`.
- **Raw-asset packaging invariant and `receipt-jsonl-v1` segment-pruning
  contract.** Codifies the byte-for-byte raw-executable contract across GitHub
  Releases, `3leaps/homebrew-tap`, and `3leaps/scoop-bucket` (including the
  Windows Scoop `shims\gonimbus.exe` vs `scoop which gonimbus` distinction for
  SHA-256 verifiers and the minor-line `@0.4` retention rule for `v0.5.0`), and
  documents and contract-tests the four `receipt-jsonl-v1` segment-pruning
  accounting invariants.

### Upgrade

```bash
brew install 3leaps/tap/gonimbus
go install github.com/3leaps/gonimbus/cmd/gonimbus@v0.4.7
```

See [docs/releases/v0.4.7.md](docs/releases/v0.4.7.md) for the complete
release notes.
