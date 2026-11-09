# Release Notes

This file contains release notes for up to the three most recent releases in reverse chronological order. For the complete release history, see the [CHANGELOG](CHANGELOG.md) or the [docs/releases/](docs/releases/) directory.

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

---

## [0.4.6] - 2026-11-08

**Advisory control-record marker, faster durable prefix queries, look-alike flag hints, and release provenance hardening**

v0.4.6 adds a forward-compatible `"advisory": true` envelope marker for future
control records in `transfer reflow` pipelines, prunes trailing segments during
durable `index query --pattern` prefix scans, appends actionable hints for
look-alike unknown CLI flags, and tightens release provenance and dependency
hygiene.

- **Advisory control-record envelope marker.** `transfer reflow` skips
  unrecognized future control records marked with top-level `"advisory": true`
  (outside recognized work, integrity, and error record families) without
  incrementing `invalid_inputs`, emits at most one `gonimbus.warning.v1`
  (`REFLOW_ADVISORY_RECORD_SKIPPED`) per distinct type (up to 16 types, with a
  single `REFLOW_ADVISORY_WARNING_CAP_REACHED` warning record if exceeded), and
  continues without aborting the batch. `content probe` stamps `"advisory":
true` on `gonimbus.content.probe.summary.v1`.
- **Durable prefix segment pruning.** `index query --pattern <glob>` stops
  opening and hashing segments once a segment's `MinRelKey` sorts strictly
  after the pattern's literal prefix (while still reading segments with
  inverted bounds).
- **Look-alike CLI flag hints.** Unknown `--profile`/`-p`, `--json` and
  `--count` flags on subcommands with different flag names or invocation shapes
  append a targeted hint before the usage block, and `inspect` documents its
  default 100-object `--limit` cap.
- **Release provenance and immutable publication.** Release publication stages
  tagged public verification keys as release assets, verifies Decernor's
  SHA-256 before execution, uses an isolated `GNUPGHOME` for tag and anchor
  verification, and verifies GitHub release immutability on publish.
- **Security and dependency updates.** Updates `github.com/andybalholm/brotli`
  to v1.2.7 (`GO-2026-6888`), `github.com/klauspost/compress` to v1.20.2
  (`GO-2026-6865`), `github.com/go-chi/chi/v5` to v5.4.0,
  `github.com/go-jose/go-jose/v4` to v4.1.6, `golang.org/x/net` to v0.62.0,
  `golang.org/x/crypto` to `v0.58.1-0.20261009181718-2f1f834330b2` (cooled
  2026-10-09 commit), `golang.org/x/sys` to v0.49.0, `golang.org/x/sync` to
  v0.24.0, and `golang.org/x/text` to v0.43.0, and canonicalizes top-level
  `LICENSE` and `NOTICE`.

### Upgrade

```bash
go install github.com/3leaps/gonimbus/cmd/gonimbus@v0.4.6
```

See [docs/releases/v0.4.6.md](docs/releases/v0.4.6.md) for the complete
release notes.

---

## [0.4.5] - 2026-10-21

**Go 1.26.10 minimum, quieter stderr, single-source release tag**

v0.4.5 raises the minimum Go toolchain, keeps stderr for real problems, and
tightens how the release tooling selects a tag.

- **Go 1.26.10 minimum.** gonimbus builds with Go toolchain `go1.26.10`, to
  ensure secure operation.
- **Quieter stderr.** Commands against AWS S3 no longer print an SDK checksum
  debug line, and runtime failures no longer append the usage block. Usage
  still prints for invocation errors.
- **Release tag from one variable.** Release tooling reads the tag only from
  `GONIMBUS_RELEASE_TAG` and checks it against `VERSION` before any
  tag-consuming step.
- **Docs.** What `dest_etag` holds per destination provider, and the shape of
  the `index build --json` receipt.

No commands, flags, record schemas or exit codes change.

### Upgrade

```bash
go install github.com/3leaps/gonimbus/cmd/gonimbus@v0.4.5
```

See [docs/releases/v0.4.5.md](docs/releases/v0.4.5.md) for the complete
release notes.
