# Release Notes

This file contains release notes for up to the three most recent releases in reverse chronological order. For the complete release history, see the [CHANGELOG](CHANGELOG.md) or the [docs/releases/](docs/releases/) directory.

---

## v0.3.4 (2026-06-27)

**Google Cloud Storage Provider and Library-Exposure Foundation**

v0.3.4 widens the provider matrix beyond S3 and `file://` for the first time:
Google Cloud Storage (`gs://`) becomes a first-class crawl/inspect source and
`transfer reflow` destination, reusing the same adaptive-concurrency and
IfAbsent-capability model already documented for S3. The release also lands the
Experimental `pkg/reflow` library-exposure foundation, with CLI behavior
unchanged.

### Google Cloud Storage provider

A bucket living in GCS no longer needs separate tooling to inspect, index, or
reflow: the same records, index hub, and content-aware reflow semantics now apply
to `gs://`, and cross-provider reflow (S3 → GCS, `file://` → GCS, GCS → S3) works
through the one dispatch seam. GCS extends the provider matrix without forking the
operating model.

`gs://` works as a source and reflow destination across
inspect/index/tree/stream/content/doctor and `transfer reflow`. GCS reports the
same IfAbsent honored/probe-status summary fields as S3, maps throttling
(`RESOURCE_EXHAUSTED`/429/503) to `provider.ErrThrottled`, and plugs into the
adaptive `--parallel` model. Authentication uses Application Default Credentials
or service-account keys under the credential-source discipline (no URI- or
manifest-sourced credential filepaths); `STORAGE_EMULATOR_HOST` is test-only.

ETag-based `If-Match` conditional writes are unsupported (GCS uses generation
preconditions), so `--on-collision overwrite-if-source-newer` is unavailable on
GCS destinations and fails closed; IfAbsent `skip-if-duplicate` is supported.

### Library-exposure foundation (Experimental)

`pkg/reflow` exposes typed records, the adaptive-concurrency substrate, and
provider-error redaction helpers as an Experimental surface; the CLI still drives
the internal path. The migration completes in a later release.

### Fixes and housekeeping

- `--resume-run` restored across the v0.3.2 → v0.3.3 upgrade boundary
  (op-checkpoint `no_adaptive` now omitted when unset; cross-version compat test).
- Reflow probe operations now bounded by adaptive concurrency (probes back off
  with copies under throttling; deadlock-free at the floor).
- `golang.org/x/net` upgraded to v0.56.0.
- Corrected broken `transfer reflow` rewrite examples in the shipped v0.2.1 /
  v0.2.3 release notes.

### Upgrade

```bash
go install github.com/3leaps/gonimbus/cmd/gonimbus@v0.3.4
```

CLI workflows remain compatible with v0.3.3. GCS is a first-class CLI and server
capability; the **Experimental** label applies only to the new Go package import
surfaces (`pkg/reflow` and direct `pkg/provider/gcs` imports).

See [docs/releases/v0.3.4.md](docs/releases/v0.3.4.md) for the complete release
notes.

---

## v0.3.3 (2026-06-17)

**Adaptive Reflow Concurrency and Reflow Surface Hardening**

v0.3.3 makes large `transfer reflow` runs faster to right-size and safer to
trust. `--parallel` becomes a requested ceiling that the engine adapts within —
discovering an endpoint's sustainable rate on its own — while a resource-derived
cap keeps even an extreme value from harming the host. The release also hardens
the reflow surface: capability-aware collision fallback, fail-closed priority
probe extraction, and classified, credential-redacted error output.

### Adaptive Reflow Concurrency

`transfer reflow --parallel` is now a **requested ceiling**, not a fixed worker
count. A run resolves an effective ceiling — `min(--parallel, resource cap)`,
where the cap is derived from available memory and the file-descriptor limit —
then, in adaptive mode (the default), varies active concurrency within it using
throttle-aware AIMD control. `--no-adaptive` runs fixed at the effective ceiling.

```bash
# Ask for up to 256 workers; the engine settles where the endpoint + host allow.
gonimbus transfer reflow --stdin \
  --dest 's3://dest-bucket/reflowed/' \
  --parallel 256 < reflow-input.jsonl
```

The clamp is never silent (`concurrency_ceiling_reason` plus a stderr notice),
connection-error bursts freeze the ramp rather than cutting it, and additive
`concurrency_*` run/summary fields record the rate a run converged to. On
S3-compatible destinations, reflow opts into HTTP transport sizing derived from
the effective ceiling so high concurrency reuses connections. See
[Concurrency and Throughput](docs/user-guide/concurrency-and-throughput.md) for
the provider-generalized model.

### Capability-Aware Collision Fallback

On S3-compatible stores that do not honor `If-None-Match: *`, no-overwrite
collision modes previously risked a silent degrade to overwrite. Reflow now runs
a semantic IfAbsent probe (`honored` / `not_honored` / `inconclusive`) and fails
closed to a HEAD/compare fallback, emitting a structured warning and
`gonimbus.reflow.summary.v1` audit fields for the probe status, fallback
activation, and degraded-path object count.

### Probe Priority Fallbacks

`content probe` `xml_xpath` extractors accept `xpath_priority` for ordered
fallback tags, with `resolved_priority`, `resolved_xpath`, `truncated_fallback`,
and `truncated_fallback_count` audit fields. Lower-priority values observed at a
truncated read boundary fail closed to quarantine rather than being accepted as
final matches.

### Reflow Error Classification and Redaction

Resumable reflow aborts emit a compact classified cause for automation, and
published reflow error/warning messages redact credential material from provider
URLs — every URL query value is redacted by default — so credential-bearing
provider errors do not leak into JSONL, logs, or CI artifacts.

### Library API

This release has additive Stable API changes and no Stable breaks:
`pkg/provider/s3.Config` adds optional `MaxIdleConnsPerHost` and
`MaxConnsPerHost` transport sizing fields. Zero values preserve the AWS SDK
defaults.

### Upgrade

```bash
go install github.com/3leaps/gonimbus/cmd/gonimbus@v0.3.3
```

Existing `transfer reflow --parallel` invocations keep working; the value is now
a requested ceiling, adaptive control is on by default, and `--no-adaptive`
restores fixed-concurrency behavior at the resource-capped effective ceiling.

See [docs/releases/v0.3.3.md](docs/releases/v0.3.3.md) for the complete release
notes.

---

## v0.3.2 (2026-06-15)

**Package-Manager Distribution Plumbing**

v0.3.2 is a small release focused on package-manager distribution. It prepares
the release pipeline for Homebrew and Scoop publishing and updates the release
build matrix to match those distribution targets.

### Package-Manager Distribution

The release upload ceremony now prints the GitHub download URL and SHA256 for
the assets consumed by downstream package managers:

- Homebrew: macOS ARM64, Linux AMD64, and Linux ARM64.
- Scoop: Windows AMD64 and Windows ARM64.

Those values come from the uploaded GitHub release assets and the signed
`SHA256SUMS` manifest, so the Homebrew tap and Scoop bucket can be updated from
the same release evidence used in the signing ceremony.

### Build Matrix

The release matrix remains five artifacts:

- `gonimbus-linux-amd64`
- `gonimbus-linux-arm64`
- `gonimbus-darwin-arm64`
- `gonimbus-windows-amd64.exe`
- `gonimbus-windows-arm64.exe`

The native Intel Mac artifact, `gonimbus-darwin-amd64`, is no longer published.
Apple-silicon macOS remains the Homebrew macOS baseline, and Linux AMD64 plus
Linux ARM64 remain available through Homebrew.

### Upgrade

```bash
go install github.com/3leaps/gonimbus/cmd/gonimbus@v0.3.2
```

After the package-manager manifests are published, supported users can also
install with Homebrew or Scoop:

```bash
brew install 3leaps/tap/gonimbus
scoop install gonimbus
```

See [docs/releases/v0.3.2.md](docs/releases/v0.3.2.md) for the complete
release notes.

For v0.3.0 and earlier release notes, see [docs/releases/](docs/releases/) or
the [CHANGELOG](CHANGELOG.md).

<!-- v0.3.0 entry removed when v0.3.3 rolled into the 3-most-recent window; full notes preserved at docs/releases/v0.3.0.md -->
<!-- v0.2.3 entry removed when v0.3.2 rolled into the 3-most-recent window; full notes preserved at docs/releases/v0.2.3.md -->
<!-- v0.2.2 entry removed when v0.3.1 rolled into the 3-most-recent window; full notes preserved at docs/releases/v0.2.2.md -->
<!-- v0.2.1 entry removed when v0.3.0 rolled into the 3-most-recent window; full notes preserved at docs/releases/v0.2.1.md -->
<!-- v0.3.1 entry removed when v0.3.4 rolled into the 3-most-recent window; full notes preserved at docs/releases/v0.3.1.md -->
