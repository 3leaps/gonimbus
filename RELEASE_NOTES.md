# Release Notes

This file contains release notes for up to the three most recent releases in reverse chronological order. For the complete release history, see the [CHANGELOG](CHANGELOG.md) or the [docs/releases/](docs/releases/) directory.

---

## v0.3.5 (2026-07-01)

**Multipart Export/Reflow and the Migrated Reflow Engine**

v0.3.5 removes the practical single-PUT ceiling from the release's largest
operator workflows. `index export` can publish a large `index.db` to an
S3-compatible hub through multipart upload, and `transfer reflow` can copy large
objects through the same shared upload primitive. The release also moves the
migrated stdin reflow subset onto the Experimental `pkg/reflow` engine and
tightens two error paths that matter during long runs: content-probe terminal
errors keep their original code, and throttled reflow probes retry before falling
back to per-object failure handling.

### Multipart for large exports and reflow writes

Large writes now use a shared multipart primitive in `pkg/transfer`. Once an
upload crosses the multipart threshold, Gonimbus spools parts to disk, uploads
bounded parts, completes conditionally when the destination supports IfAbsent,
and aborts the multipart upload on every failure path it controls.

The immediate operator effect is simple: a large hub export or reflow write no
longer fails only because the destination enforces a >5 GiB single-PUT limit.
Multipart-form ETags are still treated carefully; Gonimbus does not use them as
blind byte-equality proof for collision decisions.

### Reflow engine migration (Experimental)

The stdin `transfer reflow` subset now routes through the Experimental
`pkg/reflow` engine for metadata planning, dry-run planning, record-stream copy
execution, collision decisions, adaptive concurrency, and typed run / summary
records. CLI behavior is intended to remain compatible; unsupported forms keep
using the legacy path until later migration work.

`pkg/reflow` remains **Experimental**. There are no Stable library API breaks in
this release.

### Error-path hardening

- `content probe` now recognizes `gonimbus.error.v1` input records as terminal
  records, preserves their original error code, and avoids wrapping retryable
  records as `INTERNAL`.
- Content-probe error output now uses the same provider-error sanitizer as
  reflow and no longer carries raw input JSONL lines forward in parser
  diagnostics.
- Reflow HEAD/body-compare probe operations now retry throttled provider
  attempts through the adaptive limiter. If throttling never clears, the
  existing per-object failure behavior remains in place; the checkpoint/resume
  classifier boundary is unchanged.

### Upgrade

```bash
go install github.com/3leaps/gonimbus/cmd/gonimbus@v0.3.5
```

Existing CLI workflows remain compatible with v0.3.4. Operators planning large
multipart writes should size local scratch space for part spooling and configure
provider lifecycle cleanup for incomplete multipart uploads, such as S3
`AbortIncompleteMultipartUpload` lifecycle rules.

See [docs/releases/v0.3.5.md](docs/releases/v0.3.5.md) for the complete release
notes.

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
same IfAbsent honored/probe-status summary fields as S3, maps `429` and
`403`+`RESOURCE_EXHAUSTED` to `provider.ErrThrottled` (and `5xx`, including
`503`, to `provider.ErrProviderUnavailable`), and plugs into the
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

For v0.3.0 and earlier release notes, see [docs/releases/](docs/releases/) or
the [CHANGELOG](CHANGELOG.md).

<!-- v0.3.0 entry removed when v0.3.3 rolled into the 3-most-recent window; full notes preserved at docs/releases/v0.3.0.md -->
<!-- v0.2.3 entry removed when v0.3.2 rolled into the 3-most-recent window; full notes preserved at docs/releases/v0.2.3.md -->
<!-- v0.2.2 entry removed when v0.3.1 rolled into the 3-most-recent window; full notes preserved at docs/releases/v0.2.2.md -->
<!-- v0.2.1 entry removed when v0.3.0 rolled into the 3-most-recent window; full notes preserved at docs/releases/v0.2.1.md -->
<!-- v0.3.1 entry removed when v0.3.4 rolled into the 3-most-recent window; full notes preserved at docs/releases/v0.3.1.md -->
<!-- v0.3.2 entry removed when v0.3.5 rolled into the 3-most-recent window; full notes preserved at docs/releases/v0.3.2.md -->
