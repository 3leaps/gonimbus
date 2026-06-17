# Release Notes

This file contains release notes for up to the three most recent releases in reverse chronological order. For the complete release history, see the [CHANGELOG](CHANGELOG.md) or the [docs/releases/](docs/releases/) directory.

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
  --rewrite-from '{key}' \
  --rewrite-to '{key}' \
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

---

## v0.3.1 (2026-06-14)

**Embedded S3 Auth Controls and Dependency Refresh**

v0.3.1 is a focused release for Go embedders and release operators. It adds
explicit S3 construction modes for unsigned public reads and caller-managed AWS
credential providers, pins the module toolchain directive to Go `1.26.4`, and
refreshes the past-cooling AWS SDK, smithy, routing, and platform packages used
by the release lane.

### Embedded S3 Auth Controls

`pkg/provider/s3.Config` now supports two explicit embedded-use credential
shapes:

- `Anonymous: true` for unsigned public-bucket reads.
- `CredentialsProvider` for caller-managed AWS SDK credential handles.

Credential precedence is now documented and enforced: anonymous reads, injected
provider, static keys, profile, then the AWS SDK default chain. Anonymous mode
is mutually exclusive with every credential source. It sends no
`Authorization` header and does not fall back to ambient credentials, even when
the embedding process has AWS environment variables, profiles, or instance
credentials available.

Anonymous mode is read-only. S3 write paths fail closed with
`provider.ErrAnonymousReadOnly` joined with `provider.ErrAccessDenied` before
issuing provider requests.

See [docs/library-consumers.md](docs/library-consumers.md#credential-injection)
for the supported embedded-use contract and examples.

### Dependency and Toolchain Refresh

The release pins `go.mod` to `toolchain go1.26.4`, matching the CI,
dependency-security, and release workflow Go patch version.

The dependency refresh is intentionally bounded to packages that cleared the
release cooling policy:

- AWS SDK for Go v2 root SDK `v1.41.12`, S3 service `v1.102.2`, and
  smithy-go `v1.27.2` family.
- `github.com/go-chi/chi/v5 v5.3.0`.
- `golang.org/x/sys v0.46.0`.

`golang.org/x/net v0.56.0` is deferred to v0.3.2 because its module timestamp
was still inside the seven-day cooling window for this release. The v0.3.1
release keeps `golang.org/x/net v0.55.0`, and dependency-security evidence
remains clean at the high-and-critical gate.

### Server Request IP Posture

The default server router no longer installs `chi/middleware.RealIP`. That
middleware rewrites `RemoteAddr` from caller-supplied forwarding headers; the
default server path now leaves `RemoteAddr` as the network peer address.
Gonimbus does not currently key access control, rate limiting, or audit logic
on rewritten client IPs. Future proxy-aware behavior should be added as an
explicit trusted-proxy configuration rather than implicit header trust.

### Probe Quarantine Routing Fix

`content probe` now preserves quarantine routing when a required `derived`
field depends on a missing required extractor configured with
`on_missing: quarantine`. The affected record routes to quarantine instead of
rendering with an unresolved derived field, including until-resolved probe
flows that derive date-style partition fields.

### Upgrade

```bash
go install github.com/3leaps/gonimbus/cmd/gonimbus@v0.3.1
```

CLI workflows remain compatible with v0.3.0. Go embedders using
`pkg/provider/s3` can opt into the new anonymous and injected-credential modes;
existing static-key, profile, and SDK-default-chain construction continues to
work with the documented precedence.

See [docs/releases/v0.3.1.md](docs/releases/v0.3.1.md) for the complete
release notes.

For v0.3.0 and earlier release notes, see [docs/releases/](docs/releases/) or
the [CHANGELOG](CHANGELOG.md).

<!-- v0.3.0 entry removed when v0.3.3 rolled into the 3-most-recent window; full notes preserved at docs/releases/v0.3.0.md -->
<!-- v0.2.3 entry removed when v0.3.2 rolled into the 3-most-recent window; full notes preserved at docs/releases/v0.2.3.md -->
<!-- v0.2.2 entry removed when v0.3.1 rolled into the 3-most-recent window; full notes preserved at docs/releases/v0.2.2.md -->
<!-- v0.2.1 entry removed when v0.3.0 rolled into the 3-most-recent window; full notes preserved at docs/releases/v0.2.1.md -->
