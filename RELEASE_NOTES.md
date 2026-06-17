# Release Notes

This file contains release notes for up to the three most recent releases in reverse chronological order. For the complete release history, see the [CHANGELOG](CHANGELOG.md) or the [docs/releases/](docs/releases/) directory.

---

## v0.3.3 (TBD)

**Adaptive Reflow Concurrency and Release Documentation**

Draft release-notes slot for v0.3.3. Product and technical wording should be
collapsed here from `docs/releases/v0.3.3.md` after cxotech and prodmktg finish
the detailed doc pass.

### Highlights

- `transfer reflow --parallel` now acts as a requested ceiling with adaptive
  concurrency enabled by default and `--no-adaptive` available for fixed-mode
  operation at the resource-capped effective ceiling.
- Priority XML XPath probe fallbacks now include audit fields and fail closed
  to quarantine when lower-priority values are observed at a truncated read
  boundary.
- Transfer reflow now emits compact classified resumable abort causes and
  redacts credential-bearing provider error details from published JSONL
  surfaces.
- S3 provider transport tuning is opt-in from transfer reflow and preserves the
  AWS SDK buildable HTTP client stack.

See [docs/releases/v0.3.3.md](docs/releases/v0.3.3.md) for the draft detailed
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

---

## v0.3.0 (2026-06-12)

**Pure-Go Index Defaults, Resumable Operations, Provider-Dispatched Reflow, and Release-Gate Hardening**

v0.3.0 moves the default local index store to pure-Go SQLite, adds resumable
failure handling for long-running index and reflow operations, expands
provider-dispatched transfer reflow so local file trees can be copied into
cloud object stores, and tightens release security gates.

### Pure-Go Index Store by Default

Default builds now use the pure-Go `modernc.org/sqlite` driver for local index
stores. Local SQLite-backed index workflows work without CGO:

```bash
gonimbus index init
gonimbus index build --job crawl-manifest.yaml
gonimbus index query 's3://bucket/path/**/*.xml'
```

The libsql/Turso driver remains available for operators who need remote libsql
databases:

```bash
go build -tags gonimbus_libsql ./cmd/gonimbus
```

### Resumable Long-Running Operations

Long-running `index build`, `index enrich-with-head`, and `transfer reflow`
runs now have a failed-resumable path for runtime interruptions. When a
resumable runtime failure occurs, gonimbus writes a sensitive-local operation
checkpoint outside the repository tree and prints a redacted resume summary.

Examples:

```bash
gonimbus index build --resume-run <run_id>
gonimbus index enrich-with-head --resume-run <run_id>
gonimbus transfer reflow --resume-run <run_id>
```

The resume path validates checkpoint identity and current credentials before it
makes data-plane calls or mutates run state.

### Provider-Dispatched Reflow and Local Backup Pipe

Command code now constructs providers through `internal/providerdispatch`,
which gives `transfer reflow` the same source/destination capability checks
across S3-compatible and local file providers.

The practical operator result is that local directory trees can be selected by
`crawl` and copied into object storage with normal reflow machinery:

```bash
gonimbus crawl --job backup-select.yaml --emit reflow-input \
  | gonimbus transfer reflow --stdin \
      --dest 's3://dest-bucket/landing/'
```

Hidden files and dot-directories are skipped by default. Symlinks are also
skipped by default; `--symlinks=follow` must be selected explicitly when the
operator wants resolved symlink targets included.

### Resume Collision and Failure-Class Fixes

v0.3.0 includes two reflow correctness fixes that matter for automation around
long-running copies:

- A resumed reflow no longer false-conflicts when the interrupted tail maps to
  already completed, byte-identical destination data.
- Temporary network and transport failures are classified as `TRANSIENT`
  rather than falling through to internal or unknown surfaces.

### Dependency Security Gate

The local pre-push hook stays scoped to changed-file format, lint, and security
checks. Dependency vulnerability enforcement runs in GitHub Actions on pull
requests, pushes to `main`, a daily schedule, and manual dispatch.

The release gate is clean at `fail_on=high`: `golang.org/x/net` is updated to
`v0.55.0`, CI and release workflows use Go `1.26.4`, and no dependency
suppressions are added.

### Upgrade

```bash
go install github.com/3leaps/gonimbus/cmd/gonimbus@v0.3.0
```

For local index workflows, the default binary no longer requires CGO.
Operators using remote libsql/Turso indexes should build with
`-tags gonimbus_libsql`.

See [docs/releases/v0.3.0.md](docs/releases/v0.3.0.md) for the complete
release notes.

---

## v0.2.3 (2026-05-31)

**Stream Put Completion, Reflow Freshness Arbitration, and API Guardrails**

v0.2.3 completes the stream write path, adds destination verification for
reflow outputs, adds a newest-wins collision mode for mirror-style reflow, and
formalizes Stable API and public-repository hygiene guardrails.

### Stream Put Completion

`gonimbus stream put` now writes stdin to a destination object. Raw mode writes
one object from raw bytes:

```bash
cat payload.xml | gonimbus stream put 's3://bucket/landing/payload.xml'
```

Framed JSONL mode consumes one or more `stream get` frames:

```bash
gonimbus stream get 's3://source-bucket/path/payload.xml' \
  | gonimbus stream put --framing jsonl 's3://dest-bucket/landing/payload.xml'
```

The CLI destination is authoritative. Exact destinations write one framed
object; trailing-slash destinations act as roots for framed batches. Frame
`dest_key` values are ignored unless `--dest-from-frame` is explicitly enabled,
and enabled frame keys must remain relative under the CLI root.

Large S3 writes use multipart upload after `--multipart-threshold` (default
64 MiB), with `--part-size` defaulting to 8 MiB. Once multipart upload begins,
gonimbus streams parts without buffering the full object and emits
`gonimbus.stream.progress.v1` records.

### Reflow Freshness Arbitration

`transfer reflow` adds `--on-collision overwrite-if-source-newer` for
mirror-style flows where object-store `LastModified` is the freshness signal:

```bash
gonimbus transfer reflow --stdin \
  --dest 's3://dest-bucket/landing/' \
  --rewrite-from '{key}' \
  --rewrite-to '{key}' \
  --on-collision overwrite-if-source-newer
```

The mode first attempts the normal atomic create path. If a non-identical
destination already exists, gonimbus HEADs the destination, compares source and
destination `LastModified`, and overwrites only when the source is newer or when
timestamps are equal but sizes differ. The overwrite uses the observed
destination ETag as an `If-Match` precondition; a concurrent destination change
becomes a deterministic skipped record rather than an unguarded overwrite.

### Pair Verification

`gonimbus inspect-pair` verifies terminal reflow write claims after a run:

```bash
gonimbus inspect-pair \
  --from-reflow reflow.jsonl \
  --expected-dest-prefix 's3://dest-bucket/landing/'
```

The command reads `gonimbus.reflow.v1` records, validates destination URIs
against the expected prefix, HEADs claimed writes, and emits per-object
`gonimbus.inspect.pair.v1` records plus a
`gonimbus.inspect.pair.summary.v1` summary.

### Stable API and Public-Surface Guardrails

v0.2.3 introduces `docs/api-stability.md` and `make api-stability` as the
release guard for the Stable embedded library packages. Stable API changes now
need a changelog entry that states the break, migration path, and advance
notice status before release.

This release includes one Stable API break: `provider.MultipartUploader` now
requires implementers to provide `UploadPart` and `CompleteMultipartUpload`.
Providers that advertise multipart capability must implement the full
create/upload-part/complete/abort lifecycle, or stop advertising
`MultipartUploader` until they can support it.

The repository also now points contributor and agent guidance at the canonical
3leaps OSS sensitive-data policy. The practical rule is structural: sensitive
or proprietary local data belongs outside repository working trees, not merely
behind `.gitignore`.

### Upgrade

```bash
go install github.com/3leaps/gonimbus/cmd/gonimbus@v0.2.3
```

CLI JSONL additions are additive. The notable compatibility item is for Go
embedders implementing `provider.MultipartUploader`; update those providers
before moving to v0.2.3, or remove multipart capability advertisement until the
new lifecycle is implemented.

See [docs/releases/v0.2.3.md](docs/releases/v0.2.3.md) for the complete
release notes.

---

For v0.2.2 and earlier release notes, see [docs/releases/](docs/releases/) or
the [CHANGELOG](CHANGELOG.md).

<!-- v0.2.2 entry removed when v0.3.1 rolled into the 3-most-recent window; full notes preserved at docs/releases/v0.2.2.md -->
<!-- v0.2.1 entry removed when v0.3.0 rolled into the 3-most-recent window; full notes preserved at docs/releases/v0.2.1.md -->
