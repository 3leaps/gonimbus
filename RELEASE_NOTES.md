# Release Notes

This file contains release notes for up to the three most recent releases in reverse chronological order. For the complete release history, see the [CHANGELOG](CHANGELOG.md) or the [docs/releases/](docs/releases/) directory.

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

## v0.2.2 (2026-05-26)

**Index Archive Operations and Local-Tree Reflow**

v0.2.2 improves two operator paths: using `transfer reflow` with local
directory sources, and using indexes to plan work around storage class,
archive, restore, and content-type state. It also moves CI and release builds
to Go `1.26.3` with updated lint tooling.

### Local-Tree Reflow

`transfer reflow` now accepts `file://` sources and routes them through the
same rewrite, collision, metadata, checkpoint, dry-run, and JSONL audit path
used for object-store sources:

```bash
gonimbus transfer reflow 'file:///absolute/source-root/' \
  --dest 's3://bucket/landing/' \
  --rewrite-from '{path}/{file}' \
  --rewrite-to '{path}/{file}' \
  --dry-run
```

Local-tree reflow skips hidden files and dot-directories by default. Use
`--hidden=include` only when those paths are expected at the destination, and
pair dry-run review with explicit `--exclude` rules for non-hidden generated
paths such as `node_modules/*`, `dist/*`, `target/*`, and log files.

### Storage-Class Querying

Index builds now retain LIST-derived provider storage class. Query JSONL emits
`storage_class` when present, and `index query --storage-class` filters exact,
case-sensitive values:

```bash
gonimbus index query 's3://bucket/prefix/' \
  --storage-class GLACIER,DEEP_ARCHIVE
```

The flag is repeatable, accepts comma-separated values, and composes with
canonical-by-ETag query mode.

### HEAD Enrichment

`index enrich-with-head` caches HEAD-derived archive and restore metadata on an
existing index after applying candidate filters:

```bash
gonimbus index enrich-with-head idx_da038d8171b4a9ba \
  --storage-class GLACIER,DEEP_ARCHIVE \
  --pattern "**/*.xml" \
  --parallel 32
```

The command writes `archive_status`, `restore_state`, `restore_expiry`,
`content_type`, and `head_enriched_at`. It does not overwrite LIST-derived
storage class, size, ETag, last-modified, or deleted-state fields. `index query`
can emit the enriched fields and filter by `--enriched-after`.

### Release Toolchain

- CI and release workflows pin Go `1.26.3`.
- CI uses `golangci-lint-action` v2.11.2 for the Go 1.26 lane.
- Version identity is stamped as `0.2.2` across the repository and embedded
  build identity.

### Upgrade

```bash
go install github.com/3leaps/gonimbus/cmd/gonimbus@v0.2.2
```

Existing object-store reflow and index query workflows remain compatible. For
local-tree reflow, run `--dry-run` before the first write and review hidden path
and exclude policy explicitly.

See [docs/releases/v0.2.2.md](docs/releases/v0.2.2.md) for the complete
release notes.

---

## v0.2.1 (2026-05-22)

**Reflow Destination Metadata — Explicit Control, Explicit Disclosure Discipline**

v0.2.1 is a focused operator release for landing reflowed objects with durable destination metadata. It adds caller-controlled metadata, content-type and storage-class controls, per-object metadata derivation from source metadata and system fields, and a release-lane hardening pass. The headline is not only the feature surface: the disclosure posture is part of the contract. Destination metadata is durable and visible to destination readers, so v0.2.1 makes allow-list discipline and destination-system-metadata boundaries explicit.

### Destination Metadata Controls

`transfer reflow` can now set destination user metadata and selected destination PUT attributes directly:

```bash
gonimbus transfer reflow --stdin \
  --dest 's3://dest/landing/' \
  --rewrite-from '{key}' \
  --rewrite-to '{business_date}/{key}' \
  --metadata-policy clear \
  --metadata-set dataset=transactions \
  --metadata-set owner=data-platform \
  --preserve-content-type \
  --destination-storage-class STANDARD_IA
```

`--metadata-policy clear|preserve|merge` controls source user-metadata handling. `clear` keeps the historic default: write only explicitly requested destination metadata. `preserve` copies source user metadata and fails the object on canonical key collisions. `merge` copies source metadata, then applies `--metadata-set` overrides. File destinations write metadata to cleartext `.gnb-meta.json` sidecars.

### Per-Object Metadata Derivation

When each destination object needs values derived from the corresponding source object, v0.2.1 adds explicit per-object rules:

```bash
gonimbus transfer reflow --stdin \
  --dest 's3://dest/landing/' \
  --rewrite-from '{key}' \
  --rewrite-to '{business_date}/{key}' \
  --metadata-policy clear \
  --metadata-set source-system=example \
  --metadata-set-from-source-key source-md5=md5 \
  --metadata-set-from-source-derived source-etag='system.etag' \
  --metadata-set-from-source-derived broker-device='urldecode(meta.payload).device' \
  --metadata-on-missing-source skip
```

Supported v1 expressions are deliberately small: JSON subfields (`meta.payload.device`), URL-decoded JSON subfields (`urldecode(meta.payload).device`), source system fields (`system.etag`, `system.last_modified`, `system.content_length`, `system.content_type`, `system.storage_class`), and one string-literal concatenation (`system.etag + "-src"`). JSON numbers preserve source precision via `json.Decoder.UseNumber`-style handling; JSON booleans render as `true` / `false`.

`--metadata-on-missing-source skip|fail|empty` controls missing keys, invalid JSON, URL-decode failures, null values, arrays, and objects. The default `skip` omits only the affected destination key. `empty` writes an empty string. `fail` emits a redacted per-object `gonimbus.error.v1`; when paired with `--on-collision quarantine`, the object routes to the quarantine prefix with reason `metadata.derivation.quarantined`.

### Security and Disclosure Posture

This release treats metadata disclosure as an operator-visible headline:

- **Destination metadata is durable and not redacted at destination.** Values supplied by `--metadata-set`, copied via `preserve`/`merge`, derived per object, preserved as content type, propagated as storage class, or written to local file sidecars are visible to destination readers with HEAD/GET or filesystem access.
- **Use allow-lists for sensitive source buckets.** Prefer `--metadata-policy clear` plus explicit `--metadata-set`, `--metadata-set-from-source-key`, and `--metadata-set-from-source-derived` rules when source metadata may contain credential URIs, tokens, or other sensitive values. Gonimbus rejects wildcard metadata projection.
- **Scalar-only derivation protects the allow-list boundary.** v1 writes only string, number, and boolean JSON subfield values. Null, arrays, and objects route through `--metadata-on-missing-source`, preventing accidental serialization of entire nested structures.
- **Destination system metadata is not writable through derivation.** `system.<field>` reads source-side system fields as inputs, then writes a destination user-metadata key. It does not override destination ETag, LastModified, ContentLength, or other object-store-generated system fields. Content-Type and StorageClass are controlled only by their explicit flags.
- **Failure paths redact raw source values.** Derivation-evaluation failures report destination key, expression, and reason kind without echoing the source metadata value in error JSONL, checkpoint rows, or default stderr.

### Release Lane Hardening

- CI and release workflows pin Go `1.25.10` for attributable vulnerability scans and SBOMs.
- `make dependencies` emits stable SBOM and vulnerability-report artifacts under `sbom/`.
- YAML formatting is pinned with `.yamlfmt` and `.yamllint` so local formatting and CI agree.
- Index-store setup applies `busy_timeout` before WAL mode to reduce intermittent root DB open lock flakes.

### Upgrade

```bash
go install github.com/3leaps/gonimbus/cmd/gonimbus@v0.2.1
```

Reflow behavior remains opt-in: existing reflow runs that do not pass the new metadata flags keep writing destination objects without caller-controlled metadata. Review any new metadata rules as part of pipeline disclosure review before running against source buckets with sensitive metadata.

See [docs/releases/v0.2.1.md](docs/releases/v0.2.1.md) for the complete release notes.

---

For v0.1.8 and earlier release notes, see [docs/releases/](docs/releases/) or
the [CHANGELOG](CHANGELOG.md).

<!-- v0.2.0 entry removed when v0.2.3 rolled into the 3-most-recent window; full notes preserved at docs/releases/v0.2.0.md -->
<!-- v0.1.8 entry removed when v0.2.2 rolled into the 3-most-recent window; full notes preserved at docs/releases/v0.1.8.md -->
