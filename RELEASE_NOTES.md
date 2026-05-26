# Release Notes

This file contains release notes for up to the three most recent releases in reverse chronological order. For the complete release history, see the [CHANGELOG](CHANGELOG.md) or the [docs/releases/](docs/releases/) directory.

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

## v0.2.0 (2026-05-20)

**Library-Enabling and Scaling — Far More Complex Reflow Patterns**

v0.2.0 grows the tool along three axes simultaneously: stable library surface for Go consumers, deeper content-aware reflow patterns (derived vars, mixed segments, lookups, mirrored sidecars, Hive partitions, canonical-by-ETag dedup), and correctness primitives that keep behavior right at scale (atlas, conditional CAS, parallel-race arbitration). The core promise stays the same: predictable, prefix-first crawls with JSONL-first outputs.

### Library Enablement

Public URI parser package (`pkg/uri`), library-consumer config-contract docs, and reliable version stamping for `go install`-built binaries:

```go
import "github.com/3leaps/gonimbus/pkg/uri"

ou, err := uri.ParseObjectURI("s3://my-bucket/some/prefix/object.json")
```

```bash
go install github.com/3leaps/gonimbus/cmd/gonimbus@v0.2.0
gonimbus version
# gonimbus 0.2.0
```

See [`docs/library-consumers.md`](docs/library-consumers.md) for the embedding contract (credentials, env-var precedence, hermetic-embedder posture, dep-tree boundary).

### Reflow + Probe Sophistication

Derived variables, mixed-segment rewrites, lookup transforms, mirrored sidecars, Hive-partition pattern, and canonical-by-ETag dedup all compose in one pipeline:

```yaml
# probe.yaml — derived vars + lookup transform
extract:
  - name: date
    xpath: //Record/Date

derived:
  - name: year
    from: date
    transform: substring
    start: 0
    length: 4
  - name: region
    from: site_id
    transform: lookup
    match: prefix
    table:
      a1: north
      b2: west
```

```yaml
# rewrite.yaml — mixed-segment rendering
rewrite:
  destination_template: "reflowed/region={region}/year={year}/{rel_key}"
```

Provenance sidecars (`.gnb.json` next to each destination, or under `--provenance-sidecar-root`) record source URI, derived fields, and rewrite path. `--on-collision quarantine` writes collisions to an explicit prefix; nested `collision` metadata replaces the legacy flat fields.

Content-fingerprint dedup at query time:

```bash
gonimbus index query 's3://bucket/prefix/' --canonical-by-etag
```

### Scaling Correctness

- **Atlas phase A** (`gonimbus atlas build`) — derived views across completed indexes for cross-run analytics without re-scanning the substrate
- **Conditional `latest.json` CAS** — fail-closed publish semantics via `If-Match` / `If-None-Match` on substrates that support it; best-effort fallback preserved for v0.1.x-compatible hubs
- **Atomic conditional puts** — provider-level `If-None-Match: *` opt-in for race-safe creation
- **Parallel-race arbitration** — `transfer reflow --parallel N>1` arbitrates per destination key before issuing conditional writes, preserving the `skip-if-duplicate` contract even on substrates that don't enforce `If-None-Match: *`
- **Opt-in build summary** — `gonimbus index build --summary` emits per-run JSONL totals without changing default streaming behavior

### Stack

Bounded dependency refresh: gofulmen v0.3.5 (pulls crucible v0.4.12, doublestar v4.10.0, zap v1.28.0), AWS SDK family coherent settle (`aws-sdk-go-v2` v1.41.7, `s3` v1.101.0, smithy v1.25.1), chi v5.2.5, mapstructure v2.5.0, cobra v1.10.2, `golang.org/x/net` v0.54.0, `golang.org/x/time` v0.15.0.

CI runner image now `goneat-tools-runner-glibc:v0.4.2` (glibc variant required for libsql cgo); see [`docs/development/ci.md`](docs/development/ci.md).

### Breaking Changes

**Reflow collision flat fields removed.** The legacy `collision_kind`, `collision_etag`, `collision_size_bytes` are no longer emitted in `gonimbus.reflow.v1` records. Audit tooling must read the nested `collision` object. The Phase A warning window has expired.

**`--on-collision log` deprecated.** Use `--on-collision skip-if-duplicate` (same behavior, clearer name). The alias is retained for one minor release and removed in v0.3.0.

### Upgrade

```bash
go install github.com/3leaps/gonimbus/cmd/gonimbus@v0.2.0
```

Re-install required to pick up the version-stamping fix; older binaries built via `go install` report incorrect version strings.

See [docs/releases/v0.2.0.md](docs/releases/v0.2.0.md) for the complete release notes.

---

For v0.1.8 and earlier release notes, see [docs/releases/](docs/releases/) or
the [CHANGELOG](CHANGELOG.md).

<!-- v0.1.8 entry removed when v0.2.2 rolled into the 3-most-recent window; full notes preserved at docs/releases/v0.1.8.md -->
