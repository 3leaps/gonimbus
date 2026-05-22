# Release Notes

This file contains release notes for up to the three most recent releases in reverse chronological order. For the complete release history, see the [CHANGELOG](CHANGELOG.md) or the [docs/releases/](docs/releases/) directory.

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

## v0.1.8 (2026-05-05)

**Index Hub + Workspace Pattern + DX Hardening — Final v0.1.x Release**

This release closes out the v0.1.x line by delivering the publishable / consumable index lifecycle: build an index locally, **publish** it to a hub, **consume** it on another host, and manage hub contents over time. Paired with a documented workspace convention and DX hardening, this is the operational toolchain that production data-acquisition pipelines need.

### Index Hub

Publish (`export`) and consume (`hydrate`) index runs against `file://` and `s3://` hubs, with full CRUD:

```bash
# Initialize a hub root
gonimbus index hub init --hub s3://my-hub/

# Publish a run
gonimbus index hub init --hub s3://my-hub/
gonimbus index export --index-set idx_<sha256> --hub s3://my-hub/

# Consume on another host
gonimbus index hydrate --index-set idx_<sha256> --hub s3://my-hub/

# Manage hub contents
gonimbus index hub ls --hub s3://my-hub/
gonimbus index hub show --hub s3://my-hub/ --index-set idx_<sha256>
gonimbus index hub set-latest --hub s3://my-hub/ --index-set idx_<sha256> --run-id run_<id>
gonimbus index hub rm-run --hub s3://my-hub/ --index-set idx_<sha256> --run-id run_<id>
gonimbus index hub gc --hub s3://my-hub/ --keep 5 --json
```

#### Publish Sequence (atomic-ish)

`index.db` → `identity.json` → `complete.json` (commit marker) → `latest.json`. Hydrate verifies SHA-256 + size against the integrity manifest in `complete.json` and rejects uncommitted runs.

#### latest.json Pointer

`latest.json` updates use plain `PutObject` — best-effort, last-writer-wins. CAS / fail-closed semantics (If-Match / If-None-Match, etag plumbing) are tracked for v0.2.x.

### Index Query Flags

```bash
# Explicit index-set selection (resolves prefix or full idx_<64hex>)
gonimbus index query 's3://bucket/prefix/' --index-set idx_da038d8

# Stream results to S3 / file destinations
gonimbus index query 's3://bucket/prefix/' --output 's3://results/query.jsonl'
gonimbus index query 's3://bucket/prefix/' --output 'file:///tmp/query.jsonl'
```

### Workspace Pattern

`workspace.yaml` convention with documented layout, shard strategies, and operational flows:

- Build + publish (crawl → index → export)
- Hydrate + query (consume on remote host)
- Extract + reflow (probe → transfer reflow with content-aware routing)
- Hub maintenance (set-latest, rm-run, gc)

See [`docs/user-guide/workspace.md`](docs/user-guide/workspace.md) for the full pattern.

### DX Hardening

- Pre-push hook scoped to `--new-issues-only --new-issues-base origin/main` so unrelated changes don't pay for legacy lint debt
- Pre-existing high-severity gosec / golangci-lint findings annotated with rationale or fixed
- Guardian browser-intercept hooks removed; the team is on a feature-branch workflow

### Bug Fixes

- `gonimbus index hub gc --json` (without `--dry-run`) silently no-oped deletions; fixed to honor `--dry-run` correctly and emit per-run outcomes in the JSON envelope

### Upgrade Notes

No breaking changes from v0.1.7. Upgrade with:

```bash
go install github.com/3leaps/gonimbus/cmd/gonimbus@v0.1.8
```

### What's Next

v0.1.x is complete. v0.2.x will introduce control-plane capabilities: managed runner, queue consumer, job lifecycle, and conditional-update (CAS / fail-closed) semantics for `latest.json`. GCS provider also lands in v0.2.x.

See [docs/releases/v0.1.8.md](docs/releases/v0.1.8.md) for complete release notes.

---

For v0.1.7 and earlier release notes, see [docs/releases/](docs/releases/) or the [CHANGELOG](CHANGELOG.md).

<!-- v0.1.7 entry removed when v0.2.1 rolled into the 3-most-recent window; full notes preserved at docs/releases/v0.1.7.md -->
