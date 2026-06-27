# Transfer Operations

The `gonimbus transfer` command copies or moves objects between S3-compatible buckets with optional path transformation.

## Quick Start

```bash
# Validate manifest syntax
gonimbus transfer --job transfer.yaml --plan

# Safety latch (recommended when dogfooding): disable all provider-side mutations
# This blocks both transfers and write-probe preflight.
export GONIMBUS_READONLY=1

# Run preflight checks without transferring
gonimbus transfer --job transfer.yaml --dry-run

# Execute transfer (requires readonly disabled)
unset GONIMBUS_READONLY
# or: gonimbus transfer --job transfer.yaml --readonly=false

gonimbus transfer --job transfer.yaml
```

## Transfer Manifest

Transfer operations are defined in YAML manifests:

```yaml
version: "1.0"

source:
  provider: s3
  bucket: source-bucket
  region: us-east-1

target:
  provider: s3
  bucket: target-bucket
  region: us-west-2

match:
  includes:
    - "data/**/*.parquet"
  excludes:
    - "**/_temporary/**"

transfer:
  mode: copy # copy or move
  concurrency: 16 # parallel workers
  on_exists: skip # skip, overwrite, or fail
  path_template: "" # optional path transformation
  dedup:
    enabled: true
    strategy: etag # etag, key, or none
  preflight:
    mode: write-probe # plan-only, read-safe, or write-probe
    probe_strategy: multipart-abort

output:
  destination: stdout # or file:/path/to/output.jsonl
```

## Transfer Modes

| Mode   | Description                                       |
| ------ | ------------------------------------------------- |
| `copy` | Copy objects, keep source intact (default)        |
| `move` | Copy objects, delete source after successful copy |

## On-Exists Behavior

| Setting     | Description                     |
| ----------- | ------------------------------- |
| `skip`      | Skip if target exists (default) |
| `overwrite` | Replace existing target objects |
| `fail`      | Error if target exists          |

## Deduplication

When `on_exists: skip`, deduplication prevents redundant transfers:

| Strategy | Comparison    | Use Case                         |
| -------- | ------------- | -------------------------------- |
| `etag`   | Compare ETags | Same content detection (default) |
| `key`    | Key exists    | Skip if target key exists        |
| `none`   | Disabled      | Always transfer                  |

**Note:** ETag comparison may not work reliably for multipart uploads or server-side encrypted objects.

## Path Transformation

The `path_template` field transforms source keys to target keys during transfer.

### Supported Placeholders

| Placeholder  | Description             | Example Input               | Example Output |
| ------------ | ----------------------- | --------------------------- | -------------- |
| `{filename}` | Final path segment      | `a/b/c.txt`                 | `c.txt`        |
| `{dir[n]}`   | Nth directory (0-based) | `a/b/c.txt` with `{dir[0]}` | `a`            |
| `{key}`      | Full source key         | `a/b/c.txt`                 | `a/b/c.txt`    |

### Examples

**Flatten structure:**

```yaml
# Source: region/store/device/2024/01/file.json
# Target: file.json
transfer:
  path_template: "{filename}"
```

**Date-first reorganization:**

```yaml
# Source: region/store/device/2024/01/file.json
# Target: 2024/01/region/file.json
transfer:
  path_template: "{dir[3]}/{dir[4]}/{dir[0]}/{filename}"
```

**Add archive prefix:**

```yaml
# Source: data/file.json
# Target: archive/2024/data/file.json
transfer:
  path_template: "archive/2024/{key}"
```

**Remove middle layers:**

```yaml
# Source: vendor/raw/unprocessed/data/file.json
# Target: data/file.json
transfer:
  path_template: "{dir[3]}/{filename}"
```

### Validation

Templates are validated before any transfers occur. Invalid templates (unsupported placeholders, out-of-range directory indices) cause immediate failure with a clear error message.

## Preflight Checks

Transfer operations run preflight checks before enumeration to fail fast on permission issues.

**Readonly safety latch:** If `--readonly` (or `GONIMBUS_READONLY=1`) is set, Gonimbus refuses `write-probe` preflight and refuses to execute transfers.

### Preflight Modes

| Mode          | Provider Calls      | Use Case                      |
| ------------- | ------------------- | ----------------------------- |
| `plan-only`   | None                | Validate manifest syntax only |
| `read-safe`   | List, Head, Get     | Validate read permissions     |
| `write-probe` | Above + write probe | Validate write permissions    |

### Capability Checks

Preflight validates these capabilities in fail-fast order:

1. **target.write** (write-probe mode only): Can we write to the target?
2. **source.list**: Can we list the source prefix?
3. **source.read**: Can we read objects from source?
4. **target.head**: Can we check target existence? (when `on_exists != overwrite`)

### Write Probe Strategies

| Strategy          | Operation                       | Side Effects       |
| ----------------- | ------------------------------- | ------------------ |
| `multipart-abort` | Create + abort multipart upload | None (preferred)   |
| `put-delete`      | Put 0-byte object + delete      | Minimal (fallback) |

## Output Records

Transfer emits JSONL records for each operation:

### Transfer Record

```json
{
  "type": "gonimbus.transfer.v1",
  "ts": "2024-01-15T10:00:00Z",
  "job_id": "abc123",
  "provider": "s3",
  "data": {
    "source_key": "data/input/file.json",
    "target_key": "archive/2024/file.json",
    "bytes": 1048576
  }
}
```

### Skip Record

```json
{
  "type": "gonimbus.skip.v1",
  "ts": "2024-01-15T10:00:01Z",
  "job_id": "abc123",
  "provider": "s3",
  "data": {
    "source_key": "data/input/existing.json",
    "target_key": "archive/2024/existing.json",
    "reason": "dedup.etag"
  }
}
```

### Error Record and Failure Classes

Per-object transfer and reflow failures are emitted as `gonimbus.error.v1`
records with a machine-readable `code` and a human-readable `message`. The
stable failure classes are:

| Code                   | Meaning                                                                                                                                 |
| ---------------------- | --------------------------------------------------------------------------------------------------------------------------------------- |
| `ACCESS_DENIED`        | Credentials or policy denied the operation.                                                                                             |
| `NOT_FOUND`            | The source object, destination object, or bucket was missing for the attempted operation.                                               |
| `TIMEOUT`              | The operation context expired or was canceled before completion.                                                                        |
| `THROTTLED`            | The provider reported rate limiting.                                                                                                    |
| `PROVIDER_UNAVAILABLE` | The provider service reported an availability failure.                                                                                  |
| `TRANSIENT`            | A temporary network or transport failure, such as DNS failure, connection reset, mid-stream EOF, I/O timeout, or TLS handshake timeout. |
| `ALREADY_EXISTS`       | An atomic destination create was refused because the object already exists.                                                             |
| `INVALID_INPUT`        | The input record, template, metadata expression, or caller configuration was malformed.                                                 |
| `INTERNAL`             | An unexpected Gonimbus failure that does not match a more specific class.                                                               |

Use `TRANSIENT` for caller-side retry decisions when orchestrating reflow over
flaky networks. Gonimbus v0.3.0 classifies these failures but does not add a new
automatic retry policy inside `transfer reflow`; callers that own the broader
pipeline can retry or resume based on the emitted class.

### Summary Record

```json
{
  "type": "gonimbus.summary.v1",
  "ts": "2024-01-15T10:01:00Z",
  "job_id": "abc123",
  "provider": "s3",
  "data": {
    "objects_found": 1000,
    "objects_matched": 850,
    "bytes_total": 1073741824,
    "duration_ns": 60000000000,
    "duration": "1m0s",
    "errors": 2
  }
}
```

## Cross-Account Transfers

For transfers between different AWS accounts, use separate credentials:

```yaml
source:
  provider: s3
  bucket: source-bucket
  profile: source-account # Uses ~/.aws/credentials [source-account]

target:
  provider: s3
  bucket: target-bucket
  profile: target-account # Uses ~/.aws/credentials [target-account]
```

Or use environment variables with role assumption.

## S3-Compatible Storage

Transfer works with any S3-compatible storage:

```yaml
# Wasabi to AWS S3
source:
  provider: s3
  bucket: wasabi-bucket
  endpoint: https://s3.us-east-2.wasabisys.com
  region: us-east-2

target:
  provider: s3
  bucket: aws-bucket
  region: us-east-1

transfer:
  preflight:
    mode: write-probe
    probe_strategy: put-delete # Use fallback for Wasabi
```

## Best Practices

1. **Always run preflight first**: Use `--dry-run` to validate permissions before large transfers
2. **Use write-probe for production**: Validates write permissions before wasting time on enumeration
3. **Start with small batches**: Test path_template with a narrow include pattern first
4. **Monitor with JSONL output**: Pipe output to analysis tools for large transfers
5. **Use dedup for reruns**: `on_exists: skip` with `strategy: etag` prevents redundant copies

## Transfer Reflow

The `transfer reflow` command reorganizes objects by rewriting keys based on templates. Unlike manifest-based transfers, reflow operates directly from URIs or probe output, making it ideal for:

- **Reorganizing misfiled data** - files placed by arrival date but need organization by business date
- **Structural migrations** - flattening hierarchies or adding prefix layers
- **Content-aware routing** - using fields extracted from file content to determine destination paths

### Quick Start

```bash
# Path-based reflow (extract variables from path segments)
gonimbus transfer reflow 's3://source/data/' \
  --dest 's3://dest/reorganized/' \
  --rewrite-from '{region}/{store}/{date}/{file}' \
  --rewrite-to '{date}/{region}/{store}/{file}' \
  --dry-run

# Content-aware reflow (using probe-derived variables)
gonimbus content probe --stdin --config probe.yaml < uris.txt | \
  gonimbus transfer reflow --stdin \
    --dest 's3://dest/by-business-date/' \
    --rewrite-from '{_}/{store}/{device}/{folder_date}/{file}' \
    --rewrite-to '{business_date}/{store}/{file}'
```

For S3 destinations, `transfer reflow --dry-run` performs one explicit
conditional zero-byte write probe under `<dest>/.gonimbus-preflight/`, then
deletes the probe object. Set `--readonly` or `GONIMBUS_READONLY=1` to suppress
that provider-side mutation.

### Template Variables

Templates use `{variable}` placeholders that are extracted from source paths or probe output:

| Variable    | Source                     | Example                           |
| ----------- | -------------------------- | --------------------------------- |
| `{segment}` | Path segment by name       | `{store}`, `{date}`, `{file}`     |
| `{_}`       | Ignored segment (wildcard) | Matches any segment, not captured |
| Probe vars  | From `content probe`       | `{business_date}`, `{version}`    |

Template segments may be fully literal, a single placeholder, or one
placeholder with a literal prefix and/or suffix. This supports segments such
as `year={year}` and `prefix-{token}-suffix`. A segment may contain only one
placeholder; templates such as `{a}-{b}` remain unsupported.

When a probe recipe derives values from path captures, pass the same
`--rewrite-from` template to `content probe` and `transfer reflow`.
`content probe --rewrite-from` applies to the parsed source key, not the full
URI, and seeds captures such as `{file}` before `derived` evaluation.

### Path-Based Reflow

Extract variables directly from source path structure:

```bash
# Source: data/us-east/store-123/2024-01-15/receipt.xml
# Template: data/{region}/{store}/{date}/{file}
# Variables: region=us-east, store=store-123, date=2024-01-15, file=receipt.xml

gonimbus transfer reflow 's3://bucket/data/' \
  --dest 's3://bucket/reorganized/' \
  --rewrite-from 'data/{region}/{store}/{date}/{file}' \
  --rewrite-to '{date}/{region}/{store}/{file}' \
  --dry-run
```

### Content-Aware Reflow

When the destination path depends on content inside files (not just the path), use `content probe` to extract variables first:

```bash
# 1. Create probe config
cat > probe.yaml << 'EOF'
extract:
  - name: business_date
    type: xml_xpath
    xpath: //BusinessDate
EOF

# 2. List and probe objects
gonimbus inspect 's3://source/prefix/' --json | \
  jq -r 'select(.key?) | "s3://source/" + .key' | \
  gonimbus content probe --stdin --config probe.yaml \
  --rewrite-from 'prefix/{store}/{device}/{folder_date}/{file}' \
  --emit reflow-input \
  > probe-output.jsonl

# 3. Reflow using extracted business_date
gonimbus transfer reflow --stdin \
  --dest 's3://dest/by-date/' \
  --rewrite-from 'prefix/{store}/{device}/{folder_date}/{file}' \
  --rewrite-to '{business_date}/{store}/{file}' \
  < probe-output.jsonl
```

Derived probe variables can decompose one extracted value before reflow:

```yaml
extract:
  - name: date
    type: regex
    pattern: "date=([0-9-]+)"
    group: 1
  - name: subject
    type: regex
    pattern: "subject=([A-Za-z0-9_-]+)"
    group: 1

derived:
  - name: year
    from: date
    transform: substring
    args: { start: 0, end: 4 }
  - name: month
    from: date
    transform: substring
    args: { start: 5, end: 7 }
  - name: day
    from: date
    transform: substring
    args: { start: 8, end: 10 }
  - name: subject_lower
    from: subject
    transform: lowercase
```

Those derived names render like any other variable:

```bash
gonimbus transfer reflow --stdin \
  --dest 's3://dest/partitioned/' \
  --rewrite-from 'prefix/{subject}/{file}' \
  --rewrite-to 'year={year}/month={month}/day={day}/subject={subject_lower}/{file}' \
  < probe-output.jsonl
```

`lookup` can classify a path-captured basename in the same recipe:

```yaml
derived:
  - name: category
    from: file
    transform: lookup
    args:
      match_mode: prefix
      table:
        - { match: "RecordTypeAlpha", value: "category_alpha" }
        - { match: "RecordTypeBeta", value: "category_alpha" }
        - { match: "RecordTypeGamma", value: "category_beta" }
      default: "category_unclassified"
```

```bash
gonimbus content probe --stdin \
  --config probe.yaml \
  --rewrite-from 'prefix/{store}/{device}/{folder_date}/{file}' \
  --emit reflow-input < uris.txt |
gonimbus transfer reflow --stdin \
  --dest 's3://dest/classified/' \
  --rewrite-from 'prefix/{store}/{device}/{folder_date}/{file}' \
  --rewrite-to '{category}/{business_date}/{store}/{file}'
```

### Hive-Style Partition Layouts

Operators emitting into downstream-discoverable data trees for tools such as
Spark, Trino, Glue crawlers, or Iceberg often want Hive-style partition segments
such as `year=2026/month=01/day=12`. Gonimbus does not need a dedicated
partition-style flag for this: compose
`content probe` [`extract`](streaming.md#probe-configuration) rules, optional
[`derived`](streaming.md#derived-variables) transforms, and mixed
literal-variable rewrite segments from [Template Variables](#template-variables).

Suppose a source object at `s3://<bucket>/<source-prefix>/record-001.xml`
contains:

```xml
<record>
  <business_date>20260112</business_date>
  <subject_id>00042</subject_id>
  <body>...</body>
</record>
```

Use one probe recipe for both positional and Hive-style destination layouts:

```yaml
# recipe.yaml
extract:
  - name: business_date
    type: xml_xpath
    xpath: "//business_date"
    required: true
  - name: subject_id
    type: xml_xpath
    xpath: "//subject_id"
    required: true

derived:
  - name: year
    from: business_date
    transform: substring
    args: { start: 0, end: 4 }
  - name: month
    from: business_date
    transform: substring
    args: { start: 4, end: 6 }
  - name: day
    from: business_date
    transform: substring
    args: { start: 6, end: 8 }
```

`xml_xpath` extractors accept the bare-element-name form (`//business_date`).
Deeper paths such as `//header/business_date` and XPath function calls such as
`/text()` are out of scope for the current extractor.

Each example below uses the same two-step pipeline: `content probe
--emit reflow-input` resolves source URIs into JSONL records carrying the
extracted and derived `vars`, then `transfer reflow --stdin` renders destination
keys with the operator-chosen `--rewrite-to` template. This split is
intentional: operators can cache probe output, parallelize stages, or re-emit
the same probed records with a different template without re-probing.

Example A uses positional rendering:

```bash
gonimbus content probe \
    --config recipe.yaml \
    --emit reflow-input \
    s3://<bucket>/<source-prefix>/ \
  | gonimbus transfer reflow --stdin \
    --dest s3://<bucket>/<dest-prefix>/ \
    --rewrite-to '{year}/{month}/{day}/{subject_id}/{file}'
```

It renders:

```text
s3://<bucket>/<dest-prefix>/2026/01/12/00042/record-001.xml
```

Example B uses Hive-style rendering. The recipe and probe stage are unchanged;
only the `--rewrite-to` template differs:

```bash
gonimbus content probe \
    --config recipe.yaml \
    --emit reflow-input \
    s3://<bucket>/<source-prefix>/ \
  | gonimbus transfer reflow --stdin \
    --dest s3://<bucket>/<dest-prefix>/ \
    --rewrite-to 'year={year}/month={month}/day={day}/subject={subject_id}/{file}'
```

It renders:

```text
s3://<bucket>/<dest-prefix>/year=2026/month=01/day=12/subject=00042/record-001.xml
```

The same recipe and the same probe invocation produce both renderings. Operators
choose positional or Hive-style output in the `transfer reflow` template, not in
the probe recipe.

`derived.substring` is a positional slice over a string, so the input's
zero-padded shape is preserved in the output. In the example above,
`business_date: "20260112"` yields `month: "01"` and `day: "12"` without an
extra `pad` transform, and `subject_id: "00042"` renders unchanged. Use
`derived.pad` only when the source value is short and needs padding added.

Two concerns stay separate:

- **Which dimensions appear**: the operator or orchestration layer decides which
  path-captured vars and content-probed vars should participate.
- **How dimensions render**: the `--rewrite-to` template decides whether those
  vars render positionally (`2026/01/12`) or as Hive-style segments
  (`year=2026/month=01/day=12`).

Gonimbus deliberately does not auto-promote every extracted variable into a
partition dimension. Omit any extracted or derived variable from `--rewrite-to`
when it should remain available for audit but not appear in the destination key.

This section does not add or imply these deferred capabilities:

- **Auto-Hive-emission flag**: there is no `--hive-partition` flag that wraps
  every extracted token as `<name>={<name>}`. Write the template explicitly when
  a real pipeline needs Hive-style output.
- **Dimension-name aliases**: reuse extracted and derived names directly in the
  template. If recipes routinely grow large enough that aliasing matters, file a
  follow-on brief with that operator need.
- **Partition-character validation**: Gonimbus does not currently reject
  extracted values containing characters such as `/`, `=`, `?`, `#`, or `%`.
  If a downstream partition-discovery tool needs a specific reserved set, file a
  follow-on brief grounded in that tool's rule.
- **Type-aware formatting or int conversion**: the current closed transform set
  covers `substring`, `regex_capture`, `format`, `pad`, `lowercase`, and
  `uppercase`. If a real consumer needs integer conversion, file a follow-on
  brief for that transform.

### Destination Providers

Reflow supports multiple destination types:

| Destination | Example                            | Use Case                 |
| ----------- | ---------------------------------- | ------------------------ |
| S3          | `s3://bucket/prefix/`              | Cloud-to-cloud migration |
| S3-compat   | `s3://bucket/` + `--dest-endpoint` | Wasabi, R2, MinIO        |
| Local       | `file:///tmp/output/`              | Download and reorganize  |

### Collision Handling

Control behavior when destination objects already exist:

| Option                                       | Behavior                                                                                                                            |
| -------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| `--on-collision skip-if-duplicate` (default) | Atomically write only if absent; if the destination exists, skip byte-identical duplicates and fail non-identical conflicts         |
| `--on-collision fail`                        | Atomically write only if absent; if the destination exists, mark the object failed whether it is duplicate or conflicting           |
| `--on-collision overwrite --overwrite`       | Replace the destination object unconditionally                                                                                      |
| `--on-collision overwrite-if-source-newer`   | On non-identical conflicts, replace the destination only when the source LastModified is newer, or equal-time with a different size |
| `--on-collision quarantine`                  | Atomically write only if absent; route non-identical conflicts to `--collision-quarantine-prefix` and leave the original intact     |

`--on-collision log` remains a deprecated alias for `skip-if-duplicate` for one minor-version cycle.

For S3-compatible destinations, Gonimbus probes whether `If-None-Match: *`
is semantically honored before non-overwrite collision modes run. If the
destination accepts a second IfAbsent write to the same probe key, or if the
probe is inconclusive because writes are disabled or cleanup cannot be verified,
Gonimbus routes `skip-if-duplicate`, `fail`, `quarantine`, and
`overwrite-if-source-newer` through a HEAD/compare fallback before writing. The
fallback preserves duplicate/conflict correctness and emits a once-per-run
`gonimbus.warning.v1` plus a terminal `gonimbus.reflow.summary.v1` with
`dest_ifabsent_honored`, `dest_ifabsent_probe_status`, `fallback_active`, and
`ifabsent_fallback_objects`. It does not restore cross-process create-if-absent
atomicity on destinations that do not enforce the precondition.

For `quarantine`, provide a relative collision prefix:

```bash
gonimbus transfer reflow --stdin \
  --dest 's3://dest/landing/' \
  --on-collision quarantine \
  --collision-quarantine-prefix '_conflict/'
```

Collision records include a nested `collision` object when a collision was actually observed:

```json
{
  "status": "skipped",
  "reason": "collision.duplicate",
  "collision": {
    "kind": "duplicate",
    "dest_etag_observed": "60eda685...",
    "dest_size_observed": 3729736,
    "decision_path": "ifabsent_then_head"
  }
}
```

The nested `collision` field is omitted on the no-collision happy path. Older audit logs from the GON-020 Phase A migration window may also include legacy flat collision fields; current GON-026 Phase B output uses only the nested object.

`overwrite-if-source-newer` first attempts the same atomic IfAbsent write as the default mode. If a non-identical destination already exists, Gonimbus HEADs the destination, compares source and destination `LastModified`, and overwrites only when the source is newer or when both timestamps are equal but sizes differ. The overwrite PUT is guarded with the observed destination ETag (`IfMatch`), so a destination mutation between HEAD and PUT is reported as a deterministic skip with `reason: "collision.skipped_concurrent_mutation"` rather than retried as an unconditional overwrite.

This mode uses object-store `LastModified`, which is a provider write timestamp, not a business/content timestamp. A later but smaller source object can replace an earlier larger destination object; that is intentional newest-wins mirror semantics. The `IfMatch` guard protects against concurrent mutation of the destination object that was HEADed, but it does not correct LastModified skew between buckets, accounts, or providers.

`overwrite-if-source-newer` adds collision outcomes:

| Outcome                         | Record fields                                                                                                           |
| ------------------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| Source replaces destination     | `status: "complete"`, `collision.kind: "overwritten"`, `decision_reason: "src_newer"` or `equal_time_size_differs`      |
| Destination is newer/equivalent | `status: "skipped"`, `reason: "collision.skipped_src_older"`, `collision.kind: "skipped_src_older"`                     |
| Destination changed after HEAD  | `status: "skipped"`, `reason: "collision.skipped_concurrent_mutation"`, `collision.kind: "skipped_concurrent_mutation"` |

The `collision` object also includes `src_last_modified`, `dest_last_modified_observed`, and `decision_reason` for this mode. Existing modes omit those fields.

`decision_path` values are `ifabsent_then_head`, `unconditional_overwrite`, `quarantine_routed`, `head_compare_then_conditional_overwrite`, and `head_compare_fallback`. `ifabsent_succeeded` is reserved in schemas but not emitted by the default happy path.

### Post-Reflow Verification

Use `inspect-pair` after a reflow run to verify the destination objects claimed
by the reflow audit stream:

```bash
gonimbus transfer reflow --stdin \
  --dest 's3://dest/landing/' \
  < reflow-input.jsonl > reflow-output.jsonl

gonimbus inspect-pair \
  --from-reflow reflow-output.jsonl \
  --expected-dest-prefix 's3://dest/landing/'
```

`inspect-pair` is audit-driven. It reads `gonimbus.reflow.v1` records,
ignoring other envelope records in complete reflow stdout, validates each
`dest_uri` against the operator-supplied
`--expected-dest-prefix`, and then HEADs only in-scope destination objects. The
expected destination prefix is required and repeatable. A record outside every
declared scope emits `verdict: "invalid_dest"` before any provider is
constructed or any HEAD/stat-like operation is issued.

The destination scope prevents an untrusted or mixed audit stream from choosing
arbitrary HEAD targets. Scope matching requires the same provider scheme,
bucket, and prefix root. For `file://` destinations, paths are canonicalized
under the declared root and symlink escapes are rejected before HEAD.

`inspect-pair` verifies terminal write claims:

- `status: "complete"`
- `status: "quarantined"` against the quarantine `dest_uri`
- records with `collision.kind: "overwritten"`

`status: "skipped"` and `status: "failed"` emit `verdict: "not_verified"` and
do not HEAD the destination. `status: "in_progress"` and `status: "planned"`
are ignored before scope checks, provider construction, or HEAD; they emit no
per-object record and are counted only in the summary's
`ignored_nonterminal`.

Records use the standard JSONL envelope:

```json
{
  "type": "gonimbus.inspect.pair.v1",
  "data": {
    "source_uri": "s3://source/path/object.json",
    "dest_uri": "s3://dest/landing/2026-05-30/object.json",
    "verdict": "verified",
    "source_size_bytes": 1048576,
    "dest_size_bytes": 1048576,
    "source_etag": "abc123",
    "dest_etag_observed": "abc123",
    "etag_comparable": true
  }
}
```

The stream ends with `gonimbus.inspect.pair.summary.v1`. `total` is the number
of terminal records considered, not raw input lines. `ignored_nonterminal`
counts `planned` and `in_progress` reflow records that were dropped before any
handling.

Verdicts are size-authoritative and ETag-confirming:

| Verdict                      | Meaning                                                                                                    |
| ---------------------------- | ---------------------------------------------------------------------------------------------------------- |
| `verified`                   | Destination exists, sizes match, and ETags are equal or at least one ETag is absent                        |
| `verified_size_etag_differs` | Destination exists, sizes match, and both ETags are present but differ; advisory, not an integrity failure |
| `size_mismatch`              | Destination exists but size differs; hard verification failure                                             |
| `missing`                    | Destination HEAD returned not-found                                                                        |
| `error`                      | Destination HEAD failed for another reason                                                                 |
| `invalid_dest`               | Destination URI is outside every declared expected destination prefix; no HEAD issued                      |
| `not_verified`               | Upstream record did not claim a write (`skipped` or `failed`)                                              |

`etag_comparable` is true only when both ETags are present and plain
non-multipart ETags. It is false when either ETag is absent or multipart-form.
At equal size, differing present ETags always produce
`verified_size_etag_differs`, even when one or both ETags are multipart-form.
Equal multipart ETags still produce `verified`, with `etag_comparable: false`.

The process exits non-zero when any `size_mismatch`, `missing`, `error`, or
`invalid_dest` verdict occurs. `verified_size_etag_differs` is advisory and
does not fail the process by itself, which makes the command suitable as a CI
or batch gate without treating multipart ETag differences as content
corruption.

#### Reconciling Quarantined Conflicts

When collision quarantine triggers, the destination keeps two versions: the original object at the normal key and the incoming source object under `<dest>/<collision_quarantine_prefix>/<source-key>`. A typical reconciliation pass is:

1. List the quarantine prefix with `gonimbus inspect <dest>/<collision_quarantine_prefix>/`.
2. Compare each quarantined object with the corresponding normal-key object using provider-native object metadata commands such as `aws s3api head-object`, `gcloud storage objects describe`, or `mc stat`; use `gonimbus content probe` when a content-aware comparison is useful.
3. Pick the canonical version using domain knowledge.
4. Promote or delete objects with the normal object-store CLI for that provider, such as `aws s3 cp` / `aws s3 rm`, `gcloud storage cp` / `gcloud storage rm`, or `mc cp` / `mc rm`.

### Quarantine Routing

Probe-emitted records may carry `routing_class: "quarantine"` when a required extractor could not be resolved within the probe's read budget. For these records `transfer reflow` writes to a deterministic parallel location:

```
<dest>/<quarantine_prefix>/<source-key>
```

`--rewrite-from` and `--rewrite-to` are bypassed for quarantined records — they land under the probe-configured `quarantine_prefix` with the original source key preserved. Normal records in the same input stream continue to flow through the rewrite templates, so a single reflow run can process both classes without an out-of-band step.

Use this when bulk pipelines should keep moving past anomalies. See [Reflow → Quarantine Routing](reflow.md#quarantine-routing) for the end-to-end flow, including how to configure `on_missing: quarantine` on the probe side.

### Destination Metadata

By default, `transfer reflow` preserves the historical behavior: destination objects are written without caller-controlled user metadata, without preserving source content type, and without setting a storage class.

Use explicit metadata flags when destination consumers need durable object attributes:

```bash
gonimbus transfer reflow --stdin \
  --dest 's3://dest/landing/' \
  --metadata-policy clear \
  --metadata-set dataset=transactions \
  --metadata-set owner=data-platform \
  --preserve-content-type \
  --destination-storage-class STANDARD_IA
```

User metadata policy:

| Policy     | Behavior                                                                                         |
| ---------- | ------------------------------------------------------------------------------------------------ |
| `clear`    | Do not copy source user metadata; only `--metadata-set` values are used                          |
| `preserve` | Copy source user metadata; fail the object if source keys collide after lower-case normalization |
| `merge`    | Copy source user metadata, then apply `--metadata-set` overrides                                 |

`--metadata-set key=value` is repeatable. Keys are normalized to lower case when the command starts, and the last value wins for repeated keys. Values are not redacted at the destination.

Per-object metadata derivation is also available when destination metadata must vary by source object:

```bash
gonimbus transfer reflow --stdin \
  --dest 's3://dest/landing/' \
  --metadata-policy clear \
  --metadata-set source-system=example \
  --metadata-set-from-source-key source-md5=md5 \
  --metadata-set-from-source-derived source-etag='system.etag' \
  --metadata-set-from-source-derived broker-device='urldecode(meta.payload).device' \
  --metadata-on-missing-source skip
```

`--metadata-set-from-source-key dest=src` copies one source user-metadata key into one destination user-metadata key per object. `--metadata-set-from-source-derived dest=expr` evaluates a small per-object expression against the source object's metadata and system fields. Supported v1 expressions are JSON subfield access (`meta.payload.device`), URL-decoded JSON subfield access (`urldecode(meta.payload).device`), system fields (`system.etag`, `system.last_modified`, `system.content_length`, `system.content_type`, `system.storage_class`), and one string concatenation (`system.etag + "-src"`).

`--metadata-on-missing-source` controls absent source keys, missing JSON subfields, invalid JSON, URL-decode failures, null values, and unsupported non-scalar results. The default `skip` omits only the affected destination key; `fail` emits a per-object `gonimbus.error.v1`; `empty` writes an empty string.

Per-object derivation is an explicit allow-list. Gonimbus rejects wildcard destination keys and wildcard subfield projection; it never auto-projects all source subfields. Audit each `--metadata-set-from-source-derived` expression against the actual source-side subfield inventory before running against source buckets that may contain sensitive subfields.

`--preserve-content-type` copies the source object's content type. `--destination-storage-class <class>` sets a storage class for destination PUTs; use `--destination-storage-class propagate` to copy the source storage class. Gonimbus accepts only PUT-target classes such as `STANDARD`, `INTELLIGENT_TIERING`, `STANDARD_IA`, `ONEZONE_IA`, `GLACIER_IR`, and `REDUCED_REDUNDANCY`; archive classes such as Glacier and Deep Archive are rejected as destination PUT targets.

Metadata warning: values supplied via `--metadata-set`, `--metadata-set-from-source-key`, `--metadata-set-from-source-derived`, or carried by `--metadata-policy=preserve|merge`, `--preserve-content-type`, or `--destination-storage-class` are durable destination metadata visible to callers with destination HEAD/GET permission and are not redacted at destination. Use `--metadata-policy clear` plus explicit `--metadata-set` and per-object allow-list rules when source metadata might contain credential URIs, tokens, or other sensitive values.

For local file destinations, Gonimbus stores destination metadata in a cleartext JSON sidecar next to the object. The default suffix is `.gnb-meta.json` and can be changed with `--metadata-sidecar-suffix`.

### Provenance Sidecars

`transfer reflow` can write an opt-in JSON sidecar next to each destination object:

```bash
gonimbus content probe --stdin --config probe.yaml --emit reflow-input < uris.txt |
  gonimbus transfer reflow --stdin \
    --dest 's3://dest/landing/' \
    --provenance sidecar
```

Sidecar mode writes one JSON object at `<dest-key>.gnb.json` by default. Each sidecar uses the `gonimbus.provenance.v1` schema and records the source URI, source ETag and size when known, destination URI and available metadata, run ID, tool version, rewrite routing, collision decision when present, probe-derived `vars`, the full `probe.extractors[]` audit block when the input record carries it, and the object action:

| Action              | When written                                  |
| ------------------- | --------------------------------------------- |
| `landed`            | A new destination object was written          |
| `skipped.duplicate` | Existing destination bytes matched the source |
| `quarantined`       | The object landed under the quarantine prefix |

No sidecar is written for `gonimbus.error.v1` records because there is no successful destination object to colocate with.

For production data lake landing zones where recursive listings should contain only data files, use a mirrored sidecar root:

<!-- TODO(cxotech, v0.3.4 docs pass): rewrite-semantics fix. This example renders
     {tenant}/{partition}/{subject}/{file} but uses a single-segment --rewrite-from
     '{key}' that cannot bind those vars. Replace with a matching multi-segment
     matcher (e.g. --rewrite-from '{tenant}/{partition}/{subject}/{file}') or the
     stdin dest_rel_key form. The prose below references <rendered-key>. -->

```bash
gonimbus transfer reflow --stdin \
  --dest 's3://bucket/data/landing/' \
  --rewrite-from '{key}' \
  --rewrite-to 'tenant={tenant}/partition={partition}/{subject}/{file}' \
  --provenance sidecar \
  --provenance-sidecar-root 's3://bucket/runs/run-001/sidecars/'
```

With that placement, data lands under `s3://bucket/data/landing/<rendered-key>` and sidecars land under `s3://bucket/runs/run-001/sidecars/<rendered-key>.gnb.json`. The stdout `provenance.key` remains the provider-relative sidecar object key, while `provenance.uri` carries the full sidecar URI. The sidecar root must end in `/`; it must use the same provider scheme as `--dest`, and S3 sidecar roots must use the same bucket as the destination. If the sidecar root is nested inside the destination root, or the destination root is nested inside the sidecar root, Gonimbus warns but does not reject the run.

The sidecar key suffix is configurable:

```bash
gonimbus transfer reflow --stdin \
  --dest 's3://dest/landing/' \
  --provenance sidecar \
  --provenance-suffix '.audit.json' \
  --provenance-on-write-error warn
```

The same defaults can live in the normal Gonimbus config file:

```yaml
provenance:
  mode: sidecar
  sidecar_root: "s3://bucket/runs/run-001/sidecars/"
  suffix: ".gnb.json"
  on_write_error: warn
```

Suffixes must start with a dot, must not contain `/`, and must not look like glob patterns. Gonimbus also rejects common data extensions such as `.xml`, `.json`, `.jsonl`, `.csv`, `.parquet`, `.avro`, `.txt`, `.gz`, `.zst`, `.zip`, `.tar`, `.html`, and `.pdf` unless `--allow-unsafe-suffix` is passed.

Sidecars are written after the main object. With `--provenance-on-write-error warn` (default), a sidecar write failure emits a `gonimbus.warning.v1` record and reflow continues. With `fail`, the failure emits `gonimbus.error.v1` and marks that per-object reflow as failed; the main object may already exist and can be filled in on a later run.

Operational cost is one extra PUT per landed, duplicate, or quarantined object plus storage for the sidecar objects and any later list/get activity by audit jobs. Before enabling sidecars on sustained high-volume runs, estimate:

```
expected_objects_per_run * provider_put_price * run_cadence
```

Then add storage for sidecar size times retention duration. On versioned buckets, sidecar overwrite-on-duplicate creates a new sidecar version per touch, so lifecycle rules should account for sidecar versions as well as data-object versions.

`destination.etag` is included only when reflow already has it without issuing an additional request, such as a duplicate-skip path that has already checked the existing destination object. For S3 multipart uploads, large objects may have a `<md5>-<part-count>` ETag, and it is normal for source and destination ETags to differ when provider multipart behavior differs.

### Checkpoint and Resume

For large reflow jobs, use checkpointing to enable resume after interruption:

```bash
# Start with checkpoint
gonimbus transfer reflow 's3://source/' \
  --dest 's3://dest/' \
  --rewrite-from '...' \
  --rewrite-to '...' \
  --checkpoint /tmp/reflow-state.db

# Resume after interruption
gonimbus transfer reflow 's3://source/' \
  --dest 's3://dest/' \
  --checkpoint /tmp/reflow-state.db \
  --resume
```

For resumable fatal interruptions, Gonimbus also writes an operation checkpoint
and emits a redacted `gonimbus.operation.error.v1` record with `run_id`,
`error_class`, progress counters, and a safe command:

```bash
gonimbus transfer reflow --resume-run <run_id>
```

This operation-level resume path is explicit and uses checkpointed config. It
does not require echoing source or destination arguments on the command line.
Runtime failures print a short stderr summary with the same fields and no
command help dump; argument errors still show usage.

### Dry Run

Always preview before executing:

```bash
gonimbus transfer reflow 's3://source/' \
  --dest 's3://dest/' \
  --rewrite-from '{region}/{date}/{file}' \
  --rewrite-to '{date}/{region}/{file}' \
  --dry-run
```

Dry run output shows planned mappings without writing:

```json
{
  "type": "gonimbus.reflow.v1",
  "data": {
    "source_uri": "s3://source/us-east/2024-01-15/file.xml",
    "dest_uri": "s3://dest/2024-01-15/us-east/file.xml",
    "status": "planned"
  }
}
```

## See Also

- [Content Access (streaming.md)](streaming.md) - `content probe` for derived field extraction
- [Storage Provider Configuration](../appnotes/storage-providers.md)
- [Preflight Permission Probes](../appnotes/storage-providers.md#preflight-permission-probes)
