# Atlas Artifacts

An atlas is an opt-in companion artifact for a completed index run. It reads
the indexed objects as a post-pass, computes a SHA256 content hash for each
object, extracts typed dimensions from the payload, and writes a local
content-addressed artifact.

Phase A is intentionally local and narrow. It does not export atlas artifacts
to hubs, create lower-trust redacted exports, run drift queries, or drive
transfer reflow decisions.

## Build an Atlas

Start from an existing index set and completed run:

```bash
gonimbus atlas build \
  --from-index idx_1234abcd \
  --run run_1234567890 \
  --recipe atlas-recipe.yaml \
  --output /tmp/gonimbus-atlas/run-1234567890
```

The output directory must be empty or absent. Build writes:

| Path                | Purpose                                                                                        |
| ------------------- | ---------------------------------------------------------------------------------------------- |
| `atlas.json`        | Header with source run, digest, coverage, dimensions, system-field classifications, and counts |
| `shards/*.jsonl`    | Per-shard `gonimbus.atlas.object.v1` rows                                                      |
| `diagnostics.jsonl` | Row-level read, extraction, and validation diagnostics                                         |

The build is a post-pass. It does not re-list the bucket. Phase A requires the
selected run to be the latest local index run because local indexes store the
current object table rather than historical object snapshots for every prior
run.

## Recipe

Phase A recipes use the existing content probe extractors. The dimension kind
enum is closed for `gonimbus.atlas.v1`: `temporal-day`,
`temporal-instant`, and `categorical`.

```yaml
version: "1.0"
hash: sha256
coverage: scoped

dimensions:
  - name: event_date
    kind: temporal-day
    classification: 1-confidential
    extractor:
      type: json_path
      json_path: $.event_date

  - name: route_key
    kind: categorical
    classification: 3-proprietary
    extractor:
      type: regex
      pattern: "route=([A-Za-z0-9_-]+)"
      group: 1

shard_by: [event_date]
```

Phase A supports exactly one `shard_by` dimension. Composite sharding is a
future expansion point.

System-field classifications may be raised but not lowered. For example,
`content_hash` defaults to `3-proprietary`; a recipe may raise it to
`4-personal`, but cannot mark it `0-public`.

## Diagnostics

Object-level failures do not silently disappear. Atlas records diagnostics for
objects that cannot be read, parsed, extracted, or validated. The header counts
successful rows and diagnostics separately so operators can decide whether a
partial atlas is useful for the task at hand.

Whole-build failures are reserved for invalid recipes, missing or incompatible
index runs, provider setup failure, and artifact write failure.

## Stats

Use `atlas stats` to verify an artifact and inspect dedup counts:

```bash
gonimbus atlas stats /tmp/gonimbus-atlas/run-1234567890
gonimbus atlas stats /tmp/gonimbus-atlas/run-1234567890 --json
```

The stats command reports:

- Tier 1: observed storage-key rows
- Tier 2: distinct content hashes
- Tier 3: distinct content hashes per shard
- Diagnostic count

## Sensitivity

Atlas content is sensitive by default. Raw content hashes can be used for
membership checks, and storage keys or extracted dimensions can reveal source
structure. Phase A records classifications in `atlas.json`, but it does not
implement lower-trust export transforms. Do not treat a local atlas artifact as
redacted or shareable outside the source trust boundary.
