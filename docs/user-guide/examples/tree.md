# Tree (Prefix Summary)

The `gonimbus tree` command gives a safe, directory-like summary of an object-store prefix.

Wave 1 is **direct-only** (non-recursive): it does not traverse into child prefixes.

Wave 2 enables **depth-limited traversal** via `--depth N`.

## Quick Start

```bash
# Safety latch (recommended when dogfooding)
export GONIMBUS_READONLY=1

# Direct-only prefix summary

gonimbus tree s3://my-bucket/data/2026/
```

## S3-compatible endpoints

```bash
gonimbus tree s3://my-bucket/data/ \
  --endpoint https://s3.us-east-2.wasabisys.com \
  --region us-east-2
```

## Limits (recommended)

If a prefix has a huge number of direct objects, delimiter listing can still page for a long time.

Use hard limits to bound work:

```bash
gonimbus tree s3://my-bucket/data/ \
  --max-objects 100000 \
  --max-pages 500
```

## Depth traversal (Wave 2)

Depth traversal is still read-only (delimiter listing only), but it can visit many prefixes.
Use the safety limits to keep traversals bounded.

```bash
# Traverse two levels under the starting prefix
# Defaults: --max-prefixes 50000 --timeout 10m

gonimbus tree s3://my-bucket/production/ --depth 2 --output table
```

Scope limiting (pathfinder-style) uses include/exclude globs applied to discovered prefixes.

These are traversal-scope controls, not a general search mechanism. Patterns that require
look-ahead (e.g. `**/needle/**`) may not reduce traversal work without an index.

```bash
# Restrict traversal to a subset of prefixes

gonimbus tree s3://my-bucket/production/ \
  --depth 4 \
  --include 'production/kickback/**' \
  --exclude '**/_temporary/**'
```

## Partial results (timeouts and limits)

For long traversals, prefer JSONL output so you can stream results to a file and still get a well-formed terminal record set.

When a traversal hits a safety bound like `--timeout` or `--max-prefixes`, Gonimbus:

- streams whatever `gonimbus.prefix.v1` records were computed before stopping
- emits a final `gonimbus.error.v1` indicating the run was partial
- emits a final `gonimbus.summary.v1` with `errors=1`

Example (timeout):

```bash
export GONIMBUS_READONLY=1

# Capture streamed results for later analysis

gonimbus tree s3://my-bucket/production/kickback/ \
  --depth 3 \
  --timeout 30s \
  --output jsonl > tree-partial.jsonl
```

## Output formats

```bash
# Default JSONL output

gonimbus tree s3://my-bucket/data/ --output jsonl

# Human table output

gonimbus tree s3://my-bucket/data/ --output table
```

## Notes

- `tree` requires a prefix URI (append `/`).
- Prefix globs in the URI (e.g. `.../*`) are not supported.
- `--include/--exclude` are traversal-scope controls (matching discovered prefixes), not a wildcard search across the whole bucket.
