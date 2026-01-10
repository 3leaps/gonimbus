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

Scope limiting (pathfinder-style) uses include/exclude globs applied to discovered prefixes:

```bash
# Restrict traversal to a subset of prefixes

gonimbus tree s3://my-bucket/production/ \
  --depth 4 \
  --include 'production/kickback/**' \
  --exclude '**/_temporary/**'
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
- Prefix globs in the URI (e.g. `.../*`) are not supported; use `--include/--exclude` for traversal scoping.
