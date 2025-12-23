# Gonimbus Non-Goals

Gonimbus exists to **inspect, crawl, and inventory** cloud object storage at enterprise scale.

Anything that turns it into a filesystem client, sync tool, or desktop product is out of scope.

## Explicit Non-Goals

- FUSE mounts / "mount as a drive"
- Offline-first queues, conflict resolution, sync engines
- Caching policies (hot/warm/cold), pin/unpin UX
- "Integrated mode" desktop semantics
- Watching local folders and mirroring to buckets
- GUI apps, tray icons, Finder/Explorer extensions
- "Be rclone but simpler" feature parity

## Where Those Concerns Belong

- **NimbusNest** (3leaps ecosystem): the Mountain Duck-like app for Linux/macOS
- **Other apps/tools**: purpose-built UX layers, orchestration systems, or client-specific products

## Allowed Adjacent Functionality

These are permitted because they directly support inspection/crawl:

- Fast listing strategies (prefix-first, sharding, inventories later)
- Object metadata enrichment (HEAD, tags) where it improves crawl usefulness
- Structured outputs (JSONL) and optional indexing sinks
- Job manifests and validation (schemas-first)
- Server mode for long-running crawls and streaming results

## Relationship to NimbusNest

NimbusNest can consume gonimbus backends/matcher/crawl outputs. The integration points are:

- **Backends**: S3/GCS backends expose an interface NimbusNest can reuse
- **Matcher**: Doublestar/prefix derivation is reusable for pinning/path filters
- **Outputs**: JSONL/streaming results could feed a NimbusNest indexer
- **Server**: Remote runner could act as a discovery service NimbusNest queries

The scopes stay separate: gonimbus does not implement mount/sync/FUSE/desktop concerns.
