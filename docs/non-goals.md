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

Purpose-built UX layers, orchestration systems, or client-specific mount/sync products.

## Allowed Adjacent Functionality

These are permitted because they directly support inspection/crawl:

- Fast listing strategies (prefix-first, sharding, inventories later)
- Object metadata enrichment (HEAD, tags) where it improves crawl usefulness
- Structured outputs (JSONL) and optional indexing sinks
- Job manifests and validation (schemas-first)
- Server mode for long-running crawls and streaming results

The scopes stay clear: gonimbus does not implement mount/sync/FUSE/desktop concerns.
