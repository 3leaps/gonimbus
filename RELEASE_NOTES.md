# Release Notes

This file contains release notes for up to the three most recent releases in reverse chronological order. For the complete release history, see the [CHANGELOG](CHANGELOG.md) or the [docs/releases/](docs/releases/) directory.

---

## v0.3.7 (2026-07-05)

**Operational Data Root Overrides**

v0.3.7 gives deployments a single supported data-root control for local
operational state. `GONIMBUS_DATA_DIR` sets a one-process root, `data_root` sets
the same root in config, and Gonimbus now routes index databases, index-build job
records, server job defaults, and operation checkpoints through the shared
resolver.

`GONIMBUS_DATA_ROOT` and `data_dir` remain supported aliases, but new automation
should prefer `GONIMBUS_DATA_DIR` and `data_root`.

### Relocating operational state

Use an environment override for a single process:

```bash
GONIMBUS_DATA_DIR=/mnt/gonimbus-data gonimbus index build --job index.yaml
```

Use config for a persistent relocation:

```yaml
data_root: /mnt/gonimbus-data
```

`gonimbus doctor` now reports the resolved data root, the source that selected
it, and whether Gonimbus can write or create it.

### Guardrails

Resolved data roots inside a git working tree are rejected before state is
created, regardless of source, including symlink-resolved paths. App-data
directories and runtime state files are created with owner-only permissions
where Gonimbus controls them.

Changing the root does not migrate existing state. Move existing indexes, job
records, and checkpoints deliberately, then verify the new root with
`gonimbus doctor` before resuming production jobs.

### Upgrade

```bash
go install github.com/3leaps/gonimbus/cmd/gonimbus@v0.3.7
```

Existing command flags, manifest formats, and default platform app-data behavior
remain compatible with v0.3.6.

See [docs/releases/v0.3.7.md](docs/releases/v0.3.7.md) for the complete release
notes.

---

## v0.3.6 (2026-07-04)

**Incremental Index Top-Ups and Query-Time Deltas**

v0.3.6 makes recurring index operations cheaper and easier to feed into
downstream workflows. `index build --since <timestamp>|auto` can narrow
date-partitioned listing plans before provider LIST calls, and it reports
whether that reduction actually applied. `index query --since-run <run_id>`
then emits the current objects first seen or meaningfully changed after a
successful run, giving consumers a forward delta without rebuilding or
relisting the source.

### Incremental top-ups with `--since`

Use `--since <timestamp>` when a recurring build has a known lower bound, or
`--since auto` to derive the watermark from the latest successful run in the
same IndexSet.

On date-partitioned scopes, Gonimbus narrows the listing plan before provider
enumeration. On layouts that cannot be narrowed safely, the run falls back to
full enumeration with a last-modified ingest filter and reports that reduction
was not applied. That signal is intentional: operators should verify the
since-plan output instead of assuming every since run was cheaper.

Since builds are not full-coverage audits. They skip soft-delete and can leave
deleted objects visible until a periodic full audit build runs.

### Forward deltas with `--since-run`

`index query --since-run <run_id>` emits current active rows first seen or
changed after a successful run in the same IndexSet. Output keeps the existing
`gonimbus.index.object.v1` record type and adds optional delta fields such as
`change_kind`, `first_seen_run_id`, and `last_changed_run_id`.

This is latest-state delta query, not point-in-time history. The current index
does not ship `--at-run`, and `--include-deleted --since-run` is rejected
instead of implying deletion history.

### Compatibility

Index schema v8 adds first-seen and last-changed run metadata. Older indexes
migrate forward automatically, but `--since-run` rejects boundary runs before
the migration baseline because precise added/changed classification starts at
that point.

Index database timestamp storage is now normalized to a fixed-width UTC text
form preserved by both the default SQLite driver and optional
`gonimbus_libsql` builds. CLI and JSON output timestamps remain unchanged, but
direct `index.db` queriers may see internal timestamp fields stored with a
`+0000 UTC` suffix instead of `Z`. Existing RFC3339/RFC3339Nano stored values
remain readable and migrate automatically.

### Upgrade

```bash
go install github.com/3leaps/gonimbus/cmd/gonimbus@v0.3.6
```

Existing full-build and normal query workflows remain compatible with v0.3.5.
Add `--since auto` only after confirming the manifest identity is stable for
the shard you intend to top up, and keep periodic full-coverage audits when
deletion detection matters.

See [docs/releases/v0.3.6.md](docs/releases/v0.3.6.md) for the complete release
notes.

---

## v0.3.5 (2026-07-02)

**Multipart Export/Reflow and the Migrated Reflow Engine**

v0.3.5 removes the practical single-PUT ceiling from the release's largest
operator workflows. `index export` can publish a large `index.db` to an
S3-compatible hub through multipart upload, and `transfer reflow` can copy large
objects through the same shared upload primitive. The release also moves the
migrated stdin reflow subset onto the Experimental `pkg/reflow` engine and
tightens two error paths that matter during long runs: content-probe terminal
errors keep their original code, and throttled reflow probes retry before falling
back to per-object failure handling.

### Multipart for large exports and reflow writes

Large writes now use a shared multipart primitive in `pkg/transfer`. Once a
known-size write crosses the default 64 MiB multipart threshold, Gonimbus uploads
bounded parts, completes conditionally when the destination supports IfAbsent,
and aborts the multipart upload on every failure path it controls.

The immediate operator effect is simple: a large hub export or reflow write no
longer fails only because the destination enforces a >5 GiB single-PUT limit.
Multipart-form ETags are still treated carefully; Gonimbus does not use them as
blind byte-equality proof for collision decisions.

### Reflow engine migration (Experimental)

The stdin `transfer reflow` subset now routes through the Experimental
`pkg/reflow` engine for metadata planning, dry-run planning, record-stream copy
execution, collision decisions, adaptive concurrency, and typed run / summary
records. CLI behavior is intended to remain compatible; unsupported forms keep
using the legacy path until later migration work.

`pkg/reflow` remains **Experimental**. There are no Stable library API breaks in
this release.

### Error-path hardening

- `content probe` now recognizes `gonimbus.error.v1` input records as terminal
  records, preserves their original error code, and avoids wrapping retryable
  records as `INTERNAL`.
- Content-probe error output now uses the same provider-error sanitizer as
  reflow and no longer carries raw input JSONL lines forward in parser
  diagnostics.
- Reflow HEAD/body-compare probe operations now retry throttled provider
  attempts through the adaptive limiter. If throttling never clears, the
  existing per-object failure behavior remains in place; the checkpoint/resume
  classifier boundary is unchanged.

### Upgrade

```bash
go install github.com/3leaps/gonimbus/cmd/gonimbus@v0.3.5
```

Existing CLI workflows remain compatible with v0.3.4. Operators planning large
multipart writes should verify local disk headroom for the index database,
checkpoint files, and any retry/temp spooling, and configure provider lifecycle
cleanup for incomplete multipart uploads, such as S3
`AbortIncompleteMultipartUpload` lifecycle rules.

See [docs/releases/v0.3.5.md](docs/releases/v0.3.5.md) for the complete release
notes.

---

For v0.3.4 and earlier release notes, see [docs/releases/](docs/releases/) or
the [CHANGELOG](CHANGELOG.md).

<!-- v0.3.0 entry removed when v0.3.3 rolled into the 3-most-recent window; full notes preserved at docs/releases/v0.3.0.md -->
<!-- v0.2.3 entry removed when v0.3.2 rolled into the 3-most-recent window; full notes preserved at docs/releases/v0.2.3.md -->
<!-- v0.2.2 entry removed when v0.3.1 rolled into the 3-most-recent window; full notes preserved at docs/releases/v0.2.2.md -->
<!-- v0.2.1 entry removed when v0.3.0 rolled into the 3-most-recent window; full notes preserved at docs/releases/v0.2.1.md -->
<!-- v0.3.1 entry removed when v0.3.4 rolled into the 3-most-recent window; full notes preserved at docs/releases/v0.3.1.md -->
<!-- v0.3.2 entry removed when v0.3.5 rolled into the 3-most-recent window; full notes preserved at docs/releases/v0.3.2.md -->
<!-- v0.3.3 entry removed when v0.3.6 rolled into the 3-most-recent window; full notes preserved at docs/releases/v0.3.3.md -->
<!-- v0.3.4 entry removed when v0.3.7 rolled into the 3-most-recent window; full notes preserved at docs/releases/v0.3.4.md -->
