# Release Notes

This file contains release notes for up to the three most recent releases in reverse chronological order. For the complete release history, see the [CHANGELOG](CHANGELOG.md) or the [docs/releases/](docs/releases/) directory.

---

## [0.4.6] - 2026-11-08

**Advisory control-record marker, faster durable prefix queries, look-alike flag hints, and release provenance hardening**

v0.4.6 adds a forward-compatible `"advisory": true` envelope marker for future
control records in `transfer reflow` pipelines, prunes trailing segments during
durable `index query --pattern` prefix scans, appends actionable hints for
look-alike unknown CLI flags, and tightens release provenance and dependency
hygiene.

- **Advisory control-record envelope marker.** `transfer reflow` skips
  unrecognized future control records marked with top-level `"advisory": true`
  (outside recognized work, integrity, and error record families) without
  incrementing `invalid_inputs`, emits at most one `gonimbus.warning.v1`
  (`REFLOW_ADVISORY_RECORD_SKIPPED`) per distinct type (up to 16 types, with a
  single `REFLOW_ADVISORY_WARNING_CAP_REACHED` warning record if exceeded), and
  continues without aborting the batch. `content probe` stamps `"advisory":
true` on `gonimbus.content.probe.summary.v1`.
- **Durable prefix segment pruning.** `index query --pattern <glob>` stops
  opening and hashing segments once a segment's `MinRelKey` sorts strictly
  after the pattern's literal prefix (while still reading segments with
  inverted bounds).
- **Look-alike CLI flag hints.** Unknown `--profile`/`-p`, `--json` and
  `--count` flags on subcommands with different flag names or invocation shapes
  append a targeted hint before the usage block, and `inspect` documents its
  default 100-object `--limit` cap.
- **Release provenance and immutable publication.** Release publication stages
  tagged public verification keys as release assets, verifies Decernor's
  SHA-256 before execution, uses an isolated `GNUPGHOME` for tag and anchor
  verification, and verifies GitHub release immutability on publish.
- **Security and dependency updates.** Updates `github.com/andybalholm/brotli`
  to v1.2.7 (`GO-2026-6888`), `github.com/klauspost/compress` to v1.20.2
  (`GO-2026-6865`), `github.com/go-chi/chi/v5` to v5.4.0,
  `github.com/go-jose/go-jose/v4` to v4.1.6, `golang.org/x/net` to v0.62.0,
  `golang.org/x/crypto` to `v0.58.1-0.20261009181718-2f1f834330b2` (cooled
  2026-10-09 commit), `golang.org/x/sys` to v0.49.0, `golang.org/x/sync` to
  v0.24.0, and `golang.org/x/text` to v0.43.0, and canonicalizes top-level
  `LICENSE` and `NOTICE`.

### Upgrade

```bash
go install github.com/3leaps/gonimbus/cmd/gonimbus@v0.4.6
```

See [docs/releases/v0.4.6.md](docs/releases/v0.4.6.md) for the complete
release notes.

---

## [0.4.5] - 2026-10-21

**Go 1.26.10 minimum, quieter stderr, single-source release tag**

v0.4.5 raises the minimum Go toolchain, keeps stderr for real problems, and
tightens how the release tooling selects a tag.

- **Go 1.26.10 minimum.** gonimbus builds with Go toolchain `go1.26.10`, to
  ensure secure operation.
- **Quieter stderr.** Commands against AWS S3 no longer print an SDK checksum
  debug line, and runtime failures no longer append the usage block. Usage
  still prints for invocation errors.
- **Release tag from one variable.** Release tooling reads the tag only from
  `GONIMBUS_RELEASE_TAG` and checks it against `VERSION` before any
  tag-consuming step.
- **Docs.** What `dest_etag` holds per destination provider, and the shape of
  the `index build --json` receipt.

No commands, flags, record schemas or exit codes change.

### Upgrade

```bash
go install github.com/3leaps/gonimbus/cmd/gonimbus@v0.4.5
```

See [docs/releases/v0.4.5.md](docs/releases/v0.4.5.md) for the complete
release notes.

---

## [0.4.4] - 2026-10-20

**Write receipts, probe outcomes, and listing integrity**

v0.4.4 makes copy and probe pipelines report what each write acknowledged and
how each probe ended, and makes index builds refuse a listing that ended early.

- **Terminal write receipts.** Terminal reflow records can carry the
  destination's acknowledgement of each write: payload SHA-256, ETag, revision,
  and known source last-modified. These are transfer receipts, not independent
  read-back verification.
- **Probe summary and exit codes.** `content probe` ends with one
  `gonimbus.content.probe.summary.v1` record. A completed run with object data
  errors exits **60**, previously **32**; provider failures remain 32.
- **Listing integrity.** A page that reports more results without a usable
  continuation token fails the listing, so an index build fails before
  sealing instead of attesting a partial listing.
- **Durable include refusals** name the alternative
  (`index migrate-match-scope`, `build.scope`, or `--format sqlite`), with a
  worked `date_partitions` example for a wildcard in a middle path segment.
- **Release provenance.** A read-only tag workflow builds the release; a
  signed maintainer ceremony publishes it.

### Upgrade

```bash
go install github.com/3leaps/gonimbus/cmd/gonimbus@v0.4.4
```

Scripts that match exit code 32 for a completed probe with object data errors
must handle 60. Upgrade probe producers and reflow consumers together, or filter
the summary record for an older consumer.

See [docs/releases/v0.4.4.md](docs/releases/v0.4.4.md) for the complete
release notes.
