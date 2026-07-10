# Library API Stability

Gonimbus is pre-v1.0 and remains free to evolve, but some packages are now
supported for embedded Go use. This document is the public contract for that
surface.

The stability tier is assigned by Go import path. Subpackages are classified
independently, so a package can be Stable even when a sibling under the same
parent remains Experimental. Everything under `internal/` is private to
gonimbus and is protected by the Go compiler.

## Stability Tiers

| Tier         | Meaning                                                                                   |
| ------------ | ----------------------------------------------------------------------------------------- |
| Stable       | Supported library API. Breaking changes require the notification protocol below.          |
| Experimental | Importable, but may change with only an in-release note. No advance-notice guarantee.     |
| Internal     | Private implementation packages under `internal/`. Not importable by external Go modules. |

## Stability Manifest

| Import path                                    | Tier         | Notes                                                                                                                  |
| ---------------------------------------------- | ------------ | ---------------------------------------------------------------------------------------------------------------------- |
| `github.com/3leaps/gonimbus/pkg/atlas`         | Experimental | Atlas construction and read substrate.                                                                                 |
| `github.com/3leaps/gonimbus/pkg/content`       | Experimental | Content metadata helpers.                                                                                              |
| `github.com/3leaps/gonimbus/pkg/crawler`       | Experimental | Crawl engine internals for CLI workflows.                                                                              |
| `github.com/3leaps/gonimbus/pkg/indexstore`    | Experimental | Index storage substrate.                                                                                               |
| `github.com/3leaps/gonimbus/pkg/indexreader`   | Experimental | Format-aware local index read seam (`sqlite-v1` / `durable-v2`) for query and follow-on consume ops.                   |
| `github.com/3leaps/gonimbus/pkg/indexbuild`    | Experimental | Durable index build workflow engine (v0.4.0 default CLI path) over injected providers and journal/segment publication. |
| `github.com/3leaps/gonimbus/pkg/jobregistry`   | Experimental | Local job-control substrate.                                                                                           |
| `github.com/3leaps/gonimbus/pkg/manifest`      | Experimental | Manifest parsing and validation substrate.                                                                             |
| `github.com/3leaps/gonimbus/pkg/match`         | Stable       | Object-key pattern matching used by embedded consumers.                                                                |
| `github.com/3leaps/gonimbus/pkg/output`        | Experimental | CLI JSONL output helpers.                                                                                              |
| `github.com/3leaps/gonimbus/pkg/opcheckpoint`  | Experimental | Sensitive local operation-checkpoint substrate.                                                                        |
| `github.com/3leaps/gonimbus/pkg/preflight`     | Experimental | Operator preflight checks.                                                                                             |
| `github.com/3leaps/gonimbus/pkg/probe`         | Experimental | Content probe recipe substrate.                                                                                        |
| `github.com/3leaps/gonimbus/pkg/provider`      | Stable       | Provider interface, metadata types, and capability interfaces.                                                         |
| `github.com/3leaps/gonimbus/pkg/provider/file` | Stable       | Local filesystem provider construction with `Config` and `New`.                                                        |
| `github.com/3leaps/gonimbus/pkg/provider/gcs`  | Experimental | GCS provider contract surface while provider support matures.                                                          |
| `github.com/3leaps/gonimbus/pkg/provider/s3`   | Stable       | AWS S3 and S3-compatible provider construction with `Config` and `New`.                                                |
| `github.com/3leaps/gonimbus/pkg/reflow`        | Experimental | Reflow workflow engine for the migrated stdin subset, records, adaptive concurrency, and redaction helpers.            |
| `github.com/3leaps/gonimbus/pkg/reflowstate`   | Experimental | Reflow checkpoint state substrate.                                                                                     |
| `github.com/3leaps/gonimbus/pkg/scope`         | Experimental | Scope compilation substrate.                                                                                           |
| `github.com/3leaps/gonimbus/pkg/shard`         | Experimental | Prefix shard discovery substrate.                                                                                      |
| `github.com/3leaps/gonimbus/pkg/stream`        | Experimental | Language-neutral stream framing substrate.                                                                             |
| `github.com/3leaps/gonimbus/pkg/transfer`      | Experimental | Transfer and reflow implementation substrate.                                                                          |
| `github.com/3leaps/gonimbus/pkg/uri`           | Stable       | Object URI parsing for `s3://` and supported `file://` forms.                                                          |

## Breakage Notification Protocol

A breaking change to a Stable package includes removed or renamed exported
symbols, changed exported signatures, or changed documented behavior. Stable
breaks are allowed during the 0.x line, but they must follow this protocol:

1. Add a `Library API` entry to `CHANGELOG.md` describing the break and the
   migration path.
2. Notify known embedders at least one release cycle before the release that
   ships the break.
3. Ship a migration note that describes the before and after shape.
4. Ship the break only in a new release tag. A published tag must never be
   rewritten or republished with a Stable break, so pinned consumers do not see
   retroactive changes.

Experimental-package breaks should be mentioned in the release notes when they
matter to operators or embedders, but they do not carry the Stable advance
notice guarantee.

## Notification Registry

Maintainers keep a private registry of known embedders and notify them through
the maintainers' downstream-consumer coordination channel when the Stable
protocol requires advance notice. The named registry and channel are internal
coordination data and are not stored in this public repository.

## Pre-v1.0 Posture

The 0.x line uses notify-and-sync rather than a hard compatibility freeze.
Stable package breaks may still happen when the project needs them, provided
the protocol above is followed. Revisit a longer-support tier or a v1.0 freeze
when the embedder set grows or a consumer exits alpha and needs a stricter
compatibility window.

## Enforcement

`make api-stability` runs two checks:

- exported-symbol diff for Stable packages against the latest release tag. If
  a Stable API diff exists, the check fails unless the current `Unreleased`
  changelog, or the section matching the current `VERSION` during release prep,
  has a `Library API` entry acknowledging it.
- manifest coverage for every package under `pkg/`, including a guard that only
  the documented Stable packages are covered by the diff gate.

See `docs/development/api-stability.md` for implementation details.
