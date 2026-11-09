# Canonical-State Implementation and Evidence Scope

[ADR-0007](adr/ADR-0007-canonical-index-set-authority-and-recoverable-artifact-transactions.md)
is the accepted architecture. This page separates that decision from claims
about implementation and verification. Acceptance is not a blanket certification
of every operation, platform or failure mode.

## Implementation map

These are inspection/evidence locations, not pass results. A release review must
bind its result to an exact revision and the commands actually executed.

| Obligation                                     | Implementation / evidence location                                                     | Qualification                                                                                                                  |
| ---------------------------------------------- | -------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| Whole-set authority and lifetime-held identity | `internal/indexsubstrate/set_authority*.go`, `pkg/indexcoord`                          | Evaluate the exact public entry point and adapter, not only the primitive                                                      |
| Durable publication and exact parent binding   | `internal/indexsubstrate/publish*.go`, `lineage*.go`                                   | Publication does not establish provider snapshot completeness                                                                  |
| Canonical SQLite transaction custody           | `pkg/indexstore`, `pkg/indexreader/sqlite_write_target.go`                             | Canonical database and transaction-sidecar identity must both hold                                                             |
| Same-open durable reads                        | `pkg/indexreader/durable.go`, `internal/indexsubstrate/parent_reader*.go`              | Pinned and latest-resolved paths require their own identity checks                                                             |
| Discovery-to-open identity binding             | `pkg/indexreader/resolve.go`, `pkg/indexreader/durable.go`                             | Check the finally opened snapshot against the caller's admitted set identity; internal chain consistency alone is insufficient |
| Streaming capacity refusal                     | `internal/indexsubstrate/spillmerge*.go`, `segment_stream*.go`                         | Deterministic budgets and process resource measurements are different evidence                                                 |
| Authority observation/reclaim                  | `internal/indexsubstrate/set_authority_probe.go`, `set_authority_reclaim.go` and tests | No process/mtime inference may replace the OS-lock verdict                                                                     |
| CLI/library equivalence                        | Direct library tests plus command-path tests                                           | Shared code alone does not prove adapter error mapping                                                                         |

## Release review record

For each affected obligation, record:

- Source revision and supported operation/format.
- Test or inspection reference and exact exercised scenario.
- Native OS/filesystem and runtime result; distinguish cross-compilation.
- Detecting negative control where an enforcement claim depends on a gate.
- Result: verified, unsupported, not exercised, or open defect.
- The user-visible limitation and closure condition for an unresolved row.

Do not turn a skipped native test into a cross-platform pass. Do not infer all
SQLite/durable operations from one format's success. A process-kill test alone
does not establish behavior under storage-device power loss.

## Current evidence status

Decision status alone supplies no runtime verification result. The required-evidence
section of ADR-0007 remains normative. Detailed tests and release evidence
establish the supported implementation scope; open rows must remain visible
during release review.

Future receipt stores and other state engines must define their own adoption
mapping. Their use of familiar locks, manifests or Parquet does not itself prove
conformance to the index authority contract.
