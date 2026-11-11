---
title: "Segmented Snapshot Conformance Cases"
description: "Proposed detecting cases and producer mappings for immutable snapshot interoperability"
category: "standards"
status: "draft"
version: "0.0.0"
lastUpdated: "2026-09-30"
maintainer: "core-standards"
reviewers: ["architecture", "security", "data-engineering"]
approvers: ["lead-maintainer"]
tags: ["data", "snapshots", "conformance", "interoperability"]
content_license: "CC0"
relatedDocs:
  - "docs/standards/segmented-snapshot-contract.md"
  - "docs/standards/data-artifact-contract-examples.md"
audience: "implementers"
---

# Segmented Snapshot Conformance Cases

This is the evidence catalog for the [experimental contract](segmented-snapshot-contract.md).
The [schema bundle and repository controls](../../schemas/segmented-snapshot/v0/README.md)
exercise record shapes, exact byte bindings and cross-record semantics. The table
also identifies runtime obligations that repository controls do not establish.

## Detecting case catalog

Every refusal fixture needs a conforming positive control and a specific expected
reason. A parser or unrelated setup failure is not evidence that the intended
invariant was enforced.

| Case | Positive control | Required refusal or narrower claim | Evidence layer |
| --- | --- | --- | --- |
| Identity | Requested namespace/snapshot matches publication | Same id in another namespace; publication binds a different manifest | Cross-record semantics |
| Alias advance | Read retains the initially resolved snapshot | Re-resolve `latest` halfway through a read | Reader runtime |
| Schema binding | Profile/catalog bytes match committed digests | Familiar schema id resolves to different bytes | Resolution + semantics |
| Graph closure | Exhaustive list/pages and dependencies present | Missing page, duplicate id, required-edge cycle, conflicting binding | Graph semantics |
| Publication | Complete bound graph under declared profile | Marker absent, torn, or published before required members | Semantics + writer interruption |
| Byte integrity | Digest and length match the consumed artifact | Segment swapped after validation; checksum checked on different bytes | Reader runtime |
| Physical versus semantic digest | Exact emitted manifest bytes match their pin | Differently encoded but logically equivalent JSON accepted under the same physical pin | Integrity validation |
| Read scope | Selective read reports only checked members | Selected-member check reported as whole-snapshot verification | Result semantics |
| Streaming | Declared provisional or verified output mode | Partial output followed by error reported as complete success | Consumer integration |
| Counts | Representation count follows declared mapping | Physical count silently treated as complete source coverage | Semantic validation |
| Coverage subject | Attestation binds the selected immutable snapshot/grain | Attestation for a different snapshot accepted through a reused logical artifact id | Cross-record semantics |
| Capability | Full scan supported; lookup explicitly unsupported | Unsupported key lookup reported as zero rows | Consumer integration |
| Resource limit | Work within the advertised budget | Over-budget graph treated as complete or empty | Runtime capacity |
| Retention | Protected pin preserves the full dependency graph | Reader absence or alias advance alone authorizes reclamation | Concurrent/restart runtime |
| Repacking | New graph has a new exact binding | Repacked bytes substituted under an old pin | Identity + retention |
| Protection | Authorized logical references and safe metadata | Raw protected bounds or credentials exported in a manifest | Export semantics |

Storage-specific fault tests belong to the profile. A local-filesystem producer
and an object-store producer do not have the same publication mechanism. Record
native environment and failure model; cross-compilation is not runtime evidence.

The first wire bundle is flat/self-contained, so paged and external-parent graph
cases are unsupported-shape refusals rather than implemented traversal. Runtime
alias races, same-open custody, persistence, stale accelerators and reclamation
remain adopter evidence. The [semantic layer](../../schemas/segmented-snapshot/v0/semantic-validation.md)
lists the exact executed control scope and the static baseline/mutation corpus.

## Mapping A: observed object inventory

- **Grain:** current observed object state for a declared scope.
- **Representations:** immutable columnar segments; optional query-NDJSON output.
- **Identity:** store/set namespace, selected snapshot and native crawl/run id
  mapped explicitly rather than conflated.
- **Row semantics:** profile owns object key, source revision, observation time,
  duplicate handling and coverage-qualified deletion meaning.
- **Publication:** exact completed manifest/segment graph.
- **Coverage:** independent scope assessment; successful publication alone does
  not establish complete enumeration or a provider-wide point-in-time snapshot.

An existing native index may already implement many invariants. The adoption
record still identifies missing bindings and unsupported capabilities. A query
export is a separate representation with its own field catalog and verification
scope, not proof of the entire native index's conformance.

## Mapping B: retained event-history snapshot

- **Grain:** immutable events through a declared producer-owned cut.
- **Representations:** columnar scan plus an optional line-oriented export.
- **Identity:** history namespace and exact snapshot, independent of process id.
- **Row semantics:** profile owns event identity, ordering, duplicates and phases.
- **Publication:** exact selected graph; the producer protocol supplies the
  meaning and evidence for its cut.
- **Coverage:** a history cut does not establish source inventory completeness
  or that every remote side effect was acknowledged.

Compaction may repack retained events while preserving their logical content.
It creates a new physical binding and cannot invalidate a protected old pin.
This mapping does not define a live WAL, its acknowledgement watermark or replay
rules. Those are protocol work outside this companion.

## Adoption record

Use the existing [data-artifact adoption preview](data-artifact-contract-examples.md#producer-adoption-preview-template)
and add:

| Item | Required evidence |
| --- | --- |
| Pins | Exact contract/profile revision and schema digests |
| Native mapping | Every identity, manifest, publication and read-result field |
| Conformance scope | Producer, reader, exporter, verifier; supported operations |
| Limitations | Unsupported or unverified invariants with closure conditions |
| Protection | Internal versus boundary-crossing representation distinction |
| Independent witness | Consumer/validator implementation and detected negative cases |
| Runtime evidence | Native substrate, fault model, resource envelope, exact revision |

Neither this table nor a valid JSON instance proves durability. Evidence remains
scoped to the mechanism and environment actually exercised.
