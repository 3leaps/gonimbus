---
id: "DDR-0002"
title: "Segmented snapshot identity and evidence"
status: "proposed"
date: "2026-09-30"
last_updated: "2026-09-30"
deciders:
  - "@3leapsdave"
  - "entarch"
scope: "Crucible foundation / segmented-snapshot data model"
tags: ["data-contracts", "snapshots", "identity", "verification"]
relates-to:
  - "crucible ADR-0011 (segmented-snapshot boundary)"
  - "crucible ADR-0004 (coverage attestation)"
---

# DDR-0002: Segmented Snapshot Identity and Evidence

## Status

**Proposed.** Data-model decisions for the experimental v0 schema bundle. Pin the
exact schema and semantic-layer revision; this is not a stable-format freeze.

## Decision

### Keep distinct identities

| Identity | Meaning |
| --- | --- |
| Store/dataset | The namespace in which a snapshot is selected |
| Snapshot | One immutable published artifact graph within that namespace |
| Artifact | The data-artifact discovery identity bound by the publication |
| Grain | What a logical row/item means |
| Representation | One exact physical way to read the grain |
| Segment | One immutable physical member of that representation |
| Publication | A recorded completion fact binding the selected graph |

A timestamp, path, file name or row count is not an identity substitute. A
mutable alias selects a snapshot once; verification and result provenance retain
that exact binding. No later re-resolution may silently switch the read.

Manifest digest changes produce a different snapshot binding. Repacking the same
logical rows creates a new snapshot; a profile may prove semantic equivalence,
but that proof cannot substitute the new bytes under an old exact pin.

### Bind an acyclic artifact graph

A publication binds the namespace, snapshot, data-artifact descriptor and
manifest by identity and digest. The manifest binds its profile/schema artifacts
and all required segments or immutable segment-list pages. Dependencies needed
to read the graph must be explicit and transitively verifiable.

Physical identity digests cover exact emitted/stored bytes. A profile may choose
canonical serialization before emission; verification must not normalize stored
bytes into another encoding. Semantic/canonical comparison digests are separate
claims and cannot substitute for physical identity. Avoid self-digests: the
publication binds the descriptor and manifest; these do not include the digest
of their enclosing publication.

### Separate facts and judgments

- Producer lifecycle describes the producer's output.
- Publication records completion under a named substrate profile.
- Integrity verification describes which bytes/edges were actually checked.
- Coverage assessment describes the scope of observed source data.
- Retention protection records an authority-bearing reference under a separate
  mechanism; it is not implied by merely writing down a snapshot id.

Coverage subject mappings must identify the immutable snapshot, not only a
reusable logical artifact name. The v0 wire model uses an immutable subject URI
derived from namespace/snapshot identity plus exact artifact/grain matching, and
refuses ambiguous matches. It reuses coverage-attestation's existing subject
fields; no change to that contract's schema is required.

### Initial wire graph

The initial manifest selects one grain/representation and uses a flat exhaustive
member list. Row schemas are standalone; all required metadata is directly bound.
Paged manifests and required external-parent reconstruction are unsupported.
The hard ceilings are 65,536 members and 64 MiB of metadata, with lower profile
limits allowed and typed capacity refusal. These are admission limits, not a
claim about a producer's scale or memory performance.

The publication is the contract entrypoint. The profile binds its exact behavioral
specification, operation capabilities, resource limits and optional retention
specification. Verification results apply only after admission and distinguish
full versus selective member verification and provisional versus verified output.

Neither a process success event nor a publication self-claim proves all the other
facts. Verification results identify their subject, checked scope, profile and
outcome; omitted evidence is unknown, not success.

### Exact capabilities, bounded verification

Separate support for full scans, projected scans, key lookup and scoped digest
verification. A generic format label cannot establish a reader's capability.
Profiles must explain whether a streamed row is provisional or already verified,
and which terminal result establishes success for the requested scope.

Schema validation covers record shapes. Cross-record identity, count/order
consistency and exact dependency bindings need semantic checks. Persistence,
concurrency and reclamation safety additionally need runtime evidence.

## Alternatives

- Reuse one id for store, run and snapshot: simpler labels, ambiguous selection
  and mutation scope.
- Treat logical equivalence as physical identity: easier repacking, breaks exact
  digest pins and conceals changes to verified artifacts.
- Infer completion from file presence: minimal metadata, cannot distinguish a
  complete graph from leftover or incompletely published files.

## References

- [Segmented snapshot contract](../standards/segmented-snapshot-contract.md)
- [Conformance cases and adoption mappings](../standards/segmented-snapshot-conformance.md)
