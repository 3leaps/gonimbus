---
id: "ADR-0017"
title: "Durable object inventories as a segmented snapshot profile"
status: "proposed"
date: "2026-10-18"
last_updated: "2026-10-18"
deciders: ["@3leapsdave", "entarch", "cxotech"]
scope: "Crucible foundation / portable data contracts"
tags: ["index", "snapshots", "coverage", "interchange-contract"]
---

# ADR-0017: Durable Object Inventories as a Segmented Snapshot Profile

## Status

**Proposed.** Listed deciders are prospective; this record does not record their
assent. The initial design and logical controls do not qualify a producer, physical
reader or converter. An experimental publication may contain logical vectors
only; physical twins and independent reader evidence remain adoption gates.

## Decision

Define `contract: durable-index/v0` as an object-inventory specialization of
segmented-snapshot/v0. Reuse its publication, selection, byte bindings, profile,
row schema and field catalog. Add one optional required-dependency slot,
`profile_metadata`, to the snapshot manifest. The index specialization requires
that slot and defines its plan, coverage, lineage and admission semantics.
Existing manifests without it retain their semantics.

The published state is self-contained. State-parent bindings permit transition
verification without turning ancestry into a row-reconstruction dependency.
Observation plans are immutable and independently identified from configuration.
Only confirmed-complete, gap-free, correctly bound observation scopes authorize
absence-derived tombstones. Explicit, exact-parent acknowledgments can admit
reduced observation scope; they cannot authorize removal or fabricate freshness.

Compose coverage-attestation unchanged, with one single-claim attestation per
scope. Keep schema/catalog protection under data-artifact. Durable-record and
durable-queue are independent families; writer journals and coordination engines
are outside this contract. Analytics representations can derive from exact pins.

The first physical profile is Parquet with a fixed logical inventory schema.
Packing, spill/merge, storage layout, discovery selectors, leases, fencing,
recovery machinery and reclamation remain adopter responsibilities. Their
qualification gates are distinct from artifact and transition verification.

Legacy adoption defaults to a new explicit baseline from fresh observation,
with preserved legacy bytes and an exact cutover/rollback record. A converter
requires its own evidence and must refuse unavailable facts. An opaque set token
is carried through schema, producer and format changes. A deliberately changed
token requires a cutover record and a new baseline; it is not a same-set parent
transition.

## Alternatives considered

- A separate index publication/manifest wrapper adds a second graph root.
- Product-specific manifests do not supply neutral successor semantics.
- A universal journal/storage engine exceeds the snapshot contract boundary.
- Treating reduced scope as removal confuses non-observation with absence.
- Rewriting old publications invalidates exact pins and cannot establish missing
  provenance retroactively.

## Compatibility and evidence

The optional manifest edge expands a proposed v0 schema. Consumers pin a release
and schema bytes; an older closed-schema reader may refuse the new field. That
is an explicit compatibility boundary, not a reason to ignore it. Required
unknown profile metadata always refuses. The family remains proposed and experimental through adoption. Logical-only
publication and later physical/independent-reader qualification have distinct
[conformance gates](../standards/durable-index-conformance.md).
Composition does not silently ratify its proposed dependencies.

## References

- [Durable index standard](../standards/durable-index-contract.md)
- [Row and plan encoding](DDR-0004-durable-index-row-and-plan-encoding.md)
- [Segmented snapshot](ADR-0011-segmented-snapshot-contract.md)
- [Coverage attestation](ADR-0004-coverage-attestation-contract.md)
- [Shipping charter](PDR-0006-shipping-charter.md)
