# DDR-0001: Index Semantics and Representation Parity

**Status:** Proposed
**Date:** 2026-09-30
**Decision Makers:** @3leapsdave

## Context

SQLite and durable-v2 represent object-index data through different physical
structures. The existing comparison projection validates a defined subset of
their outputs. A green comparison is useful only when its claim is explicit.

## Decision

Separate four kinds of compatibility:

1. **Logical semantics:** equivalent supported operations over equivalent admitted
   inputs produce equivalent results under a versioned comparison definition.
2. **Safety obligations:** identity, custody, incomplete-state refusal and
   recovery requirements apply to both representations.
3. **Capability coverage:** an operation may require representation-specific
   inputs or be unsupported. Document and refuse unsupported combinations.
4. **Physical behavior:** storage size, access strategy, file count, latency and
   maintenance need not be equal. They are measured independently.

No requirement to materialize both formats follows from logical parity. The
`both` build format remains explicit verification: durable publication plus a
run-scoped SQLite projection. That projection is not the canonical durable store.

### Preserve the existing comparison contract

[Compare projection v1](../index-compare-projection-v1.md) remains unchanged:

- Active LIST-derived rows, ordered by relative key.
- Digest fields: key, size, last-modified and storage class.
- A separate same-provider ETag equivalence check.
- No certification of history, HEAD enrichment, coverage attestations, physical
  metadata or readiness for a downstream reflow operation.

Its existing null/empty and timestamp normalization is part of that projection,
not a universal canonicalization rule. A receipt or another grain may require
those distinctions and must define its own comparison.

Stronger comparisons require a new version/profile, explicit field and
normalization rules, and positive and negative fixtures. Do not broaden an old
`parity_passed` claim without a contract boundary.

### Reference evidence

SQLite is a useful comparison implementation for common index semantics, not
the universal oracle for the durable architecture. Validate new durable behavior
against independent reference models and invariants: exact row sets, identity,
coverage, phase transitions, publication, recovery and resource bounds.

Keep semantic, storage-integrity and performance results distinct. Equal counts
alone do not prove equal rows. Equal digests over a limited projection do not
prove omitted fields. Schema validation does not prove crash durability.

### Domain boundaries

An observed inventory records storage observations. An acknowledged-write receipt
records a controlled operation. A derived query projection records an access
view. They are different logical grains even when each is represented in Parquet.
A destination inventory cannot reconstruct historical source-to-write evidence.

Portable descriptors should expose these grains and capabilities without
renaming native row contracts in place. Any upstream contract adoption requires
an explicit profile/version mapping and conformance evidence; matching terminology
does not establish adoption.

## Consequences

Compatibility remains supported without forcing physical symmetry. Documentation
must identify the comparison version and coverage of each parity claim. Format
differences are discoverable rather than hidden behind fallback behavior.

## References

- [Storage strata ADR](ADR-0008-storage-strata-and-durable-first-bulk-state.md)
- [Canonical-state authority ADR](ADR-0007-canonical-index-set-authority-and-recoverable-artifact-transactions.md)
- [Indexing architecture](../indexing.md)
