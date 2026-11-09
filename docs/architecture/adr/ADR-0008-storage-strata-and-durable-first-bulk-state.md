# ADR-0008: Storage Strata and Durable-First Bulk State

**Status:** Proposed
**Date:** 2026-09-30
**Decision Makers:** @3leapsdave

## Context

Gonimbus processes large object inventories through a library and CLI. A single
SQLite index is convenient for local transactions and queries, but its physical
shape couples bulk history, secondary indexes, whole-file transport and
maintenance. Durable indexes have been the default since v0.4.0; subsequent
releases added native readers and bounded streaming publication.

The architectural distinction is the responsibility and scale of state, not
whether a particular database is useful. An operational table with an enduring
row per object is still bulk state. Compatibility with an older representation
does not make that representation the design center for new bulk workloads.

## Decision

### Separate responsibilities

| Stratum                            | Owns                                                                                         | Selection rule                                                                                                 |
| ---------------------------------- | -------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------- |
| Coordination and operational state | Identity registration, live authority, durable references, bounded transactional bookkeeping | Choose a transactional database or bounded durable records according to atomicity and cardinality requirements |
| Authoritative bulk state           | Observed inventories and other large retained datasets                                       | Prefer append journals, immutable columnar segments and committed manifests                                    |
| Derived access structures          | Lookup indexes, query projections, summaries and caches                                      | Bind to an exact authoritative version; document rebuild/refusal behavior                                      |
| Observation                        | Progress, diagnostics and process telemetry                                                  | Keep separate from state that authorizes mutation or acknowledges durable completion                           |

Classification is per dataset, not per package. Record ownership, lifetime,
cardinality, mutability, recovery needs and whether state can be reconstructed.
Do not describe object-cardinality state as bounded coordination solely because
it records a control fact.

### Durable is primary for bulk indexing

The journal/segment/manifest architecture is the default for scale-oriented
indexing. SQLite remains an explicitly selected supported compatibility format.
This decision neither removes existing formats nor invalidates existing files.

Durable-native operations must not require a hidden canonical `index.db` or
materialize the entire inventory into SQLite merely to use a common API. A
derived accelerator may use SQLite or another appropriate engine when its
authority binding, rebuild cost and resource envelope are explicit. A missing
or stale accelerator cannot silently become an empty result.

New authoritative bulk-state designs start from the durable architecture.
An exception needs evidence about its workload: retained bytes, write/read
amplification, durable acknowledgement, recovery, publication and maintenance.
Existing dependency presence and query convenience alone do not select storage.
Do not require the established bulk architecture to win an unrelated database
comparison for each new feature; measure the genuinely unresolved access seams.

### Formats and engines are separate choices

Parquet is a physical representation, not a transaction protocol. An append-only
JSONL file is not automatically a durably acknowledged WAL. SQL engines may read
or accelerate datasets without owning their canonical history. No particular
analytics engine is mandatory for the durable index.

The current crawl journal is sealed-run observation input. A future live receipt
journal needs its own commit, replay, acknowledgement and recovery specification;
this ADR does not grant it those properties.

### Scale is part of acceptance

Bound memory, open files, scratch, descriptor growth, backlog and publication
work. Report the actual resource envelope and typed refusal behavior. Separate
bucket cardinality, indexed scope, retained history and working-set size.
Small fixtures establish correctness, not large-scale qualification.

Publication may reuse immutable artifacts only when identity, coverage and
reachability remain provable. Compaction and physical reclamation cannot discard
the last required authoritative representation or a live/persisted reference.
Follow [ADR-0007](ADR-0007-canonical-index-set-authority-and-recoverable-artifact-transactions.md).

## Compatibility

Supported common operations retain logical and safety parity; physical layouts,
costs and capabilities can differ. The detailed rule is
[DDR-0001](DDR-0001-index-semantics-and-representation-parity.md). Existing SQLite
functionality is not deprecated by this decision, and new unsupported cells
must be explicit rather than silently falling back to another substrate.

## Alternatives

- **One SQLite database for all state:** reduces initial integration work but
  couples object-cardinality data to monolithic storage and maintenance.
- **Durable files for all state:** avoids a database but forces small transactional
  coordination concerns into bespoke file protocols without workload justification.
- **Remove SQLite indexing:** simplifies the format matrix but breaks a supported
  choice without being necessary to establish durable as primary.
- **Responsibility-based strata:** selected; preserves useful tools while making
  authority and scale explicit.

## Consequences

Designs must identify their source of truth and the cost of reconstructing derived
state. Backend parity tests cannot substitute for resource and recovery evidence.
The architecture guide and capability documentation must distinguish shipped
behavior from proposed live-journal or analytics extensions.

## References

- [Durable storage architecture](../durable-storage.md)
- [Durable index operator guide](../../user-guide/durable-index.md)
- [CLI and library parity](ADR-0006-cli-as-adapter-over-library-engines.md)
