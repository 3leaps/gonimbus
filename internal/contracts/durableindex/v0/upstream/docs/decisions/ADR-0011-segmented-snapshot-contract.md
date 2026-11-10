---
id: "ADR-0011"
title: "Immutable segmented snapshots as a companion contract"
status: "proposed"
date: "2026-09-30"
last_updated: "2026-09-30"
deciders:
  - "@3leapsdave"
  - "entarch"
scope: "Crucible foundation / portable data contracts"
tags: ["data-contracts", "snapshots", "storage", "interchange-contract"]
relates-to:
  - "crucible ADR-0001 (schema/config versioning)"
  - "crucible ADR-0004 (coverage attestation)"
  - "crucible DDR-0002 (snapshot identity and evidence)"
  - "crucible PDR-0006 (shipping charter)"
---

# ADR-0011: Immutable Segmented Snapshots as a Companion Contract

## Status

**Proposed.** Establishes the boundary for a candidate
`contract: segmented-snapshot/v0` family. The experimental schema bundle and
semantic corpus accompany the prose; adopter conformance remains separately gated.

## Context

The portable data-artifact contract describes logical grains, representations,
read capabilities, integrity and protection. It does not define which immutable
files comprise a selected snapshot, how publication is bound to those files, or
how a reader distinguishes a committed artifact graph from incomplete residue.

Those questions recur in object indexes, analytical datasets and projections of
append-only history. They do not require the same row schema, journal protocol,
query engine or mutable coordination store.

## Decision

Define a narrow companion for **immutable segmented snapshots**. Compose with
`data-artifact/v0` for discovery, grain/representation semantics, catalogs and
protection. Keep coverage claims in `coverage-attestation/v0`.

The companion governs exact snapshot selection, manifest and segment bindings,
publication evidence, reader verification and preservation of referenced data.
Profiles specify physical encodings, row semantics and the publication mechanism
that supplies these properties on a particular substrate.

Separate portable invariants from implementation profiles:

- The core identifies the selected immutable graph and the claims a consumer can
  verify about it.
- A profile identifies the exact row/schema mapping, ordering/count rules,
  storage assumptions, verification capabilities and unsupported operations.
- Conformance evidence distinguishes data-shape checks, graph/semantic checks
  and runtime failure behavior.

Do not extend the data-artifact lifecycle into a transaction state machine.
`complete` remains a producer lifecycle claim. A publication receipt proves a
different fact from source coverage or successful downstream processing.

### Exclusions

This is not a mutable database API, WAL protocol, receipt-domain schema, SQL
engine, distributed consensus protocol, or garbage collector. It does not choose
Parquet compression, partition sizing, a lock implementation, SQLite, or DuckDB.
Live acknowledgement, replay, group commit, mutation fencing and uncertain remote
completion belong in separately versioned protocols.

### Publication and adoption

The review surface includes the prose contract, data decision, closed schemas,
semantic rules and detecting positive/negative fixtures. The initial wire bundle
supports flat, self-contained single-representation snapshots; paging and external
parent reconstruction require a subsequent explicit capability/version boundary.

Qualify with a real producer mapping and an independently implemented consumer
or validator. A second workload mapping tests portability; it need not be a
second storage engine. Pin experimental revisions. Apply the stable-promotion
gate of ADR-0001 independently of the repository's release maturity.

Crucible ships standards, schemas and repository validation material under
PDR-0006. Consumer-linked readers, writers and recovery engines belong elsewhere.

## Alternatives

- **Only producer-local specifications:** sufficient for one product, but leaves
  every new consumer to rediscover publication and identity semantics.
- **Expand data-artifact into storage governance:** mixes discovery/export with
  transactional behavior and burdens producers that do not store snapshots.
- **Standardize a general durable store now:** joins immutable interoperability
  to unproven mutable recovery and concurrency choices.
- **Narrow companion plus profiles:** selected proposal; shares the verifiable
  boundary while allowing independent physical implementations.

## Consequences

Native formats can retain their layouts and gain an explicit mapping. Existing
artifacts do not become conforming merely because they contain Parquet or a
manifest. Adoption is opt-in; publishing this proposal does not require existing
producers or consumers to change their releases.

## References

- [Segmented snapshot contract](../standards/segmented-snapshot-contract.md)
- [Snapshot identity and evidence](DDR-0002-segmented-snapshot-identity-and-evidence.md)
- [Portable data artifact contract](../standards/data-artifact-contract.md)
- [Crucible shipping charter](PDR-0006-shipping-charter.md)
