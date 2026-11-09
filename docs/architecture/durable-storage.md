# Durable Storage Architecture

This is the navigation and responsibility map for Gonimbus's durable index.
The index implementation is shipped; proposed extensions are identified below.
The storage-selection policy is proposed in [ADR-0008](adr/ADR-0008-storage-strata-and-durable-first-bulk-state.md).

## The index path

```text
provider listing
  → sealed observation journals
  → verified parent + bounded spill/merge
  → immutable Snappy-Parquet segments
  → internal manifest
  → completion marker
  → latest pointer
```

A durable snapshot is an exact committed artifact graph. It is not a directory
of Parquet files selected by globbing or modification time. Exact pinned reads
check the requested set/run and select its completion marker instead of following
a later `latest` advance. Latest-selected reads use a separate discovery/open
path; chain consistency and binding back to the caller's admitted identity are
distinct verification obligations. See the [conformance scope](canonical-state-conformance.md).

| Concern                                                   | Canonical documentation                                                                         |
| --------------------------------------------------------- | ----------------------------------------------------------------------------------------------- |
| Operator choices, format selection and supported commands | [Durable index](../user-guide/durable-index.md)                                                 |
| IndexSet identity, scope and row semantics                | [Indexing architecture](indexing.md)                                                            |
| Parent continuity and forward-delta meaning               | [Durable lineage](durable-lineage.md)                                                           |
| Memory/disk/FD-bounded row preparation                    | [Spill/merge](durable-spill-merge.md)                                                           |
| Pull-to-seal segment writing                              | [Streaming segment writer](durable-streaming-segment-writer.md)                                 |
| SQLite/durable comparison fields and exclusions           | [Compare projection v1](index-compare-projection-v1.md)                                         |
| Authority, identity, leases and recovery                  | [ADR-0007](adr/ADR-0007-canonical-index-set-authority-and-recoverable-artifact-transactions.md) |
| Decision status versus implementation evidence            | [Conformance scope](canonical-state-conformance.md)                                             |

## Four responsibilities

**Coordination** protects identity and lifetime. A live lease and a durable
reference are different facts. Absence of a live process does not prove that
retained data has no reader, recovery or lineage dependency.

**Bulk authority** contains the facts the dataset owns. For the current index,
these are observed object state and its declared lineage/coverage. Immutable
segments plus exact publication metadata keep that authority independent of a
monolithic database file.

**Derived access** provides a convenient way to query facts. Its results must be
bound to the source snapshot and its supported semantics. An optional database
projection or query engine must not silently replace the authoritative artifact.

**Observation** reports progress and outcomes. An operation may fail during
cleanup after an artifact committed. Report both facts: process status does not
erase a commit, and a progress event does not prove one.

## SQLite compatibility

Durable has been the default build format since v0.4.0. SQLite remains an explicit
compatibility path. `--format both` produces a run-scoped SQLite verification
projection alongside durable publication; it does not require a canonical
`index.db` for durable readers.

Compatibility means documented common semantics and safety obligations. It does
not mean equal storage cost or identical access paths. See
[DDR-0001](adr/DDR-0001-index-semantics-and-representation-parity.md). The operator
guide remains the source for the current per-operation capability matrix.

## Claims and their evidence

Keep these claims distinct:

- **Identity:** this is the requested set/run/artifact.
- **Integrity:** the bytes read match the committed digests under the documented
  custody assumptions. A checksum is not independent producer authentication.
- **Publication:** the selected artifact graph completed its commit protocol.
- **Coverage:** the observations cover the stated scope to the stated degree.
- **Lineage:** the required parent/descendant relationships were verified.

A completed publication can contain a partial observation of the source. An
object listing is not a point-in-time bucket snapshot unless the provider and
operation explicitly supply that guarantee. Neither observation nor a later
HEAD proves which source revision a prior engine write acknowledged.

## Hardening and performance boundaries

Ordinary builds stream current-state rows through spill/merge into the production
writer instead of materializing the whole row set in RAM. They still process
verified parent state; bounded spill/merge memory does not imply delta-only I/O
or zero whole-parent workspace cost. The writer retains a whole-run distinct-ETag
set, so that part of memory grows with distinct ETags, alongside manifest/segment
descriptors. Enrichment still materializes parent rows and has a default
2,000,000-row ceiling. Use the concrete budgets and exclusions in the
implementation specifications above; this is not an end-to-end constant-memory
claim.

Measure retained disk, scratch peaks, manifest/descriptor growth, file counts,
read/write amplification, recovery, and cold integrity verification—not only
compressed segment bytes or a warm query. State bucket size, selected inventory
size and working-set size separately. Published performance claims need a
declared workload and environment.

## Extension boundaries

The following are design boundaries, not shipped capabilities asserted by this
guide:

- A continuously acknowledged receipt journal needs a separate WAL protocol:
  commit framing, durable watermarks, replay, fences and uncertain remote effects.
  The existing sealed crawl journal is not that protocol.
- Reusing immutable segment mechanics does not merge observed inventories and
  acknowledged-write histories into one row model.
- Analytics engines can consume suitable verified representations. No engine is
  required by the current durable format merely because it supports Parquet.
- Portable artifact descriptors and segmented-snapshot profiles require explicit
  adoption mappings and conformance tests. Native Gonimbus manifests do not
  automatically claim conformance to a draft external contract.
- Internal manifests and raw Parquet metadata retain full-fidelity information.
  They are not de-identified or reduced-trust export formats. Boundary rendering
  remains governed by the current operator guide and explicit capability refusal.

## Evolution

Preserve existing artifact identities and readers. New guarantees require versioned
schemas, capability negotiation and detecting tests. An architecture decision
does not silently migrate files, change retention, or authorize deletion.
