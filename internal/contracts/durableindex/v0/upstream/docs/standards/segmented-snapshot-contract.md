---
title: "Portable Segmented Snapshot Contract"
description: "Proposed immutable snapshot identity, publication, verification and preservation semantics"
category: "standards"
status: "draft"
version: "0.0.0"
lastUpdated: "2026-10-18"
maintainer: "core-standards"
reviewers: ["architecture", "security", "data-engineering"]
approvers: ["lead-maintainer"]
tags: ["data", "snapshots", "contract", "storage", "interoperability"]
content_license: "CC0"
relatedDocs:
  - "docs/standards/data-artifact-contract.md"
  - "docs/standards/segmented-snapshot-conformance.md"
  - "docs/decisions/ADR-0011-segmented-snapshot-contract.md"
  - "docs/decisions/DDR-0002-segmented-snapshot-identity-and-evidence.md"
audience: "implementers"
---

# Portable Segmented Snapshot Contract

## Maturity and scope

This is the **proposed experimental contract** `contract: segmented-snapshot/v0`.
Normative words specify the proposed behavior; they do not imply ratification.
The [schema bundle](../../schemas/segmented-snapshot/v0/README.md) supplies a
publication entrypoint, closed object shapes, semantic rules and detecting
fixtures. Schema availability alone does not establish producer conformance;
adopters need the mapping and implementation evidence for their claimed scope.

The contract identifies and verifies a published, immutable, segmented snapshot.
It composes with [data-artifact/v0](data-artifact-contract.md); it is not an
alternative grain, protection or field-catalog model.

It does not define live append acknowledgement, WAL replay, transactions across
stores, writer fencing, remote mutation recovery, or retention policy. These
protocols may publish snapshots conforming to this boundary without sharing a
storage engine. The related decisions are [ADR-0011](../decisions/ADR-0011-segmented-snapshot-contract.md)
and [DDR-0002](../decisions/DDR-0002-segmented-snapshot-identity-and-evidence.md).

## Contract composition

| Layer                      | Owns                                                                         | Does not establish                                                          |
| -------------------------- | ---------------------------------------------------------------------------- | --------------------------------------------------------------------------- |
| Data artifact              | Discovery, grains, representations, catalogs, protection, producer lifecycle | Durable commit or source completeness                                       |
| Segmented snapshot         | Exact immutable graph, publication binding, verification semantics           | Historical write acknowledgement or provider-wide point-in-time consistency |
| Coverage attestation       | Independent scoped assessment and supersession                               | Successful publication or authorization to mutate                           |
| Process run                | Observation and local process control                                        | Strict receipt persistence; telemetry may fail open                         |
| Producer/substrate profile | Native schema mapping, publication mechanism and supported operations        | Unadvertised capabilities or universal engine parity                        |

## Logical records

These record responsibilities are encoded by the experimental v0 bundle without
introducing a second artifact model. Its first wire profile is flat and
self-contained: one selected grain/representation, at most 65,536 members and
64 MiB of required metadata, with lower profile limits allowed. It refuses paged
manifests and snapshots requiring external-parent reconstruction. This is an
explicit capability limit; the broader invariants below govern any later extension.

### Snapshot manifest

A manifest MUST bind:

- Store/dataset namespace and immutable snapshot identity.
- Data-artifact identity and the represented grain/representation ids.
- Exact profile and row-schema identities and digests, including required field
  catalogs. Resolving a familiar name to different schema bytes is a mismatch.
- An exhaustive member list. A later paging extension would need digest-bound
  immutable pages with verified exhaustion. Enumeration by directory glob or
  mutable catalog is not authoritative.
- Each member's opaque id, authorized logical reference, byte length and digest.
- Declared physical counts and ordering semantics, including whether counts
  equal, project or aggregate the logical grain's count.
- Every dependency required to read the snapshot, with exact identity/digest
  bindings. Lineage mentioned only for provenance must be distinguished from a
  dependency that readers need.

The optional `profile_metadata` binding adds profile-defined required metadata.
When present, it is a required edge of the immutable graph, not a hint. The
selected profile MUST define the metadata schema, identity, selection binding,
complete dependency enumeration and verification rules. Every transitive byte
dependency counts against the same metadata budget. Cycles, unresolved metadata
and unsupported required profiles cause refusal, including on selective reads.
Metadata MUST NOT introduce external row reconstruction or paged member lists
under this flat version. Existing manifests without this field retain their
semantics. Readers that do not implement `profile_metadata` MUST refuse any
manifest containing it, including on selective reads.

Required members may be empty only under an explicit empty-dataset profile rule.
Unknown required profiles, roles or dependency kinds MUST cause refusal.

### Publication record

A publication record MUST bind the namespace, snapshot id, artifact id, exact
descriptor bytes, exact manifest bytes and publication-profile identity. A
profile may map an existing native completion marker to this role; its mapping
must account for every required binding rather than infer it from the path.

The record states that publication completed under that profile. It is not by
itself independent proof that the producer executed its durability obligations.
The record MUST NOT claim source enumeration completeness or successful
downstream processing from publication alone.

No self-digest or digest cycle is required. A publication can bind the descriptor
and manifest without either carrying the digest of that publication. Identifiers
can cross-reference logically while byte-digest dependencies remain acyclic.

### Verification result

A result MUST identify the selected snapshot/manifest, verifier/profile revision,
verification scope and outcome. Distinguish at least:

- structurally invalid input;
- unresolved or unauthorized references;
- unsupported profile or required capability;
- identity/integrity mismatch;
- incomplete publication;
- capacity refusal;
- successfully verified scope.

The v0 result schema encodes these classes for an admitted snapshot. Pre-admission
errors use the caller's typed error surface rather than fabricated bindings.
Diagnostics remain subject to the data-artifact protection boundary. Absence of
a result does not mean successful verification.

## Portable invariants

### S1 — Immutable selection

An admitted namespace/snapshot/manifest binding MUST NOT change. A `latest`
selector may advance; a reader resolves it once to an exact binding and keeps
that binding through verification, reads and result attribution. A missing pinned
snapshot MUST NOT silently resolve to another snapshot or to an empty dataset.

Repacking, reordering or rewriting the graph produces a new binding even if
logical content is unchanged. Semantic equivalence can be recorded separately.

### S2 — Exact integrity

Every required edge MUST have an exact identity and integrity binding. Physical
identity digests MUST cover exact emitted/stored bytes; the v0 schema
uses SHA-256. A profile may serialize canonically before emission but
MUST NOT normalize stored bytes during verification. Semantic or canonical
comparison digests MUST be identified separately and cannot replace physical
identity digests.

Readers MUST verify the same bytes they consume, or hold a profile-proven custody
mechanism that prevents substitution between verification and use. A checksum
detects byte mismatch against its expected value; it does not authenticate the
producer or make an untrusted manifest authoritative.

### S3 — Closed, bounded graph

Required member/dependency enumeration MUST be complete, finite and unambiguous.
Reject duplicate member ids, cycles in required byte dependencies, missing pages
and conflicting bindings. An optional hint cannot hide a required dependency.

A profile MUST state supported graph/member/descriptor limits and verification
budgets, including bounded traversal or typed capacity refusal. Paging can bound
working memory only if page identity, order and exhaustion are also verified.
The base does not promise constant-memory traversal merely because rows stream.

### S4 — Explicit publication

A conforming writer MUST finish and protect the required members before exposing
a successful publication binding. The substrate profile MUST specify write
ordering, durability acknowledgements, immutable-object/custody assumptions,
failure states, recovery and how readers recognize incomplete publication.

Profiles MUST distinguish process-crash consistency from power-loss durability
and state their filesystem or object-store assumptions. Atomic rename alone is
not a portable durability guarantee. A mutable discovery alias cannot repair or
replace an invalid publication record.

### S5 — Honest read scope

Readers MUST validate the publication, selection, required metadata and supported
capabilities before treating a snapshot as admitted. They MUST verify the bytes
used by a read before reporting that read's scope as successfully verified.

A full-graph audit and a selective read are different results. A selective read
may leave unrelated data members unverified if its profile declares that scope;
it must not claim whole-snapshot integrity. Streaming profiles MUST distinguish
provisional output from verified output and define their terminal success/error
signal. Consumers requiring all-or-nothing output must use an appropriate
staging or verification mode.

Unsupported operations and missing acceleration structures MUST NOT masquerade
as empty successful results. Advertise key lookup, projection, ordering and
digest-verification capabilities explicitly; do not infer them from Parquet,
NDJSON, SQLite or an analytics engine's name.

### S6 — Counts, coverage and semantics

Counts MUST follow the associated data-artifact grain/representation mapping.
Profiles define primary-key uniqueness, duplicate resolution, sort/null rules,
tombstone meaning and source-revision semantics when applicable. The base does
not impose object-index semantics on event-history grains.

Committed storage may represent partial source coverage. Coverage attestations
MUST refer to the selected subject under their own contract. Because data-artifact
allows reusable logical artifact ids, the v0 semantic layer specifies an immutable
`subject_uri` derived from namespace/snapshot identity, alongside exact artifact
and grain matching. See [the subject derivation](../../schemas/segmented-snapshot/v0/semantic-validation.md#immutable-coverage-subject).
Artifact-id equality alone is insufficient when that id is reused;
ambiguous or wrong-snapshot attestations MUST be refused as coverage evidence.

A full crawl does not reconstruct acknowledged-write history; equal row counts
do not prove row equivalence; a digest over a projection proves only that
projection.

### S7 — Preservation of referenced graphs

An active protected snapshot reference MUST preserve every required artifact
until released under its governing authority mechanism. Naming an id is not
enough to establish that protection. Profiles that expose retention protection
MUST identify admission, lifetime, restart behavior and loss/refusal semantics.

Reclamation MUST account for the transitive reachability of all protected
snapshots and required dependencies under a mechanism that excludes conflicting
publication/reference admission. Merely observing no live reader, or a non-current
`latest` pointer, is insufficient. An implementation without a safe reclamation
protocol may retain artifacts and declare reclamation unsupported.

This contract does not prescribe leases, reference-store schemas, TTLs or deletion
APIs. Representation retirement and logical-history pruning are distinct
operations; semantic equivalence does not permit rewriting an exact pin.

### S8 — Protected metadata and resolution

Apply data-artifact protection to descriptors, manifests, member ids, key bounds,
counts, digests, diagnostics and file metadata. Portable references MUST NOT
embed credentials, real service endpoints or machine-local absolute paths.
Opaque logical references are resolved through an authorized binding mechanism.

A raw internal manifest is not automatically a safe export representation.
Reduction or redaction that changes committed bytes requires a new representation
and binding. Profiles MUST state how protected column statistics, indexes and
membership-oracle structures are withheld. Integrity cannot launder metadata
into a lower protection class.

## Profile obligations

Each profile supplies a versioned mapping for:

1. Namespace, snapshot, artifact and native run/version identities.
2. Exact schemas/catalogs, member enumeration and publication binding.
3. Row/count/order semantics and any comparison projection.
4. Read capabilities, verification scope and resource limits.
5. Substrate durability, custody, interruption and recovery behavior.
6. Protected reference lifetime and reclamation support, if offered.
7. Export/protection behavior and unsupported combinations.

Profile support is operation-specific. A reader-only adapter can conform to its
read profile without claiming writer/recovery conformance. An exporter producing
a new portable graph must identify that new artifact, rather than imply it
retroactively upgraded the original native format.

## Conformance and evolution

[Conformance cases and mappings](segmented-snapshot-conformance.md) define the
review corpus. The v0 schemas, semantic layer and repository controls are shipped
together. Runtime claims additionally require the relevant producer/consumer
evidence; repository fixture validation does not establish that adoption.

Freeze requires a real producer mapping and an independent reader/validator
witness. Test a second logical workload for neutrality. Capability support,
artifact format versions and Crucible release versions remain separate axes.
Breaking changes to identity, digest scope, publication or read-success semantics
require an explicit version/capability boundary.
