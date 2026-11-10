---
title: "Durable Index Conformance"
description: "Design controls, physical corpus and adoption qualification gates"
category: "standards"
status: "draft"
version: "0.0.0"
lastUpdated: "2026-10-18"
maintainer: "core-standards"
content_license: "CC0"
---

# Durable Index Conformance

The [standard](durable-index-contract.md) and schemas are design candidates.
This document defines the evidence required for conformance, not a declaration
that every listed test exists. Current controls are identified in the
[schema README](../../schemas/durable-index/v0/README.md).

## Claims and boundaries

| Claim                    | Required evidence                                                                                                                | Does not prove                                                                         |
| ------------------------ | -------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------- |
| R1 structural            | Closed schemas, exact capability resolution, positive objects and one-field shape rejects                                        | Integrity or semantic agreement                                                        |
| R2 snapshot reader       | R1 plus full byte graph, supported physical profile, decoded rows, plan/coverage/selection identity, internal lineage and counts | Truth of a provider claim, correct historical transition, runtime publication ordering |
| W1 baseline writer       | R2 output plus baseline transition corpus and input observation evidence                                                         | Successor behavior                                                                     |
| W2 observation successor | W1 plus exact verified parent, admission and successor corpus                                                                    | Fencing, durability or crash recovery of a particular backend                          |
| W3 enrichment writer     | W2 plus enrichment and inherited provenance corpus                                                                               | Provider metadata coherence without provider evidence                                  |

An R2 reader can validate the current self-contained state without fetching
ancestry. It must say that historical transitions were not verified. W2/W3
verification fetches the exact parent; missing evidence is unresolved, never an
empty baseline. No claim extends beyond the provider, encoding, operation and
resource profile actually exercised.

## Evidence requirements

| Evidence class       | Artifacts and detecting controls                                                                                                                                                           | Acceptance                                                                                                                                  |
| -------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------- |
| Design               | ADR/DDR, standard, closed schemas; plan framing, containment and logical successor vectors                                                                                                 | Architecture, security and adopter design review                                                                                            |
| Structural graph     | Full worked baseline/observe/enrich graphs; exact row schema/catalog/descriptor/profile pins; one-field rejects for every public object; duplicate keys and hostile numeric/Unicode inputs | Every positive validates, each negative fails the named layer, no tool failure counts as rejection                                          |
| Semantic transitions | File-backed parent, input observations, plan, attestations and expected complete rows/refusal; digest-bound source bytes; all I1–I7 rules mapped to cases                                  | Independent authored expectations; checker mutation controls fail when each critical gate is weakened                                       |
| Physical corpus      | Small Parquet members and independently authored logical twins; null/empty, tombstone, Unicode, revision, enrichment, row-group/member boundary, nanosecond and maximum integer cases      | The physical reader agrees with independently authored logical twins on rows, counts and order; corrupt/truncated/unsupported inputs refuse |
| Publication          | Contract manifest/catalog indexes, positive/negative inventory, checker invocation set, pin inventory and release notes                                                                    | Full repository checks and reviewed exact local tip before publication                                                                      |
| Adoption             | A producer emits contract form, pins exact released schemas/corpus and runs vectors against its own implementation; separate reader consumes its output                                    | Scope-qualified evidence; no fixture-only runtime claim                                                                                     |

A published corpus may remain experimental; independent adoption evidence gates
stronger implementation and maturity claims. Coverage-attestation acceptance is a separate decision.

## Experimental publication and reader evidence

An experimental release may publish the schemas and logical vectors before
physical Parquet twins or independent reader qualification exist. It MUST label
the evidence as logical-only, keep the family experimental through adoption,
and preserve the distinction between publication and R2/W1/W2/W3 qualification.
Complete logical graph and semantic controls remain publication requirements;
deferring physical evidence does not excuse incomplete binding or authority rules.
Composition does not ratify coverage-attestation.

The reference checker reads authored JSON/NDJSON logical twins using Python's
standard library and goneat. The adopter independently implements its producer
and physical-member reader. They MUST NOT share reader, transition or validation
code; sharing pinned schemas and authored fixture data is expected. Compare both
implementations to the fixed logical oracle, never to expectations emitted by
the producer under test. Record input pins, implementation revisions, exercised
operations and failures as evidence.

This arrangement provides independent semantic checking plus physical-to-logical
parity. The logical checker does not decode Parquet and cannot claim physical
reader conformance. Two independent Parquet decoders would require an additional
physical implementation; that stronger claim and its tooling admission remain
separate. The Parquet tooling question need not be settled to publish logical
vectors or establish their independent semantics.

## Logical projection corpus

A logical-only release may exercise complete composition using a clearly named
**test-only transport profile** over actual pinned NDJSON twins. The corpus's
`example.index-logical/v0` profile specifies that transport; it does not add a
production encoding to durable-index/v0. Projected metadata retains the intended
production encoding while the descriptor and test profile identify the actual
logical bytes. Results MUST be labeled `logical_projection` and MUST NOT claim a
conforming Parquet publication or R2/W qualification. Replacing those members
with independently decoded physical twins remains an adoption gate.

The projection checks exact bytes and identities throughout the composed graph,
row-schema/catalog agreement, count/order, transitive metadata/member separation,
resource bounds, coverage subjects and volumes, exact-parent admission and row
transitions. Authoritative enumeration/admission evidence and complete expected
rows are independently authored inputs. The graph control requests transition
verification, so a missing parent refuses that claim even though the flat current
row set could be reconstructed without ancestry. Protection is inherited across
keys, metadata, diagnostics and parent state; a field-level allow cannot release
an object whose effective action remains block_export. Actual authorization and
storage enforcement still require adopter evidence.

## Required transition matrix

- Baseline: empty and nonempty; unknown baseline is explicitly not compared;
  non-null parent or nonzero generation refuses.
- Identity: wrong set, generation, parent digest/selection, stale admitted parent,
  parent cycle, reused snapshot and missing parent each refuse distinctly.
  Schema/producer-version changes preserve the old set token; a changed token
  requires an explicit cutover to a new baseline, never a cross-set parent.
- Canonical plans: reordering, duplicate and redundant prefix compiler inputs;
  namespace-major order and containment only within one namespace; reader refusal
  of noncanonical wire plans; empty versus unknown; raw path-like keys; Unicode
  scalar byte order and framed-hash collision pairs. Serialization
  round-trips preserve compiler-emitted prefix bytes; reader cleanup, implicit
  delimiter insertion and boundary-semantic widening are refused.
- Admission: addition, broader containment, same-count substitution, net growth
  with loss, empty new plan, acknowledgment wrong parent/digest/actor authority,
  and a repeated reduced plan with stale retained rows still visible.
- Coverage: exact ordered plan correspondence, missing/extra/duplicate scopes,
  wrong subject/grain, complete-with-gap, inferred or partial non-deletion,
  volume mismatch, gap outside plan, observation outside scope, and failed
  discovery that no acknowledgment can override.
- Rows: new, unchanged, changed, removed, already tombstoned, resurrected;
  duplicate keys across member boundaries; immutable retained rows; first/last
  seen and changed semantics; clock reversal and unsupported precision.
- Enrichment: revision-matching update; unknown/mismatched revision; null versus
  absent attribute; stale timestamp; changed revision clears old enrichment;
  tombstoning clears enrichment; enrich-only preserves plan and original
  coverage subjects and never creates fresh observation lineage.
- Recovery/runtime (adopter qualification): restart at every seal/publication
  boundary, unresolved write outcomes, stale parent fencing, previous pin remains
  readable after failure, no widening from ambient configuration, custody between
  verification/use, bounded metadata/decompression/scratch and safe reclamation.

## Fixture organization

Use `examples/` for complete worked objects, `rejects/schema/` for one-field
shape differences from named baselines, `rejects/semantic/reject-<reason>--<case>/`
for changed objects only, and `vectors/` for authored input/output transitions.
Every expected refusal names a stable reason. Multiple-fault cases state the
allowed reason set; no unsupported global precedence promise. Inventory and
reason tables are checked against files. Missing fixture files are a test failure.

Logical JSON/NDJSON twins are the cross-language oracle. A producer's generated
receipts are never that oracle. A generator may materialize authored values or
bind their bytes; it must not obtain expected outcomes by running the checker.
Physical twins additionally require independent decoding evidence and exact
hash provenance. Repacking need not produce identical Parquet bytes.

## Tooling and residuals

Repository controls use Python standard library plus the existing `goneat`
JSON Schema validator. No network or instance-selected command/schema path is
permitted. A Parquet decoder is not silently added to the canonical toolchain;
physical evidence must have an explicit subject-matter invocation/admission
review under [PDR-0006](../decisions/PDR-0006-shipping-charter.md), or be supplied
as separately qualified adopter evidence with honestly limited repository checks.

A contract release is not a library extraction, backend benchmark, power-loss
proof or source completeness guarantee. The build must retain that distinction
in its test output, pin documentation and release claims.
