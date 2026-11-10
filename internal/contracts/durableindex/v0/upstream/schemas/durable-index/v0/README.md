# durable-index/v0 — initial design bundle

**Proposed, design review only.** This directory contains a structural candidate
and logical controls, not a complete physical corpus or qualified implementation.
The entry schema specializes segmented-snapshot's publication without creating a
second publication record. That family's optional `profile_metadata` binding is
mandatory for the index profile and carries `index-metadata.schema.json`.

- [Standard](../../../docs/standards/durable-index-contract.md)
- [ADR](../../../docs/decisions/ADR-0017-durable-index-contract.md)
- [Encoding DDR](../../../docs/decisions/DDR-0004-durable-index-row-and-plan-encoding.md)
- [Conformance requirements](../../../docs/standards/durable-index-conformance.md)

## Implemented design controls

`python3 scripts/test-durable-index-controls.py` runs shape checks, authored
plan/framing and transition cases, byte-bound metadata/plan/coverage cases,
cutover receipts, strict parser controls and fixed checker mutations. `cases.json`
inventories every JSON fixture. The standard's refusal table and the
[reject index](rejects/README.md) must agree with the checker and authored cases;
the [vector index](vectors/README.md) inventories positive and parser vectors.
Unknown, missing or unlisted cases fail the gate. All shape inputs pass strict
JSON parsing before the existing offline goneat validator. Tool failures do not
count as data refusals.

## Independent logical reader

`scripts/durable-index-reader.ts` is a second, independently written checker.
It was written from the standard's text, not from the Python checker, and shares
no code with it. Its independence can be inspected:

| Checker                                  | Language   | Author role | Model           | Dependencies                                              |
| ---------------------------------------- | ---------- | ----------- | --------------- | --------------------------------------------------------- |
| `scripts/test-durable-index-controls.py` | Python     | entarch     | GPT-6 (Codex)   | standard library; invokes goneat (Rule 2b subject matter) |
| `scripts/durable-index-reader.ts`        | TypeScript | devlead     | Claude Opus 5.5 | `node:` standard modules only; invokes nothing            |

The commit attribution trailers record the author role and model of each. A
different language and author reduce correlated implementation mistakes; they
cannot rule out a conceptual mistake both share, which is what review of the
contract text is for.

Its validation interpreter is Node.js >= 22.6 (`node
--experimental-strip-types`), using only `node:` standard modules with no install
step; conformance is judged on Node.js. It uses erasable TypeScript syntax only,
and its type annotations are documentation: Node.js strips them and continuous
integration runs no type checker. Bun may run the same source as a developer
convenience. It is contract compliance tooling under
[PDR-0006](../../../docs/decisions/PDR-0006-shipping-charter.md) Rule 2c: it
emits nothing and holds no authority. It is not a reference implementation or a
library.

It reads the authored fixtures and reports whether its own results agree with
their expected outcomes. A disagreement is a finding against a fixture, the
standard or one of the checkers, and is resolved by the contract text, never by
copying the other checker. Disagreements kept on purpose are listed in the
reader's `KNOWN_DISAGREEMENTS` table. An entry that agrees, disagrees
differently or names no current fixture fails the run, so a fix or a renamed
fixture is noticed. Each entry is an open finding that must be resolved before
the next tagged release: the table is empty at tag.

Current coverage is partial: plan vectors (canonicalization, framed digest,
scope loss and membership), positive transition vectors with exact output rows,
and transition rejects. Binding, graph, cutover, row and parsing scenarios are
not yet independently checked by it.

Examples are standalone structural objects. Binding scenarios verify an index
metadata/plan/coverage subgraph with actual pinned bytes and authored enumeration
evidence. They check scope order/count/digest, coverage subject/time/volume,
gaps, inherited provenance and omission of returned keys. Transition scenarios
exercise the fold using explicit normalized authority; these are harness inputs,
not wire booleans. Cutover scenarios check receipts and pinned endpoint identities,
not the physical qualification of either endpoint. Cross-member logical rows are
checked in input order. Negative folders overlay only changed input objects from
a named baseline. `store_overlay` supplies only changed dependency bytes and
`omit_refs` names deliberately missing objects. All pin updates are authored in
those overlays; the checker never repairs bindings or derives expected outcomes.

Complete baseline/observe/enrich **logical projection graphs** now accompany the
subgraph cases under `vectors/graphs/`. They bind actual NDJSON twin bytes using
the explicit corpus-only [logical profile](fixtures/logical-profile.md).
Every publication/manifest/profile/descriptor/catalog/schema/metadata/plan/
coverage/member edge is checked; successor verification fetches the exact parent
and checks authored source evidence against independently authored complete rows.
Protection defaults to block_export across all dependencies, including inherited
state; no field-level allow overrides it. The full-graph control tests transition
verification, not a snapshot-only physical reader.

This projection is deliberately **not a conforming Parquet publication**. Its
index metadata names the intended production encoding, while its fixture profile
and descriptor honestly identify NDJSON transport. No fake Parquet bytes or
unresolvable placeholder member pins are used. The result is labeled
`logical_projection`; it cannot establish R2 or qualify either cutover endpoint
physically. Physical twins and independently implemented reader evidence remain
post-publication adoption gates for a logical-only experimental release. Reviewed
integration and publication assent remain required. The checker does not
authenticate actors, prove source completeness or qualify a storage backend.

## Schema and pin composition

Vendor this family with the exact segmented-snapshot, coverage-attestation and
data-artifact dependencies from the same reviewed release. Resolve logical
`contract:` ids offline. A pin inventory must include standard/DDR/schema/corpus
bytes and exact source commit/tag provenance. No runtime library is shipped.

The new segmented-snapshot field is optional in its base schema and required by
the index semantic profile. Readers that do not implement `profile_metadata` MUST refuse it. A reader
must never ignore an unsupported required profile and report full verification.

## Limits of present controls

The logical checker accepts at most 256 nested JSON containers and refuses deeper
input as `invalid_json`. Its ancestry limit is 64 snapshots. Metadata byte limits
apply separately to each snapshot, including dependencies shared with ancestors.
These are distinct resource limits. Fixed mutation witnesses must change from
refusal to acceptance, or reach an explicitly named alternative result; a different
incidental refusal or an exception is not evidence that the intended rule was tested.

R2/W1/W2/W3 evidence requires complete graphs with real Parquet members and
independent logical twins, all profile rules, refusal inventories and mutation
controls. Physical decoder invocation remains subject to PDR-0006. A released experimental contract still does not qualify adopter runtime
fencing, durability, recovery, source coverage or reclamation.
