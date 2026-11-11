# Segmented snapshot schema family v0

**Proposed / experimental.** The capability is `contract: segmented-snapshot/v0`.
Resolve it through a trusted, pinned `contract.json`; never treat instance-provided
retrieval addresses as a schema registry. The normative companion is the
[segmented snapshot standard](../../../docs/standards/segmented-snapshot-contract.md).

| Artifact                           | Responsibility                                                           |
| ---------------------------------- | ------------------------------------------------------------------------ |
| `contract.json`                    | Publication entrypoint and related object-schema catalog                 |
| `common.schema.json`               | Closed bindings, selection, capability and count definitions             |
| `snapshot-publication.schema.json` | Committed producer fact binding descriptor, manifest and profile         |
| `snapshot-manifest.schema.json`    | Exhaustive immutable member list for one grain/representation            |
| `snapshot-profile.schema.json`     | Exact behavioral specification, supported operations and resource limits |
| `verification-result.schema.json`  | Terminal result for an admitted selection and explicit checked scope     |
| `semantic-validation.md`           | Cross-record and byte-level requirements, independently of JSON shape    |
| `fixtures/base/`                   | Exact synthetic graph with real SHA-256/length bindings                  |
| `fixtures/cases.json`              | Positive and detecting negative mutations of that baseline               |

## First-version boundary

The initial wire version uses **flat, self-contained snapshots**, one selected
grain/representation per manifest. It supports at most 65,536 members and 64 MiB
of required metadata; a profile can lower those ceilings. This bounds descriptor
traversal, not dataset size or a producer's total working memory.

Paged manifests and snapshots requiring an external parent to reconstruct their
rows are unsupported. Required sidecars must be among the explicitly bound
profile/schema/catalog objects, data members, or the optional `profile_metadata`
edge and its exhaustively declared, acyclic transitive dependencies; hidden
dependencies are invalid. Readers that do not implement `profile_metadata` MUST
refuse a manifest containing it.
The row schema is standalone (fragment-local schema references only). Producers
may retain lineage separately for provenance; it cannot become an undeclared
read dependency. A new capability/version boundary is required to broaden this
wire graph. These limits are deliberate refusal boundaries, not scale benchmarks.

The descriptor remains `data-artifact/v0`. It is byte-bound rather than nested or
redefined. Its representation URI names the manifest's logical reference. The
manifest independently pins the row schema and field catalog. An aggregated
dataset is its own grain; `count_relation` is `equal` or `subset` of that grain.

Profile `semantic_spec` binds the row/count/order rules and the substrate's
publication, custody, interruption and recovery specification. `durability_claim`
is explicitly `none`, `process_crash`, or `power_loss`; it is a support claim that
needs runtime evidence. `retention: profile_defined` also requires an exact
`retention_spec`. This declares a protocol dependency, not a mutable lease/pin API
and not proof that a caller acquired protection.

## Usage and validation

Run from the repository root:

```sh
python3 scripts/test-segmented-snapshot-controls.py
make lint-contracts
make check
```

Structural tools resolve the hostless common-schema refs using a pinned local
reference tree. The control runner uses Goneat with `--ref-dir` for this purpose.
JSON Schema checks shape; semantic checks then verify exact references, matching
identities, scope and counts. Production readers still need authorized resolution,
same-bytes custody, format-specific decoding and relevant runtime evidence.

The fixture runner is repository validation material, not a consumer-linked
reader. It resolves only a fixed in-memory map of synthetic committed bytes,
performs no network access, and provides no implementation of crash durability,
concurrent reference admission or reclamation.

## Adoption

Pin the family revision, schema bytes, semantic layer and implementation profile.
Schema publication does not establish that any existing producer has adopted it.
Use the [adoption record](../../../docs/standards/segmented-snapshot-conformance.md#adoption-record)
to record a native mapping and an independent consumer/validator witness. A native
completion marker missing required bindings needs an adapter/new portable graph;
do not fill missing evidence from a convenient path or a later crawl.
