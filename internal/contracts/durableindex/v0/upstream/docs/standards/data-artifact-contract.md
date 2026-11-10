---
title: "Portable Data Artifact Contract"
description: "Source-neutral contract for produced data artifacts, representations, field catalogs, provenance links, and export protection"
category: "standards"
status: "draft"
version: "0.0.0"
lastUpdated: "2026-09-30"
maintainer: "core-standards"
reviewers: ["architecture", "security", "data-engineering"]
approvers: ["lead-maintainer"]
tags: ["data", "artifact", "contract", "metadata", "protection", "interoperability"]
content_license: "CC0"
relatedDocs:
  - "docs/standards/data-artifact-contract-examples.md"
  - "docs/standards/data-sensitivity-classification.md"
  - "docs/standards/schema-stability-classification.md"
  - "docs/standards/segmented-snapshot-contract.md"
audience: "implementers"
---

# Portable Data Artifact Contract

This draft defines the portable contract identified by
`contract: data-artifact/v0`.

The contract describes produced data artifacts so a downstream consumer can
discover what the artifact contains, choose a safe read path, understand fields,
validate provenance links, and honor export protection without knowing which
producer created the artifact.

This is not a domain schema, recipe schema, query language, storage format, or
producer implementation model. Producers keep their native payloads and
producer-specific profiles. The portable contract governs the metadata wrapper:
artifact descriptor, grains, representations, field catalogs, provenance and
accounting links, protection hooks, validation state, and version/capability
signals.

## Design Stance

- **One artifact model.** Record streams, object indexes, aggregation results,
  reference tables, and projections are represented as grains plus
  representations. They are roles in one model, not separate top-level contract
  families.
- **Source-neutral.** The base contract has no file-only, document-only,
  database-only, API-only, warehouse-only, or domain assumptions.
- **Representation-aware.** Physical formats and read paths are first-class
  because consumers choose access patterns by scale and protection posture.
- **Payload-open, metadata-governed.** Payload fields are producer-owned. The
  metadata objects that travel between tools have closed, versioned semantics.
- **Profile-friendly.** Producer-native shapes are allowed through profiles and
  mappings. A profile conforms to the portable contract without renaming the
  producer's live output surface in place.
- **Protection-aware by construction.** Read-path capabilities and export-gate
  enforceability are coupled. Partial-readable data must be protectable at the
  same effective granularity.

## Capability And Versioning

Artifact descriptors MUST carry the portable contract capability string:

```json
{
  "capabilities": ["contract: data-artifact/v0"]
}
```

The capability string is host-less. Artifact instances MUST NOT embed a schema
host or URL as the contract identity. Validators resolve the capability to a
trusted `contract.json` manifest, verify its `capability`, and load the relative
`entry_schema`. Resolution MUST fail closed when the manifest is missing, the
capability does not match, or the entry schema is missing. Direct `$id` lookup
remains valid for schema-aware tooling, but it is not the L2 contract-entry
mechanism.

Each artifact descriptor SHOULD also carry producer profile and object profile
versions:

```json
{
  "capabilities": ["contract: data-artifact/v0"],
  "producer": {
    "profile": "example.extract-artifact/v0",
    "version": "1.2.3"
  }
}
```

Consumers MUST validate the descriptor before opening any representation. They
SHOULD negotiate on capabilities such as `range_readable`, `sharded`,
`scan_capabilities`, `read_path_granularity`, and
`protection_enforceable_granularity` instead of relying only on version numbers.

### Evolution Rules

Additive changes are allowed within this `v0` draft when older consumers either
ignore the new field or fail closed safely.

Breaking changes include:

- changing the meaning of an existing field;
- weakening default protection behavior;
- making a previously optional field load-bearing without a capability or
  version boundary;
- changing row-count or lifecycle semantics;
- renaming fields without a profile mapping or alias.

Unknown sensitivity values, protection tags, action values, or protection
profiles MUST fail closed.

## Governed Objects

### Artifact Descriptor

The artifact descriptor is the discovery root for one produced dataset or
bundle. It identifies the artifact, contract, producer profile, grains,
representations, provenance and accounting links, protection posture, and
validation state.

Required responsibilities:

- identify the artifact and portable contract capability;
- identify the producer and producer profile;
- list logical grains;
- list physical representations;
- link each queryable or renderable grain to a field catalog;
- link provenance and accounting sidecars when they exist;
- state semantic and physical counts where known;
- state representation read-path capabilities and protection-enforceable
  granularity;
- carry only opaque protection/profile handles, never policy internals.

Recommended fields:

| Field             | Meaning                                                                    |
| ----------------- | -------------------------------------------------------------------------- |
| `artifact_id`     | Artifact identity. A generated UUID URN is acceptable by default.          |
| `capabilities`    | Capability tokens. MUST include `contract: data-artifact/v0`.              |
| `producer`        | Producer name, version, profile id, and run/job id where safe to expose.   |
| `lifecycle`       | Artifact lifecycle state.                                                  |
| `grains`          | Logical datasets inside the artifact.                                      |
| `representations` | Physical ways to read grains or bundle sidecars.                           |
| `provenance`      | Links to sanitized provenance sidecars.                                    |
| `accounting`      | Links to counts, ledgers, validation summaries, and diagnostics.           |
| `protection`      | Default action, export class, opaque profile ref, and policy-free signals. |

### Lifecycle

The portable lifecycle vocabulary is:

| State        | Meaning                                                               |
| ------------ | --------------------------------------------------------------------- |
| `draft`      | Descriptor exists but is not publishable.                             |
| `building`   | Producer is still writing or appending.                               |
| `complete`   | Producer finished and validation passed.                              |
| `partial`    | Producer finished with tolerated skipped or unavailable input.        |
| `incomplete` | Producer failed or output is not safe to consume without remediation. |
| `retired`    | Artifact should not be newly consumed.                                |

Producer-specific status values map into this lifecycle. Expected
not-applicable inputs are accounting dimensions, not lifecycle states.

Lifecycle is the producer's self-claim about its own output. Independent
completeness claims — what an artifact actually covers, per scope, as
verified — are out of scope for the descriptor and arrive via a companion
coverage-attestation contract keyed on `artifact_id` (proposed; see
ADR-0004).

Lifecycle does not specify a storage commit protocol. A `complete` descriptor
does not by itself prove that files were durably published; a `partial` artifact
can nevertheless have a fully committed physical representation. Process outcome,
publication and coverage are distinct facts supplied by their respective
contracts and evidence.

### Grain

A grain is a logical dataset inside an artifact. It says what a row, record, or
item means independent of how it is stored.

Recommended fields:

| Field                | Meaning                                                                                                                     |
| -------------------- | --------------------------------------------------------------------------------------------------------------------------- |
| `id`                 | Stable within the artifact.                                                                                                 |
| `kind`               | `record_stream`, `object_index`, `aggregation`, `reference_table`, `projection`, or profile extension.                      |
| `record_kind`        | Producer/profile vocabulary for the logical row or item.                                                                    |
| `row_count`          | Semantic count when known.                                                                                                  |
| `primary_keys`       | Optional field names. Sensitivity still comes from the field catalog.                                                       |
| `semantic_order`     | Optional semantic ordering promise, independent of physical order.                                                          |
| `field_catalog_ref`  | Required for queryable or renderable grains; optional for raw archival grains that are not meant to be rendered or queried. |
| `provenance_ref`     | Optional grain-specific provenance link.                                                                                    |
| `accounting_ref`     | Optional grain-specific accounting link.                                                                                    |
| `disclosure_control` | Reserved slot for aggregate disclosure controls.                                                                            |

Raw archival grains that are not queryable or renderable MAY omit
`field_catalog_ref`. Queryable or renderable grains MUST still link a field
catalog (see Field Catalog and Validation Requirements).

The base contract MUST NOT require source-file fields, document record numbers,
or source-document order. Those are producer-profile promises when applicable.
Profile extensions for `kind` use a profile-qualified token containing a `/`,
so base vocabulary typos do not silently become extensions.

`disclosure_control` is reserved for aggregation grains that need to declare
small-cell suppression or similar disclosure controls. Precise semantics are
still draft, but the slot is reserved so profiles do not invent incompatible
extensions.

Example shape:

```json
{
  "disclosure_control": {
    "method": "small_cell_suppression",
    "min_cell_size": 5,
    "applied": true
  }
}
```

### Representation

A representation is one physical way to read one grain or bundle sidecar.

Recommended fields:

| Field                                | Meaning                                                                                                             |
| ------------------------------------ | ------------------------------------------------------------------------------------------------------------------- |
| `id`                                 | Stable within the artifact.                                                                                         |
| `grain`                              | Grain id, or omitted for bundle-level sidecars.                                                                     |
| `role`                               | `audit_stream`, `tabular_projection`, `analytics_scan`, `object_index`, `summary`, `sidecar`, or profile extension. |
| `format`                             | `ndjson`, `parquet`, `duckdb`, `arrow`, `json`, or profile extension.                                               |
| `media_type`                         | Concrete media type where useful.                                                                                   |
| `profile`                            | Neutral representation or producer profile id.                                                                      |
| `uri`                                | Relative or logical URI. No credentials or local implementation paths.                                              |
| `integrity`                          | Integrity proof for emitted bytes or structure.                                                                     |
| `row_count`                          | Physical row count when known.                                                                                      |
| `field_catalog_ref`                  | Required for queryable or renderable representations.                                                               |
| `read_path`                          | Scale and access semantics.                                                                                         |
| `protection_enforceable_granularity` | Single minimum-guarantee protection floor.                                                                          |
| `format_options`                     | Namespaced format-specific details.                                                                                 |

Read-path capabilities MUST be explicit, not inferred from format or engine
names:

Profile extensions for representation `role` and `format` use
profile-qualified tokens containing a `/`. Format-specific detail belongs under
`format_options` with a namespaced key.

```json
{
  "read_path": {
    "range_readable": true,
    "partitioned": true,
    "sharded": true,
    "appendable": false,
    "scan_capabilities": ["columnar_scan", "predicate_pushdown"],
    "read_path_granularity": "column",
    "gateable_unit_granularity": "column",
    "pushdown_withheld": ["restricted_key"],
    "sidecar_required": true,
    "physical_ordering": "manifest_order",
    "split_strategy": "manifest_list",
    "shard_ids_opaque": true
  },
  "protection_enforceable_granularity": "column"
}
```

Portable scan capabilities use abstract vocabulary such as:

- `sql_scan`
- `columnar_scan`
- `predicate_pushdown`
- `random_access`

Engine names may appear only as non-normative producer hints.

### Field Catalog

The field catalog is the semantic and operational field inventory for a grain or
queryable representation. It is mandatory for any queryable or renderable grain
or representation. It is optional for raw archival artifacts that are not meant
to be rendered or queried by a generic consumer.

Required responsibilities:

- define field names, types, nullability, and repeatability;
- describe semantic roles and display hints where safe;
- carry sensitivity and protection tags;
- state source/provenance expressions only when export-classed as safe;
- represent withheld metadata keys and withheld columns;
- map producer-native field metadata to neutral field semantics.

The catalog itself is export-classed content. A producer profile MUST be able to
withhold source expressions, descriptions, split metadata, and other metadata
keys independently from data columns.

When every source-structure field name is sensitive, a catalog MAY set
`fields` to an empty array and declare only a positive `withheld_field_count`.
That shape means the catalog is **fully withheld** under default-deny — it is
not an "empty grain" and does not claim zero fields of content.

`withheld_field_count` is the **total** number of withheld fields, including any
names listed in `withheld_fields`. When both are present, `withheld_field_count`
MUST be greater than or equal to the length of `withheld_fields`. That
consistency check is a validator/prose requirement; the structural schema does
not encode the count-versus-list arithmetic.

### Provenance And Accounting Links

The portable contract links to provenance and accounting sidecars; it does not
force every producer into one provenance schema.

Common link roles:

- run or job manifest;
- input ledger;
- output ledger;
- disposition or failure ledger;
- validation summary;
- guarded value-profile summary.

Provenance and accounting sidecars are metadata content and inherit export-gate
requirements. Source refs, job specs, query text, connection descriptors,
absolute paths, service endpoints, bucket or prefix locations, and credentials
MUST NOT be inlined in portable artifact instances. Use sanitized logical refs
or opaque handles resolved by the export gate.

When a derived artifact declares lineage from source artifacts, its
`default_export_class` and `protection_enforceable_granularity` MUST NOT be
looser than the most restrictive source it declares. A derived artifact cannot
launder a restricted source into a public export by dropping the constraint;
export class and protection floor propagate monotonically from source to
derived.

## Count Ownership

Use two count layers:

- **Grain count = semantic truth.** It answers how many logical rows or items the
  artifact says it contains.
- **Representation count = physical consistency check.** It answers whether a
  physical representation matches the grain for its projection or split
  semantics.

When both are present, validation checks consistency. For projections, filters,
withheld columns, or aggregate grains, the representation must declare whether
row count is expected to equal the grain count.

## Identity And Integrity

Identity and integrity are separate:

- `artifact_id` is identity. It may be a generated UUID URN or a stable logical
  URI when a catalog owns one. The contract does not require deterministic
  artifact ids.
- Integrity proves emitted bytes or structure. A rerun may be a new artifact
  with identical integrity proofs.

Supported integrity modes:

| Mode              | Use                                                |
| ----------------- | -------------------------------------------------- |
| `whole_digest`    | Digest over one emitted representation or sidecar. |
| `shard_digests`   | Digest per shard plus manifest-level binding.      |
| `hash_chain_head` | Head/root of an append-only hash chain.            |
| `merkle_root`     | Root of a tree proof.                              |

For restricted artifacts, integrity SHOULD be published at the coarsest
granularity that still proves the property. A hash-chain head or Merkle root can
prove integrity without publishing a per-record hash vector that would become a
membership or ordering oracle.

## Protection Model

The contract is a metadata-propagation standard. The primary leak surface is the
metadata that is designed to travel: artifact descriptors, field catalogs,
provenance links, accounting sidecars, split metadata, protection refs, and
diagnostics.

The contract supplies hooks; the export gate enforces. A conforming export/read
gate MUST:

1. default to `block_export` for unknown or elevated sensitivity;
2. validate field catalogs and protection-profile refs before rendering or
   packaging;
3. enforce at the representation's declared granularity;
4. refuse partial reads it cannot enforce;
5. resolve protection profiles out of band through opaque refs;
6. treat metadata objects as data subject to the same gate.

### Sensitivity And Protection Tags

Use two orthogonal axes:

| Axis              | Values                                                                                                                                                                 |
| ----------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `sensitivity`     | `public`, `internal`, `controlled`, `restricted`, `unknown`                                                                                                            |
| `protection_tags` | `direct_identifier`, `quasi_identifier`, `linkage_key`, `access_control_metadata`, `source_structure`, `measure`, `freeform_text`, `opaque_payload`, `safe_to_profile` |

Unknown sensitivity or unknown handling semantics MUST deny export unless an
explicit policy rule handles them safely.

Stable pseudo-identifiers that preserve joinability MUST be tagged as
`linkage_key` or `quasi_identifier`; tokenized does not mean safe.

### Default-Deny Export

The default action is `block_export`.

```yaml
protection:
  default_action: block_export
  default_export_class: public
  profile_ref: "profile:portable-example"
```

The permissive bundle declaration above is valid only for homogeneous structured
artifacts. A conforming gate still refuses content-opaque unknown fields:

```yaml
field:
  name: captured_payload
  sensitivity: unknown
  protection_tags: [opaque_payload]
  export_action: block_export
```

Bundle-level `default_export_class: public` or `internal` is allowed for
homogeneous structured artifacts. It MUST be refused by default when any
content-opaque column has `unknown` sensitivity. Content-opaque columns are
fields whose sensitivity is per-cell and not determinable from catalog metadata,
such as freeform text, opaque payloads, captured external content, or other
uninterpreted content. Lenient auto-downgrade to deny is an operator option, not
the normative default.

### Two-Tier Protection References

Portable artifact instances may carry rule shape, tags, action names,
granularity names, and opaque refs. They MUST NOT carry policy internals,
reversible maps, real data locations, authorization mechanics, credentials, or
inline connection descriptors.

The export gate resolves opaque refs to real policy values and authorization
mechanics from a controlled store.

### Metadata Is Content

These are export-classed content, not neutral structure:

- field descriptions;
- source expressions;
- producer metadata keys;
- free-text artifact labels;
- withheld-column names;
- split and partition boundaries;
- source refs and job specs;
- value-profile diagnostics;
- provenance and accounting links.

When the existence or name of a withheld field is sensitive, a representation or
field catalog must be able to disclose only a withheld count, not the field
name. A field catalog with `fields: []` and `withheld_field_count >= 1` is the
fully-withheld form of that posture.

Shard and partition ids MUST be opaque when boundaries are based on restricted,
linkage, or source-structure fields. Token-to-boundary mappings resolve out of
band.

Self-describing representation formats embed physical file metadata that is
also content. In boundary-crossing representations, per-column statistics such
as row-group or page min/max values and page-index bounds MUST be suppressed or
opaqued for fields whose sensitivity is `controlled` or `restricted`, or whose
protection tags include `direct_identifier`, `linkage_key`, or
`source_structure`. Membership-oracle structures, including per-column Bloom
filters, MUST be omitted for those fields. Producer profiles state how their
format satisfies this requirement; writer-side omission is preferred over
export-time rewrite.

## Read-Path And Protection Granularity

`protection_enforceable_granularity` is a single minimum-guarantee value. It is
the finest granularity at which a conforming gate is guaranteed to enforce
protection for the representation.

Common granularity values, coarse to fine:

```
artifact > shard > frame > row_group > column > row > cell
```

Profiles may define additional physical units if they map into this ordering.

The gate MUST refuse or downgrade reads finer than the declared floor.

For protected data, the declared floor must satisfy both constraints:

- it must not claim enforcement finer than the smallest independently gateable
  physical unit of the representation; and
- it must be at least as fine as any exposed protected read path.

If those constraints cannot both hold, the producer MUST isolate restricted
fields into a separately withheld representation, shard, sidecar, or projection.

Examples:

- NDJSON record streams are commonly gateable at `row` or `artifact`, not
  `column` or `cell`, unless the gate parses and enforces each record.
- Columnar shards may claim `column` only when column reads are independently
  gateable.
- Compressed streams cannot claim a floor finer than their independently
  addressable and gateable compression frame.

### Predicate Pushdown

Predicate pushdown is a separate inference channel. A consumer can use row
counts or existence tests to infer restricted values without reading the
protected column.

Validator rule:

For a representation `R` and field `F`, if:

- `R.read_path.scan_capabilities` includes `predicate_pushdown`; and
- `F.sensitivity` is `controlled` or `restricted`, or `F.protection_tags`
  includes `direct_identifier`, `linkage_key`, or `source_structure`;

then `R` MUST satisfy one of:

- `F.name` is listed in `R.read_path.pushdown_withheld`; or
- `R.protection_enforceable_granularity` is `row` or `cell`, and the producer
  profile declares that predicates on `F` are evaluated behind the protection
  boundary with small-cell suppression.

Otherwise the representation is invalid.

## Guarded Value Profiles

A `value_profile` is an optional diagnostic. If emitted, it is governed by this
contract and ships guarded or not at all.

Concrete value enumeration is allowed only when all conditions hold:

1. the field is explicitly tagged `safe_to_profile`;
2. sensitivity is `public` or `internal`;
3. distinct count is below the producer profile's cap.

All other fields emit aggregates only:

- count and null count;
- capped distinct count;
- coarse type/shape class;
- numeric range/histogram only when the measure satisfies the full Tier-A gate:
  `safe_to_profile` and `public` or `internal`.

For `source_structure` or `direct_identifier` string fields, shape class MUST
collapse to `opaque_string`. It MUST NOT reveal path templates, prefix patterns,
segment regexes, or other reconstructive structure.

For `quasi_identifier` or `linkage_key` fields, small-cell aggregates below the
profile threshold MUST be suppressed.

A conforming gate MUST verify that small-cell suppression was actually applied
by checking the emitted aggregate against the declared threshold. It MUST NOT
accept a producer's self-asserted "suppression applied" flag as evidence. A
self-claim that cannot be verified against the emitted values fails closed.

## Relationship To Forge Infrastructure

`contract: forge-infra/v0` governs whether a provider can host, produce, or
deliver an artifact and which authority may perform that operation. This
contract governs the published bag of bytes, representations, provenance, and
protection. Forge object references and event payload references do not replace
a data-artifact descriptor.

## Composition With Immutable Snapshots

The proposed [segmented-snapshot companion](segmented-snapshot-contract.md)
describes exact immutable manifest/member bindings, publication and verification
semantics. It reuses this contract's artifact, grain, representation, catalog and
protection model. It does not change this contract's fields or require existing
producers to adopt a storage protocol.

Read capabilities describe the offered access path; they do not imply independent
verification of the entire dataset. A sharded or appendable representation does
not, by those flags alone, promise atomic publication, durable append
acknowledgement, replay or reader pinning. Those guarantees need their own
profile/contract and evidence.

An internal full-fidelity manifest and an export representation can have different
protection requirements. Adding a portable descriptor cannot make unsafe native
metadata safe to export. A boundary representation remains subject to all the
metadata, granularity and predicate-protection rules above.

## Validation Requirements

A validator MUST check:

- descriptor has `capabilities` containing the host-less
  `contract: data-artifact/v0` token;
- queryable/renderable grains and representations have field catalogs;
- raw archival grains that are not queryable or renderable MAY omit
  `field_catalog_ref`;
- a field catalog with empty `fields` MUST declare
  `withheld_field_count >= 1` and MUST be treated as fully withheld under
  default-deny (not as an empty grain);
- when both `withheld_field_count` and `withheld_fields` are present,
  `withheld_field_count` MUST be greater than or equal to the length of
  `withheld_fields`;
- grain counts and representation counts are consistent where declared;
- lifecycle state is compatible with export/read intent;
- protection refs are opaque and present when needed;
- unknown sensitivity and unknown protection tags fail closed;
- bundle-level default export does not bypass content-opaque unknown fields;
- read-path granularity, gateable physical unit, and protection floor are
  consistent;
- predicate-pushdown rule passes;
- restricted split boundaries and shard ids are opaque;
- value profiles obey enumeration, shape, numeric range, and small-cell guards;
- provenance/accounting refs are sanitized logical refs or opaque handles;
- derived-artifact export class and protection floor are not looser than any
  declared source (no lineage laundering);
- small-cell suppression is verified against emitted aggregates, not accepted
  from a producer self-asserted flag;
- columnar boundary-crossing representations declare physical metadata
  suppression for restricted-class columns, including column statistics and
  membership-oracle filter structures.

## Review-Loop Items Before Freeze

Resolved for `v0` — conservative invariant folded, full schema/profile reserved
for a later minor:

- lineage export-floor: a derived artifact's export class and protection floor
  are bounded by its most restrictive declared source (folded into Provenance
  And Accounting Links); the full structured lineage-ref object is reserved.
- aggregation disclosure control: small-cell suppression is gate-verified, not
  producer-asserted (folded into Guarded Value Profiles); exact k-anonymity
  threshold semantics for aggregation grains are reserved.
- compression-frame floor: the protection floor may not be finer than the
  independently addressable and gateable compression frame (Read-Path And
  Protection Granularity); the profile-specific mapping for a concrete
  compression format is reserved to a companion profile.
- physical file metadata: column statistics and membership-oracle filter
  structures in self-describing formats are treated as content (folded into
  Metadata Is Content); format-specific suppression mechanics are producer
  profile details.

Satisfied:

- consumer witness demonstrating fail-closed behavior for an unknown-sensitivity
  field in a gate the consumer did not author.
