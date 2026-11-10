# Segmented snapshot v0 semantic validation

Semantic layer: **`segmented-snapshot/v0-semantics/0.1.0`**.

These rules apply after structural validation. A familiar profile name is not a
trust decision: admission supplies the exact trusted profile binding and the
requested namespace/snapshot/artifact selection. An alias resolver must produce
that immutable selection once, before reading. The fixture request context is a
test harness input, not another public wire message.

## Rules

| Rule    | Required check                                                                                                                                                                                                                       |
| ------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| SEM-S00 | Reject duplicate JSON object members before interpreting metadata.                                                                                                                                                                   |
| SEM-S01 | Caller selection, publication, manifest, descriptor, profile, schema/catalog and verification-result identities/bindings agree exactly.                                                                                              |
| SEM-S02 | Resolve each required reference through authorized bindings; length and SHA-256 cover the exact bytes consumed. Missing or changed required bytes refuse.                                                                            |
| SEM-S03 | The flat graph is exhaustive and self-contained; ids/refs are unambiguous, data members do not alias required metadata, and row-schema references are fragment-local.                                                                |
| SEM-S04 | Member counts sum exactly to the manifest and representation count; the declared relation to the grain count holds and arithmetic stays within the safe-integer range. Profile decoding verifies physical row counts.                |
| SEM-S05 | A published descriptor is `complete` or `partial`; the selected representation is immutable (`appendable: false`). A publication record alone does not prove persistence.                                                            |
| SEM-S06 | Coverage binds the selected immutable namespace/snapshot, artifact and grain; reused logical artifact ids do not collapse snapshot identity.                                                                                         |
| SEM-S07 | The admitted exact profile supports the requested operation, count/order model and empty-snapshot case. Unsupported is never empty success.                                                                                          |
| SEM-S08 | Member count, metadata bytes and per-member byte size fit the declared profile and hard limits; exceeding them is capacity refusal.                                                                                                  |
| SEM-S09 | Verification scope matches the admitted request. A successful full result covers all members; a successful selective result covers exactly the selected members. A failed result may report only an actually verified prefix/subset. |
| SEM-S10 | Provisional output is allowed only under the declared provisional-streaming profile, and is never terminal verified success.                                                                                                         |

### Exact references and identity

`binding` contains identity, logical reference, SHA-256 and byte length.
`schema_binding` uses `schema_id` instead of `id`; the resolved row-schema `$id`
must match. This version admits Draft 2020-12 standalone row schemas with static
fragment-local references that resolve to actual schema nodes. Nested `$id` and
dynamic/recursive reference keywords are unsupported. Annotation payloads such
as `examples` are data, not schema locations. SHA-256 input is the complete emitted file bytes, including any
newline and whitespace. Verification MUST NOT canonicalize JSON first.
Deterministic serialization before emission is allowed. Semantic comparison
digests are different claims and cannot satisfy these physical pins.

Opaque `artifact:<token>` references are handles, not URLs, filesystem paths or
authorization. Resolution, custody and permissions belong to the adopting
boundary. No credentials, endpoints or inline resolver configuration travel in
the records. Readers apply hard metadata budgets while reading, before parsing
or allocating based on untrusted declarations; advertised lengths are not trusted
allocation instructions.

The selected profile is the exact binding admitted by the caller, not merely any
profile the producer includes. The profile specification must account for every
required read dependency. The flat wire version has no external-parent row-reconstruction or paging
edge; such requirements cause explicit refusal. Unknown required capabilities
must be refused rather than treated as optional hints.

The publication's manifest binding id equals `snapshot_id`; descriptor binding id
equals `artifact_id`; profile binding id equals `profile_id`. The manifest's
profile binding equals the publication's. Descriptor grain/representation ids
must be unambiguous, and the selected representation names the selected grain,
profile, manifest URI and field-catalog reference. Catalog `id` equals the
manifest's field-catalog binding id; its resolver `ref` may differ. Descriptor
catalog references name that identity, resolved through the manifest binding.
Catalog grain equals the selected grain.
Any embedded catalog with that id must agree with the separately bound catalog.

A present `profile_metadata` binding is required on every admitted read. Verify
its exact bytes and profile-defined identity/selection and dependencies; refuse
unknown required semantics rather than treating a valid digest as semantic proof.
Its dependencies must be exhaustive and acyclic and remain within the flat,
self-contained snapshot boundary. The synthetic metadata variant in
`fixtures/profile-metadata/` has no nested dependencies and checks this boundary
without claiming a production index profile. Because that variant admits no
dependencies at all, its `unknown-dependency`, `undeclared-transitive-dependency`
and `metadata-self-cycle` cases are refused by the closed metadata shape, not by
dependency enumeration or cycle detection. They are not evidence of a cycle
walker; a profile that admits nested dependencies needs its own enumeration and
cycle controls.

The artifact descriptor can describe other representations. Verifying the selected
manifest does not certify those other representations.

### Counts and limits

Counts and lengths are nonnegative integers at most 9,007,199,254,740,991. Require
both grain and representation row counts for this companion even though the base
data-artifact schema permits omission. `equal` requires physical count equal to
the grain count; `subset` permits a smaller physical count. It never means source
enumeration completeness. Aggregate results belong to a separate aggregate grain.

Profile limits mean:

- `max_members`: maximum data-member descriptors (hard maximum 65,536).
- `max_metadata_bytes`: sum of distinct required metadata byte artifacts used by
  admission/verification, including publication, manifest, profile/spec,
  descriptor, row schema, catalog, optional profile metadata and its transitive
  dependencies, optional retention spec and attached coverage
  attestation (hard maximum 67,108,864).
- `max_member_bytes`: maximum bytes of each individual data member.

Consumers may set stricter local limits and must refuse capacity honestly. These
metadata limits do not promise bounded Parquet decoder memory, row-level lookup
costs or a producer's whole-run working set.

### Immutable coverage subject

The first wire version uses the existing coverage contract's `subject_uri` plus
`artifact_id` and `grain_id`. All three must be present and match. Compute:

```text
subject_bytes = ASCII("segmented-snapshot/v0/subject") || 0x00
             || ASCII(namespace_id) || 0x00 || ASCII(snapshot_id)
coverage_subject_uri = "snapshot:" || lowercase_hex(SHA256(subject_bytes))
```

Namespace/snapshot ids use the ASCII, NUL-free schema vocabulary. This is a
domain-separated identity derivation, not a content-integrity digest, signature
or anonymization claim. A conforming producer never changes the graph behind an
admitted namespace/snapshot binding. The fixture vector `example-store` /
`snapshot-1` yields
`snapshot:66b50835bfb9276ed12d774aeceee04c8bd289c8868a798c78b42615f17c62a4`.

The manifest contains that derived URI. An attached attestation is itself
byte-bound, and its subject must match the manifest's URI/artifact/grain. Acceptance
of an attestation issuer or method, scope sufficiency and any destructive-action
authorization remain the coverage consumer's responsibilities. A correct binding
does not turn partial coverage into complete coverage.

### Read and result scope

The result applies after a snapshot was admitted. Pre-admission malformed input,
authorization or resolution errors use the caller's typed error surface; do not
fabricate a publication binding to fit the result schema.

`full_snapshot` has an empty `requested_member_ids` list (meaning all declared
members). `selected_members` has a nonempty explicit list. `full_scan` and
`full_verify` require full scope; `selective_verify` requires selective scope.
Other supported operations must preserve their exact admitted member scope.
Scope names do not imply query-to-input binding or certify an unrecorded query.

`verified_member_ids` is unique and contained in the requested scope. On
`outcome: verified` it equals the full requested set. On a failure it can contain
only bytes successfully checked before that failure. `output_state` describes
whether data was emitted; `none` is valid for a verification-only operation.
Provisional output requires the matching profile and a terminal non-success
result if verification never completed. Consumers needing all-or-nothing output
must stage it or choose a verified-before-emission profile.

The normative rule covers same-bytes verification/use and truthful terminal
reporting. The fixture resolver is immutable in-memory bytes; it does not prove
a real filesystem reader maintains custody or that a producer synced its files.

## Corpus and evidence boundaries

`fixtures/base/` is a fully digest-bound positive graph, including a partial
descriptor that was completely published. Every case in `fixtures/cases.json`
starts from it. Semantic mutations use `rebind: true` when the test must emit a
new consistent physical graph so that a semantic failure is not masked by an
incidental checksum mismatch. Physical-byte cases never rebind. Golden files are
never repaired by the test runner.

For semantic negatives, changed structured records must still pass their schemas
and the exact expected semantic rule must fail. Structural negatives are checked
against an already-passing baseline schema. Tool setup or schema-resolution
failure is a harness failure, not a successful rejection.

The corpus checks declaration/binding semantics. Runtime evidence still covers
alias races, same-open bytes, crash/power-loss behavior, stale derived indexes,
concurrent reference admission and safe reclamation. Physical retention and
producer-format decoders are not implemented by this repository. The conformance
case catalog must keep those obligations visibly separate from executed controls.
