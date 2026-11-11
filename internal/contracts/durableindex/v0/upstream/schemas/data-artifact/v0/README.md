# Data Artifact Schemas v0

Machine-readable companion schemas for the portable data artifact contract.

The normative standard is
[`docs/standards/data-artifact-contract.md`](../../../docs/standards/data-artifact-contract.md).
These schemas provide a structural validation surface for consumers. Behavioral
rules such as default-deny export, predicate-pushdown refusal, and protection
floor checks are still enforced by the consumer/export gate.

The contract identity is the opaque capability token
`contract: data-artifact/v0`. Consumers resolve that token through local
configuration, a vendored copy, or another trusted registry. Artifact instances
must not embed a schema host as their identity.

The L2 contract entry point is `contract.json`. Consumers resolve the capability
to that manifest, verify its `capability`, and load the relative `entry_schema`.
Resolution fails closed when the manifest is missing, the capability does not
match, or the entry schema is missing. Direct `$id` lookup remains valid for
schema-aware tooling, but it is not the contract-entry mechanism.

Golden descriptors in [`examples/`](examples/) exercise common shapes, including
fully-withheld field catalogs (`fields: []` with a positive
`withheld_field_count`) and raw archival grains that omit `field_catalog_ref`.
