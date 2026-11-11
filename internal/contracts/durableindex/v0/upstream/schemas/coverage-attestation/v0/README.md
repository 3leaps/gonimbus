# Coverage Attestation Schemas v0 (PROPOSED)

Machine-readable skeleton for the proposed companion contract
`contract: coverage-attestation/v0`. Status: **proposed** — rides the
data-artifact review loop on its own ratification gate; it does **not** block
the `data-artifact/v0` freeze. Decision record:
[`docs/decisions/ADR-0004-coverage-attestation-contract.md`](../../../docs/decisions/ADR-0004-coverage-attestation-contract.md).

## What it is

An independent, supersedable claim about **what an artifact or scoped subject
actually covers** — the completeness twin of the data-artifact descriptor.
The descriptor's `lifecycle` (`complete`/`partial`) is the **producer's
self-claim** about its own output; a coverage attestation is an **assessment**
(often by a non-producer: a reconciler, a mirror, a verification gate) of what
the subject covers, per scope, as of a point in time. Identity ≠ completeness,
the same way the contract already separates identity ≠ integrity.

## Design principles (carried from operational experience)

- **Confirmed vs inferred, per claim.** Consumers making destructive decisions
  (e.g. orphan-delete during mirror synchronization) MUST be able to gate on
  `basis: confirmed` only.
- **Volume, not just presence.** A scope can be present yet volume-deficient
  (truncated enumeration leaving a trickle where full data belongs). Claims
  carry non-negative quantitative `volume` where known; binary present/absent
  attestation has been observed to pass while a scope was materially short.
- **Machine-readable gaps.** Every uncovered scope carries a stable `code`
  from the emitting profile's open vocabulary. Human `note` text may explain
  the condition but is never the machine contract.
- **Fail-open-but-honest.** A missing attestation never blocks by itself and is
  never treated as a claim; a malformed one fails loud. `coverage_state:
unknown` is an honest verdict, not a default.
- **One emitter per subject; reconcile, don't re-derive.** Downstream parties
  reconcile against the designated emitter's attestation rather than
  re-deriving competing coverage claims.
- **Supersession is routine.** Late-arriving data makes re-attestation normal
  (`as_of` + `supersedes`), not an error path.

## Protection

Attestations inherit the **most-restrictive export class of their subject** and
start from `block_export`. Scope boundary values — partition values, key
prefixes — are export-classed content in their own right (they can reconstruct
source namespace structure). Publish raw boundary values only when the
effective export class permits it; boundary-crossing renders use opaque scope
tokens, consistent with the shard-boundary finding in the data-artifact review.

## Contract identity

The contract identity is the opaque capability token
`contract: coverage-attestation/v0` (same L2 resolution model as
`data-artifact/v0`: local configuration, vendored copy, or trusted registry —
never an embedded schema host).

The L2 contract entry point is `contract.json`. Consumers resolve the capability
to that manifest, verify its `capability`, and load the relative `entry_schema`.
Resolution fails closed when the manifest is missing, the capability does not
match, or the entry schema is missing. Direct `$id` lookup remains valid for
schema-aware tooling, but it is not the contract-entry mechanism.

## Files

- `contract.json` — contract capability manifest and entry schema pointer.
- `coverage-attestation.schema.json` — structural validation surface.
- `coverage-attestation.example.json` — an independent reconciler attesting a
  partially-covered object-index subject: one confirmed-enumerated scope with
  volume tie-out, one inferred scope, one honest gap.
- `scripts/test-coverage-attestation-controls.sh` — negative controls proving
  code-less gaps and negative observed/expected volumes fail validation.
