---
id: "ADR-0004"
title: "Coverage Attestation as a Companion Portable Contract"
status: "proposed"
date: "2026-07-02"
last_updated: "2026-08-02"
deciders:
  - "@3leapsdave"
scope: "Crucible foundation / data contracts"
tags:
  - "schemas"
  - "data-contracts"
  - "coverage"
  - "completeness"
  - "interchange-contract"
relates-to:
  - "crucible ADR-0001 (schema/config versioning — the promotion gate)"
  - "docs/standards/data-artifact-contract.md (data-artifact/v0 — the subject linkage)"
---

# ADR-0004: Coverage Attestation as a Companion Portable Contract

## Status

**Current Status**: Proposed — enters the data-artifact/v0 review loop on its
own ratification gate. It does **not** block the data-artifact/v0 freeze.

## Context

The portable data artifact contract deliberately makes `lifecycle`
(`complete` / `partial`) a **producer self-claim**: the producer's statement
about its own run. Operational experience at scale shows consumers need a
second, independent kind of claim — _what does this artifact actually cover,
per scope, as verified, as of when_ — with properties the descriptor cannot
and should not carry:

1. **Different emitter.** Coverage is frequently assessed by a party other
   than the producer — a reconciler, a mirror synchronizer, a verification
   gate. Binding the assessment into the producer-authored descriptor
   conflates the roles the contract review worked to separate (producer vs
   consumer-witness).
2. **Different lifecycle.** Late-arriving data makes re-attestation routine.
   A descriptor is republished when the artifact changes; a coverage claim is
   superseded when the _assessment_ changes. Coupling them forces descriptor
   churn on every reconcile.
3. **Destructive consumers exist today.** Mirror-synchronization tooling
   already gates orphan-deletion on confirmed-versus-inferred coverage. A
   claim that consumers delete data over needs a schema, a validation
   surface, and fail-closed vocabulary a prose standard alone cannot provide.
4. **Presence is not coverage.** A binary present/absent attestation has been
   observed to pass while a scope suffered a material volume deficit
   (truncated enumeration leaving a trickle where full data belonged).
   The claim vocabulary must carry quantitative volume, not just presence.

This is the same separation the contract already draws between identity and
integrity, extended to completeness: **identity ≠ integrity ≠ completeness.**

## Decision

Establish `contract: coverage-attestation/v0` as a **companion portable
contract family** in crucible:

- `schemas/coverage-attestation/v0/` — structural schema + example
  (skeleton lands with this ADR; hardened in the review loop).
- Subject linkage by `artifact_id` (preferred) to a data-artifact/v0
  descriptor, with `subject_uri` for non-descriptor subjects during adoption.
- Same L2 identity model as data-artifact/v0: host-less capability token,
  resolution through a trusted `contract.json` entry manifest.
- A single clarifying sentence in the data-artifact standard's Lifecycle
  section noting that lifecycle is a self-claim and independent completeness
  claims arrive via this companion contract. That sentence is the only
  freeze-coupled element of this decision.

Core vocabulary (normative intent, hardened in review):

- Per-scope **claims** with `basis: confirmed | inferred` and
  `method: enumerated | reconciled | derived | declared`.
- **Volume** (`unit` / `observed` / `expected`) alongside presence.
- **Machine-readable gaps** with a stable producer-profile `code`; prose notes
  are explanation, never the reason identity.
- **Fail-open-but-honest**: missing attestation never blocks and is never a
  claim; malformed attestation fails loud; `unknown` is an honest verdict.
- **One emitter per subject; reconcile, don't re-derive.**
- **Supersession** (`as_of` + `supersedes`) as the routine late-data path.
- **Protection**: attestations inherit the most-restrictive export class of
  their subject and MUST carry a default block-export posture. Scope boundary
  values are export-classed content; publish raw boundary values only when the
  effective export class permits it, otherwise publish opaque scope tokens for
  boundary-crossing renders (consistent with the shard-boundary finding in the
  data-artifact review).

## Consequences

- Consumers gain a portable gate input for destructive synchronization
  decisions and for trusting plan-time set-difference operations between
  source and target indexes.
- Producers/verifiers gain one claim shape across artifact families (indexes,
  record streams, aggregations) instead of per-tool completeness dialects.
- One more family to carry through ratification — mitigated by riding the
  already-convened review loop and by arriving with a live producer, a live
  destructive consumer, and a motivating failure mode already in hand.

## Open questions for the review loop

1. Scope vocabulary alignment with representation partitioning (partition
   spec vs window vs prefix) — one shared `$defs` or per-family extension?
2. Should `coverage_state` be independently assertable or derivable-only from
   claims + gaps? Current stance: assertable, but consumers may recompute and
   fail loud on inconsistency.
3. Opaque scope-token mechanics for boundary-crossing renders — shared with the
   data-artifact shard-boundary resolution rather than invented twice.
4. Whether an attestation over a `restricted` subject may itself be exported
   with claims-only content (counts, windows). Current stance: no downgrade
   from subject export class until the lineage-laundering item already carried
   in the review loop is resolved.
