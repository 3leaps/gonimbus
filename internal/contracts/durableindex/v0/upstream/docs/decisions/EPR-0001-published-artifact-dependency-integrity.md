---
id: "EPR-0001"
title: "Published artifacts carry a pinned, enforced, audited, parity-checked dependency graph"
status: "accepted"
date: "2026-07-17"
last_updated: "2026-07-22"
deciders:
  - "@3leapsdave"
  - "cxotech"
  - "entarch"
scope: "Crucible foundation / shared governance — durable engineering principle"
tags:
  - "principles"
  - "supply-chain"
  - "build"
  - "release"
  - "governance"
relates-to:
  - "crucible ADR-0003 (the *DR taxonomy; this is the inaugural EPR)"
  - "crucible ADR-0002 (single canonical source; adopters link-or-vendor)"
  - "crucible PDR-0001 (sibling governance record; domain standard-set ingestion)"
---

# EPR-0001: Published Artifacts Carry an Integral Dependency Graph

## Status

**Accepted.** Inaugural Engineering Principle Record. Proposed by entarch;
scoped, amended, and brought forward by cxotech. Graduated to accepted once the
first adopter landed conforming pin/enforce/audit/parity work — pin, enforce, and
parity demonstrated by executable negative controls, audit by on-change and
scheduled advisory scans (see
[Reference implementation](#reference-implementation)).

## Context

A repository increasingly ships the **same core through multiple distribution
surfaces**: a CLI binary, bindings for one or more host languages, prebuilt
platform libraries. Each surface is built from its own resolved dependency graph,
and those graphs frequently share security-critical components.

When a resolved graph is a **build input to a published artifact** and is not
pinned in-repo, three failure modes become available. They are stated here as
hazards of the shape, not as history:

1. **Drift.** The graph is resolved fresh at each build and may change between
   builds within permitted version ranges. The artifact is not reproducible: the
   thing that shipped cannot be rebuilt with confidence, and "what was in the
   release?" has no stable answer.

2. **Blind spots.** A reviewer cannot gate what never appears in a diff. An
   unpinned graph presents nothing to review and nothing stable for an advisory
   scanner to scan, so a dependency can carry a known vulnerability through a
   release without any point in the process where a human could have seen it.

3. **Cross-surface divergence.** Consider a repository publishing a CLI binary and
   a native language addon over a shared cryptographic core. If each surface
   resolves independently, a single tagged release can ship **different versions
   of the same security-critical library** through different surfaces. Nothing in
   the review record would reveal it, and the versions would agree or disagree
   purely by the accident of when each was last resolved. "One core, many
   bindings" quietly becomes "several cores" — while every surface still claims
   the same version of the product.

What these share: the artifact's true composition becomes **unobservable**, and
integrity claims about a release degrade from verified to assumed. The hazard
attaches to the _shape_ — any repository with more than one published surface —
not to any particular language, ecosystem, or codebase. That is why the rule
belongs in the shared foundation rather than being re-derived, differently and
partially, in each repository that encounters it.

The standing tension this record arbitrates: **build convenience and low churn**
(float the graph, one fewer artifact to refresh, fewer CI steps) **versus
reproducibility, auditability, and cross-surface integrity**. For anything we
_publish_, integrity wins, and the recurring cost is accepted as its price.

## Principle

> **Every artifact we publish that is built from a resolved dependency graph MUST
> ship that graph pinned in-repo, enforced at build, continuously audited, and
> held at parity across all distribution surfaces of the same release.**

Four obligations, each durable and surface/language-agnostic:

### 1. Pin

Every surface that produces a **distributable artifact** commits its resolved
graph in-repo — a tracked lockfile, or the built artifact itself where that is
the shipped unit. No published surface resolves its graph only ephemerally.

_Exemption:_ a **pure library published as source for downstream resolution**
(consumers own the resolution) does not ship a resolved graph and is out of
scope. The obligation attaches to **published artifacts**, not to libraries whose
consumers pin.

### 2. Enforce

The build that produces the shipped artifact consumes the pin **authoritatively**
— the ecosystem's locked/frozen mode or equivalent — so the committed pin is
load-bearing rather than decorative. **A pin the release build can silently
ignore does not satisfy this record.** An unenforced pin is worse than none: it
presents the appearance of control while permitting the drift it appears to
prevent.

### 3. Audit

Every pinned graph is gated for known advisories **on every change and on a
schedule** (cadence per-repo), across **all** pinned surfaces, not only the
primary one. Change-triggered scanning alone has a blind spot this obligation
exists to close: advisories arrive against unchanged pins, and a repository
that is quiet for a quarter is otherwise never re-scanned. An accepted residual
risk is recorded as a dated, revisit-conditioned security decision record —
never a silent ignore. An unaudited secondary surface is a supply-chain path
that no one is watching.

### 4. Parity

When one release ships the same core through multiple surfaces, the
**security-critical shared components** MUST resolve to **identical upstream
source versions across every surface's pin**. Divergence is a **build
failure**, not a warning. "One core, many bindings" means one graph.

Two load-bearing qualifiers:

- **The parity set is a declared, reviewed artifact.** "Security-critical" is a
  judgment, but parity is a machine gate, and a machine gate needs a
  machine-readable input. Each adopting repository commits an explicit parity
  manifest — the list of shared components the check asserts over — and changes
  to that manifest are reviewed like the pins themselves. Without the declared
  set, the check either asserts over the full graph (impossible across
  platforms, whose surfaces legitimately differ in transitive dependencies) or
  over an ad-hoc subset (a blind spot that looks like coverage).
- **"Identical upstream source versions", not identical version strings.** The
  same upstream component may appear at different packaging granularity per
  surface (a crate, an FFI prebuild, a vendored copy); the parity claim is
  about the resolved upstream source, which is the thing whose divergence the
  hazard describes.

## Consequences

**Makes easier**

- Reproducible published artifacts: the graph that shipped is the graph in the tag.
- Security review gates the _actual_ shipped graph, on every surface, on every change.
- A single auditable answer to "what is in the released artifacts?" — available on
  demand rather than reconstructed under pressure during an incident.
- A new binding surface inherits a pre-stated bar instead of re-litigating it, and
  the answer does not depend on who reviews it.

**Makes harder / costs (accepted)**

- Each published surface adds a pin to refresh. Dependency changes must refresh
  **every** surface's pin together, as one reviewed set, so they cannot drift apart.
- CI gains per-surface locked builds, per-surface advisory scans, and a
  cross-surface parity assertion. This is standing cost, deliberately accepted.
- Parity as a **hard failure** will occasionally block a release for a divergence
  that is benign in fact. Accepted: a gate that yields under time pressure is not
  a gate, and the cost of triaging a false block is bounded while the cost of a
  silent divergence is not.

**How it is checked (per adopting repository)**

Mechanics are a **per-repo** concern — a process record, runbook, or task. This
EPR fixes the **obligations, not the tooling**: which locked-mode flag, which
advisory scanner, and how the parity assertion is expressed are all local
choices, and are expected to change over time without disturbing this record.

One checking requirement is itself an obligation: conformance is demonstrated
with a **negative control** — the adopter shows the locked build _fails_ when
the pin is violated (a deliberately stale lockfile, a deliberately divergent
parity entry), not merely that the enforcing flag is present in CI
configuration. An enforcement mechanism that has never been seen to fail is
configured, not proven — the gap between the two is exactly the
false-evidence class obligation 2 exists to close.

## Adoption & propagation

Per ADR-0002/0003, this principle is **canonical upstream in crucible**. Adopting
repositories **link or vendor** it and record local conformance; they do not fork
divergent copies. The agreement lives upstream.

It binds any repository that publishes artifacts built from a resolved dependency
graph. Repositories shipping multiple surfaces over a shared native core are the
primary adopters — for example `seclusor`, `ipcprims`, `sysprims`. **That list is
illustrative and non-exhaustive**, offered to convey the shape rather than to
bound the net: the principle attaches to any qualifying repository whether or not
it appears here.

### Reference implementation

`seclusor` is the first conforming adopter and serves as the reference
implementation. It ships a shared cryptographic core through two published
surfaces — a CLI binary and a native language addon — the exact "one core, many
bindings" shape obligation 4 governs, and it discharges all four obligations:

- **Pin** — each surface commits its resolved graph in-repo (a tracked lockfile
  per surface, including the addon's own lock).
- **Enforce** — the artifact builds consume the pins authoritatively
  (locked-mode), with a guard proving no published-artifact build path can drop
  the locked flag.
- **Audit** — every pinned surface is scanned for advisories on change and on a
  schedule.
- **Parity** — a declared, reviewed parity manifest asserts the security-critical
  shared components resolve to an identical upstream identity across both
  surfaces; divergence fails the build.

Per the checking obligation above, pin, enforce, and parity are each demonstrated
by an **executable negative control** — the gate is shown to _fail_ on a violated
or stale pin, a bypassing build path, and a divergent, absent, or undeclared
parity entry — not merely asserted present in CI. Audit is demonstrated by
on-change and scheduled advisory scans of every committed lockfile.

Conformance work (public):

- Merged PR — <https://github.com/3leaps/seclusor/pull/43>
- Merge commit — <https://github.com/3leaps/seclusor/commit/d7b3c0cb44841520d0349e9562e1c84d822e2608>

Mechanics (which locked-mode flag, which scanner, how the parity assertion is
expressed) are the adopter's per-repo concern per "How it is checked" above and
are expected to evolve without disturbing this record.

## Not this record (one principle per record)

- **Which** advisories a repository accepts, and until when → that repository's
  **SecDR**.
- **How** a repository wires locked builds, scanners, and the parity check → that
  repository's **PDR**, runbook, or task.
- **Whether** to adopt a given major dependency bump → an **ADR/DDR** in that
  repository.
- Signing, provenance, and SBOM of published artifacts → adjacent supply-chain
  records; complementary to, but distinct from, this graph-integrity principle.

## Rationale for the record type

**EPR, not PDR.** By the two axes in ADR-0003: the **domain** is engineering
practice — how artifacts are built and published — which is `PDR/EPR` territory;
the **lifecycle** is durable. The litmus ("would a reasonable engineer expect this
to still hold in a year, unchanged?") is satisfied, and the tell is that the
tooling is deliberately out of scope: strip the flags and scanners and the four
obligations survive any ecosystem change. A PDR is a choice a later choice
revises; "stop pinning what we publish" would not be a process revision but a
reversal of principle.

**Not a security-domain record.** Supply-chain integrity invites reading this as
`SecDR`, which under ADR-0003 §6 would argue for a domain record marked
principle-lifecycle instead of an EPR. It does not hold: `SecDR` captures _threat
accepted, mitigation chosen_ — inherently dated, revisable, and per-repository —
and that slice is explicitly delegated above. This record binds a repository
publishing an artifact with no security-critical dependency at all. Security is
the **motivation**; the **domain** is build-and-release practice.

**Consistent with PDR-0001.** PDR-0001 ratified a 28-principle corpus and is
correctly a PDR: its decision content is the _ingestion act_ — admit this set, at
this altitude, opt-in — which a later decision could revise, while the principles
themselves live under `docs/standards/`. Here there is no corpus and nothing to
ingest: **the record is the principle.** That is EPR's exact purpose, and this is
its inaugural use.

## References

- [ADR-0003: Decision & Governance Record Taxonomy](ADR-0003-decision-record-taxonomy.md) — defines EPR; this is the first one
- [ADR-0002: Key-Material Fingerprint Contract as a Portable Schema](ADR-0002-keymaterial-fingerprint-portable-contract.md) — single-canonical-source precedent
- [PDR-0001: Adopt the Data-Pipeline Engineering Principles](PDR-0001-adopt-data-pipeline-principles.md) — sibling governance record
- [Decision & Governance Records — the `*DR` family](../repository/decision-records.md) — the normative catalog

## Revision History

| Date       | Status Change | Summary                                                                                                                                | Updated By |
| ---------- | ------------- | -------------------------------------------------------------------------------------------------------------------------------------- | ---------- |
| 2026-07-17 | → proposed    | Inaugural EPR; pin/enforce/audit/parity obligations                                                                                    | cxotech    |
| 2026-07-22 | → accepted    | First adopter landed conforming work (pin/enforce/parity negative controls; audit by scheduled scans); reference implementation linked | entarch    |
