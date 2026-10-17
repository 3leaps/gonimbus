# ADR-0009: Discovered-Segments Build Scope

**Status:** Proposed (stub; design not accepted)
**Date:** 2026-10-17
**Decision Makers:** @3leapsdave

## Context

`build.scope` compiles to an explicit LIST prefix plan before a crawl
(`pkg/scope`). Three scope types exist: `prefix_list`, `date_partitions` and
`union`. `date_partitions` already discovers path segments at build time with
delimiter listing (`discover.segments`) and then appends a date-formatted
suffix. The index-set identity hashes the scope configuration, not the
discovered values, so new partition values do not change identity.

Durable builds accept only the default match include. Partitioned layouts often
need a wildcard in a middle path segment followed by a literal tail, for
example `tenant=*/table=orders/**`. When the tail is a date,
`date_partitions` expresses the shape. When the tail is a non-date literal, no
scope type expresses it, and the remaining options are a hand-written
`prefix_list` (a snapshot of the values at authoring time) or the SQLite
format.

`IndexScopeConfig` reserves `constraints` and `depth_mode` for a `segments`
scope type. No design for that type is on record.

## Decision (proposed)

Define a `segments` scope type that fills the reserved slot:

- A template of path segments, each either a literal or a whole-segment glob,
  discovered left to right with one delimiter listing per discovered level,
  ending in a terminal prefix.
- Shared compiler internals with `date_partitions`. `date_partitions`
  validation, hashing and identity behavior stay unchanged.
- The identity hash covers the template, not discovered values.

Bounds the design must state before acceptance:

- **Grammar:** whole-segment globs only; no recursive (`**`) segment inside the
  template; no partial-segment wildcards that cannot be resolved by delimiter
  listing.
- **Resources:** a maximum template depth, a maximum discovered-prefix count
  per level and in total, and refusal (not truncation) when a bound is hit.
- **Pagination:** discovery fails closed on any incomplete listing.
- **Prefix semantics:** compiled prefixes are exact provider-key prefixes under
  the base prefix, deduplicated and sorted.
- **Recovery:** recovery reuses the sealed plan from the original run and never
  re-discovers.
- **Coverage:** no new tombstone authority. Coverage equals the compiled plan;
  rows outside the plan are retained without a fresh observation claim.

## Prerequisites

This type is not built until:

1. Durable builds detect and refuse an unexplained loss of effective coverage
   against the verified parent, and record the compiled plan with the
   published snapshot.
2. The published durable snapshot contract defines the plan record and the
   successor rules the compiled plan must satisfy. The scope grammar in this
   ADR is a build input; the plan it produces is governed by that contract.

## Alternatives Considered

- **Generalize `date_partitions`.** Rejected: it would change the validation
  and identity behavior of an existing type.
- **Expand the wildcard once at authoring time into a `prefix_list`.**
  Rejected as the primary path: new partition values are missed until the
  manifest is regenerated.
- **Accept non-default includes on durable builds by recording the match
  predicate in coverage.** Not pursued: it adds a new predicate to tombstone
  authority.

## Consequences

- Partitioned layouts with a non-date literal tail become expressible on
  durable builds without a manifest edit when partition values change.
- Discovery cost scales with the number of discovered prefixes, so the resource
  bounds are part of the contract, not tuning.
