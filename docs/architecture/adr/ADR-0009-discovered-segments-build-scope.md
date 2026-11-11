# ADR-0009: Discovered-Segments Build Scope

**Status:** Accepted
**Date:** 2026-10-17
**Decision Makers:** @3leapsdave

## Context

`build.scope` compiles to an explicit LIST prefix plan before a crawl
(`pkg/scope`). Three scope types existed prior to this ADR: `prefix_list`,
`date_partitions` and `union`. `date_partitions` discovers path segments at
build time with delimiter listing (`discover.segments`) and then appends a
date-formatted suffix. The index-set identity hashes the scope configuration,
not the discovered values, so new partition values do not change identity.

Durable builds accept only the default match include. Partitioned layouts often
need a wildcard in a middle path segment followed by a literal tail, for
example `tenant=*/table=orders/**`. When the tail is a date,
`date_partitions` expresses the shape. When the tail is a non-date literal, no
scope type expressed it, and the remaining options were a hand-written
`prefix_list` (a snapshot of the values at authoring time) or the SQLite
format.

`IndexScopeConfig` reserved `constraints` and `depth_mode` for a `segments`
scope type.

## Decision

Define the `segments` scope type (`build.scope.type: segments`):

- A template of path segments (`discover.segments` or the shorthand
  `constraints` list), each either a literal (`allow`) or a whole-segment glob
  (`glob_allow`, with optional `deny` / `glob_deny`), discovered left to right:
  - Discovered segment levels perform one bounded delimiter listing per prefix
    from the previous level.
  - Pure literal segment levels (`allow` only, no `deny` / `glob_allow` /
    `glob_deny`) append `<literal><delimiter>` directly without an extra
    delimiter listing call.
  - At least one segment level in the template must be discovered (all-literal
    templates are refused in favor of `prefix_list`).
- Shared compiler internals with `date_partitions`. `date_partitions`
  validation, hashing and identity behavior remain unchanged.
- The identity hash (`pkg/scope.HashConfig`) covers the canonicalized segment
  template (`discover.segments` and `constraints` canonicalize to the same
  ordered segment rules), not the discovered runtime values.

Enforced contract bounds:

- **Grammar:** contiguous 0-based segment indices (`0 .. N-1`); whole-segment
  globs or literals only; no path delimiter (`/`), backslash (`\`), or
  recursive (`**`) wildcard inside a segment pattern.
- **Resources:** `MaxSegmentsTemplateDepth = 16`,
  `MaxDiscoveredPrefixesPerLevel = 10000`, and
  `MaxCompiledScopePrefixes = 65536`. Exceeding any bound fails closed with
  `ErrDiscoveryBudgetExceeded` (never truncating discovery).
- **Pagination:** discovery fails closed on any incomplete or cyclic
  continuation token (`provider.ErrIncompleteListing`).
- **Prefix semantics:** compiled prefixes are exact provider-key prefixes under
  the base prefix, deduplicated and sorted in canonical `unicode-prefix/v0`
  order (`index_plan`).
- **Recovery:** journal recovery reuses the sealed plan from the original run
  and never re-discovers prefixes from the provider.
- **Coverage & Scope-Loss Safeguard:** coverage equals the compiled `index_plan`
  and its per-scope `coverage_attestation`s. Unexplained loss of effective
  prefix coverage against the verified parent publication refuses publication by
  default (`scope_loss`) unless explicitly acknowledged via
  `--acknowledge-scope-loss`, which records `retained_unobserved_rows` on the
  resulting `durable-index/v0` snapshot.

## Prerequisites (Satisfied)

1. Durable builds detect and refuse an unexplained loss of effective coverage
   against the verified parent, and record the compiled `index_plan` with the
   published `durable-index/v0` snapshot.
2. The published durable snapshot contract (`durable-index/v0`) defines the
   plan record and the successor rules the compiled plan must satisfy.

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
- Offline `gonimbus index migrate-match-scope` can propose `date_partitions` or
  `segments` directly from a single mid-path wildcard include pattern.
- Discovery cost scales with the number of discovered prefixes, bounded by the
  explicit depth and prefix budgets above.
