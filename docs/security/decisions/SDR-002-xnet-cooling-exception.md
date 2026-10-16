# SDR-002: Temporary cooling exception for golang.org/x/net v0.60.0

**Date:** 2026-10-12
**Status:** Closed (exception removed 2026-10-15)
**Author:** @3leapsdave
**Reviewer:** secrev

## Context

`govulncheck` reports five reachable advisories in `golang.org/x/net`
v0.58.0. The only fixed release, v0.60.0, was published 2026-10-08T17:22Z and
is inside the seven-day cooling window until 2026-10-15T17:22Z. The
dependency gate fails on high for that age violation.

## Finding

| Field      | Value                                                                |
| ---------- | -------------------------------------------------------------------- |
| ID         | GO-2026-6603, GO-2026-6610, GO-2026-6611, GO-2026-6612, GO-2026-6617 |
| Package    | golang.org/x/net v0.58.0                                             |
| Fixed in   | golang.org/x/net v0.60.0                                             |
| Scanner    | govulncheck (reachable)                                              |
| First Seen | 2026-10-12                                                           |

## Decision

Take `golang.org/x/net` v0.60.0 now with a time-bounded cooling exception
for that module only, expiring 2026-10-16. The release is a first-party Go
project security fix. Do not take v0.61.0, which adds no security fix and
pulls `golang.org/x/sys` inside cooling. Do not lower `min_age_days` or
`fail_on`. Every other module in the same change is past cooling.

Approved by @3leapsdave on 2026-10-12.

## Verification

```bash
GOTOOLCHAIN=go1.26.9 govulncheck ./...
goneat assess --categories dependencies --check --fail-on high
```

## Action Items

- [x] Delete the `golang.org/x/net` cooling exception after 2026-10-15T17:22Z
      (goneat `until` expires at the start of 2026-10-16). Removed 2026-10-15
      after v0.60.0 cleared the seven-day window; no later x/net release
      superseded it under the exception.

## References

- https://pkg.go.dev/vuln/GO-2026-6603
- https://pkg.go.dev/vuln/GO-2026-6610
- https://pkg.go.dev/vuln/GO-2026-6611
- https://pkg.go.dev/vuln/GO-2026-6612
- https://pkg.go.dev/vuln/GO-2026-6617
