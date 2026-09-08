# SDR-001: Temporary accept of x/crypto SSH advisories

**Date:** 2026-09-08
**Status:** Approved
**Author:** @3leapsdave
**Reviewer:** devrev

## Context

The dependency gate fails on high for two `golang.org/x/crypto` advisories
while the fixed module (`v0.56.0`) is still inside the seven-day cooling
window. The next cooling-clear cut will take `v0.56.0` and remove these
accepts.

## Finding

| Field      | Value                       |
| ---------- | --------------------------- |
| ID         | GO-2026-6354, GO-2026-6355  |
| Severity   | High                        |
| Package    | golang.org/x/crypto v0.55.0 |
| Scanner    | grype (goneat dependencies) |
| First Seen | 2026-09-08                  |

## Analysis

Both advisories are `x/crypto/ssh` channel denial-of-service findings.
`govulncheck -show verbose ./...` reports zero reachable vulnerabilities in
this repository. The module is present through the fake-GCS / MinIO test
path (`argon2`), not through SSH constructors.

## Decision

Accept the two IDs as `accepted_risk` until **2026-09-10**. Do not lower
`fail_on`. Do not suppress the gRPC advisory (that is fixed by bumping
`google.golang.org/grpc` to v1.83.1). Do not take `x/crypto v0.56.0` in the
same change.

## Verification

```bash
govulncheck -show verbose ./...
goneat assess --categories dependencies --check --fail-on high
```

## Action Items

- [ ] Remove or verify expired accepts by 2026-09-10 (Goneat `until` expires at the start of that date)
- [ ] Take `golang.org/x/crypto v0.56.0` after cooling and delete both allow entries

## References

- https://pkg.go.dev/vuln/GO-2026-6354
- https://pkg.go.dev/vuln/GO-2026-6355
