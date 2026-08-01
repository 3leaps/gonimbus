// Package stalledrecovery is the developer opt-in evidence harness for managed
// stalled-plan / recover-stalled protocol gates.
//
// It is intentionally NOT part of default CI (`make test`). Same pattern as
// test/cloudtest real-cloud lanes and test/reflowthroughput BYO:
//
//   - Credential-free local defaults when the opt-in env is set
//   - Skip (do not fail) when the opt-in env is absent
//   - No hard-coded bucket, account, profile, or client identifiers
//   - Optional external roots via env only
//   - Reports and logs must not print secret values or private bucket names
//
// Enable:
//
//	GONIMBUS_STALLED_EVIDENCE=1 make test-stalled-evidence
//
// See docs/development/testing.md § Stalled recovery evidence (opt-in).
package stalledrecovery
