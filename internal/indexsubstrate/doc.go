// Package indexsubstrate contains the experimental durable index substrate.
//
// The package is internal while the journal, segment, and manifest formats are
// still evolving. The package owns append-only observation journals,
// compaction-derived rows, immutable segment manifests, and fail-closed boundary
// render contracts for the durable snapshot format.
package indexsubstrate
