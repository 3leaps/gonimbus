// Package indexsubstrate contains the experimental durable index substrate.
//
// The package is internal while the journal, segment, and manifest formats are
// still evolving. The package owns append-only observation journals,
// compaction-derived rows, immutable segment manifests, and fail-closed boundary
// render contracts for the durable snapshot format. Segment reachability is
// modeled from retained manifests, parent chains, and latest pointers; refcounts
// are derived audit data for future compact/GC planning, not mutable truth.
package indexsubstrate
