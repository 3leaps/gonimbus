// Package indexsubstrate contains the experimental durable index substrate.
//
// The package is internal while the journal, segment, and manifest formats are
// still evolving. Slice 1 defines append-only observation journals; compaction,
// segment writing, and manifest publication build on this boundary in later
// slices.
package indexsubstrate
