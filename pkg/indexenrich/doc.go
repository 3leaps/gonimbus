// Package indexenrich implements durable HEAD enrichment as a library-owned
// write transaction: set write lease, one verified parent snapshot under
// SegmentSetRoot/latest.json, filtered HEAD work, enrich-only journal, and
// expected-parent CAS publication.
//
// Per-candidate audit uses a typed StateSink (fail-closed by default). Adapters
// encode the established gonimbus.index.enrich_with_head.state.v1 envelope.
//
// Classification: internal full-fidelity render only. Enrichment does not create
// a reduced-trust or boundary-safe artifact.
package indexenrich
