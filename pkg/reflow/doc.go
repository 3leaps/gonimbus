// Package reflow contains embeddable workflow substrate for transfer reflow.
//
// The substrate began (v0.3.4) with the adaptive concurrency contract, typed
// JSONL record/config payloads, and provider-error redaction helpers shared by
// the CLI and library runners. It now adds the embeddable engine contract:
// Runner (NewRunner/Run), Config, the typed Source forms (ObjectSource,
// PrefixSource, FileTreeSource, RecordStreamSource), the structured Destination,
// the EventSink event boundary, and the minimal CheckpointStore resume interface.
//
// The execution surface is migrating incrementally. Runner.Run executes the
// migrated paths — currently the dry-run and copy planes over a
// RecordStreamSource: it streams S3 reflow-input records, plans destination
// mappings, emits typed events, performs conditional PUTs for copied objects, and
// returns an InvalidInputsError after the summary when a stream carries invalid
// records (mirroring the command path's non-zero exit). Forms and scenarios not
// yet migrated (object/prefix/file-tree sources and later collision/provenance
// variants) return ErrNotImplemented, decided from the source form and config
// before any stream bytes are read, so a caller can fall back to the CLI path with
// the same Source. Providers are injected as provider.Provider handles, so the
// engine never imports a concrete provider, SDK, or the storageful
// (sqlite/index-store) graph — enforced by the dependency-boundary test.
//
// The package is Experimental; see docs/api-stability.md for the stability tiers.
package reflow
