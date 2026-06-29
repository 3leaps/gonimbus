// Package reflow contains embeddable workflow substrate for transfer reflow.
//
// The substrate began (v0.3.4) with the adaptive concurrency contract, typed
// JSONL record/config payloads, and provider-error redaction helpers shared by
// the CLI and library runners. It now adds the embeddable engine contract:
// Runner (NewRunner/Run), Config, the typed Source forms (ObjectSource,
// PrefixSource, FileTreeSource, RecordStreamSource), the structured Destination,
// the EventSink event boundary, and the minimal CheckpointStore resume interface.
//
// The execution surface is a skeleton: Runner.Run returns ErrNotImplemented until
// the data/decision plane migration lands; the CLI remains the execution path
// meanwhile. Providers are injected as provider.Provider handles, so the engine
// never imports a concrete provider, SDK, or the storageful (sqlite/index-store)
// graph — enforced by the dependency-boundary test.
//
// The package is Experimental; see docs/api-stability.md for the stability tiers.
package reflow
