package reflow

import "fmt"

// Config configures a reflow Runner. It holds injected provider handles and
// policy; provider construction and credential resolution are caller concerns.
//
// Config implements redacted String/GoString so debug logging cannot dump an
// injected provider or credential handle.
//
// This surface carries the structural knobs that have migrated into the engine.
// Provenance and file-source policy fields are added alongside their decision
// logic rather than designed speculatively here. Experimental.
type Config struct {
	// Destination is the structured reflow target.
	Destination Destination
	// Rewrite controls source->destination key mapping.
	Rewrite RewriteConfig
	// Collision selects the destination collision policy.
	Collision CollisionPolicy
	// Concurrency bounds the adaptive copy ceiling. The zero value is resolved to
	// defaults by the runner.
	Concurrency ConcurrencyConfig
	// DryRun plans mappings without writing.
	DryRun bool
	// ReadOnly blocks provider-side mutations, including write-probe preflight.
	ReadOnly bool
	// Metadata controls destination metadata and storage-class PUT options.
	Metadata MetadataPlan
	// Checkpoint, when non-nil, enables resume via an embedder-supplied store.
	Checkpoint CheckpointStore
	// Events, when non-nil, receives typed redacted engine events.
	Events EventSink
}

// RewriteConfig holds the segment-capture rewrite templates (the library form of
// --rewrite-from / --rewrite-to). An empty pair preserves source-relative keys.
type RewriteConfig struct {
	From string
	To   string
}

// CollisionPolicy selects how the engine handles an existing destination object.
// Mode is one of skip-if-duplicate|fail|overwrite|quarantine|overwrite-if-source-newer.
// The copy-plane migration currently executes skip-if-duplicate and fail;
// other non-dry-run modes return ErrNotImplemented until their command-path
// semantics migrate.
type CollisionPolicy struct {
	Mode             string
	QuarantinePrefix string
}

// String returns a redacted summary; it never recurses into the injected
// provider handle or the checkpoint/event sinks.
func (c Config) String() string {
	return fmt.Sprintf("reflow.Config{Destination:%s, Rewrite:%+v, Collision:%+v, DryRun:%t, ReadOnly:%t, Metadata:%+v, Checkpoint:%s, Events:%s}",
		c.Destination, c.Rewrite, c.Collision, c.DryRun, c.ReadOnly, c.Metadata,
		ifacePresence(c.Checkpoint == nil), ifacePresence(c.Events == nil))
}

// GoString implements fmt %#v with the same redaction as String.
func (c Config) GoString() string { return c.String() }

// ifacePresence renders an injected interface's presence without exposing it.
func ifacePresence(isNil bool) string {
	if isNil {
		return "<nil>"
	}
	return "<set>"
}

func fieldPresence(isEmpty bool) string {
	if isEmpty {
		return "<empty>"
	}
	return "<set>"
}
