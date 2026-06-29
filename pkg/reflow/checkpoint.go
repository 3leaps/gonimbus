package reflow

import "context"

// CheckpointStore is the minimal resume interface the engine needs. The CLI wraps
// its storageful (sqlite-backed) implementation behind this interface so the
// embeddable engine never imports the storage graph (enforced by the
// dependency-boundary test). Only sanitized values cross this boundary — no
// credential, signed-URL, or raw-config material. Experimental.
//
// Precondition: the store instance handed to a Runner is already scoped and
// identity-validated for the compatible run/config by the caller (the CLI adapter
// performs the resume identity binding before injection). The minimal interface
// does not itself represent run-identity, so that fail-closed invariant stays with
// the caller and must not be assumed to be enforced here.
//
// The surface is deliberately narrow for this cut; it grows as the engine's resume
// needs migrate, driven by real requirements rather than the full internal store
// shape.
type CheckpointStore interface {
	// ItemDone reports whether a source->dest item completed in a prior run,
	// returning the recorded terminal status when done.
	ItemDone(ctx context.Context, sourceURI, destURI string) (done bool, status string, err error)
	// MarkItem records the terminal outcome of a reflow item. The item carries
	// only sanitized values.
	MarkItem(ctx context.Context, item CheckpointItem) error
	// Close releases store resources.
	Close() error
}

// CheckpointItem is the sanitized per-item record an engine hands to a
// CheckpointStore. It carries no credential, signed-URL, or raw-config material.
type CheckpointItem struct {
	SourceURI string
	DestURI   string
	Status    string
}
