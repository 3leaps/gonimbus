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
// The surface is deliberately narrower than the CLI's sqlite-backed store and
// grows only with engine-owned resume requirements.
type CheckpointStore interface {
	// ItemDone reports whether a source->dest item completed in a prior run,
	// returning the recorded terminal status when done.
	ItemDone(ctx context.Context, sourceURI, destURI string) (done bool, status string, err error)
	// UpsertItem records the terminal outcome of a reflow item. The item carries
	// only sanitized values.
	UpsertItem(ctx context.Context, item CheckpointItem) error
	// DestKeyObserved reports whether this run/checkpoint has already observed a
	// destination key. It backs in-run arbitration when conditional create is not
	// the whole decision.
	DestKeyObserved(ctx context.Context, destKey string) (bool, error)
	// MarkDestKeyObserved records that a destination key has been observed or
	// written in this run/checkpoint.
	MarkDestKeyObserved(ctx context.Context, destKey string) error
	// NoteDestKeySource records the source identity that produced a destination
	// key after a successful write.
	NoteDestKeySource(ctx context.Context, destKey, sourceURI, sourceETag string, sourceSize int64) error
	// NoteCollision records sanitized collision metadata for audit/resume.
	NoteCollision(ctx context.Context, collision CheckpointCollision) error
	// Close releases store resources.
	Close() error
}

// CheckpointItem is the sanitized per-item record an engine hands to a
// CheckpointStore. It carries no credential, signed-URL, or raw-config material.
type CheckpointItem struct {
	SourceURI    string
	DestURI      string
	SourceKey    string
	DestKey      string
	SourceETag   string
	SourceSize   int64
	Status       string
	Reason       string
	Bytes        int64
	ErrorCode    string
	ErrorMessage string
}

// CheckpointCollision is the sanitized collision observation an engine hands to
// a CheckpointStore. It carries only provider-neutral object identity and
// metadata, never credentials, signed URLs, or raw config.
type CheckpointCollision struct {
	DestKey    string
	Kind       string
	SourceURI  string
	SourceETag string
	SourceSize int64
	DestETag   string
	DestSize   int64
}
