package reflow

import (
	"context"

	"github.com/3leaps/gonimbus/pkg/producer"
)

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

// LaneCheckpointStore is the optional keyed durability capability required by
// live partitioned sources. It combines producer state with the downstream
// checkpoint so one transaction can record an item terminal and acknowledge
// its stable work-unit key. Experimental.
type LaneCheckpointStore interface {
	CheckpointStore
	producer.DurableStore

	// UnitDone reconciles a previously committed terminal by stable unit key.
	UnitDone(ctx context.Context, unitKey string) (done bool, status string, err error)
	// ResumeEnabled reports whether this scoped store is being used for replay.
	ResumeEnabled() bool
	// CheckpointUnit atomically records the terminal item and acknowledges the
	// unit. It returns only after the shared durable transaction commits.
	CheckpointUnit(ctx context.Context, item CheckpointItem, unitKey string) error
}

// SourceRunMetadataStore is an optional CheckpointStore capability: a store that
// records the run-level source a positional run resolved. Positional execution
// type-asserts for it when wired and skips the write for a store that does not
// implement it — absence is a clean no-op, not a deficiency, because a
// record-stream run has no single positional selector to record. Experimental.
//
// This is run-level SELECTOR metadata. It is unrelated to the per-object source
// metadata of metadata.go (user metadata copied or derived into destination PUT
// options); the two share a word, not a contract.
//
// The write is run setup performed through the coordinator, ordered ahead of any
// per-item authority write.
type SourceRunMetadataStore interface {
	// SetSourceRunMetadata records the resolved run-level source.
	SetSourceRunMetadata(ctx context.Context, meta SourceRunMetadata) error
}

// SourceRunMetadata is the resolved run-level description of the source a
// positional run selected, for persistence in a trusted checkpoint store. It
// carries no credential, signed-URL, or raw-config material — but it is NOT an
// event or log payload: Root and the selector can be disclosure-sensitive (a
// local filesystem root on the file path) and a store records them verbatim.
//
// URI is the run-level source SELECTOR — for a prefix or pattern source it is
// the prefix/pattern itself, which matches many objects and matches no single
// one. It is never item resume authority: ItemDone, UpsertItem, collision, and
// destination-source entries are keyed by each enumerated object's exact object
// URI. Reconstructing one identity from the other would either make resume
// lookups miss or make the run-level source record misdescribe what was
// selected.
type SourceRunMetadata struct {
	Provider string
	Bucket   string
	Root     string
	URI      string
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
