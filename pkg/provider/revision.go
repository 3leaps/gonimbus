package provider

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
)

// ErrSourceChanged reports that a revision-bound read no longer resolves to
// the source revision admitted by the run.
var ErrSourceChanged = errors.New("source changed after admission")

// ErrReplayUnverifiable reports that a provider cannot enforce the revision
// token needed to re-drive an admitted object safely.
var ErrReplayUnverifiable = errors.New("source replay is unverifiable")

// RevisionKind identifies the provider-neutral enforcement shape of an opaque
// source revision token.
type RevisionKind string

const (
	RevisionNative RevisionKind = "native"
	RevisionETag   RevisionKind = "etag"
)

// SourceRevision is a non-secret, opaque source revision admitted by a
// coordinator. Provider adapters interpret Value according to Kind.
type SourceRevision struct {
	Kind  RevisionKind
	Value string
}

// Validate refuses revision tokens that cannot be enforced.
func (r SourceRevision) Validate() error {
	if strings.TrimSpace(r.Value) == "" {
		return fmt.Errorf("%w: revision token is empty", ErrReplayUnverifiable)
	}
	switch r.Kind {
	case RevisionNative, RevisionETag:
		return nil
	default:
		return fmt.Errorf("%w: unsupported revision kind %q", ErrReplayUnverifiable, r.Kind)
	}
}

// RevisionGetter reads exactly the admitted source revision or fails closed.
// A moved validator returns an error wrapping ErrSourceChanged; an adapter that
// cannot enforce the supplied token returns ErrReplayUnverifiable.
type RevisionGetter interface {
	GetObjectRevision(ctx context.Context, key string, revision SourceRevision) (body io.ReadCloser, meta ObjectMeta, err error)
}

// IsSourceChanged reports whether a revision-bound read observed source drift.
func IsSourceChanged(err error) bool { return errors.Is(err, ErrSourceChanged) }

// IsReplayUnverifiable reports whether a revision-bound read cannot be proven.
func IsReplayUnverifiable(err error) bool { return errors.Is(err, ErrReplayUnverifiable) }
