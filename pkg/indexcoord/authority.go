// Package indexcoord provides Experimental whole-set mutation authority shared
// by embeddable index engines, CLI adapters, and destructive maintenance.
package indexcoord

import (
	"context"
	"fmt"

	"github.com/3leaps/gonimbus/internal/indexsubstrate"
)

var (
	// ErrHeld reports contention with another whole-set operation.
	ErrHeld = indexsubstrate.ErrSetAuthorityHeld
	// ErrLost reports that the caller no longer owns the canonical lock binding.
	ErrLost = indexsubstrate.ErrSetAuthorityLost
	// ErrScope reports that a lease was presented for a different set or root.
	ErrScope = indexsubstrate.ErrSetAuthorityScope
)

// Lease is an unforgeable whole-set authority token. Its backing OS lock lives
// outside the set-specific segment root and therefore survives root quarantine.
type Lease struct {
	inner *indexsubstrate.SetAuthority
}

// Acquire takes authority for indexSetID at the stable sibling of
// segmentSetRoot. The call is non-blocking and returns ErrHeld on contention.
func Acquire(ctx context.Context, segmentSetRoot, indexSetID, holder string) (*Lease, error) {
	inner, err := indexsubstrate.AcquireSetAuthority(ctx, segmentSetRoot, indexSetID, holder)
	if err != nil {
		return nil, err
	}
	return &Lease{inner: inner}, nil
}

func (l *Lease) AssertHeld() error {
	if l == nil || l.inner == nil {
		return ErrLost
	}
	return l.inner.AssertHeld()
}

func (l *Lease) AssertHeldFor(indexSetID, segmentSetRoot string) error {
	if l == nil || l.inner == nil {
		return ErrLost
	}
	return l.inner.AssertHeldFor(indexSetID, segmentSetRoot)
}

func (l *Lease) Release() error {
	if l == nil || l.inner == nil {
		return nil
	}
	err := l.inner.Release()
	l.inner = nil
	return err
}

// AuthorityRoot returns the stable authority directory for a segment-set root.
func AuthorityRoot(segmentSetRoot string) (string, error) {
	root, err := indexsubstrate.SetAuthorityRootForSegmentSet(segmentSetRoot)
	if err != nil {
		return "", fmt.Errorf("resolve index set authority root: %w", err)
	}
	return root, nil
}
