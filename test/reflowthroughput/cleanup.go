package reflowthroughput

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// PrefixDeleter removes every object under a minted object-store prefix and
// verifies none remain. Injected so the cleanup lifecycle can be controlled
// without a provider: the failure modes that matter are ordering and error
// propagation, and neither needs a bucket to exercise.
type PrefixDeleter func(ctx context.Context, prefix string) error

// MintedPoint is the local residue one measurement point owns.
type MintedPoint struct {
	DestDir        string
	CheckpointPath string
}

// CleanupLedger owns everything a run mints, so that whatever fails, there is
// exactly one place that knows what has to be removed.
//
// The ordering rule it exists to enforce: a prefix is registered when it is
// minted, not when it has been successfully populated. Registering after a
// successful upload leaves the whole upload window uncovered — a failure
// partway through strands objects that nothing is responsible for removing.
type CleanupLedger struct {
	// Keep suppresses removal entirely. It is an operator choice to retain
	// artifacts, not a failure, so residue under Keep is expected.
	Keep bool
	// DeletePrefix is nil when the run has no object-store residue.
	DeletePrefix PrefixDeleter

	sourcePrefix string
	destPrefixes []string
	points       []MintedPoint
}

// RegisterSourcePrefix takes ownership of the minted source prefix. Call it
// immediately after minting and before the first upload.
func (l *CleanupLedger) RegisterSourcePrefix(prefix string) {
	l.sourcePrefix = prefix
}

// RegisterDestPrefix takes ownership of a minted destination prefix.
func (l *CleanupLedger) RegisterDestPrefix(prefix string) {
	if prefix == "" {
		return
	}
	l.destPrefixes = append(l.destPrefixes, prefix)
}

// RegisterPoint takes ownership of a point's local residue.
func (l *CleanupLedger) RegisterPoint(p MintedPoint) {
	l.points = append(l.points, p)
}

// Owned reports the prefixes currently registered, for controls and diagnostics.
func (l *CleanupLedger) Owned() (source string, dests []string) {
	return l.sourcePrefix, append([]string(nil), l.destPrefixes...)
}

// Run removes everything registered and verifies it is gone.
//
// Every failure is collected rather than the first one returned alone: a
// destination that would not delete must not hide a source that also would not,
// because the operator needs to know the full extent of what was left behind.
func (l *CleanupLedger) Run() error {
	if l.Keep {
		return nil
	}
	var errs []error

	if l.DeletePrefix != nil {
		if l.sourcePrefix != "" {
			if err := l.DeletePrefix(context.Background(), l.sourcePrefix); err != nil {
				errs = append(errs, fmt.Errorf("source prefix cleanup: %w", err))
			}
		}
		for _, pref := range l.destPrefixes {
			if err := l.DeletePrefix(context.Background(), pref); err != nil {
				errs = append(errs, fmt.Errorf("dest prefix cleanup: %w", err))
			}
		}
	}

	for _, m := range l.points {
		if m.DestDir != "" {
			if err := os.RemoveAll(m.DestDir); err != nil {
				errs = append(errs, err)
			}
		}
		if m.CheckpointPath != "" {
			if err := os.Remove(m.CheckpointPath); err != nil && !os.IsNotExist(err) {
				errs = append(errs, fmt.Errorf("checkpoint remove: %w", err))
			}
			if _, err := os.Stat(m.CheckpointPath); !os.IsNotExist(err) {
				errs = append(errs, fmt.Errorf("checkpoint still present after cleanup"))
			}
		}
	}
	// Verify local destinations are gone rather than trusting the removal.
	for _, m := range l.points {
		if m.DestDir == "" {
			continue
		}
		if _, err := os.Stat(m.DestDir); !os.IsNotExist(err) {
			errs = append(errs, fmt.Errorf("cleanup left destination %s", filepath.Base(m.DestDir)))
		}
	}
	if err := errors.Join(errs...); err != nil {
		return err
	}
	// Everything registered is gone, so a later call — the deferred fail-safe
	// after an explicit one — has nothing to do. Idempotency lives here rather
	// than in each caller clearing its own bookkeeping.
	l.sourcePrefix = ""
	l.destPrefixes = nil
	l.points = nil
	return nil
}

// PrepareSourcePrefix mints a source prefix, registers it for cleanup, and only
// then uploads into it.
//
// The registration sits between the two deliberately. Minting and the first
// object are separated by an entire corpus upload, and a failure inside that
// window is the one case a post-upload registration cannot cover.
func PrepareSourcePrefix(ledger *CleanupLedger, mint func() string, upload func(prefix string) error) (string, error) {
	prefix := mint()
	ledger.RegisterSourcePrefix(prefix)
	if err := upload(prefix); err != nil {
		return prefix, err
	}
	return prefix, nil
}
