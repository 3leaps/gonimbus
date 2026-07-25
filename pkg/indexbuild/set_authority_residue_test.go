package indexbuild

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/internal/indexsubstrate"
	"github.com/3leaps/gonimbus/pkg/provider"
)

// authorityWitnessProvider probes the set-authority lease from inside the crawl —
// while the build is running and therefore holding it — and then applies an
// injected outcome. It is what makes the residue guard non-vacuous: without it, a
// guard asserting "no artifact afterwards" would pass just as happily against a
// build that never acquired an authority at all.
type authorityWitnessProvider struct {
	objects       []provider.ObjectSummary
	authorityRoot string
	indexSetID    string
	// during records the verdict observed while the build was running.
	during indexsubstrate.LeaseVerdict
	// onList applies the outcome under test (failure, cancellation) after the
	// observation is recorded.
	onList func() error
}

func (p *authorityWitnessProvider) List(_ context.Context, opts provider.ListOptions) (*provider.ListResult, error) {
	lease, err := indexsubstrate.ProbeSetAuthorityLease(p.authorityRoot, p.indexSetID)
	if err == nil {
		p.during = lease.Verdict
	}
	if p.onList != nil {
		if listErr := p.onList(); listErr != nil {
			return nil, listErr
		}
	}
	var out []provider.ObjectSummary
	for _, object := range p.objects {
		if strings.HasPrefix(object.Key, opts.Prefix) {
			out = append(out, object)
		}
	}
	return &provider.ListResult{Objects: out}, nil
}

func (*authorityWitnessProvider) Head(context.Context, string) (*provider.ObjectMeta, error) {
	return nil, provider.ErrNotFound
}

func (*authorityWitnessProvider) Close() error { return nil }

// residueSetID is a canonical index-set ID (idx_ + 64 hex), so the artifact this
// guard inspects is the same shape the lease boundary classifies in production.
const residueSetID = "idx_" + "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

// assertNoSetAuthorityResidue is the set-authority residue guard: after a build
// reaches any trappable outcome, no authority artifact for it may remain.
//
// It asserts at two levels because they fail differently. The typed enumeration
// is what an operator would see (`index lease ls`), and the raw directory listing
// catches an artifact that enumeration would not classify as a lease at all.
func assertNoSetAuthorityResidue(t *testing.T, authorityRoot string) {
	t.Helper()
	leases, err := indexsubstrate.EnumerateSetAuthorityLeases(authorityRoot)
	require.NoError(t, err)
	require.Empty(t, leases, "a completed run must leave no set-authority lease behind")

	entries, err := os.ReadDir(authorityRoot)
	if os.IsNotExist(err) {
		return
	}
	require.NoError(t, err)
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	require.Empty(t, names, "no artifact of any kind may survive in the authority directory")
}

// TestBuildReportsOwnerCleanupRefusalAlongsideItsOwnFailure pins the library
// half of the cleanup-reporting contract. Build releases the authority it owns
// through a deferred call; that release now removes the artifact, and it can
// refuse. A discarded refusal would mean the library silently left residue and
// swallowed the only evidence that something rebound its lease pathname.
//
// Rebinding the pathname mid-crawl also breaks the build's own authority
// assertion at publication, which is the point: both causes must survive to the
// caller. Cleanup reporting must never displace the reason the work failed.
func TestBuildReportsOwnerCleanupRefusalAlongsideItsOwnFailure(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("platform refuses to rebind a held authority pathname; the swap is unconstructible here")
	}
	setRoot := t.TempDir()
	segmentSetRoot := filepath.Join(setRoot, "segset")
	require.NoError(t, os.MkdirAll(segmentSetRoot, 0o700))
	authorityRoot, err := indexsubstrate.SetAuthorityRootForSegmentSet(segmentSetRoot)
	require.NoError(t, err)

	const decoy = "decoy-inode-B"
	lockPath := filepath.Join(authorityRoot, residueSetID+".lock")
	witness := &authorityWitnessProvider{
		objects:       []provider.ObjectSummary{obj("data/a.xml", `"a"`, 10, time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC))},
		authorityRoot: authorityRoot,
		indexSetID:    residueSetID,
		onList: func() error {
			decoyPath := lockPath + ".decoy"
			if writeErr := os.WriteFile(decoyPath, []byte(decoy), 0o600); writeErr != nil {
				return writeErr
			}
			return os.Rename(decoyPath, lockPath)
		},
	}

	cfg := contConfig(segmentSetRoot, "run_refusal", nil, time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC))
	cfg.IndexSetID = residueSetID
	cfg.Source = Source{Provider: witness, ProviderName: "s3"}
	cfg.Paths = contRunPaths(segmentSetRoot, "run_refusal")

	_, buildErr := NewRunner(cfg).Build(context.Background())
	require.Error(t, buildErr)
	require.Contains(t, buildErr.Error(), "release index set authority",
		"a refused owner cleanup must reach the caller, not be discarded by the deferred release")
	// Both causes, pinned independently: rebinding the pathname also breaks the
	// build's own authority assertion at publication, and cleanup reporting must
	// never displace the reason the work failed.
	require.ErrorIs(t, buildErr, indexsubstrate.ErrSetAuthorityLost,
		"the primary failure must survive alongside the cleanup failure, not be replaced by it")
	require.Equal(t, indexsubstrate.LeaseHeld, witness.during,
		"the build must have been holding its set authority when the pathname was rebound")

	content, err := os.ReadFile(lockPath) // #nosec G304 -- test-owned temp path
	require.NoError(t, err, "the rebound occupant must not have been removed")
	require.Equal(t, decoy, string(content), "cleanup must never delete the inode that now occupies the pathname")

	// The lock was released despite the refusal: a successor can acquire.
	successor, err := indexsubstrate.AcquireSetAuthority(context.Background(), segmentSetRoot, residueSetID, "build-successor")
	require.NoError(t, err, "a refused cleanup must still have released the lock")
	require.NoError(t, successor.Release())
}

// TestBuildLeavesNoSetAuthorityResidue is the guard that ships with the
// completion-path change so the change self-verifies: for every trappable
// outcome — success, failure, and cancellation — the run's own set-authority
// artifact is gone when Build returns.
//
// Untrappable termination (SIGKILL, os.Exit) is deliberately absent: no cleanup
// path runs there, residue is real, and it is what lease detection and reclaim
// exist to collect. This guard must not be read as covering it.
func TestBuildLeavesNoSetAuthorityResidue(t *testing.T) {
	objects := []provider.ObjectSummary{
		obj("data/a.xml", `"a"`, 10, time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)),
	}

	cases := []struct {
		name    string
		inject  func(cancel context.CancelFunc) func() error
		wantErr bool
	}{
		{
			name:   "success",
			inject: func(context.CancelFunc) func() error { return nil },
		},
		{
			name: "failure",
			inject: func(context.CancelFunc) func() error {
				return func() error { return fmt.Errorf("injected list failure: %w", provider.ErrAccessDenied) }
			},
			wantErr: true,
		},
		{
			name: "cancellation",
			inject: func(cancel context.CancelFunc) func() error {
				return func() error {
					cancel()
					return context.Canceled
				}
			},
			wantErr: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			setRoot := t.TempDir()
			segmentSetRoot := filepath.Join(setRoot, "segset")
			require.NoError(t, os.MkdirAll(segmentSetRoot, 0o700))
			authorityRoot, err := indexsubstrate.SetAuthorityRootForSegmentSet(segmentSetRoot)
			require.NoError(t, err)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			witness := &authorityWitnessProvider{
				objects:       objects,
				authorityRoot: authorityRoot,
				indexSetID:    residueSetID,
				onList:        tc.inject(cancel),
			}

			cfg := contConfig(segmentSetRoot, "run_residue", nil, time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC))
			cfg.IndexSetID = residueSetID
			cfg.Source = Source{Provider: witness, ProviderName: "s3"}
			cfg.Paths = contRunPaths(segmentSetRoot, "run_residue")

			_, buildErr := NewRunner(cfg).Build(ctx)
			if tc.wantErr {
				require.Error(t, buildErr)
			} else {
				require.NoError(t, buildErr)
			}

			// Non-vacuity: the run really did hold an authority artifact at the path
			// this guard inspects, so "nothing there afterwards" means it was removed
			// rather than never created.
			require.Equal(t, indexsubstrate.LeaseHeld, witness.during,
				"the build must have been holding its set authority during the crawl")

			assertNoSetAuthorityResidue(t, authorityRoot)
		})
	}
}
