package indexsubstrate

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestRelease_RemovesItsOwnLease pins that a run which completes removes its own
// set-authority artifact, so no residue survives a normal exit.
//
// It also covers the reopen/reacquire prohibition: cleanup runs on the descriptor
// held since acquisition, and an implementation that reopened and re-locked the
// lease would contend with itself (see
// TestSetAuthority_SecondOpenCannotLockHeldLease), failing the removal.
func TestRelease_RemovesItsOwnLease(t *testing.T) {
	segmentRoot := t.TempDir()
	authorityRoot := authorityRootFor(t, segmentRoot)
	id := fixtureIndexSetID('a')

	auth, err := AcquireSetAuthority(context.Background(), segmentRoot, id, "index-build-owner")
	require.NoError(t, err)
	path := filepath.Join(authorityRoot, id+".lock")
	require.FileExists(t, path, "an acquired authority must exist while held")

	require.NoError(t, auth.Release())
	require.NoFileExists(t, path, "a completed owner must leave no set-authority residue")

	lease, err := ProbeSetAuthorityLease(authorityRoot, id)
	require.NoError(t, err)
	require.Equal(t, LeaseMissing, lease.Verdict, "the observation boundary agrees the artifact is gone")
}

// TestRelease_IsIdempotent pins that a second Release is a no-op success, so the
// belt-and-braces `defer Release()` plus an explicit Release cannot double-remove
// a pathname a successor may already own.
func TestRelease_IsIdempotent(t *testing.T) {
	segmentRoot := t.TempDir()
	authorityRoot := authorityRootFor(t, segmentRoot)
	id := fixtureIndexSetID('a')

	auth, err := AcquireSetAuthority(context.Background(), segmentRoot, id, "index-build-owner")
	require.NoError(t, err)
	require.NoError(t, auth.Release())

	// A successor takes the pathname between the two Release calls.
	successor, err := AcquireSetAuthority(context.Background(), segmentRoot, id, "index-build-successor")
	require.NoError(t, err)
	t.Cleanup(func() { _ = successor.Release() })

	require.NoError(t, auth.Release(), "a repeated release must be a no-op")
	require.NoError(t, successor.AssertHeld(), "a repeated release must not disturb the successor")
	require.FileExists(t, filepath.Join(authorityRoot, id+".lock"))
}

// TestRelease_RefusesSwappedPathnameAndStillReleases is the owner-cleanup
// negative control. If the lease pathname is rebound to a different inode while
// the owner holds the lock, cleanup must refuse to remove — it must never delete
// the file that now occupies the name — and must still release its own lock so
// authority is never stranded.
func TestRelease_RefusesSwappedPathnameAndStillReleases(t *testing.T) {
	segmentRoot := t.TempDir()
	authorityRoot := authorityRootFor(t, segmentRoot)
	id := fixtureIndexSetID('a')
	name := id + ".lock"

	auth, err := AcquireSetAuthority(context.Background(), segmentRoot, id, "index-build-owner")
	require.NoError(t, err)

	resolved, err := resolveAuthorityRootForRead(authorityRoot)
	require.NoError(t, err)
	lockPath := filepath.Join(resolved, name)
	decoyPath := lockPath + ".decoy"
	require.NoError(t, os.WriteFile(decoyPath, []byte("decoy-inode-B"), 0o600))
	if swapErr := os.Rename(decoyPath, lockPath); swapErr != nil {
		// Where the platform refuses to rebind a pathname whose file is held, the
		// swap cannot be constructed: the OS enforces at its layer what the refusal
		// enforces in code, and that code path does not execute here.
		require.True(t, lockedRangeUnreadable(swapErr), "unexpected swap failure: %v", swapErr)
		require.NoError(t, os.Remove(decoyPath))
		require.NoError(t, auth.Release())
		t.Skip("platform refuses to rebind a held authority pathname; the swap is unconstructible here")
	}

	releaseErr := auth.Release()

	// Assert the substantive harm first: whatever cleanup decided, it must not
	// have deleted the inode that now occupies the pathname. An implementation
	// that unlinked after releasing the lock — rather than under it — removes the
	// successor's file here, which is exactly the authority split this guards.
	content, err := os.ReadFile(lockPath) // #nosec G304 -- test-owned temp path
	require.NoError(t, err, "the swapped-in inode must not have been removed")
	require.Equal(t, "decoy-inode-B", string(content), "the swapped-in inode must survive intact")
	require.Error(t, releaseErr, "owner cleanup must report the refusal, not silently skip cleanup")

	// The lock was still released: a fresh acquisition succeeds rather than
	// reporting contention with the refused owner.
	successor, err := AcquireSetAuthority(context.Background(), segmentRoot, id, "index-build-successor")
	require.NoError(t, err, "a refused cleanup must still have released the lock")
	require.NoError(t, successor.Release())
}

// TestSetAuthority_SecondOpenCannotLockHeldLease pins the platform property that
// makes the reopen/reacquire prohibition load-bearing: an OS file lock belongs to
// an open-file description, not to the process, so a second open of a lease this
// same process already holds cannot be locked. An owner that reopened its lease
// to clean it up would therefore refuse itself.
func TestSetAuthority_SecondOpenCannotLockHeldLease(t *testing.T) {
	segmentRoot := t.TempDir()
	authorityRoot := authorityRootFor(t, segmentRoot)
	id := fixtureIndexSetID('a')

	auth, err := AcquireSetAuthority(context.Background(), segmentRoot, id, "index-build-owner")
	require.NoError(t, err)
	t.Cleanup(func() { _ = auth.Release() })

	resolved, err := resolveAuthorityRootForRead(authorityRoot)
	require.NoError(t, err)
	root, second, _, err := openBoundSetAuthority(resolved, id+".lock")
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = second.Close()
		_ = root.Close()
	})

	require.Error(t, lockFileExclusive(second),
		"a second open-file description must not be able to lock a lease this process already holds")
}

// TestAcquire_RetriesPathnameRemovedByDepartingOwner covers the race that owner
// cleanup introduces: an acquirer can open the lease file just before the
// departing owner unlinks it, then lock an inode whose pathname is already gone.
// That acquirer must still end up with real authority — not a hard binding error
// — because nothing is holding the lease.
//
// The hook opens exactly that window deterministically, on the first attempt
// only, by removing the pathname after the lock is taken and before the binding
// is revalidated.
func TestAcquire_RetriesPathnameRemovedByDepartingOwner(t *testing.T) {
	segmentRoot := t.TempDir()
	authorityRoot := authorityRootFor(t, segmentRoot)
	id := fixtureIndexSetID('a')
	lockPath := filepath.Join(authorityRoot, id+".lock")

	attempts := 0
	acquireAfterLockHook = func() {
		attempts++
		if attempts == 1 {
			require.NoError(t, os.Remove(lockPath))
		}
	}
	t.Cleanup(func() { acquireAfterLockHook = nil })

	auth, err := AcquireSetAuthority(context.Background(), segmentRoot, id, "index-build-successor")
	require.NoError(t, err, "a pathname removed by a departing owner must be re-acquired, not reported as an error")
	require.Equal(t, 2, attempts, "the first attempt is invalidated by the removal; the second must succeed")
	require.FileExists(t, lockPath)
	require.NoError(t, auth.AssertHeld(), "the retried acquisition must hold real authority over the pathname")
	require.NoError(t, auth.Release())
}

// TestAcquire_FailsClosedWhenPathnameChurnsEveryAttempt is the bound's control.
// Retrying absorbs a departing owner; it must not become an unbounded loop. When
// every attempt's binding is invalidated, acquisition fails rather than spinning,
// and it fails closed — no authority is returned.
//
// The failure is reported as contention, the same public outcome a live holder
// produces, so a caller can act on it with the one taxonomy the authority
// contract defines. The binding-change cause stays reachable inside the error.
func TestAcquire_FailsClosedWhenPathnameChurnsEveryAttempt(t *testing.T) {
	segmentRoot := t.TempDir()
	authorityRoot := authorityRootFor(t, segmentRoot)
	id := fixtureIndexSetID('a')
	lockPath := filepath.Join(authorityRoot, id+".lock")

	attempts := 0
	acquireAfterLockHook = func() {
		attempts++
		_ = os.Remove(lockPath)
	}
	t.Cleanup(func() { acquireAfterLockHook = nil })

	auth, err := AcquireSetAuthority(context.Background(), segmentRoot, id, "index-build-churn")
	require.ErrorIs(t, err, ErrSetAuthorityHeld,
		"bounded exhaustion must classify as the public held-authority outcome, not a private binding taxonomy")
	require.ErrorIs(t, err, errSetAuthorityBindingChanged,
		"the binding-change cause must stay reachable for diagnosis")
	require.Nil(t, auth, "a failed acquisition must never hand back authority")
	require.Equal(t, setAuthorityAcquireAttempts, attempts, "retries must be bounded")
}
