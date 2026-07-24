package indexsubstrate

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestReclaim_UnheldRemovesAndIsIdempotent(t *testing.T) {
	segmentRoot := t.TempDir()
	authorityRoot := authorityRootFor(t, segmentRoot)
	id := fixtureIndexSetID('a')

	// Leave dead-holder residue: acquire then release (Release does not remove).
	seed, err := AcquireSetAuthority(context.Background(), segmentRoot, id, "index-build-seed")
	require.NoError(t, err)
	require.NoError(t, seed.Release())
	path := filepath.Join(authorityRoot, id+".lock")
	require.FileExists(t, path)

	res, err := ReclaimUnheldSetAuthorityLease(authorityRoot, id)
	require.NoError(t, err)
	require.True(t, res.Reclaimed)
	require.Equal(t, LeaseUnheld, res.Verdict)
	require.Equal(t, "index-build-seed", res.Holder, "reclaim reports the holder it reaped")
	require.NoFileExists(t, path)

	// Idempotent: reclaiming an already-gone lease is a no-op success.
	res, err = ReclaimUnheldSetAuthorityLease(authorityRoot, id)
	require.NoError(t, err)
	require.False(t, res.Reclaimed)
	require.Equal(t, LeaseMissing, res.Verdict)
}

func TestReclaim_HeldIsRefused(t *testing.T) {
	segmentRoot := t.TempDir()
	id := fixtureIndexSetID('a')
	spawnAuthorityHolder(t, segmentRoot, id)
	authorityRoot := authorityRootFor(t, segmentRoot)
	path := filepath.Join(authorityRoot, id+".lock")

	res, err := ReclaimUnheldSetAuthorityLease(authorityRoot, id)
	require.ErrorIs(t, err, ErrSetAuthorityHeld, "a live holder must never be reclaimed")
	require.False(t, res.Reclaimed)
	require.Equal(t, LeaseHeld, res.Verdict)
	require.Equal(t, heldHolderAttribution("index-build-fixture-holder"), res.Holder,
		"the refused holder must be named wherever the doc is readable under a live lock")
	require.FileExists(t, path, "a held lease file must be left untouched")

	// Still held afterward — the refused attempt changed nothing.
	lease, err := ProbeSetAuthorityLease(authorityRoot, id)
	require.NoError(t, err)
	require.Equal(t, LeaseHeld, lease.Verdict)
}

func TestReclaim_MissingRootIsIdempotent(t *testing.T) {
	segmentRoot := t.TempDir()
	authorityRoot := authorityRootFor(t, segmentRoot)
	res, err := ReclaimUnheldSetAuthorityLease(authorityRoot, fixtureIndexSetID('a'))
	require.NoError(t, err)
	require.False(t, res.Reclaimed)
	require.Equal(t, LeaseMissing, res.Verdict)
}

func TestReclaim_InvalidDocUnheldIsRefused(t *testing.T) {
	segmentRoot := t.TempDir()
	authorityRoot := authorityRootFor(t, segmentRoot)
	id := fixtureIndexSetID('c')
	path := writeInvalidLease(t, authorityRoot, id)

	// The lock proves unheld, but not that the artifact carries the expected
	// identity. A corrupt doc is indeterminate residue for operator attention,
	// never blind removal — even when provably unheld.
	res, err := ReclaimUnheldSetAuthorityLease(authorityRoot, id)
	require.Error(t, err, "corrupt-doc residue must not be reaped on lock alone")
	require.False(t, res.Reclaimed)
	require.Equal(t, LeaseInvalid, res.Verdict)
	require.FileExists(t, path, "invalid residue is preserved for operator attention")
}

func TestReclaim_MismatchedIDIsRefused(t *testing.T) {
	segmentRoot := t.TempDir()
	authorityRoot := authorityRootFor(t, segmentRoot)
	fileID := fixtureIndexSetID('a')
	// A correctly-named, well-formed, unheld lock whose doc claims a DIFFERENT
	// index set. The lock says unheld; exact identity says do not touch it.
	path := writeLeaseWithDocID(t, authorityRoot, fileID, fixtureIndexSetID('b'))

	res, err := ReclaimUnheldSetAuthorityLease(authorityRoot, fileID)
	require.Error(t, err, "a wrong embedded index_set_id must never be reaped")
	require.False(t, res.Reclaimed)
	require.Equal(t, LeaseInvalid, res.Verdict)
	require.FileExists(t, path)
}

// TestReclaim_ValidatesUnderLock_Barrier proves the decisive doc read/validation
// runs AFTER the lock is acquired, not merely at enumeration time. The seam
// corrupts the doc in the window between lock acquisition and validation: the
// reclaim must then refuse. This is mutation-verifiable — if validation were
// moved before lockFileExclusive, the pre-lock read would see the still-valid
// doc and reap, failing this test.
func TestReclaim_ValidatesUnderLock_Barrier(t *testing.T) {
	segmentRoot := t.TempDir()
	authorityRoot := authorityRootFor(t, segmentRoot)
	id := fixtureIndexSetID('a')

	seed, err := AcquireSetAuthority(context.Background(), segmentRoot, id, "index-build-seed")
	require.NoError(t, err)
	require.NoError(t, seed.Release())
	path := filepath.Join(authorityRoot, id+".lock")

	// Enumeration sees a valid, unheld lease.
	pre, err := ProbeSetAuthorityLease(authorityRoot, id)
	require.NoError(t, err)
	require.Equal(t, LeaseUnheld, pre.Verdict)

	// Corrupt the doc exactly in the post-lock, pre-validation window.
	reclaimAfterLockHook = func() {
		_ = os.WriteFile(path, []byte("{corrupt-under-lock"), 0o600)
	}
	t.Cleanup(func() { reclaimAfterLockHook = nil })

	res, err := ReclaimUnheldSetAuthorityLease(authorityRoot, id)
	require.Error(t, err, "reclaim must validate under the lock, not before acquiring it")
	require.False(t, res.Reclaimed)
	require.Equal(t, LeaseInvalid, res.Verdict)
	require.FileExists(t, path)
}

func TestReclaim_RejectsNonCanonicalID(t *testing.T) {
	segmentRoot := t.TempDir()
	authorityRoot := authorityRootFor(t, segmentRoot)
	require.NoError(t, os.MkdirAll(authorityRoot, 0o700))
	foreign := filepath.Join(authorityRoot, "not-an-idx.lock")
	require.NoError(t, os.WriteFile(foreign, []byte("x"), 0o600))

	// A foreign, unheld artifact is operator-attention residue, never a reclaim
	// target: the mutating path acts only on well-formed canonical lease names.
	res, err := ReclaimUnheldSetAuthorityLease(authorityRoot, "not-an-idx")
	require.Error(t, err, "the mutating path must reject non-canonical ids")
	require.False(t, res.Reclaimed)
	require.Equal(t, LeaseInvalid, res.Verdict,
		"a refused reclaim must carry the same typed verdict the probe reports, never a zero value")
	require.FileExists(t, foreign, "a foreign artifact must never be removed by reclaim")
}

// TestUnlinkUnderHeldLock_RefusesSwappedBinding is the deterministic anti-split
// proof: if the pathname is swapped to a different inode while we hold the lock
// on the original inode, unlinkUnderHeldLock must refuse to remove — it must
// never delete the file that now occupies the name (a successor's live file).
func TestUnlinkUnderHeldLock_RefusesSwappedBinding(t *testing.T) {
	segmentRoot := t.TempDir()
	authorityRoot := authorityRootFor(t, segmentRoot)
	id := fixtureIndexSetID('a')

	// Create the original lease file (inode A) and hold its lock.
	seed, err := AcquireSetAuthority(context.Background(), segmentRoot, id, "index-build-original")
	require.NoError(t, err)
	require.NoError(t, seed.Release())

	resolved, err := resolveAuthorityRootForRead(authorityRoot)
	require.NoError(t, err)
	name := id + ".lock"
	root, fdA, _, err := openBoundSetAuthority(resolved, name)
	require.NoError(t, err)
	require.NoError(t, lockFileExclusive(fdA))

	// Swap: a different inode B atomically replaces the pathname while we still
	// hold the lock on A (models a successor having recreated the authority file).
	successorPath := filepath.Join(resolved, name+".successor")
	require.NoError(t, os.WriteFile(successorPath, []byte("successor-inode-B"), 0o600))
	if swapErr := os.Rename(successorPath, filepath.Join(resolved, name)); swapErr != nil {
		// Some platforms refuse to rebind a pathname whose file is open and
		// locked, so the swap this test guards against cannot be constructed
		// there: the kernel enforces at the OS layer what unlinkUnderHeldLock
		// enforces in code. Release our own descriptor and the decoy so the temp
		// dir can be torn down.
		require.True(t, lockedRangeUnreadable(swapErr), "unexpected swap failure: %v", swapErr)
		require.NoError(t, unlockFile(fdA))
		require.NoError(t, fdA.Close())
		require.NoError(t, root.Close())
		require.NoError(t, os.Remove(successorPath))
		t.Skip("platform refuses to rebind a held authority pathname; the swap is unconstructible here")
	}

	// The primitive must refuse to unlink the swapped-in successor inode.
	removed, err := unlinkUnderHeldLock(root, name, fdA)
	require.False(t, removed, "must not remove when the pathname no longer names the held inode")
	require.Error(t, err, "must refuse to unlink when the pathname no longer names the held inode")
	require.NoError(t, root.Close())

	// The successor's file (inode B) must still exist — never deleted.
	content, err := os.ReadFile(filepath.Join(resolved, name)) // #nosec G304 -- test temp path
	require.NoError(t, err)
	require.Equal(t, "successor-inode-B", string(content), "the successor inode must survive intact")
}

// TestReclaim_AdversarialRaceNeverDeletesHeldSuccessor runs a reaper and a
// competing acquirer against the same lease concurrently. Because reclaim
// unlinks only while holding the lock, a successor that holds the lock can never
// have its authority file deleted out from under it: AssertHeld must stay valid
// throughout every critical section.
func TestReclaim_AdversarialRaceNeverDeletesHeldSuccessor(t *testing.T) {
	segmentRoot := t.TempDir()
	authorityRoot := authorityRootFor(t, segmentRoot)
	id := fixtureIndexSetID('a')

	// Seed initial unheld residue.
	seed, err := AcquireSetAuthority(context.Background(), segmentRoot, id, "index-build-seed")
	require.NoError(t, err)
	require.NoError(t, seed.Release())

	var wg sync.WaitGroup
	stop := make(chan struct{})
	var heldViolations, acquisitions int64

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			a, err := AcquireSetAuthority(context.Background(), segmentRoot, id, "index-build-successor")
			if err != nil {
				continue
			}
			atomic.AddInt64(&acquisitions, 1)
			held := true
			for i := 0; i < 64; i++ {
				if a.AssertHeld() != nil {
					held = false
					break
				}
			}
			if !held {
				atomic.AddInt64(&heldViolations, 1)
			}
			_ = a.Release()
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			_, _ = ReclaimUnheldSetAuthorityLease(authorityRoot, id)
		}
	}()

	time.Sleep(500 * time.Millisecond)
	close(stop)
	wg.Wait()

	require.Zero(t, atomic.LoadInt64(&heldViolations),
		"a reaper must never unlink a successor's held authority file")
	require.Positive(t, atomic.LoadInt64(&acquisitions),
		"the successor must have actually acquired at least once for the race to be meaningful")
}
