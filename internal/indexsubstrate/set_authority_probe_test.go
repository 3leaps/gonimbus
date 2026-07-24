package indexsubstrate

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// authorityRootFor returns the resolved authority root for a segment-set root,
// matching what AcquireSetAuthority uses.
func authorityRootFor(t *testing.T, segmentSetRoot string) string {
	t.Helper()
	root, err := SetAuthorityRootForSegmentSet(segmentSetRoot)
	require.NoError(t, err)
	return root
}

func TestSetAuthorityProbe_HeldThenUnheld(t *testing.T) {
	segmentRoot := t.TempDir()
	id := fixtureIndexSetID('a')

	holder := spawnAuthorityHolder(t, segmentRoot, id)
	authorityRoot := authorityRootFor(t, segmentRoot)

	held, err := ProbeSetAuthorityLease(authorityRoot, id)
	require.NoError(t, err)
	require.Equal(t, LeaseHeld, held.Verdict, "a live holder must probe as held")
	// Attribution comes from the on-disk doc wherever a live lock leaves it
	// readable. Under a mandatory lock the doc is unreadable until the holder
	// exits, so attribution is absent — but the verdict above is unchanged,
	// because the lock alone decides held.
	require.Equal(t, heldHolderAttribution("index-build-fixture-holder"), held.Holder)
	if held.Holder != "" {
		require.Equal(t, setAuthorityDocType, held.DocType)
		require.False(t, held.AcquiredAt.IsZero())
	}

	holder.killAndWaitUnheld(authorityRoot, id)

	unheld, err := ProbeSetAuthorityLease(authorityRoot, id)
	require.NoError(t, err)
	require.Equal(t, LeaseUnheld, unheld.Verdict, "dead-holder residue must probe as unheld")
	// Attribution survives the holder's death (doc is untouched by the kill).
	require.Equal(t, "index-build-fixture-holder", unheld.Holder)
}

func TestSetAuthorityProbe_Missing(t *testing.T) {
	segmentRoot := t.TempDir()
	authorityRoot := authorityRootFor(t, segmentRoot)

	// Authority root does not exist yet.
	lease, err := ProbeSetAuthorityLease(authorityRoot, fixtureIndexSetID('a'))
	require.NoError(t, err)
	require.Equal(t, LeaseMissing, lease.Verdict)

	// Authority root exists but this set has no lock file.
	require.NoError(t, os.MkdirAll(authorityRoot, 0o700))
	lease, err = ProbeSetAuthorityLease(authorityRoot, fixtureIndexSetID('b'))
	require.NoError(t, err)
	require.Equal(t, LeaseMissing, lease.Verdict)
}

func TestSetAuthorityProbe_InvalidDoc(t *testing.T) {
	segmentRoot := t.TempDir()
	authorityRoot := authorityRootFor(t, segmentRoot)
	id := fixtureIndexSetID('c')
	writeInvalidLease(t, authorityRoot, id)

	lease, err := ProbeSetAuthorityLease(authorityRoot, id)
	require.NoError(t, err)
	require.Equal(t, LeaseInvalid, lease.Verdict, "an unparseable authority doc is indeterminate, not unheld")
	require.NotEmpty(t, lease.Detail)
}

func TestSetAuthorityProbe_MismatchedIDIsInvalid(t *testing.T) {
	segmentRoot := t.TempDir()
	authorityRoot := authorityRootFor(t, segmentRoot)
	fileID := fixtureIndexSetID('a')
	// Correctly named, well-formed, unheld — but the doc claims a different set.
	writeLeaseWithDocID(t, authorityRoot, fileID, fixtureIndexSetID('b'))

	lease, err := ProbeSetAuthorityLease(authorityRoot, fileID)
	require.NoError(t, err)
	require.Equal(t, LeaseInvalid, lease.Verdict, "a doc whose index_set_id does not match the lease name is invalid, not unheld")
	require.NotEmpty(t, lease.Detail)
}

func TestSetAuthorityEnumerate_DirectoryLockIsInvalid(t *testing.T) {
	segmentRoot := t.TempDir()
	authorityRoot := authorityRootFor(t, segmentRoot)
	require.NoError(t, os.MkdirAll(authorityRoot, 0o700))
	dirID := fixtureIndexSetID('a')
	require.NoError(t, os.Mkdir(filepath.Join(authorityRoot, dirID+".lock"), 0o700))

	leases, err := EnumerateSetAuthorityLeases(authorityRoot)
	require.NoError(t, err)
	require.Len(t, leases, 1)
	require.Equal(t, dirID, leases[0].IndexSetID)
	require.Equal(t, LeaseInvalid, leases[0].Verdict, "a directory named like a lock must surface as invalid, not vanish")
}

func TestSetAuthorityEnumerate_SymlinkLockIsInvalidNotFollowed(t *testing.T) {
	segmentRoot := t.TempDir()
	authorityRoot := authorityRootFor(t, segmentRoot)
	require.NoError(t, os.MkdirAll(authorityRoot, 0o700))
	symID := fixtureIndexSetID('a')
	// Point the symlink at a real regular file to prove enumeration does not
	// follow it (following would misreport the target as the lease).
	target := filepath.Join(authorityRoot, "target-regular")
	require.NoError(t, os.WriteFile(target, []byte("not-a-lock"), 0o600))
	if err := os.Symlink(target, filepath.Join(authorityRoot, symID+".lock")); err != nil {
		t.Skipf("symlinks unsupported on this platform: %v", err)
	}

	leases, err := EnumerateSetAuthorityLeases(authorityRoot)
	require.NoError(t, err)
	var found *SetAuthorityLease
	for i := range leases {
		if leases[i].IndexSetID == symID {
			found = &leases[i]
		}
	}
	require.NotNil(t, found, "a symlink .lock must appear in the listing")
	require.Equal(t, LeaseInvalid, found.Verdict, "a symlink .lock must be invalid, reported without being followed")
}

// TestSetAuthorityProbe_ZeroMutation is the load-bearing zero-mutation gate: the
// probe must leave the lock file byte-identical across every verdict.
func TestSetAuthorityProbe_ZeroMutation(t *testing.T) {
	t.Run("held", func(t *testing.T) {
		segmentRoot := t.TempDir()
		id := fixtureIndexSetID('a')
		spawnAuthorityHolder(t, segmentRoot, id)
		authorityRoot := authorityRootFor(t, segmentRoot)
		path := filepath.Join(authorityRoot, id+".lock")

		before := snapshotLockDoc(t, path)
		lease, err := ProbeSetAuthorityLease(authorityRoot, id)
		require.NoError(t, err)
		require.Equal(t, LeaseHeld, lease.Verdict)
		before.assertUnchanged(t, path)
	})

	t.Run("unheld", func(t *testing.T) {
		segmentRoot := t.TempDir()
		id := fixtureIndexSetID('a')
		holder := spawnAuthorityHolder(t, segmentRoot, id)
		authorityRoot := authorityRootFor(t, segmentRoot)
		holder.killAndWaitUnheld(authorityRoot, id)
		path := filepath.Join(authorityRoot, id+".lock")

		before := snapshotLockDoc(t, path)
		lease, err := ProbeSetAuthorityLease(authorityRoot, id)
		require.NoError(t, err)
		require.Equal(t, LeaseUnheld, lease.Verdict)
		before.assertUnchanged(t, path)
	})

	t.Run("invalid", func(t *testing.T) {
		segmentRoot := t.TempDir()
		authorityRoot := authorityRootFor(t, segmentRoot)
		id := fixtureIndexSetID('c')
		path := writeInvalidLease(t, authorityRoot, id)

		before := snapshotLockDoc(t, path)
		lease, err := ProbeSetAuthorityLease(authorityRoot, id)
		require.NoError(t, err)
		require.Equal(t, LeaseInvalid, lease.Verdict)
		before.assertUnchanged(t, path)
	})
}

// TestSetAuthorityProbe_TruncationIsDetected is the mutation control: it proves
// the byte-identical assertion used by the zero-mutation gate is load-bearing —
// a probe that truncated the doc (as AcquireSetAuthority does) would be caught.
func TestSetAuthorityProbe_TruncationIsDetected(t *testing.T) {
	segmentRoot := t.TempDir()
	holder := spawnAuthorityHolder(t, segmentRoot, fixtureIndexSetID('a'))
	authorityRoot := authorityRootFor(t, segmentRoot)
	holder.killAndWaitUnheld(authorityRoot, fixtureIndexSetID('a'))
	path := filepath.Join(authorityRoot, fixtureIndexSetID('a')+".lock")

	before := snapshotLockDoc(t, path)
	require.NotEmpty(t, before.content, "fixture must write a non-empty authority doc")

	// Simulate a mutating probe.
	require.NoError(t, os.Truncate(path, 0))
	after := snapshotLockDoc(t, path)
	require.NotEqual(t, before.content, after.content, "the equality assertion must detect a truncating probe")
	require.NotEqual(t, before.size, after.size)
}

func TestSetAuthorityEnumerate_MixedVerdicts(t *testing.T) {
	segmentRoot := t.TempDir()
	authorityRoot := authorityRootFor(t, segmentRoot)

	heldID := fixtureIndexSetID('a')
	unheldID := fixtureIndexSetID('b')
	invalidID := fixtureIndexSetID('c')

	// held: a live child.
	spawnAuthorityHolder(t, segmentRoot, heldID)
	// unheld residue: a killed child.
	unheldHolder := spawnAuthorityHolder(t, segmentRoot, unheldID)
	unheldHolder.killAndWaitUnheld(authorityRoot, unheldID)
	// invalid: corrupt doc.
	writeInvalidLease(t, authorityRoot, invalidID)
	// foreign artifact: a .lock file whose name is not a valid index-set ID.
	require.NoError(t, os.WriteFile(filepath.Join(authorityRoot, "not-an-idx.lock"), []byte("x"), 0o600))

	leases, err := EnumerateSetAuthorityLeases(authorityRoot)
	require.NoError(t, err)

	byID := make(map[string]SetAuthorityLease, len(leases))
	for _, l := range leases {
		byID[l.IndexSetID] = l
	}
	require.Equal(t, LeaseHeld, byID[heldID].Verdict)
	require.Equal(t, LeaseUnheld, byID[unheldID].Verdict)
	require.Equal(t, LeaseInvalid, byID[invalidID].Verdict)
	require.Equal(t, LeaseInvalid, byID["not-an-idx"].Verdict, "foreign .lock artifacts are reported, not dropped")

	// Deterministic sort by index-set ID.
	for i := 1; i < len(leases); i++ {
		require.LessOrEqual(t, leases[i-1].IndexSetID, leases[i].IndexSetID)
	}
}

func TestSetAuthorityEnumerate_MissingRootIsEmpty(t *testing.T) {
	segmentRoot := t.TempDir()
	authorityRoot := authorityRootFor(t, segmentRoot)
	leases, err := EnumerateSetAuthorityLeases(authorityRoot)
	require.NoError(t, err)
	require.Empty(t, leases)
}
