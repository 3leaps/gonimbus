package indexsubstrate

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestSetAuthorityHeldMalformed_LockWins proves the OS lock is the sole
// live-holder verdict: a live holder whose doc has been corrupted still probes
// held and is never reclaimed. It lives in the in-package test file because it
// needs the re-exec holder fixture; the artifact-class matrix that every layer
// shares lives in the external test package (set_authority_matrix_ext_test.go).
func TestSetAuthorityHeldMalformed_LockWins(t *testing.T) {
	segmentRoot := t.TempDir()
	id := fixtureIndexSetID('a')
	spawnAuthorityHolder(t, segmentRoot, id)
	authorityRoot := authorityRootFor(t, segmentRoot)
	path := filepath.Join(authorityRoot, id+".lock")

	// Corrupt the doc while the holder still holds the lock. Under advisory
	// locking the write lands (locks do not gate writes) and the probe then sees
	// a garbage doc under a live lock. Under mandatory locking the write is
	// refused outright and the doc is equally unusable to the probe. Either way
	// the assertions below pin the same property: the verdict comes from the
	// lock, never from the document.
	if corruptErr := os.WriteFile(path, []byte("{corrupt-but-held"), 0o600); corruptErr != nil {
		require.True(t, lockedRangeUnreadable(corruptErr), "unexpected corrupt-write failure: %v", corruptErr)
	}

	lease, err := ProbeSetAuthorityLease(authorityRoot, id)
	require.NoError(t, err)
	require.Equal(t, LeaseHeld, lease.Verdict, "a live holder wins over a garbage doc")

	res, err := ReclaimUnheldSetAuthorityLease(authorityRoot, id)
	require.ErrorIs(t, err, ErrSetAuthorityHeld, "a held lease is never reclaimed regardless of doc quality")
	require.False(t, res.Reclaimed)
	require.FileExists(t, path)
}
