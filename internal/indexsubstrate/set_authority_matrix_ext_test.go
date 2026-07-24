package indexsubstrate_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/internal/indexsubstrate"
	"github.com/3leaps/gonimbus/internal/leasefixture"
)

// writeOutsideVictim plants a file OUTSIDE the authority root that a traversal
// target would reach if the name gate ever let one through.
func writeOutsideVictim(path string) error {
	return os.WriteFile(path, []byte("outside-the-authority-root"), 0o600)
}

// PlantRow plants row under a fresh authority root and returns the root, the
// probe/reclaim target, and a snapshot pair (artifact + any external symlink
// target) so a caller can prove nothing moved. Shared shape with the wrapper and
// CLI matrices; each layer supplies its own authority root.
func plantRow(t *testing.T, authorityRoot, id string, row leasefixture.Row) (target string, before, external leasefixture.Snapshot) {
	t.Helper()
	planted, err := row.Plant(authorityRoot, id)
	if err != nil && row.NeedsSymlink {
		t.Skipf("symlinks unsupported on this platform: %v", err)
	}
	require.NoError(t, err)

	before, err = leasefixture.TakeSnapshot(planted.Path)
	require.NoError(t, err)
	if planted.External != "" {
		external, err = leasefixture.TakeSnapshot(planted.External)
		require.NoError(t, err)
	}
	return row.Target(id), before, external
}

// assertPreserved proves the planted artifact and any external symlink target
// survived the observation byte-for-byte.
func assertPreserved(t *testing.T, before, external leasefixture.Snapshot) {
	t.Helper()
	require.NoError(t, leasefixture.AssertUnchanged(before), "the artifact must be preserved for operator attention")
	if external.Path != "" {
		require.NoError(t, leasefixture.AssertUnchanged(external), "a symlink target outside the lease must never be touched")
	}
}

func substrateAuthorityRoot(t *testing.T) string {
	t.Helper()
	root, err := indexsubstrate.SetAuthorityRootForSegmentSet(t.TempDir())
	require.NoError(t, err)
	return root
}

// TestSetAuthorityInvalidMatrix_Substrate drives every artifact class through the
// substrate probe and the substrate reclaim. Each row must reach the typed
// invalid verdict, must never be reclaimed, and must survive byte-for-byte —
// including a symlink's external target, which proves no layer follows the link.
func TestSetAuthorityInvalidMatrix_Substrate(t *testing.T) {
	id := leasefixture.FullID('a')
	for _, row := range leasefixture.InvalidRows() {
		t.Run(row.Name, func(t *testing.T) {
			authorityRoot := substrateAuthorityRoot(t)
			target, before, external := plantRow(t, authorityRoot, id, row)

			lease, err := indexsubstrate.ProbeSetAuthorityLease(authorityRoot, target)
			require.NoError(t, err)
			require.Equal(t, indexsubstrate.LeaseInvalid, lease.Verdict, "probe must classify %s as invalid", row.Name)
			assertPreserved(t, before, external)

			res, err := indexsubstrate.ReclaimUnheldSetAuthorityLease(authorityRoot, target)
			require.Error(t, err, "reclaim must refuse %s", row.Name)
			require.False(t, res.Reclaimed)
			require.Equal(t, indexsubstrate.LeaseInvalid, res.Verdict,
				"reclaim must return the same typed verdict the probe reports, never a zero value")
			assertPreserved(t, before, external)
		})
	}
}

// TestSetAuthorityValidUnheld_Substrate is the positive control the matrix is
// measured against: a canonical, well-formed, unheld artifact IS reclaimed.
// Without it, a gate that rejected everything would pass the whole matrix.
func TestSetAuthorityValidUnheld_Substrate(t *testing.T) {
	authorityRoot := substrateAuthorityRoot(t)
	id := leasefixture.FullID('a')
	planted, err := leasefixture.PlantValidUnheld(authorityRoot, id)
	require.NoError(t, err)

	lease, err := indexsubstrate.ProbeSetAuthorityLease(authorityRoot, id)
	require.NoError(t, err)
	require.Equal(t, indexsubstrate.LeaseUnheld, lease.Verdict)

	res, err := indexsubstrate.ReclaimUnheldSetAuthorityLease(authorityRoot, id)
	require.NoError(t, err)
	require.True(t, res.Reclaimed)
	require.Equal(t, indexsubstrate.LeaseUnheld, res.Verdict)
	require.NoFileExists(t, planted.Path)
}

// TestSetAuthorityInfrastructureFailure_ClaimsNoVerdict is the other half of the
// refusal contract. An unusable authority root is not a judgement about any
// artifact — nothing was examined — so the call must return an error WITHOUT
// claiming a state. Asserting the absence of a verdict is deliberate: a
// manufactured "invalid" here would be indistinguishable from one an actual
// probe reached, which is exactly the ambiguity the narrowed contract forbids.
func TestSetAuthorityInfrastructureFailure_ClaimsNoVerdict(t *testing.T) {
	id := leasefixture.FullID('a')

	fileRoot := filepath.Join(t.TempDir(), "root-is-a-file")
	require.NoError(t, writeOutsideVictim(fileRoot))

	linkRoot := filepath.Join(t.TempDir(), "root-is-a-symlink")
	realDir := t.TempDir()
	symlinkSupported := os.Symlink(realDir, linkRoot) == nil

	roots := map[string]string{
		"empty_root":         "",
		"non_directory_root": fileRoot,
	}
	if symlinkSupported {
		roots["symlinked_root"] = linkRoot
	}

	for name, root := range roots {
		t.Run(name, func(t *testing.T) {
			lease, probeErr := indexsubstrate.ProbeSetAuthorityLease(root, id)
			require.Error(t, probeErr, "an unusable authority root must be an error")
			require.Empty(t, lease.Verdict,
				"an infrastructure failure must claim no artifact state — nothing was examined")

			res, reclaimErr := indexsubstrate.ReclaimUnheldSetAuthorityLease(root, id)
			require.Error(t, reclaimErr, "an unusable authority root must be an error")
			require.False(t, res.Reclaimed)
			require.Empty(t, res.Verdict,
				"an infrastructure failure must claim no artifact state — nothing was examined")
		})
	}
}

// TestSetAuthorityMalformedTarget_TypedVerdict pins the typed state on the
// rejection path that never reaches the filesystem: a target carrying a path
// separator (or an empty target) is refused with an error, and that error still
// carries LeaseInvalid rather than a zero-value verdict.
func TestSetAuthorityMalformedTarget_TypedVerdict(t *testing.T) {
	authorityRoot := substrateAuthorityRoot(t)
	victim := filepath.Join(filepath.Dir(authorityRoot), "outside.lock")
	require.NoError(t, writeOutsideVictim(victim))
	before, err := leasefixture.TakeSnapshot(victim)
	require.NoError(t, err)

	for _, target := range []string{"", "../outside", "idx_/../../etc"} {
		lease, probeErr := indexsubstrate.ProbeSetAuthorityLease(authorityRoot, target)
		require.Error(t, probeErr, "a malformed target %q must be refused", target)
		require.Equal(t, indexsubstrate.LeaseInvalid, lease.Verdict, "a refused probe still reports a typed state")

		res, reclaimErr := indexsubstrate.ReclaimUnheldSetAuthorityLease(authorityRoot, target)
		require.Error(t, reclaimErr, "a malformed target %q must be refused", target)
		require.False(t, res.Reclaimed)
		require.Equal(t, indexsubstrate.LeaseInvalid, res.Verdict, "a refused reclaim still reports a typed state")
	}
	require.NoError(t, leasefixture.AssertUnchanged(before), "a traversal target must never reach a file outside the authority root")
}
