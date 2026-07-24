package indexcoord_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/internal/leasefixture"
	"github.com/3leaps/gonimbus/pkg/indexcoord"
)

// TestLeaseInvalidMatrix_Wrapper drives the shared artifact-class matrix through
// the public coordination wrapper. The wrapper maps substrate results into its
// own types; a mapping bug there could otherwise let an artifact the library
// refuses surface as reclaimable to an API consumer.
func TestLeaseInvalidMatrix_Wrapper(t *testing.T) {
	id := leasefixture.FullID('a')
	for _, row := range leasefixture.InvalidRows() {
		t.Run(row.Name, func(t *testing.T) {
			authorityRoot, err := indexcoord.AuthorityRoot(filepath.Join(t.TempDir(), "segset"))
			require.NoError(t, err)

			planted, err := row.Plant(authorityRoot, id)
			if err != nil && row.NeedsSymlink {
				t.Skipf("symlinks unsupported on this platform: %v", err)
			}
			require.NoError(t, err)
			target := row.Target(id)

			before, err := leasefixture.TakeSnapshot(planted.Path)
			require.NoError(t, err)
			var external leasefixture.Snapshot
			if planted.External != "" {
				external, err = leasefixture.TakeSnapshot(planted.External)
				require.NoError(t, err)
			}
			preserved := func() {
				require.NoError(t, leasefixture.AssertUnchanged(before))
				if external.Path != "" {
					require.NoError(t, leasefixture.AssertUnchanged(external))
				}
			}

			rep, err := indexcoord.ProbeLease(authorityRoot, target, nil)
			require.NoError(t, err)
			require.Equal(t, indexcoord.LeaseInvalid, rep.Verdict, "the wrapper must report %s as invalid", row.Name)
			preserved()

			rr, err := indexcoord.ReclaimUnheldLease(authorityRoot, target)
			require.Error(t, err, "the wrapper must refuse %s", row.Name)
			require.False(t, rr.Reclaimed)
			require.Equal(t, indexcoord.LeaseInvalid, rr.Verdict,
				"the wrapper must propagate the typed verdict, never a zero value")
			preserved()

			// Enumeration must agree with the single-artifact verdict. Assert
			// cardinality and identity FIRST: a range over an empty slice would pass
			// vacuously, so silently dropping the artifact from the listing must fail
			// here rather than look like agreement.
			reports, err := indexcoord.EnumerateLeases(authorityRoot, nil)
			require.NoError(t, err)
			require.Len(t, reports, 1, "enumeration must surface %s, never drop it", row.Name)
			require.Equal(t, target, reports[0].IndexSetID, "enumeration must report the planted artifact")
			require.Equal(t, indexcoord.LeaseInvalid, reports[0].Verdict,
				"enumeration must classify %s the same way the probe does", row.Name)
			preserved()
		})
	}
}

// TestLeaseValidUnheld_Wrapper is the positive control for the wrapper matrix.
func TestLeaseValidUnheld_Wrapper(t *testing.T) {
	authorityRoot, err := indexcoord.AuthorityRoot(filepath.Join(t.TempDir(), "segset"))
	require.NoError(t, err)
	id := leasefixture.FullID('a')
	planted, err := leasefixture.PlantValidUnheld(authorityRoot, id)
	require.NoError(t, err)

	rep, err := indexcoord.ProbeLease(authorityRoot, id, nil)
	require.NoError(t, err)
	require.Equal(t, indexcoord.LeaseUnheld, rep.Verdict)

	rr, err := indexcoord.ReclaimUnheldLease(authorityRoot, id)
	require.NoError(t, err)
	require.True(t, rr.Reclaimed)
	require.Equal(t, indexcoord.LeaseUnheld, rr.Verdict)
	require.NoFileExists(t, planted.Path)
}

// TestLeaseMalformedTarget_WrapperTypedVerdict pins typed-state parity on the
// wrapper's error path: an artifact-grounded refusal carries LeaseInvalid, not a
// zero-value report an API consumer would have to interpret.
func TestLeaseMalformedTarget_WrapperTypedVerdict(t *testing.T) {
	authorityRoot, err := indexcoord.AuthorityRoot(filepath.Join(t.TempDir(), "segset"))
	require.NoError(t, err)

	for _, target := range []string{"", "../outside"} {
		rep, probeErr := indexcoord.ProbeLease(authorityRoot, target, nil)
		require.Error(t, probeErr)
		require.Equal(t, indexcoord.LeaseInvalid, rep.Verdict)

		rr, reclaimErr := indexcoord.ReclaimUnheldLease(authorityRoot, target)
		require.Error(t, reclaimErr)
		require.False(t, rr.Reclaimed)
		require.Equal(t, indexcoord.LeaseInvalid, rr.Verdict)
	}
}

// TestLeaseInfrastructureFailure_WrapperClaimsNoVerdict pins the other half of
// the refusal contract at the public boundary: the wrapper propagates the
// library's silence about an artifact it never examined, rather than
// substituting a verdict of its own.
func TestLeaseInfrastructureFailure_WrapperClaimsNoVerdict(t *testing.T) {
	id := leasefixture.FullID('a')
	fileRoot := filepath.Join(t.TempDir(), "root-is-a-file")
	require.NoError(t, os.WriteFile(fileRoot, []byte("not-a-directory"), 0o600))

	for name, root := range map[string]string{"empty_root": "", "non_directory_root": fileRoot} {
		t.Run(name, func(t *testing.T) {
			rep, probeErr := indexcoord.ProbeLease(root, id, nil)
			require.Error(t, probeErr)
			require.Empty(t, rep.Verdict, "the wrapper must not invent a verdict the library never reached")

			rr, reclaimErr := indexcoord.ReclaimUnheldLease(root, id)
			require.Error(t, reclaimErr)
			require.False(t, rr.Reclaimed)
			require.Empty(t, rr.Verdict, "the wrapper must not invent a verdict the library never reached")
		})
	}
}
