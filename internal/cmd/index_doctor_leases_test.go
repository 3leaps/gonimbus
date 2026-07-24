package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/internal/leasefixture"
)

// execIndexDoctor executes the real Cobra doctor command — flag parsing, RunE, and
// all. Calling the guard helper directly would leave the command path unproven:
// deleting the guard call from runIndexDoctor must fail these tests, and it can
// only do that if they go through the command.
func execIndexDoctor(t *testing.T, args ...string) (string, error) {
	t.Helper()
	cmd := newIndexDoctorCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(args)
	err := cmd.ExecuteContext(context.Background())
	return out.String(), err
}

// TestIndexDoctorCommandPath_RejectsIncompatibleFlags proves every incompatible
// combination is refused by the command itself, before any target resolution,
// listing, or reclaim can run.
func TestIndexDoctorCommandPath_RejectsIncompatibleFlags(t *testing.T) {
	cases := []struct {
		name string
		args []string
	}{
		{"leases_with_root", []string{"--leases", "--root", "/tmp/decoy"}},
		{"leases_with_db", []string{"--leases", "--db", "/tmp/decoy/index.db"}},
		{"leases_with_format", []string{"--leases", "--format", "durable-v2"}},
		{"leases_with_stats", []string{"--leases", "--stats"}},
		{"leases_with_detail", []string{"--leases", "--detail"}},
		{"leases_with_verbose", []string{"--leases", "--verbose"}},
		{"release_stale_with_root", []string{"--release-stale", "--confirm", "--root", "/tmp/decoy"}},
		{"release_stale_with_db", []string{"--release-stale", "--confirm", "--db", "/tmp/decoy/index.db"}},
		{"leases_with_release_stale", []string{"--leases", "--release-stale"}},
		// A lease-mutation opt-in without the mutating mode is an operator error,
		// not a silently-ignored flag — including on plain `index doctor`.
		{"confirm_without_release_stale", []string{"--confirm"}},
		{"force_without_release_stale", []string{"--force"}},
		{"leases_with_confirm", []string{"--leases", "--confirm"}},
		{"leases_with_force", []string{"--leases", "--force"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			setLeaseDataRoot(t)
			_, err := execIndexDoctor(t, tc.args...)
			require.Error(t, err, "doctor must reject %v on the command path", tc.args)
		})
	}
}

// TestIndexDoctorCommandPath_ExplicitRootNeverMutatesDefault is the two-root
// proof. An explicit --root names a decoy while the destructive lease mode would
// derive the DEFAULT authority root: the command must refuse, and neither root
// may change. If the guard call were removed from runIndexDoctor, the default
// root's lease would be reclaimed here and this test would fail.
func TestIndexDoctorCommandPath_ExplicitRootNeverMutatesDefault(t *testing.T) {
	scr := setLeaseDataRoot(t)
	defaultAuthorityRoot := leaseAuthorityRootUnderCache(scr)
	id := leasefixture.FullID('a')

	// A genuinely reclaimable lease in the DEFAULT store: this is what a removed
	// guard would delete.
	planted, err := leasefixture.PlantValidUnheld(defaultAuthorityRoot, id)
	require.NoError(t, err)
	defaultBefore, err := leasefixture.TakeSnapshot(planted.Path)
	require.NoError(t, err)
	require.True(t, defaultBefore.Exists)

	// A separate decoy root the operator actually named.
	decoyRoot := t.TempDir()
	decoyMarker := filepath.Join(decoyRoot, "decoy-marker")
	require.NoError(t, os.WriteFile(decoyMarker, []byte("decoy"), 0o600))
	decoyBefore, err := leasefixture.TakeSnapshot(decoyMarker)
	require.NoError(t, err)

	_, cmdErr := execIndexDoctor(t, "--release-stale", "--confirm", "--root", decoyRoot)

	// Assert the no-mutation property BEFORE the error, so removing the guard call
	// fails on the mutation it actually permits (the default store's lease being
	// reclaimed) rather than merely on a missing error.
	require.NoError(t, leasefixture.AssertUnchanged(defaultBefore), "the default store must not be mutated when an explicit --root names somewhere else")
	require.NoError(t, leasefixture.AssertUnchanged(decoyBefore), "the named decoy root must not be mutated either")
	require.FileExists(t, planted.Path)
	require.Error(t, cmdErr, "a destructive lease mode must never silently retarget the default store")
}

// TestIndexDoctorCommandPath_PositiveControls proves the guard did not simply
// disable the lease surface: clean modes still behave exactly as specified.
func TestIndexDoctorCommandPath_PositiveControls(t *testing.T) {
	t.Run("leases_lists", func(t *testing.T) {
		scr := setLeaseDataRoot(t)
		id := leasefixture.FullID('a')
		_, err := leasefixture.PlantValidUnheld(leaseAuthorityRootUnderCache(scr), id)
		require.NoError(t, err)

		out, err := execIndexDoctor(t, "--leases", "--json")
		require.NoError(t, err)
		var rows []leaseReportJSON
		require.NoError(t, json.Unmarshal([]byte(out), &rows))
		require.Len(t, rows, 1)
		require.Equal(t, "unheld", rows[0].Verdict)
	})

	t.Run("release_stale_dry_run_never_mutates", func(t *testing.T) {
		scr := setLeaseDataRoot(t)
		id := leasefixture.FullID('a')
		planted, err := leasefixture.PlantValidUnheld(leaseAuthorityRootUnderCache(scr), id)
		require.NoError(t, err)
		before, err := leasefixture.TakeSnapshot(planted.Path)
		require.NoError(t, err)

		_, err = execIndexDoctor(t, "--release-stale")
		require.NoError(t, err)
		require.NoError(t, leasefixture.AssertUnchanged(before), "a dry run must never remove a lease")
	})

	t.Run("release_stale_confirm_reclaims", func(t *testing.T) {
		scr := setLeaseDataRoot(t)
		id := leasefixture.FullID('a')
		planted, err := leasefixture.PlantValidUnheld(leaseAuthorityRootUnderCache(scr), id)
		require.NoError(t, err)

		_, err = execIndexDoctor(t, "--release-stale", "--confirm")
		require.NoError(t, err)
		require.NoFileExists(t, planted.Path, "confirmed reclaim must remove provably-unheld residue")
	})
}
