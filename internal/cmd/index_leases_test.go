package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/fulmenhq/gofulmen/appidentity"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/internal/indexsubstrate"
	"github.com/3leaps/gonimbus/pkg/indexcoord"
)

// setLeaseDataRoot points the CLI's app-data resolution at a temp dir and
// returns the resulting segment-cache root, under which set-authority leases
// live at <segmentCacheRoot>/.gonimbus-set-authority/.
func setLeaseDataRoot(t *testing.T) string {
	t.Helper()
	oldIdentity := appIdentity
	appIdentity = &appidentity.Identity{BinaryName: "gonimbus", ConfigName: "gonimbus", EnvPrefix: "GONIMBUS_"}
	t.Cleanup(func() { appIdentity = oldIdentity })
	dataRoot := t.TempDir()
	t.Setenv("GONIMBUS_DATA_DIR", dataRoot)
	t.Setenv("GONIMBUS_DATA_ROOT", "")
	t.Setenv("XDG_DATA_HOME", "")
	scr, err := appDataPath(appDataClassSegmentCache)
	require.NoError(t, err)
	return scr
}

func leaseFullID(seed rune) string { return "idx_" + strings.Repeat(string(seed), 64) }

// acquireLeaseUnderCache acquires a set authority whose lock file lands under the
// CLI's segment-cache authority root. The caller decides whether to release
// (leaving unheld residue) or hold (a live holder).
func acquireLeaseUnderCache(t *testing.T, segmentCacheRoot, id, holder string) *indexsubstrate.SetAuthority {
	t.Helper()
	segmentSetRoot := filepath.Join(segmentCacheRoot, id)
	auth, err := indexsubstrate.AcquireSetAuthority(context.Background(), segmentSetRoot, id, holder)
	require.NoError(t, err)
	return auth
}

func leaseLockPath(segmentCacheRoot, id string) string {
	return filepath.Join(segmentCacheRoot, indexsubstrate.SetAuthorityDirectoryName, id+".lock")
}

func TestIndexLeaseLs_JSONReportsVerdicts(t *testing.T) {
	scr := setLeaseDataRoot(t)
	id := leaseFullID('a')
	auth := acquireLeaseUnderCache(t, scr, id, "index-build-JOB1")
	require.NoError(t, auth.Release()) // unheld residue

	var buf bytes.Buffer
	require.NoError(t, listLeases(&buf, true))

	var rows []leaseReportJSON
	require.NoError(t, json.Unmarshal(buf.Bytes(), &rows))
	require.Len(t, rows, 1)
	require.Equal(t, leaseReportDocType, rows[0].Type)
	require.Equal(t, id, rows[0].IndexSetID)
	require.Equal(t, "unheld", rows[0].Verdict)
	require.Equal(t, "index-build-JOB1", rows[0].Holder)
}

func TestIndexLeaseReap_DryRunThenConfirm(t *testing.T) {
	scr := setLeaseDataRoot(t)
	id := leaseFullID('a')
	auth := acquireLeaseUnderCache(t, scr, id, "index-build-seed")
	require.NoError(t, auth.Release())
	lockPath := leaseLockPath(scr, id)
	require.FileExists(t, lockPath)

	// Dry run must not remove anything.
	var dry bytes.Buffer
	require.NoError(t, reapLeases(&dry, nil, false, true))
	require.FileExists(t, lockPath, "dry run must never remove a lease")
	var dryRows []leaseReapJSON
	require.NoError(t, json.Unmarshal(dry.Bytes(), &dryRows))
	require.Len(t, dryRows, 1)
	require.True(t, dryRows[0].DryRun)
	require.False(t, dryRows[0].Reclaimed)

	// Confirm removes the unheld residue.
	var done bytes.Buffer
	require.NoError(t, reapLeases(&done, nil, true, true))
	var rows []leaseReapJSON
	require.NoError(t, json.Unmarshal(done.Bytes(), &rows))
	require.Len(t, rows, 1)
	require.True(t, rows[0].Reclaimed)
	require.NoFileExists(t, lockPath)
}

func TestIndexLeaseReap_RefusesHeld(t *testing.T) {
	scr := setLeaseDataRoot(t)
	id := leaseFullID('a')
	// Hold the lock for the duration of the test (a second open contends on flock
	// even within this process).
	auth := acquireLeaseUnderCache(t, scr, id, "index-build-holder")
	t.Cleanup(func() { _ = auth.Release() })
	lockPath := leaseLockPath(scr, id)

	var buf bytes.Buffer
	require.NoError(t, reapLeases(&buf, []string{id}, true, true))
	var rows []leaseReapJSON
	require.NoError(t, json.Unmarshal(buf.Bytes(), &rows))
	require.Len(t, rows, 1)
	require.True(t, rows[0].Refused, "a held lease must be refused")
	require.False(t, rows[0].Reclaimed)
	require.Equal(t, "held", rows[0].Verdict)
	require.FileExists(t, lockPath, "a held lease file must be left untouched")
}

func TestIndexLeaseReap_SkipsInvalid(t *testing.T) {
	scr := setLeaseDataRoot(t)
	id := leaseFullID('a')
	lockPath := leaseLockPath(scr, id)
	require.NoError(t, os.MkdirAll(filepath.Dir(lockPath), 0o700))
	// A valid-ID lease with an unparseable doc probes as invalid/indeterminate.
	require.NoError(t, os.WriteFile(lockPath, []byte("{corrupt"), 0o600))

	var buf bytes.Buffer
	require.NoError(t, reapLeases(&buf, []string{id}, true, true))
	var rows []leaseReapJSON
	require.NoError(t, json.Unmarshal(buf.Bytes(), &rows))
	require.Len(t, rows, 1)
	require.True(t, rows[0].Skipped, "an invalid lease must be skipped for operator attention, not reaped")
	require.False(t, rows[0].Reclaimed)
	require.FileExists(t, lockPath, "an invalid lease must never be removed by reap")
}

func TestIndexLeaseReap_SkipsMismatchedID(t *testing.T) {
	scr := setLeaseDataRoot(t)
	fileID := leaseFullID('a')
	lockPath := leaseLockPath(scr, fileID)
	require.NoError(t, os.MkdirAll(filepath.Dir(lockPath), 0o700))
	// Well-formed JSON, correct doc type, but the embedded id names a different
	// set: the CLI must classify it invalid and skip it, not reap it.
	body := `{"type":"gonimbus.index.set_authority.v1","index_set_id":"` + leaseFullID('b') +
		`","holder":"index-build-x","acquired_at":"2026-01-01T00:00:00Z"}`
	require.NoError(t, os.WriteFile(lockPath, []byte(body), 0o600))

	var buf bytes.Buffer
	require.NoError(t, reapLeases(&buf, []string{fileID}, true, true))
	var rows []leaseReapJSON
	require.NoError(t, json.Unmarshal(buf.Bytes(), &rows))
	require.Len(t, rows, 1)
	require.True(t, rows[0].Skipped, "a mismatched-id lease must be skipped, not reaped")
	require.False(t, rows[0].Reclaimed)
	require.FileExists(t, lockPath)
}

func TestGuardDoctorLeaseFlags(t *testing.T) {
	set := func(c *cobra.Command, flags ...string) *cobra.Command {
		for _, f := range flags {
			require.NoError(t, c.Flags().Set(f, "true"))
		}
		return c
	}

	// Health/target flag with a lease mode is rejected (an explicit --root can
	// never silently mutate the default store because it errors out first).
	require.Error(t, guardDoctorLeaseFlags(set(newIndexDoctorCommand(), "leases", "root"), true, false))
	require.Error(t, guardDoctorLeaseFlags(set(newIndexDoctorCommand(), "release-stale", "db"), false, true))
	// --leases and --release-stale are mutually exclusive.
	require.Error(t, guardDoctorLeaseFlags(set(newIndexDoctorCommand(), "leases", "release-stale"), true, true))
	// --confirm/--force require --release-stale.
	require.Error(t, guardDoctorLeaseFlags(set(newIndexDoctorCommand(), "leases", "confirm"), true, false))
	require.Error(t, guardDoctorLeaseFlags(set(newIndexDoctorCommand(), "leases", "force"), true, false))
	// Clean lease modes pass.
	require.NoError(t, guardDoctorLeaseFlags(set(newIndexDoctorCommand(), "leases"), true, false))
	require.NoError(t, guardDoctorLeaseFlags(set(newIndexDoctorCommand(), "release-stale", "confirm"), false, true))
}

// TestIndexLeaseReap_CLIMatchesLibrary proves the CLI reaps exactly what the
// library authorizes: only the unheld lease is removed; the held lease is left
// intact and still reported held by the library.
func TestIndexLeaseReap_CLIMatchesLibrary(t *testing.T) {
	scr := setLeaseDataRoot(t)
	unheldID := leaseFullID('a')
	heldID := leaseFullID('b')

	unheld := acquireLeaseUnderCache(t, scr, unheldID, "index-build-dead")
	require.NoError(t, unheld.Release())
	held := acquireLeaseUnderCache(t, scr, heldID, "index-build-live")
	t.Cleanup(func() { _ = held.Release() })

	authorityRoot, err := leaseAuthorityRoot()
	require.NoError(t, err)

	// CLI reap (auto-select unheld) with confirm.
	var buf bytes.Buffer
	require.NoError(t, reapLeases(&buf, nil, true, true))

	// The library agrees on the resulting state.
	unheldProbe, err := indexcoord.ProbeLease(authorityRoot, unheldID, nil)
	require.NoError(t, err)
	require.Equal(t, indexcoord.LeaseMissing, unheldProbe.Verdict, "the unheld lease was reclaimed")

	heldProbe, err := indexcoord.ProbeLease(authorityRoot, heldID, nil)
	require.NoError(t, err)
	require.Equal(t, indexcoord.LeaseHeld, heldProbe.Verdict, "the held lease was left intact")
	require.FileExists(t, leaseLockPath(scr, heldID))
}
