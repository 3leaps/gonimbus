package indexcoord_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/3leaps/gonimbus/internal/indexsubstrate"
	"github.com/3leaps/gonimbus/pkg/indexcoord"
	"github.com/3leaps/gonimbus/pkg/jobregistry"
	"github.com/stretchr/testify/require"
)

func fullID(seed rune) string { return "idx_" + strings.Repeat(string(seed), 64) }

// seedUnheldResidue acquires and releases a set authority, leaving dead-holder
// residue (Release does not remove the file).
func seedUnheldResidue(t *testing.T, segmentSetRoot, indexSetID, holder string) {
	t.Helper()
	auth, err := indexsubstrate.AcquireSetAuthority(context.Background(), segmentSetRoot, indexSetID, holder)
	require.NoError(t, err)
	require.NoError(t, auth.Release())
}

func TestEnumerateLeases_AttributionJoin(t *testing.T) {
	segmentSetRoot := filepath.Join(t.TempDir(), "segset")
	authorityRoot, err := indexcoord.AuthorityRoot(segmentSetRoot)
	require.NoError(t, err)

	matchedID := fullID('a')
	unmatchedID := fullID('b')
	seedUnheldResidue(t, segmentSetRoot, matchedID, "index-build-JOB123")
	seedUnheldResidue(t, segmentSetRoot, unmatchedID, "index-build-orphan-uuid")

	// JOB123 exists and is alive (this process); no record for the orphan.
	jobs := []jobregistry.JobRecord{{JobID: "JOB123", PID: os.Getpid(), State: jobregistry.JobStateRunning}}

	reports, err := indexcoord.EnumerateLeases(authorityRoot, jobs)
	require.NoError(t, err)

	byID := make(map[string]indexcoord.LeaseReport, len(reports))
	for _, r := range reports {
		byID[r.IndexSetID] = r
	}

	matched := byID[matchedID]
	require.Equal(t, indexcoord.LeaseUnheld, matched.Verdict, "verdict comes from the probe, not attribution")
	require.True(t, matched.Attribution.Matched)
	require.Equal(t, "JOB123", matched.Attribution.JobID)
	require.Equal(t, os.Getpid(), matched.Attribution.PID)
	require.True(t, matched.Attribution.ProcessAlive)

	unmatched := byID[unmatchedID]
	require.Equal(t, indexcoord.LeaseUnheld, unmatched.Verdict)
	require.False(t, unmatched.Attribution.Matched, "an unmatched holder is reported honestly, never inferred")
	require.Empty(t, unmatched.Attribution.JobID)
}

func TestInvalidLease_ProbeAndReclaimRefusedThroughWrapper(t *testing.T) {
	segmentSetRoot := filepath.Join(t.TempDir(), "segset")
	authorityRoot, err := indexcoord.AuthorityRoot(segmentSetRoot)
	require.NoError(t, err)
	id := fullID('a')
	seedUnheldResidue(t, segmentSetRoot, id, "index-build-x")
	lockPath := filepath.Join(authorityRoot, id+".lock")
	// Corrupt the doc: invalid must surface identically through the wrapper.
	require.NoError(t, os.WriteFile(lockPath, []byte("{corrupt"), 0o600))

	rep, err := indexcoord.ProbeLease(authorityRoot, id, nil)
	require.NoError(t, err)
	require.Equal(t, indexcoord.LeaseInvalid, rep.Verdict)

	rr, err := indexcoord.ReclaimUnheldLease(authorityRoot, id)
	require.Error(t, err, "the wrapper must refuse invalid residue, matching the library")
	require.False(t, rr.Reclaimed)
	require.Equal(t, indexcoord.LeaseInvalid, rr.Verdict)
	require.FileExists(t, lockPath)
}

func TestReclaimUnheldLease_Wrapper(t *testing.T) {
	segmentSetRoot := filepath.Join(t.TempDir(), "segset")
	authorityRoot, err := indexcoord.AuthorityRoot(segmentSetRoot)
	require.NoError(t, err)
	id := fullID('a')
	seedUnheldResidue(t, segmentSetRoot, id, "index-build-seed")

	report, err := indexcoord.ReclaimUnheldLease(authorityRoot, id)
	require.NoError(t, err)
	require.True(t, report.Reclaimed)
	require.Equal(t, indexcoord.LeaseUnheld, report.Verdict)
	require.NoFileExists(t, filepath.Join(authorityRoot, id+".lock"))

	// Idempotent.
	report, err = indexcoord.ReclaimUnheldLease(authorityRoot, id)
	require.NoError(t, err)
	require.False(t, report.Reclaimed)
	require.Equal(t, indexcoord.LeaseMissing, report.Verdict)
}
