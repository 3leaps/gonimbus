package cmd

import (
	"bytes"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/internal/indexsubstrate"
	"github.com/3leaps/gonimbus/internal/leasefixture"
)

// leaseAuthorityRootUnderCache is where the CLI's own resolution puts lease
// artifacts for a given segment-cache root.
func leaseAuthorityRootUnderCache(segmentCacheRoot string) string {
	return filepath.Join(segmentCacheRoot, indexsubstrate.SetAuthorityDirectoryName)
}

// TestIndexLeaseMatrix_CLIListAndReap drives the shared artifact-class matrix
// through the CLI adapters. The adapters pre-filter on the enumerated verdict
// before ever calling reclaim, so a classification or mapping slip here could
// reap an artifact the library would refuse — this pins list and reap to the
// same answer the other two layers give.
func TestIndexLeaseMatrix_CLIListAndReap(t *testing.T) {
	id := leasefixture.FullID('a')
	for _, row := range leasefixture.InvalidRows() {
		t.Run(row.Name, func(t *testing.T) {
			scr := setLeaseDataRoot(t)
			authorityRoot := leaseAuthorityRootUnderCache(scr)

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

			// list: every row reports the typed invalid verdict.
			var listBuf bytes.Buffer
			require.NoError(t, listLeases(&listBuf, true))
			var rows []leaseReportJSON
			require.NoError(t, json.Unmarshal(listBuf.Bytes(), &rows))
			require.Len(t, rows, 1, "the artifact must be listed, never silently dropped")
			require.Equal(t, "invalid", rows[0].Verdict, "list must classify %s as invalid", row.Name)
			preserved()

			// reap with an explicit target and --confirm: still skipped, never reaped.
			// Naming an artifact must not let an operator escalate past the gate.
			var reapBuf bytes.Buffer
			require.NoError(t, reapLeases(&reapBuf, []string{target}, true, true))
			var reaped []leaseReapJSON
			require.NoError(t, json.Unmarshal(reapBuf.Bytes(), &reaped))
			require.Len(t, reaped, 1)
			require.True(t, reaped[0].Skipped, "%s must be skipped for operator attention", row.Name)
			require.False(t, reaped[0].Reclaimed)
			require.Equal(t, "invalid", reaped[0].Verdict)
			preserved()

			// Auto-reap (no targets) selects only unheld leases, so it must not
			// touch this row either.
			var autoBuf bytes.Buffer
			require.NoError(t, reapLeases(&autoBuf, nil, true, true))
			preserved()
		})
	}
}

// TestIndexLeaseMatrix_CLIValidUnheld is the positive control for the CLI
// matrix: a canonical unheld artifact IS reaped on confirm.
func TestIndexLeaseMatrix_CLIValidUnheld(t *testing.T) {
	scr := setLeaseDataRoot(t)
	authorityRoot := leaseAuthorityRootUnderCache(scr)
	id := leasefixture.FullID('a')
	planted, err := leasefixture.PlantValidUnheld(authorityRoot, id)
	require.NoError(t, err)

	var buf bytes.Buffer
	require.NoError(t, reapLeases(&buf, nil, true, true))
	var rows []leaseReapJSON
	require.NoError(t, json.Unmarshal(buf.Bytes(), &rows))
	require.Len(t, rows, 1)
	require.True(t, rows[0].Reclaimed)
	require.NoFileExists(t, planted.Path)
}
