package cmd

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndexBuildSpillFlags_DefaultEmpty(t *testing.T) {
	for _, name := range []string{"spill-workspace-max", "spill-root"} {
		flag := indexBuildCmd.Flags().Lookup(name)
		require.NotNil(t, flag, "flag %q must be registered", name)
		require.Equal(t, "", flag.DefValue)
	}
}

// withIndexBuildSpill saves and restores the spill flag package vars.
func withIndexBuildSpill(t *testing.T, workspaceMax, root string) {
	t.Helper()
	oldMax := indexBuildSpillWorkspaceMax
	oldRoot := indexBuildSpillRoot
	oldBytes := indexBuildSpillWorkspaceBytes
	indexBuildSpillWorkspaceMax = workspaceMax
	indexBuildSpillRoot = root
	indexBuildSpillWorkspaceBytes = 0
	t.Cleanup(func() {
		indexBuildSpillWorkspaceMax = oldMax
		indexBuildSpillRoot = oldRoot
		indexBuildSpillWorkspaceBytes = oldBytes
	})
}

func TestResolveIndexBuildSpillFlags(t *testing.T) {
	cases := []struct {
		name  string
		in    string
		want  int64
		errIs string
	}{
		{"empty uses default", "", 0, ""},
		{"raw bytes", "1048576", 1 << 20, ""},
		{"gib", "8GiB", 8 << 30, ""},
		{"gb base10", "16GB", 16_000_000_000, ""},
		{"whitespace trimmed", "  4GiB  ", 4 << 30, ""},
		{"bad unit", "8XB", 0, "--spill-workspace-max"},
		{"zero rejected", "0", 0, "at least 1 byte"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			withIndexBuildSpill(t, tc.in, "")
			err := resolveIndexBuildSpillFlags()
			if tc.errIs != "" {
				require.ErrorContains(t, err, tc.errIs)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, indexBuildSpillWorkspaceBytes)
		})
	}
}

func TestIndexBuildBackgroundRejectsSpillWorkspaceMax(t *testing.T) {
	withIndexBuildModes(t, true, false, false)
	withIndexBuildSpill(t, "8GiB", "")

	err := validateIndexBuildBackgroundFlags()
	require.ErrorContains(t, err, "--spill-workspace-max/--spill-root are not yet supported with --background")
}

func TestIndexBuildBackgroundRejectsSpillRoot(t *testing.T) {
	withIndexBuildModes(t, true, false, false)
	withIndexBuildSpill(t, "", "/tmp/scratch")

	err := validateIndexBuildBackgroundFlags()
	require.ErrorContains(t, err, "--spill-workspace-max/--spill-root are not yet supported with --background")
}

func TestIndexBuildBackgroundAcceptsNoSpillFlags(t *testing.T) {
	withIndexBuildModes(t, true, false, false)
	withIndexBuildSpill(t, "", "")

	require.NoError(t, validateIndexBuildBackgroundFlags())
}
