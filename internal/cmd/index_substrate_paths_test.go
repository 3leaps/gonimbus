package cmd

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndexSubstratePathsUseDedicatedAppDataClasses(t *testing.T) {
	resetAppDataRootTestState(t)

	root := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", root)
	root = normalizedPathForTest(t, root)

	journalDir, err := indexSubstrateJournalRunDir("idx_test", "run_test")
	require.NoError(t, err)
	require.Equal(t, filepath.Join(root, "journals", "crawl", "idx_test", "run_test"), journalDir)

	segmentCacheDir, err := indexSubstrateSegmentCacheDir("idx_test")
	require.NoError(t, err)
	require.Equal(t, filepath.Join(root, "cache", "segments", "idx_test"), segmentCacheDir)

	indexRoot, err := indexRootDir()
	require.NoError(t, err)
	require.False(t, pathWithin(journalDir, indexRoot), "journal path must not live under index db root")
	require.False(t, pathWithin(segmentCacheDir, indexRoot), "segment cache path must not live under index db root")
}

func TestIndexSubstratePathsRejectMissingIdentityParts(t *testing.T) {
	resetAppDataRootTestState(t)
	t.Setenv("GONIMBUS_DATA_DIR", filepath.Join(t.TempDir(), "gonimbus-data"))

	_, err := indexSubstrateJournalRunDir("", "run_test")
	require.Error(t, err)
	require.Contains(t, err.Error(), "index set id")

	_, err = indexSubstrateJournalRunDir("idx_test", "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "run id")

	_, err = indexSubstrateSegmentCacheDir("")
	require.Error(t, err)
	require.Contains(t, err.Error(), "index set id")

	_, err = indexSubstrateJournalRunDir("idx/test", "run_test")
	require.Error(t, err)
	require.Contains(t, err.Error(), "path separators")

	_, err = indexSubstrateJournalRunDir("..", "run_test")
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid path part")

	_, err = indexSubstrateSegmentCacheDir(".")
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid path part")
}

func pathWithin(path, root string) bool {
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return false
	}
	return rel == "." || (!strings.HasPrefix(rel, ".."+string(filepath.Separator)) && rel != "..")
}
