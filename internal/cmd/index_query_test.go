package cmd

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/pkg/indexstore"
)

// createTestIndex creates a minimal index DB in a temp directory and returns the identity.
func createTestIndex(t *testing.T, root string, baseURI string) *indexstore.IndexSetIdentityResult {
	t.Helper()
	return createTestIndexWithIncludes(t, root, baseURI, []string{"**"})
}

// createTestIndexWithIncludes creates a minimal index DB with custom includes (for distinct hashes).
func createTestIndexWithIncludes(t *testing.T, root string, baseURI string, includes []string) *indexstore.IndexSetIdentityResult {
	t.Helper()

	params := indexstore.IndexSetParams{
		BaseURI:         baseURI,
		Provider:        "s3",
		StorageProvider: "aws_s3",
		CloudProvider:   "aws",
		RegionKind:      "aws",
		Region:          "us-east-1",
		BuildParams: indexstore.BuildParams{
			SourceType:      "crawl",
			SchemaVersion:   indexstore.SchemaVersion,
			GonimbusVersion: "test",
			Includes:        includes,
		},
	}

	identity, err := indexstore.ComputeIndexSetID(params)
	require.NoError(t, err)

	idxDir := filepath.Join(root, identity.DirName)
	require.NoError(t, os.MkdirAll(idxDir, 0755))

	dbPath := filepath.Join(idxDir, "index.db")
	db, err := indexstore.Open(context.Background(), indexstore.Config{Path: dbPath})
	require.NoError(t, err)

	require.NoError(t, indexstore.Migrate(context.Background(), db))
	_, _, err = indexstore.FindOrCreateIndexSet(context.Background(), db, params)
	require.NoError(t, err)

	// Checkpoint WAL before closing so the DB file is self-contained.
	_, _ = db.Exec("PRAGMA wal_checkpoint(TRUNCATE)")
	require.NoError(t, db.Close())

	return identity
}

func TestOpenIndexDBByIDInRoot_ExactDirName(t *testing.T) {
	root := t.TempDir()
	identity := createTestIndex(t, root, "s3://bucket/prefix/")

	db, indexSet, err := openIndexDBByIDInRoot(context.Background(), root, identity.DirName)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	require.Equal(t, identity.IndexSetID, indexSet.IndexSetID)
	require.Equal(t, "s3://bucket/prefix/", indexSet.BaseURI)
}

func TestOpenIndexDBByIDInRoot_FullID(t *testing.T) {
	root := t.TempDir()
	identity := createTestIndex(t, root, "s3://bucket/prefix/")

	db, indexSet, err := openIndexDBByIDInRoot(context.Background(), root, identity.IndexSetID)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	require.Equal(t, identity.IndexSetID, indexSet.IndexSetID)
}

func TestOpenIndexDBByIDInRoot_PartialHex(t *testing.T) {
	root := t.TempDir()
	identity := createTestIndex(t, root, "s3://bucket/prefix/")

	// Use first 8 chars of the hex (without idx_ prefix)
	partialHex := identity.CanonicalSHA256[:8]
	db, indexSet, err := openIndexDBByIDInRoot(context.Background(), root, partialHex)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	require.Equal(t, identity.IndexSetID, indexSet.IndexSetID)
}

func TestOpenIndexDBByIDInRoot_NotFound(t *testing.T) {
	root := t.TempDir()

	_, _, err := openIndexDBByIDInRoot(context.Background(), root, "idx_0000000000000000")
	require.Error(t, err)
	require.Contains(t, err.Error(), "no index found matching ID")
}

func TestOpenIndexDBByIDInRoot_EmptyID(t *testing.T) {
	root := t.TempDir()

	_, _, err := openIndexDBByIDInRoot(context.Background(), root, "idx_")
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid index set ID")
}

func TestOpenIndexDBByIDInRoot_MissingRoot(t *testing.T) {
	_, _, err := openIndexDBByIDInRoot(context.Background(), "/nonexistent/path", "idx_abc123")
	require.Error(t, err)
	require.Contains(t, err.Error(), "no indexes found")
}

func TestOpenIndexDBByIDInRoot_NonHexID(t *testing.T) {
	root := t.TempDir()

	_, _, err := openIndexDBByIDInRoot(context.Background(), root, "idx_ZZZZ")
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid index set ID")
	require.Contains(t, err.Error(), "must be hex")
}

func TestOpenIndexDBByIDInRoot_OverlongID(t *testing.T) {
	root := t.TempDir()

	// 65 hex chars exceeds the 64-char max for SHA-256
	overlong := "idx_" + "a" + "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	_, _, err := openIndexDBByIDInRoot(context.Background(), root, overlong)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid index set ID")
}

func TestOpenIndexDBByIDInRoot_OverlongSuffixNoMatch(t *testing.T) {
	root := t.TempDir()
	identity := createTestIndex(t, root, "s3://bucket/prefix/")

	// Valid prefix + extra chars: should NOT match (old code would match via reverse prefix)
	badID := identity.DirName + "ffffffff"
	_, _, err := openIndexDBByIDInRoot(context.Background(), root, badID)
	require.Error(t, err)
	// The overlong input (16 + 8 = 24 hex chars) is still valid hex under 64,
	// but should not match because the dir hex doesn't start with the full cleanID.
	require.Contains(t, err.Error(), "no index found matching ID")
}

func TestOpenIndexDBByIDInRoot_AmbiguousPrefix(t *testing.T) {
	root := t.TempDir()

	// Create one real index, then plant a second empty dir that shares the same prefix.
	identity := createTestIndex(t, root, "s3://bucket/prefix/")
	dirHex := strings.TrimPrefix(identity.DirName, "idx_")

	// Create a sibling dir with the same prefix + different suffix.
	sibling := "idx_" + dirHex + "ff"
	siblingDir := filepath.Join(root, sibling)
	require.NoError(t, os.MkdirAll(siblingDir, 0755))

	// Plant a DB so the dir counts as a match.
	sibDB, err := indexstore.Open(context.Background(), indexstore.Config{
		Path: filepath.Join(siblingDir, "index.db"),
	})
	require.NoError(t, err)
	require.NoError(t, indexstore.Migrate(context.Background(), sibDB))
	_, _ = sibDB.Exec("PRAGMA wal_checkpoint(TRUNCATE)")
	require.NoError(t, sibDB.Close())

	// A prefix that matches both dirs should fail with ambiguity.
	_, _, err = openIndexDBByIDInRoot(context.Background(), root, dirHex[:8])
	require.Error(t, err)
	require.Contains(t, err.Error(), "ambiguous index ID")
	require.Contains(t, err.Error(), identity.DirName)
	require.Contains(t, err.Error(), sibling)
}

func TestOpenIndexDBByIDInRoot_BaseURIFromIndex(t *testing.T) {
	root := t.TempDir()
	identity := createTestIndex(t, root, "s3://my-bucket/data/")

	db, indexSet, err := openIndexDBByIDInRoot(context.Background(), root, identity.DirName)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	// The returned IndexSet carries the authoritative base_uri from the DB.
	require.Equal(t, "s3://my-bucket/data/", indexSet.BaseURI)
}
