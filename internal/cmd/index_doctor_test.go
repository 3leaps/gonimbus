package cmd

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/pkg/indexstore"
)

func executeIndexDoctorCommand(t *testing.T, args ...string) (string, string, error) {
	t.Helper()

	oldRootDir := indexDoctorRootDir
	oldDB := indexDoctorDB
	oldFormat := indexDoctorFormat
	indexDoctorRootDir = ""
	indexDoctorDB = ""
	indexDoctorFormat = ""
	t.Cleanup(func() {
		indexDoctorRootDir = oldRootDir
		indexDoctorDB = oldDB
		indexDoctorFormat = oldFormat
	})

	cmd := newIndexDoctorCommand()
	cmd.SetArgs(args)
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	oldStdout := os.Stdout
	oldStderr := os.Stderr
	stdoutR, stdoutW, err := os.Pipe()
	require.NoError(t, err)
	stderrR, stderrW, err := os.Pipe()
	require.NoError(t, err)
	os.Stdout = stdoutW
	os.Stderr = stderrW
	cmd.SetOut(stdoutW)
	cmd.SetErr(stderrW)

	execErr := cmd.Execute()

	require.NoError(t, stdoutW.Close())
	require.NoError(t, stderrW.Close())
	os.Stdout = oldStdout
	os.Stderr = oldStderr

	stdout, err := io.ReadAll(stdoutR)
	require.NoError(t, err)
	stderr, err := io.ReadAll(stderrR)
	require.NoError(t, err)

	return string(stdout), string(stderr), execErr
}

func TestIndexDoctorCommand_DefaultListsRootIndexes(t *testing.T) {
	root := t.TempDir()
	first := createTestIndex(t, root, "s3://bucket/first/")
	second := createTestIndex(t, root, "s3://bucket/second/")

	stdout, stderr, err := executeIndexDoctorCommand(t, "--root", root)
	require.NoError(t, err)
	require.Empty(t, stderr)
	require.Contains(t, stdout, "DIR")
	require.Contains(t, stdout, "INDEX_SET")
	require.Contains(t, stdout, first.DirName)
	require.Contains(t, stdout, "s3://bucket/first/")
	require.Contains(t, stdout, second.DirName)
	require.Contains(t, stdout, "s3://bucket/second/")
}

func TestIndexDoctorCommand_PositionalIDTargetsSingleIndex(t *testing.T) {
	root := t.TempDir()
	first := createTestIndex(t, root, "s3://bucket/first/")
	second := createTestIndex(t, root, "s3://bucket/second/")

	stdout, stderr, err := executeIndexDoctorCommand(t, "--root", root, first.CanonicalSHA256[:8])
	require.NoError(t, err)
	require.Empty(t, stderr)
	require.Contains(t, stdout, first.DirName)
	require.Contains(t, stdout, "s3://bucket/first/")
	require.NotContains(t, stdout, second.DirName)
	require.NotContains(t, stdout, "s3://bucket/second/")
}

func TestIndexDoctorCommand_PositionalPathTargetsIndex(t *testing.T) {
	root := t.TempDir()
	identity := createTestIndex(t, root, "s3://bucket/path-target/")

	stdout, stderr, err := executeIndexDoctorCommand(t, filepath.Join(root, identity.DirName))
	require.NoError(t, err)
	require.Empty(t, stderr)
	require.Contains(t, stdout, identity.DirName)
	require.Contains(t, stdout, "s3://bucket/path-target/")
}

func TestIndexDoctorCommand_PositionalTargetConflictsWithDB(t *testing.T) {
	root := t.TempDir()
	identity := createTestIndex(t, root, "s3://bucket/conflict/")
	dbPath := filepath.Join(root, identity.DirName, "index.db")

	stdout, stderr, err := executeIndexDoctorCommand(t, "--db", dbPath, identity.DirName)
	require.Error(t, err)
	require.Empty(t, stdout)
	require.Empty(t, stderr)
	require.Contains(t, err.Error(), "cannot use positional target with --db")
}

func TestIndexDoctorCommand_TooManyArgsFailsBeforeResolution(t *testing.T) {
	stdout, stderr, err := executeIndexDoctorCommand(t, "idx_1234", "idx_5678")
	require.Error(t, err)
	require.Empty(t, stdout)
	require.Empty(t, stderr)
	require.Contains(t, err.Error(), "accepts at most 1 arg")
}

func TestIndexDoctorCommand_AmbiguousPrefixFails(t *testing.T) {
	root := t.TempDir()
	identity := createTestIndex(t, root, "s3://bucket/prefix/")
	dirHex := strings.TrimPrefix(identity.DirName, "idx_")

	sibling := "idx_" + dirHex + "ff"
	siblingDir := filepath.Join(root, sibling)
	require.NoError(t, os.MkdirAll(siblingDir, 0755))
	siblingDB, err := indexstore.Open(context.Background(), indexstore.Config{
		Path: filepath.Join(siblingDir, "index.db"),
	})
	require.NoError(t, err)
	require.NoError(t, indexstore.Migrate(context.Background(), siblingDB))
	_, _ = siblingDB.Exec("PRAGMA wal_checkpoint(TRUNCATE)")
	require.NoError(t, siblingDB.Close())

	stdout, stderr, err := executeIndexDoctorCommand(t, "--root", root, dirHex[:8])
	require.Error(t, err)
	require.Empty(t, stdout)
	require.Empty(t, stderr)
	require.Contains(t, err.Error(), "ambiguous index ID")
	require.Contains(t, err.Error(), identity.DirName)
	require.Contains(t, err.Error(), sibling)
}

func TestIndexDoctorCommand_MissingPathDoesNotFallThroughToID(t *testing.T) {
	missingPath := filepath.Join(t.TempDir(), "missing", "index.db")

	stdout, stderr, err := executeIndexDoctorCommand(t, missingPath)
	require.Error(t, err)
	require.Empty(t, stdout)
	require.Empty(t, stderr)
	require.Contains(t, err.Error(), "index db not found")
	require.Contains(t, err.Error(), missingPath)
}

func TestInspectIndexDBForDoctor_IdentityOK(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()

	params := indexstore.IndexSetParams{
		BaseURI:         "s3://bucket/prefix/",
		Provider:        "s3",
		StorageProvider: "aws_s3",
		CloudProvider:   "aws",
		RegionKind:      "aws",
		Region:          "us-east-1",
		Endpoint:        "",
		EndpointHost:    "",
		BuildParams: indexstore.BuildParams{
			SourceType:      "crawl",
			SchemaVersion:   indexstore.SchemaVersion,
			GonimbusVersion: "test",
			Includes:        []string{"prefix/**"},
			IncludeHidden:   false,
		},
	}

	identity, err := indexstore.ComputeIndexSetID(params)
	require.NoError(t, err)

	idxDir := filepath.Join(root, identity.DirName)
	require.NoError(t, os.MkdirAll(idxDir, 0755))

	dbPath := filepath.Join(idxDir, "index.db")
	db, err := indexstore.Open(ctx, indexstore.Config{Path: dbPath})
	require.NoError(t, err)

	require.NoError(t, indexstore.Migrate(ctx, db))

	_, _, err = indexstore.FindOrCreateIndexSet(ctx, db, params)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	// Write identity.json alongside the DB.
	require.NoError(t, writeIndexIdentityFile(idxDir, identity))

	entry, err := inspectIndexDBForDoctor(ctx, dbPath, indexDoctorOptions{IncludeIdentityPayload: true})
	require.NoError(t, err)
	require.Equal(t, dbPath, entry.DBPath)
	require.Equal(t, identity.DirName, entry.DirName)
	require.True(t, entry.IdentityPresent)
	require.True(t, entry.IdentityValidJSON)
	require.True(t, entry.IdentityHashMatchesDB)
	require.True(t, entry.IdentityDirMatches)
	require.True(t, entry.IdentityBaseURIMatch)
	require.True(t, entry.IdentityProviderMatch)
	// With no warnings, IdentityOK should be true.
	require.True(t, entry.IdentityOK)
	// Payload is included in verbose mode.
	require.NotNil(t, entry.IdentityPayload)
}

func TestInspectIndexDBForDoctor_IdentityHashMismatch(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()

	params := indexstore.IndexSetParams{
		BaseURI:         "s3://bucket/prefix/",
		Provider:        "s3",
		StorageProvider: "aws_s3",
		CloudProvider:   "aws",
		RegionKind:      "aws",
		Region:          "us-east-1",
		BuildParams: indexstore.BuildParams{
			SourceType:      "crawl",
			SchemaVersion:   indexstore.SchemaVersion,
			GonimbusVersion: "test",
			Includes:        []string{"prefix/**"},
		},
	}

	identity, err := indexstore.ComputeIndexSetID(params)
	require.NoError(t, err)

	idxDir := filepath.Join(root, identity.DirName)
	require.NoError(t, os.MkdirAll(idxDir, 0755))

	dbPath := filepath.Join(idxDir, "index.db")
	db, err := indexstore.Open(ctx, indexstore.Config{Path: dbPath})
	require.NoError(t, err)
	require.NoError(t, indexstore.Migrate(ctx, db))
	_, _, err = indexstore.FindOrCreateIndexSet(ctx, db, params)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	// Write an identity.json that doesn't match the DB identity.
	other := params
	other.BaseURI = "s3://bucket/other/"
	otherIdentity, err := indexstore.ComputeIndexSetID(other)
	require.NoError(t, err)
	require.NoError(t, writeIndexIdentityFile(idxDir, otherIdentity))

	entry, err := inspectIndexDBForDoctor(ctx, dbPath, indexDoctorOptions{})
	require.NoError(t, err)
	require.True(t, entry.IdentityPresent)
	require.True(t, entry.IdentityValidJSON)
	require.False(t, entry.IdentityHashMatchesDB)
	require.False(t, entry.IdentityOK)
}

func TestInspectIndexDBForDoctor_MissingIdentityFile(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()

	params := indexstore.IndexSetParams{
		BaseURI:         "s3://bucket/prefix/",
		Provider:        "s3",
		StorageProvider: "aws_s3",
		CloudProvider:   "aws",
		RegionKind:      "aws",
		Region:          "us-east-1",
		BuildParams: indexstore.BuildParams{
			SourceType:      "crawl",
			SchemaVersion:   indexstore.SchemaVersion,
			GonimbusVersion: "test",
			Includes:        []string{"prefix/**"},
		},
	}

	identity, err := indexstore.ComputeIndexSetID(params)
	require.NoError(t, err)

	idxDir := filepath.Join(root, identity.DirName)
	require.NoError(t, os.MkdirAll(idxDir, 0755))

	dbPath := filepath.Join(idxDir, "index.db")
	db, err := indexstore.Open(ctx, indexstore.Config{Path: dbPath})
	require.NoError(t, err)
	require.NoError(t, indexstore.Migrate(ctx, db))
	_, _, err = indexstore.FindOrCreateIndexSet(ctx, db, params)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	entry, err := inspectIndexDBForDoctor(ctx, dbPath, indexDoctorOptions{})
	require.NoError(t, err)
	require.False(t, entry.IdentityPresent)
	require.False(t, entry.IdentityOK)
	require.NotEmpty(t, entry.Notes)
}
