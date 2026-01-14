package cmd

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/pkg/indexstore"
)

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
	t.Cleanup(func() { _ = db.Close() })

	require.NoError(t, indexstore.Migrate(ctx, db))

	_, _, err = indexstore.FindOrCreateIndexSet(ctx, db, params)
	require.NoError(t, err)

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
	t.Cleanup(func() { _ = db.Close() })
	require.NoError(t, indexstore.Migrate(ctx, db))
	_, _, err = indexstore.FindOrCreateIndexSet(ctx, db, params)
	require.NoError(t, err)

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
	t.Cleanup(func() { _ = db.Close() })
	require.NoError(t, indexstore.Migrate(ctx, db))
	_, _, err = indexstore.FindOrCreateIndexSet(ctx, db, params)
	require.NoError(t, err)

	entry, err := inspectIndexDBForDoctor(ctx, dbPath, indexDoctorOptions{})
	require.NoError(t, err)
	require.False(t, entry.IdentityPresent)
	require.False(t, entry.IdentityOK)
	require.NotEmpty(t, entry.Notes)
}
