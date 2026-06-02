package cmd

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/fulmenhq/gofulmen/appidentity"
	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/pkg/opcheckpoint"
)

func TestOpenDefaultOperationCheckpointStoreRejectsRepoRootFromNestedCwd(t *testing.T) {
	repoRoot := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(repoRoot, "go.mod"), []byte("module example.test/gonimbus\n"), 0o644))
	nested := filepath.Join(repoRoot, "internal", "cmd")
	require.NoError(t, os.MkdirAll(nested, 0o755))

	originalWD, err := os.Getwd()
	require.NoError(t, err)
	require.NoError(t, os.Chdir(nested))
	t.Cleanup(func() { require.NoError(t, os.Chdir(originalWD)) })

	originalIdentity := appIdentity
	appIdentity = &appidentity.Identity{
		BinaryName: "gonimbus",
		ConfigName: "gonimbus",
		EnvPrefix:  "GONIMBUS_",
	}
	t.Cleanup(func() { appIdentity = originalIdentity })
	t.Setenv("XDG_DATA_HOME", repoRoot)

	_, err = openDefaultOperationCheckpointStore(context.Background())
	require.True(t, errors.Is(err, opcheckpoint.ErrPathInsideForbiddenRoot), "got error: %v", err)
}

func TestDiscoverRepositoryRootPrefersNearestMarker(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, "go.mod"), []byte("module example.test/root\n"), 0o644))
	nested := filepath.Join(root, "a", "b")
	require.NoError(t, os.MkdirAll(nested, 0o755))

	got, err := discoverRepositoryRoot(nested)
	require.NoError(t, err)
	require.Equal(t, root, got)
}
