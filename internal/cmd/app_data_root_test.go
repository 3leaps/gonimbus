package cmd

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/fulmenhq/gofulmen/appidentity"
	gfconfig "github.com/fulmenhq/gofulmen/config"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func resetAppDataRootTestState(t *testing.T) {
	t.Helper()
	oldIdentity := appIdentity
	oldEnv, hadEnv := os.LookupEnv("GONIMBUS_DATA_DIR")
	oldEnvAlias, hadEnvAlias := os.LookupEnv("GONIMBUS_DATA_ROOT")
	oldXDG, hadXDG := os.LookupEnv("XDG_DATA_HOME")
	viper.Reset()
	setDefaults()
	appIdentity = &appidentity.Identity{
		BinaryName: "gonimbus",
		ConfigName: "gonimbus",
		EnvPrefix:  "GONIMBUS_",
	}
	t.Cleanup(func() {
		appIdentity = oldIdentity
		viper.Reset()
		if hadEnv {
			_ = os.Setenv("GONIMBUS_DATA_DIR", oldEnv)
		} else {
			_ = os.Unsetenv("GONIMBUS_DATA_DIR")
		}
		if hadEnvAlias {
			_ = os.Setenv("GONIMBUS_DATA_ROOT", oldEnvAlias)
		} else {
			_ = os.Unsetenv("GONIMBUS_DATA_ROOT")
		}
		if hadXDG {
			_ = os.Setenv("XDG_DATA_HOME", oldXDG)
		} else {
			_ = os.Unsetenv("XDG_DATA_HOME")
		}
	})
	t.Setenv("GONIMBUS_DATA_DIR", "")
	t.Setenv("GONIMBUS_DATA_ROOT", "")
	t.Setenv("XDG_DATA_HOME", "")
	_ = os.Unsetenv("GONIMBUS_DATA_DIR")
	_ = os.Unsetenv("GONIMBUS_DATA_ROOT")
	_ = os.Unsetenv("XDG_DATA_HOME")
}

func TestResolveAppDataRootPrecedence(t *testing.T) {
	resetAppDataRootTestState(t)

	envRoot := filepath.Join(t.TempDir(), "env-root")
	configRoot := filepath.Join(t.TempDir(), "config-root")
	xdgRoot := t.TempDir()

	viper.Set("data_root", configRoot)
	t.Setenv("XDG_DATA_HOME", xdgRoot)
	t.Setenv("GONIMBUS_DATA_DIR", envRoot)

	resolved, err := resolveAppDataRoot()
	require.NoError(t, err)
	require.Equal(t, normalizedPathForTest(t, envRoot), resolved.Dir)
	require.Equal(t, appDataRootSourceEnv, resolved.Source)
	require.True(t, resolved.Explicit)

	_ = os.Unsetenv("GONIMBUS_DATA_DIR")
	resolved, err = resolveAppDataRoot()
	require.NoError(t, err)
	require.Equal(t, normalizedPathForTest(t, configRoot), resolved.Dir)
	require.Equal(t, appDataRootSourceConfig, resolved.Source)
	require.True(t, resolved.Explicit)

	viper.Reset()
	setDefaults()
	viper.Set("data_dir", configRoot)
	resolved, err = resolveAppDataRoot()
	require.NoError(t, err)
	require.Equal(t, normalizedPathForTest(t, configRoot), resolved.Dir)
	require.Equal(t, appDataRootSourceConfig, resolved.Source)

	viper.Reset()
	setDefaults()
	resolved, err = resolveAppDataRoot()
	require.NoError(t, err)
	require.Equal(t, normalizedPathForTest(t, gfconfig.GetAppDataDir("gonimbus")), resolved.Dir)
	require.Equal(t, appDataRootSourceXDG, resolved.Source)
	require.False(t, resolved.Explicit)
}

func TestResolveAppDataRootEnvAliasReportsAliasSource(t *testing.T) {
	resetAppDataRootTestState(t)

	aliasRoot := filepath.Join(t.TempDir(), "alias-root")
	t.Setenv("GONIMBUS_DATA_ROOT", aliasRoot)

	resolved, err := resolveAppDataRoot()
	require.NoError(t, err)
	require.Equal(t, normalizedPathForTest(t, aliasRoot), resolved.Dir)
	require.Equal(t, appDataRootSourceEnvAlias, resolved.Source)
	require.True(t, resolved.Explicit)
}

func TestResolveAppDataRootRejectsGitWorktree(t *testing.T) {
	resetAppDataRootTestState(t)

	repo := filepath.Join(t.TempDir(), "repo")
	require.NoError(t, os.MkdirAll(filepath.Join(repo, ".git"), 0o700))

	_, err := resolveExplicitAppDataRoot(filepath.Join(repo, "data"), appDataRootSourceEnv)
	require.Error(t, err)
	require.Contains(t, err.Error(), "must be outside git working tree")
	require.Contains(t, err.Error(), appDataRootSourceEnv)
}

func TestResolveAppDataRootRejectsXDGInsideGitWorktree(t *testing.T) {
	resetAppDataRootTestState(t)

	repo := filepath.Join(t.TempDir(), "repo")
	require.NoError(t, os.MkdirAll(filepath.Join(repo, ".git"), 0o700))
	t.Setenv("XDG_DATA_HOME", filepath.Join(repo, ".tmp-xdg"))

	_, err := resolveAppDataRoot()
	require.Error(t, err)
	require.Contains(t, err.Error(), "must be outside git working tree")
	require.Contains(t, err.Error(), appDataRootSourceXDG)
}

func TestResolveAppDataRootRejectsSymlinkedXDGInsideGitWorktree(t *testing.T) {
	resetAppDataRootTestState(t)

	base := t.TempDir()
	repo := filepath.Join(base, "repo")
	require.NoError(t, os.MkdirAll(filepath.Join(repo, ".git"), 0o700))
	link := filepath.Join(base, "xdg-link")
	require.NoError(t, os.Symlink(repo, link))
	t.Setenv("XDG_DATA_HOME", filepath.Join(link, ".tmp-xdg"))

	_, err := resolveAppDataRoot()
	require.Error(t, err)
	require.Contains(t, err.Error(), "must be outside git working tree")
	require.Contains(t, err.Error(), appDataRootSourceXDG)
}

func TestResolveAppDataRootRejectsSymlinkedGitWorktree(t *testing.T) {
	resetAppDataRootTestState(t)

	base := t.TempDir()
	repo := filepath.Join(base, "repo")
	require.NoError(t, os.MkdirAll(filepath.Join(repo, ".git"), 0o700))
	link := filepath.Join(base, "link-to-repo")
	require.NoError(t, os.Symlink(repo, link))

	_, err := resolveExplicitAppDataRoot(filepath.Join(link, "data"), appDataRootSourceEnv)
	require.Error(t, err)
	require.Contains(t, err.Error(), "must be outside git working tree")
}

func TestAppDataRootPlacementSurfacesUseSingleRoot(t *testing.T) {
	resetAppDataRootTestState(t)

	root := filepath.Join(t.TempDir(), "gonimbus-data")
	t.Setenv("GONIMBUS_DATA_DIR", root)

	dataDir, err := indexDataDir()
	require.NoError(t, err)
	root = normalizedPathForTest(t, root)
	require.Equal(t, root, dataDir)

	indexRoot, err := indexRootDir()
	require.NoError(t, err)
	require.Equal(t, filepath.Join(root, "indexes"), indexRoot)

	jobsRoot, err := indexJobsRootDir()
	require.NoError(t, err)
	require.Equal(t, filepath.Join(root, "jobs", "index-build"), jobsRoot)

	opts, err := serveServerOptions(context.Background(), "127.0.0.1")
	require.NoError(t, err)
	require.Equal(t, jobsRoot, opts.JobsRoot)

	store, err := openDefaultOperationCheckpointStore(context.Background())
	require.NoError(t, err)
	require.Equal(t, filepath.Join(root, "operation-checkpoints"), store.RootDir())

	journalRoot, err := appDataPath(appDataClassCrawlJournals)
	require.NoError(t, err)
	require.Equal(t, filepath.Join(root, "journals", "crawl"), journalRoot)

	segmentCacheRoot, err := appDataPath(appDataClassSegmentCache)
	require.NoError(t, err)
	require.Equal(t, filepath.Join(root, "cache", "segments"), segmentCacheRoot)
}

func TestMkdirAppDataDirUsesOwnerOnlyMode(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "a", "b")
	require.NoError(t, mkdirAppDataDir(dir))
	info, err := os.Stat(dir)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o700), info.Mode().Perm())
}

func normalizedPathForTest(t *testing.T, path string) string {
	t.Helper()
	normalized, err := resolvePathForPolicy(path)
	require.NoError(t, err)
	return normalized
}
