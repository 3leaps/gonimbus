package config

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/fulmenhq/gofulmen/schema"
)

const testModulePath = "github.com/3leaps/gonimbus"

// testRepoRoot is the module root, resolved once in TestMain from the package
// directory. It is independent of $HOME so tests pass from any checkout
// location; the production walk and its home boundary are unchanged.
var testRepoRoot string

func TestMain(m *testing.M) {
	root, err := moduleRootFrom(".")
	if err != nil {
		fmt.Fprintf(os.Stderr, "config tests: %v\n", err)
		os.Exit(2)
	}
	testRepoRoot = root
	resolveProjectRoot = func() (string, error) { return testRepoRoot, nil }
	os.Exit(m.Run())
}

// moduleRootFrom walks up from start to the nearest go.mod and requires it to
// declare this module, so a stray or nested go.mod fails instead of silently
// supplying the wrong config and schema roots.
func moduleRootFrom(start string) (string, error) {
	dir, err := filepath.Abs(start)
	if err != nil {
		return "", fmt.Errorf("resolve start directory: %w", err)
	}
	for {
		data, err := os.ReadFile(filepath.Join(dir, "go.mod"))
		if err == nil {
			if goModModulePath(data) != testModulePath {
				return "", fmt.Errorf("nearest go.mod at %s is not module %s", dir, testModulePath)
			}
			return dir, nil
		}
		if !errors.Is(err, fs.ErrNotExist) {
			return "", fmt.Errorf("read go.mod in %s: %w", dir, err)
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("no go.mod found above %s", start)
		}
		dir = parent
	}
}

// goModModulePath returns the module path declared in go.mod content.
func goModModulePath(data []byte) string {
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 2 && fields[0] == "module" {
			return strings.Trim(fields[1], `"`)
		}
	}
	return ""
}

// useRealProjectRootFinder restores the production root finder for tests that
// exercise its boundary behavior.
func useRealProjectRootFinder(t *testing.T) {
	t.Helper()
	prev := resolveProjectRoot
	resolveProjectRoot = findProjectRoot
	t.Cleanup(func() { resolveProjectRoot = prev })
}

func TestLoad(t *testing.T) {
	ctx := context.Background()

	// Regression test: in CI containers the repo checkout may be outside $HOME.
	// When $HOME is not an ancestor of the repo, pathfinder's default home boundary
	// can prevent repo root discovery unless a CI boundary hint is applied.
	t.Run("CIBoundaryHint", func(t *testing.T) {
		useRealProjectRootFinder(t)
		repoRoot := testRepoRoot
		t.Setenv("HOME", t.TempDir())
		t.Setenv("CI", "true")
		t.Setenv("FULMEN_WORKSPACE_ROOT", repoRoot)

		cfg, err := Load(ctx)
		require.NoError(t, err)
		require.NotNil(t, cfg)
	})

	// Test basic config loading with defaults
	t.Run("LoadDefaults", func(t *testing.T) {
		cfg, err := Load(ctx)
		require.NoError(t, err)
		require.NotNil(t, cfg)

		// Verify server defaults
		assert.Equal(t, "localhost", cfg.Server.Host)
		assert.Equal(t, 8080, cfg.Server.Port)
		assert.Equal(t, 30*time.Second, cfg.Server.ReadTimeout)
		assert.Equal(t, 30*time.Second, cfg.Server.WriteTimeout)
		assert.Equal(t, 120*time.Second, cfg.Server.IdleTimeout)
		assert.Equal(t, 10*time.Second, cfg.Server.ShutdownTimeout)

		// Verify logging defaults
		assert.Equal(t, "info", cfg.Logging.Level)
		assert.Equal(t, "STRUCTURED", cfg.Logging.Profile)

		// Verify metrics defaults
		assert.True(t, cfg.Metrics.Enabled)
		assert.Equal(t, 9090, cfg.Metrics.Port)

		// Verify health defaults
		assert.True(t, cfg.Health.Enabled)

		// Verify debug defaults
		assert.False(t, cfg.Debug.Enabled)
		assert.False(t, cfg.Debug.PprofEnabled)

		// Verify workers default
		assert.Equal(t, 4, cfg.Workers)
	})

	// Test runtime overrides
	t.Run("RuntimeOverrides", func(t *testing.T) {
		overrides := map[string]any{
			"server": map[string]any{
				"port": 9000,
				"host": "0.0.0.0",
			},
			"logging": map[string]any{
				"level": "debug",
			},
		}

		cfg, err := Load(ctx, overrides)
		require.NoError(t, err)
		require.NotNil(t, cfg)

		// Verify overrides were applied
		assert.Equal(t, "0.0.0.0", cfg.Server.Host)
		assert.Equal(t, 9000, cfg.Server.Port)
		assert.Equal(t, "debug", cfg.Logging.Level)

		// Verify non-overridden values remain default
		assert.Equal(t, "STRUCTURED", cfg.Logging.Profile)
		assert.Equal(t, 9090, cfg.Metrics.Port)
	})

	// Test environment variable overrides
	t.Run("EnvOverrides", func(t *testing.T) {
		// Set environment variables
		require.NoError(t, os.Setenv("GONIMBUS_PORT", "3000"))
		require.NoError(t, os.Setenv("GONIMBUS_LOG_LEVEL", "warn"))
		require.NoError(t, os.Setenv("GONIMBUS_METRICS_ENABLED", "false"))
		require.NoError(t, os.Setenv("GONIMBUS_DATA_DIR", "/tmp/gonimbus-data"))
		defer func() {
			_ = os.Unsetenv("GONIMBUS_PORT")
			_ = os.Unsetenv("GONIMBUS_LOG_LEVEL")
			_ = os.Unsetenv("GONIMBUS_METRICS_ENABLED")
			_ = os.Unsetenv("GONIMBUS_DATA_DIR")
		}()

		cfg, err := Load(ctx)
		require.NoError(t, err)
		require.NotNil(t, cfg)

		// Verify env overrides were applied
		assert.Equal(t, 3000, cfg.Server.Port)
		assert.Equal(t, "warn", cfg.Logging.Level)
		assert.False(t, cfg.Metrics.Enabled)
		assert.Equal(t, "/tmp/gonimbus-data", cfg.DataDir)
	})

	// Test config precedence: runtime > env > defaults
	t.Run("ConfigPrecedence", func(t *testing.T) {
		// Set environment variable
		require.NoError(t, os.Setenv("GONIMBUS_PORT", "4000"))
		defer func() {
			_ = os.Unsetenv("GONIMBUS_PORT")
		}()

		// Runtime override should win
		overrides := map[string]any{
			"server": map[string]any{
				"port": 5000,
			},
		}

		cfg, err := Load(ctx, overrides)
		require.NoError(t, err)
		require.NotNil(t, cfg)

		// Runtime override should take precedence over env var
		assert.Equal(t, 5000, cfg.Server.Port)
	})
}

func TestGetConfig(t *testing.T) {
	ctx := context.Background()

	// Load config first
	cfg, err := Load(ctx)
	require.NoError(t, err)
	require.NotNil(t, cfg)

	// Test GetConfig returns the same instance
	t.Run("GetConfigReturnsLoadedConfig", func(t *testing.T) {
		retrieved := GetConfig()
		assert.NotNil(t, retrieved)
		assert.Equal(t, cfg.Server.Port, retrieved.Server.Port)
		assert.Equal(t, cfg.Logging.Level, retrieved.Logging.Level)
	})
}

func TestEnvSpecs(t *testing.T) {
	// Need to set app identity for env specs
	ctx := context.Background()
	_, err := Load(ctx)
	require.NoError(t, err)

	specs := getEnvSpecs()
	assert.NotEmpty(t, specs)

	// Verify critical env var mappings exist
	envVarNames := make(map[string]bool)
	for _, spec := range specs {
		envVarNames[spec.Name] = true
	}

	// Check required Workhorse Standard env vars
	assert.True(t, envVarNames["GONIMBUS_LOG_LEVEL"], "LOG_LEVEL env var must be mapped")
	assert.True(t, envVarNames["GONIMBUS_PORT"], "PORT env var must be mapped")
	assert.True(t, envVarNames["GONIMBUS_HOST"], "HOST env var must be mapped")
	assert.True(t, envVarNames["GONIMBUS_METRICS_PORT"], "METRICS_PORT env var must be mapped")
	assert.True(t, envVarNames["GONIMBUS_DATA_ROOT"], "DATA_ROOT env var must be mapped")
	assert.True(t, envVarNames["GONIMBUS_DATA_DIR"], "DATA_DIR env var must be mapped")
}

func TestDurationParsing(t *testing.T) {
	ctx := context.Background()

	// Test duration parsing from string env var
	t.Run("DurationFromEnv", func(t *testing.T) {
		require.NoError(t, os.Setenv("GONIMBUS_READ_TIMEOUT", "45s"))
		require.NoError(t, os.Setenv("GONIMBUS_SHUTDOWN_TIMEOUT", "5m"))
		defer func() {
			_ = os.Unsetenv("GONIMBUS_READ_TIMEOUT")
			_ = os.Unsetenv("GONIMBUS_SHUTDOWN_TIMEOUT")
		}()

		cfg, err := Load(ctx)
		require.NoError(t, err)
		require.NotNil(t, cfg)

		assert.Equal(t, 45*time.Second, cfg.Server.ReadTimeout)
		assert.Equal(t, 5*time.Minute, cfg.Server.ShutdownTimeout)
	})
}

func TestConfigReload(t *testing.T) {
	ctx := context.Background()

	// Load initial config
	cfg1, err := Load(ctx)
	require.NoError(t, err)
	require.NotNil(t, cfg1)
	initialPort := cfg1.Server.Port

	// Reload with different runtime overrides
	overrides := map[string]any{
		"server": map[string]any{
			"port": initialPort + 1000,
		},
	}

	cfg2, err := Load(ctx, overrides)
	require.NoError(t, err)
	require.NotNil(t, cfg2)

	// Verify reload updated the config
	assert.Equal(t, initialPort+1000, cfg2.Server.Port)

	// Verify GetConfig returns the updated config
	current := GetConfig()
	assert.Equal(t, cfg2.Server.Port, current.Server.Port)
}

// resetAppIdentity resets package state for isolated tests.
// Must only be used in tests.
func resetAppIdentity() {
	configMu.Lock()
	defer configMu.Unlock()
	appIdentity = nil
	appConfig = nil
}

func TestGetUserConfigPathsNilIdentity(t *testing.T) {
	// Save and restore state
	resetAppIdentity()
	defer func() {
		ctx := context.Background()
		_, _ = Load(ctx) // Restore state for other tests
	}()

	// When appIdentity is nil, getUserConfigPaths should return empty slice
	paths := getUserConfigPaths()
	assert.Empty(t, paths)
}

func TestGetEnvSpecsNilIdentity(t *testing.T) {
	// Save and restore state
	resetAppIdentity()
	defer func() {
		ctx := context.Background()
		_, _ = Load(ctx) // Restore state for other tests
	}()

	// When appIdentity is nil, getEnvSpecs should return empty slice
	specs := getEnvSpecs()
	assert.Empty(t, specs)
}

func TestFindProjectRootCIBoundaryEdgeCases(t *testing.T) {
	// These tests verify CI boundary edge cases using isolated temp directories
	// with proper repo markers. This avoids os.Chdir issues in parallel test runs.
	//
	// IMPORTANT: We create the temp dir inside HOME because pathfinder uses $HOME
	// as the default boundary ceiling. System temp dirs (/var/folders, /tmp) are
	// outside HOME and would fail the boundary check before marker discovery.

	// Helper to create a temp directory with repo markers and run test from there
	withTempRepoRoot := func(t *testing.T, testFn func(t *testing.T, tempRoot string)) {
		t.Helper()

		// Create temp dir inside HOME to satisfy pathfinder's default boundary
		homeDir, err := os.UserHomeDir()
		require.NoError(t, err)

		tempDir, err := os.MkdirTemp(homeDir, "gonimbus-test-*")
		require.NoError(t, err)
		t.Cleanup(func() { _ = os.RemoveAll(tempDir) })

		// Create go.mod marker so findProjectRoot can discover this as a repo root
		goModPath := filepath.Join(tempDir, "go.mod")
		err = os.WriteFile(goModPath, []byte("module testmodule\n\ngo 1.21\n"), 0644)
		require.NoError(t, err)

		// Change to temp dir for the test
		oldWd, err := os.Getwd()
		require.NoError(t, err)
		require.NoError(t, os.Chdir(tempDir))
		t.Cleanup(func() { _ = os.Chdir(oldWd) })

		testFn(t, tempDir)
	}

	t.Run("CITrueButEmptyBoundaryVars", func(t *testing.T) {
		withTempRepoRoot(t, func(t *testing.T, tempRoot string) {
			// Set CI=true but leave all boundary vars empty
			t.Setenv("CI", "true")
			t.Setenv("FULMEN_WORKSPACE_ROOT", "")
			t.Setenv("GITHUB_WORKSPACE", "")
			t.Setenv("CI_PROJECT_DIR", "")
			t.Setenv("WORKSPACE", "")

			// Should still find root via fallback (go.mod marker in tempRoot)
			root, err := findProjectRoot()
			require.NoError(t, err)
			assert.Equal(t, tempRoot, root)
		})
	})

	t.Run("CITrueWithRelativeBoundary", func(t *testing.T) {
		withTempRepoRoot(t, func(t *testing.T, tempRoot string) {
			t.Setenv("CI", "true")
			t.Setenv("FULMEN_WORKSPACE_ROOT", "./relative/path") // Not absolute

			// Should fall back to default discovery
			root, err := findProjectRoot()
			require.NoError(t, err)
			assert.Equal(t, tempRoot, root)
		})
	})

	t.Run("CITrueWithNonexistentBoundary", func(t *testing.T) {
		withTempRepoRoot(t, func(t *testing.T, tempRoot string) {
			t.Setenv("CI", "true")
			t.Setenv("FULMEN_WORKSPACE_ROOT", "/nonexistent/path/that/does/not/exist")

			// Should fall back to default discovery
			root, err := findProjectRoot()
			require.NoError(t, err)
			assert.Equal(t, tempRoot, root)
		})
	})

	t.Run("CITrueWithBoundaryNotContainingCwd", func(t *testing.T) {
		withTempRepoRoot(t, func(t *testing.T, tempRoot string) {
			t.Setenv("CI", "true")
			// Use a valid directory that doesn't contain our cwd
			t.Setenv("FULMEN_WORKSPACE_ROOT", os.TempDir())

			// Should fall back to default discovery
			root, err := findProjectRoot()
			require.NoError(t, err)
			assert.Equal(t, tempRoot, root)
		})
	})

	t.Run("GitHubActionsEnvVar", func(t *testing.T) {
		withTempRepoRoot(t, func(t *testing.T, tempRoot string) {
			t.Setenv("GITHUB_ACTIONS", "true")
			t.Setenv("GITHUB_WORKSPACE", tempRoot)

			root, err := findProjectRoot()
			require.NoError(t, err)
			assert.Equal(t, tempRoot, root)
		})
	})
}

func TestEnvSpecsPrefixHandling(t *testing.T) {
	ctx := context.Background()

	// Ensure appIdentity is loaded
	_, err := Load(ctx)
	require.NoError(t, err)

	specs := getEnvSpecs()
	require.NotEmpty(t, specs)

	// Verify all specs have the GONIMBUS_ prefix
	for _, spec := range specs {
		assert.True(t, len(spec.Name) > 0, "env var name should not be empty")
		assert.Contains(t, spec.Name, "GONIMBUS_", "all specs should have GONIMBUS_ prefix")
	}

	// Verify path structure
	for _, spec := range specs {
		assert.NotEmpty(t, spec.Path, "env var %s should have a path", spec.Name)
	}
}

// TestConfigSchemaMemoryBudgetContract pins the transfer memory-budget config
// surface against the strict canonical schema (additionalProperties:false):
// the advertised key validates with SI/IEC/raw-byte values, wrong types and
// malformed shapes are rejected, and unknown keys remain rejected.
func TestConfigSchemaMemoryBudgetContract(t *testing.T) {
	root := testRepoRoot
	catalog := schema.NewCatalog(filepath.Join(root, "schemas"))
	validator, err := catalog.ValidatorByID("gonimbus/v1.0.0/config")
	require.NoError(t, err)

	valid := []string{
		`{"memory_budget":"128MiB"}`,
		`{"memory_budget":"8GiB"}`,
		`{"memory_budget":"500MB"}`,
		`{"memory_budget":"134217728"}`,
	}
	for _, doc := range valid {
		diags, err := validator.ValidateJSON([]byte(doc))
		require.NoError(t, err, doc)
		require.Empty(t, diags, "expected %s to validate", doc)
	}

	invalid := []string{
		`{"memory_budget":134217728}`,
		`{"memory_budget":"lots"}`,
		`{"memory_budget":"MiB128"}`,
		`{"memory_budget_bytes":"128MiB"}`,
	}
	for _, doc := range invalid {
		diags, err := validator.ValidateJSON([]byte(doc))
		require.NoError(t, err, doc)
		require.NotEmpty(t, diags, "expected %s to be rejected", doc)
	}
}

func TestModuleRootFromFailsFast(t *testing.T) {
	t.Run("FindsThisModule", func(t *testing.T) {
		root, err := moduleRootFrom(".")
		require.NoError(t, err)
		assert.FileExists(t, filepath.Join(root, "go.mod"))
		assert.DirExists(t, filepath.Join(root, "config"))
	})

	t.Run("RejectsForeignModule", func(t *testing.T) {
		dir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(dir, "go.mod"), []byte("module example.com/other\n\ngo 1.26\n"), 0o600))
		nested := filepath.Join(dir, "a", "b")
		require.NoError(t, os.MkdirAll(nested, 0o755))

		_, err := moduleRootFrom(nested)
		require.ErrorContains(t, err, "is not module "+testModulePath)
	})

	t.Run("RejectsMissingGoMod", func(t *testing.T) {
		if _, err := os.Stat("/go.mod"); err == nil {
			t.Skip("filesystem root has a go.mod")
		}
		_, err := moduleRootFrom(t.TempDir())
		require.Error(t, err)
	})
}
