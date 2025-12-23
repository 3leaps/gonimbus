package config

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func findRepoRootForTest(t *testing.T) string {
	cwd, err := os.Getwd()
	require.NoError(t, err)

	dir := cwd
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}

	t.Fatalf("could not locate repo root containing go.mod from %s", cwd)
	return ""
}

func TestLoad(t *testing.T) {
	ctx := context.Background()

	// Regression test: in CI containers the repo checkout may be outside $HOME.
	// When $HOME is not an ancestor of the repo, pathfinder's default home boundary
	// can prevent repo root discovery unless a CI boundary hint is applied.
	t.Run("CIBoundaryHint", func(t *testing.T) {
		repoRoot := findRepoRootForTest(t)
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
		defer func() {
			_ = os.Unsetenv("GONIMBUS_PORT")
			_ = os.Unsetenv("GONIMBUS_LOG_LEVEL")
			_ = os.Unsetenv("GONIMBUS_METRICS_ENABLED")
		}()

		cfg, err := Load(ctx)
		require.NoError(t, err)
		require.NotNil(t, cfg)

		// Verify env overrides were applied
		assert.Equal(t, 3000, cfg.Server.Port)
		assert.Equal(t, "warn", cfg.Logging.Level)
		assert.False(t, cfg.Metrics.Enabled)
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
