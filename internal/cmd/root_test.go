package cmd

import (
	"bytes"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSetVersionInfo(t *testing.T) {
	// Save original values
	origVersion := versionInfo.Version
	origCommit := versionInfo.Commit
	origBuildDate := versionInfo.BuildDate
	defer func() {
		versionInfo.Version = origVersion
		versionInfo.Commit = origCommit
		versionInfo.BuildDate = origBuildDate
	}()

	tests := []struct {
		name      string
		version   string
		commit    string
		buildDate string
	}{
		{
			name:      "set all values",
			version:   "1.0.0",
			commit:    "abc123",
			buildDate: "2024-01-15",
		},
		{
			name:      "set dev version",
			version:   "dev",
			commit:    "HEAD",
			buildDate: "unknown",
		},
		{
			name:      "set empty values",
			version:   "",
			commit:    "",
			buildDate: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SetVersionInfo(tt.version, tt.commit, tt.buildDate)

			assert.Equal(t, tt.version, versionInfo.Version)
			assert.Equal(t, tt.commit, versionInfo.Commit)
			assert.Equal(t, tt.buildDate, versionInfo.BuildDate)
		})
	}
}

func TestGetAppIdentity(t *testing.T) {
	t.Run("returns nil before init", func(t *testing.T) {
		// Save and restore
		orig := appIdentity
		appIdentity = nil
		defer func() { appIdentity = orig }()

		result := GetAppIdentity()
		assert.Nil(t, result)
	})

	t.Run("returns identity after set", func(t *testing.T) {
		// If appIdentity is already set from other tests, verify it returns
		if appIdentity != nil {
			result := GetAppIdentity()
			assert.NotNil(t, result)
			assert.Equal(t, appIdentity, result)
		}
	})
}

func TestRootHelpUsesGonimbusCopy(t *testing.T) {
	var out bytes.Buffer
	rootCmd.SetOut(&out)
	rootCmd.SetErr(&out)
	rootCmd.SetArgs([]string{"--help"})
	defer func() {
		rootCmd.SetOut(nil)
		rootCmd.SetErr(nil)
		rootCmd.SetArgs(nil)
	}()

	require.NoError(t, rootCmd.Execute())

	help := out.String()
	assert.Contains(t, help, "Cloud object storage crawl/inspect/transfer engine")
	assert.Contains(t, help, "Build localized indexes")
	assert.Contains(t, help, "$XDG_CONFIG_HOME/gonimbus/config.yaml")
	assert.NotContains(t, help, "Fulmen workhorse")
	assert.NotContains(t, help, "workhorse service template")
}

func TestSetDefaults(t *testing.T) {
	// Reset viper for clean test
	v := viper.New()
	viper.Reset()
	defer func() {
		// Restore defaults
		viper.Reset()
		_ = v
	}()

	// Call setDefaults
	setDefaults()

	// Verify server defaults
	assert.Equal(t, "localhost", viper.GetString("server.host"))
	assert.Equal(t, 8080, viper.GetInt("server.port"))
	assert.Equal(t, "30s", viper.GetString("server.read_timeout"))
	assert.Equal(t, "30s", viper.GetString("server.write_timeout"))
	assert.Equal(t, "120s", viper.GetString("server.idle_timeout"))
	assert.Equal(t, "10s", viper.GetString("server.shutdown_timeout"))

	// Verify logging defaults
	assert.Equal(t, "info", viper.GetString("logging.level"))
	assert.Equal(t, "structured", viper.GetString("logging.profile"))

	// Verify metrics defaults
	assert.True(t, viper.GetBool("metrics.enabled"))
	assert.Equal(t, 9090, viper.GetInt("metrics.port"))

	// Verify health defaults
	assert.True(t, viper.GetBool("health.enabled"))

	// Verify worker defaults
	assert.Equal(t, 4, viper.GetInt("workers"))

	// Verify debug defaults
	assert.False(t, viper.GetBool("debug.enabled"))
	assert.False(t, viper.GetBool("debug.pprof_enabled"))
}
