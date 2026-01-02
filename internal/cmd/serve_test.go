package cmd

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/internal/observability"
)

func TestSignalHealthChecker(t *testing.T) {
	checker := signalHealthChecker{}

	t.Run("always returns nil", func(t *testing.T) {
		err := checker.CheckHealth(context.Background())
		assert.NoError(t, err)
	})
}

func TestTelemetryHealthChecker(t *testing.T) {
	checker := telemetryHealthChecker{}

	t.Run("returns error when telemetry not initialized", func(t *testing.T) {
		// Save and restore
		origTelemetry := observability.TelemetrySystem
		origExporter := observability.PrometheusExporter
		defer func() {
			observability.TelemetrySystem = origTelemetry
			observability.PrometheusExporter = origExporter
		}()

		observability.TelemetrySystem = nil
		observability.PrometheusExporter = nil

		err := checker.CheckHealth(context.Background())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "telemetry system not initialized")
	})

}

func TestIdentityHealthChecker(t *testing.T) {
	tests := []struct {
		name       string
		binaryName string
		envPrefix  string
		configName string
		wantErr    bool
		errContain string
	}{
		{
			name:       "all fields valid",
			binaryName: "myapp",
			envPrefix:  "MYAPP",
			configName: "myapp",
			wantErr:    false,
		},
		{
			name:       "missing binary name",
			binaryName: "",
			envPrefix:  "MYAPP",
			configName: "myapp",
			wantErr:    true,
			errContain: "missing binary name",
		},
		{
			name:       "missing env prefix",
			binaryName: "myapp",
			envPrefix:  "",
			configName: "myapp",
			wantErr:    true,
			errContain: "missing env prefix",
		},
		{
			name:       "missing config name",
			binaryName: "myapp",
			envPrefix:  "MYAPP",
			configName: "",
			wantErr:    true,
			errContain: "missing config name",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			checker := identityHealthChecker{
				binaryName: tt.binaryName,
				envPrefix:  tt.envPrefix,
				configName: tt.configName,
			}

			err := checker.CheckHealth(context.Background())
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errContain)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
