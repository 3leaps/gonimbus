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

func TestIsLoopbackServeHost(t *testing.T) {
	tests := []struct {
		name string
		host string
		want bool
	}{
		{name: "localhost", host: "localhost", want: true},
		{name: "localhost with port", host: "localhost:8080", want: true},
		{name: "ipv4 loopback", host: "127.0.0.1", want: true},
		{name: "ipv4 loopback range", host: "127.1.2.3", want: true},
		{name: "ipv6 loopback", host: "::1", want: true},
		{name: "bracketed ipv6 loopback", host: "[::1]", want: true},
		{name: "bracketed ipv6 loopback with port", host: "[::1]:8080", want: true},
		{name: "empty host", host: "", want: false},
		{name: "all ipv4 interfaces", host: "0.0.0.0", want: false},
		{name: "all ipv6 interfaces", host: "::", want: false},
		{name: "lan ipv4", host: "192.168.1.20", want: false},
		{name: "public hostname", host: "gonimbus.example.com", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, isLoopbackServeHost(tt.host))
		})
	}
}

func TestServeServerOptionsRejectsNonLoopbackHost(t *testing.T) {
	_, err := serveServerOptions(t.Context(), "0.0.0.0")

	require.Error(t, err)
	require.Contains(t, err.Error(), "local job control API requires a loopback serve host")
}
