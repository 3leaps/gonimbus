package config

import "time"

// Config represents the complete application configuration
// following the Fulmen Forge Workhorse Standard three-layer pattern:
// Layer 1: Crucible defaults (config/gonimbus-defaults.yaml)
// Layer 2: User overrides (~/.config/gonimbus/config.yaml)
// Layer 3: Environment variables and runtime overrides
type Config struct {
	Server  ServerConfig  `mapstructure:"server"`
	Logging LoggingConfig `mapstructure:"logging"`
	Metrics MetricsConfig `mapstructure:"metrics"`
	Health  HealthConfig  `mapstructure:"health"`
	Debug   DebugConfig   `mapstructure:"debug"`
	Workers int           `mapstructure:"workers"`
}

// ServerConfig contains HTTP server configuration
type ServerConfig struct {
	Host            string        `mapstructure:"host"`
	Port            int           `mapstructure:"port"`
	ReadTimeout     time.Duration `mapstructure:"read_timeout"`
	WriteTimeout    time.Duration `mapstructure:"write_timeout"`
	IdleTimeout     time.Duration `mapstructure:"idle_timeout"`
	ShutdownTimeout time.Duration `mapstructure:"shutdown_timeout"`
}

// LoggingConfig contains logging configuration
// Supports progressive logging profiles per Fulmen Forge Workhorse Standard:
// - SIMPLE: Console output only, minimal configuration (CLI tools)
// - STRUCTURED: Structured sinks, correlation IDs (API services)
// - ENTERPRISE: Multiple sinks, middleware, throttling, policy enforcement (production)
type LoggingConfig struct {
	// Level controls the minimum log level
	// Valid values: trace, debug, info, warn, error
	Level string `mapstructure:"level"`

	// Profile selects the logging complexity level
	// Valid values: SIMPLE, STRUCTURED, ENTERPRISE
	// See: gofulmen/docs/crucible-go/standards/observability/logging.md
	Profile string `mapstructure:"profile"`
}

// MetricsConfig contains Prometheus metrics configuration
type MetricsConfig struct {
	// Enabled controls whether metrics are exposed
	Enabled bool `mapstructure:"enabled"`

	// Port is the dedicated metrics endpoint port (Prometheus format)
	// Metrics are also available at the main HTTP port in JSON format
	Port int `mapstructure:"port"`
}

// HealthConfig contains health check configuration
type HealthConfig struct {
	// Enabled controls whether health endpoints are exposed
	Enabled bool `mapstructure:"enabled"`
}

// DebugConfig contains debug and profiling configuration
type DebugConfig struct {
	// Enabled controls whether debug mode is active
	Enabled bool `mapstructure:"enabled"`

	// PprofEnabled controls whether pprof endpoints are exposed
	// WARNING: Only enable in development/staging environments
	PprofEnabled bool `mapstructure:"pprof_enabled"`
}
