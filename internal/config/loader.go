// Package config provides centralized configuration management for the Gonimbus service.
// It implements the three-layer config pattern using gofulmen/config:
// Layer 1: Crucible defaults (config/gonimbus/v1.0.0/gonimbus-defaults.yaml)
// Layer 2: User overrides (discovered via app identity)
// Layer 3: Environment variables and runtime overrides
package config

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/fulmenhq/gofulmen/appidentity"
	gfconfig "github.com/fulmenhq/gofulmen/config"

	"github.com/3leaps/gonimbus/internal/appid"
	"github.com/fulmenhq/gofulmen/pathfinder"
	"github.com/fulmenhq/gofulmen/schema"
	"github.com/go-viper/mapstructure/v2"
)

var (
	// appConfig holds the current application configuration
	appConfig   *Config
	configMu    sync.RWMutex
	appIdentity *appidentity.Identity
)

// findProjectRoot walks up from the current working directory to find the project root.
// It looks for project markers like go.mod or .git directory.
// This ensures config paths work correctly regardless of where the process is run from.
//
// This now uses gofulmen/pathfinder.FindRepositoryRoot() which provides:
// - Security boundaries (home directory ceiling, max depth protection)
// - Symlink loop detection
// - Cross-platform compatibility
// - Performance optimized (<30µs)
func findProjectRoot() (string, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("failed to get current directory: %w", err)
	}

	markers := []string{"go.mod", ".git"}

	// CI-only boundary hint pattern:
	// - Treat CI workspace env vars as a boundary hint, not "the root".
	// - Still require repository markers.
	isCI := strings.EqualFold(strings.TrimSpace(os.Getenv("GITHUB_ACTIONS")), "true") ||
		strings.EqualFold(strings.TrimSpace(os.Getenv("CI")), "true")
	if isCI {
		boundaryKeys := []string{"FULMEN_WORKSPACE_ROOT", "GITHUB_WORKSPACE", "CI_PROJECT_DIR", "WORKSPACE"}
		for _, key := range boundaryKeys {
			boundary := strings.TrimSpace(os.Getenv(key))
			if boundary == "" {
				continue
			}
			boundary = filepath.Clean(boundary)
			if !filepath.IsAbs(boundary) {
				continue
			}
			st, err := os.Stat(boundary)
			if err != nil || !st.IsDir() {
				continue
			}
			// Only accept a boundary that contains the start path.
			if rel, err := filepath.Rel(boundary, cwd); err != nil || strings.HasPrefix(rel, "..") {
				continue
			}
			rootPath, err := pathfinder.FindRepositoryRoot(cwd, markers,
				pathfinder.WithBoundary(boundary),
				pathfinder.WithMaxDepth(20),
			)
			if err == nil {
				return rootPath, nil
			}
		}
	}

	rootPath, err := pathfinder.FindRepositoryRoot(cwd, markers, pathfinder.WithMaxDepth(10))
	if err != nil {
		return "", fmt.Errorf("project root not found: %w", err)
	}

	return rootPath, nil
}

// EnvVarSpec defines environment variable mappings for config fields
// following the pattern: {PREFIX}{NAME} maps to config path
type EnvVarSpec = gfconfig.EnvVarSpec

// Environment variable types
const (
	EnvString = gfconfig.EnvString
	EnvInt    = gfconfig.EnvInt
	EnvBool   = gfconfig.EnvBool
)

// Load loads configuration using the three-layer pattern:
// 1. Crucible defaults from config/gonimbus-defaults.yaml
// 2. User overrides from XDG config paths
// 3. Environment variables and runtime overrides
//
// This function is safe to call multiple times (e.g., for config reload)
func Load(ctx context.Context, runtimeOverrides ...map[string]any) (*Config, error) {
	// Get app identity if not already loaded
	if appIdentity == nil {
		identity, err := appid.Get(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to load app identity: %w", err)
		}
		appIdentity = identity
	}

	// Find project root for absolute paths
	// This ensures config loading works from any working directory (including tests)
	projectRoot, err := findProjectRoot()
	if err != nil {
		return nil, fmt.Errorf("failed to find project root: %w", err)
	}

	// Build layered config options
	// Gonimbus uses its own schema (not from Crucible) located in schemas/gonimbus/
	// Defaults are in config/gonimbus/v1.0.0/gonimbus-defaults.yaml
	// Using absolute paths ensures this works from any working directory
	catalog := schema.NewCatalog(filepath.Join(projectRoot, "schemas"))
	opts := gfconfig.LayeredConfigOptions{
		Category:     "gonimbus",
		Version:      "v1.0.0",
		DefaultsFile: "gonimbus-defaults.yaml",
		SchemaID:     "gonimbus/v1.0.0/config",
		UserPaths:    getUserConfigPaths(),
		Catalog:      catalog,
		DefaultsRoot: filepath.Join(projectRoot, "config"), // Absolute path for Layer 2 template
	}

	// Load environment variable overrides
	envOverrides, err := gfconfig.LoadEnvOverrides(getEnvSpecs())
	if err != nil {
		return nil, fmt.Errorf("failed to load environment overrides: %w", err)
	}

	// Combine environment overrides with runtime overrides
	allOverrides := []map[string]any{envOverrides}
	allOverrides = append(allOverrides, runtimeOverrides...)

	// Load layered configuration
	merged, diagnostics, err := gfconfig.LoadLayeredConfig(opts, allOverrides...)
	if err != nil {
		return nil, fmt.Errorf("failed to load layered config: %w", err)
	}

	// Log validation diagnostics (warnings/errors from schema validation)
	// Note: We log these but don't fail hard to maintain flexibility
	for _, diag := range diagnostics {
		// TODO: Use logger when available
		fmt.Printf("Config validation: %s: %s\n", diag.Pointer, diag.Message)
	}

	// Unmarshal into typed config struct
	cfg := &Config{}
	decoder, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		Result:           cfg,
		WeaklyTypedInput: true,
		DecodeHook: mapstructure.ComposeDecodeHookFunc(
			mapstructure.StringToTimeDurationHookFunc(),
			mapstructure.StringToSliceHookFunc(","),
		),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create decoder: %w", err)
	}

	if err := decoder.Decode(merged); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	// Store the loaded config
	setConfig(cfg)

	return cfg, nil
}

// GetConfig returns the current application configuration (thread-safe)
func GetConfig() *Config {
	configMu.RLock()
	defer configMu.RUnlock()
	return appConfig
}

// setConfig updates the current configuration (thread-safe)
func setConfig(cfg *Config) {
	configMu.Lock()
	defer configMu.Unlock()
	appConfig = cfg
}

// getUserConfigPaths returns the list of user config file paths to check
// Uses gofulmen/config for XDG-compliant path discovery
func getUserConfigPaths() []string {
	if appIdentity == nil {
		return []string{}
	}

	// Get standard config paths from gofulmen
	standardPaths := gfconfig.GetConfigPaths()

	// Convert generic paths to app-specific paths
	// Example: ~/.config/gofulmen/config.yaml → ~/.config/gonimbus/config.yaml
	var paths []string
	for _, p := range standardPaths {
		// Replace "gofulmen" with our app's config name
		customPath := strings.ReplaceAll(p, "gofulmen", appIdentity.ConfigName)
		paths = append(paths, customPath)
	}

	return paths
}

// getEnvSpecs returns environment variable specifications for config mapping
// Maps {PREFIX}{NAME} environment variables to config paths
func getEnvSpecs() []EnvVarSpec {
	if appIdentity == nil {
		return []EnvVarSpec{}
	}

	prefix := appIdentity.EnvPrefix
	if !strings.HasSuffix(prefix, "_") {
		prefix += "_"
	}

	return []EnvVarSpec{
		// Server config
		{Name: prefix + "HOST", Path: []string{"server", "host"}, Type: EnvString},
		{Name: prefix + "PORT", Path: []string{"server", "port"}, Type: EnvInt},
		// Duration fields are parsed as strings and converted by mapstructure decode hook
		{Name: prefix + "READ_TIMEOUT", Path: []string{"server", "read_timeout"}, Type: EnvString},
		{Name: prefix + "WRITE_TIMEOUT", Path: []string{"server", "write_timeout"}, Type: EnvString},
		{Name: prefix + "IDLE_TIMEOUT", Path: []string{"server", "idle_timeout"}, Type: EnvString},
		{Name: prefix + "SHUTDOWN_TIMEOUT", Path: []string{"server", "shutdown_timeout"}, Type: EnvString},

		// Logging config (REQUIRED per Workhorse Standard)
		{Name: prefix + "LOG_LEVEL", Path: []string{"logging", "level"}, Type: EnvString},
		{Name: prefix + "LOG_PROFILE", Path: []string{"logging", "profile"}, Type: EnvString},

		// Metrics config
		{Name: prefix + "METRICS_ENABLED", Path: []string{"metrics", "enabled"}, Type: EnvBool},
		{Name: prefix + "METRICS_PORT", Path: []string{"metrics", "port"}, Type: EnvInt},

		// Health config
		{Name: prefix + "HEALTH_ENABLED", Path: []string{"health", "enabled"}, Type: EnvBool},

		// Debug config
		{Name: prefix + "DEBUG_ENABLED", Path: []string{"debug", "enabled"}, Type: EnvBool},
		{Name: prefix + "DEBUG_PPROF_ENABLED", Path: []string{"debug", "pprof_enabled"}, Type: EnvBool},

		// Workers
		{Name: prefix + "WORKERS", Path: []string{"workers"}, Type: EnvInt},
	}
}
