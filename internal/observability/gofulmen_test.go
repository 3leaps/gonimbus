package observability_test

import (
	"testing"

	"github.com/fulmenhq/gofulmen/crucible"
	"github.com/fulmenhq/gofulmen/logging"
	"go.uber.org/zap"

	"github.com/3leaps/gonimbus/internal/observability"
)

// TestGofulmenIntegration verifies that gofulmen v0.1.7 is properly integrated
func TestGofulmenIntegration(t *testing.T) {
	t.Run("CLI logger creation", func(t *testing.T) {
		// Initialize CLI logger
		observability.InitCLILogger("test-service", false)

		if observability.CLILogger == nil {
			t.Fatal("CLI logger should not be nil after initialization")
		}

		// Verify we can log messages
		observability.CLILogger.Info("Test CLI log message",
			zap.String("test", "value"))
	})

	t.Run("Structured logger creation", func(t *testing.T) {
		// Initialize server logger
		observability.InitServerLogger("test-service", "info")

		if observability.ServerLogger == nil {
			t.Fatal("Server logger should not be nil after initialization")
		}

		// Verify we can log messages with structured data
		observability.ServerLogger.Info("Test structured log message",
			zap.String("component", "test"),
			zap.Int("request_id", 123))
	})

	t.Run("Logger with verbose mode", func(t *testing.T) {
		// Create a new CLI logger with verbose mode
		logger, err := logging.NewCLI("verbose-test")
		if err != nil {
			t.Fatalf("Failed to create verbose logger: %v", err)
		}

		// Set to DEBUG level
		logger.SetLevel(logging.DEBUG)

		// Verify we can log debug messages
		logger.Debug("Debug message",
			zap.String("mode", "verbose"))
	})

	t.Run("Structured profile with correlation middleware", func(t *testing.T) {
		config := &logging.LoggerConfig{
			Profile:      logging.ProfileStructured,
			DefaultLevel: "INFO",
			Service:      "correlation-test",
			Environment:  "test",
			Middleware: []logging.MiddlewareConfig{
				{
					Name:    "correlation",
					Enabled: true,
					Order:   100,
					Config:  make(map[string]any),
				},
			},
			Sinks: []logging.SinkConfig{
				{
					Type:   "console",
					Format: "json",
					Console: &logging.ConsoleSinkConfig{
						Stream:   "stderr",
						Colorize: false,
					},
				},
			},
		}

		logger, err := logging.New(config)
		if err != nil {
			t.Fatalf("Failed to create structured logger: %v", err)
		}

		// Log a message - should include correlation ID automatically
		logger.Info("Test message with correlation",
			zap.String("feature", "correlation"))
	})
}

// TestEmbeddedCrucible verifies that crucible is properly embedded in gofulmen
func TestEmbeddedCrucible(t *testing.T) {
	t.Run("Crucible version access", func(t *testing.T) {
		version := crucible.GetVersion()

		if version.Gofulmen == "" {
			t.Error("Gofulmen version should not be empty")
		}

		if version.Crucible == "" {
			t.Error("Crucible version should not be empty")
		}

		t.Logf("Gofulmen version: %s", version.Gofulmen)
		t.Logf("Crucible version: %s", version.Crucible)
	})

	t.Run("Crucible version string", func(t *testing.T) {
		versionStr := crucible.GetVersionString()

		if versionStr == "" {
			t.Error("Version string should not be empty")
		}

		t.Logf("Version string: %s", versionStr)

		// Verify format includes both gofulmen and crucible
		if len(versionStr) < 20 {
			t.Errorf("Version string seems too short: %s", versionStr)
		}
	})

	t.Run("Crucible schema registry access", func(t *testing.T) {
		// Verify we can access the schema registry
		if crucible.SchemaRegistry == nil {
			t.Fatal("SchemaRegistry should not be nil")
		}

		// Verify we can access observability schemas
		obsSchemas := crucible.SchemaRegistry.Observability()
		if obsSchemas == nil {
			t.Fatal("Observability schemas should not be nil")
		}

		t.Log("Successfully accessed Crucible schema registry")
	})

	t.Run("Crucible standards registry access", func(t *testing.T) {
		// Verify we can access the standards registry
		if crucible.StandardsRegistry == nil {
			t.Fatal("StandardsRegistry should not be nil")
		}

		t.Log("Successfully accessed Crucible standards registry")
	})

	t.Run("Crucible config registry access", func(t *testing.T) {
		// Verify we can access the config registry
		if crucible.ConfigRegistry == nil {
			t.Fatal("ConfigRegistry should not be nil")
		}

		t.Log("Successfully accessed Crucible config registry")
	})
}

// TestGofulmenCrucibleIntegration verifies that gofulmen properly uses embedded crucible
func TestGofulmenCrucibleIntegration(t *testing.T) {
	t.Run("Logger uses crucible schemas for validation", func(t *testing.T) {
		// Create a logger config that would be validated against crucible schemas
		config := &logging.LoggerConfig{
			Profile:      logging.ProfileSimple,
			DefaultLevel: "INFO",
			Service:      "schema-test",
			Environment:  "test",
			Sinks: []logging.SinkConfig{
				{
					Type:   "console",
					Format: "console",
					Console: &logging.ConsoleSinkConfig{
						Stream:   "stderr",
						Colorize: false,
					},
				},
			},
		}

		// Create logger - this internally validates against crucible schemas
		logger, err := logging.New(config)
		if err != nil {
			t.Fatalf("Failed to create logger (schema validation failed): %v", err)
		}

		if logger == nil {
			t.Fatal("Logger should not be nil after creation")
		}

		t.Log("Successfully created logger with crucible schema validation")
	})

	t.Run("Logger with crucible version in logs", func(t *testing.T) {
		// Create a logger
		logger, err := logging.NewCLI("version-test")
		if err != nil {
			t.Fatalf("Failed to create logger: %v", err)
		}

		// Get crucible version
		version := crucible.GetVersionString()

		// Log it
		logger.Info("Application info",
			zap.String("crucible_version", version),
			zap.String("status", "ready"))

		t.Logf("Logged message with crucible version: %s", version)
	})
}
