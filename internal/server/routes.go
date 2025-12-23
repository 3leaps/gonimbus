package server

import (
	"context"
	"os"

	"github.com/fulmenhq/gofulmen/signals"

	"github.com/3leaps/gonimbus/internal/appid"
	"go.uber.org/zap"

	"github.com/3leaps/gonimbus/internal/observability"
	"github.com/3leaps/gonimbus/internal/server/handlers"
)

// registerRoutes registers all HTTP routes
func (s *Server) registerRoutes() {
	// Standard health endpoints per Workhorse §9
	s.router.Get("/health", handlers.HealthHandler)
	s.router.Get("/health/live", handlers.LivenessHandler)
	s.router.Get("/health/ready", handlers.ReadinessHandler)
	s.router.Get("/health/startup", handlers.StartupHandler)

	// Version endpoint
	s.router.Get("/version", handlers.VersionHandler)

	// Metrics endpoint (in server package to access HandleError)
	s.router.Get("/metrics", MetricsHandler)

	// Admin signal endpoint (optional, requires GONIMBUS_ADMIN_TOKEN)
	s.registerAdminEndpoint()
}

// registerAdminEndpoint optionally registers the admin signal endpoint
func (s *Server) registerAdminEndpoint() {
	// Get admin token from environment (identity-aware)
	ctx := context.Background()
	identity, _ := appid.Get(ctx)
	envPrefix := "WORKHORSE_"
	if identity != nil && identity.EnvPrefix != "" {
		envPrefix = identity.EnvPrefix
	}

	adminToken := os.Getenv(envPrefix + "ADMIN_TOKEN")
	logger := observability.ServerLogger

	if adminToken == "" {
		if logger != nil {
			logger.Debug("Admin signal endpoint disabled (no " + envPrefix + "ADMIN_TOKEN set)")
		}
		return
	}

	// Create HTTP signal handler with bearer token auth and rate limiting
	handler := signals.NewHTTPHandler(signals.HTTPConfig{
		TokenAuth: adminToken,
		RateLimit: 10,  // 10 requests per minute
		RateBurst: 5,   // burst size
		Manager:   nil, // use default global manager
	})

	// Register admin endpoint
	s.router.Post("/admin/signal", handler.ServeHTTP)

	if logger != nil {
		logger.Info("Admin signal endpoint enabled",
			zap.String("path", "/admin/signal"),
			zap.String("auth", "bearer token"),
			zap.String("rate_limit", "10/min, burst 5"))
		logger.Warn("Admin endpoint enabled - ensure this server is not exposed to public internet")
	}
}
