package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/fulmenhq/gofulmen/errors"
)

// HealthResponse represents the aggregate health check response
type HealthResponse struct {
	Status    string            `json:"status"`
	Version   string            `json:"version"`
	Timestamp string            `json:"timestamp"`
	Checks    map[string]string `json:"checks,omitempty"`
}

// ProbeResponse represents individual probe response
type ProbeResponse struct {
	Status    string    `json:"status"`
	Timestamp time.Time `json:"timestamp"`
}

// HealthChecker defines interface for health checkable components
type HealthChecker interface {
	CheckHealth(ctx context.Context) error
}

// HealthManager manages health checks and probe states
type HealthManager struct {
	checkers map[string]HealthChecker
	version  string
}

// NewHealthManager creates a new health manager
func NewHealthManager(version string) *HealthManager {
	return &HealthManager{
		checkers: make(map[string]HealthChecker),
		version:  version,
	}
}

// RegisterChecker registers a health checker
func (hm *HealthManager) RegisterChecker(name string, checker HealthChecker) {
	hm.checkers[name] = checker
}

// runHealthChecks executes all registered health checks
func (hm *HealthManager) runHealthChecks(ctx context.Context) map[string]string {
	checks := make(map[string]string)

	for name, checker := range hm.checkers {
		select {
		case <-ctx.Done():
			checks[name] = "timeout"
			return checks
		default:
			if err := checker.CheckHealth(ctx); err != nil {
				checks[name] = "unhealthy"
			} else {
				checks[name] = "healthy"
			}
		}
	}

	return checks
}

// determineOverallStatus determines overall health status
func (hm *HealthManager) determineOverallStatus(checks map[string]string) string {
	degraded := false
	for _, status := range checks {
		if status == "unhealthy" {
			return "unhealthy"
		}
		if status == "degraded" || status == "timeout" {
			degraded = true
		}
	}

	// If we recorded any degraded/timeout checks, reflect that in aggregate status
	if degraded {
		return "degraded"
	}

	return "healthy"
}

// HealthHandler handles aggregate health check requests
func (hm *HealthManager) HealthHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Run health checks with timeout
	checkCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	checks := hm.runHealthChecks(checkCtx)
	status := hm.determineOverallStatus(checks)

	if status == "unhealthy" {
		envelope := errors.NewErrorEnvelope("SERVICE_UNAVAILABLE", "aggregate health check failed")
		envelope = enrichHealthEnvelope(envelope, "", status, checks)
		respondWithError(w, r, envelope)
		return
	}

	response := HealthResponse{
		Status:    status,
		Version:   hm.version,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Checks:    checks,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(response)
}

// LivenessHandler handles liveness probe requests
// Liveness indicates if the application is running
func (hm *HealthManager) LivenessHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Run health checks with timeout for liveness (shorter timeout)
	checkCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	checks := hm.runHealthChecks(checkCtx)
	status := hm.determineOverallStatus(checks)

	if status == "unhealthy" {
		envelope := errors.NewErrorEnvelope("SERVICE_UNAVAILABLE", "liveness probe failed")
		envelope = enrichHealthEnvelope(envelope, "live", status, checks)
		respondWithError(w, r, envelope)
		return
	}

	response := ProbeResponse{
		Status:    status,
		Timestamp: time.Now().UTC(),
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(response)
}

// ReadinessHandler handles readiness probe requests
// Readiness indicates if the application is ready to serve traffic
func (hm *HealthManager) ReadinessHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Run health checks with timeout for readiness
	checkCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	checks := hm.runHealthChecks(checkCtx)
	status := hm.determineOverallStatus(checks)

	if status == "unhealthy" {
		envelope := errors.NewErrorEnvelope("SERVICE_UNAVAILABLE", "readiness probe failed")
		envelope = enrichHealthEnvelope(envelope, "ready", status, checks)
		respondWithError(w, r, envelope)
		return
	}

	response := ProbeResponse{
		Status:    status,
		Timestamp: time.Now().UTC(),
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(response)
}

// StartupHandler handles startup probe requests
// Startup indicates if the application has completed initialization
func (hm *HealthManager) StartupHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Run health checks with timeout for startup
	checkCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	checks := hm.runHealthChecks(checkCtx)
	status := hm.determineOverallStatus(checks)

	if status == "unhealthy" {
		envelope := errors.NewErrorEnvelope("SERVICE_UNAVAILABLE", "startup probe failed")
		envelope = enrichHealthEnvelope(envelope, "startup", status, checks)
		respondWithError(w, r, envelope)
		return
	}

	response := ProbeResponse{
		Status:    status,
		Timestamp: time.Now().UTC(),
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(response)
}

func enrichHealthEnvelope(envelope *errors.ErrorEnvelope, probe, status string, checks map[string]string) *errors.ErrorEnvelope {
	if envelope == nil {
		return nil
	}

	details := map[string]interface{}{
		"status": status,
	}
	if len(checks) > 0 {
		details["checks"] = checks
	}
	if probe != "" {
		details["probe"] = probe
	}
	envelope = envelope.WithDetails(details)

	contextData := map[string]interface{}{
		"status": status,
	}
	if probe != "" {
		contextData["probe"] = probe
	}

	var unhealthy []string
	for name, result := range checks {
		if result != "healthy" {
			unhealthy = append(unhealthy, name)
		}
	}
	if len(unhealthy) > 0 {
		contextData["unhealthy_checks"] = unhealthy
	}

	envelope, _ = envelope.WithContext(contextData)
	return envelope
}

// Global health manager instance
var globalHealthManager *HealthManager

// InitHealthManager initializes the global health manager
func InitHealthManager(version string) {
	globalHealthManager = NewHealthManager(version)
}

// GetHealthManager returns the global health manager
func GetHealthManager() *HealthManager {
	return globalHealthManager
}

// LivenessHandler is the backward-compatible handler that uses the global manager
func LivenessHandler(w http.ResponseWriter, r *http.Request) {
	if globalHealthManager != nil {
		globalHealthManager.LivenessHandler(w, r)
		return
	}

	envelope := errors.NewErrorEnvelope("SERVICE_UNAVAILABLE", "health manager not initialized")
	envelope = enrichHealthEnvelope(envelope, "live", "unknown", nil)
	respondWithError(w, r, envelope)
}

// ReadinessHandler is the backward-compatible handler that uses the global manager
func ReadinessHandler(w http.ResponseWriter, r *http.Request) {
	if globalHealthManager != nil {
		globalHealthManager.ReadinessHandler(w, r)
		return
	}

	envelope := errors.NewErrorEnvelope("SERVICE_UNAVAILABLE", "health manager not initialized")
	envelope = enrichHealthEnvelope(envelope, "ready", "unknown", nil)
	respondWithError(w, r, envelope)
}

// StartupHandler is the backward-compatible handler that uses the global manager
func StartupHandler(w http.ResponseWriter, r *http.Request) {
	if globalHealthManager != nil {
		globalHealthManager.StartupHandler(w, r)
		return
	}

	envelope := errors.NewErrorEnvelope("SERVICE_UNAVAILABLE", "health manager not initialized")
	envelope = enrichHealthEnvelope(envelope, "startup", "unknown", nil)
	respondWithError(w, r, envelope)
}

// HealthHandler is the backward-compatible handler that uses the global manager
func HealthHandler(w http.ResponseWriter, r *http.Request) {
	if globalHealthManager != nil {
		globalHealthManager.HealthHandler(w, r)
		return
	}

	envelope := errors.NewErrorEnvelope("SERVICE_UNAVAILABLE", "health manager not initialized")
	envelope = enrichHealthEnvelope(envelope, "aggregate", "unknown", nil)
	respondWithError(w, r, envelope)
}
