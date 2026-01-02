package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

type stubChecker struct {
	err error
}

func (s stubChecker) CheckHealth(ctx context.Context) error {
	return s.err
}

func TestHealthHandlerReturnsHealthyStatus(t *testing.T) {
	manager := NewHealthManager("1.2.3")
	manager.RegisterChecker("ok", stubChecker{err: nil})

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()

	manager.HealthHandler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	var resp HealthResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if resp.Status != "healthy" {
		t.Fatalf("expected healthy status, got %s", resp.Status)
	}

	if resp.Version != "1.2.3" {
		t.Fatalf("expected version 1.2.3, got %s", resp.Version)
	}

	if resp.Checks["ok"] != "healthy" {
		t.Fatalf("expected ok check to be healthy, got %s", resp.Checks["ok"])
	}
}

func TestHealthHandlerReturnsServiceUnavailableWhenUnhealthy(t *testing.T) {
	manager := NewHealthManager("1.2.3")
	manager.RegisterChecker("db", stubChecker{err: errors.New("down")})

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()

	manager.HealthHandler(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected status 503, got %d", rec.Code)
	}

	var resp struct {
		Error struct {
			Code    string                 `json:"code"`
			Message string                 `json:"message"`
			Details map[string]interface{} `json:"details"`
		} `json:"error"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if resp.Error.Code != "SERVICE_UNAVAILABLE" {
		t.Fatalf("expected SERVICE_UNAVAILABLE error code, got %s", resp.Error.Code)
	}

	details := resp.Error.Details
	if details == nil {
		t.Fatalf("expected error details to include probe context")
	}

	tests, ok := details["checks"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected checks in error details")
	}

	if status, ok := tests["db"].(string); !ok || status != "unhealthy" {
		t.Fatalf("expected db check to be unhealthy, got %v", tests["db"])
	}
}

func TestDetermineOverallStatusTreatsTimeoutAsDegraded(t *testing.T) {
	manager := NewHealthManager("dev")

	status := manager.determineOverallStatus(map[string]string{
		"db": "timeout",
	})

	if status != "degraded" {
		t.Fatalf("expected degraded status, got %s", status)
	}
}

func TestInitHealthManager(t *testing.T) {
	// Save original
	original := globalHealthManager
	defer func() { globalHealthManager = original }()

	// Reset global
	globalHealthManager = nil

	InitHealthManager("test-version")

	if globalHealthManager == nil {
		t.Fatal("expected global manager to be initialized")
	}
}

func TestGetHealthManager(t *testing.T) {
	// Save original
	original := globalHealthManager
	defer func() { globalHealthManager = original }()

	t.Run("returns nil when not initialized", func(t *testing.T) {
		globalHealthManager = nil
		manager := GetHealthManager()
		if manager != nil {
			t.Fatal("expected nil manager")
		}
	})

	t.Run("returns manager after init", func(t *testing.T) {
		InitHealthManager("1.0.0")
		manager := GetHealthManager()
		if manager == nil {
			t.Fatal("expected non-nil manager")
		}
	})
}

func TestGlobalHealthHandler(t *testing.T) {
	// Save original
	original := globalHealthManager
	defer func() { globalHealthManager = original }()

	InitHealthManager("test-version")

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()

	HealthHandler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}
}

func TestGlobalLivenessHandler(t *testing.T) {
	// Save original
	original := globalHealthManager
	defer func() { globalHealthManager = original }()

	InitHealthManager("test-version")

	req := httptest.NewRequest(http.MethodGet, "/health/live", nil)
	rec := httptest.NewRecorder()

	LivenessHandler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}
}

func TestGlobalReadinessHandler(t *testing.T) {
	// Save original
	original := globalHealthManager
	defer func() { globalHealthManager = original }()

	InitHealthManager("test-version")

	req := httptest.NewRequest(http.MethodGet, "/health/ready", nil)
	rec := httptest.NewRecorder()

	ReadinessHandler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}
}

func TestGlobalStartupHandler(t *testing.T) {
	// Save original
	original := globalHealthManager
	defer func() { globalHealthManager = original }()

	InitHealthManager("test-version")

	req := httptest.NewRequest(http.MethodGet, "/health/startup", nil)
	rec := httptest.NewRecorder()

	StartupHandler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}
}

func TestGlobalHandlers_WhenNotInitialized(t *testing.T) {
	// Save original
	original := globalHealthManager
	defer func() { globalHealthManager = original }()

	globalHealthManager = nil

	tests := []struct {
		name    string
		handler http.HandlerFunc
	}{
		{"HealthHandler", HealthHandler},
		{"LivenessHandler", LivenessHandler},
		{"ReadinessHandler", ReadinessHandler},
		{"StartupHandler", StartupHandler},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/test", nil)
			rec := httptest.NewRecorder()

			tt.handler(rec, req)

			// Should return 503 when not initialized
			if rec.Code != http.StatusServiceUnavailable {
				t.Fatalf("expected status 503 when not initialized, got %d", rec.Code)
			}
		})
	}
}
