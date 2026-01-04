package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	apperrors "github.com/3leaps/gonimbus/internal/errors"
	"github.com/3leaps/gonimbus/internal/server/handlers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestServerUsesStandardErrorHandlers(t *testing.T) {
	srv := New("127.0.0.1", 0)

	req := httptest.NewRequest(http.MethodGet, "/does-not-exist", nil)
	rec := httptest.NewRecorder()

	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected status 404, got %d", rec.Code)
	}

	var body apperrors.HTTPErrorResponse
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode error response: %v", err)
	}

	if body.Error.Code != "NOT_FOUND" {
		t.Fatalf("expected error code NOT_FOUND, got %s", body.Error.Code)
	}
}

func TestServer_Port(t *testing.T) {
	tests := []struct {
		name string
		port int
	}{
		{"default port", 8080},
		{"custom port", 9000},
		{"zero port", 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := New("127.0.0.1", tt.port)
			assert.Equal(t, tt.port, srv.Port())
		})
	}
}

func TestServer_Handler(t *testing.T) {
	srv := New("127.0.0.1", 8080)
	handler := srv.Handler()
	assert.NotNil(t, handler)
}

func TestServer_MethodNotAllowed(t *testing.T) {
	srv := New("127.0.0.1", 0)

	// POST to a GET-only endpoint should return 405
	req := httptest.NewRequest(http.MethodPost, "/version", nil)
	rec := httptest.NewRecorder()

	srv.Handler().ServeHTTP(rec, req)

	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)

	var body apperrors.HTTPErrorResponse
	err := json.NewDecoder(rec.Body).Decode(&body)
	require.NoError(t, err)

	assert.Equal(t, "METHOD_NOT_ALLOWED", body.Error.Code)
}

func TestServer_RoutesRegistered(t *testing.T) {
	// Initialize health manager for health endpoint tests
	handlers.InitHealthManager("test")

	srv := New("127.0.0.1", 0)

	endpoints := []struct {
		method string
		path   string
		want   int // expected status (200 or other success code)
	}{
		{"GET", "/health", http.StatusOK},
		{"GET", "/health/live", http.StatusOK},
		{"GET", "/health/ready", http.StatusOK},
		{"GET", "/health/startup", http.StatusOK},
		{"GET", "/version", http.StatusOK},
	}

	for _, ep := range endpoints {
		t.Run(ep.method+" "+ep.path, func(t *testing.T) {
			req := httptest.NewRequest(ep.method, ep.path, nil)
			rec := httptest.NewRecorder()

			srv.Handler().ServeHTTP(rec, req)

			// Just verify route is registered and returns expected status
			assert.Equal(t, ep.want, rec.Code, "endpoint %s %s should return %d", ep.method, ep.path, ep.want)
		})
	}
}

func TestServer_AdminEndpointDisabledByDefault(t *testing.T) {
	// Ensure no admin token is set
	t.Setenv("GONIMBUS_ADMIN_TOKEN", "")
	t.Setenv("WORKHORSE_ADMIN_TOKEN", "")

	srv := New("127.0.0.1", 0)

	// Admin endpoint should not be registered
	req := httptest.NewRequest(http.MethodPost, "/admin/signal", nil)
	rec := httptest.NewRecorder()

	srv.Handler().ServeHTTP(rec, req)

	// Should be 404 (not found) since endpoint is not registered
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// Note: TestServer_AdminEndpointEnabled is skipped because it requires
// controlling appid.Get() return value which depends on global state.
// The registerAdminEndpoint function is tested implicitly through
// TestServer_AdminEndpointDisabledByDefault which covers the "no token" path.
