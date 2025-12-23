package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/3leaps/gonimbus/internal/observability"
	"github.com/fulmenhq/gofulmen/telemetry"
	telemetrytesting "github.com/fulmenhq/gofulmen/telemetry/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTelemetry(t *testing.T) *telemetrytesting.FakeCollector {
	t.Helper()

	collector := telemetrytesting.NewFakeCollector()
	config := &telemetry.Config{
		Enabled: true,
		Emitter: collector,
	}

	sys, err := telemetry.NewSystem(config)
	require.NoError(t, err)

	originalTelemetry := observability.TelemetrySystem
	observability.TelemetrySystem = sys

	t.Cleanup(func() {
		observability.TelemetrySystem = originalTelemetry
	})

	return collector
}

func TestRequestMetrics_BasicFunctionality(t *testing.T) {
	collector := setupTelemetry(t)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("test response"))
	})

	middleware := RequestMetrics(handler)

	req := httptest.NewRequest("GET", "/test", nil)
	rec := httptest.NewRecorder()

	middleware.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "test response", rec.Body.String())

	assert.Greater(t, collector.CountMetricsByName("http_requests_total"), 0,
		"expected http_requests_total metric to be emitted")
	assert.Greater(t, collector.CountMetricsByName("http_request_duration_ms"), 0,
		"expected http_request_duration_ms metric to be emitted")
}

func TestRequestMetrics_WithTelemetryDisabled(t *testing.T) {
	// Disable telemetry
	originalTelemetry := observability.TelemetrySystem
	observability.TelemetrySystem = nil
	defer func() {
		observability.TelemetrySystem = originalTelemetry
	}()

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	middleware := RequestMetrics(handler)

	req := httptest.NewRequest("GET", "/test", nil)
	rec := httptest.NewRecorder()

	// Should not panic and should work normally
	middleware.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestRequestMetrics_WithErrorStatus(t *testing.T) {
	collector := setupTelemetry(t)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})

	middleware := RequestMetrics(handler)

	req := httptest.NewRequest("GET", "/test", nil)
	rec := httptest.NewRecorder()

	middleware.ServeHTTP(rec, req)

	assert.Greater(t, collector.CountMetricsByName("http_requests_total"), 0)
	assert.Greater(t, collector.CountMetricsByName("http_errors_total"), 0,
		"expected http_errors_total metric for non-2xx response")
}

func TestRequestMetrics_WithRequestSize(t *testing.T) {
	collector := setupTelemetry(t)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	middleware := RequestMetrics(handler)

	req := httptest.NewRequest("POST", "/test", nil)
	req.Header.Set("Content-Length", "1024")
	rec := httptest.NewRecorder()

	middleware.ServeHTTP(rec, req)

	assert.Greater(t, collector.CountMetricsByName("http_request_size_bytes"), 0,
		"expected http_request_size_bytes gauge to be emitted")
}

func TestRequestMetrics_WithResponseSize(t *testing.T) {
	collector := setupTelemetry(t)

	responseBody := "test response with some content"
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(responseBody))
	})

	middleware := RequestMetrics(handler)

	req := httptest.NewRequest("GET", "/test", nil)
	rec := httptest.NewRecorder()

	middleware.ServeHTTP(rec, req)

	assert.Greater(t, collector.CountMetricsByName("http_response_size_bytes"), 0,
		"expected http_response_size_bytes metric to be emitted")
}

func TestGetEndpointPattern_StandardPaths(t *testing.T) {
	tests := []struct {
		path     string
		expected string
	}{
		{"/health", "/health/*"},
		{"/health/live", "/health/*"},
		{"/health/ready", "/health/*"},
		{"/health/startup", "/health/*"},
		{"/version", "/version"},
		{"/metrics", "/metrics"},
		{"/api/users/123", "/unknown"},
		{"/", "/"},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			req := httptest.NewRequest("GET", tt.path, nil)
			pattern := getEndpointPattern(req)
			assert.Equal(t, tt.expected, pattern, "Path %s should map to pattern %s", tt.path, tt.expected)
		})
	}
}

func TestRequestMetrics_WithRequestID(t *testing.T) {
	collector := setupTelemetry(t)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	middleware := RequestID(RequestMetrics(handler))

	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("X-Request-ID", "test-request-id")
	rec := httptest.NewRecorder()

	middleware.ServeHTTP(rec, req)

	assert.Equal(t, "test-request-id", rec.Header().Get("X-Request-ID"))
	assert.Greater(t, collector.CountMetricsByName("http_requests_total"), 0)
}

func TestRequestMetrics_DurationMeasurement(t *testing.T) {
	collector := setupTelemetry(t)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(10 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	})

	middleware := RequestMetrics(handler)

	req := httptest.NewRequest("GET", "/test", nil)
	rec := httptest.NewRecorder()

	start := time.Now()
	middleware.ServeHTTP(rec, req)
	elapsed := time.Since(start)

	assert.True(t, elapsed >= 10*time.Millisecond, "Should have waited at least 10ms")
	assert.Greater(t, collector.CountMetricsByName("http_request_duration_ms"), 0,
		"expected http_request_duration_ms metric to be emitted")
}
