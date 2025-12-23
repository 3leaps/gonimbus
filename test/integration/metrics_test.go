package integration

import (
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/3leaps/gonimbus/internal/observability"
	"github.com/3leaps/gonimbus/internal/server"
	"github.com/3leaps/gonimbus/internal/server/handlers"
	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// cleanupMetrics tears down global telemetry state so each test starts clean.
// This matters in sandboxes where lingering exporters can block future binds.
func cleanupMetrics(t *testing.T) {
	t.Helper()
	t.Cleanup(func() {
		if observability.PrometheusExporter != nil {
			_ = observability.PrometheusExporter.Stop()
			observability.PrometheusExporter = nil
		}
		observability.TelemetrySystem = nil
	})
}

// isPermissionError normalizes OS-specific permission errors (macOS/Linux/BSD)
// so we can gracefully skip when loopback sockets are blocked.
func isPermissionError(err error) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, os.ErrPermission) || errors.Is(err, syscall.EACCES) {
		return true
	}

	msg := strings.ToLower(err.Error())
	for _, fragment := range []string{"permission denied", "operation not permitted", "not permitted"} {
		if strings.Contains(msg, fragment) {
			return true
		}
	}

	return false
}

// initMetricsOrSkip attempts to start the metrics exporter; if the environment
// forbids network binds we skip instead of failing the entire suite.
func initMetricsOrSkip(t *testing.T) {
	t.Helper()

	if err := observability.InitMetrics("test", 0, "test"); err != nil {
		if isPermissionError(err) {
			t.Skipf("skipping metrics tests due to sandbox permissions: %v", err)
		}
		require.NoError(t, err)
	}

	cleanupMetrics(t)
}

// newTestServer binds to IPv4 loopback explicitly (avoiding IPv6-only defaults)
// and skips when the sandbox refuses to open sockets.
func newTestServer(t *testing.T, setup func(*chi.Mux)) (*httptest.Server, *http.Client) {
	t.Helper()
	srv := server.New("127.0.0.1", 0)
	if setup != nil {
		if mux, ok := srv.Handler().(*chi.Mux); ok {
			setup(mux)
		}
	}

	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		if isPermissionError(err) {
			t.Skipf("skipping metrics server setup: %v", err)
		}
		require.NoError(t, err)
	}

	ts := &httptest.Server{
		Listener: listener,
		Config:   &http.Server{Handler: srv.Handler()},
	}
	ts.Start()
	t.Cleanup(ts.Close)
	return ts, ts.Client()
}

func TestMetricsEndpoint_Integration(t *testing.T) {
	observability.InitCLILogger("test", false)
	observability.InitServerLogger("test", "info")

	initMetricsOrSkip(t)

	handlers.InitHealthManager("test")

	ts, client := newTestServer(t, func(mux *chi.Mux) {
		mux.Get("/fast", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("fast response"))
		})
		mux.Get("/slow", func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(50 * time.Millisecond)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("slow response"))
		})
		mux.Get("/error", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("error response"))
		})
	})

	serverURL := ts.URL

	const numRequests = 50
	const numWorkers = 10

	requestChan := make(chan int, numRequests)
	for i := 0; i < numRequests; i++ {
		requestChan <- i
	}
	close(requestChan)

	start := time.Now()

	var wg sync.WaitGroup
	wg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		go func() {
			defer wg.Done()
			for reqNum := range requestChan {
				var path string
				switch reqNum % 4 {
				case 0:
					path = "/fast"
				case 1:
					path = "/slow"
				case 2:
					path = "/error"
				default:
					path = "/health"
				}

				resp, err := client.Get(serverURL + path)
				if err == nil {
					require.NoError(t, resp.Body.Close())
				}
			}
		}()
	}
	wg.Wait()

	elapsed := time.Since(start)

	resp, err := client.Get(serverURL + "/metrics")
	require.NoError(t, err)
	body, readErr := io.ReadAll(resp.Body)
	require.NoError(t, resp.Body.Close())
	require.NoError(t, readErr)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	metricsContent := string(body)
	assert.Contains(t, metricsContent, "test_http_requests_total", "Should have HTTP request metrics")
	assert.Contains(t, metricsContent, "test_http_request_duration_ms", "Should have duration metrics")
	assert.True(t, elapsed < 5*time.Second, "Load test should complete in reasonable time")
	t.Logf("Load test completed: %d requests in %v (%.2f req/s)", numRequests, elapsed, float64(numRequests)/elapsed.Seconds())
}

func TestMetricsEndpoint_PrometheusFormat(t *testing.T) {
	observability.InitCLILogger("test", false)
	observability.InitServerLogger("test", "info")

	initMetricsOrSkip(t)

	handlers.InitHealthManager("test")

	ts, client := newTestServer(t, func(mux *chi.Mux) {
		mux.Get("/format-test", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"message": "format test"}`))
		})
	})

	serverURL := ts.URL

	resp, err := client.Get(serverURL + "/format-test")
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	resp, err = client.Get(serverURL + "/metrics")
	require.NoError(t, err)
	contentType := resp.Header.Get("Content-Type")
	assert.True(t,
		contentType == "text/plain; version=0.0.4" ||
			contentType == "text/plain; version=0.0.4; charset=utf-8",
		"Expected Prometheus content type, got: %s", contentType)

	body, readErr := io.ReadAll(resp.Body)
	require.NoError(t, resp.Body.Close())
	require.NoError(t, readErr)
	metricsContent := string(body)

	lines := strings.Split(strings.TrimSpace(metricsContent), "\n")
	hasValidMetrics := false
	for _, line := range lines {
		if strings.HasPrefix(line, "#") {
			continue
		}
		if strings.Contains(line, "{") && len(strings.Fields(line)) >= 2 {
			hasValidMetrics = true
			break
		}
	}
	assert.True(t, hasValidMetrics, "Should have valid Prometheus metric lines")

	metricLines := 0
	for _, line := range lines {
		if !strings.HasPrefix(line, "#") && strings.TrimSpace(line) != "" {
			metricLines++
		}
	}
	assert.Greater(t, metricLines, 0, "Should have actual metric values")
}

func TestMetricsEndpoint_WithTelemetryDisabled(t *testing.T) {
	observability.InitCLILogger("test", false)
	observability.InitServerLogger("test", "info")

	originalExporter := observability.PrometheusExporter
	originalTelemetry := observability.TelemetrySystem
	observability.PrometheusExporter = nil
	observability.TelemetrySystem = nil
	t.Cleanup(func() {
		observability.PrometheusExporter = originalExporter
		observability.TelemetrySystem = originalTelemetry
	})

	originalEnabled := os.Getenv("GONIMBUS_METRICS_ENABLED")
	_ = os.Setenv("GONIMBUS_METRICS_ENABLED", "false")
	t.Cleanup(func() {
		if originalEnabled != "" {
			_ = os.Setenv("GONIMBUS_METRICS_ENABLED", originalEnabled)
		} else {
			_ = os.Unsetenv("GONIMBUS_METRICS_ENABLED")
		}
	})

	handlers.InitHealthManager("test")

	ts, client := newTestServer(t, func(mux *chi.Mux) {
		mux.Get("/test", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("test"))
		})
	})

	serverURL := ts.URL

	resp, err := client.Get(serverURL + "/test")
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	resp, err = client.Get(serverURL + "/metrics")
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
}

// Test with real command execution
func TestMetrics_CommandIntegration(t *testing.T) {
	t.Skip("Command integration test skipped - cmd.NewRootCommand not exported")
}
