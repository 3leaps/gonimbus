package server

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/3leaps/gonimbus/internal/observability"
	"github.com/fulmenhq/gofulmen/telemetry/exporters"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func TestMetricsHandlerProxiesPrometheusOutput(t *testing.T) {
	originalClient := metricsProxyClient
	t.Cleanup(func() {
		metricsProxyClient = originalClient
	})

	metricsProxyClient = &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			body := "# HELP http_requests_total Total number of HTTP requests\nhttp_requests_total 1\n"
			resp := &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(body)),
				Header:     make(http.Header),
			}
			resp.Header.Set("Content-Type", "text/plain; version=0.0.4")
			return resp, nil
		}),
	}

	observability.PrometheusExporter = exporters.NewPrometheusExporter("test", ":9090")
	t.Cleanup(func() {
		observability.PrometheusExporter = nil
	})

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()

	MetricsHandler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	contentType := rec.Header().Get("Content-Type")
	if !strings.Contains(contentType, "text/plain") {
		t.Fatalf("expected text/plain content type, got %s", contentType)
	}

	body := rec.Body.String()
	if !strings.Contains(body, "http_requests_total") {
		t.Fatalf("expected Prometheus output to include metric name, got: %s", body)
	}
}

func TestMetricsHandlerReturnsServiceUnavailableWithoutExporter(t *testing.T) {
	observability.PrometheusExporter = nil

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()

	MetricsHandler(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected status 503, got %d", rec.Code)
	}

	var resp struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if resp.Error.Code != "SERVICE_UNAVAILABLE" {
		t.Fatalf("expected error code SERVICE_UNAVAILABLE, got %s", resp.Error.Code)
	}
}
