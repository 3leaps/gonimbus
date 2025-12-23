package server

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/3leaps/gonimbus/internal/observability"
	"github.com/fulmenhq/gofulmen/errors"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

var metricsProxyClient = &http.Client{
	Timeout: 5 * time.Second,
}

// MetricsHandler proxies Prometheus metrics from the internal exporter so callers
// can scrape /metrics on the main HTTP server.
func MetricsHandler(w http.ResponseWriter, r *http.Request) {
	exporter := observability.PrometheusExporter
	if exporter == nil {
		err := errors.NewErrorEnvelope("SERVICE_UNAVAILABLE", "Metrics exporter not initialized")
		HandleError(w, r, err)
		return
	}

	// Get metrics URL using the actual port the exporter is listening on
	metricsPort := observability.GetMetricsPort()
	if metricsPort == 0 {
		// Fallback: try viper config or default port
		metricsPort = viper.GetInt("metrics.port")
		if metricsPort == 0 {
			metricsPort = 9090
		}
	}
	metricsURL := fmt.Sprintf("http://127.0.0.1:%d/metrics", metricsPort)
	req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, metricsURL, nil)
	if err != nil {
		wrappedErr, _ := errors.NewErrorEnvelope("INTERNAL_ERROR", "Unable to construct metrics request").
			WithContext(map[string]interface{}{
				"metrics_url":    metricsURL,
				"original_error": err.Error(),
			})
		HandleError(w, r, wrappedErr)
		return
	}

	// Preserve caller hint for content negotiation
	if accept := r.Header.Get("Accept"); accept != "" {
		req.Header.Set("Accept", accept)
	}

	resp, err := metricsProxyClient.Do(req)
	if err != nil {
		wrappedErr, _ := errors.NewErrorEnvelope("EXTERNAL_SERVICE_ERROR", "Prometheus exporter unavailable").
			WithContext(map[string]interface{}{
				"metrics_url":    metricsURL,
				"original_error": err.Error(),
			})
		HandleError(w, r, wrappedErr)
		return
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			observability.ServerLogger.Warn("Failed to close metrics response body",
				zap.Error(err))
		}
	}()

	for key, values := range resp.Header {
		// Skip hop-by-hop headers; net/http handles them.
		if strings.EqualFold(key, "Connection") || strings.EqualFold(key, "Keep-Alive") ||
			strings.EqualFold(key, "Proxy-Authenticate") || strings.EqualFold(key, "Proxy-Authorization") ||
			strings.EqualFold(key, "TE") || strings.EqualFold(key, "Trailer") ||
			strings.EqualFold(key, "Transfer-Encoding") || strings.EqualFold(key, "Upgrade") {
			continue
		}

		for _, v := range values {
			w.Header().Add(key, v)
		}
	}

	// Ensure we always advertise Prometheus content type
	if resp.Header.Get("Content-Type") == "" {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	}

	w.WriteHeader(resp.StatusCode)
	if _, err := io.Copy(w, resp.Body); err != nil && observability.ServerLogger != nil {
		observability.ServerLogger.Warn("Failed to write metrics response",
			zap.Error(err))
	}
}
