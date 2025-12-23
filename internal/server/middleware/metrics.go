package middleware

import (
	"net/http"
	"strconv"
	"time"

	"github.com/3leaps/gonimbus/internal/observability"
	"github.com/go-chi/chi/v5"
	"go.uber.org/zap"
)

// responseWriter wraps http.ResponseWriter to capture status code and response size
type responseWriter struct {
	http.ResponseWriter
	statusCode   int
	bytesWritten int64
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

func (rw *responseWriter) Write(b []byte) (int, error) {
	n, err := rw.ResponseWriter.Write(b)
	rw.bytesWritten += int64(n)
	return n, err
}

// getEndpointPattern extracts chi route pattern to avoid high-cardinality paths
func getEndpointPattern(r *http.Request) string {
	// Try to get chi route pattern
	routePattern := chi.RouteContext(r.Context()).RoutePattern()
	if routePattern != "" {
		return routePattern
	}

	// Fallback to path-based categorization for non-chi routes
	path := r.URL.Path
	switch path {
	case "/health", "/health/live", "/health/ready", "/health/startup":
		return "/health/*"
	case "/version":
		return "/version"
	case "/metrics":
		return "/metrics"
	case "/":
		return "/"
	default:
		// For unknown paths, use a generic pattern to avoid cardinality issues
		return "/unknown"
	}
}

// RequestMetrics middleware captures HTTP request metrics following Prometheus standards
func RequestMetrics(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if observability.TelemetrySystem == nil {
			next.ServeHTTP(w, r)
			return
		}

		start := time.Now()
		wrapped := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}

		// Get request size from Content-Length header
		requestSize := int64(0)
		if contentLength := r.Header.Get("Content-Length"); contentLength != "" {
			if size, err := strconv.ParseInt(contentLength, 10, 64); err == nil {
				requestSize = size
			}
		}

		next.ServeHTTP(wrapped, r)

		duration := time.Since(start)
		endpoint := getEndpointPattern(r)

		// Common labels for all metrics (avoid high cardinality)
		commonLabels := map[string]string{
			"method":   r.Method,
			"endpoint": endpoint,
			"status":   strconv.Itoa(wrapped.statusCode),
		}

		// Emit request counter
		_ = observability.TelemetrySystem.Counter(
			"http_requests_total",
			1,
			commonLabels,
		)

		// Emit duration histogram in milliseconds (keep gofulmen standard)
		_ = observability.TelemetrySystem.Histogram(
			"http_request_duration_ms",
			duration,
			commonLabels,
		)

		// Emit request size as gauge (not histogram since it's a single value)
		_ = observability.TelemetrySystem.Gauge(
			"http_request_size_bytes",
			float64(requestSize),
			map[string]string{
				"method":   r.Method,
				"endpoint": endpoint,
			},
		)

		// Emit response size as gauge (not histogram since it's a single value)
		_ = observability.TelemetrySystem.Gauge(
			"http_response_size_bytes",
			float64(wrapped.bytesWritten),
			map[string]string{
				"method":   r.Method,
				"endpoint": endpoint,
			},
		)

		// Emit error counter for non-2xx responses
		if wrapped.statusCode >= 400 {
			errorType := "client_error" // 4xx
			if wrapped.statusCode >= 500 {
				errorType = "server_error" // 5xx
			}

			_ = observability.TelemetrySystem.Counter(
				"http_errors_total",
				1,
				map[string]string{
					"method":     r.Method,
					"endpoint":   endpoint,
					"status":     strconv.Itoa(wrapped.statusCode),
					"error_type": errorType,
				},
			)
		}

		// Log request with request ID for tracing (request ID stays in logs, not metrics)
		requestID := GetRequestID(r.Context())
		if observability.ServerLogger != nil {
			observability.ServerLogger.Info("HTTP request completed",
				zap.String("method", r.Method),
				zap.String("path", r.URL.Path),
				zap.String("endpoint", endpoint),
				zap.Int("status", wrapped.statusCode),
				zap.Duration("duration", duration),
				zap.Int64("request_size", requestSize),
				zap.Int64("response_size", wrapped.bytesWritten),
				zap.String("requestID", requestID),
			)
		}
	})
}
