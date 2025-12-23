package metrics

import (
	"strconv"

	"github.com/3leaps/gonimbus/internal/observability"
)

// Metric names
const (
	ErrorsTotalName      = "errors_total"
	PanicsTotalName      = "panics_total"
	ErrorsByEndpointName = "errors_by_endpoint"
)

// RecordError records an error with code and status
func RecordError(errorCode string, httpStatus int) {
	if observability.TelemetrySystem != nil {
		_ = observability.TelemetrySystem.Counter(
			"errors_total",
			1,
			map[string]string{
				"error_code":  errorCode,
				"http_status": strconv.Itoa(httpStatus),
			},
		)
	}
}

// RecordPanic records a panic recovery
func RecordPanic() {
	if observability.TelemetrySystem != nil {
		_ = observability.TelemetrySystem.Counter(
			"panics_total",
			1,
			nil,
		)
	}
}

// RecordErrorByEndpoint records an error by endpoint
func RecordErrorByEndpoint(endpoint string, errorCode string) {
	if observability.TelemetrySystem != nil {
		_ = observability.TelemetrySystem.Counter(
			"errors_by_endpoint",
			1,
			map[string]string{
				"endpoint":   endpoint,
				"error_code": errorCode,
			},
		)
	}
}
