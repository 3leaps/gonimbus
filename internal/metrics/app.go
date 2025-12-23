package metrics

import (
	"time"

	"github.com/3leaps/gonimbus/internal/observability"
)

// Application-level metrics following Prometheus conventions
var (
	// Operations metrics
	OperationsTotal       = "app_operations_total"
	OperationsErrorsTotal = "app_operations_errors_total"

	// Connection metrics
	ActiveConnections = "app_active_connections"

	// Health check metrics
	HealthCheckTotal    = "app_health_check_total"
	HealthCheckDuration = "app_health_check_duration_ms"

	// Server lifecycle metrics
	ServerStartTime = "app_server_start_time_seconds"
	ServerUptime    = "app_server_uptime_seconds"
)

// RecordOperation records an application operation with status
func RecordOperation(operation string, success bool) {
	status := "success"
	if !success {
		status = "failure"
	}

	if observability.TelemetrySystem != nil {
		_ = observability.TelemetrySystem.Counter(
			OperationsTotal,
			1,
			map[string]string{
				"operation": operation,
				"status":    status,
			},
		)
	}
}

// RecordOperationError records an application operation error
func RecordOperationError(operation string, errorType string) {
	if observability.TelemetrySystem != nil {
		_ = observability.TelemetrySystem.Counter(
			OperationsErrorsTotal,
			1,
			map[string]string{
				"operation":  operation,
				"error_type": errorType,
			},
		)
	}
}

// SetActiveConnections sets the current number of active connections
func SetActiveConnections(count int64) {
	if observability.TelemetrySystem != nil {
		_ = observability.TelemetrySystem.Gauge(
			ActiveConnections,
			float64(count),
			nil,
		)
	}
}

// RecordHealthCheck records a health check execution
func RecordHealthCheck(checkName string, healthy bool, duration time.Duration) {
	status := "healthy"
	if !healthy {
		status = "unhealthy"
	}

	if observability.TelemetrySystem != nil {
		_ = observability.TelemetrySystem.Counter(
			HealthCheckTotal,
			1,
			map[string]string{
				"check":  checkName,
				"status": status,
			},
		)

		_ = observability.TelemetrySystem.Histogram(
			HealthCheckDuration,
			duration,
			map[string]string{
				"check": checkName,
			},
		)
	}
}

// SetServerStartTime records the server start time (Unix timestamp)
func SetServerStartTime(timestamp int64) {
	if observability.TelemetrySystem != nil {
		_ = observability.TelemetrySystem.Gauge(
			ServerStartTime,
			float64(timestamp),
			nil,
		)
	}
}

// SetServerUptime records the server uptime in seconds
func SetServerUptime(seconds int64) {
	if observability.TelemetrySystem != nil {
		_ = observability.TelemetrySystem.Gauge(
			ServerUptime,
			float64(seconds),
			nil,
		)
	}
}
