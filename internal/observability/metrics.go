package observability

import (
	"fmt"
	"net"
	"strconv"

	"github.com/fulmenhq/gofulmen/telemetry"
	"github.com/fulmenhq/gofulmen/telemetry/exporters"
)

var (
	// TelemetrySystem is the global telemetry system
	TelemetrySystem *telemetry.System

	// PrometheusExporter is the prometheus metrics exporter
	PrometheusExporter *exporters.PrometheusExporter

	// metricsPort stores the port the Prometheus exporter is listening on
	metricsPort int
)

// InitMetrics initializes the telemetry system with Prometheus exporter.
// The exporter listens on the provided port (use 0 for random assignment).
// Optional namespace parameter for telemetry integration.
func InitMetrics(serviceName string, port int, namespace ...string) error {
	requestedPort := port
	if requestedPort < 0 {
		requestedPort = 0
	}
	// Store requested port as default in case discovery fails
	metricsPort = requestedPort

	// Use namespace if provided, otherwise use service name
	metricNamespace := serviceName
	if len(namespace) > 0 && namespace[0] != "" {
		metricNamespace = namespace[0]
	}

	endpoint := fmt.Sprintf(":%d", requestedPort)

	// Create Prometheus exporter with namespace
	PrometheusExporter = exporters.NewPrometheusExporter(metricNamespace, endpoint)

	// Start Prometheus HTTP server
	if err := PrometheusExporter.Start(); err != nil {
		return err
	}

	// Update metricsPort with the actual port the exporter bound to
	if actualPort, err := resolvePort(PrometheusExporter.GetAddr()); err == nil {
		metricsPort = actualPort
	} else if requestedPort == 0 {
		// Fall back to default port if we requested :0 and could not determine actual port
		metricsPort = 9090
	}

	// Create telemetry system with Prometheus exporter
	config := &telemetry.Config{
		Enabled: true,
		Emitter: PrometheusExporter,
	}

	sys, err := telemetry.NewSystem(config)
	if err != nil {
		return err
	}

	TelemetrySystem = sys

	// Note: Error metrics (errors_total, panics_total, errors_by_endpoint)
	// are auto-registered by gofulmen telemetry on first use.
	// See internal/metrics/errors.go for metric emission.

	return nil
}

// GetMetricsPort returns the port the Prometheus exporter is listening on
func GetMetricsPort() int {
	return metricsPort
}

func resolvePort(addr string) (int, error) {
	_, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return 0, err
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return 0, err
	}
	return port, nil
}
