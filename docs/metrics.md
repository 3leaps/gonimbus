# Metrics Reference

Gonimbus exposes comprehensive metrics for monitoring application performance, health, and business operations. All metrics follow Prometheus conventions and are available via the `/metrics` endpoint.

## Configuration

Metrics can be configured via environment variables:

```bash
# Enable/disable metrics collection
GONIMBUS_METRICS_ENABLED=true

# Metrics server port (separate from main HTTP server)
GONIMBUS_METRICS_PORT=9090

# Metrics endpoint path on main server
GONIMBUS_METRICS_PATH=/metrics
```

## HTTP Metrics

### `http_requests_total`

**Type:** Counter  
**Description:** Total number of HTTP requests processed  
**Labels:**

- `method` - HTTP method (GET, POST, PUT, DELETE, etc.)
- `endpoint` - Route pattern (e.g., "/health/\*", "/version", "/unknown")
- `status` - HTTP status code (200, 404, 500, etc.)

**Example Queries:**

```promql
# Total requests per second
rate(http_requests_total[5m])

# Requests by method
sum(rate(http_requests_total[5m])) by (method)

# Requests by endpoint
sum(rate(http_requests_total[5m])) by (endpoint)

# Error rate (4xx and 5xx responses)
sum(rate(http_requests_total[5m])) by (status)
```

### `http_request_duration_ms`

**Type:** Histogram  
**Description:** HTTP request duration in milliseconds  
**Labels:**

- `method` - HTTP method
- `endpoint` - Route pattern
- `status` - HTTP status code

**Example Queries:**

```promql
# 95th percentile request duration
histogram_quantile(0.95, rate(http_request_duration_ms_bucket[5m]))

# Average request duration by endpoint
rate(http_request_duration_ms_sum[5m]) / rate(http_request_duration_ms_count[5m])

# Request duration trends
histogram_quantile(0.50, rate(http_request_duration_ms_bucket[5m]))
```

### `http_request_size_bytes`

**Type:** Gauge  
**Description:** HTTP request body size in bytes  
**Labels:**

- `method` - HTTP method
- `endpoint` - Route pattern

**Example Queries:**

```promql
# Average request size by method
avg(http_request_size_bytes) by (method)

# Request size distribution
histogram_quantile(0.95, http_request_size_bytes)
```

### `http_response_size_bytes`

**Type:** Gauge  
**Description:** HTTP response body size in bytes  
**Labels:**

- `method` - HTTP method
- `endpoint` - Route pattern

**Example Queries:**

```promql
# Average response size by endpoint
avg(http_response_size_bytes) by (endpoint)

# Total bytes transferred
sum(rate(http_response_size_bytes[5m])) by (endpoint)
```

### `http_errors_total`

**Type:** Counter  
**Description:** Total HTTP errors (4xx and 5xx responses)  
**Labels:**

- `method` - HTTP method
- `endpoint` - Route pattern
- `status` - HTTP status code
- `error_type` - Error classification ("client_error" or "server_error")

**Example Queries:**

```promql
# Error rate by type
sum(rate(http_errors_total[5m])) by (error_type)

# Error rate by endpoint
sum(rate(http_errors_total[5m])) by (endpoint)

# 5xx error rate (server errors)
sum(rate(http_errors_total{error_type="server_error"}[5m]))
```

## Application Metrics

### `app_operations_total`

**Type:** Counter  
**Description:** Total application operations performed  
**Labels:**

- `operation` - Operation name (e.g., "health_check", "config_reload")
- `status` - Operation status ("success" or "failure")

**Example Queries:**

```promql
# Operations per second by type
sum(rate(app_operations_total[5m])) by (operation)

# Operation success rate
sum(rate(app_operations_total{status="success"}[5m])) / sum(rate(app_operations_total[5m]))

# Failed operations
sum(rate(app_operations_total{status="failure"}[5m])) by (operation)
```

### `app_operations_errors_total`

**Type:** Counter  
**Description:** Total application operation errors  
**Labels:**

- `operation` - Operation name
- `error_type` - Specific error type

**Example Queries:**

```promql
# Error rate by type
sum(rate(app_operations_errors_total[5m])) by (error_type)

# Most common errors
topk(10, sum(rate(app_operations_errors_total[5m])) by (error_type))
```

### `app_active_connections`

**Type:** Gauge  
**Description:** Current number of active connections  
**Labels:** None

**Example Queries:**

```promql
# Current active connections
app_active_connections

# Connection usage over time
rate(app_active_connections[5m])
```

### `app_health_check_total`

**Type:** Counter  
**Description:** Total health check executions  
**Labels:**

- `check` - Health check name (e.g., "telemetry", "identity")
- `status` - Check result ("healthy" or "unhealthy")

**Example Queries:**

```promql
# Health check success rate
sum(rate(app_health_check_total{status="healthy"}[5m])) / sum(rate(app_health_check_total[5m]))

# Failed health checks by type
sum(rate(app_health_check_total{status="unhealthy"}[5m])) by (check)
```

### `app_health_check_duration_ms`

**Type:** Histogram  
**Description:** Health check execution duration in milliseconds  
**Labels:**

- `check` - Health check name

**Example Queries:**

```promql
# 95th percentile health check duration
histogram_quantile(0.95, rate(app_health_check_duration_ms_bucket[5m]))

# Slowest health checks
topk(5, histogram_quantile(0.95, rate(app_health_check_duration_ms_bucket[5m])) by (check))
```

### `app_server_start_time_seconds`

**Type:** Gauge  
**Description:** Server start time as Unix timestamp  
**Labels:** None

**Example Queries:**

```promql
# Server uptime (current time - start time)
time() - app_server_start_time_seconds

# Server restarts (changes in start time)
changes(app_server_start_time_seconds[1h])
```

### `app_server_uptime_seconds`

**Type:** Gauge  
**Description:** Server uptime in seconds  
**Labels:** None

**Example Queries:**

```promql
# Current uptime
app_server_uptime_seconds

# Uptime trends
rate(app_server_uptime_seconds[5m])
```

## Go Runtime Metrics

Gonimbus automatically exposes Go runtime metrics provided by the Prometheus client library:

### `go_goroutines`

**Type:** Gauge  
**Description:** Current number of goroutines

### `go_threads`

**Type:** Gauge  
**Description:** Current number of OS threads

### `go_memstats_alloc_bytes`

**Type:** Gauge  
**Description:** Current heap allocation in bytes

### `go_memstats_heap_alloc_bytes`

**Type:** Gauge  
**Description:** Current heap allocation in bytes

### `go_memstats_heap_sys_bytes`

**Type:** Gauge  
**Description:** Heap system memory in bytes

**Example Queries:**

```promql
# Goroutine count trends
rate(go_goroutines[5m])

# Memory usage
go_memstats_alloc_bytes / (1024 * 1024)  # MB

# GC pressure
rate(go_memstats_gc_cpu_fraction[5m])
```

## Dashboard Examples

### Main Application Dashboard

```promql
# Request Rate
sum(rate(http_requests_total[5m])) by (endpoint)

# Error Rate
sum(rate(http_errors_total[5m])) by (error_type)

# Response Time
histogram_quantile(0.95, rate(http_request_duration_ms_bucket[5m])) by (endpoint)

# Active Connections
app_active_connections

# Health Status
sum(rate(app_health_check_total{status="healthy"}[5m])) / sum(rate(app_health_check_total[5m]))
```

### Infrastructure Dashboard

```promql
# Memory Usage
go_memstats_alloc_bytes / (1024 * 1024)

# Goroutine Count
go_goroutines

# GC Activity
rate(go_memstats_gc_cpu_fraction[5m])

# Server Uptime
app_server_uptime_seconds
```

## Alerting Examples

### High Error Rate

```promql
# Alert when error rate exceeds 5%
sum(rate(http_errors_total[5m])) / sum(rate(http_requests_total[5m])) > 0.05
```

### High Response Time

```promql
# Alert when 95th percentile response time exceeds 500ms
histogram_quantile(0.95, rate(http_request_duration_ms_bucket[5m])) > 500
```

### Health Check Failures

```promql
# Alert when health checks are failing
sum(rate(app_health_check_total{status="unhealthy"}[5m])) > 0
```

### Memory Usage

```promql
# Alert when memory usage exceeds 100MB
go_memstats_alloc_bytes > (100 * 1024 * 1024)
```

## Best Practices

1. **Use rate() for counters:** Always wrap counters with `rate()` to get per-second values
2. **Use histogram_quantile() for latency:** Use percentiles rather than averages for response times
3. **Label cardinality:** Keep label values low-cardinality to avoid performance issues
4. **Recording rules:** Use recording rules for complex queries to improve dashboard performance
5. **Alert thresholds:** Set alert thresholds based on baseline metrics, not arbitrary values

## Troubleshooting

### Missing Metrics

If metrics are missing:

1. Check `GONIMBUS_METRICS_ENABLED=true`
2. Verify metrics port is accessible: `curl http://localhost:9090/metrics`
3. Check application logs for telemetry initialization errors
4. Ensure middleware stack is correctly ordered

### High Cardinality Warnings

If you see high cardinality warnings:

1. Check for dynamic values in labels (user IDs, request IDs)
2. Use endpoint patterns instead of raw paths
3. Limit label value sets to known categories

### Performance Issues

If metrics impact performance:

1. Reduce histogram bucket count
2. Disable unused metrics
3. Use separate metrics server port
4. Consider metric sampling for high-volume endpoints
