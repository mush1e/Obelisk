package handlers

// HTTP handlers for the Obelisk message broker's REST API.
// Enhanced health handlers provide comprehensive health checking for monitoring systems.

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/mush1e/obelisk/internal/metrics"
	"github.com/mush1e/obelisk/internal/services"
)

// HealthResponse represents the complete health status of the system
type HealthResponse struct {
	Status     string               `json:"status"` // overall: healthy, degraded, unhealthy
	Timestamp  time.Time            `json:"timestamp"`
	Uptime     string               `json:"uptime"`
	Components map[string]Component `json:"components"`
	Summary    HealthSummary        `json:"summary"`
}

// Component represents the health status of a specific system component
type Component struct {
	Status    string         `json:"status"`
	Details   map[string]any `json:"details,omitempty"`
	LastCheck time.Time      `json:"last_check"`
}

// HealthSummary provides a count of component health statuses
type HealthSummary struct {
	TotalComponents int `json:"total_components"`
	Healthy         int `json:"healthy"`
	Degraded        int `json:"degraded"`
	Unhealthy       int `json:"unhealthy"`
}

// EnhancedHealth creates an HTTP handler for comprehensive health check endpoints
func EnhancedHealth(brokerService *services.BrokerService) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		// Check all components
		components := map[string]Component{
			"buffer":     checkBufferHealth(brokerService),
			"batcher":    checkBatcherHealth(brokerService),
			"storage":    checkStorageHealth(brokerService),
			"tcp_server": checkTCPServerHealth(brokerService),
		}

		// Determine overall status
		overallStatus := determineOverallStatus(components)

		// Calculate summary
		summary := calculateSummary(components)

		// Record metrics
		recordHealthMetrics(overallStatus, components)

		response := HealthResponse{
			Status:     overallStatus,
			Timestamp:  time.Now(),
			Uptime:     brokerService.GetUptime(),
			Components: components,
			Summary:    summary,
		}

		// Set appropriate HTTP status code
		w.Header().Set("Content-Type", "application/json")
		switch overallStatus {
		case "unhealthy":
			w.WriteHeader(http.StatusServiceUnavailable)
		case "degraded":
			w.WriteHeader(http.StatusOK) // Still operational
		default:
			w.WriteHeader(http.StatusOK)
		}

		// Add health check duration header
		duration := time.Since(start)
		w.Header().Set("X-Health-Check-Duration", duration.String())

		// Record health check duration metric
		metrics.Metrics.HealthCheckDuration.WithLabelValues("health").Observe(duration.Seconds())

		json.NewEncoder(w).Encode(response)
	})
}

// ReadinessCheck creates an HTTP handler for Kubernetes readiness probes
func ReadinessCheck(brokerService *services.BrokerService) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		// Readiness means the service is ready to accept traffic
		// Check if system is initialized and basic components are healthy
		if !brokerService.IsInitialized() {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]string{
				"status": "not_ready",
				"reason": "system_not_initialized",
			})

			// Record metrics
			metrics.Metrics.HealthCheckDuration.WithLabelValues("ready").Observe(time.Since(start).Seconds())
			return
		}

		// Check if basic components are operational
		bufferRate, bufferHealthy := brokerService.GetBufferHealth()
		batcherRate, batcherHealthy := brokerService.GetBatcherHealth()

		if !bufferHealthy || !batcherHealthy {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"status": "not_ready",
				"reason": "core_components_degraded",
				"details": map[string]any{
					"buffer_success_rate":  bufferRate,
					"batcher_success_rate": batcherRate,
				},
			})

			// Record metrics
			metrics.Metrics.HealthCheckDuration.WithLabelValues("ready").Observe(time.Since(start).Seconds())
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{
			"status": "ready",
		})

		// Record metrics
		metrics.Metrics.HealthCheckDuration.WithLabelValues("ready").Observe(time.Since(start).Seconds())
	})
}

// LivenessCheck creates an HTTP handler for Kubernetes liveness probes
func LivenessCheck(brokerService *services.BrokerService) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		// Liveness means the service is alive and responding
		// This should be very lightweight and fast
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{
			"status":    "alive",
			"timestamp": time.Now().Format(time.RFC3339),
		})

		// Record metrics
		metrics.Metrics.HealthCheckDuration.WithLabelValues("live").Observe(time.Since(start).Seconds())
	})
}

// checkBufferHealth checks the health of the message buffer system
func checkBufferHealth(brokerService *services.BrokerService) Component {
	rate, healthy := brokerService.GetBufferHealth()
	status := "healthy"
	if !healthy {
		status = "degraded"
	}

	return Component{
		Status: status,
		Details: map[string]any{
			"success_rate":    rate,
			"threshold":       0.95,
			"operation_count": brokerService.GetBufferOperationCount(),
		},
		LastCheck: time.Now(),
	}
}

// checkBatcherHealth checks the health of the message batching system
func checkBatcherHealth(brokerService *services.BrokerService) Component {
	rate, healthy := brokerService.GetBatcherHealth()
	lastFlushStr := brokerService.GetLastFlushTime()

	status := "healthy"
	if !healthy {
		status = "degraded"
	}

	// Parse the last flush time string
	lastFlush, err := time.Parse("2006-01-02T15:04:05Z07:00", lastFlushStr)
	if err != nil {
		// If we can't parse the time, assume it's very old
		lastFlush = time.Time{}
	}

	// Check if batcher has flushed recently
	if time.Since(lastFlush) > 30*time.Second {
		status = "unhealthy"
	}

	return Component{
		Status: status,
		Details: map[string]any{
			"success_rate":    rate,
			"last_flush":      lastFlushStr,
			"threshold":       0.95,
			"operation_count": brokerService.GetBatcherOperationCount(),
		},
		LastCheck: time.Now(),
	}
}

// checkStorageHealth checks the health of the storage system
func checkStorageHealth(brokerService *services.BrokerService) Component {
	// Check if we can access the storage system by trying to get stats for a non-existent topic
	// This will fail with a "topic not found" error, but that's expected and shows the system is accessible
	_, _, err := brokerService.GetTopicStats("health-check-topic")

	status := "healthy"
	details := map[string]any{
		"accessible": true,
		"error_type": "topic_not_found", // This is expected for health checks
	}

	// If we get an error other than "topic not found", that indicates a real problem
	if err != nil && !strings.Contains(err.Error(), "topic not found") {
		status = "degraded"
		details["accessible"] = false
		details["error"] = err.Error()
	}

	return Component{
		Status:    status,
		Details:   details,
		LastCheck: time.Now(),
	}
}

// checkTCPServerHealth checks the health of the TCP server
func checkTCPServerHealth(brokerService *services.BrokerService) Component {
	// For now, assume TCP server is healthy if we can get basic info
	// In a real implementation, you'd check listener status, connection counts, etc.
	status := "healthy"
	details := map[string]any{
		"listening":          true,
		"connections_active": 0, // You'd get this from your TCP server
	}

	return Component{
		Status:    status,
		Details:   details,
		LastCheck: time.Now(),
	}
}

// determineOverallStatus determines the overall health status based on component health
func determineOverallStatus(components map[string]Component) string {
	unhealthyCount := 0
	degradedCount := 0

	for _, component := range components {
		switch component.Status {
		case "unhealthy":
			unhealthyCount++
		case "degraded":
			degradedCount++
		}
	}

	if unhealthyCount > 0 {
		return "unhealthy"
	}

	if degradedCount > 0 {
		return "degraded"
	}

	return "healthy"
}

// calculateSummary calculates the summary of component health statuses
func calculateSummary(components map[string]Component) HealthSummary {
	summary := HealthSummary{
		TotalComponents: len(components),
	}

	for _, component := range components {
		switch component.Status {
		case "healthy":
			summary.Healthy++
		case "degraded":
			summary.Degraded++
		case "unhealthy":
			summary.Unhealthy++
		}
	}

	return summary
}

// recordHealthMetrics records health-related metrics to Prometheus
func recordHealthMetrics(overallStatus string, components map[string]Component) {
	// Record overall health status
	statusValue := 0.0
	switch overallStatus {
	case "healthy":
		statusValue = 2.0
	case "degraded":
		statusValue = 1.0
	case "unhealthy":
		statusValue = 0.0
	}
	metrics.Metrics.HealthStatus.WithLabelValues("overall").Set(statusValue)

	// Record individual component health
	for name, component := range components {
		componentValue := 0.0
		switch component.Status {
		case "healthy":
			componentValue = 2.0
		case "degraded":
			componentValue = 1.0
		case "unhealthy":
			componentValue = 0.0
		}
		metrics.Metrics.ComponentHealth.WithLabelValues(name, component.Status).Set(componentValue)
	}
}
