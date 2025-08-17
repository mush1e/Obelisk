package server

// HTTP server components for REST API endpoints, health checks, and monitoring.
// - Middleware integration for logging, recovery, and cross-cutting concerns
// - Graceful shutdown coordination with the main server lifecycle

import (
	"context"
	"fmt"
	"net/http"

	"github.com/mush1e/obelisk/internal/handlers"
	"github.com/mush1e/obelisk/internal/services"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// HTTPServer provides a REST API interface for the Obelisk message broker.
// This server runs alongside the main TCP message ingestion server to provide
// administrative and monitoring capabilities through standard HTTP endpoints.
// It's designed to be lightweight and focused on operational concerns rather
// than high-throughput message processing.
//
// The server integrates with the broker's service layer to provide:
// - Real-time topic statistics and health information
// - Standardized REST API responses with proper HTTP status codes
// - Middleware-based request processing for logging and error handling
// - Graceful shutdown capabilities for clean service termination
// - Prometheus metrics endpoint for monitoring
type HTTPServer struct {
	srv *http.Server // Underlying Go HTTP server instance with configured routes and middleware
}

// NewHTTPServer creates a new HTTP server instance with configured routes and middleware.
func NewHTTPServer(addr string, brokerService *services.BrokerService) *HTTPServer {
	// Create HTTP router and configure endpoint handlers
	mux := http.NewServeMux()

	// Enhanced health check endpoints for comprehensive monitoring
	// Main health endpoint with detailed component status
	mux.Handle("GET /health", handlers.EnhancedHealth(brokerService))

	// Kubernetes readiness probe - checks if service is ready to accept traffic
	mux.Handle("GET /health/ready", handlers.ReadinessCheck(brokerService))

	// Kubernetes liveness probe - checks if service is alive and responding
	mux.Handle("GET /health/live", handlers.LivenessCheck(brokerService))

	// Statistics endpoint for operational monitoring and capacity planning
	// Requires topic query parameter and returns JSON with buffer/persist counts
	mux.Handle("GET /stats", handlers.TopicStats(brokerService))

	// Topics POST endpoint to send topic based messages
	mux.Handle("POST /topics/{topic}/messages", handlers.PostMessageHandler(brokerService))

	// 📊 Prometheus metrics endpoint
	mux.Handle("GET /metrics", promhttp.Handler())

	return &HTTPServer{
		srv: &http.Server{
			Addr: addr,
			// Apply complete middleware stack to all routes for consistent behavior
			Handler: handlers.WithMiddlewares(mux),
		},
	}
}

// Start begins accepting HTTP requests on the configured address.
func (h *HTTPServer) Start() {
	// Log server startup for operational visibility
	fmt.Println("HTTP server started on", h.srv.Addr)

	// Start accepting requests - this blocks until shutdown or error
	if err := h.srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		// Log unexpected errors, but ignore expected shutdown signal
		fmt.Println("HTTP server error:", err)
	}
}

// Stop gracefully shuts down the HTTP server with a timeout context.
func (h *HTTPServer) Stop(ctx context.Context) error {
	// Initiate graceful shutdown with timeout from provided context
	// This allows active requests to complete while preventing indefinite blocking
	return h.srv.Shutdown(ctx)
}
