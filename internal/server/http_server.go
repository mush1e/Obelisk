package server

// This package provides HTTP server components for the Obelisk message broker's REST API.
// The HTTP server exposes management and monitoring endpoints that complement the main
// TCP-based message ingestion interface. It provides a web-based interface for health
// checks, statistics monitoring, and other administrative operations.
//
// The HTTP server handles:
// - Health check endpoints for load balancer and monitoring integration
// - Topic statistics and metrics exposure for operational visibility
// - RESTful API endpoints with proper error handling and response formatting
// - Middleware integration for logging, recovery, and cross-cutting concerns
// - Graceful shutdown coordination with the main server lifecycle

import (
	"context"
	"fmt"
	"net/http"

	"github.com/mush1e/obelisk/internal/handlers"
	"github.com/mush1e/obelisk/internal/services"
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
type HTTPServer struct {
	srv *http.Server // Underlying Go HTTP server instance with configured routes and middleware
}

// NewHTTPServer creates a new HTTP server instance with configured routes and middleware.
// The server is initialized with a complete middleware stack and all necessary endpoints
// for monitoring and administration. It requires a topic service for retrieving broker
// statistics and operational data.
//
// Configured endpoints:
// - /health: Basic health check endpoint for load balancer integration
// - /stats: Topic statistics endpoint for monitoring message counts and buffer status
//
// The server applies a comprehensive middleware stack including:
// - Request/response logging for observability
// - Panic recovery to prevent server crashes
// - Consistent error handling and response formatting
//
// Parameters:
//   - addr: Network address to bind the HTTP server to (e.g., ":8081")
//   - topicService: Service instance for retrieving topic statistics and broker state
//
// Returns:
//   - *HTTPServer: Configured server ready to start accepting HTTP requests
func NewHTTPServer(addr string, brokerService *services.BrokerService) *HTTPServer {
	// Create HTTP router and configure endpoint handlers
	mux := http.NewServeMux()

	// Health check endpoint for load balancer and monitoring system integration
	// This endpoint provides a simple way to verify server availability
	mux.Handle("GET /health", handlers.Health())

	// Statistics endpoint for operational monitoring and capacity planning
	// Requires topic query parameter and returns JSON with buffer/persist counts
	mux.Handle("GET /stats", handlers.TopicStats(brokerService))

	// Topics POST endpoint to send topic based messages
	mux.Handle("POST /topics/{topic}/messages", handlers.PostMessageHandler(brokerService))

	return &HTTPServer{
		srv: &http.Server{
			Addr: addr,
			// Apply complete middleware stack to all routes for consistent behavior
			Handler: handlers.WithMiddlewares(mux),
		},
	}
}

// Start begins accepting HTTP requests on the configured address.
// This method starts the HTTP server in the current goroutine and will block
// until the server is shut down or encounters a fatal error. It should typically
// be called in a separate goroutine to avoid blocking the main application.
//
// The method handles the standard HTTP server lifecycle and logs startup
// information for operational visibility. It distinguishes between expected
// shutdown scenarios and actual server errors to provide appropriate logging.
//
// Expected usage pattern:
//
//	go httpServer.Start() // Start in background goroutine
//	// ... do other work ...
//	httpServer.Stop(ctx)  // Graceful shutdown when needed
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
// This method initiates a graceful shutdown process that allows active requests
// to complete processing before terminating the server. It respects the provided
// context timeout to ensure shutdown completes within reasonable time bounds.
//
// During graceful shutdown:
// 1. Server stops accepting new connections
// 2. Active requests are allowed to complete processing
// 3. Server waits up to the context timeout for completion
// 4. Any remaining connections are forcibly closed after timeout
//
// This method should be called during application shutdown to ensure that
// client requests are handled properly and no data is lost during termination.
//
// Parameters:
//   - ctx: Context with timeout controlling maximum shutdown duration
//
// Returns:
//   - error: Any error that occurred during the shutdown process
func (h *HTTPServer) Stop(ctx context.Context) error {
	// Initiate graceful shutdown with timeout from provided context
	// This allows active requests to complete while preventing indefinite blocking
	return h.srv.Shutdown(ctx)
}
