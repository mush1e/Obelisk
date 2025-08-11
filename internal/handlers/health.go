package handlers

// This package provides HTTP handlers for the Obelisk message broker's REST API.
// The health handler specifically provides a simple health check endpoint that
// external monitoring systems can use to verify the server is running and
// responding to requests.
//
// The health check handler implements a basic "ping" style endpoint that:
// - Returns a JSON response indicating server status
// - Uses minimal processing to ensure reliable health reporting
// - Provides consistent response format for monitoring tools
// - Responds quickly to avoid timeout issues in health check systems

import (
	"net/http"
)

// Health creates an HTTP handler for health check endpoints.
// This handler provides a simple way for load balancers, monitoring systems,
// and orchestration platforms to verify that the Obelisk server is operational
// and capable of processing HTTP requests.
//
// The handler always returns a 200 OK status with a JSON response indicating
// the server is healthy. This lightweight operation ensures that health checks
// complete quickly and don't consume significant server resources.
//
// Response format:
//
//	{"status":"ok"}
//
// The handler is designed to be mounted at endpoints like:
//   - /health
//   - /healthz
//   - /ping
//   - /status
//
// Returns:
//   - http.Handler: Configured handler ready to be mounted on an HTTP router
func Health() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Set appropriate content type for JSON response
		w.Header().Set("Content-Type", "application/json")

		// Return simple status indicator - 200 OK with JSON body
		// This confirms the server is running and capable of handling requests
		w.Write([]byte(`{"status":"ok"}`))
	})
}
