package handlers

// HTTP handlers for the Obelisk message broker's REST API.
// Health handler provides a simple health check endpoint for monitoring.
// - Responds quickly to avoid timeout issues in health check systems

import (
	"net/http"
)

// Health creates an HTTP handler for health check endpoints.
func Health() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Set appropriate content type for JSON response
		w.Header().Set("Content-Type", "application/json")

		// Return simple status indicator - 200 OK with JSON body
		// This confirms the server is running and capable of handling requests
		w.Write([]byte(`{"status":"ok"}`))
	})
}
