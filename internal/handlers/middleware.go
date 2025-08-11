package handlers

// This package provides HTTP middleware components for the Obelisk message broker's REST API.
// The middleware system implements a composable architecture that wraps HTTP handlers with
// cross-cutting concerns like logging, error recovery, and request processing utilities.
//
// The middleware stack handles:
// - Request/response logging with timing information for observability
// - Panic recovery to prevent server crashes from unhandled errors
// - Middleware composition for building processing pipelines
// - Consistent error handling and response formatting
// - Performance monitoring through request duration tracking

import (
	"log"
	"net/http"
	"time"
)

// WithMiddlewares applies the complete middleware stack to an HTTP handler.
// This function composes all available middleware in the correct order to ensure
// proper request processing flow. The middleware stack is applied in reverse order
// of execution, with the outermost middleware executing first.
//
// Middleware execution order:
// 1. loggingMiddleware - Records request details and timing
// 2. recoverMiddleware - Catches panics and prevents crashes
// 3. Provided handler - The actual business logic handler
//
// This composition pattern ensures that logging captures the complete request
// lifecycle, including any recovery operations, while panic recovery protects
// the server from unhandled errors in business logic.
//
// Parameters:
//   - h: The base HTTP handler to wrap with middleware
//
// Returns:
//   - http.Handler: Handler wrapped with the complete middleware stack
func WithMiddlewares(h http.Handler) http.Handler {
	return loggingMiddleware(recoverMiddleware(h))
}

// loggingMiddleware provides request/response logging with performance timing.
// This middleware captures essential request information and measures processing
// duration to support observability and performance monitoring requirements.
//
// The middleware logs:
// - HTTP method (GET, POST, PUT, etc.)
// - Request path and any query parameters
// - Total processing time from request start to response completion
//
// Timing measurement begins immediately when the request enters the middleware
// and concludes after the downstream handler completes processing. This provides
// accurate end-to-end request duration metrics.
//
// Log format: "METHOD /path duration"
// Example: "GET /health 1.234ms"
//
// The middleware uses a pass-through approach, allowing the request to proceed
// normally while capturing telemetry data. This ensures minimal performance
// impact while providing valuable operational insights.
//
// Parameters:
//   - next: The next handler in the middleware chain
//
// Returns:
//   - http.Handler: Handler that logs requests and delegates to next handler
func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Capture start time for duration calculation
		start := time.Now()

		// Execute the next handler in the chain
		next.ServeHTTP(w, r)

		// Log request details with processing duration
		log.Printf("%s %s %s", r.Method, r.URL.Path, time.Since(start))
	})
}

// recoverMiddleware provides panic recovery to prevent server crashes.
// This middleware acts as a safety net that catches unhandled panics in downstream
// handlers and converts them into proper HTTP error responses. Without this
// protection, panics would crash the entire server process.
//
// Recovery behavior:
// 1. Catches any panic that occurs in downstream handlers
// 2. Logs the panic details for debugging and monitoring
// 3. Returns a 500 Internal Server Error response to the client
// 4. Prevents the panic from propagating and crashing the server
//
// The middleware uses Go's built-in panic/recover mechanism with a deferred
// function that executes even when panics occur. This ensures reliable error
// handling regardless of where the panic originates in the request processing
// pipeline.
//
// Error response format:
// - HTTP Status: 500 Internal Server Error
// - Response body: "Internal Server Error"
// - Content-Type: text/plain (default from http.Error)
//
// This middleware should be placed close to the business logic handlers in the
// middleware stack to catch panics from application code while still allowing
// outer middleware (like logging) to capture the recovery event.
//
// Parameters:
//   - next: The next handler in the middleware chain
//
// Returns:
//   - http.Handler: Handler that recovers from panics and delegates to next handler
func recoverMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Set up panic recovery that executes even if downstream handler panics
		defer func() {
			if rec := recover(); rec != nil {
				// Log panic details for debugging and monitoring
				log.Printf("Recovered from panic: %v", rec)

				// Return proper HTTP error response to client
				http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			}
		}()

		// Execute the next handler in the chain (may panic)
		next.ServeHTTP(w, r)
	})
}
