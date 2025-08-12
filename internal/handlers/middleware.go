package handlers

// HTTP middleware components with logging, panic recovery, and composition utilities.
// - Performance monitoring through request duration tracking

import (
	"log"
	"net/http"
	"time"
)

// WithMiddlewares applies the complete middleware stack to an HTTP handler.
func WithMiddlewares(h http.Handler) http.Handler {
	return loggingMiddleware(recoverMiddleware(h))
}

// loggingMiddleware provides request/response logging with performance timing.
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
