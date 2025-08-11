package handlers

// This package provides HTTP handlers for the Obelisk message broker's REST API.
// The stats handler specifically provides topic-level statistics and metrics
// that are useful for monitoring message broker health, performance, and capacity
// planning. It exposes buffered and persisted message counts per topic.
//
// The stats handler enables:
// - Real-time visibility into topic message counts and buffer status
// - Monitoring of message processing lag and throughput
// - Capacity planning based on buffer utilization patterns
// - Debugging of batching behavior and flush performance
// - Integration with external monitoring and alerting systems

import (
	"encoding/json"
	"net/http"

	"github.com/mush1e/obelisk/internal/services"
)

// TopicStats creates an HTTP handler for retrieving topic-level statistics.
// This handler provides detailed metrics about message counts and buffer status
// for a specific topic, enabling monitoring and observability of the message
// broker's operational state.
//
// The handler requires a "topic" query parameter and returns JSON containing:
// - topic: The name of the requested topic
// - buffered: Number of messages currently in memory awaiting flush to storage
// - persisted: Number of messages that have been written to persistent storage
//
// Request format:
//
//	GET /stats?topic=my-topic
//
// Response format:
//
//	{
//	  "topic": "my-topic",
//	  "buffered": 42,
//	  "persisted": 1337
//	}
//
// Error responses:
// - 400 Bad Request: If topic query parameter is missing
// - 500 Internal Server Error: If statistics retrieval fails
//
// The buffered count indicates messages waiting to be flushed and helps
// monitor batching behavior and potential memory pressure. The persisted
// count shows the total number of messages durably stored for the topic.
//
// Parameters:
//   - topicService: Service instance for retrieving topic statistics
//
// Returns:
//   - http.Handler: Configured handler ready to be mounted on an HTTP router
func TopicStats(topicService *services.TopicService) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Extract topic name from query parameters
		topic := r.URL.Query().Get("topic")
		if topic == "" {
			// Return error if required topic parameter is missing
			http.Error(w, "topic query param required", http.StatusBadRequest)
			return
		}

		// Retrieve statistics from the topic service
		buffered, persisted, err := topicService.GetStats(topic)
		if err != nil {
			// Return error if statistics retrieval fails
			// This could happen if the topic doesn't exist or storage access fails
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// Set appropriate content type for JSON response
		w.Header().Set("Content-Type", "application/json")

		// Encode and return statistics as JSON response
		// The response includes the topic name for clarity and the two key metrics
		json.NewEncoder(w).Encode(map[string]interface{}{
			"topic":     topic,     // Topic name for response clarity
			"buffered":  buffered,  // Messages in memory buffer
			"persisted": persisted, // Messages written to storage
		})
	})
}
