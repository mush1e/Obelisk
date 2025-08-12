package handlers

// HTTP handlers for the Obelisk message broker's REST API.

import (
	"encoding/json"
	"net/http"

	"github.com/mush1e/obelisk/internal/services"
)

// TopicStats creates an HTTP handler for retrieving topic-level statistics.
func TopicStats(brokerService *services.BrokerService) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Extract topic name from query parameters
		topic := r.URL.Query().Get("topic")
		if topic == "" {
			// Return error if required topic parameter is missing
			http.Error(w, "topic query param required", http.StatusBadRequest)
			return
		}

		// Retrieve statistics from the topic service
		buffered, persisted, err := brokerService.GetTopicStats(topic)
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
