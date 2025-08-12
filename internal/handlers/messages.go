package handlers

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/mush1e/obelisk/internal/message"
	"github.com/mush1e/obelisk/internal/services"
)

func PostMessageHandler(brokerService *services.BrokerService) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		topic := r.PathValue("topic")

		if topic == "" || strings.Contains(topic, "/") {
			http.Error(w, "Invalid topic name", http.StatusBadRequest)
			return
		}

		var msg message.Message
		if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
			http.Error(w, "Invalid JSON: "+err.Error(), http.StatusBadRequest)
			return
		}

		msg.Topic = topic

		if msg.Timestamp.IsZero() {
			msg.Timestamp = time.Now()
		}

		if msg.Key == "" {
			http.Error(w, "Missing required field: key", http.StatusBadRequest)
			return
		}
		if msg.Value == "" {
			http.Error(w, "Missing required field: value", http.StatusBadRequest)
			return
		}

		if err := brokerService.PublishMessage(&msg); err != nil {
			http.Error(w, "Failed to store message: "+err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":    "accepted",
			"topic":     topic,
			"timestamp": msg.Timestamp.Format(time.RFC3339),
			"key":       msg.Key,
		})
	})
}
