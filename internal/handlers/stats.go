package handlers

import (
	"encoding/json"
	"net/http"

	"github.com/mush1e/obelisk/internal/services"
)

func TopicStats(topicService *services.TopicService) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		topic := r.URL.Query().Get("topic")
		if topic == "" {
			http.Error(w, "topic query param required", http.StatusBadRequest)
			return
		}

		buffered, persisted, err := topicService.GetStats(topic)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"topic":     topic,
			"buffered":  buffered,
			"persisted": persisted,
		})
	})
}
