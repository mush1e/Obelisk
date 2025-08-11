package handlers

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/mush1e/obelisk/internal/message"
	"github.com/mush1e/obelisk/internal/services"
)

func PostMessageHandler(brokerService *services.BrokerService) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := strings.Trim(r.URL.Path, "/")
		parts := strings.Split(path, "/")
		if len(parts) != 3 || parts[0] != "topics" || parts[2] != "messages" {
			http.NotFound(w, r)
			return
		}
		topic := parts[1]
		var msg message.Message
		if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
			http.Error(w, "Bad Request: invalid JSON", http.StatusBadRequest)
			return
		}
		msg.Topic = topic
		if err := brokerService.PublishMessage(&msg); err != nil {
			http.Error(w, "Bad Request: invalid JSON", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]string{"status": "message received", "topic": topic})

	})
}
