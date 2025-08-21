package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/mush1e/obelisk/internal/consumer"
	"github.com/mush1e/obelisk/internal/services"
)

// ConsumerGroupRequest represents the request body for creating a consumer group
type ConsumerGroupRequest struct {
	GroupID string   `json:"group_id"`
	Topics  []string `json:"topics"`
}

// ConsumerGroupResponse represents a consumer group in API responses
type ConsumerGroupResponse struct {
	ID            string                    `json:"id"`
	Topics        []string                  `json:"topics"`
	Members       map[string]MemberResponse `json:"members"`
	Assignments   map[string][]int          `json:"assignments"`
	Generation    int64                     `json:"generation"`
	LastRebalance string                    `json:"last_rebalance"`
}

// MemberResponse represents a group member in API responses
type MemberResponse struct {
	ID         string `json:"id"`
	JoinedAt   string `json:"joined_at"`
	Partitions []int  `json:"partitions"`
}

// JoinGroupRequest represents the request to join a consumer group
type JoinGroupRequest struct {
	MemberID string `json:"member_id"`
}

// CreateConsumerGroupHandler handles POST /consumer-groups
func CreateConsumerGroupHandler(brokerService *services.BrokerService) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req ConsumerGroupRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, fmt.Sprintf("Invalid JSON: %v", err), http.StatusBadRequest)
			return
		}

		if req.GroupID == "" {
			http.Error(w, "group_id is required", http.StatusBadRequest)
			return
		}

		if len(req.Topics) == 0 {
			http.Error(w, "topics are required", http.StatusBadRequest)
			return
		}

		group, err := brokerService.CreateConsumerGroup(req.GroupID, req.Topics)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to create group: %v", err), http.StatusConflict)
			return
		}

		response := convertGroupToResponse(group)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	})
}

// GetConsumerGroupHandler handles GET /consumer-groups/{groupId}
func GetConsumerGroupHandler(brokerService *services.BrokerService) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		groupID := r.PathValue("groupId")
		if groupID == "" {
			http.Error(w, "group_id is required", http.StatusBadRequest)
			return
		}

		group, err := brokerService.GetConsumerGroup(groupID)
		if err != nil {
			http.Error(w, fmt.Sprintf("Group not found: %v", err), http.StatusNotFound)
			return
		}

		response := convertGroupToResponse(group)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	})
}

// JoinConsumerGroupHandler handles POST /consumer-groups/{groupId}/members
func JoinConsumerGroupHandler(brokerService *services.BrokerService) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		groupID := r.PathValue("groupId")
		if groupID == "" {
			http.Error(w, "group_id is required", http.StatusBadRequest)
			return
		}

		var req JoinGroupRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, fmt.Sprintf("Invalid JSON: %v", err), http.StatusBadRequest)
			return
		}

		if req.MemberID == "" {
			http.Error(w, "member_id is required", http.StatusBadRequest)
			return
		}

		// Create a temporary consumer instance for the member
		tempConsumer := consumer.NewConsumer("data/topics", req.MemberID)

		err := brokerService.JoinConsumerGroup(groupID, req.MemberID, tempConsumer)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to join group: %v", err), http.StatusBadRequest)
			return
		}

		// Return updated group info
		group, err := brokerService.GetConsumerGroup(groupID)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to get updated group: %v", err), http.StatusInternalServerError)
			return
		}

		response := convertGroupToResponse(group)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	})
}

// LeaveConsumerGroupHandler handles DELETE /consumer-groups/{groupId}/members/{memberId}
func LeaveConsumerGroupHandler(brokerService *services.BrokerService) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		groupID := r.PathValue("groupId")
		memberID := r.PathValue("memberId")

		if groupID == "" {
			http.Error(w, "group_id is required", http.StatusBadRequest)
			return
		}

		if memberID == "" {
			http.Error(w, "member_id is required", http.StatusBadRequest)
			return
		}

		err := brokerService.LeaveConsumerGroup(groupID, memberID)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to leave group: %v", err), http.StatusBadRequest)
			return
		}

		// Return updated group info
		group, err := brokerService.GetConsumerGroup(groupID)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to get updated group: %v", err), http.StatusInternalServerError)
			return
		}

		response := convertGroupToResponse(group)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	})
}

// convertGroupToResponse converts a ConsumerGroup to API response format
func convertGroupToResponse(group *consumer.ConsumerGroup) ConsumerGroupResponse {
	members := make(map[string]MemberResponse)
	for memberID, member := range group.Members {
		members[memberID] = MemberResponse{
			ID:         member.ID,
			JoinedAt:   member.JoinedAt.Format("2006-01-02T15:04:05Z"),
			Partitions: member.Partitions,
		}
	}

	return ConsumerGroupResponse{
		ID:            group.ID,
		Topics:        group.Topics,
		Members:       members,
		Assignments:   group.Assignments,
		Generation:    group.Generation,
		LastRebalance: group.LastRebalance.Format("2006-01-02T15:04:05Z"),
	}
}
