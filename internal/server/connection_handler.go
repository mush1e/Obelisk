package server

import (
	"fmt"

	"github.com/mush1e/obelisk/internal/batch"
	"github.com/mush1e/obelisk/internal/buffer"
	"github.com/mush1e/obelisk/internal/message"
)

type ObeliskConnectionHandler struct {
	topicBuffers *buffer.TopicBuffers
	batcher      *batch.TopicBatcher
}

func NewObeliskConnectionHandler(tb *buffer.TopicBuffers, b *batch.TopicBatcher) *ObeliskConnectionHandler {
	return &ObeliskConnectionHandler{
		topicBuffers: tb,
		batcher:      b,
	}
}

func (h *ObeliskConnectionHandler) HandleMessage(msg *message.Message) error {
	fmt.Printf("Received message - Topic: %s, Key: %s, Value: %s\n", msg.Topic, msg.Key, msg.Value)

	// Store in memory buffer
	h.topicBuffers.Push(*msg)

	// Add to batcher for persistent storage
	if err := h.batcher.AddMessage(*msg); err != nil {
		return fmt.Errorf("failed to add message to batcher: %w", err)
	}

	return nil
}
