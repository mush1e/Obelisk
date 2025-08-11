package services

import (
	"github.com/mush1e/obelisk/internal/batch"
	"github.com/mush1e/obelisk/internal/buffer"
	"github.com/mush1e/obelisk/internal/message"
)

type BrokerService struct {
	buffers *buffer.TopicBuffers
	batcher *batch.TopicBatcher
}

func NewBrokerService(buffers *buffer.TopicBuffers, batcher *batch.TopicBatcher) *BrokerService {
	return &BrokerService{
		buffers: buffers,
		batcher: batcher,
	}
}

// PublishMessage saves a message (used by both TCP and HTTP!)
func (s *BrokerService) PublishMessage(msg *message.Message) error {
	// Save to memory for fast access
	s.buffers.Push(*msg)

	// Save to disk for permanent storage
	return s.batcher.AddMessage(*msg)
}

// GetTopicStats gets statistics about a topic
func (s *BrokerService) GetTopicStats(topic string) (int, int64, error) {
	return s.batcher.GetTopicStats(topic)
}
