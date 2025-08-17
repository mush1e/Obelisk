package services

import (
	"github.com/mush1e/obelisk/internal/batch"
	"github.com/mush1e/obelisk/internal/buffer"
	"github.com/mush1e/obelisk/internal/health"
	"github.com/mush1e/obelisk/internal/message"
	"github.com/mush1e/obelisk/internal/metrics"
)

type BrokerService struct {
	buffers       *buffer.TopicBuffers
	batcher       *batch.TopicBatcher
	healthTracker *health.HealthTracker
}

func NewBrokerService(buffers *buffer.TopicBuffers, batcher *batch.TopicBatcher) *BrokerService {
	return &BrokerService{
		buffers:       buffers,
		batcher:       batcher,
		healthTracker: health.NewHealthTracker(),
	}
}

// PublishMessage saves a message (used by both TCP and HTTP!)
func (s *BrokerService) PublishMessage(msg *message.Message) error {
	// 📊 Track message received
	metrics.Metrics.MessagesReceived.WithLabelValues(msg.Topic).Inc()

	// Save to memory for fast access
	if err := s.buffers.Push(*msg); err != nil {
		// Track buffer failures
		// is buffer failure even possible?
		metrics.Metrics.MessagesFailed.WithLabelValues(msg.Topic, "buffer_full").Inc()
		// Continue to storage even if buffer fails
		s.healthTracker.RecordBufferPublish(false)
	} else {
		s.healthTracker.RecordBufferPublish(true)
	}

	// Save to disk for permanent storage
	err := s.batcher.AddMessage(*msg)
	if err != nil {
		// 📊 Track storage failures
		metrics.Metrics.MessagesFailed.WithLabelValues(msg.Topic, "storage_error").Inc()
		s.healthTracker.RecordBatcherPublish(false)
		return err
	}

	// 📊 Track successful storage
	metrics.Metrics.MessagesStored.WithLabelValues(msg.Topic).Inc()
	s.healthTracker.RecordBatcherPublish(true)
	return nil
}

// GetTopicStats gets statistics about a topic
func (s *BrokerService) GetTopicStats(topic string) (int, int64, error) {
	buffered, persisted, err := s.batcher.GetTopicStats(topic)

	if err == nil {
		// 📊 Update buffer size metric
		metrics.Metrics.BufferSize.WithLabelValues(topic).Set(float64(buffered))
	}

	return buffered, persisted, err
}
