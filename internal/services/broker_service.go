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
	metrics       *metrics.BrokerMetrics
}

func NewBrokerService(buffers *buffer.TopicBuffers, batcher *batch.TopicBatcher, metrics *metrics.BrokerMetrics) *BrokerService {
	return &BrokerService{
		buffers:       buffers,
		batcher:       batcher,
		healthTracker: health.NewHealthTracker(),
		metrics:       metrics,
	}
}

// SetBatcher sets the batcher for the broker service
func (s *BrokerService) SetBatcher(batcher *batch.TopicBatcher) {
	s.batcher = batcher
}

// GetHealthTracker returns the health tracker
func (s *BrokerService) GetHealthTracker() *health.HealthTracker {
	return s.healthTracker
}

// PublishMessage saves a message (used by both TCP and HTTP!)
func (s *BrokerService) PublishMessage(msg *message.Message) error {
	// 📊 Track message received
	if s.metrics != nil {
		s.metrics.MessagesReceived.WithLabelValues(msg.Topic).Inc()
	}

	// Save to memory for fast access
	if err := s.buffers.Push(*msg); err != nil {
		// Track buffer failures
		// is buffer failure even possible?
		if s.metrics != nil {
			s.metrics.MessagesFailed.WithLabelValues(msg.Topic, "buffer_full").Inc()
		}
		// Continue to storage even if buffer fails
		s.healthTracker.RecordBufferPublish(false)
	} else {
		s.healthTracker.RecordBufferPublish(true)
	}

	// Save to disk for permanent storage
	err := s.batcher.AddMessage(*msg)
	if err != nil {
		// 📊 Track storage failures
		if s.metrics != nil {
			s.metrics.MessagesFailed.WithLabelValues(msg.Topic, "storage_error").Inc()
		}
		s.healthTracker.RecordBatcherPublish(false)
		return err
	}

	// 📊 Track successful storage
	if s.metrics != nil {
		s.metrics.MessagesStored.WithLabelValues(msg.Topic).Inc()
	}
	s.healthTracker.RecordBatcherPublish(true)
	return nil
}

// GetTopicStats gets statistics about a topic
func (s *BrokerService) GetTopicStats(topic string) (int, int64, error) {
	buffered, persisted, err := s.batcher.GetTopicStats(topic)

	if err == nil && s.metrics != nil {
		// 📊 Update buffer size metric
		s.metrics.BufferSize.WithLabelValues(topic).Set(float64(buffered))
	}

	return buffered, persisted, err
}

// Health-related methods for the enhanced health checking system

// GetUptime returns the duration since the health tracker was created
func (s *BrokerService) GetUptime() string {
	return s.healthTracker.GetUptime().String()
}

// IsInitialized returns whether the system has been marked as initialized
func (s *BrokerService) IsInitialized() bool {
	return s.healthTracker.IsInitialized()
}

// GetBufferHealth returns the health status of the buffer system
func (s *BrokerService) GetBufferHealth() (float64, bool) {
	return s.healthTracker.GetBufferHealth()
}

// GetBatcherHealth returns the health status of the batcher system
func (s *BrokerService) GetBatcherHealth() (float64, bool) {
	return s.healthTracker.GetBatcherHealth()
}

// GetLastFlushTime returns the last flush time
func (s *BrokerService) GetLastFlushTime() string {
	return s.healthTracker.GetLastFlushTime().Format("2006-01-02T15:04:05Z07:00")
}

// GetBufferOperationCount returns the number of buffer operations tracked
func (s *BrokerService) GetBufferOperationCount() int {
	return s.healthTracker.GetBufferOperationCount()
}

// GetBatcherOperationCount returns the number of batcher operations tracked
func (s *BrokerService) GetBatcherOperationCount() int {
	return s.healthTracker.GetBatcherOperationCount()
}

// SetInitialized marks the system as initialized
func (s *BrokerService) SetInitialized() {
	s.healthTracker.SetInitialized()
}
