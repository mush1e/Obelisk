package consumer

import (
	"fmt"
	"path/filepath"

	"github.com/mush1e/obelisk/internal/message"
	"github.com/mush1e/obelisk/internal/storage"
)

// Consumer represents a consumer implementation for the Obelisk framework.
// It maps subscribed topics to their respective offsets.
type Consumer struct {
	subscribedTopics map[string]uint64 // topic -> last processed offset
	baseDir          string
}

// NewConsumer returns a new consumer and sets up subscriptions to topics
func NewConsumer(baseDir string, topics ...string) *Consumer {
	c := &Consumer{
		subscribedTopics: make(map[string]uint64),
		baseDir:          baseDir,
	}
	for _, t := range topics {
		c.subscribedTopics[t] = 0
	}
	return c
}

// Poll reads through topic logs to get latest messages starting at current offset
func (c *Consumer) Poll(topic string) ([]message.Message, error) {
	offset, subscribed := c.subscribedTopics[topic]
	if !subscribed {
		return nil, fmt.Errorf("not subscribed to topic %s", topic)
	}

	logFile := filepath.Join(c.baseDir, topic+".log")
	idxFile := filepath.Join(c.baseDir, topic+".idx")

	// If offset is 0, read all messages
	if offset == 0 {
		return storage.ReadAllMessages(logFile)
	}

	// Read from specific offset using index
	return storage.ReadMessagesFromOffset(logFile, idxFile, offset)
}

// Commit updates offset for topic after processing
func (c *Consumer) Commit(topic string, offset uint64) error {
	if _, ok := c.subscribedTopics[topic]; !ok {
		return fmt.Errorf("not subscribed to topic %s", topic)
	}
	c.subscribedTopics[topic] = offset
	return nil
}

// Subscribe adds a new topic to the consumer
func (c *Consumer) Subscribe(topic string) {
	c.subscribedTopics[topic] = 0
}

// Unsubscribe removes a topic from the consumer
func (c *Consumer) Unsubscribe(topic string) {
	delete(c.subscribedTopics, topic)
}

// GetCurrentOffset returns the current offset for a topic
func (c *Consumer) GetCurrentOffset(topic string) (uint64, error) {
	offset, subscribed := c.subscribedTopics[topic]
	if !subscribed {
		return 0, fmt.Errorf("not subscribed to topic %s", topic)
	}
	return offset, nil
}

// GetTopicMessageCount returns the total number of messages available for a topic
func (c *Consumer) GetTopicMessageCount(topic string) (int64, error) {
	idxFile := filepath.Join(c.baseDir, topic+".idx")
	return storage.GetTopicMessageCount(idxFile)
}

// Reset resets the consumer's offset for a topic back to 0
func (c *Consumer) Reset(topic string) error {
	if _, ok := c.subscribedTopics[topic]; !ok {
		return fmt.Errorf("not subscribed to topic %s", topic)
	}
	c.subscribedTopics[topic] = 0
	return nil
}
