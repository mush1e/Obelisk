package consumer

import (
	"fmt"

	"github.com/mush1e/obelisk/internal/message"
)

// This Package provides a consumer implementation for the Obelisk framework.
// a consumer is a component that consumes messages from Obelisk topics and processes them.

// Consumer represents a consumer implementation for the Obelisk framework.
// It maps subscribed topics to their respective offsets.
type Consumer struct {
	subscribedTopics map[string]uint64 // topic -> last processed offset
}

// NewConsumer returns a new consumer and sets up a new topic
func NewConsumer(topics ...string) *Consumer {
	c := &Consumer{
		subscribedTopics: make(map[string]uint64),
	}
	for _, t := range topics {
		c.subscribedTopics[t] = 0
	}
	return c
}

// Reads through topic logs to get latest messages starting at offset
func (c *Consumer) Poll(topic string) ([]message.Message, error) {
	// Get messages starting from current offset
	return nil, nil
}

// updates offset for topic after processing
func (c *Consumer) Commit(topic string, offset uint64) error {
	if _, ok := c.subscribedTopics[topic]; !ok {
		return fmt.Errorf("not subscribed to topic %s", topic)
	}
	c.subscribedTopics[topic] = offset
	return nil
}
