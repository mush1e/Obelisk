package consumer

// Consumer implementation with per-topic offset tracking and subscription management.

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	"github.com/mush1e/obelisk/internal/message"
	"github.com/mush1e/obelisk/internal/retry"
	"github.com/mush1e/obelisk/internal/storage"

	obeliskErrors "github.com/mush1e/obelisk/internal/errors"
)

// Consumer represents a consumer instance for the Obelisk message broker.
// Each consumer maintains its own set of topic subscriptions and tracks the
// last processed offset for each subscribed topic. This enables multiple
// consumers to read from the same topics independently without interfering
// with each other's progress.
//
// The consumer operates in a stateful manner, remembering where it left off
// Consumer manages topic subscriptions and tracks per-topic offsets.
type Consumer struct {
	subscribedTopics map[string]uint64
	baseDir          string
}

// NewConsumer creates a new consumer subscribed to the specified topics.
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

// Poll retrieves new messages from the specified topic starting from the current offset.
func (c *Consumer) Poll(topic string) ([]message.Message, error) {
	// Check if consumer is subscribed to the requested topic
	offset, subscribed := c.subscribedTopics[topic]
	if !subscribed {
		return nil, fmt.Errorf("not subscribed to topic %s", topic)
	}

	// Construct file paths for topic storage files
	logFile := filepath.Join(c.baseDir, topic+".log")
	idxFile := filepath.Join(c.baseDir, topic+".idx")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Use closure to capture the messages
	var messages []message.Message
	var err error

	if offset == 0 {
		err = retry.Retry(ctx, retry.DefaultConfig(), func() error {
			msgs, readErr := storage.ReadAllMessages(logFile)
			if readErr != nil {
				return obeliskErrors.NewTransientError("read_all_messages", "failed to read messages", readErr)
			}
			messages = msgs
			return nil
		})
	} else {
		err = retry.Retry(ctx, retry.DefaultConfig(), func() error {
			msgs, readErr := storage.ReadMessagesFromOffset(logFile, idxFile, offset)
			if readErr != nil {
				return obeliskErrors.NewTransientError("read_messages_offset", "failed to read from offset", readErr)
			}
			messages = msgs
			return nil
		})
	}
	return messages, err
}

// Commit updates the consumer's offset for the specified topic after successful processing.
func (c *Consumer) Commit(topic string, offset uint64) error {
	// Verify subscription exists before updating offset
	if _, ok := c.subscribedTopics[topic]; !ok {
		return fmt.Errorf("not subscribed to topic %s", topic)
	}

	// Update in-memory offset tracking
	c.subscribedTopics[topic] = offset
	return nil
}

// Subscribe adds a new topic subscription to the consumer.

func (c *Consumer) Subscribe(topic string) {
	c.subscribedTopics[topic] = 0 // Start from beginning for new subscriptions
}

// Unsubscribe removes a topic subscription from the consumer.
func (c *Consumer) Unsubscribe(topic string) {
	delete(c.subscribedTopics, topic) // Remove topic and its offset information
}

// GetCurrentOffset returns the current offset position for the specified topic.
func (c *Consumer) GetCurrentOffset(topic string) (uint64, error) {
	offset, subscribed := c.subscribedTopics[topic]
	if !subscribed {
		return 0, fmt.Errorf("not subscribed to topic %s", topic)
	}
	return offset, nil
}

// GetTopicMessageCount returns the total number of messages available in the specified topic.
func (c *Consumer) GetTopicMessageCount(topic string) (int64, error) {
	idxFile := filepath.Join(c.baseDir, topic+".idx")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var count int64
	err := retry.Retry(ctx, retry.DefaultConfig(), func() error {
		cnt, readErr := storage.GetTopicMessageCount(idxFile)
		if readErr != nil {
			return obeliskErrors.NewTransientError("get_message_count", "failed to get count", readErr)
		}
		count = cnt
		return nil
	})

	return count, err
}

// Reset resets the consumer's offset for the specified topic back to 0.
func (c *Consumer) Reset(topic string) error {
	// Verify subscription exists before resetting offset
	if _, ok := c.subscribedTopics[topic]; !ok {
		return fmt.Errorf("not subscribed to topic %s", topic)
	}

	// Reset offset to beginning of topic
	c.subscribedTopics[topic] = 0
	return nil
}
