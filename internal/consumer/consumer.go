package consumer

// This package provides a consumer implementation for the Obelisk message broker.
// The consumer manages subscriptions to multiple topics and tracks per-topic offsets
// to enable reliable message consumption patterns. It supports polling for new messages,
// committing processed offsets, and managing topic subscriptions dynamically.
//
// The consumer handles:
// - Per-topic offset tracking for reliable message consumption
// - Subscription management with dynamic add/remove capabilities
// - Efficient message polling from specific offsets using storage indices
// - Commit operations to update processed message positions
// - Reset functionality to replay messages from the beginning
// - Integration with the storage layer for persistent message retrieval

import (
	"fmt"
	"path/filepath"

	"github.com/mush1e/obelisk/internal/message"
	"github.com/mush1e/obelisk/internal/storage"
)

// Consumer represents a consumer instance for the Obelisk message broker.
// Each consumer maintains its own set of topic subscriptions and tracks the
// last processed offset for each subscribed topic. This enables multiple
// consumers to read from the same topics independently without interfering
// with each other's progress.
//
// The consumer operates in a stateful manner, remembering where it left off
// in each topic to support reliable message processing patterns. Offsets
// are managed in-memory and must be explicitly committed after successful
// message processing.
type Consumer struct {
	subscribedTopics map[string]uint64 // Maps topic names to last processed message offsets
	baseDir          string            // Base directory where topic log and index files are stored
}

// NewConsumer creates a new consumer instance subscribed to the specified topics.
// All specified topics are automatically subscribed with an initial offset of 0,
// meaning the consumer will start reading from the beginning of each topic on
// first poll operation.
//
// The consumer requires a base directory path where topic storage files (.log and .idx)
// are located. This directory must be accessible and contain the topic files for
// any topics the consumer will read from.
//
// Parameters:
//   - baseDir: Directory path containing topic storage files
//   - topics: Variable list of topic names to subscribe to initially
//
// Returns:
//   - *Consumer: Configured consumer instance ready for polling operations
func NewConsumer(baseDir string, topics ...string) *Consumer {
	c := &Consumer{
		subscribedTopics: make(map[string]uint64),
		baseDir:          baseDir,
	}

	// Subscribe to all provided topics with initial offset of 0
	for _, t := range topics {
		c.subscribedTopics[t] = 0
	}
	return c
}

// Poll retrieves new messages from the specified topic starting from the current offset.
// This method reads messages from persistent storage using the storage layer's indexing
// system for efficient random access. The consumer must be subscribed to the topic
// before polling can occur.
//
// Polling behavior varies based on the current offset:
// - Offset 0: Reads all available messages from the beginning of the topic
// - Offset > 0: Reads messages starting from the specified position using index lookup
//
// The method returns messages in the order they were written to the topic, preserving
// the original sequence for reliable message processing. After processing returned
// messages, the consumer should call Commit() to update its offset position.
//
// Parameters:
//   - topic: Name of the topic to poll messages from
//
// Returns:
//   - []message.Message: Slice of messages available since last committed offset
//   - error: Error if not subscribed to topic or storage access fails
func (c *Consumer) Poll(topic string) ([]message.Message, error) {
	// Check if consumer is subscribed to the requested topic
	offset, subscribed := c.subscribedTopics[topic]
	if !subscribed {
		return nil, fmt.Errorf("not subscribed to topic %s", topic)
	}

	// Construct file paths for topic storage files
	logFile := filepath.Join(c.baseDir, topic+".log")
	idxFile := filepath.Join(c.baseDir, topic+".idx")

	// Handle initial read (offset 0) - read all available messages
	if offset == 0 {
		return storage.ReadAllMessages(logFile)
	}

	// Read messages starting from specific offset using index for efficient access
	return storage.ReadMessagesFromOffset(logFile, idxFile, offset)
}

// Commit updates the consumer's offset for the specified topic after successful processing.
// This operation marks messages as processed and advances the consumer's position in
// the topic. Subsequent poll operations will start from this new offset, ensuring
// messages are not reprocessed unless explicitly reset.
//
// The offset typically represents the number of messages that have been successfully
// processed. For example, after processing 10 messages, the consumer would commit
// offset 10 to indicate readiness to receive message 11 and beyond.
//
// This method only updates the in-memory offset state. No persistent storage is
// modified, allowing for lightweight commit operations during message processing.
//
// Parameters:
//   - topic: Name of the topic to update offset for
//   - offset: New offset position indicating number of processed messages
//
// Returns:
//   - error: Error if not subscribed to the specified topic
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
// The topic is added with an initial offset of 0, meaning the consumer will
// start reading from the beginning of the topic on the first poll operation.
//
// This method enables dynamic subscription management, allowing consumers to
// add new topics during runtime without requiring restart or reconfiguration.
// If the topic is already subscribed, this operation has no effect.
//
// Parameters:
//   - topic: Name of the topic to subscribe to
func (c *Consumer) Subscribe(topic string) {
	c.subscribedTopics[topic] = 0 // Start from beginning for new subscriptions
}

// Unsubscribe removes a topic subscription from the consumer.
// After unsubscribing, the consumer will no longer be able to poll messages
// from the specified topic until it subscribes again. The current offset
// information for the topic is discarded.
//
// This method enables dynamic subscription management and helps prevent
// accidental polling of topics that are no longer needed. If the topic
// is not currently subscribed, this operation has no effect.
//
// Parameters:
//   - topic: Name of the topic to unsubscribe from
func (c *Consumer) Unsubscribe(topic string) {
	delete(c.subscribedTopics, topic) // Remove topic and its offset information
}

// GetCurrentOffset returns the current offset position for the specified topic.
// The offset represents the number of messages that have been successfully
// processed and committed by this consumer. This information is useful for:
//
// - Monitoring consumer progress and lag
// - Debugging message processing issues
// - Implementing consumer coordination strategies
// - Determining how many messages are available for processing
//
// Parameters:
//   - topic: Name of the topic to get current offset for
//
// Returns:
//   - uint64: Current offset position (number of processed messages)
//   - error: Error if not subscribed to the specified topic
func (c *Consumer) GetCurrentOffset(topic string) (uint64, error) {
	offset, subscribed := c.subscribedTopics[topic]
	if !subscribed {
		return 0, fmt.Errorf("not subscribed to topic %s", topic)
	}
	return offset, nil
}

// GetTopicMessageCount returns the total number of messages available in the specified topic.
// This count represents all messages that have been written to the topic's storage,
// regardless of the consumer's current offset position. The information is useful for:
//
// - Calculating consumer lag (total - current offset)
// - Understanding topic activity and size
// - Monitoring system health and message throughput
// - Planning capacity and retention policies
//
// The count is retrieved from the topic's index file, which maintains accurate
// message count information as messages are appended to the topic.
//
// Parameters:
//   - topic: Name of the topic to get message count for
//
// Returns:
//   - int64: Total number of messages available in the topic
//   - error: Error if topic index cannot be accessed or read
func (c *Consumer) GetTopicMessageCount(topic string) (int64, error) {
	// Construct path to topic's index file
	idxFile := filepath.Join(c.baseDir, topic+".idx")

	// Delegate to storage layer for accurate count retrieval
	return storage.GetTopicMessageCount(idxFile)
}

// Reset resets the consumer's offset for the specified topic back to 0.
// This operation causes the consumer to restart reading from the beginning
// of the topic on the next poll operation, effectively replaying all messages.
//
// Reset is useful for:
// - Reprocessing messages after fixing bugs in message handling logic
// - Implementing replay scenarios for data recovery
// - Testing consumer behavior with known message sets
// - Migrating consumers to different processing logic
//
// The reset only affects the in-memory offset tracking and does not modify
// the topic's storage files or their contents.
//
// Parameters:
//   - topic: Name of the topic to reset offset for
//
// Returns:
//   - error: Error if not subscribed to the specified topic
func (c *Consumer) Reset(topic string) error {
	// Verify subscription exists before resetting offset
	if _, ok := c.subscribedTopics[topic]; !ok {
		return fmt.Errorf("not subscribed to topic %s", topic)
	}

	// Reset offset to beginning of topic
	c.subscribedTopics[topic] = 0
	return nil
}
