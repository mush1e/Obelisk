package consumer

// Consumer implementation with per-topic offset tracking and subscription management.

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
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
	id               string            // Unique consumer identifier
	subscribedTopics map[string]uint64 // Topic -> Current Offset
	baseDir          string            // Base directory for topics
	offsetFile       string            // Path to this consumer's offset file
	mtx              sync.RWMutex      // Protects offset operations
}

// NewConsumer creates a new consumer subscribed to the specified topics.
func NewConsumer(baseDir, consumerID string, topics ...string) *Consumer {
	consumersDir := filepath.Join(baseDir, "../consumers")
	os.MkdirAll(consumersDir, 0755)

	offsetFile := filepath.Join(consumersDir, fmt.Sprintf("%s.json", consumerID))

	c := &Consumer{
		id:               consumerID,
		subscribedTopics: make(map[string]uint64),
		baseDir:          baseDir,
		offsetFile:       offsetFile,
	}

	// Load existing offsets from disk
	if err := c.loadOffsets(); err != nil {
		fmt.Printf("Consumer %s: no existing offsets found, starting fresh\n", consumerID)
	}

	// Subscribe to requested topics
	for _, t := range topics {
		if _, exists := c.subscribedTopics[t]; !exists {
			c.subscribedTopics[t] = 0 // Start from beginning if new
		}
	}

	// Save initial state
	c.saveOffsets()

	return c
}

// loadOffsets reads the consumer's offsets from disk
func (c *Consumer) loadOffsets() error {
	data, err := os.ReadFile(c.offsetFile)
	if err != nil {
		if os.IsNotExist(err) {
			return err // Normal for new consumers
		}
		return obeliskErrors.NewPermanentError("load_offsets", "failed to read offset file", err)
	}

	c.mtx.Lock()
	defer c.mtx.Unlock()

	if err := json.Unmarshal(data, &c.subscribedTopics); err != nil {
		return obeliskErrors.NewDataError("load_offsets", "invalid offset file format", err)
	}

	fmt.Printf("Consumer %s: loaded offsets for %d topics\n", c.id, len(c.subscribedTopics))
	return nil
}

// saveOffsets persists the consumer's current offsets to disk
func (c *Consumer) saveOffsets() error {
	c.mtx.RLock()
	data, err := json.MarshalIndent(c.subscribedTopics, "", "  ")
	c.mtx.RUnlock()

	if err != nil {
		return obeliskErrors.NewPermanentError("save_offsets", "failed to marshal offsets", err)
	}

	// Write atomically using temp file + rename
	tempFile := c.offsetFile + ".tmp"
	if err := os.WriteFile(tempFile, data, 0644); err != nil {
		return obeliskErrors.NewTransientError("save_offsets", "failed to write temp file", err)
	}

	// Atomic rename (on POSIX systems)
	if err := os.Rename(tempFile, c.offsetFile); err != nil {
		return obeliskErrors.NewTransientError("save_offsets", "failed to rename offset file", err)
	}

	return nil
}

// Poll retrieves new messages from the specified topic starting from the current offset.
func (c *Consumer) Poll(topic string) ([]message.Message, error) {
	c.mtx.RLock()
	offset, subscribed := c.subscribedTopics[topic]
	c.mtx.RUnlock()

	if !subscribed {
		return nil, obeliskErrors.NewPermanentError("poll", "not subscribed to topic",
			fmt.Errorf("topic: %s", topic))
	}

	logFile := filepath.Join(c.baseDir, topic+".log")
	idxFile := filepath.Join(c.baseDir, topic+".idx")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var messages []message.Message
	var err error

	if offset == 0 {
		err = retry.Retry(ctx, retry.DefaultConfig(), func() error {
			msgs, readErr := storage.ReadAllMessages(logFile)
			if readErr != nil {
				return readErr
			}
			messages = msgs
			return nil
		})
	} else {
		err = retry.Retry(ctx, retry.DefaultConfig(), func() error {
			msgs, readErr := storage.ReadMessagesFromOffset(logFile, idxFile, offset)
			if readErr != nil {
				return readErr
			}
			messages = msgs
			return nil
		})
	}

	return messages, err
}

// Commit updates the consumer's offset for the specified topic after successful processing.
func (c *Consumer) Commit(topic string, offset uint64) error {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	if _, ok := c.subscribedTopics[topic]; !ok {
		return obeliskErrors.NewPermanentError("commit", "not subscribed to topic",
			fmt.Errorf("topic: %s", topic))
	}

	// Update in-memory
	c.subscribedTopics[topic] = offset

	// Persist to disk immediately
	// In production, you might batch these for performance
	if err := c.saveOffsets(); err != nil {
		// Log but don't fail - at least we have it in memory
		fmt.Printf("Warning: failed to persist offset for consumer %s: %v\n", c.id, err)
		return err
	}

	return nil
}

// Subscribe adds a new topic subscription
func (c *Consumer) Subscribe(topic string) {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	if _, exists := c.subscribedTopics[topic]; !exists {
		c.subscribedTopics[topic] = 0
		c.saveOffsets() // Persist the new subscription
	}
}

// Unsubscribe removes a topic subscription
func (c *Consumer) Unsubscribe(topic string) {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	delete(c.subscribedTopics, topic)
	c.saveOffsets() // Persist the removal
}

// GetCurrentOffset returns the current offset for a topic
func (c *Consumer) GetCurrentOffset(topic string) (uint64, error) {
	c.mtx.RLock()
	defer c.mtx.RUnlock()

	offset, subscribed := c.subscribedTopics[topic]
	if !subscribed {
		return 0, obeliskErrors.NewPermanentError("get_offset", "not subscribed to topic",
			fmt.Errorf("topic: %s", topic))
	}
	return offset, nil
}

// GetTopicMessageCount returns total messages in a topic
func (c *Consumer) GetTopicMessageCount(topic string) (int64, error) {
	idxFile := filepath.Join(c.baseDir, topic+".idx")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var count int64
	err := retry.Retry(ctx, retry.DefaultConfig(), func() error {
		cnt, readErr := storage.GetTopicMessageCount(idxFile)
		if readErr != nil {
			return readErr
		}
		count = cnt
		return nil
	})

	return count, err
}

// Reset resets the offset to 0 AND persists it
func (c *Consumer) Reset(topic string) error {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	if _, ok := c.subscribedTopics[topic]; !ok {
		return obeliskErrors.NewPermanentError("reset", "not subscribed to topic",
			fmt.Errorf("topic: %s", topic))
	}

	c.subscribedTopics[topic] = 0
	return c.saveOffsets() // Persist the reset
}

// GetConsumerID returns the consumer's unique identifier
func (c *Consumer) GetConsumerID() string {
	return c.id
}
