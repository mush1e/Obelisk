package consumer

// Consumer implementation with per-topic offset tracking and subscription management.

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
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
	id               string                    // Unique consumer identifier
	subscribedTopics map[string]uint64         // Topic -> Current Offset (for legacy topics)
	partitionOffsets map[string]map[int]uint64 // Topic -> Partition -> Offset (for partitioned topics)
	baseDir          string                    // Base directory for topics
	offsetFile       string                    // Path to this consumer's offset file
	mtx              sync.RWMutex              // Protects offset operations
}

// NewConsumer creates a new consumer subscribed to the specified topics.
func NewConsumer(baseDir, consumerID string, topics ...string) *Consumer {
	consumersDir := filepath.Join(baseDir, "../consumers")
	os.MkdirAll(consumersDir, 0755)

	offsetFile := filepath.Join(consumersDir, fmt.Sprintf("%s.json", consumerID))

	c := &Consumer{
		id:               consumerID,
		subscribedTopics: make(map[string]uint64),
		partitionOffsets: make(map[string]map[int]uint64),
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

// loadOffsets reads the consumer's offsets from disk (handles both legacy and partitioned)
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

	// Try to unmarshal into a generic map first
	var offsetData map[string]interface{}
	if err := json.Unmarshal(data, &offsetData); err != nil {
		return obeliskErrors.NewDataError("load_offsets", "invalid offset file format", err)
	}

	// Parse the offset data
	for key, value := range offsetData {
		if strings.HasSuffix(key, "_partitions") {
			// This is partition offset data
			topic := strings.TrimSuffix(key, "_partitions")
			if partitionData, ok := value.(map[string]interface{}); ok {
				c.partitionOffsets[topic] = make(map[int]uint64)
				for pStr, offset := range partitionData {
					partition, _ := strconv.Atoi(pStr)
					if offsetFloat, ok := offset.(float64); ok {
						c.partitionOffsets[topic][partition] = uint64(offsetFloat)
					}
				}
			}
		} else {
			// This is a legacy topic offset
			if offsetFloat, ok := value.(float64); ok {
				c.subscribedTopics[key] = uint64(offsetFloat)
			}
		}
	}

	fmt.Printf("Consumer %s: loaded offsets for %d topics, %d partitioned topics\n",
		c.id, len(c.subscribedTopics), len(c.partitionOffsets))
	return nil
}

// saveOffsets persists all consumer offsets to disk
func (c *Consumer) saveOffsets() error {
	c.mtx.RLock()
	defer c.mtx.RUnlock()

	// Create a combined offset file that tracks both legacy and partitioned topics
	offsetData := make(map[string]interface{})

	// Add legacy offsets
	for topic, offset := range c.subscribedTopics {
		offsetData[topic] = offset
	}

	// Add partition offsets
	for topic, partitions := range c.partitionOffsets {
		// Convert int keys to string for JSON marshaling
		partitionMap := make(map[string]uint64)
		for partition, offset := range partitions {
			partitionMap[strconv.Itoa(partition)] = offset
		}
		offsetData[topic+"_partitions"] = partitionMap
	}

	data, err := json.MarshalIndent(offsetData, "", "  ")
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

// isPartitionedTopic checks if a topic uses the new partitioned structure
func (c *Consumer) isPartitionedTopic(topic string) bool {
	// Check if topic directory exists (partitioned topics use directories)
	topicDir := filepath.Join(c.baseDir, topic)
	if stat, err := os.Stat(topicDir); err == nil && stat.IsDir() {
		return true
	}
	// Otherwise it's a legacy single-file topic
	return false
}

// Poll retrieves new messages from the specified topic starting from the current offset.
func (c *Consumer) Poll(topic string) ([]message.Message, error) {
	c.mtx.RLock()
	_, subscribed := c.subscribedTopics[topic]
	c.mtx.RUnlock()

	if !subscribed {
		return nil, obeliskErrors.NewPermanentError("poll", "not subscribed to topic",
			fmt.Errorf("topic: %s", topic))
	}

	// Check if this is a partitioned topic
	if c.isPartitionedTopic(topic) {
		return c.pollPartitionedTopic(topic)
	} else {
		// Legacy: single file topic
		c.mtx.RLock()
		offset := c.subscribedTopics[topic]
		c.mtx.RUnlock()
		return c.pollLegacyTopic(topic, offset)
	}
}

// pollPartitionedTopic reads from all partitions of a topic with proper offset tracking
func (c *Consumer) pollPartitionedTopic(topic string) ([]message.Message, error) {
	fmt.Printf("Consumer %s: Polling partitioned topic %s\n", c.id, topic)

	partitions, err := c.discoverPartitions(topic)
	if err != nil {
		return nil, err
	}

	fmt.Printf("Consumer %s: Found %d partitions for topic %s\n", c.id, len(partitions), topic)

	var allMessages []message.Message

	// Get or initialize partition offsets for this topic
	c.mtx.Lock()
	if _, exists := c.partitionOffsets[topic]; !exists {
		c.partitionOffsets[topic] = make(map[int]uint64)
	}
	c.mtx.Unlock()

	// Read from each partition starting from its own offset
	for _, partition := range partitions {
		c.mtx.RLock()
		partitionOffset := c.partitionOffsets[topic][partition]
		c.mtx.RUnlock()

		logFile, idxFile := storage.GetPartitionedPaths(c.baseDir, topic, partition)

		fmt.Printf("Consumer %s: Reading partition %d from offset %d (file: %s)\n",
			c.id, partition, partitionOffset, logFile)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		var messages []message.Message

		err := retry.Retry(ctx, retry.DefaultConfig(), func() error {
			var readErr error
			if partitionOffset == 0 {
				messages, readErr = storage.ReadAllMessages(logFile)
			} else {
				messages, readErr = storage.ReadMessagesFromOffset(logFile, idxFile, partitionOffset)
			}
			return readErr
		})
		cancel()

		if err != nil {
			fmt.Printf("Consumer %s: Error reading partition %d: %v\n", c.id, partition, err)
			continue
		}

		fmt.Printf("Consumer %s: Read %d messages from partition %d\n",
			c.id, len(messages), partition)

		if len(messages) > 0 {
			allMessages = append(allMessages, messages...)
		}
	}

	// Sort messages by timestamp to maintain order across partitions
	sort.Slice(allMessages, func(i, j int) bool {
		return allMessages[i].Timestamp.Before(allMessages[j].Timestamp)
	})

	fmt.Printf("Consumer %s: Total messages read from %s: %d\n",
		c.id, topic, len(allMessages))

	return allMessages, nil
}

// pollLegacyTopic reads from old single-file topics (backward compatibility)
func (c *Consumer) pollLegacyTopic(topic string, offset uint64) ([]message.Message, error) {
	logFile := filepath.Join(c.baseDir, topic+".log")
	idxFile := filepath.Join(c.baseDir, topic+".idx")

	fmt.Printf("Consumer %s: Polling legacy topic %s from offset %d\n", c.id, topic, offset)

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

	fmt.Printf("Consumer %s: Read %d messages from legacy topic %s\n",
		c.id, len(messages), topic)

	return messages, err
}

// Commit updates the consumer's offset for the specified topic after successful processing.
func (c *Consumer) Commit(topic string, newOffset uint64) error {
	fmt.Printf("Consumer %s: Committing offset %d for topic %s\n", c.id, newOffset, topic)

	if c.isPartitionedTopic(topic) {
		// For partitioned topics, we need to update partition offsets
		return c.commitPartitionedTopic(topic, newOffset)
	}

	// Legacy single-file topic commit
	c.mtx.Lock()
	if _, ok := c.subscribedTopics[topic]; !ok {
		c.mtx.Unlock()
		return obeliskErrors.NewPermanentError("commit", "not subscribed to topic",
			fmt.Errorf("topic: %s", topic))
	}

	c.subscribedTopics[topic] = newOffset
	c.mtx.Unlock()

	return c.saveOffsets()
}

// commitPartitionedTopic updates offsets for all partitions of a topic
func (c *Consumer) commitPartitionedTopic(topic string, totalMessagesRead uint64) error {
	// For simplicity, distribute the offset evenly across partitions
	// In a real implementation, you'd track messages per partition
	partitions, err := c.discoverPartitions(topic)
	if err != nil {
		return err
	}

	c.mtx.Lock()
	if _, exists := c.partitionOffsets[topic]; !exists {
		c.partitionOffsets[topic] = make(map[int]uint64)
	}

	// Simple approach: advance each partition's offset
	// In production, you'd track which messages came from which partition
	messagesPerPartition := totalMessagesRead / uint64(len(partitions))
	for _, partition := range partitions {
		c.partitionOffsets[topic][partition] += messagesPerPartition
	}
	c.mtx.Unlock()

	return c.saveOffsets()
}

// Subscribe adds a new topic subscription
func (c *Consumer) Subscribe(topic string) {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	if _, exists := c.subscribedTopics[topic]; !exists {
		c.subscribedTopics[topic] = 0
		_ = c.saveOffsets()
	}
}

// Unsubscribe removes a topic subscription
func (c *Consumer) Unsubscribe(topic string) {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	delete(c.subscribedTopics, topic)
	delete(c.partitionOffsets, topic)
	_ = c.saveOffsets()
}

// GetCurrentOffset returns the current offset for a topic
func (c *Consumer) GetCurrentOffset(topic string) (uint64, error) {
	c.mtx.RLock()
	defer c.mtx.RUnlock()

	if c.isPartitionedTopic(topic) {
		// For partitioned topics, return the sum of all partition offsets
		total := uint64(0)
		if partitions, exists := c.partitionOffsets[topic]; exists {
			for _, offset := range partitions {
				total += offset
			}
		}
		return total, nil
	}

	offset, subscribed := c.subscribedTopics[topic]
	if !subscribed {
		return 0, obeliskErrors.NewPermanentError("get_offset", "not subscribed to topic",
			fmt.Errorf("topic: %s", topic))
	}
	return offset, nil
}

// GetTopicMessageCount returns total messages in a topic
func (c *Consumer) GetTopicMessageCount(topic string) (int64, error) {
	if c.isPartitionedTopic(topic) {
		// Sum messages across all partitions
		partitions, err := c.discoverPartitions(topic)
		if err != nil {
			return 0, err
		}

		total := int64(0)
		for _, partition := range partitions {
			_, idxFile := storage.GetPartitionedPaths(c.baseDir, topic, partition)

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			var count int64
			err := retry.Retry(ctx, retry.DefaultConfig(), func() error {
				cnt, readErr := storage.GetTopicMessageCount(idxFile)
				if readErr != nil {
					return readErr
				}
				count = cnt
				return nil
			})
			cancel()

			if err == nil {
				total += count
			}
		}

		return total, nil
	}

	// Legacy single-file topic
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
	fmt.Printf("Consumer %s: Resetting offset for topic %s\n", c.id, topic)

	c.mtx.Lock()
	defer c.mtx.Unlock()

	if _, ok := c.subscribedTopics[topic]; !ok {
		return obeliskErrors.NewPermanentError("reset", "not subscribed to topic",
			fmt.Errorf("topic: %s", topic))
	}

	// Reset legacy offset
	c.subscribedTopics[topic] = 0

	// Reset partition offsets if they exist
	if partitions, exists := c.partitionOffsets[topic]; exists {
		for partition := range partitions {
			c.partitionOffsets[topic][partition] = 0
		}
	}

	return c.saveOffsets()
}

// GetConsumerID returns the consumer's unique identifier
func (c *Consumer) GetConsumerID() string {
	return c.id
}

// discoverPartitions finds all partition numbers for a topic
func (c *Consumer) discoverPartitions(topic string) ([]int, error) {
	topicDir := filepath.Join(c.baseDir, topic)
	entries, err := os.ReadDir(topicDir)
	if err != nil {
		return nil, obeliskErrors.NewPermanentError("discover_partitions", "failed to read topic directory", err)
	}

	var partitions []int
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "partition-") && strings.HasSuffix(entry.Name(), ".log") {
			// Extract partition number from "partition-X.log"
			name := strings.TrimPrefix(entry.Name(), "partition-")
			name = strings.TrimSuffix(name, ".log")
			if partition, parseErr := strconv.Atoi(name); parseErr == nil {
				partitions = append(partitions, partition)
			}
		}
	}

	sort.Ints(partitions) // Keep them in order
	return partitions, nil
}
