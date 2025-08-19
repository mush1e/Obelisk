package consumer

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

	obeliskErrors "github.com/mush1e/obelisk/internal/errors"
	"github.com/mush1e/obelisk/internal/message"
	"github.com/mush1e/obelisk/internal/retry"
	"github.com/mush1e/obelisk/internal/storage"
)

// MessageWithPartition tracks which partition a message came from
type MessageWithPartition struct {
	Message   message.Message
	Partition int
	Offset    uint64
}

// Consumer represents a consumer instance for the Obelisk message broker.
type Consumer struct {
	id               string
	subscribedTopics map[string]uint64         // Topic -> Current Offset (for legacy topics)
	partitionOffsets map[string]map[int]uint64 // Topic -> Partition -> Offset (for partitioned topics)
	baseDir          string
	offsetFile       string
	mtx              sync.RWMutex

	// Track messages read per partition in current poll
	lastPollPartitionCounts map[string]map[int]int // Topic -> Partition -> MessageCount
}

// NewConsumer creates a new consumer subscribed to the specified topics.
func NewConsumer(baseDir, consumerID string, topics ...string) *Consumer {
	consumersDir := filepath.Join(baseDir, "../consumers")
	os.MkdirAll(consumersDir, 0755)

	offsetFile := filepath.Join(consumersDir, fmt.Sprintf("%s.json", consumerID))

	c := &Consumer{
		id:                      consumerID,
		subscribedTopics:        make(map[string]uint64),
		partitionOffsets:        make(map[string]map[int]uint64),
		baseDir:                 baseDir,
		offsetFile:              offsetFile,
		lastPollPartitionCounts: make(map[string]map[int]int),
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

// PollWithPartitionTracking retrieves new messages and tracks their partition origins
func (c *Consumer) PollWithPartitionTracking(topic string) ([]MessageWithPartition, error) {
	c.mtx.RLock()
	_, subscribed := c.subscribedTopics[topic]
	c.mtx.RUnlock()

	if !subscribed {
		return nil, obeliskErrors.NewPermanentError("poll", "not subscribed to topic",
			fmt.Errorf("topic: %s", topic))
	}

	// Check if this is a partitioned topic
	if c.isPartitionedTopic(topic) {
		return c.pollPartitionedTopicWithTracking(topic)
	} else {
		// Legacy: single file topic
		c.mtx.RLock()
		offset := c.subscribedTopics[topic]
		c.mtx.RUnlock()

		messages, err := c.pollLegacyTopic(topic, offset)
		if err != nil {
			return nil, err
		}

		// Convert to MessageWithPartition format (partition -1 for legacy)
		result := make([]MessageWithPartition, len(messages))
		for i, msg := range messages {
			result[i] = MessageWithPartition{
				Message:   msg,
				Partition: -1, // Indicate legacy topic
				Offset:    offset + uint64(i),
			}
		}
		return result, nil
	}
}

// pollPartitionedTopicWithTracking reads from all partitions and tracks message origins
func (c *Consumer) pollPartitionedTopicWithTracking(topic string) ([]MessageWithPartition, error) {
	fmt.Printf("Consumer %s: Polling partitioned topic %s with tracking\n", c.id, topic)

	partitions, err := c.discoverPartitions(topic)
	if err != nil {
		return nil, err
	}

	fmt.Printf("Consumer %s: Found %d partitions for topic %s\n", c.id, len(partitions), topic)

	var allMessages []MessageWithPartition

	// Initialize partition count tracking for this poll
	c.lastPollPartitionCounts[topic] = make(map[int]int)

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

		// Track messages with their partition and offset
		for i, msg := range messages {
			allMessages = append(allMessages, MessageWithPartition{
				Message:   msg,
				Partition: partition,
				Offset:    partitionOffset + uint64(i),
			})
		}

		// Track how many messages came from this partition
		c.lastPollPartitionCounts[topic][partition] = len(messages)
	}

	// Sort messages by timestamp to maintain order across partitions
	sort.Slice(allMessages, func(i, j int) bool {
		return allMessages[i].Message.Timestamp.Before(allMessages[j].Message.Timestamp)
	})

	fmt.Printf("Consumer %s: Total messages read from %s: %d\n",
		c.id, topic, len(allMessages))

	return allMessages, nil
}

// Poll retrieves new messages (backward compatibility wrapper)
func (c *Consumer) Poll(topic string) ([]message.Message, error) {
	messagesWithPartition, err := c.PollWithPartitionTracking(topic)
	if err != nil {
		return nil, err
	}

	// Extract just the messages for backward compatibility
	messages := make([]message.Message, len(messagesWithPartition))
	for i, mwp := range messagesWithPartition {
		messages[i] = mwp.Message
	}

	return messages, nil
}

// CommitWithPartitionTracking commits offsets with proper partition tracking
func (c *Consumer) CommitWithPartitionTracking(topic string) error {
	fmt.Printf("Consumer %s: Committing offsets for topic %s with partition tracking\n", c.id, topic)

	if c.isPartitionedTopic(topic) {
		return c.commitPartitionedTopicProper(topic)
	}

	// Legacy single-file topic commit (unchanged)
	c.mtx.Lock()
	if counts, exists := c.lastPollPartitionCounts[topic]; exists && counts[-1] > 0 {
		c.subscribedTopics[topic] += uint64(counts[-1])
	}
	c.mtx.Unlock()

	return c.saveOffsets()
}

// commitPartitionedTopicProper properly updates offsets per partition based on actual messages read
func (c *Consumer) commitPartitionedTopicProper(topic string) error {
	c.mtx.Lock()

	// Make sure we have partition offsets initialized
	if _, exists := c.partitionOffsets[topic]; !exists {
		c.partitionOffsets[topic] = make(map[int]uint64)
	}

	// Update each partition's offset based on actual messages read
	if partitionCounts, exists := c.lastPollPartitionCounts[topic]; exists {
		for partition, count := range partitionCounts {
			if count > 0 {
				// Advance this partition's offset by the actual number of messages read
				c.partitionOffsets[topic][partition] += uint64(count)
				fmt.Printf("Consumer %s: Advanced partition %d offset by %d to %d\n",
					c.id, partition, count, c.partitionOffsets[topic][partition])
			}
		}
	}
	c.mtx.Unlock()

	return c.saveOffsets()
}

// Commit updates the consumer's offset (improved version)
func (c *Consumer) Commit(topic string, newOffset uint64) error {
	fmt.Printf("Consumer %s: Committing offset %d for topic %s\n", c.id, newOffset, topic)

	// Use the partition-aware commit
	return c.CommitWithPartitionTracking(topic)
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

	// Clear last poll counts
	delete(c.lastPollPartitionCounts, topic)

	return c.saveOffsets()
}

// Helper methods (unchanged from original)

func (c *Consumer) isPartitionedTopic(topic string) bool {
	topicDir := filepath.Join(c.baseDir, topic)
	if stat, err := os.Stat(topicDir); err == nil && stat.IsDir() {
		return true
	}
	return false
}

func (c *Consumer) discoverPartitions(topic string) ([]int, error) {
	topicDir := filepath.Join(c.baseDir, topic)
	entries, err := os.ReadDir(topicDir)
	if err != nil {
		return nil, obeliskErrors.NewPermanentError("discover_partitions", "failed to read topic directory", err)
	}

	var partitions []int
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "partition-") && strings.HasSuffix(entry.Name(), ".log") {
			name := strings.TrimPrefix(entry.Name(), "partition-")
			name = strings.TrimSuffix(name, ".log")
			if partition, parseErr := strconv.Atoi(name); parseErr == nil {
				partitions = append(partitions, partition)
			}
		}
	}

	sort.Ints(partitions)
	return partitions, nil
}

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

func (c *Consumer) loadOffsets() error {
	data, err := os.ReadFile(c.offsetFile)
	if err != nil {
		if os.IsNotExist(err) {
			return err
		}
		return obeliskErrors.NewPermanentError("load_offsets", "failed to read offset file", err)
	}

	c.mtx.Lock()
	defer c.mtx.Unlock()

	var offsetData map[string]interface{}
	if err := json.Unmarshal(data, &offsetData); err != nil {
		return obeliskErrors.NewDataError("load_offsets", "invalid offset file format", err)
	}

	for key, value := range offsetData {
		if strings.HasSuffix(key, "_partitions") {
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
			if offsetFloat, ok := value.(float64); ok {
				c.subscribedTopics[key] = uint64(offsetFloat)
			}
		}
	}

	fmt.Printf("Consumer %s: loaded offsets for %d topics, %d partitioned topics\n",
		c.id, len(c.subscribedTopics), len(c.partitionOffsets))
	return nil
}

func (c *Consumer) saveOffsets() error {
	c.mtx.RLock()
	defer c.mtx.RUnlock()

	offsetData := make(map[string]interface{})

	for topic, offset := range c.subscribedTopics {
		offsetData[topic] = offset
	}

	for topic, partitions := range c.partitionOffsets {
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

	tempFile := c.offsetFile + ".tmp"
	if err := os.WriteFile(tempFile, data, 0644); err != nil {
		return obeliskErrors.NewTransientError("save_offsets", "failed to write temp file", err)
	}

	if err := os.Rename(tempFile, c.offsetFile); err != nil {
		return obeliskErrors.NewTransientError("save_offsets", "failed to rename offset file", err)
	}

	return nil
}

// Additional helper methods remain unchanged...
func (c *Consumer) Subscribe(topic string) {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	if _, exists := c.subscribedTopics[topic]; !exists {
		c.subscribedTopics[topic] = 0
		_ = c.saveOffsets()
	}
}

func (c *Consumer) Unsubscribe(topic string) {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	delete(c.subscribedTopics, topic)
	delete(c.partitionOffsets, topic)
	delete(c.lastPollPartitionCounts, topic)
	_ = c.saveOffsets()
}

func (c *Consumer) GetConsumerID() string {
	return c.id
}

func (c *Consumer) GetTopicMessageCount(topic string) (int64, error) {
	if c.isPartitionedTopic(topic) {
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
