package consumer

import (
	"context"
	"crypto/rand"
	"encoding/hex"
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
	subscribedTopics sync.Map // Topic -> Current Offset (for legacy topics)
	partitionOffsets sync.Map // Topic -> sync.Map[int]uint64 (for partitioned topics)
	baseDir          string
	offsetFile       string

	// Track messages read per partition in current poll
	lastPollPartitionCounts sync.Map // Topic -> sync.Map[int]int

	// Mutex to protect offset file operations
	offsetMutex sync.RWMutex
}

// NewConsumer creates a new consumer subscribed to the specified topics.
func NewConsumer(baseDir, consumerID string, topics ...string) *Consumer {
	consumersDir := filepath.Join(baseDir, "../consumers")
	if err := os.MkdirAll(consumersDir, 0755); err != nil {
		fmt.Printf("Consumer %s: failed to create consumers directory: %v\n", consumerID, err)
		// Continue anyway, the consumer will fail later if it can't write offsets
	}

	offsetFile := filepath.Join(consumersDir, fmt.Sprintf("%s.json", consumerID))

	c := &Consumer{
		id:         consumerID,
		baseDir:    baseDir,
		offsetFile: offsetFile,
	}

	// Load existing offsets from disk
	if err := c.loadOffsets(); err != nil {
		fmt.Printf("Consumer %s: no existing offsets found, starting fresh\n", consumerID)
	}

	// Subscribe to requested topics
	for _, t := range topics {
		if _, exists := c.subscribedTopics.Load(t); !exists {
			c.subscribedTopics.Store(t, uint64(0)) // Start from beginning if new
		}
	}

	// Save initial state
	c.saveOffsets()

	return c
}

// PollWithPartitionTracking retrieves new messages and tracks their partition origins
func (c *Consumer) PollWithPartitionTracking(topic string) ([]MessageWithPartition, error) {
	_, subscribed := c.subscribedTopics.Load(topic)
	if !subscribed {
		return nil, obeliskErrors.NewPermanentError("poll", "not subscribed to topic",
			fmt.Errorf("topic: %s", topic))
	}

	// Check if this is a partitioned topic
	if c.isPartitionedTopic(topic) {
		return c.pollPartitionedTopicWithTracking(topic)
	} else {
		// Legacy: single file topic
		offsetInterface, _ := c.subscribedTopics.Load(topic)
		offset := offsetInterface.(uint64)

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
	c.lastPollPartitionCounts.Store(topic, &sync.Map{})

	// Get or initialize partition offsets for this topic
	if _, exists := c.partitionOffsets.Load(topic); !exists {
		c.partitionOffsets.Store(topic, &sync.Map{})
	}

	// Read from each partition starting from its own offset
	for _, partition := range partitions {
		partitionOffsetInterface, _ := c.partitionOffsets.Load(topic)
		partitionOffsetMap := partitionOffsetInterface.(*sync.Map)
		partitionOffsetInterface, exists := partitionOffsetMap.Load(partition)
		var partitionOffset uint64
		if exists {
			partitionOffset = partitionOffsetInterface.(uint64)
		} else {
			partitionOffset = 0
			// Initialize the partition offset to 0
			partitionOffsetMap.Store(partition, uint64(0))
		}

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
		partitionCounts, _ := c.lastPollPartitionCounts.Load(topic)
		partitionCountMap := partitionCounts.(*sync.Map)
		partitionCountMap.Store(partition, len(messages))
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
	partitionCounts, _ := c.lastPollPartitionCounts.Load(topic)
	legacyOffset, _ := c.subscribedTopics.Load(topic)
	if counts, exists := partitionCounts.(*sync.Map); exists {
		if countInterface, ok := counts.Load(-1); ok {
			count := countInterface.(int)
			if count > 0 {
				c.subscribedTopics.Store(topic, legacyOffset.(uint64)+uint64(count))
			}
		}
	}

	return c.saveOffsets()
}

// commitPartitionedTopicProper properly updates offsets per partition based on actual messages read
func (c *Consumer) commitPartitionedTopicProper(topic string) error {
	partitionOffsets, _ := c.partitionOffsets.Load(topic)
	partitionOffsetMap := partitionOffsets.(*sync.Map)

	// Update each partition's offset based on actual messages read
	partitionCounts, _ := c.lastPollPartitionCounts.Load(topic)
	partitionCountMap := partitionCounts.(*sync.Map)

	partitionCountMap.Range(func(partitionInterface, countInterface interface{}) bool {
		partition := partitionInterface.(int)
		count := countInterface.(int)
		if count > 0 {
			// Advance this partition's offset by the actual number of messages read
			currentOffsetInterface, exists := partitionOffsetMap.Load(partition)
			var currentOffset uint64
			if exists {
				currentOffset = currentOffsetInterface.(uint64)
			} else {
				currentOffset = 0
			}
			newOffset := currentOffset + uint64(count)
			partitionOffsetMap.Store(partition, newOffset)
			fmt.Printf("Consumer %s: Advanced partition %d offset by %d to %d\n",
				c.id, partition, count, newOffset)
		}
		return true
	})

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
	if c.isPartitionedTopic(topic) {
		// For partitioned topics, return the sum of all partition offsets
		partitionOffsets, exists := c.partitionOffsets.Load(topic)
		if !exists {
			return 0, nil
		}
		partitionOffsetMap := partitionOffsets.(*sync.Map)

		total := uint64(0)
		partitionOffsetMap.Range(func(partitionInterface, offsetInterface interface{}) bool {
			offset := offsetInterface.(uint64)
			total += offset
			return true
		})
		return total, nil
	}

	offsetInterface, subscribed := c.subscribedTopics.Load(topic)
	if !subscribed {
		return 0, obeliskErrors.NewPermanentError("get_offset", "not subscribed to topic",
			fmt.Errorf("topic: %s", topic))
	}
	return offsetInterface.(uint64), nil
}

// Reset resets the offset to 0 AND persists it
func (c *Consumer) Reset(topic string) error {
	fmt.Printf("Consumer %s: Resetting offset for topic %s\n", c.id, topic)

	_, subscribed := c.subscribedTopics.Load(topic)
	if !subscribed {
		return obeliskErrors.NewPermanentError("reset", "not subscribed to topic",
			fmt.Errorf("topic: %s", topic))
	}

	// Reset legacy offset
	c.subscribedTopics.Store(topic, uint64(0))

	// Reset partition offsets if they exist
	if partitions, exists := c.partitionOffsets.Load(topic); exists {
		partitionOffsetMap := partitions.(*sync.Map)
		partitionOffsetMap.Range(func(partitionInterface, offsetInterface interface{}) bool {
			partition := partitionInterface.(int)
			partitionOffsetMap.Store(partition, uint64(0))
			return true
		})
	}

	// Clear last poll counts
	c.lastPollPartitionCounts.Delete(topic)

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
	c.offsetMutex.RLock()
	defer c.offsetMutex.RUnlock()

	data, err := os.ReadFile(c.offsetFile)
	if err != nil {
		if os.IsNotExist(err) {
			return err
		}
		return obeliskErrors.NewPermanentError("load_offsets", "failed to read offset file", err)
	}

	var offsetData map[string]interface{}
	if err := json.Unmarshal(data, &offsetData); err != nil {
		return obeliskErrors.NewDataError("load_offsets", "invalid offset file format", err)
	}

	for key, value := range offsetData {
		if strings.HasSuffix(key, "_partitions") {
			topic := strings.TrimSuffix(key, "_partitions")
			if partitionData, ok := value.(map[string]interface{}); ok {
				partitionMap := &sync.Map{}
				c.partitionOffsets.Store(topic, partitionMap)
				for pStr, offset := range partitionData {
					partition, _ := strconv.Atoi(pStr)
					if offsetFloat, ok := offset.(float64); ok {
						partitionMap.Store(partition, uint64(offsetFloat))
					}
				}
			}
		} else {
			if offsetFloat, ok := value.(float64); ok {
				c.subscribedTopics.Store(key, uint64(offsetFloat))
			}
		}
	}

	// Count topics and partitioned topics
	var topicCount, partitionedCount int
	c.subscribedTopics.Range(func(key, value interface{}) bool {
		topicCount++
		return true
	})
	c.partitionOffsets.Range(func(key, value interface{}) bool {
		partitionedCount++
		return true
	})

	fmt.Printf("Consumer %s: loaded offsets for %d topics, %d partitioned topics\n",
		c.id, topicCount, partitionedCount)
	return nil
}

func (c *Consumer) saveOffsets() error {
	c.offsetMutex.Lock()
	defer c.offsetMutex.Unlock()

	offsetData := make(map[string]interface{})
	c.subscribedTopics.Range(func(key, value interface{}) bool {
		offsetData[key.(string)] = value
		return true
	})

	c.partitionOffsets.Range(func(key, value interface{}) bool {
		topic := key.(string)
		partitionMap := make(map[string]uint64)
		partitionOffsetMap := value.(*sync.Map)
		partitionOffsetMap.Range(func(partitionInterface, offsetInterface interface{}) bool {
			partition := partitionInterface.(int)
			offset := offsetInterface.(uint64)
			partitionMap[strconv.Itoa(partition)] = offset
			return true
		})
		offsetData[topic+"_partitions"] = partitionMap
		return true
	})

	data, err := json.MarshalIndent(offsetData, "", "  ")
	if err != nil {
		return obeliskErrors.NewPermanentError("save_offsets", "failed to marshal offsets", err)
	}

	// Create a unique temp file to avoid race conditions
	randBytes := make([]byte, 8)
	if _, err := rand.Read(randBytes); err != nil {
		return obeliskErrors.NewPermanentError("save_offsets", "failed to generate random bytes", err)
	}
	tempFile := fmt.Sprintf("%s.%s.tmp", c.offsetFile, hex.EncodeToString(randBytes))
	if err := os.WriteFile(tempFile, data, 0644); err != nil {
		return obeliskErrors.NewTransientError("save_offsets", "failed to write temp file", err)
	}

	// Clean up temp file on error
	defer func() {
		if _, err := os.Stat(tempFile); err == nil {
			os.Remove(tempFile)
		}
	}()

	if err := os.Rename(tempFile, c.offsetFile); err != nil {
		return obeliskErrors.NewTransientError("save_offsets", "failed to rename offset file", err)
	}

	return nil
}

// Additional helper methods remain unchanged...
func (c *Consumer) Subscribe(topic string) {
	_, subscribed := c.subscribedTopics.Load(topic)
	if !subscribed {
		c.subscribedTopics.Store(topic, uint64(0))
	}
	_ = c.saveOffsets()
}

func (c *Consumer) Unsubscribe(topic string) {
	c.subscribedTopics.Delete(topic)
	c.partitionOffsets.Delete(topic)
	c.lastPollPartitionCounts.Delete(topic)
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
