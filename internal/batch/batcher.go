package batch

// Topic-based batching system that buffers messages in memory
// and flushes them to persistent storage in batches.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/mush1e/obelisk/internal/config"
	"github.com/mush1e/obelisk/internal/health"
	"github.com/mush1e/obelisk/internal/message"
	"github.com/mush1e/obelisk/internal/retry"
	"github.com/mush1e/obelisk/internal/storage"

	obeliskErrors "github.com/mush1e/obelisk/internal/errors"
)

// TopicBatcher manages batching per topic and flushes them to disk.
// It maintains separate batches for each topic to ensure isolation and
// optimal storage patterns. Each topic has its own buffer and index,
// allowing independent flush decisions and storage operations.
//
// The batcher operates with two main triggers:
// 1. Size-based: When a topic buffer reaches maxSize messages
// 2. Time-based: When maxWait time has elapsed since the last flush
//
// TODO: use sync.Map instead of map+mutex for better read performance
// on read-heavy workloads with many concurrent topic accesses.
type TopicBatcher struct {
	batches map[string]*TopicBatch
	baseDir string
	maxSize uint32
	maxWait time.Duration
	pool    *storage.FilePool
	health  *health.HealthTracker
	quit    chan struct{}
	mtx     sync.RWMutex
	wg      sync.WaitGroup
	config  *config.Config
}

// TopicBatch holds buffered messages and storage metadata for a single topic.
type TopicBatch struct {
	buffer  []message.Message
	index   *storage.OffsetIndex
	logFile string
	idxFile string
	mtx     sync.Mutex
}

// NewTopicBatcher creates a new topic batcher.
func NewTopicBatcher(baseDir string, maxSize uint32, maxWait time.Duration, pool *storage.FilePool, health *health.HealthTracker, cfg *config.Config) *TopicBatcher {
	return &TopicBatcher{
		batches: make(map[string]*TopicBatch),
		baseDir: baseDir,
		maxSize: maxSize,
		maxWait: maxWait,
		pool:    pool,
		health:  health,
		quit:    make(chan struct{}),
		config:  cfg,
	}
}

// Start initializes the batcher and starts background flush routine.
func (tb *TopicBatcher) Start() error {

	if err := os.MkdirAll(tb.baseDir, 0755); err != nil {
		return obeliskErrors.NewConfigurationError("start_batcher", "failed to create base directory", err)
	}

	if err := tb.discoverExistingTopics(); err != nil {
		fmt.Printf("[BATCHER] Warning: failed to discover existing topics: %v\n", err)
	}

	tb.wg.Add(1)
	ticker := time.NewTicker(tb.maxWait)
	go func() {
		defer ticker.Stop()
		defer tb.wg.Done()
		for {
			select {
			case <-ticker.C:
				tb.FlushAll()
			case <-tb.quit:
				tb.FlushAll()
				return
			}
		}
	}()
	return nil
}

func (tb *TopicBatcher) discoverExistingTopics() error {
	// First, find all entries in the base directory
	entries, err := os.ReadDir(tb.baseDir)
	if err != nil {
		return err
	}

	tb.mtx.Lock()
	defer tb.mtx.Unlock()

	for _, entry := range entries {
		if !entry.IsDir() {
			// Check for legacy single-file topics (backward compatibility)
			if strings.HasSuffix(entry.Name(), ".log") {
				topic := strings.TrimSuffix(entry.Name(), ".log")
				logFile := filepath.Join(tb.baseDir, entry.Name())
				idxFile := filepath.Join(tb.baseDir, topic+".idx")

				index, err := storage.LoadIndex(idxFile)
				if err != nil {
					fmt.Printf("[BATCHER] Warning: failed to load index for legacy topic %s: %v\n", topic, err)
					index = &storage.OffsetIndex{Positions: []int64{}}
				}

				batch := &TopicBatch{
					buffer:  make([]message.Message, 0, tb.maxSize),
					index:   index,
					logFile: logFile,
					idxFile: idxFile,
				}

				// Use topic name as key for legacy topics
				tb.batches[topic] = batch
				fmt.Printf("[BATCHER] Discovered legacy topic: %s (messages: %d)\n", topic, len(index.Positions))
			}
		} else {
			// This is a topic directory with partitions
			topicName := entry.Name()
			topicDir := filepath.Join(tb.baseDir, topicName)

			// Find all partition files in this topic directory
			partitionFiles, err := filepath.Glob(filepath.Join(topicDir, "partition-*.log"))
			if err != nil {
				fmt.Printf("[BATCHER] Warning: failed to glob partition files for %s: %v\n", topicName, err)
				continue
			}

			for _, pFile := range partitionFiles {
				// Extract partition number from filename
				base := filepath.Base(pFile)
				var partition int
				_, err := fmt.Sscanf(base, "partition-%d.log", &partition)
				if err != nil {
					fmt.Printf("[BATCHER] Warning: failed to parse partition from %s: %v\n", base, err)
					continue
				}

				logFile := pFile
				idxFile := strings.TrimSuffix(pFile, ".log") + ".idx"

				index, err := storage.LoadIndex(idxFile)
				if err != nil {
					fmt.Printf("[BATCHER] Warning: failed to load index for %s partition %d: %v\n", topicName, partition, err)
					index = &storage.OffsetIndex{Positions: []int64{}}
				}

				batch := &TopicBatch{
					buffer:  make([]message.Message, 0, tb.maxSize),
					index:   index,
					logFile: logFile,
					idxFile: idxFile,
				}

				// Use consistent partition key format: topic::partition
				partitionKey := fmt.Sprintf("%s::%d", topicName, partition)
				tb.batches[partitionKey] = batch

				fmt.Printf("[BATCHER] Discovered topic %s partition %d (messages: %d)\n",
					topicName, partition, len(index.Positions))
			}
		}
	}

	return nil
}

// Stop signals shutdown and waits for final flush.
func (tb *TopicBatcher) Stop() {
	tb.mtx.Lock()
	select {
	case <-tb.quit:
		// Already stopped
		tb.mtx.Unlock()
		return
	default:
		close(tb.quit)
	}
	tb.mtx.Unlock()

	// Wait for background goroutine to finish (without holding the mutex)
	tb.wg.Wait()
}

// createTopicPartitionBatch creates or loads a TopicBatch for a specific partition.
func (tb *TopicBatcher) createTopicPartitionBatch(topic string, partition int) *TopicBatch {
	// Ensure topic directory exists
	if err := storage.EnsureTopicDirectory(tb.baseDir, topic); err != nil {
		fmt.Printf("[BATCHER] Warning: failed to create topic directory for %s: %v\n", topic, err)
	}

	// Get partition-specific file paths
	logFile, idxFile := storage.GetPartitionedPaths(tb.baseDir, topic, partition)

	index, err := storage.LoadIndex(idxFile)
	if err != nil {
		fmt.Printf("[BATCHER] Warning: failed to load index for %s partition %d: %v\n", topic, partition, err)
		index = &storage.OffsetIndex{Positions: []int64{}}
	}

	// fmt.Printf("[BATCHER] Created batch for topic %s partition %d\n", topic, partition)

	return &TopicBatch{
		buffer:  make([]message.Message, 0, tb.maxSize),
		index:   index,
		logFile: logFile,
		idxFile: idxFile,
	}
}

// AddMessage adds a message to its topic batch. Triggers flush when full.
func (tb *TopicBatcher) AddMessage(msg message.Message) error {
	// Get partition count for this topic
	partitionCount := tb.config.GetTopicPartitions(msg.Topic)

	// Determine which partition this message goes to
	partition := storage.GetPartition(msg.Key, partitionCount)

	// Create a consistent partition key for batching: topic::partition
	partitionKey := fmt.Sprintf("%s::%d", msg.Topic, partition)

	// Debug logging (disabled for performance)
	// fmt.Printf("[BATCHER] Message for topic '%s' with key '%s' -> partition %d/%d (batch key: %s)\n",
	//	msg.Topic, msg.Key, partition, partitionCount, partitionKey)

	// Cheap check
	tb.mtx.RLock()
	batch, exists := tb.batches[partitionKey]
	tb.mtx.RUnlock()

	if !exists {
		tb.mtx.Lock()
		// Double-check after acquiring write lock
		batch, exists = tb.batches[partitionKey]
		if !exists {
			batch = tb.createTopicPartitionBatch(msg.Topic, partition)
			tb.batches[partitionKey] = batch
		}
		tb.mtx.Unlock()
	}

	// Now work with the specific partition batch
	batch.mtx.Lock()
	batch.buffer = append(batch.buffer, msg)
	shouldFlush := len(batch.buffer) >= int(tb.maxSize)
	batch.mtx.Unlock()

	// Debug logging disabled for performance
	// bufferSize := len(batch.buffer)
	// fmt.Printf("[BATCHER] Added message to %s (buffer size: %d/%d)\n",
	//	partitionKey, bufferSize, tb.maxSize)

	if shouldFlush {
		// fmt.Printf("[BATCHER] Triggering flush for %s (buffer full)\n", partitionKey)
		return tb.flushTopic(batch)
	}
	return nil
}

// FlushAll flushes all batches with pending messages.
func (tb *TopicBatcher) FlushAll() {
	// fmt.Printf("[BATCHER] FlushAll triggered\n")

	tb.mtx.RLock()
	snap := make([]*TopicBatch, 0, len(tb.batches))
	batchNames := make([]string, 0, len(tb.batches))
	for name, b := range tb.batches {
		b.mtx.Lock()
		if len(b.buffer) > 0 {
			snap = append(snap, b)
			batchNames = append(batchNames, name)
		}
		b.mtx.Unlock()
	}
	tb.mtx.RUnlock()

	if len(snap) > 0 {
		// fmt.Printf("[BATCHER] Flushing %d batches: %v\n", len(snap), batchNames)
	}

	for i, b := range snap {
		if err := tb.flushTopic(b); err != nil {
			fmt.Printf("[BATCHER] error flushing %s: %v\n", batchNames[i], err)
		}
	}
	tb.health.RecordFlush()
}

// flushTopic atomically flushes a topic batch using RAII pattern - RACE-FREE!
func (tb *TopicBatcher) flushTopic(batch *TopicBatch) error {
	batch.mtx.Lock()
	if len(batch.buffer) == 0 {
		batch.mtx.Unlock()
		return nil
	}

	// Create local copy and clear buffer atomically
	local := make([]message.Message, len(batch.buffer))
	copy(local, batch.buffer)
	batch.buffer = batch.buffer[:0]
	logFile := batch.logFile
	idxFile := batch.idxFile
	batch.mtx.Unlock()

	// fmt.Printf("[BATCHER] Flushing %d messages to %s\n", len(local), logFile)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	aggressiveConfig := retry.Config{
		MaxAttempts:   5,
		InitialDelay:  100 * time.Millisecond,
		MaxDelay:      10 * time.Second,
		BackoffFactor: 2.0,
	}

	// File is automatically acquired, protected from cleanup, and released!
	err := tb.pool.WithFile(logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, func(f *storage.File) error {
		// File is guaranteed to be available and protected from cleanup here!
		return retry.Retry(ctx, aggressiveConfig, func() error {
			// Use the file directly - no pool access needed, no race conditions!
			return storage.AppendMessagesWithFile(f, idxFile, local, batch.index)
		})
	})

	if err != nil {
		if obeliskErrors.IsRetryable(err) {
			// Re-queue messages for retryable errors
			batch.mtx.Lock()
			batch.buffer = append(local, batch.buffer...)
			batch.mtx.Unlock()
		} else {
			// Log permanent errors with proper formatting
			fmt.Printf("[BATCHER] CRITICAL: Dropping %d messages - %s\n", len(local), err.Error())
		}
		return err
	}

	// fmt.Printf("[BATCHER] Successfully flushed %d messages to %s\n", len(local), logFile)
	// File automatically released when WithFile callback exits
	return nil
}

// GetTopicStats returns buffered and persisted message counts.
// Note: This needs updating for partitioned topics
func (tb *TopicBatcher) GetTopicStats(topic string) (int, int64, error) {
	tb.mtx.RLock()
	defer tb.mtx.RUnlock()

	totalBuffered := 0
	totalPersisted := int64(0)
	found := false

	// Check all batches for this topic (including all partitions)
	for key, batch := range tb.batches {
		// Check if this batch belongs to the requested topic
		if key == topic || strings.HasPrefix(key, topic+"::") {
			found = true
			batch.mtx.Lock()
			totalBuffered += len(batch.buffer)
			totalPersisted += int64(len(batch.index.Positions))
			batch.mtx.Unlock()
		}
	}

	if !found {
		return 0, 0, obeliskErrors.NewPermanentError("get_topic_stats", "topic not found",
			fmt.Errorf("topic: %s", topic))
	}

	return totalBuffered, totalPersisted, nil
}
