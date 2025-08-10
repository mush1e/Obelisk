package batch

// This package provides batching functionality for Obelisk messages.
// It groups messages by topic and periodically flushes them to persistent storage
// for efficient disk I/O operations. The batcher ensures messages are not lost
// by maintaining in-memory buffers and flushing based on size or time thresholds.

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/mush1e/obelisk/internal/message"
	"github.com/mush1e/obelisk/internal/storage"
)

// TopicBatcher manages batching and persistence of messages across multiple topics.
// It maintains separate batches for each topic and flushes them to disk based on
// configurable size and time thresholds.
type TopicBatcher struct {
	batches map[string]*TopicBatch // Map of topic name to its batch
	baseDir string                 // Base directory for storing topic files
	maxSize uint32                 // Maximum number of messages before flush
	maxWait time.Duration          // Maximum time to wait before flush
	quit    chan struct{}          // Channel to signal shutdown
	mtx     sync.RWMutex           // Protects batches map
}

// TopicBatch represents a batch of messages for a specific topic.
// It contains an in-memory buffer and references to the topic's log and index files.
type TopicBatch struct {
	buffer  []message.Message    // In-memory buffer for pending messages
	index   *storage.OffsetIndex // Index mapping offsets to byte positions
	logFile string               // Path to the topic's log file
	idxFile string               // Path to the topic's index file
	mtx     sync.RWMutex         // Protects buffer operations
}

// NewTopicBatcher creates a new TopicBatcher with the specified configuration.
// maxSize determines how many messages to batch before flushing to disk.
// maxWait determines the maximum time to wait before flushing incomplete batches.
func NewTopicBatcher(baseDir string, maxSize uint32, maxWait time.Duration) *TopicBatcher {
	return &TopicBatcher{
		batches: make(map[string]*TopicBatch),
		baseDir: baseDir,
		maxSize: maxSize,
		maxWait: maxWait,
		quit:    make(chan struct{}),
	}
}

// Start initializes the batcher by creating the base directory and starting
// the background flush ticker that periodically flushes pending batches.
func (tb *TopicBatcher) Start() error {
	if err := os.MkdirAll(tb.baseDir, 0755); err != nil {
		return errors.New("failed to create base directory " + err.Error())
	}

	// Start background ticker to flush batches periodically
	ticker := time.NewTicker(tb.maxWait)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				tb.FlushAll() // Flush all topics on timer
			case <-tb.quit:
				tb.FlushAll() // Final flush on shutdown
				return
			}
		}
	}()
	return nil
}

// FlushAll flushes all topic batches that have pending messages to disk.
// This is called periodically by the background ticker and during shutdown.
func (tb *TopicBatcher) FlushAll() {
	tb.mtx.RLock()
	defer tb.mtx.RUnlock()

	for topic, batch := range tb.batches {
		if len(batch.buffer) == 0 {
			continue // Skip empty batches
		}
		if err := tb.flushTopic(tb.batches[topic]); err != nil {
			fmt.Printf("[BATCHER] error flushing topic %s: %v\n", topic, err)
		}
	}
}

// flushTopic writes all buffered messages for a topic to persistent storage.
// It appends messages to the log file and updates the index for fast lookups.
func (tb *TopicBatcher) flushTopic(batch *TopicBatch) error {
	if len(batch.buffer) == 0 {
		return nil
	}

	batch.mtx.Lock()
	defer batch.mtx.Unlock()

	fmt.Printf("[BATCHER] Flushing %d messages for topic to disk...\n", len(batch.buffer))
	start := time.Now()

	// Write all buffered messages to disk with indexing
	for _, msg := range batch.buffer {
		if err := storage.AppendMessage(batch.logFile, batch.idxFile, msg, batch.index); err != nil {
			return fmt.Errorf("failed to append message: %w", err)
		}
	}

	duration := time.Since(start)
	fmt.Printf("[BATCHER] Flushed %d messages in %v\n", len(batch.buffer), duration)

	// Clear buffer but preserve capacity for reuse
	batch.buffer = batch.buffer[:0]
	return nil
}

// createTopicBatch initializes a new TopicBatch for the given topic.
// It creates the necessary file paths and loads any existing index.
func (tb *TopicBatcher) createTopicBatch(topic string) *TopicBatch {
	logFile := filepath.Join(tb.baseDir, topic+".log")
	idxFile := filepath.Join(tb.baseDir, topic+".idx")

	// Load existing index from disk or create new empty index
	index, err := storage.LoadIndex(idxFile)
	if err != nil {
		fmt.Printf("[BATCHER] Warning: failed to load index for topic %s: %v\n", topic, err)
		index = &storage.OffsetIndex{Positions: []int64{}}
	}

	return &TopicBatch{
		buffer:  make([]message.Message, 0, tb.maxSize), // Pre-allocate with capacity
		index:   index,
		logFile: logFile,
		idxFile: idxFile,
	}
}

// AddMessage adds a message to the appropriate topic batch.
// If the batch doesn't exist, it creates one. If the batch reaches maxSize,
// it immediately flushes the batch to disk.
func (tb *TopicBatcher) AddMessage(msg message.Message) error {
	tb.mtx.Lock()
	batch, exists := tb.batches[msg.Topic]
	if !exists {
		batch = tb.createTopicBatch(msg.Topic)
		tb.batches[msg.Topic] = batch
	}
	tb.mtx.Unlock()

	batch.mtx.Lock()
	defer batch.mtx.Unlock()

	batch.buffer = append(batch.buffer, msg)

	// Flush immediately if batch is full
	if len(batch.buffer) >= int(tb.maxSize) {
		return tb.flushTopic(batch)
	}
	return nil
}

// Stop signals the batcher to stop and performs a final flush of all batches.
func (tb *TopicBatcher) Stop() {
	close(tb.quit)
}

// GetTopicStats returns statistics for a specific topic including buffered
// message count and total persisted message count.
func (tb *TopicBatcher) GetTopicStats(topic string) (int, int64, error) {
	tb.mtx.RLock()
	batch, exists := tb.batches[topic]
	tb.mtx.RUnlock()

	if !exists {
		return 0, 0, fmt.Errorf("topic %s not found", topic)
	}

	batch.mtx.Lock()
	defer batch.mtx.Unlock()

	bufferedCount := len(batch.buffer)                 // Messages waiting to be flushed
	totalMessages := int64(len(batch.index.Positions)) // Messages already persisted

	return bufferedCount, totalMessages, nil
}
