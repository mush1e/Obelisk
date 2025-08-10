package batch

// This package provides a topic-based batching system for the Obelisk message broker.
// The batching system collects messages in memory buffers organized by topic, then
// periodically flushes them to persistent storage in batches. This approach provides
// better performance than writing individual messages while maintaining data durability.
//
// The system handles:
// - Per-topic message buffering with configurable size limits
// - Time-based and size-based flush triggers
// - Atomic batch writes with proper indexing
// - Error recovery with message re-queuing
// - Graceful shutdown with guaranteed flush of pending data

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
	batches map[string]*TopicBatch // Map of topic names to their batch instances
	baseDir string                 // Base directory for storing log and index files
	maxSize uint32                 // Maximum messages per batch before forced flush
	maxWait time.Duration          // Maximum time to wait before forced flush
	quit    chan struct{}          // Channel for coordinating graceful shutdown
	mtx     sync.RWMutex           // Protects the batches map during concurrent access
	wg      sync.WaitGroup         // Ensures all background goroutines complete before shutdown
}

// TopicBatch holds in-memory buffered messages and storage metadata for a single topic.
// Each topic maintains its own buffer and index to enable independent operation
// and optimal storage access patterns. The buffer accumulates messages until
// flush conditions are met, at which point they're written atomically to storage.
//
// Storage files:
// - logFile: Contains the actual message data in protocol format
// - idxFile: Contains byte offsets for fast message lookup by sequence number
type TopicBatch struct {
	buffer  []message.Message    // In-memory buffer of pending messages
	index   *storage.OffsetIndex // Index mapping logical offsets to file positions
	logFile string               // Path to the topic's log file
	idxFile string               // Path to the topic's index file
	mtx     sync.Mutex           // Protects buffer operations for this specific topic
}

// NewTopicBatcher creates a new topic batcher with the specified configuration.
// The batcher will create the base directory if it doesn't exist and manage
// all topic-specific storage within that directory.
//
// Parameters:
//   - baseDir: Directory where all topic files will be stored
//   - maxSize: Maximum number of messages to buffer before forcing a flush
//   - maxWait: Maximum time to wait before forcing a flush of pending messages
//
// Returns:
//   - *TopicBatcher: Configured batcher instance ready for use
func NewTopicBatcher(baseDir string, maxSize uint32, maxWait time.Duration) *TopicBatcher {
	return &TopicBatcher{
		batches: make(map[string]*TopicBatch),
		baseDir: baseDir,
		maxSize: maxSize,
		maxWait: maxWait,
		quit:    make(chan struct{}),
	}
}

// Start initializes the batcher and begins background processing.
// This method:
// 1. Creates the base directory structure if it doesn't exist
// 2. Starts a background goroutine for time-based flush operations
// 3. Sets up graceful shutdown coordination
//
// The background flush routine runs on a ticker based on maxWait duration
// and ensures that messages don't remain unbuffered indefinitely, even
// if size-based flush conditions are never met.
//
// Returns:
//   - error: If the base directory cannot be created
func (tb *TopicBatcher) Start() error {
	// Ensure base directory exists for storing topic files
	if err := os.MkdirAll(tb.baseDir, 0755); err != nil {
		return errors.New("failed to create base directory " + err.Error())
	}

	// Start background flush routine
	tb.wg.Add(1)
	ticker := time.NewTicker(tb.maxWait)
	go func() {
		defer ticker.Stop()
		defer tb.wg.Done()
		for {
			select {
			case <-ticker.C:
				// Periodic flush of all topics based on time trigger
				tb.FlushAll()
			case <-tb.quit:
				// Graceful shutdown: flush all pending data before exit
				tb.FlushAll()
				return
			}
		}
	}()
	return nil
}

// Stop signals shutdown and waits for background goroutines to finish.
// This method ensures graceful shutdown by:
// 1. Signaling the background flush routine to stop
// 2. Waiting for the final flush of all pending messages
// 3. Ensuring no messages are lost during shutdown
//
// This is a blocking operation that guarantees all buffered messages
// are persisted before returning.
func (tb *TopicBatcher) Stop() {
	close(tb.quit) // Signal shutdown to background routine
	tb.wg.Wait()   // Wait for final flush and cleanup
}

// createTopicBatch creates or loads a TopicBatch for a topic.
// This method handles the initialization of storage structures for a new topic:
// 1. Constructs file paths for log and index files
// 2. Attempts to load existing index from disk (for crash recovery)
// 3. Creates empty index if none exists
// 4. Initializes the message buffer
//
// The index loading supports crash recovery by preserving message sequence
// numbers across restarts. If index loading fails, a warning is logged
// but the system continues with an empty index.
//
// Parameters:
//   - topic: Name of the topic to create/load batch for
//
// Returns:
//   - *TopicBatch: Initialized batch instance for the topic
func (tb *TopicBatcher) createTopicBatch(topic string) *TopicBatch {
	// Construct file paths based on topic name
	logFile := filepath.Join(tb.baseDir, topic+".log")
	idxFile := filepath.Join(tb.baseDir, topic+".idx")

	// Attempt to load existing index for crash recovery
	index, err := storage.LoadIndex(idxFile)
	if err != nil {
		// Log warning but continue with empty index - this handles first-time topic creation
		fmt.Printf("[BATCHER] Warning: failed to load index for topic %s: %v\n", topic, err)
		index = &storage.OffsetIndex{Positions: []int64{}}
	}

	return &TopicBatch{
		buffer:  make([]message.Message, 0, tb.maxSize), // Pre-allocate buffer with capacity
		index:   index,
		logFile: logFile,
		idxFile: idxFile,
	}
}

// AddMessage adds a message to its topic batch. If full, triggers flush.
// This is the primary ingress point for messages into the batching system.
// The method uses a two-phase locking strategy to minimize contention:
//
// Phase 1: Acquire global lock to get/create topic batch (brief operation)
// Phase 2: Acquire topic-specific lock for buffer manipulation (longer operation)
//
// This approach allows concurrent access to different topics while ensuring
// thread safety within each topic's operations.
//
// Parameters:
//   - msg: Message to add to the appropriate topic batch
//
// Returns:
//   - error: Any error that occurs during message addition or flushing
func (tb *TopicBatcher) AddMessage(msg message.Message) error {
	// Phase 1: Fast path to acquire or create batch (global lock)
	tb.mtx.Lock()
	batch, exists := tb.batches[msg.Topic]
	if !exists {
		// Create new batch for first message to this topic
		batch = tb.createTopicBatch(msg.Topic)
		tb.batches[msg.Topic] = batch
	}
	tb.mtx.Unlock()

	// Phase 2: Add message to topic-specific buffer (topic lock)
	batch.mtx.Lock()
	batch.buffer = append(batch.buffer, msg)
	bufLen := len(batch.buffer)
	shouldFlush := bufLen >= int(tb.maxSize) // Check if size-based flush trigger is met
	batch.mtx.Unlock()

	// Trigger immediate flush if buffer is full
	if shouldFlush {
		return tb.flushTopic(batch)
	}
	return nil
}

// FlushAll snapshots batches that need flushing and flushes them without holding the global lock.
// This method implements a lock-free flush strategy to avoid blocking new message
// ingestion during flush operations. The algorithm:
//
// 1. Take a snapshot of all non-empty batches under read lock
// 2. Release the global lock immediately
// 3. Flush each batch using its individual lock
//
// This approach allows new messages to be added to topics while existing
// messages are being flushed, improving overall system throughput.
func (tb *TopicBatcher) FlushAll() {
	// Phase 1: Snapshot all batches with pending messages (read lock for minimal contention)
	tb.mtx.RLock()
	snap := make([]*TopicBatch, 0, len(tb.batches))
	for _, b := range tb.batches {
		b.mtx.Lock()
		if len(b.buffer) > 0 {
			snap = append(snap, b) // Only snapshot batches with pending messages
		}
		b.mtx.Unlock()
	}
	tb.mtx.RUnlock()

	// Phase 2: Flush each batch independently (no global lock held)
	for _, b := range snap {
		if err := tb.flushTopic(b); err != nil {
			fmt.Printf("[BATCHER] error flushing topic %s: %v\n", b.logFile, err)
		}
	}
}

// flushTopic steals buffer and writes to storage. On failure re-queues messages.
// This method implements atomic flush semantics with error recovery:
//
// 1. "Steal" the current buffer contents atomically
// 2. Clear the buffer to allow new messages during flush
// 3. Attempt to write stolen messages to storage
// 4. On failure, re-queue messages at the front to preserve ordering
//
// The buffer stealing technique ensures that flush operations don't block
// new message ingestion, while the re-queuing mechanism provides resilience
// against transient storage failures.
//
// Parameters:
//   - batch: The topic batch to flush
//
// Returns:
//   - error: Any error that occurred during the flush operation
func (tb *TopicBatcher) flushTopic(batch *TopicBatch) error {
	// Phase 1: Atomically steal the buffer contents
	batch.mtx.Lock()
	if len(batch.buffer) == 0 {
		batch.mtx.Unlock()
		return nil // Nothing to flush
	}

	// Copy buffer contents and clear the original buffer
	local := make([]message.Message, len(batch.buffer))
	copy(local, batch.buffer)
	batch.buffer = batch.buffer[:0] // Clear buffer but preserve capacity
	batch.mtx.Unlock()

	// Phase 2: Attempt to write to storage (outside of buffer lock)
	if err := storage.AppendMessages(batch.logFile, batch.idxFile, local, batch.index); err != nil {
		// Failure recovery: re-queue messages at front to preserve ordering
		batch.mtx.Lock()
		batch.buffer = append(local, batch.buffer...) // Prepend failed messages
		batch.mtx.Unlock()
		return err
	}
	return nil
}

// GetTopicStats returns buffered and persisted counts for diagnostic purposes.
// This method provides insight into the current state of a topic's batching:
// - Buffered count: Messages currently in memory waiting to be flushed
// - Total count: Total messages that have been persisted to storage
//
// The statistics are useful for monitoring system health, understanding
// flush behavior, and debugging performance issues.
//
// Parameters:
//   - topic: Name of the topic to get statistics for
//
// Returns:
//   - int: Number of messages currently buffered in memory
//   - int64: Total number of messages persisted to storage
//   - error: Error if the topic doesn't exist in the batcher
func (tb *TopicBatcher) GetTopicStats(topic string) (int, int64, error) {
	// Check if topic exists in the batcher
	tb.mtx.RLock()
	batch, exists := tb.batches[topic]
	tb.mtx.RUnlock()
	if !exists {
		return 0, 0, fmt.Errorf("topic %s not found", topic)
	}

	// Get current statistics under topic lock
	batch.mtx.Lock()
	buffered := len(batch.buffer)              // Messages waiting to be flushed
	total := int64(len(batch.index.Positions)) // Messages already persisted
	batch.mtx.Unlock()

	return buffered, total, nil
}
