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
	quit    chan struct{}
	mtx     sync.RWMutex
	wg      sync.WaitGroup
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
func NewTopicBatcher(baseDir string, maxSize uint32, maxWait time.Duration, pool *storage.FilePool) *TopicBatcher {
	return &TopicBatcher{
		batches: make(map[string]*TopicBatch),
		baseDir: baseDir,
		maxSize: maxSize,
		maxWait: maxWait,
		pool:    pool,
		quit:    make(chan struct{}),
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

	files, err := filepath.Glob(filepath.Join(tb.baseDir, "*.log"))
	if err != nil {
		return err
	}

	tb.mtx.Lock()
	defer tb.mtx.Unlock()

	for _, logFile := range files {
		base := filepath.Base(logFile)
		topic := strings.TrimSuffix(base, ".log")

		if _, exists := tb.batches[topic]; exists {
			continue
		}

		idxFile := filepath.Join(tb.baseDir, topic+".idx")
		index, err := storage.LoadIndex(idxFile)
		if err != nil {
			fmt.Printf("[BATCHER] Warning: failed to load index for topic %s: %v\n", topic, err)
			index = &storage.OffsetIndex{Positions: []int64{}}
		}

		batch := &TopicBatch{
			buffer:  make([]message.Message, 0, tb.maxSize),
			index:   index,
			logFile: logFile,
			idxFile: idxFile,
		}

		tb.batches[topic] = batch
		fmt.Printf("[BATCHER] Discovered existing topic: %s (messages: %d)\n", topic, len(index.Positions))
	}
	return nil
}

// Stop signals shutdown and waits for final flush.
func (tb *TopicBatcher) Stop() {
	close(tb.quit)
	tb.wg.Wait()
}

// createTopicBatch creates or loads a TopicBatch for a topic.
func (tb *TopicBatcher) createTopicBatch(topic string) *TopicBatch {

	logFile := filepath.Join(tb.baseDir, topic+".log")
	idxFile := filepath.Join(tb.baseDir, topic+".idx")

	index, err := storage.LoadIndex(idxFile)
	if err != nil {

		fmt.Printf("[BATCHER] Warning: failed to load index for topic %s: %v\n", topic, err)
		index = &storage.OffsetIndex{Positions: []int64{}}
	}

	return &TopicBatch{
		buffer:  make([]message.Message, 0, tb.maxSize),
		index:   index,
		logFile: logFile,
		idxFile: idxFile,
	}
}

// AddMessage adds a message to its topic batch. Triggers flush when full.
func (tb *TopicBatcher) AddMessage(msg message.Message) error {
	// Cheap check
	tb.mtx.RLock()
	batch, exists := tb.batches[msg.Topic]
	tb.mtx.RUnlock()

	if !exists {
		tb.mtx.Lock()
		// Double-check after acquiring write lock since it might take time to get the lock
		batch, exists = tb.batches[msg.Topic]
		if !exists {
			batch = tb.createTopicBatch(msg.Topic)
			tb.batches[msg.Topic] = batch
		}
		tb.mtx.Unlock()
	}

	// Now work with the specific topic batch
	batch.mtx.Lock()
	batch.buffer = append(batch.buffer, msg)
	shouldFlush := len(batch.buffer) >= int(tb.maxSize)
	batch.mtx.Unlock()

	if shouldFlush {
		return tb.flushTopic(batch)
	}
	return nil
}

// FlushAll flushes all batches with pending messages.
func (tb *TopicBatcher) FlushAll() {

	tb.mtx.RLock()
	snap := make([]*TopicBatch, 0, len(tb.batches))
	for _, b := range tb.batches {
		b.mtx.Lock()
		if len(b.buffer) > 0 {
			snap = append(snap, b)
		}
		b.mtx.Unlock()
	}
	tb.mtx.RUnlock()

	for _, b := range snap {
		if err := tb.flushTopic(b); err != nil {
			fmt.Printf("[BATCHER] error flushing topic %s: %v\n", b.logFile, err)
		}
	}
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
	batch.mtx.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	aggressiveConfig := retry.Config{
		MaxAttempts:   5,
		InitialDelay:  100 * time.Millisecond,
		MaxDelay:      10 * time.Second,
		BackoffFactor: 2.0,
	}

	// File is automatically acquired, protected from cleanup, and released!
	err := tb.pool.WithFile(batch.logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, func(f *storage.File) error {
		// File is guaranteed to be available and protected from cleanup here!
		return retry.Retry(ctx, aggressiveConfig, func() error {
			// Use the file directly - no pool access needed, no race conditions!
			return storage.AppendMessagesWithFile(f, batch.idxFile, local, batch.index)
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

	// File automatically released when WithFile callback exits
	return nil
}

// GetTopicStats returns buffered and persisted message counts.
func (tb *TopicBatcher) GetTopicStats(topic string) (int, int64, error) {

	tb.mtx.RLock()
	batch, exists := tb.batches[topic]
	tb.mtx.RUnlock()
	if !exists {
		return 0, 0, obeliskErrors.NewPermanentError("get_topic_stats", "topic not found",
			fmt.Errorf("topic: %s", topic))
	}

	batch.mtx.Lock()
	buffered := len(batch.buffer)
	total := int64(len(batch.index.Positions))
	batch.mtx.Unlock()

	return buffered, total, nil
}
