package batch

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

type TopicBatcher struct {
	batches map[string]*TopicBatch
	baseDir string
	maxSize uint32
	maxWait time.Duration
	quit    chan struct{}
	mtx     sync.RWMutex
}

type TopicBatch struct {
	buffer  []message.Message
	index   *storage.OffsetIndex
	logFile string
	idxFile string
	mtx     sync.RWMutex
}

func NewTopicBatcher(baseDir string, maxSize uint32, maxWait time.Duration) *TopicBatcher {
	return &TopicBatcher{
		batches: make(map[string]*TopicBatch),
		baseDir: baseDir,
		maxSize: maxSize,
		maxWait: maxWait,
		quit:    make(chan struct{}),
	}
}

func (tb *TopicBatcher) Start() error {
	if err := os.MkdirAll(tb.baseDir, 0755); err != nil {
		return errors.New("failed to create base directory " + err.Error())
	}
	ticker := time.NewTicker(tb.maxWait)
	go func() {
		defer ticker.Stop()
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

func (tb *TopicBatcher) FlushAll() {
	tb.mtx.RLock()
	defer tb.mtx.RUnlock()

	for topic, batch := range tb.batches {
		if len(batch.buffer) == 0 {
			continue
		}
		if err := tb.flushTopic(tb.batches[topic]); err != nil {
			fmt.Printf("[BATCHER] error flushing topic %s: %v\n", topic, err)
		}

	}
}

func (tb *TopicBatcher) flushTopic(batch *TopicBatch) error {
	if len(batch.buffer) == 0 {
		return nil
	}

	batch.mtx.Lock()
	defer batch.mtx.Unlock()

	fmt.Printf("[BATCHER] Flushing %d messages for topic to disk...\n", len(batch.buffer))
	start := time.Now()

	// Flush all messages in the batch
	for _, msg := range batch.buffer {
		if err := storage.AppendMessage(batch.logFile, batch.idxFile, msg, batch.index); err != nil {
			return fmt.Errorf("failed to append message: %w", err)
		}
	}

	duration := time.Since(start)
	fmt.Printf("[BATCHER] Flushed %d messages in %v\n", len(batch.buffer), duration)

	// Clear buffer but keep capacity
	batch.buffer = batch.buffer[:0]
	return nil
}

func (tb *TopicBatcher) createTopicBatch(topic string) *TopicBatch {
	logFile := filepath.Join(tb.baseDir, topic+".log")
	idxFile := filepath.Join(tb.baseDir, topic+".idx")

	// Load existing index or create new one
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

	// Check if we need to flush this topic
	if len(batch.buffer) >= int(tb.maxSize) {
		return tb.flushTopic(batch)
	}
	return nil
}

func (tb *TopicBatcher) Stop() {
	close(tb.quit)
}

func (tb *TopicBatcher) GetTopicStats(topic string) (int, int64, error) {
	tb.mtx.RLock()
	batch, exists := tb.batches[topic]
	tb.mtx.RUnlock()

	if !exists {
		return 0, 0, fmt.Errorf("topic %s not found", topic)
	}

	batch.mtx.Lock()
	defer batch.mtx.Unlock()

	bufferedCount := len(batch.buffer)
	totalMessages := int64(len(batch.index.Positions))

	return bufferedCount, totalMessages, nil
}
