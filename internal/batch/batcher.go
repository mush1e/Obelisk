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

// TopicBatcher manages batching per topic and flushes them to disk.
type TopicBatcher struct {
	batches map[string]*TopicBatch
	baseDir string
	maxSize uint32
	maxWait time.Duration
	quit    chan struct{}
	mtx     sync.RWMutex
	wg      sync.WaitGroup
}

// TopicBatch holds in-memory buffered messages and storage metadata
type TopicBatch struct {
	buffer  []message.Message
	index   *storage.OffsetIndex
	logFile string
	idxFile string
	mtx     sync.Mutex
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

// Stop signals shutdown and waits for background goroutines to finish
func (tb *TopicBatcher) Stop() {
	close(tb.quit)
	tb.wg.Wait()
}

// createTopicBatch creates or loads a TopicBatch for a topic
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

// AddMessage adds a message to its topic batch. If full, triggers flush.
func (tb *TopicBatcher) AddMessage(msg message.Message) error {
	// fast path: acquire or create batch
	tb.mtx.Lock()
	batch, exists := tb.batches[msg.Topic]
	if !exists {
		batch = tb.createTopicBatch(msg.Topic)
		tb.batches[msg.Topic] = batch
	}
	tb.mtx.Unlock()

	batch.mtx.Lock()
	batch.buffer = append(batch.buffer, msg)
	bufLen := len(batch.buffer)
	batch.mtx.Unlock()

	if bufLen >= int(tb.maxSize) {
		return tb.flushTopic(batch)
	}
	return nil
}

// FlushAll snapshots batches that need flushing and flushes them without holding the global lock
func (tb *TopicBatcher) FlushAll() {
	// snapshot
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

// flushTopic steals buffer and writes to storage. On failure re-queues messages.
func (tb *TopicBatcher) flushTopic(batch *TopicBatch) error {
	// steal the buffer
	batch.mtx.Lock()
	if len(batch.buffer) == 0 {
		batch.mtx.Unlock()
		return nil
	}
	local := make([]message.Message, len(batch.buffer))
	copy(local, batch.buffer)
	batch.buffer = batch.buffer[:0]
	batch.mtx.Unlock()

	// attempt write
	if err := storage.AppendMessages(batch.logFile, batch.idxFile, local, batch.index); err != nil {
		// requeue at front preserving order
		batch.mtx.Lock()
		batch.buffer = append(local, batch.buffer...)
		batch.mtx.Unlock()
		return err
	}
	return nil
}

// GetTopicStats returns buffered and persisted counts
func (tb *TopicBatcher) GetTopicStats(topic string) (int, int64, error) {
	tb.mtx.RLock()
	batch, exists := tb.batches[topic]
	tb.mtx.RUnlock()
	if !exists {
		return 0, 0, fmt.Errorf("topic %s not found", topic)
	}
	batch.mtx.Lock()
	buffered := len(batch.buffer)
	total := int64(len(batch.index.Positions))
	batch.mtx.Unlock()
	return buffered, total, nil
}
