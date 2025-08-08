package batch

import (
	"sync"
	"time"

	"github.com/mush1e/obelisk/internal/message"
	"github.com/mush1e/obelisk/internal/storage"
)

// This package provides a batcher for messages.
// It is used to batch messages together before sending them to the server.
// This avoids doing unnecessary file writes.

type Batcher struct {
	buffer  []message.Message
	maxSize uint32
	maxWait time.Duration
	logFile string
	quit    chan struct{}
	mtx     sync.RWMutex
}

func NewBatcher(logFile string, maxSize int, maxWait time.Duration) *Batcher {
	return &Batcher{
		buffer:  make([]message.Message, 0, maxSize),
		maxSize: uint32(maxSize),
		maxWait: maxWait,
		logFile: logFile,
		quit:    make(chan struct{}),
	}
}

func (b *Batcher) AddMessage(msg message.Message) error {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	b.buffer = append(b.buffer, msg)
	if uint32(len(b.buffer)) >= b.maxSize {
		return b.flush()
	}
	return nil
}

func (b *Batcher) Start() {
	ticker := time.NewTicker(b.maxWait)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				b.mtx.Lock()
				if len(b.buffer) > 0 {
					_ = b.flush()
				}
				b.mtx.Unlock()
			case <-b.quit:
				// final flush
				b.mtx.Lock()
				if len(b.buffer) > 0 {
					_ = b.flush()
				}
				b.mtx.Unlock()
				return
			}
		}
	}()
}

func (b *Batcher) Stop() {
	close(b.quit)
}

func (b *Batcher) flush() error {
	for _, msg := range b.buffer {
		if err := storage.AppendMessage(b.logFile, msg); err != nil {
			return err
		}
	}
	// reset slice but keep capacity
	b.buffer = b.buffer[:0]
	return nil
}
