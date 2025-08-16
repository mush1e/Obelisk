package health

import (
	"sync"
	"sync/atomic"
	"time"
)

type HealthTracker struct {
	mtx           sync.RWMutex
	initialized   atomic.Bool
	startTime     time.Time
	recentOps     *ringBuffer
	batcherAlive  atomic.Bool
	lastFlushTime atomic.Int64
}

func NewHealthTracker() *HealthTracker {
	return &HealthTracker{
		startTime: time.Now(),
		recentOps: newRingBuffer(100), // Track last 100 operations
	}
}

// Called by BrokerService on each publish
func (t *HealthTracker) RecordPublish(success bool) {
	t.recentOps.Add(success)
}

// Called by batcher after each flush
func (t *HealthTracker) RecordFlush() {
	t.lastFlushTime.Store(time.Now().UnixNano())
	t.batcherAlive.Store(true)
}

func (t *HealthTracker) SetInitialized() {
	t.initialized.Store(true)
}
