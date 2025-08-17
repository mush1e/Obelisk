package health

import (
	"sync/atomic"
	"time"
)

type HealthTracker struct {
	initialized      atomic.Bool
	startTime        time.Time
	recentBufferOps  *ringBuffer
	recentBatcherOps *ringBuffer
	batcherAlive     atomic.Bool
	lastFlushTime    atomic.Int64 // unix nano for atomic updates
}

// Constructor for NewHealthTracker
func NewHealthTracker() *HealthTracker {
	return &HealthTracker{
		startTime:        time.Now(),
		recentBufferOps:  newRingBuffer(100), // Track last 100 operations
		recentBatcherOps: newRingBuffer(100),
	}
}

// Called by BrokerService on each buffer publish
func (t *HealthTracker) RecordBufferPublish(success bool) {
	t.recentBufferOps.Add(success)
}

// Called by BrokerService on each batcher publish
func (t *HealthTracker) RecordBatcherPublish(success bool) {
	t.recentBatcherOps.Add(success)
}

// Called by batcher after each flush
func (t *HealthTracker) RecordFlush() {
	t.lastFlushTime.Store(time.Now().UnixNano())
	t.batcherAlive.Store(true)
}

func (t *HealthTracker) SetInitialized() {
	t.initialized.Store(true)
}

// GetBufferHealth returns if percentage of success is >= 95%
func (t *HealthTracker) GetBufferHealth() bool {
	bufferSuccessRate := t.recentBufferOps.SuccessRate()
	if bufferSuccessRate >= 0.95 {
		return true
	}
	return false
}

// GetBatcherHealth returns if percentage of success is >= 95%
func (t *HealthTracker) GetBatcherHealth() bool {
	batcherSuccessRate := t.recentBatcherOps.SuccessRate()
	if batcherSuccessRate >= 0.95 {
		return true
	}
	return false
}
