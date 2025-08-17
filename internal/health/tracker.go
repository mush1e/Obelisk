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

// GetBufferHealth returns rate of successful buffer ops and health
func (t *HealthTracker) GetBufferHealth() (rate float64, health bool) {
	rate = t.recentBufferOps.SuccessRate()
	health = rate >= 0.95
	return
}

// GetBatcherHealth returns rate of successful batcher ops and health
func (t *HealthTracker) GetBatcherHealth() (rate float64, health bool) {
	rate = t.recentBatcherOps.SuccessRate()
	health = rate >= 0.95
	return
}
