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

// GetOverallHealth returns the overall health status based on all components
func (t *HealthTracker) GetOverallHealth() string {
	bufferRate, bufferHealthy := t.GetBufferHealth()
	batcherRate, batcherHealthy := t.GetBatcherHealth()

	// Check if batcher is alive (has flushed recently)
	lastFlush := time.Unix(0, t.lastFlushTime.Load())
	batcherAlive := time.Since(lastFlush) < 30*time.Second

	if !bufferHealthy || !batcherHealthy || !batcherAlive {
		return "degraded"
	}

	if bufferRate < 0.8 || batcherRate < 0.8 {
		return "unhealthy"
	}

	return "healthy"
}

// GetUptime returns the duration since the tracker was created
func (t *HealthTracker) GetUptime() time.Duration {
	return time.Since(t.startTime)
}

// IsInitialized returns whether the system has been marked as initialized
func (t *HealthTracker) IsInitialized() bool {
	return t.initialized.Load()
}

// GetLastFlushTime returns the last flush time as a time.Time
func (t *HealthTracker) GetLastFlushTime() time.Time {
	return time.Unix(0, t.lastFlushTime.Load())
}

// GetBufferOperationCount returns the number of buffer operations tracked
func (t *HealthTracker) GetBufferOperationCount() int {
	return t.recentBufferOps.Count()
}

// GetBatcherOperationCount returns the number of batcher operations tracked
func (t *HealthTracker) GetBatcherOperationCount() int {
	return t.recentBatcherOps.Count()
}
