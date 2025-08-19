package batch

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mush1e/obelisk/internal/config"
	"github.com/mush1e/obelisk/internal/health"
	"github.com/mush1e/obelisk/internal/message"
	"github.com/mush1e/obelisk/internal/storage"
)

// TestBatcherRaceCondition_GetTopicStats tests the critical race condition
// where GetTopicStats reads batch.index.Positions without proper synchronization
// while the background flush routine is writing to it.
func TestBatcherRaceCondition_GetTopicStats(t *testing.T) {
	tempDir := t.TempDir()
	pool := storage.NewFilePool(time.Hour)
	defer pool.Stop()

	cfg := &config.Config{}
	cfg.Storage.DefaultPartitions = 2

	healthTracker := health.NewHealthTracker()
	// Small batch size to trigger frequent flushes
	batcher := NewTopicBatcher(tempDir, 5, 10*time.Millisecond, pool, healthTracker, cfg)

	if err := batcher.Start(); err != nil {
		t.Fatalf("Failed to start batcher: %v", err)
	}
	defer batcher.Stop()

	numGoroutines := 20
	messagesPerGoroutine := 50
	statsGoroutines := 10
	statsOperations := 100

	var wg sync.WaitGroup
	start := make(chan struct{})
	var totalMessages int32
	var totalStatsCalls int32

	// Start goroutines that add messages (will trigger frequent flushes)
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			<-start

			for j := 0; j < messagesPerGoroutine; j++ {
				topic := fmt.Sprintf("race-topic-%d", j%3)
				msg := message.Message{
					Timestamp: time.Now(),
					Topic:     topic,
					Key:       fmt.Sprintf("goroutine-%d-msg-%d", goroutineID, j),
					Value:     fmt.Sprintf("Message %d from goroutine %d", j, goroutineID),
				}

				if err := batcher.AddMessage(msg); err != nil {
					t.Errorf("Goroutine %d failed to add message: %v", goroutineID, err)
				} else {
					atomic.AddInt32(&totalMessages, 1)
				}

				// Small delay to allow flushes to happen
				time.Sleep(time.Microsecond)
			}
		}(i)
	}

	// Start goroutines that call GetTopicStats concurrently (the race condition)
	for i := 0; i < statsGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			<-start

			for j := 0; j < statsOperations; j++ {
				topic := fmt.Sprintf("race-topic-%d", j%3)

				buffered, persisted, err := batcher.GetTopicStats(topic)
				atomic.AddInt32(&totalStatsCalls, 1)

				if err != nil {
					// It's okay if topic doesn't exist yet
					continue
				}

				// Verify stats are reasonable
				if buffered < 0 || persisted < 0 {
					t.Errorf("Goroutine %d: negative stats for topic %s: buffered=%d, persisted=%d",
						goroutineID, topic, buffered, persisted)
				}

				// Small delay to increase chance of race condition
				time.Sleep(time.Nanosecond)
			}
		}(i)
	}

	close(start)
	wg.Wait()

	// Final flush
	batcher.FlushAll()

	// Verify final state
	if atomic.LoadInt32(&totalMessages) == 0 {
		t.Error("No messages were added during race condition test")
	}

	if atomic.LoadInt32(&totalStatsCalls) == 0 {
		t.Error("No stats calls were made during race condition test")
	}

	// Check final stats for all topics
	for i := 0; i < 3; i++ {
		topic := fmt.Sprintf("race-topic-%d", i)
		buffered, persisted, err := batcher.GetTopicStats(topic)
		if err != nil {
			t.Errorf("Failed to get final stats for topic %s: %v", topic, err)
			continue
		}

		total := buffered + int(persisted)
		if total == 0 {
			t.Errorf("Topic %s has no messages after race condition test", topic)
		}
	}
}

// TestBatcherRaceCondition_ConcurrentFlushAndStats tests concurrent flushing
// and stats retrieval to catch any remaining race conditions
func TestBatcherRaceCondition_ConcurrentFlushAndStats(t *testing.T) {
	tempDir := t.TempDir()
	pool := storage.NewFilePool(time.Hour)
	defer pool.Stop()

	cfg := &config.Config{}
	cfg.Storage.DefaultPartitions = 3

	healthTracker := health.NewHealthTracker()
	batcher := NewTopicBatcher(tempDir, 10, 5*time.Millisecond, pool, healthTracker, cfg)

	if err := batcher.Start(); err != nil {
		t.Fatalf("Failed to start batcher: %v", err)
	}
	defer batcher.Stop()

	numGoroutines := 15
	messagesPerGoroutine := 30
	flushGoroutines := 5
	flushOperations := 50

	var wg sync.WaitGroup
	start := make(chan struct{})
	var totalMessages int32
	var totalFlushes int32

	// Start goroutines that add messages
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			<-start

			for j := 0; j < messagesPerGoroutine; j++ {
				topic := fmt.Sprintf("flush-stats-topic-%d", j%4)
				msg := message.Message{
					Timestamp: time.Now(),
					Topic:     topic,
					Key:       fmt.Sprintf("goroutine-%d-msg-%d", goroutineID, j),
					Value:     fmt.Sprintf("Message %d from goroutine %d", j, goroutineID),
				}

				if err := batcher.AddMessage(msg); err != nil {
					t.Errorf("Goroutine %d failed to add message: %v", goroutineID, err)
				} else {
					atomic.AddInt32(&totalMessages, 1)
				}
			}
		}(i)
	}

	// Start goroutines that manually trigger flushes
	for i := 0; i < flushGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			<-start

			for j := 0; j < flushOperations; j++ {
				batcher.FlushAll()
				atomic.AddInt32(&totalFlushes, 1)

				// Get stats after each flush
				for k := 0; k < 4; k++ {
					topic := fmt.Sprintf("flush-stats-topic-%d", k)
					buffered, persisted, err := batcher.GetTopicStats(topic)
					if err != nil {
						// It's okay if topic doesn't exist yet
						continue
					}

					// Verify stats are reasonable
					if buffered < 0 || persisted < 0 {
						t.Errorf("Flush goroutine %d: negative stats for topic %s: buffered=%d, persisted=%d",
							goroutineID, topic, buffered, persisted)
					}
				}

				time.Sleep(time.Millisecond)
			}
		}(i)
	}

	close(start)
	wg.Wait()

	// Final flush
	batcher.FlushAll()

	// Verify final state
	if atomic.LoadInt32(&totalMessages) == 0 {
		t.Error("No messages were added during concurrent flush and stats test")
	}

	if atomic.LoadInt32(&totalFlushes) == 0 {
		t.Error("No manual flushes were performed during test")
	}

	// Check final stats for all topics
	for i := 0; i < 4; i++ {
		topic := fmt.Sprintf("flush-stats-topic-%d", i)
		buffered, persisted, err := batcher.GetTopicStats(topic)
		if err != nil {
			t.Errorf("Failed to get final stats for topic %s: %v", topic, err)
			continue
		}

		total := buffered + int(persisted)
		if total == 0 {
			t.Errorf("Topic %s has no messages after concurrent flush and stats test", topic)
		}
	}
}

// TestBatcherRaceCondition_StartStop tests race conditions during start/stop
func TestBatcherRaceCondition_StartStop(t *testing.T) {
	tempDir := t.TempDir()
	pool := storage.NewFilePool(time.Hour)
	defer pool.Stop()

	cfg := &config.Config{}
	cfg.Storage.DefaultPartitions = 2

	healthTracker := health.NewHealthTracker()
	batcher := NewTopicBatcher(tempDir, 20, 1*time.Millisecond, pool, healthTracker, cfg)

	numGoroutines := 10
	messagesPerGoroutine := 20
	startStopGoroutines := 5

	var wg sync.WaitGroup
	start := make(chan struct{})
	var totalMessages int32
	var totalStartStops int32

	// Start goroutines that add messages
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			<-start

			for j := 0; j < messagesPerGoroutine; j++ {
				msg := message.Message{
					Timestamp: time.Now(),
					Topic:     "start-stop-race-topic",
					Key:       fmt.Sprintf("goroutine-%d-msg-%d", goroutineID, j),
					Value:     fmt.Sprintf("Message %d from goroutine %d", j, goroutineID),
				}

				if err := batcher.AddMessage(msg); err != nil {
					// It's okay if batcher is stopped
					continue
				} else {
					atomic.AddInt32(&totalMessages, 1)
				}
			}
		}(i)
	}

	// Start goroutines that start/stop the batcher
	for i := 0; i < startStopGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			<-start

			for j := 0; j < 10; j++ {
				// Start the batcher
				if err := batcher.Start(); err != nil {
					// It's okay if already started
					continue
				}

				// Let it run for a bit
				time.Sleep(time.Millisecond)

				// Stop the batcher
				batcher.Stop()
				atomic.AddInt32(&totalStartStops, 1)

				time.Sleep(time.Millisecond)
			}
		}(i)
	}

	close(start)
	wg.Wait()

	// Final cleanup
	batcher.Stop()

	// Verify some operations happened
	if atomic.LoadInt32(&totalStartStops) == 0 {
		t.Error("No start/stop operations were performed during test")
	}
}

// TestBatcherRaceCondition_PartitionedTopics tests race conditions with partitioned topics
func TestBatcherRaceCondition_PartitionedTopics(t *testing.T) {
	tempDir := t.TempDir()
	pool := storage.NewFilePool(time.Hour)
	defer pool.Stop()

	cfg := &config.Config{}
	cfg.Storage.DefaultPartitions = 4
	cfg.Storage.Topics = []config.TopicConfig{
		{Name: "partitioned-race-topic", Partitions: 4},
	}

	healthTracker := health.NewHealthTracker()
	batcher := NewTopicBatcher(tempDir, 15, 2*time.Millisecond, pool, healthTracker, cfg)

	if err := batcher.Start(); err != nil {
		t.Fatalf("Failed to start batcher: %v", err)
	}
	defer batcher.Stop()

	numGoroutines := 25
	messagesPerGoroutine := 40
	statsGoroutines := 8
	statsOperations := 80

	var wg sync.WaitGroup
	start := make(chan struct{})
	var totalMessages int32
	var totalStatsCalls int32

	// Start goroutines that add messages to partitioned topic
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			<-start

			for j := 0; j < messagesPerGoroutine; j++ {
				msg := message.Message{
					Timestamp: time.Now(),
					Topic:     "partitioned-race-topic",
					Key:       fmt.Sprintf("key-%d", j), // Same key should go to same partition
					Value:     fmt.Sprintf("Message %d from goroutine %d", j, goroutineID),
				}

				if err := batcher.AddMessage(msg); err != nil {
					t.Errorf("Goroutine %d failed to add message: %v", goroutineID, err)
				} else {
					atomic.AddInt32(&totalMessages, 1)
				}

				time.Sleep(time.Microsecond)
			}
		}(i)
	}

	// Start goroutines that call GetTopicStats concurrently
	for i := 0; i < statsGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			<-start

			for j := 0; j < statsOperations; j++ {
				buffered, persisted, err := batcher.GetTopicStats("partitioned-race-topic")
				atomic.AddInt32(&totalStatsCalls, 1)

				if err != nil {
					// It's okay if topic doesn't exist yet
					continue
				}

				// Verify stats are reasonable
				if buffered < 0 || persisted < 0 {
					t.Errorf("Goroutine %d: negative stats for partitioned topic: buffered=%d, persisted=%d",
						goroutineID, buffered, persisted)
				}

				time.Sleep(time.Nanosecond)
			}
		}(i)
	}

	close(start)
	wg.Wait()

	// Final flush
	batcher.FlushAll()

	// Verify final state
	if atomic.LoadInt32(&totalMessages) == 0 {
		t.Error("No messages were added to partitioned topic during race condition test")
	}

	if atomic.LoadInt32(&totalStatsCalls) == 0 {
		t.Error("No stats calls were made for partitioned topic during race condition test")
	}

	// Check final stats for partitioned topic
	buffered, persisted, err := batcher.GetTopicStats("partitioned-race-topic")
	if err != nil {
		t.Fatalf("Failed to get final stats for partitioned topic: %v", err)
	}

	total := buffered + int(persisted)
	if total == 0 {
		t.Error("Partitioned topic has no messages after race condition test")
	}
}

// TestBatcherRaceCondition_EdgeCases tests edge cases that might reveal race conditions
func TestBatcherRaceCondition_EdgeCases(t *testing.T) {
	tempDir := t.TempDir()
	pool := storage.NewFilePool(time.Hour)
	defer pool.Stop()

	cfg := &config.Config{}
	cfg.Storage.DefaultPartitions = 1

	healthTracker := health.NewHealthTracker()
	// Very small batch size to trigger frequent flushes
	batcher := NewTopicBatcher(tempDir, 1, 1*time.Millisecond, pool, healthTracker, cfg)

	if err := batcher.Start(); err != nil {
		t.Fatalf("Failed to start batcher: %v", err)
	}
	defer batcher.Stop()

	numGoroutines := 30
	messagesPerGoroutine := 10

	var wg sync.WaitGroup
	start := make(chan struct{})
	var totalMessages int32

	// Start goroutines that add messages with very frequent flushes
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			<-start

			for j := 0; j < messagesPerGoroutine; j++ {
				msg := message.Message{
					Timestamp: time.Now(),
					Topic:     "edge-case-topic",
					Key:       fmt.Sprintf("goroutine-%d-msg-%d", goroutineID, j),
					Value:     fmt.Sprintf("Message %d from goroutine %d", j, goroutineID),
				}

				if err := batcher.AddMessage(msg); err != nil {
					t.Errorf("Goroutine %d failed to add message: %v", goroutineID, err)
				} else {
					atomic.AddInt32(&totalMessages, 1)
				}

				// Get stats after every message (high contention)
				buffered, persisted, err := batcher.GetTopicStats("edge-case-topic")
				if err != nil {
					// It's okay if topic doesn't exist yet
					continue
				}

				// Verify stats are reasonable
				if buffered < 0 || persisted < 0 {
					t.Errorf("Goroutine %d: negative stats: buffered=%d, persisted=%d",
						goroutineID, buffered, persisted)
				}
			}
		}(i)
	}

	close(start)
	wg.Wait()

	// Final flush
	batcher.FlushAll()

	// Verify final state
	if atomic.LoadInt32(&totalMessages) == 0 {
		t.Error("No messages were added during edge case test")
	}

	// Check final stats
	buffered, persisted, err := batcher.GetTopicStats("edge-case-topic")
	if err != nil {
		t.Fatalf("Failed to get final stats: %v", err)
	}

	total := buffered + int(persisted)
	if total == 0 {
		t.Error("Topic has no messages after edge case test")
	}
}
