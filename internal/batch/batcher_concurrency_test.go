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

// TestBatcherConcurrentAddMessage tests concurrent message addition to the batcher
func TestBatcherConcurrentAddMessage(t *testing.T) {
	tempDir := t.TempDir()
	pool := storage.NewFilePool(time.Hour)
	defer pool.Stop()

	cfg := &config.Config{}
	cfg.Storage.DefaultPartitions = 4

	healthTracker := health.NewHealthTracker()
	batcher := NewTopicBatcher(tempDir, 100, 5*time.Second, pool, healthTracker, cfg)

	if err := batcher.Start(); err != nil {
		t.Fatalf("Failed to start batcher: %v", err)
	}
	defer batcher.Stop()

	numGoroutines := 10
	messagesPerGoroutine := 20
	numTopics := 5

	var wg sync.WaitGroup
	start := make(chan struct{})
	var totalMessages int32

	// Start goroutines that add messages concurrently
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			<-start

			for j := 0; j < messagesPerGoroutine; j++ {
				topic := fmt.Sprintf("topic-%d", j%numTopics)
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

	close(start)
	wg.Wait()

	// Force a final flush
	batcher.FlushAll()

	// Verify all messages were processed
	if atomic.LoadInt32(&totalMessages) == 0 {
		t.Error("No messages were added during concurrent test")
	}

	// Verify messages were distributed across topics
	for i := 0; i < numTopics; i++ {
		topic := fmt.Sprintf("topic-%d", i)
		buffered, persisted, err := batcher.GetTopicStats(topic)
		if err != nil {
			t.Errorf("Failed to get stats for topic %s: %v", topic, err)
			continue
		}

		total := buffered + int(persisted)
		if total == 0 {
			t.Errorf("Topic %s has no messages", topic)
		}
	}
}

// TestBatcherConcurrentFlush tests concurrent flushing operations
func TestBatcherConcurrentFlush(t *testing.T) {
	tempDir := t.TempDir()
	pool := storage.NewFilePool(time.Hour)
	defer pool.Stop()

	cfg := &config.Config{}
	cfg.Storage.DefaultPartitions = 2

	healthTracker := health.NewHealthTracker()
	batcher := NewTopicBatcher(tempDir, 10, 100*time.Millisecond, pool, healthTracker, cfg)

	if err := batcher.Start(); err != nil {
		t.Fatalf("Failed to start batcher: %v", err)
	}
	defer batcher.Stop()

	numGoroutines := 15
	messagesPerGoroutine := 30

	var wg sync.WaitGroup
	start := make(chan struct{})

	// Start goroutines that add messages and trigger flushes
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			<-start

			for j := 0; j < messagesPerGoroutine; j++ {
				topic := fmt.Sprintf("flush-topic-%d", j%3)
				msg := message.Message{
					Timestamp: time.Now(),
					Topic:     topic,
					Key:       fmt.Sprintf("goroutine-%d-msg-%d", goroutineID, j),
					Value:     fmt.Sprintf("Message %d from goroutine %d", j, goroutineID),
				}

				if err := batcher.AddMessage(msg); err != nil {
					t.Errorf("Goroutine %d failed to add message: %v", goroutineID, err)
				}

				// Occasionally trigger manual flush
				if j%5 == 0 {
					batcher.FlushAll()
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
	for i := 0; i < 3; i++ {
		topic := fmt.Sprintf("flush-topic-%d", i)
		buffered, persisted, err := batcher.GetTopicStats(topic)
		if err != nil {
			t.Errorf("Failed to get stats for topic %s: %v", topic, err)
			continue
		}

		// Most messages should be persisted after flush
		if buffered > int(persisted) && persisted == 0 {
			t.Errorf("Topic %s: too many buffered messages (%d) vs persisted (%d)", topic, buffered, persisted)
		}
	}
}

// TestBatcherConcurrentPartitionedMessages tests concurrent messages to partitioned topics
func TestBatcherConcurrentPartitionedMessages(t *testing.T) {
	tempDir := t.TempDir()
	pool := storage.NewFilePool(time.Hour)
	defer pool.Stop()

	cfg := &config.Config{}
	cfg.Storage.DefaultPartitions = 4
	cfg.Storage.Topics = []config.TopicConfig{
		{Name: "partitioned-topic", Partitions: 4},
	}

	healthTracker := health.NewHealthTracker()
	batcher := NewTopicBatcher(tempDir, 50, 2*time.Second, pool, healthTracker, cfg)

	if err := batcher.Start(); err != nil {
		t.Fatalf("Failed to start batcher: %v", err)
	}
	defer batcher.Stop()

	numGoroutines := 25
	messagesPerGoroutine := 40

	var wg sync.WaitGroup
	start := make(chan struct{})
	var totalMessages int32

	// Start goroutines that add messages to partitioned topic
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			<-start

			for j := 0; j < messagesPerGoroutine; j++ {
				msg := message.Message{
					Timestamp: time.Now(),
					Topic:     "partitioned-topic",
					Key:       fmt.Sprintf("key-%d", j), // Same key should go to same partition
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

	close(start)
	wg.Wait()

	// Force final flush
	batcher.FlushAll()

	// Verify messages were processed
	if atomic.LoadInt32(&totalMessages) == 0 {
		t.Error("No messages were added to partitioned topic")
	}

	// Check that messages were distributed across partitions
	buffered, persisted, err := batcher.GetTopicStats("partitioned-topic")
	if err != nil {
		t.Fatalf("Failed to get stats for partitioned topic: %v", err)
	}

	total := buffered + int(persisted)
	if total == 0 {
		t.Error("Partitioned topic has no messages")
	}
}

// TestBatcherConcurrentStartStop tests concurrent operations during start/stop
func TestBatcherConcurrentStartStop(t *testing.T) {
	tempDir := t.TempDir()
	pool := storage.NewFilePool(time.Hour)
	defer pool.Stop()

	cfg := &config.Config{}
	cfg.Storage.DefaultPartitions = 2

	healthTracker := health.NewHealthTracker()
	batcher := NewTopicBatcher(tempDir, 20, 100*time.Millisecond, pool, healthTracker, cfg)

	// Start the batcher once
	if err := batcher.Start(); err != nil {
		t.Fatalf("Failed to start batcher: %v", err)
	}

	numGoroutines := 10
	messagesPerGoroutine := 5

	var wg sync.WaitGroup
	start := make(chan struct{})

	// Start goroutines that add messages concurrently
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			<-start

			for j := 0; j < messagesPerGoroutine; j++ {
				msg := message.Message{
					Timestamp: time.Now(),
					Topic:     "start-stop-test",
					Key:       fmt.Sprintf("goroutine-%d-msg-%d", goroutineID, j),
					Value:     fmt.Sprintf("Message %d from goroutine %d", j, goroutineID),
				}
				if err := batcher.AddMessage(msg); err != nil {
					t.Errorf("Goroutine %d failed to add message: %v", goroutineID, err)
				}
				time.Sleep(time.Millisecond)
			}
		}(i)
	}

	close(start)
	wg.Wait()

	// Stop the batcher once at the end
	batcher.Stop()
}

// TestBatcherConcurrentStats tests concurrent stats retrieval
func TestBatcherConcurrentStats(t *testing.T) {
	tempDir := t.TempDir()
	pool := storage.NewFilePool(time.Hour)
	defer pool.Stop()

	cfg := &config.Config{}
	cfg.Storage.DefaultPartitions = 3

	healthTracker := health.NewHealthTracker()
	batcher := NewTopicBatcher(tempDir, 30, 1*time.Second, pool, healthTracker, cfg)

	if err := batcher.Start(); err != nil {
		t.Fatalf("Failed to start batcher: %v", err)
	}
	defer batcher.Stop()

	numGoroutines := 6
	operationsPerGoroutine := 15

	var wg sync.WaitGroup
	start := make(chan struct{})

	// Start goroutines that add messages and get stats concurrently
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			<-start

			for j := 0; j < operationsPerGoroutine; j++ {
				topic := fmt.Sprintf("stats-topic-%d", j%4)

				// Add message
				msg := message.Message{
					Timestamp: time.Now(),
					Topic:     topic,
					Key:       fmt.Sprintf("goroutine-%d-op-%d", goroutineID, j),
					Value:     fmt.Sprintf("Message from goroutine %d", goroutineID),
				}
				if err := batcher.AddMessage(msg); err != nil {
					t.Errorf("Goroutine %d failed to add message: %v", goroutineID, err)
				}

				// Get stats
				buffered, persisted, err := batcher.GetTopicStats(topic)
				if err != nil {
					// It's okay if topic doesn't exist yet
					continue
				}

				// Verify stats are reasonable
				if buffered < 0 || persisted < 0 {
					t.Errorf("Goroutine %d: negative stats for topic %s: buffered=%d, persisted=%d",
						goroutineID, topic, buffered, persisted)
				}

				time.Sleep(time.Millisecond)
			}
		}(i)
	}

	close(start)
	wg.Wait()

	// Final flush and stats check
	batcher.FlushAll()

	for i := 0; i < 4; i++ {
		topic := fmt.Sprintf("stats-topic-%d", i)
		buffered, persisted, err := batcher.GetTopicStats(topic)
		if err != nil {
			t.Errorf("Failed to get final stats for topic %s: %v", topic, err)
			continue
		}

		total := buffered + int(persisted)
		if total == 0 {
			t.Errorf("Topic %s has no messages after concurrent operations", topic)
		}
	}
}

// TestBatcherConcurrentOverflow tests concurrent overflow scenarios
func TestBatcherConcurrentOverflow(t *testing.T) {
	tempDir := t.TempDir()
	pool := storage.NewFilePool(time.Hour)
	defer pool.Stop()

	cfg := &config.Config{}
	cfg.Storage.DefaultPartitions = 2

	healthTracker := health.NewHealthTracker()
	// Small batch size to trigger frequent flushes
	batcher := NewTopicBatcher(tempDir, 5, 50*time.Millisecond, pool, healthTracker, cfg)

	if err := batcher.Start(); err != nil {
		t.Fatalf("Failed to start batcher: %v", err)
	}
	defer batcher.Stop()

	numGoroutines := 15
	messagesPerGoroutine := 10

	var wg sync.WaitGroup
	start := make(chan struct{})
	var totalMessages int32

	// Start goroutines that will cause frequent overflows
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			<-start

			for j := 0; j < messagesPerGoroutine; j++ {
				topic := fmt.Sprintf("overflow-topic-%d", j%3)
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

				time.Sleep(time.Microsecond) // Small delay to allow flushes
			}
		}(i)
	}

	close(start)
	wg.Wait()

	// Final flush
	batcher.FlushAll()

	// Verify messages were processed despite overflows
	if atomic.LoadInt32(&totalMessages) == 0 {
		t.Error("No messages were processed during overflow test")
	}

	// Check final state
	for i := 0; i < 3; i++ {
		topic := fmt.Sprintf("overflow-topic-%d", i)
		buffered, persisted, err := batcher.GetTopicStats(topic)
		if err != nil {
			t.Errorf("Failed to get stats for topic %s: %v", topic, err)
			continue
		}

		total := buffered + int(persisted)
		if total == 0 {
			t.Errorf("Topic %s has no messages after overflow test", topic)
		}
	}
}

// BenchmarkConcurrentAddMessage benchmarks concurrent message addition
func BenchmarkConcurrentAddMessage(b *testing.B) {
	tempDir := b.TempDir()
	pool := storage.NewFilePool(time.Hour)
	defer pool.Stop()

	cfg := &config.Config{}
	cfg.Storage.DefaultPartitions = 4

	healthTracker := health.NewHealthTracker()
	batcher := NewTopicBatcher(tempDir, 1000, 5*time.Second, pool, healthTracker, cfg)

	if err := batcher.Start(); err != nil {
		b.Fatalf("Failed to start batcher: %v", err)
	}
	defer batcher.Stop()

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			msg := message.Message{
				Timestamp: time.Now(),
				Topic:     "benchmark-topic",
				Key:       fmt.Sprintf("key-%d", i),
				Value:     fmt.Sprintf("value-%d", i),
			}
			if err := batcher.AddMessage(msg); err != nil {
				b.Fatal(err)
			}
			i++
		}
	})
}
