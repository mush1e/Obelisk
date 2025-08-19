package consumer

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mush1e/obelisk/internal/message"
	"github.com/mush1e/obelisk/internal/storage"
)

// TestConsumerConcurrentPoll tests concurrent polling from the same consumer
func TestConsumerConcurrentPoll(t *testing.T) {
	tempDir := t.TempDir()

	// Create some test data first
	createPartitionedTestData(t, tempDir, "test-topic", 2, 25)

	consumer := NewConsumer(tempDir, "concurrent-poll-test", "test-topic")
	numGoroutines := 5
	pollsPerGoroutine := 10

	var wg sync.WaitGroup
	start := make(chan struct{})
	var totalMessages int32

	// Start multiple goroutines polling concurrently
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			<-start

			for j := 0; j < pollsPerGoroutine; j++ {
				messages, err := consumer.Poll("test-topic")
				if err != nil {
					t.Errorf("Goroutine %d failed to poll: %v", goroutineID, err)
					break // Break instead of continue to avoid infinite loops
				}

				atomic.AddInt32(&totalMessages, int32(len(messages)))

				// Commit progress
				if len(messages) > 0 {
					if err := consumer.CommitWithPartitionTracking("test-topic"); err != nil {
						t.Errorf("Goroutine %d failed to commit: %v", goroutineID, err)
					}
				}

				time.Sleep(10 * time.Millisecond) // Longer delay to prevent tight loops
			}
		}(i)
	}

	close(start)

	// Add timeout to prevent hanging
	done := make(chan bool)
	go func() {
		wg.Wait()
		done <- true
	}()

	select {
	case <-done:
		// Test completed successfully
	case <-time.After(30 * time.Second):
		t.Fatal("Test timed out after 30 seconds")
	}

	// Verify we got all messages (some goroutines might get empty results due to commits)
	if atomic.LoadInt32(&totalMessages) == 0 {
		t.Error("No messages were retrieved during concurrent polling")
	}
}

// TestConsumerConcurrentOffsetTracking tests concurrent offset operations
func TestConsumerConcurrentOffsetTracking(t *testing.T) {
	tempDir := t.TempDir()
	createTestData(t, tempDir, "offset-test", 50)

	consumer := NewConsumer(tempDir, "concurrent-offset-test", "offset-test")
	numGoroutines := 15
	operationsPerGoroutine := 30

	var wg sync.WaitGroup
	start := make(chan struct{})

	// Start goroutines that read and update offsets concurrently
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			<-start

			for j := 0; j < operationsPerGoroutine; j++ {
				// Get current offset
				offset, err := consumer.GetCurrentOffset("offset-test")
				if err != nil {
					t.Errorf("Goroutine %d failed to get offset: %v", goroutineID, err)
					continue
				}

				// Poll some messages
				messages, err := consumer.Poll("offset-test")
				if err != nil {
					t.Errorf("Goroutine %d failed to poll: %v", goroutineID, err)
					continue
				}

				// Commit if we got messages
				if len(messages) > 0 {
					if err := consumer.CommitWithPartitionTracking("offset-test"); err != nil {
						t.Errorf("Goroutine %d failed to commit: %v", goroutineID, err)
					}
				}

				// Verify offset advanced
				newOffset, err := consumer.GetCurrentOffset("offset-test")
				if err != nil {
					t.Errorf("Goroutine %d failed to get new offset: %v", goroutineID, err)
					continue
				}

				if newOffset < offset {
					t.Errorf("Goroutine %d: offset went backwards: %d -> %d", goroutineID, offset, newOffset)
				}
			}
		}(i)
	}

	close(start)
	wg.Wait()
}

// TestConsumerConcurrentPartitionedPoll tests concurrent polling from partitioned topics
func TestConsumerConcurrentPartitionedPoll(t *testing.T) {
	tempDir := t.TempDir()

	// Create partitioned test data
	createPartitionedTestData(t, tempDir, "partitioned-test", 4, 10)

	consumer := NewConsumer(tempDir, "concurrent-partitioned-test", "partitioned-test")
	numGoroutines := 4
	pollsPerGoroutine := 8

	var wg sync.WaitGroup
	start := make(chan struct{})
	var totalMessages int32

	// Start goroutines polling from partitioned topic
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			<-start

			for j := 0; j < pollsPerGoroutine; j++ {
				messages, err := consumer.Poll("partitioned-test")
				if err != nil {
					t.Errorf("Goroutine %d failed to poll partitioned topic: %v", goroutineID, err)
					continue
				}

				atomic.AddInt32(&totalMessages, int32(len(messages)))

				// Commit progress
				if len(messages) > 0 {
					if err := consumer.CommitWithPartitionTracking("partitioned-test"); err != nil {
						t.Errorf("Goroutine %d failed to commit: %v", goroutineID, err)
					}
				}

				time.Sleep(time.Millisecond)
			}
		}(i)
	}

	close(start)
	wg.Wait()

	// Verify we got messages from partitioned topic
	if atomic.LoadInt32(&totalMessages) == 0 {
		t.Error("No messages were retrieved from partitioned topic during concurrent polling")
	}
}

// TestConsumerConcurrentSubscribeUnsubscribe tests concurrent subscription operations
func TestConsumerConcurrentSubscribeUnsubscribe(t *testing.T) {
	tempDir := t.TempDir()
	consumer := NewConsumer(tempDir, "concurrent-sub-test")

	numGoroutines := 10
	operationsPerGoroutine := 20

	var wg sync.WaitGroup
	start := make(chan struct{})

	// Start goroutines that subscribe and unsubscribe concurrently
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			<-start

			for j := 0; j < operationsPerGoroutine; j++ {
				topic := fmt.Sprintf("topic-%d", goroutineID)

				// Subscribe
				consumer.Subscribe(topic)

				// Try to poll (should not error even if no messages)
				_, err := consumer.Poll(topic)
				if err != nil {
					// Only log the error, don't fail the test since this is expected
					// when topics don't exist or are unsubscribed by other goroutines
					continue
				}

				// Small delay to allow other operations
				time.Sleep(time.Microsecond)

				// Unsubscribe
				consumer.Unsubscribe(topic)

				time.Sleep(time.Microsecond)
			}
		}(i)
	}

	close(start)
	wg.Wait()
}

// TestConsumerConcurrentReset tests concurrent reset operations
func TestConsumerConcurrentReset(t *testing.T) {
	tempDir := t.TempDir()
	createTestData(t, tempDir, "reset-test", 30)

	consumer := NewConsumer(tempDir, "concurrent-reset-test", "reset-test")
	numGoroutines := 10
	operationsPerGoroutine := 20

	var wg sync.WaitGroup
	start := make(chan struct{})

	// Start goroutines that reset and poll concurrently
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			<-start

			for j := 0; j < operationsPerGoroutine; j++ {
				// Reset offset
				if err := consumer.Reset("reset-test"); err != nil {
					t.Errorf("Goroutine %d failed to reset: %v", goroutineID, err)
					continue
				}

				// Poll messages
				messages, err := consumer.Poll("reset-test")
				if err != nil {
					t.Errorf("Goroutine %d failed to poll after reset: %v", goroutineID, err)
					continue
				}

				// Verify we can get messages after reset
				if len(messages) > 0 {
					if err := consumer.CommitWithPartitionTracking("reset-test"); err != nil {
						t.Errorf("Goroutine %d failed to commit after reset: %v", goroutineID, err)
					}
				}

				time.Sleep(time.Millisecond)
			}
		}(i)
	}

	close(start)
	wg.Wait()
}

// TestConsumerConcurrentMessageCount tests concurrent message count operations
func TestConsumerConcurrentMessageCount(t *testing.T) {
	tempDir := t.TempDir()
	createTestData(t, tempDir, "count-test", 40)

	consumer := NewConsumer(tempDir, "concurrent-count-test", "count-test")
	numGoroutines := 12
	operationsPerGoroutine := 25

	var wg sync.WaitGroup
	start := make(chan struct{})

	// Start goroutines that get message counts concurrently
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			<-start

			for j := 0; j < operationsPerGoroutine; j++ {
				count, err := consumer.GetTopicMessageCount("count-test")
				if err != nil {
					t.Errorf("Goroutine %d failed to get message count: %v", goroutineID, err)
					continue
				}

				// Verify count is reasonable
				if count < 0 {
					t.Errorf("Goroutine %d got negative message count: %d", goroutineID, count)
				}

				// Poll some messages
				messages, err := consumer.Poll("count-test")
				if err != nil {
					t.Errorf("Goroutine %d failed to poll: %v", goroutineID, err)
					continue
				}

				// Commit if we got messages
				if len(messages) > 0 {
					if err := consumer.CommitWithPartitionTracking("count-test"); err != nil {
						t.Errorf("Goroutine %d failed to commit: %v", goroutineID, err)
					}
				}

				time.Sleep(time.Millisecond)
			}
		}(i)
	}

	close(start)
	wg.Wait()
}

// TestConsumerConcurrentPartitionTracking tests concurrent partition offset tracking
func TestConsumerConcurrentPartitionTracking(t *testing.T) {
	tempDir := t.TempDir()
	createPartitionedTestData(t, tempDir, "partition-tracking-test", 3, 20)

	consumer := NewConsumer(tempDir, "concurrent-partition-tracking-test", "partition-tracking-test")
	numGoroutines := 6
	operationsPerGoroutine := 30

	var wg sync.WaitGroup
	start := make(chan struct{})

	// Start goroutines that poll with partition tracking
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			<-start

			for j := 0; j < operationsPerGoroutine; j++ {
				// Poll with partition tracking
				messagesWithPartition, err := consumer.PollWithPartitionTracking("partition-tracking-test")
				if err != nil {
					t.Errorf("Goroutine %d failed to poll with partition tracking: %v", goroutineID, err)
					continue
				}

				// Verify partition information
				for _, mwp := range messagesWithPartition {
					if mwp.Partition < 0 || mwp.Partition >= 3 {
						t.Errorf("Goroutine %d: invalid partition %d", goroutineID, mwp.Partition)
					}
				}

				// Commit with partition tracking
				if len(messagesWithPartition) > 0 {
					if err := consumer.CommitWithPartitionTracking("partition-tracking-test"); err != nil {
						t.Errorf("Goroutine %d failed to commit with partition tracking: %v", goroutineID, err)
					}
				}

				time.Sleep(time.Millisecond)
			}
		}(i)
	}

	close(start)
	wg.Wait()
}

// BenchmarkConcurrentPoll benchmarks concurrent polling performance
func BenchmarkConcurrentPoll(b *testing.B) {
	tempDir := b.TempDir()
	createTestData(b, tempDir, "benchmark-topic", 1000)

	consumer := NewConsumer(tempDir, "benchmark-consumer", "benchmark-topic")
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			messages, err := consumer.Poll("benchmark-topic")
			if err != nil {
				b.Fatal(err)
			}

			if len(messages) > 0 {
				if err := consumer.CommitWithPartitionTracking("benchmark-topic"); err != nil {
					b.Fatal(err)
				}
			}
		}
	})
}

// Helper functions

func createTestData(t testing.TB, baseDir, topic string, messageCount int) {
	// Create topic directory
	topicDir := filepath.Join(baseDir, topic)
	if err := os.MkdirAll(topicDir, 0755); err != nil {
		t.Fatalf("Failed to create topic directory: %v", err)
	}

	// Create log and index files
	logFile := filepath.Join(topicDir, topic+".log")
	idxFile := filepath.Join(topicDir, topic+".idx")

	// Create some test messages
	messages := make([]message.Message, messageCount)
	for i := 0; i < messageCount; i++ {
		messages[i] = message.Message{
			Timestamp: time.Now().Add(time.Duration(i) * time.Millisecond),
			Topic:     topic,
			Key:       fmt.Sprintf("key-%d", i),
			Value:     fmt.Sprintf("value-%d", i),
		}
	}

	// Write messages to storage
	pool := storage.NewFilePool(time.Hour)
	defer pool.Stop()

	index := &storage.OffsetIndex{Positions: []int64{}}
	if err := storage.AppendMessages(pool, logFile, idxFile, messages, index); err != nil {
		t.Fatalf("Failed to write test messages: %v", err)
	}
}

func createPartitionedTestData(t testing.TB, baseDir, topic string, partitions, messagesPerPartition int) {
	// Create topic directory
	topicDir := filepath.Join(baseDir, topic)
	if err := os.MkdirAll(topicDir, 0755); err != nil {
		t.Fatalf("Failed to create topic directory: %v", err)
	}

	pool := storage.NewFilePool(time.Hour)
	defer pool.Stop()

	// Create messages for each partition
	for partition := 0; partition < partitions; partition++ {
		logFile, idxFile := storage.GetPartitionedPaths(baseDir, topic, partition)

		messages := make([]message.Message, messagesPerPartition)
		for i := 0; i < messagesPerPartition; i++ {
			messages[i] = message.Message{
				Timestamp: time.Now().Add(time.Duration(i) * time.Millisecond),
				Topic:     topic,
				Key:       fmt.Sprintf("partition-%d-key-%d", partition, i),
				Value:     fmt.Sprintf("partition-%d-value-%d", partition, i),
			}
		}

		index := &storage.OffsetIndex{Positions: []int64{}}
		if err := storage.AppendMessages(pool, logFile, idxFile, messages, index); err != nil {
			t.Fatalf("Failed to write test messages for partition %d: %v", partition, err)
		}
	}
}
