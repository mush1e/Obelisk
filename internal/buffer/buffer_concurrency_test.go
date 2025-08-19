package buffer

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/mush1e/obelisk/internal/message"
)

// TestBufferConcurrentPush tests concurrent message pushing to a single buffer
func TestBufferConcurrentPush(t *testing.T) {
	buf := NewBuffer(1000)
	numGoroutines := 50
	messagesPerGoroutine := 100

	var wg sync.WaitGroup
	start := make(chan struct{})

	// Start multiple goroutines that will push messages concurrently
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			<-start // Wait for all goroutines to be ready

			for j := 0; j < messagesPerGoroutine; j++ {
				msg := message.Message{
					Topic: "concurrent-test",
					Key:   fmt.Sprintf("goroutine-%d-msg-%d", goroutineID, j),
					Value: fmt.Sprintf("Message %d from goroutine %d", j, goroutineID),
				}
				if err := buf.Push(msg); err != nil {
					t.Errorf("Failed to push message: %v", err)
				}
			}
		}(i)
	}

	// Start all goroutines simultaneously
	close(start)
	wg.Wait()

	// Verify messages were pushed (buffer has capacity limit, so it may overwrite)
	recent := buf.GetRecent()
	if len(recent) == 0 {
		t.Error("No messages were pushed to buffer")
	}

	// Verify we have the most recent messages (capacity is 1000)
	if len(recent) != buf.capacity {
		t.Errorf("Buffer should be at capacity %d, got %d", buf.capacity, len(recent))
	}

	// Verify no duplicate messages (check by key)
	seenKeys := make(map[string]bool)
	for _, msg := range recent {
		if seenKeys[msg.Key] {
			t.Errorf("Duplicate message key found: %s", msg.Key)
		}
		seenKeys[msg.Key] = true
	}
}

// TestTopicBuffersConcurrentPush tests concurrent pushing to different topics
func TestTopicBuffersConcurrentPush(t *testing.T) {
	tb := NewTopicBuffers(100)
	numGoroutines := 20
	messagesPerGoroutine := 50
	numTopics := 5

	var wg sync.WaitGroup
	start := make(chan struct{})

	// Start goroutines that push to different topics
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			<-start

			topic := fmt.Sprintf("topic-%d", goroutineID%numTopics)
			for j := 0; j < messagesPerGoroutine; j++ {
				msg := message.Message{
					Topic: topic,
					Key:   fmt.Sprintf("goroutine-%d-msg-%d", goroutineID, j),
					Value: fmt.Sprintf("Message %d from goroutine %d", j, goroutineID),
				}
				if err := tb.Push(msg); err != nil {
					t.Errorf("Failed to push message to topic %s: %v", topic, err)
				}
			}
		}(i)
	}

	close(start)
	wg.Wait()

	// Verify messages were distributed across topics
	for i := 0; i < numTopics; i++ {
		topic := fmt.Sprintf("topic-%d", i)
		messages := tb.GetRecentByTopic(topic)
		if len(messages) == 0 {
			t.Errorf("Topic %s: no messages found", topic)
		}

		// Each topic buffer has capacity 100, so we should have recent messages
		if len(messages) > tb.capacity {
			t.Errorf("Topic %s: more messages than capacity: %d > %d", topic, len(messages), tb.capacity)
		}
	}
}

// TestBufferConcurrentReadWrite tests concurrent reads and writes
func TestBufferConcurrentReadWrite(t *testing.T) {
	buf := NewBuffer(100)
	numWriters := 10
	numReaders := 5
	messagesPerWriter := 50

	var wg sync.WaitGroup
	start := make(chan struct{})
	stop := make(chan struct{})

	// Start writer goroutines
	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			<-start

			for j := 0; j < messagesPerWriter; j++ {
				select {
				case <-stop:
					return
				default:
					msg := message.Message{
						Topic: "read-write-test",
						Key:   fmt.Sprintf("writer-%d-msg-%d", writerID, j),
						Value: fmt.Sprintf("Message %d from writer %d", j, writerID),
					}
					if err := buf.Push(msg); err != nil {
						t.Errorf("Writer %d failed to push message: %v", writerID, err)
					}
					time.Sleep(time.Microsecond) // Small delay to allow reads
				}
			}
		}(i)
	}

	// Start reader goroutines
	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()
			<-start

			for {
				select {
				case <-stop:
					return
				default:
					recent := buf.GetRecent()
					if len(recent) > 0 {
						// Verify we can read messages without errors
						_ = recent[len(recent)-1].Key
					}
					time.Sleep(time.Microsecond)
				}
			}
		}(i)
	}

	close(start)
	time.Sleep(100 * time.Millisecond) // Let them run for a bit
	close(stop)
	wg.Wait()

	// Verify final state
	recent := buf.GetRecent()
	if len(recent) == 0 {
		t.Error("No messages were written during concurrent read/write test")
	}
}

// TestBufferOverflowConcurrent tests concurrent overflow handling
func TestBufferOverflowConcurrent(t *testing.T) {
	capacity := 10
	buf := NewBuffer(capacity)
	numGoroutines := 20
	messagesPerGoroutine := 5

	var wg sync.WaitGroup
	start := make(chan struct{})

	// Start goroutines that will overflow the buffer
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			<-start

			for j := 0; j < messagesPerGoroutine; j++ {
				msg := message.Message{
					Topic: "overflow-test",
					Key:   fmt.Sprintf("goroutine-%d-msg-%d", goroutineID, j),
					Value: fmt.Sprintf("Message %d from goroutine %d", j, goroutineID),
				}
				if err := buf.Push(msg); err != nil {
					t.Errorf("Failed to push message: %v", err)
				}
			}
		}(i)
	}

	close(start)
	wg.Wait()

	// Verify buffer maintains capacity
	recent := buf.GetRecent()
	if len(recent) != capacity {
		t.Errorf("Buffer should maintain capacity %d, got %d", capacity, len(recent))
	}

	// Verify we have the most recent messages (oldest should be overwritten)
	if len(recent) > 0 {
		lastMsg := recent[len(recent)-1]
		if lastMsg.Key == "" {
			t.Error("Last message should not be empty after overflow")
		}
	}
}

// TestTopicBuffersConcurrentAccess tests concurrent access to topic buffers
func TestTopicBuffersConcurrentAccess(t *testing.T) {
	tb := NewTopicBuffers(50)
	numGoroutines := 30
	operationsPerGoroutine := 100

	var wg sync.WaitGroup
	start := make(chan struct{})

	// Start goroutines that perform mixed operations
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			<-start

			for j := 0; j < operationsPerGoroutine; j++ {
				topic := fmt.Sprintf("topic-%d", j%5)

				// Push a message
				msg := message.Message{
					Topic: topic,
					Key:   fmt.Sprintf("goroutine-%d-op-%d", goroutineID, j),
					Value: fmt.Sprintf("Operation %d from goroutine %d", j, goroutineID),
				}
				if err := tb.Push(msg); err != nil {
					t.Errorf("Failed to push message: %v", err)
				}

				// Read messages from the same topic
				messages := tb.GetRecentByTopic(topic)
				if len(messages) > 0 {
					// Verify we can access messages without errors
					_ = messages[len(messages)-1].Key
				}
			}
		}(i)
	}

	close(start)
	wg.Wait()

	// Verify final state
	for i := 0; i < 5; i++ {
		topic := fmt.Sprintf("topic-%d", i)
		messages := tb.GetRecentByTopic(topic)
		if len(messages) == 0 {
			t.Errorf("Topic %s should have messages", topic)
		}
	}
}

// BenchmarkConcurrentPush benchmarks concurrent pushing performance
func BenchmarkConcurrentPush(b *testing.B) {
	buf := NewBuffer(10000)
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			msg := message.Message{
				Topic: "benchmark",
				Key:   fmt.Sprintf("key-%d", i),
				Value: fmt.Sprintf("value-%d", i),
			}
			if err := buf.Push(msg); err != nil {
				b.Fatal(err)
			}
			i++
		}
	})
}

// BenchmarkConcurrentReadWrite benchmarks concurrent read/write performance
func BenchmarkConcurrentReadWrite(b *testing.B) {
	buf := NewBuffer(10000)
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			// Write
			msg := message.Message{
				Topic: "benchmark",
				Key:   fmt.Sprintf("key-%d", i),
				Value: fmt.Sprintf("value-%d", i),
			}
			if err := buf.Push(msg); err != nil {
				b.Fatal(err)
			}

			// Read
			_ = buf.GetRecent()
			i++
		}
	})
}
