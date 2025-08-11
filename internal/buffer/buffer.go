package buffer

// This package provides a ring buffer implementation for the Obelisk message broker.
// The ring buffer system maintains recent messages in memory for fast retrieval and
// analysis operations. It uses a circular buffer design that overwrites old messages
// when the capacity is reached, ensuring bounded memory usage while preserving the
// most recent activity.
//
// The system handles:
// - Per-topic ring buffers with configurable capacity
// - Thread-safe operations with concurrent read/write support
// - Automatic buffer creation on first message to a topic
// - Efficient circular buffer operations with O(1) insertion
// - Ordered retrieval of messages from oldest to newest

import (
	"sync"

	"github.com/mush1e/obelisk/internal/message"
)

// TopicBuffers manages a collection of ring buffers, one per topic.
// Each topic gets its own isolated buffer to prevent cross-topic interference
// and allow independent capacity management. The collection automatically
// creates new buffers when messages arrive for previously unseen topics.
//
// Thread safety is achieved through a read-write mutex that allows concurrent
// reads of different topic buffers while ensuring exclusive access during
// buffer creation and modification operations.
type TopicBuffers struct {
	mtx      sync.RWMutex       // Protects the buffers map during concurrent access
	buffers  map[string]*Buffer // Maps topic names to their respective ring buffers
	capacity int                // Maximum number of messages each topic buffer can hold
}

// NewTopicBuffers creates a new collection of topic buffers with the specified capacity.
// All topic buffers created by this instance will use the same capacity limit.
// The capacity determines how many recent messages are kept in memory per topic.
//
// Parameters:
//   - capacity: Maximum number of messages to store per topic buffer
//
// Returns:
//   - *TopicBuffers: Initialized collection ready for use
func NewTopicBuffers(capacity int) *TopicBuffers {
	return &TopicBuffers{
		buffers:  make(map[string]*Buffer),
		capacity: capacity,
	}
}

// Push adds a message to the appropriate topic buffer.
// If no buffer exists for the message's topic, a new one is created automatically.
// This method uses a two-phase locking strategy to minimize contention:
//
// Phase 1: Check for existing buffer under read lock (fast path)
// Phase 2: Create new buffer under write lock if needed (slow path)
//
// The message is then added to the topic's ring buffer, potentially overwriting
// the oldest message if the buffer is at capacity.
//
// Parameters:
//   - msg: Message to add to the appropriate topic buffer
func (tb *TopicBuffers) Push(msg message.Message) {
	// Phase 1: Fast path - check if buffer exists (read lock for low contention)
	tb.mtx.RLock()
	buf, exists := tb.buffers[msg.Topic]
	tb.mtx.RUnlock()

	// Phase 2: Slow path - create new buffer if needed (write lock)
	if !exists {
		tb.mtx.Lock()
		// Double-check pattern: another goroutine might have created the buffer
		if buf, exists = tb.buffers[msg.Topic]; !exists {
			buf = NewBuffer(tb.capacity)
			tb.buffers[msg.Topic] = buf
		}
		tb.mtx.Unlock()
	}

	// Add message to the topic's buffer (buffer handles its own thread safety)
	buf.Push(msg)
}

// GetRecentByTopic retrieves all messages currently stored in the specified topic's buffer.
// Messages are returned in chronological order (oldest to newest) as they were received.
// If the topic doesn't exist or has no messages, returns nil.
//
// This method is useful for:
// - Debugging recent activity on a topic
// - Providing recent message history to new consumers
// - Monitoring message patterns and frequency
//
// Parameters:
//   - topic: Name of the topic to retrieve messages from
//
// Returns:
//   - []message.Message: Slice of recent messages in chronological order, or nil if topic not found
func (tb *TopicBuffers) GetRecentByTopic(topic string) []message.Message {
	tb.mtx.RLock()
	defer tb.mtx.RUnlock()

	if buf, exists := tb.buffers[topic]; exists {
		return buf.GetRecent()
	}
	return nil
}

// Buffer implements a thread-safe ring buffer for storing recent messages.
// The ring buffer uses a circular array with head and tail pointers to efficiently
// manage message storage. When the buffer reaches capacity, new messages overwrite
// the oldest ones, maintaining a sliding window of recent activity.
//
// Key characteristics:
// - Fixed capacity with automatic overwrite of old messages
// - O(1) insertion and peek operations
// - O(n) retrieval of all messages where n is current buffer size
// - Thread-safe operations using read-write mutex
// - Preserves message order for chronological retrieval
type Buffer struct {
	mtx      sync.RWMutex      // Protects buffer state during concurrent operations
	data     []message.Message // Circular array storing the actual messages
	head     int               // Index of the oldest message in the buffer
	tail     int               // Index where the next message will be stored
	size     int               // Current number of messages in the buffer
	capacity int               // Maximum number of messages the buffer can hold
}

// NewBuffer creates a new ring buffer with the specified capacity.
// The buffer starts empty and will grow up to the specified capacity before
// beginning to overwrite old messages.
//
// Parameters:
//   - capacity: Maximum number of messages the buffer can store
//
// Returns:
//   - *Buffer: Initialized ring buffer ready for use
func NewBuffer(capacity int) *Buffer {
	return &Buffer{
		data:     make([]message.Message, capacity), // Pre-allocate full capacity
		head:     0,
		tail:     0,
		size:     0,
		capacity: capacity,
	}
}

// Push adds a new message to the ring buffer.
// If the buffer is at capacity, the oldest message is overwritten and the
// head pointer is advanced. This maintains a sliding window of the most
// recent messages while keeping memory usage bounded.
//
// The operation is thread-safe and executes in O(1) time regardless of
// buffer size or current fill level.
//
// Parameters:
//   - msg: Message to add to the buffer
func (b *Buffer) Push(msg message.Message) {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	// Store message at current tail position
	b.data[b.tail] = msg
	b.tail = (b.tail + 1) % b.capacity

	if b.size == b.capacity {
		// Buffer is full: advance head pointer (oldest message gets overwritten)
		b.head = (b.head + 1) % b.capacity
	} else {
		// Buffer still growing: increment size
		b.size++
	}
}

// Peek returns the most recently added message without removing it from the buffer.
// If the buffer is empty, returns a zero-value Message. This operation is useful
// for checking the latest activity without modifying buffer state.
//
// The operation is thread-safe and executes in O(1) time.
//
// Returns:
//   - message.Message: The most recent message, or zero-value if buffer is empty
func (b *Buffer) Peek() message.Message {
	b.mtx.RLock()
	defer b.mtx.RUnlock()

	if b.size == 0 {
		return message.Message{} // Return zero-value for empty buffer
	}

	// Calculate index of most recent message (tail - 1, wrapping around)
	recentIndex := (b.tail - 1 + b.capacity) % b.capacity
	return b.data[recentIndex]
}

// GetRecent returns all messages currently in the buffer in chronological order.
// Messages are returned from oldest to newest, preserving the order they were
// received. If the buffer is empty, returns an empty slice.
//
// This method handles the circular nature of the ring buffer by properly
// reconstructing the chronological sequence even when the data wraps around
// the end of the underlying array.
//
// The operation is thread-safe and creates a copy of the current buffer state,
// so the returned slice won't be affected by subsequent push operations.
//
// Returns:
//   - []message.Message: Slice containing all buffered messages in chronological order
func (b *Buffer) GetRecent() []message.Message {
	b.mtx.RLock()
	defer b.mtx.RUnlock()

	if b.size == 0 {
		return []message.Message{} // Return empty slice for empty buffer
	}

	// Create result slice with exact size needed
	result := make([]message.Message, b.size)

	if b.head < b.tail {
		// Simple case: no wrap-around, copy directly from head to tail
		copy(result, b.data[b.head:b.tail])
	} else {
		// Wrap-around case: copy in two parts
		// Part 1: from head to end of array
		n := copy(result, b.data[b.head:])
		// Part 2: from start of array to tail
		copy(result[n:], b.data[:b.tail])
	}

	return result
}
