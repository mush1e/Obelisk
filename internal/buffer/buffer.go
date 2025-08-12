package buffer

// Ring buffer implementation for storing recent messages per topic.
// Uses circular buffers with automatic overflow handling.

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
	mtx      sync.RWMutex
	buffers  map[string]*Buffer
	capacity int
}

// NewTopicBuffers creates a new collection of topic buffers.
func NewTopicBuffers(capacity int) *TopicBuffers {
	return &TopicBuffers{
		buffers:  make(map[string]*Buffer),
		capacity: capacity,
	}
}

// Push adds a message to its topic buffer, creating the buffer if needed.
func (tb *TopicBuffers) Push(msg message.Message) {
	// Fast path: check if buffer exists
	tb.mtx.RLock()
	buf, exists := tb.buffers[msg.Topic]
	tb.mtx.RUnlock()

	// Create buffer if needed
	if !exists {
		tb.mtx.Lock()
		// Double-check pattern
		if buf, exists = tb.buffers[msg.Topic]; !exists {
			buf = NewBuffer(tb.capacity)
			tb.buffers[msg.Topic] = buf
		}
		tb.mtx.Unlock()
	}

	buf.Push(msg)
}

// GetRecentByTopic returns recent messages for a topic in chronological order.
func (tb *TopicBuffers) GetRecentByTopic(topic string) []message.Message {
	tb.mtx.RLock()
	defer tb.mtx.RUnlock()

	if buf, exists := tb.buffers[topic]; exists {
		return buf.GetRecent()
	}
	return nil
}

// Buffer implements a thread-safe ring buffer with fixed capacity.
type Buffer struct {
	mtx      sync.RWMutex
	data     []message.Message
	head     int
	tail     int
	size     int
	capacity int
}

// NewBuffer creates a new ring buffer with the specified capacity.
func NewBuffer(capacity int) *Buffer {
	return &Buffer{
		data:     make([]message.Message, capacity),
		head:     0,
		tail:     0,
		size:     0,
		capacity: capacity,
	}
}

// Push adds a message to the ring buffer, overwriting old messages when full.
func (b *Buffer) Push(msg message.Message) {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	b.data[b.tail] = msg
	b.tail = (b.tail + 1) % b.capacity

	if b.size == b.capacity {
		b.head = (b.head + 1) % b.capacity
	} else {
		b.size++
	}
}

// Peek returns the most recent message without removing it.
func (b *Buffer) Peek() message.Message {
	b.mtx.RLock()
	defer b.mtx.RUnlock()

	if b.size == 0 {
		return message.Message{}
	}

	recentIndex := (b.tail - 1 + b.capacity) % b.capacity
	return b.data[recentIndex]
}

// GetRecent returns all buffered messages in chronological order.
func (b *Buffer) GetRecent() []message.Message {
	b.mtx.RLock()
	defer b.mtx.RUnlock()

	if b.size == 0 {
		return []message.Message{}
	}

	result := make([]message.Message, b.size)

	if b.head < b.tail {
		copy(result, b.data[b.head:b.tail])
	} else {
		n := copy(result, b.data[b.head:])
		copy(result[n:], b.data[:b.tail])
	}

	return result
}
