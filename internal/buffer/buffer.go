package buffer

import (
	"sync"

	"github.com/mush1e/obelisk/internal/message"
)

// This package provides a buffer for messages.
// It is used to store messages that are received from the network.
// The buffer is a ring buffer, which means that it will overwrite
// the oldest message when it is full.

// TopicBuffers represents a collection of buffers for different topics
type TopicBuffers struct {
	mtx      sync.RWMutex
	buffers  map[string]*Buffer
	capacity int
}

// NewTopicBuffers creates a new TopicBuffers instance with the specified capacity.
func NewTopicBuffers(capacity int) *TopicBuffers {
	return &TopicBuffers{
		buffers:  make(map[string]*Buffer),
		capacity: capacity,
	}
}

// Push adds a message to the buffer for the specified topic.
func (tb *TopicBuffers) Push(msg message.Message) {
	tb.mtx.RLock()
	buf, exists := tb.buffers[msg.Topic]
	tb.mtx.RUnlock()

	if !exists {
		tb.mtx.Lock()
		buf = NewBuffer(tb.capacity)
		tb.buffers[msg.Topic] = buf
		tb.mtx.Unlock()
	}

	buf.Push(msg)
}

// GetRecent gets all recent messages in the buffer for the specified topic, in order.
func (tb *TopicBuffers) GetRecentByTopic(topic string) []message.Message {
	tb.mtx.RLock()
	defer tb.mtx.RUnlock()
	if buf, exists := tb.buffers[topic]; exists {
		return buf.GetRecent()
	}
	return nil
}

// Buffer represents a queue (ring buffer) to keep track of recent events and for fast retrieval
type Buffer struct {
	mtx      sync.RWMutex
	data     []message.Message
	head     int
	tail     int
	size     int
	capacity int
}

// NewBuffer creates a new Buffer for messages
func NewBuffer(capacity int) *Buffer {
	return &Buffer{
		data:     make([]message.Message, capacity),
		head:     0,
		tail:     0,
		size:     0,
		capacity: capacity,
	}
}

// Push adds new messages to the ring buffer
func (b *Buffer) Push(msg message.Message) {
	b.mtx.Lock()
	defer b.mtx.Unlock()
	b.data[b.tail] = msg
	b.tail = (b.tail + 1) % b.capacity

	if b.size == b.capacity {
		// Buffer full: move head forward (oldest message just got overwritten)
		b.head = (b.head + 1) % b.capacity
	} else {
		b.size++
	}
}

// Peak returns the most recent message which was recieved/processed
func (b *Buffer) Peek() message.Message {
	b.mtx.RLock()
	defer b.mtx.RUnlock()
	if b.size == 0 {
		return message.Message{}
	}
	return b.data[(b.tail-1+b.capacity)%b.capacity]
}

// GetRecent gets all recent messages in our buffer, in order
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
		// Wrapped case: copy from head to end, then from start to tail
		n := copy(result, b.data[b.head:])
		copy(result[n:], b.data[:b.tail])
	}

	return result
}
