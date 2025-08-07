package buffer

import "github.com/mush1e/obelisk/internal/message"

// This package provides a buffer for messages.
// It is used to store messages that are received from the network.
// The buffer is a ring buffer, which means that it will overwrite
// the oldest message when it is full.

type Buffer struct {
	data     []message.Message
	head     int
	tail     int
	size     int
	capacity int
}

func NewBuffer(capacity int) *Buffer {
	return &Buffer{
		data:     make([]message.Message, capacity),
		head:     0,
		tail:     0,
		size:     0,
		capacity: capacity,
	}
}

func (b *Buffer) Push(msg message.Message) {
	b.data[b.tail] = msg
	b.tail = (b.tail + 1) % b.capacity

	if b.size == b.capacity {
		// Buffer full: move head forward (oldest message just got overwritten)
		b.head = (b.head + 1) % b.capacity
	} else {
		b.size++
	}
}

func (b *Buffer) Peek() message.Message {
	if b.size == 0 {
		return message.Message{}
	}
	return b.data[b.head]
}

func (b *Buffer) GetRecent() []message.Message {
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
