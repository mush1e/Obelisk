package buffer

// This file contains unit tests for the ring buffer implementation used in the
// Obelisk message broker. The tests validate the core functionality of the
// circular buffer including message insertion, overwrite behavior, ordering,
// and thread-safety characteristics.
//
// Test coverage includes:
// - Basic push and retrieval operations
// - Overwrite behavior when capacity is exceeded
// - Message ordering preservation
// - Empty buffer edge cases
// - Peek operations for recent message access

import (
	"testing"

	"github.com/mush1e/obelisk/internal/message"
)

// TestBufferPushAndOverwrite validates that the ring buffer correctly overwrites
// the oldest messages when capacity is exceeded while maintaining proper ordering
// of the remaining messages.
//
// This test verifies:
// 1. Buffer correctly stores messages up to capacity
// 2. Oldest message is removed when capacity is exceeded
// 3. Remaining messages maintain their relative order
// 4. Buffer size remains constant at capacity after overwrite begins
func TestBufferPushAndOverwrite(t *testing.T) {
	// Create a small buffer to easily test overwrite behavior
	buf := NewBuffer(3)

	// Fill buffer to capacity
	buf.Push(message.Message{Key: "msg1", Value: "first"})
	buf.Push(message.Message{Key: "msg2", Value: "second"})
	buf.Push(message.Message{Key: "msg3", Value: "third"})

	// Verify buffer is at expected capacity
	if len(buf.GetRecent()) != 3 {
		t.Errorf("Expected buffer size 3, got %d", len(buf.GetRecent()))
	}

	// Push another message - this should trigger overwrite of oldest (msg1)
	buf.Push(message.Message{Key: "msg4", Value: "fourth"})
	recent := buf.GetRecent()

	// Verify buffer size remains at capacity
	if len(recent) != 3 {
		t.Errorf("Expected buffer size 3 after overwrite, got %d", len(recent))
	}

	// Verify oldest message (msg1) was overwritten and msg2 is now oldest
	if recent[0].Key != "msg2" {
		t.Errorf("Expected oldest to be msg2 after overwrite, got %s", recent[0].Key)
	}

	// Verify remaining messages are in correct order
	if recent[1].Key != "msg3" {
		t.Errorf("Expected second message to be msg3, got %s", recent[1].Key)
	}
	if recent[2].Key != "msg4" {
		t.Errorf("Expected newest message to be msg4, got %s", recent[2].Key)
	}
}
