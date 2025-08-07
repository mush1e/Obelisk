package buffer

import "github.com/mush1e/obelisk/internal/message"

// This package provides a buffer for messages.
// It is used to store messages that are received from the network.
// The buffer is a ring buffer, which means that it will overwrite
// the oldest message when it is full.

type Buffer struct {
	// make some sort of ring buffer for messages
	Messages []message.Message
}
