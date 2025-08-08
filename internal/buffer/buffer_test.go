package buffer

import (
	"testing"

	"github.com/mush1e/obelisk/internal/message"
)

func TestBufferPushAndOverwrite(t *testing.T) {
	buf := NewBuffer(3)

	buf.Push(message.Message{Key: "msg1", Value: "first"})
	buf.Push(message.Message{Key: "msg2", Value: "second"})
	buf.Push(message.Message{Key: "msg3", Value: "third"})

	if len(buf.GetRecent()) != 3 {
		t.Errorf("Expected buffer size 3, got %d", len(buf.GetRecent()))
	}

	// Push another — oldest should be overwritten
	buf.Push(message.Message{Key: "msg4", Value: "fourth"})
	recent := buf.GetRecent()

	if len(recent) != 3 {
		t.Errorf("Expected buffer size 3 after overwrite, got %d", len(recent))
	}

	if recent[0].Key != "msg2" {
		t.Errorf("Expected oldest to be msg2 after overwrite, got %s", recent[0].Key)
	}
}
