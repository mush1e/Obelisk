package storage

import (
	"os"
	"testing"
	"time"

	"github.com/mush1e/obelisk/internal/message"
)

func TestAppendAndRead(t *testing.T) {
	tmpFile := "test_segment.log"
	defer os.Remove(tmpFile)

	msg1 := message.Message{Timestamp: time.Now(), Key: "user1", Value: "hello"}
	msg2 := message.Message{Timestamp: time.Now(), Key: "user2", Value: "world"}

	if err := AppendMessage(tmpFile, msg1); err != nil {
		t.Fatalf("AppendMessage failed: %v", err)
	}
	if err := AppendMessage(tmpFile, msg2); err != nil {
		t.Fatalf("AppendMessage failed: %v", err)
	}

	msgs, err := ReadAllMessages(tmpFile)
	if err != nil {
		t.Fatalf("ReadAllMessages failed: %v", err)
	}

	if len(msgs) != 2 {
		t.Errorf("Expected 2 messages, got %d", len(msgs))
	}
}
