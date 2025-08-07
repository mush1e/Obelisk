package main

import (
	"fmt"
	"time"

	"github.com/mush1e/obelisk/internal/message"
	"github.com/mush1e/obelisk/internal/storage"
)

func main() {
	msg := message.Message{
		Timestamp: time.Now(),
		Key:       "user123",
		Value:     "Hello, Obelisk!",
	}

	// Test Binary
	binData, _ := message.Serialize(msg)
	msgFromBin, _ := message.Deserialize(binData)
	fmt.Printf("Binary size: %d bytes\n", len(binData))
	fmt.Printf("Binary works: %v\n", msg.Key == msgFromBin.Key)

	// Test the file operations
	msg1 := message.Message{Timestamp: time.Now(), Key: "user1", Value: "hello"}
	msg2 := message.Message{Timestamp: time.Now(), Key: "user2", Value: "world"}

	// Write messages
	logPath := "data/segments/test.log"
	storage.AppendMessage(logPath, msg1)
	storage.AppendMessage(logPath, msg2)

	// Read them back
	messages, err := storage.ReadAllMessages(logPath)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
	} else {
		fmt.Printf("Read %d messages:\n", len(messages))
		for i, msg := range messages {
			fmt.Printf("  %d: %s -> %s\n", i, msg.Key, msg.Value)
		}
	}
}
