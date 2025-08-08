package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mush1e/obelisk/internal/buffer"
	"github.com/mush1e/obelisk/internal/message"
	"github.com/mush1e/obelisk/internal/server"
	"github.com/mush1e/obelisk/internal/storage"
)

func main() {
	gracefulShutdown := make(chan os.Signal, 1)
	signal.Notify(gracefulShutdown, syscall.SIGINT, syscall.SIGTERM)
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

	buf := buffer.NewBuffer(3)

	// Add some messages
	buf.Push(message.Message{Key: "msg1", Value: "first"})
	buf.Push(message.Message{Key: "msg2", Value: "second"})
	buf.Push(message.Message{Key: "msg3", Value: "third"})

	fmt.Printf("Buffer has %d messages\n", len(buf.GetRecent()))

	// Add one more to trigger overwrite
	buf.Push(message.Message{Key: "msg4", Value: "fourth"})

	recent := buf.GetRecent()
	fmt.Printf("After overwrite, buffer has %d messages:\n", len(recent))
	for i, msg := range recent {
		fmt.Printf("  %d: %s -> %s\n", i, msg.Key, msg.Value)
	}

	srv := server.NewServer(":8080")
	if err := srv.Start(); err != nil {
		fmt.Printf("Failed to start server: %v\n", err)
		return
	}

	fmt.Println("Server running. Press Ctrl+C to stop...")
	<-gracefulShutdown
	fmt.Println("\nServer shutting down!")
	if err := srv.Stop(); err != nil {
		fmt.Println("error while graceful shutdown : ", err)
	}
}
