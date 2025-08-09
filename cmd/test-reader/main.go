package main

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/mush1e/obelisk/internal/storage"
)

func main() {
	logFile := "data/segments/test.log"

	if len(os.Args) > 1 {
		logFile = os.Args[1]
	}

	fmt.Printf("Reading messages from: %s\n", logFile)

	messages, err := storage.ReadAllMessages(logFile)
	if err != nil {
		log.Fatalf("Failed to read messages: %v", err)
	}

	fmt.Printf("\nFound %d messages:\n", len(messages))
	fmt.Println(strings.Repeat("=", 50))

	for i, msg := range messages {
		fmt.Printf("%d. %s | %s %s -> %s\n",
			i+1,
			msg.Timestamp.Format("15:04:05.000"),
			msg.Topic,
			msg.Key,
			msg.Value)
	}
}
