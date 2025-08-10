// Package main provides a test reader utility for the Obelisk message broker.
// It reads and displays all stored messages from topic log files in a specified directory,
// sorted by timestamp within each topic.
package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/mush1e/obelisk/internal/storage"
)

// main reads and displays all messages from topic log files in the specified directory.
// It accepts an optional command-line argument to specify the topics directory path.
func main() {
	topicsDir := "data/topics"

	// Allow custom topics directory via command line argument
	if len(os.Args) > 1 {
		topicsDir = os.Args[1]
	}

	fmt.Printf("Reading messages from topics dir: %s\n", topicsDir)

	// Discover all .log files in the topics directory
	files, err := filepath.Glob(filepath.Join(topicsDir, "*.log"))
	if err != nil {
		log.Fatalf("Failed to list topic logs: %v", err)
	}

	if len(files) == 0 {
		fmt.Println("No topic log files found.")
		return
	}

	// Read and display messages from each topic log file
	for _, file := range files {
		// Extract topic name from file path
		topicName := strings.TrimSuffix(filepath.Base(file), ".log")

		messages, err := storage.ReadAllMessages(file)
		if err != nil {
			log.Printf("Failed to read %s: %v", file, err)
			continue
		}

		// Sort messages chronologically within each topic
		sort.Slice(messages, func(i, j int) bool {
			return messages[i].Timestamp.Before(messages[j].Timestamp)
		})

		fmt.Printf("\nTopic: %s (%d messages)\n", topicName, len(messages))
		fmt.Println(strings.Repeat("-", 50))

		// Display each message with index, timestamp, key, and value
		for i, msg := range messages {
			fmt.Printf("%d. %s | %s -> %s\n",
				i+1,
				msg.Timestamp.Format("15:04:05.000"),
				msg.Key,
				msg.Value)
		}
	}
}
