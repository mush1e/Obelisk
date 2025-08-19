// Test reader utility for displaying stored messages from topic log files.
package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/mush1e/obelisk/internal/message"
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

	// Discover all topic directories
	topicDirs, err := filepath.Glob(filepath.Join(topicsDir, "*"))
	if err != nil {
		log.Fatalf("Failed to list topic directories: %v", err)
	}

	totalMessages := 0
	for _, topicDir := range topicDirs {
		// Check if it's a directory
		if info, err := os.Stat(topicDir); err != nil || !info.IsDir() {
			continue
		}

		topicName := filepath.Base(topicDir)
		fmt.Printf("\nTopic: %s\n", topicName)
		fmt.Println(strings.Repeat("=", 60))

		// Find all partition log files in this topic directory
		partitionFiles, err := filepath.Glob(filepath.Join(topicDir, "partition-*.log"))
		if err != nil {
			log.Printf("Failed to list partition files for topic %s: %v", topicName, err)
			continue
		}

		if len(partitionFiles) == 0 {
			fmt.Printf("No partition log files found for topic %s\n", topicName)
			continue
		}

		var allMessages []message.Message
		for _, partitionFile := range partitionFiles {
			partitionName := strings.TrimSuffix(filepath.Base(partitionFile), ".log")
			
			messages, err := storage.ReadAllMessages(partitionFile)
			if err != nil {
				log.Printf("Failed to read %s: %v", partitionFile, err)
				continue
			}

			fmt.Printf("\n--- %s (%d messages) ---\n", partitionName, len(messages))
			
			// Show first 5 messages from each partition
			for i, msg := range messages {
				if i >= 5 {
					fmt.Printf("... (showing first 5 messages)\n")
					break
				}
				fmt.Printf("%d. [%s] %s -> %s\n",
					i+1,
					msg.Timestamp.Format("15:04:05.000"),
					msg.Key,
					truncateString(msg.Value, 50))
			}

			allMessages = append(allMessages, messages...)
		}

		// Sort all messages chronologically
		sort.Slice(allMessages, func(i, j int) bool {
			return allMessages[i].Timestamp.Before(allMessages[j].Timestamp)
		})

		fmt.Printf("\nTopic %s summary: %d total messages\n", topicName, len(allMessages))
		totalMessages += len(allMessages)
	}

	fmt.Printf("\nGrand total: %d messages across all topics\n", totalMessages)
}

func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}
