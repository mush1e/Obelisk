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

func main() {
	topicsDir := "data/topics"

	if len(os.Args) > 1 {
		topicsDir = os.Args[1]
	}

	fmt.Printf("Reading messages from topics dir: %s\n", topicsDir)

	// Find all log files in the directory
	files, err := filepath.Glob(filepath.Join(topicsDir, "*.log"))
	if err != nil {
		log.Fatalf("Failed to list topic logs: %v", err)
	}

	if len(files) == 0 {
		fmt.Println("No topic log files found.")
		return
	}

	// Process each topic file separately
	for _, file := range files {
		topicName := strings.TrimSuffix(filepath.Base(file), ".log")

		messages, err := storage.ReadAllMessages(file)
		if err != nil {
			log.Printf("Failed to read %s: %v", file, err)
			continue
		}

		// Sort messages within this topic by timestamp
		sort.Slice(messages, func(i, j int) bool {
			return messages[i].Timestamp.Before(messages[j].Timestamp)
		})

		fmt.Printf("\nTopic: %s (%d messages)\n", topicName, len(messages))
		fmt.Println(strings.Repeat("-", 50))

		for i, msg := range messages {
			fmt.Printf("%d. %s | %s -> %s\n",
				i+1,
				msg.Timestamp.Format("15:04:05.000"),
				msg.Key,
				msg.Value)
		}
	}
}
