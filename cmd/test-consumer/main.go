// Test consumer for the Obelisk message broker with various consumption modes.
package main

import (
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/mush1e/obelisk/internal/consumer"
)

// main connects to the Obelisk storage layer and runs the specified consumer test mode.
// Use flags to configure topic, consumer ID, directory, and operation mode.
func main() {
	topic := flag.String("topic", "", "Topic to consume from (deprecated, prefer -topics)")
	topicsCSV := flag.String("topics", "topic-0", "Comma-separated list of topics to consume from")
	consumerID := flag.String("id", "test-consumer", "Consumer ID")
	baseDir := flag.String("dir", "data/topics", "Base directory for topic files")
	mode := flag.String("mode", "poll", "Mode: poll, continuous, reset")
	flag.Parse()

	// Determine topics
	topics := []string{}
	if *topicsCSV != "" {
		for _, t := range strings.Split(*topicsCSV, ",") {
			t = strings.TrimSpace(t)
			if t != "" {
				topics = append(topics, t)
			}
		}
	}
	if len(topics) == 0 && *topic != "" {
		topics = []string{*topic}
	}
	if len(topics) == 0 {
		log.Fatal("no topics provided; use -topics=topic-0,topic-1 or -topic=topic-0")
	}

	fmt.Printf("Starting consumer '%s' for topics %v\n", *consumerID, topics)
	fmt.Printf("Reading from directory: %s\n", *baseDir)

	// Create new consumer instance subscribed to the specified topics (with persisted offsets)
	c := consumer.NewConsumer(*baseDir, *consumerID, topics...)

	// Run the specified test mode
	switch *mode {
	case "poll":
		testSinglePoll(c, topics)
	case "continuous":
		testContinuousPolling(c, topics)
	case "reset":
		testResetAndPoll(c, topics)
	default:
		fmt.Println("Unknown mode. Use: poll, continuous, reset")
	}
}

// testSinglePoll performs a single poll operation to retrieve all available messages
// from the current offset and commits the new offset after processing.
func testSinglePoll(c *consumer.Consumer, topics []string) {
	fmt.Println("\n=== Single Poll Test ===")

	for _, topic := range topics {
		fmt.Printf("\n-- Topic: %s --\n", topic)

		// Display current consumer state
		offset, err := c.GetCurrentOffset(topic)
		if err != nil {
			log.Printf("Error getting offset for %s: %v", topic, err)
			continue
		}
		fmt.Printf("Current offset: %d\n", offset)

		// Show total messages available in the topic
		total, err := c.GetTopicMessageCount(topic)
		if err != nil {
			log.Printf("Error getting message count for %s: %v", topic, err)
		} else {
			fmt.Printf("Total messages in topic: %d\n", total)
		}

		// Poll for all messages from current offset
		messages, err := c.Poll(topic)
		if err != nil {
			log.Printf("Error polling %s: %v", topic, err)
			continue
		}

		fmt.Printf("Retrieved %d messages:\n", len(messages))
		for i, msg := range messages {
			fmt.Printf("  %d. [%s] %s -> %s\n",
				i+1,
				msg.Timestamp.Format("15:04:05.000"),
				msg.Key,
				msg.Value)
		}

		if len(messages) > 0 {
			newOffset := offset + uint64(len(messages))
			if err := c.Commit(topic, newOffset); err != nil {
				log.Printf("Error committing %s: %v", topic, err)
			} else {
				fmt.Printf("Committed offset: %d\n", newOffset)
			}
		}
	}
}

// testContinuousPolling runs a continuous polling loop that checks for new messages
// every 3 seconds and processes them as they arrive.
func testContinuousPolling(c *consumer.Consumer, topics []string) {
	fmt.Println("\n=== Continuous Polling Test ===")
	fmt.Println("Polling every 3 seconds. Press Ctrl+C to stop...")

	for {
		for _, topic := range topics {
			offset, _ := c.GetCurrentOffset(topic)
			fmt.Printf("\n[%s] Polling %s from offset %d...\n",
				time.Now().Format("15:04:05"), topic, offset)

			messages, err := c.Poll(topic)
			if err != nil {
				log.Printf("Error polling %s: %v", topic, err)
				continue
			}

			if len(messages) == 0 {
				fmt.Println("No new messages")
			} else {
				fmt.Printf("Got %d new messages:\n", len(messages))
				for i, msg := range messages {
					fmt.Printf("  %d. %s -> %s\n", i+1, msg.Key, msg.Value)
				}

				newOffset := offset + uint64(len(messages))
				if err := c.Commit(topic, newOffset); err != nil {
					log.Printf("Error committing %s: %v", topic, err)
				} else {
					fmt.Printf("Committed offset: %d\n", newOffset)
				}
			}
		}
		time.Sleep(3 * time.Second)
	}
}

// testResetAndPoll demonstrates resetting the consumer offset to 0 and reading
// all messages from the beginning of the topic.
func testResetAndPoll(c *consumer.Consumer, topics []string) {
	fmt.Println("\n=== Reset and Poll Test ===")

	for _, topic := range topics {
		fmt.Printf("\n-- Topic: %s --\n", topic)
		offset, _ := c.GetCurrentOffset(topic)
		fmt.Printf("Current offset: %d\n", offset)

		if err := c.Reset(topic); err != nil {
			log.Printf("Error resetting %s: %v", topic, err)
			continue
		}
		fmt.Println("Reset consumer to offset 0")

		messages, err := c.Poll(topic)
		if err != nil {
			log.Printf("Error polling %s: %v", topic, err)
			continue
		}

		fmt.Printf("Retrieved %d messages from beginning:\n", len(messages))
		for i, msg := range messages[:min(5, len(messages))] {
			fmt.Printf("  %d. [%s] %s -> %s\n",
				i+1,
				msg.Timestamp.Format("15:04:05.000"),
				msg.Key,
				msg.Value)
		}
		if len(messages) > 5 {
			fmt.Printf("  ... and %d more messages\n", len(messages)-5)
		}
	}
}

// min returns the smaller of two integers.
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
