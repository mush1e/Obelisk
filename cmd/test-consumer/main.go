// Test consumer for the Obelisk message broker with various consumption modes.
package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/mush1e/obelisk/internal/consumer"
)

// main connects to the Obelisk storage layer and runs the specified consumer test mode.
// Use flags to configure topic, consumer ID, directory, and operation mode.
func main() {
	topic := flag.String("topic", "topic-0", "Topic to consume from")
	consumerID := flag.String("id", "test-consumer", "Consumer ID")
	baseDir := flag.String("dir", "data/topics", "Base directory for topic files")
	mode := flag.String("mode", "poll", "Mode: poll, continuous, reset")
	flag.Parse()

	fmt.Printf("Starting consumer '%s' for topic '%s'\n", *consumerID, *topic)
	fmt.Printf("Reading from directory: %s\n", *baseDir)

	// Create new consumer instance subscribed to the specified topic
	consumer := consumer.NewConsumer(*baseDir, *topic)

	// Run the specified test mode
	switch *mode {
	case "poll":
		testSinglePoll(consumer, *topic)
	case "continuous":
		testContinuousPolling(consumer, *topic)
	case "reset":
		testResetAndPoll(consumer, *topic)
	default:
		fmt.Println("Unknown mode. Use: poll, continuous, reset")
	}
}

// testSinglePoll performs a single poll operation to retrieve all available messages
// from the current offset and commits the new offset after processing.
func testSinglePoll(c *consumer.Consumer, topic string) {
	fmt.Println("\n=== Single Poll Test ===")

	// Display current consumer state
	offset, err := c.GetCurrentOffset(topic)
	if err != nil {
		log.Printf("Error getting offset: %v", err)
		return
	}
	fmt.Printf("Current offset: %d\n", offset)

	// Show total messages available in the topic
	total, err := c.GetTopicMessageCount(topic)
	if err != nil {
		log.Printf("Error getting message count: %v", err)
	} else {
		fmt.Printf("Total messages in topic: %d\n", total)
	}

	// Poll for all messages from current offset
	messages, err := c.Poll(topic)
	if err != nil {
		log.Printf("Error polling: %v", err)
		return
	}

	fmt.Printf("Retrieved %d messages:\n", len(messages))
	// Display retrieved messages with timestamp and content
	for i, msg := range messages {
		fmt.Printf("  %d. [%s] %s -> %s\n",
			i+1,
			msg.Timestamp.Format("15:04:05.000"),
			msg.Key,
			msg.Value)
	}

	if len(messages) > 0 {
		// Update consumer offset to reflect processed messages
		newOffset := offset + uint64(len(messages))
		if err := c.Commit(topic, newOffset); err != nil {
			log.Printf("Error committing: %v", err)
		} else {
			fmt.Printf("Committed offset: %d\n", newOffset)
		}
	}
}

// testContinuousPolling runs a continuous polling loop that checks for new messages
// every 3 seconds and processes them as they arrive.
func testContinuousPolling(c *consumer.Consumer, topic string) {
	fmt.Println("\n=== Continuous Polling Test ===")
	fmt.Println("Polling every 3 seconds. Press Ctrl+C to stop...")

	// Continuous polling loop
	for {
		offset, _ := c.GetCurrentOffset(topic)
		fmt.Printf("\n[%s] Polling from offset %d...\n",
			time.Now().Format("15:04:05"), offset)

		messages, err := c.Poll(topic)
		if err != nil {
			log.Printf("Error polling: %v", err)
			time.Sleep(3 * time.Second)
			continue
		}

		if len(messages) == 0 {
			fmt.Println("No new messages")
		} else {
			fmt.Printf("Got %d new messages:\n", len(messages))
			for i, msg := range messages {
				fmt.Printf("  %d. %s -> %s\n", i+1, msg.Key, msg.Value)
			}

			// Commit offset after successfully processing messages
			newOffset := offset + uint64(len(messages))
			if err := c.Commit(topic, newOffset); err != nil {
				log.Printf("Error committing: %v", err)
			} else {
				fmt.Printf("Committed offset: %d\n", newOffset)
			}
		}

		time.Sleep(3 * time.Second)
	}
}

// testResetAndPoll demonstrates resetting the consumer offset to 0 and reading
// all messages from the beginning of the topic.
func testResetAndPoll(c *consumer.Consumer, topic string) {
	fmt.Println("\n=== Reset and Poll Test ===")

	// Display current consumer state before reset
	offset, _ := c.GetCurrentOffset(topic)
	fmt.Printf("Current offset: %d\n", offset)

	// Reset consumer offset to start from beginning
	if err := c.Reset(topic); err != nil {
		log.Printf("Error resetting: %v", err)
		return
	}
	fmt.Println("Reset consumer to offset 0")

	// Poll all messages from the beginning of the topic
	messages, err := c.Poll(topic)
	if err != nil {
		log.Printf("Error polling: %v", err)
		return
	}

	fmt.Printf("Retrieved %d messages from beginning:\n", len(messages))
	// Show first few messages to avoid overwhelming output
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

// min returns the smaller of two integers.
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
