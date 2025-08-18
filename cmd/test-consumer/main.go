// Test consumer for the Obelisk message broker with various consumption modes.
package main

import (
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/mush1e/obelisk/internal/consumer"
	"github.com/mush1e/obelisk/internal/message"
)

// main connects to the Obelisk storage layer and runs the specified consumer test mode.
// Use flags to configure topic, consumer ID, directory, and operation mode.
func main() {
	topic := flag.String("topic", "", "Topic to consume from (deprecated, prefer -topics)")
	topicsCSV := flag.String("topics", "orders", "Comma-separated list of topics to consume from")
	consumerID := flag.String("id", "test-consumer", "Consumer ID")
	baseDir := flag.String("dir", "data/topics", "Base directory for topic files")
	mode := flag.String("mode", "poll", "Mode: poll, continuous, reset, partition-test")
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
		log.Fatal("no topics provided; use -topics=orders,payments or -topic=orders")
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
	case "partition-test":
		testPartitionedConsumption(c, topics)
	default:
		fmt.Println("Unknown mode. Use: poll, continuous, reset, partition-test")
	}
}

// testPartitionedConsumption specifically tests partitioned topic consumption
func testPartitionedConsumption(c *consumer.Consumer, topics []string) {
	fmt.Println("\n=== Partitioned Topic Test ===")
	fmt.Println("This test shows how messages are consumed from partitioned topics")

	for _, topic := range topics {
		fmt.Printf("\n-- Testing topic: %s --\n", topic)

		// Get current offset
		offset, err := c.GetCurrentOffset(topic)
		if err != nil {
			log.Printf("Error getting offset for %s: %v", topic, err)
			continue
		}
		fmt.Printf("Current combined offset: %d\n", offset)

		// Show total messages available
		total, err := c.GetTopicMessageCount(topic)
		if err != nil {
			log.Printf("Error getting message count for %s: %v", topic, err)
		} else {
			fmt.Printf("Total messages across all partitions: %d\n", total)
		}

		// Poll messages
		fmt.Println("\nPolling for messages...")
		messages, err := c.Poll(topic)
		if err != nil {
			log.Printf("Error polling %s: %v", topic, err)
			continue
		}

		fmt.Printf("Retrieved %d messages (sorted by timestamp):\n", len(messages))

		// Group messages by key to show partitioning effect
		messagesByKey := make(map[string][]message.Message)
		for _, msg := range messages {
			messagesByKey[msg.Key] = append(messagesByKey[msg.Key], msg)
		}

		// Display messages grouped by key
		fmt.Println("\nMessages grouped by key (showing partitioning):")
		for key, msgs := range messagesByKey {
			fmt.Printf("\n  Key: %s (%d messages)\n", key, len(msgs))
			for i, msg := range msgs {
				if i < 3 { // Show first 3 messages per key
					fmt.Printf("    [%s] %s\n",
						msg.Timestamp.Format("15:04:05.000"),
						msg.Value)
				}
			}
			if len(msgs) > 3 {
				fmt.Printf("    ... and %d more\n", len(msgs)-3)
			}
		}

		// Commit offset
		if len(messages) > 0 {
			newOffset := offset + uint64(len(messages))
			if err := c.Commit(topic, newOffset); err != nil {
				log.Printf("Error committing %s: %v", topic, err)
			} else {
				fmt.Printf("\nCommitted offset: %d\n", newOffset)
			}
		}
	}

	fmt.Println("\n=== Partition test complete ===")
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

		// Show first 10 messages
		displayCount := min(10, len(messages))
		for i := 0; i < displayCount; i++ {
			msg := messages[i]
			fmt.Printf("  %d. [%s] %s -> %s\n",
				i+1,
				msg.Timestamp.Format("15:04:05.000"),
				msg.Key,
				msg.Value)
		}

		if len(messages) > displayCount {
			fmt.Printf("  ... and %d more messages\n", len(messages)-displayCount)
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

				// Show first 5 messages
				displayCount := min(5, len(messages))
				for i := 0; i < displayCount; i++ {
					msg := messages[i]
					fmt.Printf("  %d. %s -> %s\n", i+1, msg.Key, msg.Value)
				}

				if len(messages) > displayCount {
					fmt.Printf("  ... and %d more\n", len(messages)-displayCount)
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

		// Show first 5 messages
		displayCount := min(5, len(messages))
		for i := 0; i < displayCount; i++ {
			msg := messages[i]
			fmt.Printf("  %d. [%s] %s -> %s\n",
				i+1,
				msg.Timestamp.Format("15:04:05.000"),
				msg.Key,
				msg.Value)
		}

		if len(messages) > displayCount {
			fmt.Printf("  ... and %d more messages\n", len(messages)-displayCount)
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
