// Test client for the Obelisk message broker with various batching scenarios.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/mush1e/obelisk/internal/message"
	"github.com/mush1e/obelisk/pkg/protocol"
)

// main connects to the Obelisk server and runs the specified test scenario.
// Use -test flag to specify test type: size, time, realistic, or partition.
func main() {
	testType := flag.String("test", "partition", "Test type: size, time, realistic, partition")
	flag.Parse()

	// Connect to the Obelisk server
	addr := "localhost:8080"
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		log.Fatal("Could not dial " + addr)
	}
	defer conn.Close()

	writer := bufio.NewWriter(conn)

	// Run the specified test scenario
	switch *testType {
	case "size":
		testSizeBatching(writer)
	case "time":
		testTimeBatching(writer)
	case "realistic":
		testRealisticLoad(writer)
	case "partition":
		testPartitioning(writer)
	default:
		fmt.Println("Unknown test type. Use: size, time, realistic, or partition")
	}
}

// testPartitioning tests the partitioning functionality
func testPartitioning(writer *bufio.Writer) {
	fmt.Println("Testing partitioning with consistent topic names...")
	fmt.Println("Sending messages to 'orders' topic with different keys")
	fmt.Println("Keys with same hash should go to same partition")

	// Test with a single topic but different keys
	topic := "orders"

	// These keys should distribute across partitions
	testKeys := []string{
		"user-123",
		"user-456",
		"user-789",
		"user-111",
		"user-222",
		"user-333",
		"user-444",
		"user-555",
		"user-666",
		"user-777",
	}

	for i := 0; i < 30; i++ {
		key := testKeys[i%len(testKeys)]

		msg := message.Message{
			Timestamp: time.Now(),
			Topic:     topic,
			Key:       key,
			Value:     fmt.Sprintf("Order %d for %s", i, key),
		}

		msgBytes, _ := message.Serialize(msg)
		if err := protocol.WriteMessage(writer, msgBytes); err != nil {
			log.Printf("Error sending message %d: %v", i, err)
		}

		fmt.Printf("Sent: Topic=%s, Key=%s, Value='Order %d for %s'\n",
			topic, key, i, key)

		// Small delay to see the partitioning in action
		time.Sleep(100 * time.Millisecond)
	}

	fmt.Println("\nPartitioning test complete!")
	fmt.Println("Check the logs to see how messages were distributed across partitions")
	fmt.Println("Look for patterns like 'orders::0', 'orders::1', etc.")

	// Wait a bit for final flush
	time.Sleep(6 * time.Second)
}

// testSizeBatching tests size-based batch flushing by sending 150 messages quickly.
// This should trigger multiple flushes when the batch size limit (100) is reached.
func testSizeBatching(writer *bufio.Writer) {
	fmt.Println("Testing size-based batching (sending 150 messages quickly)...")

	start := time.Now()
	for i := 0; i < 150; i++ {
		// Use consistent topic names (not topic-0, topic-1, etc.)
		topics := []string{"orders", "payments", "inventory"}
		topic := topics[i%3]

		msg := message.Message{
			Timestamp: time.Now(),
			Topic:     topic,
			Key:       fmt.Sprintf("batch-user%d", i),
			Value:     fmt.Sprintf("Size test message %d", i),
		}

		// Send message to server
		msgBytes, _ := message.Serialize(msg)
		if err := protocol.WriteMessage(writer, msgBytes); err != nil {
			log.Printf("Error sending message %d: %v", i, err)
		}

		if i%50 == 0 {
			fmt.Printf("Sent %d messages...\n", i)
		}
	}

	fmt.Printf("Sent 150 messages in %v\n", time.Since(start))
	fmt.Println("Should see multiple flushes when hitting maxSize (100)")
}

// testTimeBatching tests time-based batch flushing by sending messages with delays.
// This should trigger flushes based on the time threshold (5 seconds) rather than size.
func testTimeBatching(writer *bufio.Writer) {
	fmt.Println("Testing time-based batching (5 messages with 2s delays)...")

	for i := 0; i < 5; i++ {
		// Use consistent topic names
		topics := []string{"orders", "payments", "inventory"}
		topic := topics[i%3]

		msg := message.Message{
			Timestamp: time.Now(),
			Topic:     topic,
			Key:       fmt.Sprintf("time-user%d", i),
			Value:     fmt.Sprintf("Time test message %d", i),
		}

		// Send message and wait to test time-based flushing
		msgBytes, _ := message.Serialize(msg)
		protocol.WriteMessage(writer, msgBytes)

		fmt.Printf("Sent message %d to topic '%s', waiting 2 seconds...\n", i+1, topic)
		time.Sleep(2 * time.Second)
	}

	fmt.Println("Done. Should see flush after 5-second timeout")
	fmt.Println("Wait 5 more seconds to see final flush...")
	time.Sleep(6 * time.Second)
}

// testRealisticLoad simulates a realistic message load with variable delays.
// This tests how the batcher performs under more realistic usage patterns.
func testRealisticLoad(writer *bufio.Writer) {
	fmt.Println("Testing realistic load (messages over time)...")

	for i := 0; i < 50; i++ {
		// Use consistent topic names
		topics := []string{"orders", "payments", "inventory"}
		topic := topics[i%3]

		msg := message.Message{
			Timestamp: time.Now(),
			Topic:     topic,
			Key:       fmt.Sprintf("real-user%d", i),
			Value:     fmt.Sprintf("Realistic message %d", i),
		}

		// Send message with variable delays to simulate real usage
		msgBytes, _ := message.Serialize(msg)
		protocol.WriteMessage(writer, msgBytes)

		// Variable delays between 50-500ms to simulate realistic traffic
		delay := time.Duration(50+i*10) * time.Millisecond
		time.Sleep(delay)

		if i%10 == 0 {
			fmt.Printf("Sent %d messages...\n", i)
		}
	}

	fmt.Println("Realistic test complete")
}
