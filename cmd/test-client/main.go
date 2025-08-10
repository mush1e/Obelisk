// Package main provides a test client for the Obelisk message broker.
// It supports different test scenarios to validate batching behavior,
// including size-based batching, time-based batching, and realistic load testing.
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
// Use -test flag to specify test type: size, time, or realistic.
func main() {
	testType := flag.String("test", "size", "Test type: size, time, realistic")
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
	default:
		fmt.Println("Unknown test type. Use: size, time, or realistic")
	}
}

// testSizeBatching tests size-based batch flushing by sending 150 messages quickly.
// This should trigger multiple flushes when the batch size limit (100) is reached.
func testSizeBatching(writer *bufio.Writer) {
	fmt.Println("Testing size-based batching (sending 150 messages quickly)...")

	start := time.Now()
	for i := range 150 {
		// Create test message with rotating topics
		msg := message.Message{
			Timestamp: time.Now(),
			Topic:     fmt.Sprintf("topic-%d", i%3),
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

	for i := range 5 {
		// Create test message with rotating topics
		msg := message.Message{
			Timestamp: time.Now(),
			Topic:     fmt.Sprintf("topic-%d", i%3),
			Key:       fmt.Sprintf("time-user%d", i),
			Value:     fmt.Sprintf("Time test message %d", i),
		}

		// Send message and wait to test time-based flushing
		msgBytes, _ := message.Serialize(msg)
		protocol.WriteMessage(writer, msgBytes)

		fmt.Printf("Sent message %d, waiting 2 seconds...\n", i+1)
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

	for i := range 20 {
		// Create test message with rotating topics
		msg := message.Message{
			Timestamp: time.Now(),
			Topic:     fmt.Sprintf("topic-%d", i%3),
			Key:       fmt.Sprintf("real-user%d", i),
			Value:     fmt.Sprintf("Realistic message %d", i),
		}

		// Send message with variable delays to simulate real usage
		msgBytes, _ := message.Serialize(msg)
		protocol.WriteMessage(writer, msgBytes)

		// Variable delays between 50-500ms to simulate realistic traffic
		delay := time.Duration(50+i*20) * time.Millisecond
		time.Sleep(delay)

		if i%5 == 0 {
			fmt.Printf("Sent %d messages...\n", i)
		}
	}

	fmt.Println("Realistic test complete")
}
