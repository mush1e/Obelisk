package e2e

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/mush1e/obelisk/internal/config"
	"github.com/mush1e/obelisk/internal/consumer"
	"github.com/mush1e/obelisk/internal/message"
	"github.com/mush1e/obelisk/pkg/protocol"
)

// findAvailablePort finds an available port in the given range
func findAvailablePort(start, end int) int {
	for port := start; port <= end; port++ {
		addr := fmt.Sprintf(":%d", port)
		listener, err := net.Listen("tcp", addr)
		if err == nil {
			listener.Close()
			return port
		}
	}
	return start // fallback
}

// E2ETestSuite provides comprehensive end-to-end testing of the Obelisk system
type E2ETestSuite struct {
	serverProcess *exec.Cmd
	serverAddr    string
	httpAddr      string
	tempDir       string
	config        *config.Config
	serverPort    int
	httpPort      int
}

// NewE2ETestSuite creates a new end-to-end test suite
func NewE2ETestSuite(t *testing.T) *E2ETestSuite {
	tempDir := t.TempDir()

	// Find available ports
	serverPort := findAvailablePort(8080, 9000)
	httpPort := findAvailablePort(8081, 9000)

	// Create config for testing
	cfg := &config.Config{}
	cfg.Server.TCPAddr = fmt.Sprintf(":%d", serverPort)
	cfg.Server.HTTPAddr = fmt.Sprintf(":%d", httpPort)
	cfg.Storage.DataDir = filepath.Join(tempDir, "data/topics") // Use temp directory
	cfg.Storage.DefaultPartitions = 1                           // Use 1 partition for legacy compatibility
	cfg.Storage.BatchSize = 50
	cfg.Storage.FlushInterval = 2 * time.Second
	cfg.Metrics.Enabled = false

	return &E2ETestSuite{
		serverAddr: fmt.Sprintf("localhost:%d", serverPort),
		httpAddr:   fmt.Sprintf("localhost:%d", httpPort),
		tempDir:    tempDir,
		config:     cfg,
		serverPort: serverPort,
		httpPort:   httpPort,
	}
}

// StartServer starts the Obelisk server for testing
func (e *E2ETestSuite) StartServer(t *testing.T) error {
	// Build the server binary
	buildCmd := exec.Command("go", "build", "-o", filepath.Join(e.tempDir, "obelisk"), "./cmd/Obelisk")
	buildCmd.Dir = "../../" // Run from project root
	if err := buildCmd.Run(); err != nil {
		return fmt.Errorf("failed to build server: %v", err)
	}

	// Create config file for the server
	configPath := filepath.Join(e.tempDir, "obelisk.yaml")
	configData := fmt.Sprintf(`
server:
  tcp_addr: ":%d"
  http_addr: ":%d"
storage:
  data_dir: "%s"
  batch_size: 50
  flush_interval: "2s"
  default_partitions: %d
metrics:
  enabled: false
`, e.serverPort, e.httpPort, e.config.Storage.DataDir, e.config.Storage.DefaultPartitions)

	if err := os.WriteFile(configPath, []byte(configData), 0644); err != nil {
		return fmt.Errorf("failed to write config file: %v", err)
	}

	// Start the server
	serverPath := filepath.Join(e.tempDir, "obelisk")
	e.serverProcess = exec.Command(serverPath, "-config", configPath)
	e.serverProcess.Dir = e.tempDir

	// Capture server output for debugging
	var stdout, stderr bytes.Buffer
	e.serverProcess.Stdout = &stdout
	e.serverProcess.Stderr = &stderr

	// Also log to console for debugging
	e.serverProcess.Stdout = io.MultiWriter(&stdout, os.Stdout)
	e.serverProcess.Stderr = io.MultiWriter(&stderr, os.Stderr)

	if err := e.serverProcess.Start(); err != nil {
		return fmt.Errorf("failed to start server: %v", err)
	}

	// Wait for server to be ready with retries
	maxRetries := 10
	for i := 0; i < maxRetries; i++ {
		time.Sleep(500 * time.Millisecond)

		// Test connection
		conn, err := net.Dial("tcp", e.serverAddr)
		if err == nil {
			conn.Close()
			t.Logf("Server started successfully on %s", e.serverAddr)
			return nil
		}

		// Check if server process is still running
		if e.serverProcess.ProcessState != nil && e.serverProcess.ProcessState.Exited() {
			return fmt.Errorf("server process exited: stdout=%s, stderr=%s", stdout.String(), stderr.String())
		}

		t.Logf("Retry %d: Server not ready yet, stdout=%s, stderr=%s", i+1, stdout.String(), stderr.String())
	}

	return fmt.Errorf("server not responding after %d retries: stdout=%s, stderr=%s",
		maxRetries, stdout.String(), stderr.String())
}

// StopServer stops the Obelisk server
func (e *E2ETestSuite) StopServer() {
	if e.serverProcess != nil && e.serverProcess.Process != nil {
		// Try graceful shutdown first
		e.serverProcess.Process.Signal(os.Interrupt)

		// Wait a bit for graceful shutdown
		done := make(chan error, 1)
		go func() {
			done <- e.serverProcess.Wait()
		}()

		select {
		case <-done:
			// Process exited gracefully
		case <-time.After(2 * time.Second):
			// Force kill if it doesn't exit
			e.serverProcess.Process.Kill()
			e.serverProcess.Wait()
		}
	}
}

// SendMessage sends a single message to the server
func (e *E2ETestSuite) SendMessage(t *testing.T, msg message.Message) error {
	conn, err := net.Dial("tcp", e.serverAddr)
	if err != nil {
		return fmt.Errorf("failed to connect: %v", err)
	}
	defer conn.Close()

	writer := bufio.NewWriter(conn)
	msgBytes, err := message.Serialize(msg)
	if err != nil {
		return fmt.Errorf("failed to serialize message: %v", err)
	}

	t.Logf("Sending message: topic=%s, key=%s, value=%s, size=%d bytes",
		msg.Topic, msg.Key, msg.Value, len(msgBytes))

	if err := protocol.WriteMessage(writer, msgBytes); err != nil {
		return fmt.Errorf("failed to write message: %v", err)
	}

	// Read response from server
	reader := bufio.NewReader(conn)
	response, err := reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("failed to read response: %v", err)
	}

	t.Logf("Server response: %s", response)

	// Check if server acknowledged the message
	if response != "OK\n" {
		return fmt.Errorf("server returned error: %s", response)
	}

	return nil
}

// WaitForMessages waits for messages to be persisted to disk
func (e *E2ETestSuite) WaitForMessages(topic string, expected int) error {
	maxRetries := 20
	for i := 0; i < maxRetries; i++ {
		time.Sleep(500 * time.Millisecond)

		// Check if topic directory exists
		topicDir := filepath.Join(e.config.Storage.DataDir, topic)
		if _, err := os.Stat(topicDir); err == nil {
			// Check for log files (partition-0.log for single partition)
			files, err := os.ReadDir(topicDir)
			if err == nil {
				for _, file := range files {
					if file.Name() == "partition-0.log" {
						// Topic exists, assume messages are persisted
						return nil
					}
				}
			}
		}
	}
	return fmt.Errorf("messages not persisted for topic %s after %d retries", topic, maxRetries)
}

// SendMessages sends multiple messages concurrently
func (e *E2ETestSuite) SendMessages(t *testing.T, messages []message.Message) error {
	var wg sync.WaitGroup
	errChan := make(chan error, len(messages))

	for _, msg := range messages {
		wg.Add(1)
		go func(m message.Message) {
			defer wg.Done()
			if err := e.SendMessage(t, m); err != nil {
				errChan <- err
			}
		}(msg)
	}

	wg.Wait()
	close(errChan)

	// Check for errors
	for err := range errChan {
		if err != nil {
			return err
		}
	}

	return nil
}

// TestBasicMessageFlow tests the basic message production and consumption flow
func TestBasicMessageFlow(t *testing.T) {
	suite := NewE2ETestSuite(t)
	defer suite.StopServer()

	if err := suite.StartServer(t); err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}

	// Send test messages
	messages := []message.Message{
		{Timestamp: time.Now(), Topic: "test-topic", Key: "key1", Value: "value1"},
	}

	t.Logf("Sending %d messages to topic test-topic", len(messages))
	if err := suite.SendMessage(t, messages[0]); err != nil {
		t.Fatalf("Failed to send message: %v", err)
	}
	t.Logf("Successfully sent message")

	// Wait for messages to be persisted
	if err := suite.WaitForMessages("test-topic", len(messages)); err != nil {
		t.Fatalf("Messages not persisted: %v", err)
	}

	// Create consumer and read messages
	c := consumer.NewConsumer(suite.config.Storage.DataDir, "test-consumer", "test-topic")

	received, err := c.Poll("test-topic")
	if err != nil {
		t.Fatalf("Failed to poll messages: %v", err)
	}

	if len(received) != len(messages) {
		t.Errorf("Expected %d messages, got %d", len(messages), len(received))
	}

	// Verify message content
	for i, msg := range received {
		if msg.Value != messages[i].Value {
			t.Errorf("Message %d: expected %s, got %s", i, messages[i].Value, msg.Value)
		}
	}
}

// TestPartitioning tests message partitioning functionality
func TestPartitioning(t *testing.T) {
	suite := NewE2ETestSuite(t)
	// Override config for partitioning test
	suite.config.Storage.DefaultPartitions = 4
	defer suite.StopServer()

	if err := suite.StartServer(t); err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}

	// Send messages with different keys to test partitioning
	topic := "partition-test"
	keys := []string{"user-123", "user-456", "user-789", "user-111", "user-222"}

	var messages []message.Message
	for i := 0; i < 50; i++ {
		key := keys[i%len(keys)]
		messages = append(messages, message.Message{
			Timestamp: time.Now(),
			Topic:     topic,
			Key:       key,
			Value:     fmt.Sprintf("message-%d", i),
		})
	}

	if err := suite.SendMessages(t, messages); err != nil {
		t.Fatalf("Failed to send messages: %v", err)
	}

	// Wait for batching to complete
	time.Sleep(3 * time.Second)

	// Check that partitions were created
	topicDir := filepath.Join(suite.tempDir, "data/topics", topic)
	partitions, err := filepath.Glob(filepath.Join(topicDir, "partition-*.log"))
	if err != nil {
		t.Fatalf("Failed to list partitions: %v", err)
	}

	if len(partitions) < 2 {
		t.Errorf("Expected multiple partitions, got %d: %v", len(partitions), partitions)
	}

	t.Logf("Created %d partitions: %v", len(partitions), partitions)

	// Verify all messages can be consumed
	c := consumer.NewConsumer(filepath.Join(suite.tempDir, "data/topics"), "test-consumer", topic)
	received, err := c.Poll(topic)
	if err != nil {
		t.Fatalf("Failed to poll messages: %v", err)
	}

	if len(received) != len(messages) {
		t.Errorf("Expected %d messages, got %d", len(messages), len(received))
	}
}

// TestBatchingBehavior tests both size-based and time-based batching
func TestBatchingBehavior(t *testing.T) {
	suite := NewE2ETestSuite(t)
	defer suite.StopServer()

	if err := suite.StartServer(t); err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}

	// Test size-based batching (send more than maxSize)
	topic := "batch-test"
	maxSize := 50

	var messages []message.Message
	for i := 0; i < maxSize+10; i++ {
		messages = append(messages, message.Message{
			Timestamp: time.Now(),
			Topic:     topic,
			Key:       fmt.Sprintf("key-%d", i),
			Value:     fmt.Sprintf("value-%d", i),
		})
	}

	start := time.Now()
	if err := suite.SendMessages(t, messages); err != nil {
		t.Fatalf("Failed to send messages: %v", err)
	}
	sendTime := time.Since(start)

	// Wait for batching to complete
	time.Sleep(3 * time.Second)

	// Verify messages were persisted
	c := consumer.NewConsumer(filepath.Join(suite.tempDir, "data/topics"), "test-consumer", topic)
	received, err := c.Poll(topic)
	if err != nil {
		t.Fatalf("Failed to poll messages: %v", err)
	}

	if len(received) != len(messages) {
		t.Errorf("Expected %d messages, got %d", len(messages), len(received))
	}

	t.Logf("Sent %d messages in %v, received %d messages", len(messages), sendTime, len(received))
}

// TestConcurrentProducers tests multiple producers sending messages simultaneously
func TestConcurrentProducers(t *testing.T) {
	suite := NewE2ETestSuite(t)
	defer suite.StopServer()

	if err := suite.StartServer(t); err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}

	// Create multiple producers
	numProducers := 5
	messagesPerProducer := 20
	topic := "concurrent-test"

	var wg sync.WaitGroup
	var allMessages []message.Message
	var messagesMutex sync.Mutex

	for p := 0; p < numProducers; p++ {
		wg.Add(1)
		go func(producerID int) {
			defer wg.Done()

			var producerMessages []message.Message
			for i := 0; i < messagesPerProducer; i++ {
				msg := message.Message{
					Timestamp: time.Now(),
					Topic:     topic,
					Key:       fmt.Sprintf("producer-%d-key-%d", producerID, i),
					Value:     fmt.Sprintf("producer-%d-value-%d", producerID, i),
				}
				producerMessages = append(producerMessages, msg)

				if err := suite.SendMessage(t, msg); err != nil {
					t.Errorf("Producer %d failed to send message %d: %v", producerID, i, err)
				}
			}

			// Thread-safe append
			messagesMutex.Lock()
			allMessages = append(allMessages, producerMessages...)
			messagesMutex.Unlock()
		}(p)
	}

	wg.Wait()

	// Wait for batching to complete
	time.Sleep(3 * time.Second)

	// Verify all messages were received
	c := consumer.NewConsumer(filepath.Join(suite.tempDir, "data/topics"), "test-consumer", topic)
	received, err := c.Poll(topic)
	if err != nil {
		t.Fatalf("Failed to poll messages: %v", err)
	}

	expectedCount := numProducers * messagesPerProducer
	if len(received) != expectedCount {
		t.Errorf("Expected %d messages, got %d", expectedCount, len(received))
	}

	t.Logf("Successfully handled %d concurrent producers, %d total messages", numProducers, len(received))
}

// TestConsumerOffsetManagement tests consumer offset tracking and recovery
func TestConsumerOffsetManagement(t *testing.T) {
	suite := NewE2ETestSuite(t)
	defer suite.StopServer()

	if err := suite.StartServer(t); err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}

	topic := "offset-test"

	// Send initial messages
	initialMessages := []message.Message{
		{Timestamp: time.Now(), Topic: topic, Key: "key1", Value: "value1"},
		{Timestamp: time.Now(), Topic: topic, Key: "key2", Value: "value2"},
		{Timestamp: time.Now(), Topic: topic, Key: "key3", Value: "value3"},
	}

	// Send messages sequentially to maintain order
	for _, msg := range initialMessages {
		if err := suite.SendMessage(t, msg); err != nil {
			t.Fatalf("Failed to send initial message: %v", err)
		}
	}

	time.Sleep(2 * time.Second)

	// Create consumer and read first message
	c := consumer.NewConsumer(filepath.Join(suite.tempDir, "data/topics"), "offset-consumer", topic)

	received, err := c.Poll(topic)
	if err != nil {
		t.Fatalf("Failed to poll messages: %v", err)
	}

	if len(received) != len(initialMessages) {
		t.Errorf("Expected %d messages, got %d", len(initialMessages), len(received))
	}

	// Commit offset after reading first message only
	if err := c.Commit(topic, 1); err != nil {
		t.Fatalf("Failed to commit offset: %v", err)
	}

	// Send more messages
	additionalMessages := []message.Message{
		{Timestamp: time.Now(), Topic: topic, Key: "key4", Value: "value4"},
		{Timestamp: time.Now(), Topic: topic, Key: "key5", Value: "value5"},
	}

	// Send messages sequentially to maintain order
	for _, msg := range additionalMessages {
		if err := suite.SendMessage(t, msg); err != nil {
			t.Fatalf("Failed to send additional message: %v", err)
		}
	}

	time.Sleep(2 * time.Second)

	// Poll again - should only get new messages
	received2, err := c.Poll(topic)
	if err != nil {
		t.Fatalf("Failed to poll messages: %v", err)
	}

	if len(received2) != len(additionalMessages) {
		t.Errorf("Expected %d new messages, got %d", len(additionalMessages), len(received2))
	}

	// Verify we got the right messages
	for i, msg := range received2 {
		if msg.Value != additionalMessages[i].Value {
			t.Errorf("Message %d: expected %s, got %s", i, additionalMessages[i].Value, msg.Value)
		}
	}
}

// TestServerRecovery tests server restart and message recovery
func TestServerRecovery(t *testing.T) {
	suite := NewE2ETestSuite(t)
	defer suite.StopServer()

	if err := suite.StartServer(t); err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}

	topic := "recovery-test"

	// Send messages
	messages := []message.Message{
		{Timestamp: time.Now(), Topic: topic, Key: "key1", Value: "value1"},
		{Timestamp: time.Now(), Topic: topic, Key: "key2", Value: "value2"},
		{Timestamp: time.Now(), Topic: topic, Key: "key3", Value: "value3"},
	}

	if err := suite.SendMessages(t, messages); err != nil {
		t.Fatalf("Failed to send messages: %v", err)
	}

	// Wait for persistence
	time.Sleep(1 * time.Second)

	// Stop server
	suite.StopServer()

	// Restart server
	if err := suite.StartServer(t); err != nil {
		t.Fatalf("Failed to restart server: %v", err)
	}

	// Verify messages are still available
	c := consumer.NewConsumer(filepath.Join(suite.tempDir, "data/topics"), "recovery-consumer", topic)
	received, err := c.Poll(topic)
	if err != nil {
		t.Fatalf("Failed to poll messages after restart: %v", err)
	}

	if len(received) != len(messages) {
		t.Errorf("Expected %d messages after restart, got %d", len(messages), len(received))
	}

	t.Logf("Successfully recovered %d messages after server restart", len(received))
}

// TestHighLoadPerformance tests system performance under high load
func TestHighLoadPerformance(t *testing.T) {
	suite := NewE2ETestSuite(t)
	defer suite.StopServer()

	if err := suite.StartServer(t); err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}

	topic := "performance-test"
	numMessages := 1000
	numProducers := 10

	// Send messages concurrently
	var wg sync.WaitGroup
	start := time.Now()

	for p := 0; p < numProducers; p++ {
		wg.Add(1)
		go func(producerID int) {
			defer wg.Done()

			for i := 0; i < numMessages/numProducers; i++ {
				msg := message.Message{
					Timestamp: time.Now(),
					Topic:     topic,
					Key:       fmt.Sprintf("producer-%d-key-%d", producerID, i),
					Value:     fmt.Sprintf("producer-%d-value-%d", producerID, i),
				}

				if err := suite.SendMessage(t, msg); err != nil {
					t.Errorf("Producer %d failed to send message %d: %v", producerID, i, err)
				}
			}
		}(p)
	}

	wg.Wait()
	sendTime := time.Since(start)

	// Wait for batching to complete
	time.Sleep(2 * time.Second)

	// Verify all messages
	c := consumer.NewConsumer(filepath.Join(suite.tempDir, "data/topics"), "performance-consumer", topic)
	received, err := c.Poll(topic)
	if err != nil {
		t.Fatalf("Failed to poll messages: %v", err)
	}

	consumeTime := time.Since(start)

	if len(received) != numMessages {
		t.Errorf("Expected %d messages, got %d", numMessages, len(received))
	}

	t.Logf("Performance test: Sent %d messages in %v, consumed in %v",
		numMessages, sendTime, consumeTime)
	t.Logf("Throughput: %.2f messages/second", float64(numMessages)/sendTime.Seconds())
}

// TestDataIntegrity tests that no messages are lost or corrupted
func TestDataIntegrity(t *testing.T) {
	suite := NewE2ETestSuite(t)
	defer suite.StopServer()

	if err := suite.StartServer(t); err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}

	topic := "integrity-test"
	numMessages := 500

	// Generate messages with predictable content
	var sentMessages []message.Message
	for i := 0; i < numMessages; i++ {
		msg := message.Message{
			Timestamp: time.Now(),
			Topic:     topic,
			Key:       fmt.Sprintf("key-%d", i),
			Value:     fmt.Sprintf("value-%d-content-%d", i, i*2),
		}
		sentMessages = append(sentMessages, msg)
	}

	// Send messages sequentially to maintain order
	for _, msg := range sentMessages {
		if err := suite.SendMessage(t, msg); err != nil {
			t.Fatalf("Failed to send message: %v", err)
		}
	}

	// Wait for persistence
	time.Sleep(3 * time.Second)

	// Consume messages
	c := consumer.NewConsumer(filepath.Join(suite.tempDir, "data/topics"), "integrity-consumer", topic)
	received, err := c.Poll(topic)
	if err != nil {
		t.Fatalf("Failed to poll messages: %v", err)
	}

	if len(received) != len(sentMessages) {
		t.Errorf("Message count mismatch: sent %d, received %d", len(sentMessages), len(received))
	}

	// Verify each message content
	for i, receivedMsg := range received {
		if receivedMsg.Value != sentMessages[i].Value {
			t.Errorf("Message %d content mismatch: expected %s, got %s",
				i, sentMessages[i].Value, receivedMsg.Value)
		}
		if receivedMsg.Key != sentMessages[i].Key {
			t.Errorf("Message %d key mismatch: expected %s, got %s",
				i, sentMessages[i].Key, receivedMsg.Key)
		}
	}

	t.Logf("Data integrity test passed: %d messages verified", len(received))
}

// TestMultipleTopics tests handling of multiple topics simultaneously
func TestMultipleTopics(t *testing.T) {
	suite := NewE2ETestSuite(t)
	defer suite.StopServer()

	if err := suite.StartServer(t); err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}

	topics := []string{"orders", "payments", "inventory", "notifications"}
	messagesPerTopic := 25

	// Send messages to multiple topics
	for _, topic := range topics {
		var messages []message.Message
		for i := 0; i < messagesPerTopic; i++ {
			messages = append(messages, message.Message{
				Timestamp: time.Now(),
				Topic:     topic,
				Key:       fmt.Sprintf("%s-key-%d", topic, i),
				Value:     fmt.Sprintf("%s-value-%d", topic, i),
			})
		}

		if err := suite.SendMessages(t, messages); err != nil {
			t.Fatalf("Failed to send messages to topic %s: %v", topic, err)
		}
	}

	// Wait for batching to complete
	time.Sleep(3 * time.Second)

	// Verify each topic
	for _, topic := range topics {
		c := consumer.NewConsumer(filepath.Join(suite.tempDir, "data/topics"), "multi-topic-consumer", topic)
		received, err := c.Poll(topic)
		if err != nil {
			t.Fatalf("Failed to poll topic %s: %v", topic, err)
		}

		if len(received) != messagesPerTopic {
			t.Errorf("Topic %s: expected %d messages, got %d", topic, messagesPerTopic, len(received))
		}

		t.Logf("Topic %s: %d messages verified", topic, len(received))
	}
}

// TestGracefulShutdown tests that the server shuts down gracefully
func TestGracefulShutdown(t *testing.T) {
	suite := NewE2ETestSuite(t)

	if err := suite.StartServer(t); err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}

	topic := "shutdown-test"

	// Send some messages
	messages := []message.Message{
		{Timestamp: time.Now(), Topic: topic, Key: "key1", Value: "value1"},
		{Timestamp: time.Now(), Topic: topic, Key: "key2", Value: "value2"},
	}

	if err := suite.SendMessages(t, messages); err != nil {
		t.Fatalf("Failed to send messages: %v", err)
	}

	// Wait a bit for processing
	time.Sleep(1 * time.Second)

	// Stop server gracefully
	suite.StopServer()

	// Verify messages were persisted
	c := consumer.NewConsumer(filepath.Join(suite.tempDir, "data/topics"), "shutdown-consumer", topic)
	received, err := c.Poll(topic)
	if err != nil {
		t.Fatalf("Failed to poll messages after shutdown: %v", err)
	}

	if len(received) != len(messages) {
		t.Errorf("Expected %d messages after shutdown, got %d", len(messages), len(received))
	}

	t.Logf("Graceful shutdown test passed: %d messages persisted", len(received))
}
