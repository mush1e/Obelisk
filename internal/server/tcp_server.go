package server

// TCP server components for high-performance message ingestion with concurrent connection handling.
// - Error recovery for network issues, malformed messages, and handler failures
// - Client acknowledgment responses to confirm message receipt

import (
    "bufio"
    "context"
    "errors"
    "fmt"
    "io"
    "net"
    "sync"
    "time"

    "github.com/mush1e/obelisk/internal/message"
    "github.com/mush1e/obelisk/internal/retry"
    "github.com/mush1e/obelisk/internal/services"
    "github.com/mush1e/obelisk/pkg/protocol"

    obeliskErrors "github.com/mush1e/obelisk/internal/errors"
)

// TCPServer manages TCP connections and delegates message processing to handlers.
// The server implements a concurrent connection model where each client connection
// is handled in its own goroutine, allowing independent processing and preventing
// slow clients from blocking others. The server coordinates graceful shutdown
// across all active connections.
//
// Architecture:
// - Accept loop runs in background goroutine to handle new connections
// - Each client connection spawns its own handling goroutine
// - All goroutines coordinate through quit channel for clean shutdown
// - WaitGroup ensures all goroutines complete before server termination
type TCPServer struct {
	address  string         // Network address to bind the TCP listener to
	listener net.Listener   // TCP listener for accepting incoming connections
	quit     chan struct{}  // Channel for coordinating graceful shutdown across goroutines
	wg       sync.WaitGroup // Ensures all connection goroutines complete before shutdown
	service  *services.BrokerService
}

// NewTCPServer creates a new TCP server with the specified address and message handler.
//   - *TCPServer: Configured server instance ready to start accepting connections
func NewTCPServer(address string, service *services.BrokerService) *TCPServer {
	return &TCPServer{
		address: address,
		quit:    make(chan struct{}), // Unbuffered channel for shutdown coordination
		service: service,
	}
}

// Start initializes the TCP listener and begins accepting connections.
//
// The startup process:
// 1. Creates TCP listener on the configured address
// 2. Logs startup information for operational visibility
// 3. Launches accept loop in background goroutine
// 4. Returns control to caller while server runs in background
//
// Returns:
//   - error: Any error that occurred during listener initialization
func (t *TCPServer) Start() error {
	var err error
	// Create TCP listener on configured address
	t.listener, err = net.Listen("tcp", t.address)
	if err != nil {
		return obeliskErrors.NewConfigurationError("start_tcp_server", "failed to start TCP listener", err)
	}

	// Log startup for operational visibility
	fmt.Println("TCP server started on", t.address)

	// Start accept loop in background goroutine
	t.wg.Add(1)
	go t.acceptLoop()
	return nil
}

// Stop initiates graceful shutdown of the TCP server and all active connections.
// The shutdown process ensures that all in-flight messages are processed and
// all client connections are cleanly terminated before the method returns.
//
// Shutdown sequence:
// 1. Signal shutdown to all goroutines via quit channel
// 2. Close listener to stop accepting new connections
// Stop gracefully shuts down the server and waits for connections to close.
func (t *TCPServer) Stop() {
	close(t.quit)

	// Close listener to stop accepting new connections
	if t.listener != nil {
		t.listener.Close()
	}

	// Wait for all connection handlers to complete
	t.wg.Wait()
	fmt.Println("TCP server stopped")
}

// acceptLoop accepts connections until shutdown, spawning goroutines for each client.
//
// The loop distinguishes between shutdown-related errors (expected) and actual
// network errors (unexpected) to provide appropriate logging and error handling.
func (t *TCPServer) acceptLoop() {
	defer t.wg.Done()

	for {
		// Accept new client connection (blocks until connection or error)
		conn, err := t.listener.Accept()
		if err != nil {
			select {
			case <-t.quit:
				// Shutdown was signaled - exit accept loop gracefully
				return
			default:
				// Unexpected error during accept - log and continue
				fmt.Println("Accept error:", err)
				continue
			}
		}

		// Log new connection for operational visibility
		fmt.Println("New client connected:", conn.RemoteAddr())

		// Spawn goroutine to handle this client connection
		t.wg.Add(1)
		go t.handleConnection(conn)
	}
}

// handleConnection processes messages from a single client connection until disconnect or shutdown.
func (t *TCPServer) handleConnection(conn net.Conn) {
	defer t.wg.Done()
	defer conn.Close()

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	// Configure retry for acknowledgments
	ackRetryConfig := retry.Config{
		MaxAttempts:   3,
		InitialDelay:  50 * time.Millisecond,
		MaxDelay:      500 * time.Millisecond,
		BackoffFactor: 2.0,
	}

	for {
		select {
		case <-t.quit:
			fmt.Println("Closing connection:", conn.RemoteAddr())
			return
		default:
			// Set read deadline to prevent blocking forever
			conn.SetReadDeadline(time.Now().Add(30 * time.Second))

			msgBytes, err := protocol.ReadMessage(reader)
			if err != nil {
				// Check if it's a retryable error
				if obeliskErrors.IsRetryable(err) {
					// Log and continue for transient errors
					fmt.Printf("Transient error from %s: %v\n", conn.RemoteAddr(), err)
					continue
				}

				// Handle specific error types
				switch obeliskErrors.GetErrorType(err) {
				case obeliskErrors.ErrorTypeData:
					// Data corruption - log and continue
					fmt.Printf("Corrupted message from %s: %v\n", conn.RemoteAddr(), err)
					// Send NACK to client
					t.sendNack(writer, "CORRUPTED")
					continue
				case obeliskErrors.ErrorTypePermanent:
					// Permanent error - close connection
					fmt.Printf("Protocol violation from %s: %v\n", conn.RemoteAddr(), err)
					t.sendNack(writer, "PROTOCOL_ERROR")
					return
                default:
                    // EOF or other errors - disconnect
                    if errors.Is(err, io.EOF) {
                        fmt.Println("Client disconnected:", conn.RemoteAddr())
                    } else {
                        fmt.Printf("Connection error from %s: %v\n", conn.RemoteAddr(), err)
                    }
                    return
				}
			}

			msg, err := message.Deserialize(msgBytes)
			if err != nil {
				fmt.Printf("Invalid message format from %s: %v\n", conn.RemoteAddr(), err)
				t.sendNack(writer, "INVALID_FORMAT")
				continue
			}

			// Process message with retry for transient service errors
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			err = retry.Retry(ctx, ackRetryConfig, func() error {
				return t.service.PublishMessage(&msg)
			})
			cancel()

			if err != nil {
				fmt.Printf("Failed to publish message: %v\n", err)
				t.sendNack(writer, "PUBLISH_FAILED")
				continue
			}

			// Send acknowledgment with retry
			ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
			err = retry.Retry(ctx, ackRetryConfig, func() error {
				if _, err := writer.WriteString("OK\n"); err != nil {
					return obeliskErrors.NewTransientError("send_ack", "failed to send acknowledgment", err)
				}
				return writer.Flush()
			})
			cancel()

			if err != nil {
				fmt.Printf("Failed to send acknowledgment: %v\n", err)
				// Connection might be broken, exit handler
				return
			}
		}
	}
}

func (t *TCPServer) sendNack(w *bufio.Writer, reason string) {
	w.WriteString(fmt.Sprintf("NACK:%s\n", reason))
	w.Flush()
}
