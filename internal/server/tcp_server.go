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
	"github.com/mush1e/obelisk/internal/metrics"
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
	metrics  *metrics.BrokerMetrics // Optional metrics instance for tracking connection/message stats
}

// NewTCPServer creates a new TCP server with the specified address and message handler.
//   - *TCPServer: Configured server instance ready to start accepting connections
func NewTCPServer(address string, service *services.BrokerService, metrics *metrics.BrokerMetrics) *TCPServer {
	return &TCPServer{
		address: address,
		quit:    make(chan struct{}), // Unbuffered channel for shutdown coordination
		service: service,
		metrics: metrics,
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
		conn, err := t.listener.Accept()
		if err != nil {
			select {
			case <-t.quit:
				return
			default:
				// Track connection errors
				if t.metrics != nil {
					t.metrics.ConnectionErrors.WithLabelValues("accept_error").Inc()
				}
				fmt.Println("Accept error:", err)
				continue
			}
		}

		// Track new connections
		if t.metrics != nil {
			t.metrics.ConnectionsTotal.Inc()
			t.metrics.ActiveConnections.Inc()
		}

		fmt.Println("New client connected:", conn.RemoteAddr())

		t.wg.Add(1)
		go t.handleConnection(conn)
	}
}

// handleConnection processes messages from a single client connection until disconnect or shutdown.
func (t *TCPServer) handleConnection(conn net.Conn) {
	defer t.wg.Done()
	defer conn.Close()
	defer func() {
		if t.metrics != nil {
			t.metrics.ActiveConnections.Dec()
		}
	}() // Track disconnection

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
				// Handle errors with metrics tracking
				switch {
				case errors.Is(err, io.EOF):
					// Normal disconnection - no error metric
					fmt.Println("Client disconnected:", conn.RemoteAddr())
					return
				case obeliskErrors.GetErrorType(err) == obeliskErrors.ErrorTypeData:
					if t.metrics != nil {
						t.metrics.ConnectionErrors.WithLabelValues("data_corruption").Inc()
					}
					fmt.Printf("Corrupted message from %s: %v\n", conn.RemoteAddr(), err)
					t.sendNack(conn, writer, "CORRUPTED")
					continue
				case obeliskErrors.GetErrorType(err) == obeliskErrors.ErrorTypePermanent:
					if t.metrics != nil {
						t.metrics.ConnectionErrors.WithLabelValues("protocol_violation").Inc()
					}
					fmt.Printf("Protocol violation from %s: %v\n", conn.RemoteAddr(), err)
					t.sendNack(conn, writer, "PROTOCOL_ERROR")
					return
				case obeliskErrors.IsRetryable(err):
					if t.metrics != nil {
						t.metrics.ConnectionErrors.WithLabelValues("transient_error").Inc()
					}
					fmt.Printf("Transient error from %s: %v\n", conn.RemoteAddr(), err)
					continue
				default:
					if t.metrics != nil {
						t.metrics.ConnectionErrors.WithLabelValues("network_error").Inc()
					}
					fmt.Printf("Connection error from %s: %v\n", conn.RemoteAddr(), err)
					return
				}
			}

			msg, err := message.Deserialize(msgBytes)
			if err != nil {
				// Track deserialization errors
				if t.metrics != nil {
					t.metrics.ConnectionErrors.WithLabelValues("invalid_message").Inc()
				}
				fmt.Printf("Invalid message format from %s: %v\n", conn.RemoteAddr(), err)
				t.sendNack(conn, writer, "INVALID_FORMAT")
				continue
			}

			// Process message with retry for transient service errors
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			err = retry.Retry(ctx, ackRetryConfig, func() error {
				return t.service.PublishMessage(&msg)
			})
			cancel()

			if err != nil {
				// Track publish failures
				if t.metrics != nil {
					t.metrics.MessagesFailed.WithLabelValues(msg.Topic, "publish_failed").Inc()
				}
				fmt.Printf("Failed to publish message: %v\n", err)
				t.sendNack(conn, writer, "PUBLISH_FAILED")
				continue
			}

			// Send acknowledgment with retry
			ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
			err = retry.Retry(ctx, ackRetryConfig, func() error {
				// Set a write deadline to prevent blocking on slow/stalled clients
				_ = conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
				if _, err := writer.WriteString("OK\n"); err != nil {
					return obeliskErrors.NewTransientError("send_ack", "failed to send acknowledgment", err)
				}
				// Ensure data is flushed within the deadline as well
				_ = conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
				return writer.Flush()
			})
			cancel()

			if err != nil {
				// Track acknowledgment failures
				if t.metrics != nil {
					t.metrics.ConnectionErrors.WithLabelValues("ack_failed").Inc()
				}
				fmt.Printf("Failed to send acknowledgment: %v\n", err)
				// Connection might be broken, exit handler
				return
			}
		}
	}
}

func (t *TCPServer) sendNack(conn net.Conn, w *bufio.Writer, reason string) {
	// Set a write deadline to avoid blocking on slow clients
	_ = conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	_, _ = w.WriteString(fmt.Sprintf("NACK:%s\n", reason))
	_ = conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	_ = w.Flush()
}
