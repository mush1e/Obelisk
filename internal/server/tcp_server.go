package server

// This package provides TCP server components for the Obelisk message broker's high-performance
// message ingestion interface. The TCP server handles concurrent client connections and processes
// incoming messages through a configurable handler interface. It implements connection pooling,
// graceful shutdown, and error recovery to ensure reliable message processing under high load.
//
// The TCP server handles:
// - Concurrent client connection management with goroutine-per-connection model
// - Binary protocol message parsing and deserialization from network streams
// - Message delegation to pluggable connection handlers for processing
// - Graceful shutdown with proper connection cleanup and message drain
// - Error recovery for network issues, malformed messages, and handler failures
// - Client acknowledgment responses to confirm message receipt

import (
	"bufio"
	"fmt"
	"net"
	"sync"

	"github.com/mush1e/obelisk/internal/message"
	"github.com/mush1e/obelisk/internal/services"
	"github.com/mush1e/obelisk/pkg/protocol"
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

// NewTCPServer creates a new TCP server instance with the specified address and handler.
// The server is initialized but not yet started - call Start() to begin accepting connections.
// The handler will be called for each message received from any connected client.
//
// Parameters:
//   - address: Network address to bind to (e.g., ":8080" or "localhost:8080")
//   - handler: Implementation of ConnectionHandler interface for processing messages
//
// Returns:
//   - *TCPServer: Configured server instance ready to start accepting connections
func NewTCPServer(address string, service *services.BrokerService) *TCPServer {
	return &TCPServer{
		address: address,
		quit:    make(chan struct{}), // Unbuffered channel for shutdown coordination
		service: service,
	}
}

// Start initializes the TCP listener and begins accepting client connections.
// This method starts the server infrastructure but returns immediately after
// launching the accept loop in a background goroutine. The server will continue
// running until Stop() is called.
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
		return fmt.Errorf("failed to start TCP listener: %w", err)
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
// 3. Wait for all connection handling goroutines to complete
//
// This method blocks until all connections are closed and all messages are processed.
func (t *TCPServer) Stop() {
	// Signal shutdown to all goroutines
	close(t.quit)

	// Close listener to stop accepting new connections
	if t.listener != nil {
		t.listener.Close()
	}

	// Wait for all connection handlers to complete
	t.wg.Wait()
	fmt.Println("TCP server stopped")
}

// acceptLoop continuously accepts new client connections until shutdown is signaled.
// This method runs in a background goroutine and spawns a new goroutine for each
// accepted connection. It handles the race condition between shutdown signaling
// and connection acceptance to ensure clean termination.
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
// This method runs in its own goroutine per connection, enabling concurrent processing of
// multiple clients. It implements the binary message protocol and handles various error
// conditions gracefully.
//
// Connection lifecycle:
// 1. Create buffered reader for efficient message parsing
// 2. Enter message processing loop
// 3. Read and deserialize messages using binary protocol
// 4. Delegate message processing to configured handler
// 5. Send acknowledgment to client
// 6. Handle errors and disconnection scenarios appropriately
//
// The method ensures proper cleanup of connection resources and coordinated shutdown
// when the server is being terminated.
//
// Parameters:
//   - conn: TCP connection to the client that will be processed
func (t *TCPServer) handleConnection(conn net.Conn) {
	defer t.wg.Done()
	defer conn.Close() // Ensure connection is closed when goroutine exits

	// Create buffered reader for efficient message parsing
	reader := bufio.NewReader(conn)

	for {
		select {
		case <-t.quit:
			// Server shutdown requested - close connection gracefully
			fmt.Println("Closing connection:", conn.RemoteAddr())
			return
		default:
			// Read message using binary protocol
			msgBytes, err := protocol.ReadMessage(reader)
			if err != nil {
				if err.Error() == "EOF" {
					// Client disconnected normally
					fmt.Println("Client disconnected:", conn.RemoteAddr())
				} else {
					// Network or protocol error
					fmt.Println("Error receiving message from", conn.RemoteAddr(), ":", err)
				}
				return // Exit connection handler
			}

			// Deserialize binary message data to Message struct
			msg, err := message.Deserialize(msgBytes)
			if err != nil {
				// Malformed message - log error but continue processing
				fmt.Println("Invalid message format:", err)
				continue // Skip this message but keep connection alive
			}

			// Delegate message processing to configured handler
			if err := t.service.PublishMessage(&msg); err != nil {
				// Handler error - log but continue processing
				fmt.Printf("Error handling message: %v\n", err)
			}

			// Send acknowledgment to client to confirm message receipt
			// Simple text-based acknowledgment for easy client implementation
			conn.Write([]byte("OK\n"))
		}
	}
}
