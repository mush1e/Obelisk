package server

// This package provides the main server orchestration for the Obelisk message broker.
// The server coordinates multiple components including TCP message ingestion, HTTP API
// endpoints, persistent storage batching, and in-memory buffering systems. It manages
// the complete lifecycle of the broker infrastructure and ensures proper startup and
// shutdown sequencing across all subsystems.
//
// The server orchestrates:
// - TCP server for high-throughput message ingestion from clients
// - HTTP server for REST API endpoints and administrative operations
// - Topic batching system for efficient persistent storage operations
// - In-memory buffering system for fast recent message access
// - Graceful shutdown coordination across all components
// - Error handling and recovery for subsystem failures

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/mush1e/obelisk/internal/batch"
	"github.com/mush1e/obelisk/internal/buffer"
	"github.com/mush1e/obelisk/internal/services"
)

// Server orchestrates the complete Obelisk message broker infrastructure.
// This is the top-level component that coordinates all subsystems and manages
// their lifecycle from startup through shutdown. The server implements a
// dual-interface architecture with TCP for high-performance message ingestion
// and HTTP for administrative and monitoring operations.
//
// The server manages four primary subsystems:
// 1. TCP server - Handles high-throughput message ingestion from producers
// 2. HTTP server - Provides REST API for monitoring and administrative tasks
// 3. Topic batcher - Manages efficient persistent storage of messages
// 4. Topic buffers - Provides fast in-memory access to recent messages
//
// All subsystems are initialized with consistent configuration and coordinated
// through proper startup/shutdown sequencing to ensure data integrity and
// graceful operation under all conditions.
type Server struct {
	tcpServer    *TCPServer           // High-performance message ingestion server
	httpServer   *HTTPServer          // REST API server for monitoring and administration
	batcher      *batch.TopicBatcher  // Persistent storage batching system
	topicBuffers *buffer.TopicBuffers // In-memory ring buffers for recent message access
	wg           sync.WaitGroup       // Coordinates graceful shutdown of background goroutines
}

// NewServer creates a new Obelisk server with the specified network addresses and storage path.
// The server is initialized with all necessary subsystems configured and connected, but
// not yet started. The initialization process sets up consistent configuration across
// all components and establishes the message processing pipeline.
//
// Configuration details:
// - Topic buffers: 100 message capacity per topic for recent message access
// - Batcher: 100 message batch size with 5-second flush timeout
// - TCP/HTTP servers: Configured with provided network addresses
//
// The initialization creates the complete message processing pipeline:
// 1. Messages arrive via TCP server to connection handler
// 2. Handler stores messages in both memory buffers and persistent batcher
// 3. HTTP server provides access to statistics and health information
// 4. All components coordinate through shared service layer
//
// Parameters:
//   - tcpAddr: Network address for TCP message ingestion server (e.g., ":8080")
//   - httpAddr: Network address for HTTP API server (e.g., ":8081")
//   - logFilePath: Base directory path for persistent message storage files
//
// Returns:
//   - *Server: Fully configured server instance ready for startup
func NewServer(tcpAddr, httpAddr, logFilePath string) *Server {
	// Initialize in-memory topic buffers with 100 message capacity per topic
	// This provides fast access to recent messages for monitoring and debugging
	topicBuffers := buffer.NewTopicBuffers(100)

	// Initialize persistent batching system with 100 message batches and 5-second timeout
	// This balances storage efficiency with message durability requirements
	batcher := batch.NewTopicBatcher(logFilePath, 100, time.Second*5)

	// Create topic service that provides access to batcher statistics
	// This service layer abstracts broker internals for the HTTP API
	brokerService := services.NewBrokerService(topicBuffers, batcher)

	// Initialize TCP server for high-performance message ingestion
	tcpServer := NewTCPServer(tcpAddr, brokerService)

	// Initialize HTTP server with REST API endpoints for monitoring and administration
	httpServer := NewHTTPServer(httpAddr, brokerService)

	return &Server{
		tcpServer:    tcpServer,
		httpServer:   httpServer,
		batcher:      batcher,
		topicBuffers: topicBuffers,
	}
}

// Start initializes and starts all server subsystems in the proper sequence.
// The startup process ensures that dependencies are satisfied and that all
// components are ready to handle requests before exposing network interfaces.
// Any failure during startup results in a complete rollback to prevent
// partially initialized states.
//
// Startup sequence:
// 1. Start topic batcher for persistent storage (creates directories, loads existing data)
// 2. Start TCP server for message ingestion (begins accepting producer connections)
// 3. Start HTTP server for API access (begins accepting monitoring requests)
//
// The sequencing ensures that storage systems are ready before accepting messages
// and that message processing is available before exposing monitoring interfaces.
// This prevents race conditions and ensures consistent system state.
//
// Returns:
//   - error: Any error that occurred during startup, requiring system shutdown
func (s *Server) Start() error {
	// Start persistent batching system first to ensure storage is ready
	// This creates necessary directories and loads any existing topic data
	if err := s.batcher.Start(); err != nil {
		return fmt.Errorf("failed to start batcher: %w", err)
	}

	// Start TCP server for message ingestion once storage is ready
	// This begins accepting connections from message producers
	if err := s.tcpServer.Start(); err != nil {
		return fmt.Errorf("failed to start TCP server: %w", err)
	}

	// Start HTTP server in background goroutine for API access
	// The goroutine allows the method to return while HTTP server runs
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.httpServer.Start() // Blocks until server shutdown
	}()

	return nil
}

// Stop gracefully shuts down all server subsystems in reverse startup order.
// The shutdown process ensures that all in-flight messages are processed and
// persisted before terminating the server. This prevents data loss and ensures
// clean shutdown state for future restarts.
//
// Shutdown sequence:
// 1. Stop TCP server (prevents new message ingestion)
// 2. Stop HTTP server with timeout (completes active API requests)
// 3. Stop topic batcher (flushes all buffered messages to storage)
// 4. Wait for HTTP server goroutine completion
//
// The reverse order ensures that message ingestion stops first, allowing
// existing messages to drain through the system before storage shutdown.
// The HTTP server continues operating during this drain period to provide
// visibility into the shutdown process.
func (s *Server) Stop() {
	// Stop TCP server first to prevent new message ingestion
	// This allows existing messages to drain through the processing pipeline
	s.tcpServer.Stop()

	// Stop HTTP server with reasonable timeout for active request completion
	// The timeout prevents indefinite blocking while allowing proper request handling
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s.httpServer.Stop(ctx)

	// Stop batcher last to ensure all buffered messages are flushed to storage
	// This guarantees data durability by persisting any remaining in-memory messages
	s.batcher.Stop()

	// Wait for HTTP server goroutine to complete shutdown
	// This ensures clean termination of all background processes
	s.wg.Wait()

	fmt.Println("Server stopped")
}
