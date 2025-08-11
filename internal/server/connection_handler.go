package server

// This package provides connection handling components for the Obelisk message broker server.
// The connection handler acts as the central message processing coordinator, managing the
// flow of incoming messages through the broker's storage and buffering systems. It serves
// as the bridge between network connections and the internal message processing pipeline.
//
// The connection handler coordinates:
// - Message ingestion from network connections into the broker pipeline
// - Dual-path message storage (memory buffer + persistent batching)
// - Error handling and logging for message processing operations
// - Integration between the buffering and batching subsystems
// - Request/response flow management for client connections

import (
	"fmt"

	"github.com/mush1e/obelisk/internal/batch"
	"github.com/mush1e/obelisk/internal/buffer"
	"github.com/mush1e/obelisk/internal/message"
)

// ObeliskConnectionHandler manages message processing for client connections.
// This handler coordinates the dual-path message storage strategy used by Obelisk:
// messages are simultaneously stored in fast in-memory buffers for recent access
// and added to persistent batchers for durable storage.
//
// The handler ensures that all messages received from network connections are
// properly processed through both storage paths, providing both fast access to
// recent messages and reliable persistence for long-term storage and recovery.
//
// Architecture integration:
// - Receives messages from network layer (TCP/HTTP servers)
// - Stores messages in topic-specific memory buffers for fast retrieval
// - Queues messages in batching system for efficient persistent storage
// - Handles errors from both storage paths and provides unified error reporting
type ObeliskConnectionHandler struct {
	topicBuffers *buffer.TopicBuffers // In-memory ring buffers for fast recent message access
	batcher      *batch.TopicBatcher  // Persistent batching system for durable storage
}

// NewObeliskConnectionHandler creates a new connection handler with the specified storage components.
// The handler requires both a topic buffer system for in-memory operations and a topic batcher
// for persistent storage operations. These components work together to provide the dual-path
// storage architecture that enables both fast access and reliable persistence.
//
// The handler assumes that both provided components are properly initialized and ready
// for operation. It does not perform any initialization or configuration of the storage
// components, delegating that responsibility to the caller.
//
// Parameters:
//   - tb: Initialized topic buffers for in-memory message storage
//   - b: Initialized topic batcher for persistent message storage
//
// Returns:
//   - *ObeliskConnectionHandler: Configured handler ready to process messages
func NewObeliskConnectionHandler(tb *buffer.TopicBuffers, b *batch.TopicBatcher) *ObeliskConnectionHandler {
	return &ObeliskConnectionHandler{
		topicBuffers: tb,
		batcher:      b,
	}
}

// HandleMessage processes an incoming message through the broker's storage pipeline.
// This method implements the core message processing logic, ensuring that each message
// is stored in both the in-memory buffer system and the persistent batching system.
// The dual storage approach provides both fast access to recent messages and reliable
// persistence for durability and recovery.
//
// Processing flow:
// 1. Log the incoming message for debugging and monitoring
// 2. Store the message in topic-specific in-memory buffer for fast access
// 3. Add the message to the persistent batcher for durable storage
// 4. Handle any errors from the batching system (buffer operations don't fail)
//
// The method uses a fail-fast approach where any error in the persistent storage
// path causes the entire operation to be reported as failed, even though the
// message may have been successfully stored in the memory buffer.
//
// Error handling:
// - Memory buffer operations are assumed to be infallible
// - Batching errors are propagated to the caller for appropriate handling
// - Errors include context information for debugging and monitoring
//
// Parameters:
//   - msg: Pointer to the message to be processed through the storage pipeline
//
// Returns:
//   - error: Any error that occurred during persistent storage operations
func (h *ObeliskConnectionHandler) HandleMessage(msg *message.Message) error {
	// Log incoming message for debugging and operational visibility
	// This provides insight into message flow and helps with troubleshooting
	fmt.Printf("Received message - Topic: %s, Key: %s, Value: %s\n", msg.Topic, msg.Key, msg.Value)

	// Store message in in-memory buffer for fast recent message access
	// This operation is designed to be infallible and provides immediate
	// availability for consumers requesting recent message history
	h.topicBuffers.Push(*msg)

	// Add message to persistent batcher for durable storage
	// This operation may fail due to storage issues and requires error handling
	if err := h.batcher.AddMessage(*msg); err != nil {
		// Wrap error with context for better debugging and monitoring
		// The error indicates that while the message is in memory buffer,
		// it may not be durably persisted, which could lead to data loss
		return fmt.Errorf("failed to add message to batcher: %w", err)
	}

	// Message successfully processed through both storage paths
	return nil
}
