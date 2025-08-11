package protocol

// This package provides the binary protocol implementation for the Obelisk message broker.
// The protocol defines the wire format used for message transmission over TCP connections
// and storage in log files. It implements a simple length-prefixed binary protocol that
// ensures reliable message framing and supports efficient parsing.
//
// The protocol handles:
// - Length-prefixed message framing for reliable parsing over TCP streams
// - Binary encoding with little-endian byte ordering for cross-platform compatibility
// - Message size validation to prevent memory exhaustion attacks
// - Efficient streaming read/write operations with minimal memory allocation
// - Error handling for malformed messages and network failures
// - Automatic buffer flushing for buffered writers

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strconv"
)

// MaxMessageSize defines the maximum allowed message size in bytes.
// This limit prevents memory exhaustion attacks and ensures reasonable
// memory usage patterns. Messages larger than this limit are rejected
// with ErrMessageTooLarge.
const MaxMessageSize uint32 = 10 * 1024 * 1024 // 10MB

// Protocol error definitions for message parsing and validation.
var (
	// ErrZeroLengthMessage indicates that a message with zero length was received.
	// Zero-length messages are not valid in the Obelisk protocol and may indicate
	// protocol violations or network transmission errors.
	ErrZeroLengthMessage = errors.New("zero-length message received")

	// ErrMessageTooLarge indicates that a message exceeds the MaxMessageSize limit.
	// This prevents memory exhaustion attacks and ensures bounded resource usage
	// during message processing.
	ErrMessageTooLarge = errors.New("message too large")
)

// ReadMessage reads a length-prefixed binary message from the provided reader.
// This function implements the Obelisk binary protocol for message deserialization
// from network streams or storage files. The protocol uses a 4-byte little-endian
// length prefix followed by the message payload.
//
// Binary format:
// 1. Length prefix: 4 bytes (uint32, little-endian) - size of message payload
// 2. Message payload: Variable length bytes - the actual serialized message data
//
// The function performs comprehensive validation:
// - Ensures complete reads of both length prefix and message payload
// - Validates message length is not zero (protocol violation)
// - Enforces maximum message size limits to prevent resource exhaustion
// - Handles various I/O error conditions gracefully
//
// This method is commonly used by:
// - TCP server connection handlers for reading client messages
// - Storage layer for reading messages from log files
// - Test clients for receiving acknowledgments and responses
//
// Parameters:
//   - r: Buffered reader containing the binary protocol stream
//
// Returns:
//   - []byte: The deserialized message payload bytes
//   - error: ErrZeroLengthMessage, ErrMessageTooLarge, or I/O errors
func ReadMessage(r *bufio.Reader) ([]byte, error) {
	// Read 4-byte length prefix using little-endian encoding
	var lengthBuf [4]byte
	if _, err := io.ReadFull(r, lengthBuf[:]); err != nil {
		// Return I/O errors directly (including EOF for connection closure)
		return nil, err
	}

	// Decode message length from binary format
	messageLength := binary.LittleEndian.Uint32(lengthBuf[:])

	// Validate message length constraints
	if messageLength == 0 {
		// Zero-length messages are not allowed in the protocol
		return nil, ErrZeroLengthMessage
	}
	if messageLength > MaxMessageSize {
		// Prevent memory exhaustion by rejecting oversized messages
		return nil, fmt.Errorf("%w: %s bytes", ErrMessageTooLarge, strconv.FormatUint(uint64(messageLength), 10))
	}

	// Allocate buffer for message payload with exact size
	message := make([]byte, messageLength)

	// Read complete message payload - ReadFull ensures all bytes are read
	if _, err := io.ReadFull(r, message); err != nil {
		// Return I/O errors (truncated messages, connection failures, etc.)
		return nil, err
	}

	return message, nil
}

// WriteMessage writes a binary message using the Obelisk length-prefixed protocol.
// This function implements the encoding side of the binary protocol, preparing
// serialized messages for transmission over network connections or storage in
// log files. The message is framed with a length prefix for reliable parsing.
//
// Binary format output:
// 1. Length prefix: 4 bytes (uint32, little-endian) - size of serializedMessage
// 2. Message payload: Variable length bytes - the serializedMessage data
//
// The function performs atomic write operations:
// - Writes length prefix first to establish message boundaries
// - Writes message payload immediately after
// - Flushes buffered writers to ensure data transmission
// - Provides detailed error context for debugging
//
// This method is commonly used by:
// - TCP clients for sending messages to the broker
// - Storage layer for writing messages to log files
// - Server components for sending acknowledgments
//
// Error handling distinguishes between length prefix write failures and
// payload write failures to aid in debugging network and storage issues.
//
// Parameters:
//   - w: Writer to send the framed message to (network connection, file, buffer)
//   - serializedMessage: Pre-serialized message bytes to frame and transmit
//
// Returns:
//   - error: Any error that occurred during write operations or flushing
func WriteMessage(w io.Writer, serializedMessage []byte) error {
	// Prepare 4-byte length prefix in little-endian format
	var sizeBuf [4]byte
	binary.LittleEndian.PutUint32(sizeBuf[:], uint32(len(serializedMessage)))

	// Write length prefix first to establish message boundaries
	if _, err := w.Write(sizeBuf[:]); err != nil {
		// Provide context for length prefix write failures
		return fmt.Errorf("error writing message length: %w", err)
	}

	// Write message payload immediately after length prefix
	if _, err := w.Write(serializedMessage); err != nil {
		// Provide context for payload write failures
		return fmt.Errorf("error writing message body: %w", err)
	}

	// Flush buffered writers to ensure data is transmitted
	// This is critical for network connections to avoid message delays
	if bw, ok := w.(*bufio.Writer); ok {
		return bw.Flush()
	}

	return nil
}
