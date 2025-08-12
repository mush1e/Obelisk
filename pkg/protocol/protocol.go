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

// ReadMessage reads a length-prefixed binary message from the reader.
func ReadMessage(r *bufio.Reader) ([]byte, error) {

	var lengthBuf [4]byte
	if _, err := io.ReadFull(r, lengthBuf[:]); err != nil {

		return nil, err
	}

	messageLength := binary.LittleEndian.Uint32(lengthBuf[:])

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

// WriteMessage writes a length-prefixed binary message to the writer.
func WriteMessage(w io.Writer, serializedMessage []byte) error {
	var sizeBuf [4]byte
	binary.LittleEndian.PutUint32(sizeBuf[:], uint32(len(serializedMessage)))

	if _, err := w.Write(sizeBuf[:]); err != nil {
		return fmt.Errorf("error writing message length: %w", err)
	}

	if _, err := w.Write(serializedMessage); err != nil {
		return fmt.Errorf("error writing message body: %w", err)
	}

	if bw, ok := w.(*bufio.Writer); ok {
		return bw.Flush()
	}

	return nil
}
