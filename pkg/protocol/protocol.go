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
	"fmt"
	"io"
	"strconv"

	obeliskErrors "github.com/mush1e/obelisk/internal/errors"
)

const MaxMessageSize uint32 = 10 * 1024 * 1024 // 10MB

// ReadMessage reads a length-prefixed binary message from the reader.
func ReadMessage(r *bufio.Reader) ([]byte, error) {
	var lengthBuf [4]byte
	if _, err := io.ReadFull(r, lengthBuf[:]); err != nil {
		if err == io.EOF {
			return nil, err // Normal EOF, don't wrap
		}
		// Network errors are usually transient
		return nil, obeliskErrors.NewTransientError("read_message_length", "failed to read message length", err)
	}

	messageLength := binary.LittleEndian.Uint32(lengthBuf[:])

	if messageLength == 0 {
		return nil, obeliskErrors.NewDataError("read_message", "zero-length message received", nil)
	}
	if messageLength > MaxMessageSize {
		return nil, obeliskErrors.NewPermanentError("read_message",
			fmt.Sprintf("message too large: %s bytes", strconv.FormatUint(uint64(messageLength), 10)), nil)
	}

	message := make([]byte, messageLength)
	if _, err := io.ReadFull(r, message); err != nil {
		if err == io.EOF {
			return nil, obeliskErrors.NewDataError("read_message_body", "truncated message", err)
		}
		return nil, obeliskErrors.NewTransientError("read_message_body", "failed to read message body", err)
	}

	return message, nil
}

// WriteMessage writes a length-prefixed binary message to the writer.
func WriteMessage(w io.Writer, serializedMessage []byte) error {
	var sizeBuf [4]byte
	binary.LittleEndian.PutUint32(sizeBuf[:], uint32(len(serializedMessage)))

	if _, err := w.Write(sizeBuf[:]); err != nil {
		return obeliskErrors.NewTransientError("write_message_length", "failed to write message length", err)
	}

	if _, err := w.Write(serializedMessage); err != nil {
		return obeliskErrors.NewTransientError("write_message_body", "failed to write message body", err)
	}

	if bw, ok := w.(*bufio.Writer); ok {
		if err := bw.Flush(); err != nil {
			return obeliskErrors.NewTransientError("flush_message", "failed to flush buffer", err)
		}
	}

	return nil
}
