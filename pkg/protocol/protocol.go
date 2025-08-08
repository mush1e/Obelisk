package protocol

// This package provides functions for reading and writing messages over different protocols.

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strconv"
)

const MaxMessageSize uint32 = 10 * 1024 * 1024 // 10MB

var (
	ErrZeroLengthMessage = errors.New("zero-length message received")
	ErrMessageTooLarge   = errors.New("message too large")
)

// ReadMessage reads a message from the given reader.
func ReadMessage(r *bufio.Reader) ([]byte, error) {
	var lengthBuf [4]byte
	if _, err := io.ReadFull(r, lengthBuf[:]); err != nil {
		return nil, err
	}

	messageLength := binary.LittleEndian.Uint32(lengthBuf[:])
	if messageLength == 0 {
		return nil, ErrZeroLengthMessage
	}
	if messageLength > MaxMessageSize {
		return nil, fmt.Errorf("%w: %s bytes", ErrMessageTooLarge, strconv.FormatUint(uint64(messageLength), 10))
	}

	message := make([]byte, messageLength)
	if _, err := io.ReadFull(r, message); err != nil {
		return nil, err
	}
	return message, nil
}

// WriteMessage writes a message to the given writer.
func WriteMessage(w *bufio.Writer, serializedMessage []byte) error {
	var sizeBuf [4]byte
	binary.LittleEndian.PutUint32(sizeBuf[:], uint32(len(serializedMessage)))

	if _, err := w.Write(sizeBuf[:]); err != nil {
		return fmt.Errorf("error writing message length: %w", err)
	}
	if _, err := w.Write(serializedMessage); err != nil {
		return fmt.Errorf("error writing message body: %w", err)
	}
	return w.Flush()
}
