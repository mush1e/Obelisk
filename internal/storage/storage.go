package storage

// This package provides persistent storage functionality for Obelisk messages.
// It handles writing messages to log files with indexing support for efficient
// random access and reading. The storage format uses a binary protocol with
// separate log and index files for each topic.

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/mush1e/obelisk/internal/message"
	"github.com/mush1e/obelisk/pkg/protocol"
)

// Global file pool for the storage package
var pool *FilePool

// InitializePool sets up the file pool with specified settings
func InitializePool(idleTimeout time.Duration, cleanupInterval time.Duration) {
	pool = NewFilePool(idleTimeout)
	pool.StartCleanup(cleanupInterval)
}

// ShutdownPool gracefully closes all files and stops the cleanup routine
func ShutdownPool() error {
	if pool != nil {
		return pool.Stop()
	}
	return nil
}

// GetPool returns the current file pool (useful for testing or stats)
func GetPool() *FilePool {
	return pool
}

// Initialize with defaults when package loads
func init() {
	InitializePool(5*time.Minute, 30*time.Second)
}

// AppendMessage appends a serialized message to the log file and updates the index.
// This is the primary interface that should be used by all components for persistent storage.
// It ensures atomic writes by recording the byte position before writing and updating the index.
func AppendMessage(logFile, idxFile string, msg message.Message, idx *OffsetIndex) error {
	// Serialize the message to binary format
	msgBin, err := message.Serialize(msg)
	if err != nil {
		return err
	}

	// Get or create file handle from the pool
	file, err := pool.GetOrCreate(logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY)
	if err != nil {
		return err
	}

	// Get current file offset (end) to use in index
	pos, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	// Serialize message to bytes with protocol format in a buffer
	buf := &bytes.Buffer{}
	w := bufio.NewWriter(buf)
	if err := protocol.WriteMessage(w, msgBin); err != nil {
		return err
	}
	if err := w.Flush(); err != nil {
		return err
	}
	data := buf.Bytes()

	// Write data using pool's WriteAndMarkDirty so dirty flag is set
	n, err := pool.WriteAndMarkDirty(logFile, data)
	if err != nil {
		return err
	}
	if n != len(data) {
		return fmt.Errorf("partial write: wrote %d bytes, expected %d", n, len(data))
	}

	// Flush the file to ensure data is synced to disk
	if err := pool.Flush(logFile); err != nil {
		return err
	}

	// Update index with the byte position of the message
	return idx.AppendIndex(idxFile, pos)
}

// AppendMessageSimple provides backward compatibility for components that don't need indexing.
// This should be deprecated in favor of the indexed version (AppendMessage).
// It writes messages without maintaining an index, making random access impossible.
func AppendMessageSimple(logFile string, msg message.Message) error {
	// Serialize message to binary format
	msgBin, err := message.Serialize(msg)
	if err != nil {
		return err
	}

	// Open log file for appending
	file, err := os.OpenFile(logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	// Write message using protocol format (no indexing)
	w := bufio.NewWriter(file)
	if err := protocol.WriteMessage(w, msgBin); err != nil {
		return err
	}
	return w.Flush()
}

// ReadAllMessages reads all messages from the log file sequentially from beginning to end.
// This is useful for consumers that want to read the entire topic history.
func ReadAllMessages(logFile string) ([]message.Message, error) {
	var messages []message.Message

	// Open log file for reading
	file, err := os.Open(logFile)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// Read messages sequentially until EOF
	r := bufio.NewReader(file)
	for {
		msgBytes, err := protocol.ReadMessage(r)
		if err != nil {
			break // EOF reached
		}

		// Deserialize binary message back to struct
		msg, err := message.Deserialize(msgBytes)
		if err != nil {
			return nil, err
		}
		messages = append(messages, msg)
	}
	return messages, nil
}

// ReadMessagesFromOffset reads messages starting from a logical offset using the index.
// This enables efficient random access by seeking to the byte position in the log file
// that corresponds to the given message offset.
func ReadMessagesFromOffset(logFile, idxFile string, offset uint64) ([]message.Message, error) {
	// Load the index to map offsets to byte positions
	idx, err := LoadIndex(idxFile)
	if err != nil {
		return nil, err
	}

	// Return empty slice if offset is beyond available messages
	if int(offset) >= len(idx.Positions) {
		return []message.Message{}, nil
	}

	// Get the byte position for the requested offset
	pos, err := idx.GetPosition(offset)
	if err != nil {
		return nil, err
	}

	// Open log file and seek to the calculated position
	file, err := os.Open(logFile)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	if _, err := file.Seek(pos, os.SEEK_SET); err != nil {
		return nil, err
	}

	// Read all messages from the offset position to EOF
	r := bufio.NewReader(file)
	var messages []message.Message

	for {
		msgBytes, err := protocol.ReadMessage(r)
		if err != nil {
			break // EOF reached
		}

		// Deserialize each message
		msg, err := message.Deserialize(msgBytes)
		if err != nil {
			return nil, err
		}
		messages = append(messages, msg)
	}

	return messages, nil
}

// GetTopicMessageCount returns the total number of messages stored for a topic.
// It uses the index file to determine the count without reading the entire log file.
func GetTopicMessageCount(idxFile string) (int64, error) {
	// Load index and return the number of indexed positions
	idx, err := LoadIndex(idxFile)
	if err != nil {
		return 0, err
	}
	return int64(len(idx.Positions)), nil
}
