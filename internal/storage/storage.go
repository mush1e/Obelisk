package storage

// This package provides the core storage functionality for the Obelisk message broker.
// It implements a high-performance, durable storage system that combines:
// - File pooling for efficient file handle management
// - Offset indexing for fast message lookup by sequence number
// - Atomic batch operations for optimal write performance
// - Protocol-aware message serialization and framing
//
// The storage system operates with two types of files per topic:
// 1. Log files (.log): Contain the actual message data in protocol format
// 2. Index files (.idx): Contain byte offsets for fast random access
//
// This design enables both sequential writes (for performance) and random reads
// (for consumer positioning) while maintaining data durability guarantees.

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sync"
	"time"

	"github.com/mush1e/obelisk/internal/message"
	"github.com/mush1e/obelisk/pkg/protocol"
)

// Global file pool instance used by all storage operations.
// This is initialized once during application startup and shared
// across all topics to minimize file handle overhead.
var pool *FilePool

// InitializePool creates and configures the global file pool.
// This must be called once during application startup before any
// storage operations are performed. The pool manages file handles
// automatically, closing idle files to prevent resource exhaustion.
//
// Parameters:
//   - idleTimeout: How long a file can remain unused before cleanup
//   - cleanupInterval: How often to check for idle files to cleanup
func InitializePool(idleTimeout time.Duration, cleanupInterval time.Duration) {
	pool = NewFilePool(idleTimeout)
	pool.StartCleanup(cleanupInterval)
}

// ShutdownPool gracefully closes all files in the pool.
// This should be called during application shutdown to ensure
// all file handles are properly closed and any pending writes
// are flushed to disk.
//
// Returns:
//   - error: Any error that occurred during pool shutdown
func ShutdownPool() error {
	if pool != nil {
		return pool.Stop()
	}
	return nil
}

// GetPool returns the global file pool instance.
// This is primarily used for testing and diagnostic purposes.
// Normal storage operations should use the higher-level functions
// in this package rather than accessing the pool directly.
//
// Returns:
//   - *FilePool: The global file pool instance, or nil if not initialized
func GetPool() *FilePool { return pool }

// OffsetIndex maps logical message offsets (0..N-1) to byte positions in the log file.
// This enables fast random access to messages by their sequence number without
// needing to scan the entire log file. The index is kept in memory for performance
// and persisted to disk for durability across restarts.
//
// On-disk format: A sequence of little-endian int64 values representing byte positions
// Memory format: A slice of int64 positions with concurrent access protection
type OffsetIndex struct {
	Positions []int64      // Slice of byte positions corresponding to message offsets
	mtx       sync.RWMutex // Protects concurrent access to the positions slice
}

// LoadIndex loads an existing index from disk or creates a new empty index.
// This function handles both the initial creation case (file doesn't exist)
// and the recovery case (loading existing index after restart). The index
// file contains a sequence of binary-encoded int64 values representing
// byte positions in the corresponding log file.
//
// Parameters:
//   - path: Filesystem path to the index file
//
// Returns:
//   - *OffsetIndex: Loaded index with positions from disk, or empty index if file doesn't exist
//   - error: Any error that occurred during file reading (file not existing is not an error)
func LoadIndex(path string) (*OffsetIndex, error) {
	idx := &OffsetIndex{Positions: []int64{}}

	// Attempt to open the index file
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			// File doesn't exist - this is normal for new topics
			return idx, nil
		}
		return nil, err
	}
	defer f.Close()

	// Read all positions from the file
	var pos int64
	for {
		if err := binary.Read(f, binary.LittleEndian, &pos); err != nil {
			if err == io.EOF {
				break // Normal end of file
			}
			return nil, err
		}
		idx.Positions = append(idx.Positions, pos)
	}
	return idx, nil
}

// AppendIndex appends a single position to the index file and in-memory structure.
// This method ensures atomic updates by writing to disk first, then updating
// the in-memory structure only after successful disk write. This maintains
// consistency between disk and memory state.
//
// TODO: Consider using file pool for index files to avoid repeated open/close operations
// and improve performance for high-throughput scenarios.
//
// Parameters:
//   - path: Filesystem path to the index file
//   - pos: Byte position to append to the index
//
// Returns:
//   - error: Any error that occurred during file writing or memory update
func (idx *OffsetIndex) AppendIndex(path string, pos int64) error {
	idx.mtx.Lock()
	defer idx.mtx.Unlock()

	// Open file for appending (create if doesn't exist)
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	// Write position to disk first
	if err := binary.Write(f, binary.LittleEndian, pos); err != nil {
		return err
	}

	// Update in-memory structure only after successful disk write
	idx.Positions = append(idx.Positions, pos)
	return nil
}

// AppendIndices appends multiple positions atomically to the index file.
// This is an optimized version of AppendIndex for batch operations, writing
// all positions in a single file operation for better performance. The entire
// operation is atomic - either all positions are written successfully, or none
// are added to the in-memory structure.
//
// TODO: Consider using file pool for index files to avoid repeated open/close operations
// and improve performance for high-throughput batch scenarios.
//
// Parameters:
//   - path: Filesystem path to the index file
//   - poses: Slice of byte positions to append to the index
//
// Returns:
//   - error: Any error that occurred during batch writing or memory update
func (idx *OffsetIndex) AppendIndices(path string, poses []int64) error {
	idx.mtx.Lock()
	defer idx.mtx.Unlock()

	// Open file for appending (create if doesn't exist)
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	// Write all positions to disk first
	for _, p := range poses {
		if err := binary.Write(f, binary.LittleEndian, p); err != nil {
			return err
		}
	}

	// Update in-memory structure only after all disk writes succeed
	idx.Positions = append(idx.Positions, poses...)
	return nil
}

// GetPosition retrieves the byte position for a given logical offset.
// This method provides fast random access to message positions without
// needing to scan the log file. It uses read locks for concurrent access
// while protecting against concurrent modifications.
//
// Parameters:
//   - offset: Logical message offset (0-based sequence number)
//
// Returns:
//   - int64: Byte position in the log file where the message starts
//   - error: io.EOF if offset is beyond the available range
func (idx *OffsetIndex) GetPosition(offset uint64) (int64, error) {
	idx.mtx.RLock()
	defer idx.mtx.RUnlock()

	if int(offset) >= len(idx.Positions) {
		return 0, io.EOF // Offset beyond available range
	}
	return idx.Positions[offset], nil
}

// AppendMessage appends a single message atomically and updates its index.
// This is the primary method for storing individual messages with full
// durability guarantees. The operation is atomic - either both the message
// and its index entry are written successfully, or neither is committed.
//
// The process:
// 1. Serialize the message to binary format
// 2. Append to log file using file pool (with protocol framing)
// 3. Add the byte position to the index file
// 4. Update in-memory index
//
// Parameters:
//   - logFile: Path to the topic's log file
//   - idxFile: Path to the topic's index file
//   - msg: Message to store
//   - idx: In-memory index to update
//
// Returns:
//   - error: Any error during serialization, file writing, or index updates
func AppendMessage(logFile, idxFile string, msg message.Message, idx *OffsetIndex) error {
	if pool == nil {
		return errors.New("file pool not initialized")
	}

	// Serialize message to binary format
	msgBin, err := message.Serialize(msg)
	if err != nil {
		return err
	}

	// Get file handle from pool (creates if necessary)
	f, err := pool.GetOrCreate(logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY)
	if err != nil {
		return err
	}

	// Append message with protocol framing
	pos, err := f.AppendWith(func(w io.Writer) error {
		return protocol.WriteMessage(w, msgBin)
	})
	if err != nil {
		return err
	}

	// Update index with message position
	return idx.AppendIndex(idxFile, pos)
}

// AppendMessages performs batch append of multiple messages with index updates.
// This is an optimized version of AppendMessage for high-throughput scenarios.
// It minimizes disk I/O by batching operations and reduces locking overhead
// by performing all operations for a batch atomically.
//
// The batch operation process:
// 1. Serialize all messages to binary format
// 2. Write all messages to log file in a single operation
// 3. Update index with all message positions atomically
//
// This provides better performance than individual message writes while
// maintaining the same durability guarantees.
//
// Parameters:
//   - logFile: Path to the topic's log file
//   - idxFile: Path to the topic's index file
//   - msgs: Slice of messages to store
//   - idx: In-memory index to update
//
// Returns:
//   - error: Any error during serialization, file writing, or index updates
func AppendMessages(logFile, idxFile string, msgs []message.Message, idx *OffsetIndex) error {
	if pool == nil {
		return errors.New("file pool not initialized")
	}

	// Get file handle from pool (creates if necessary)
	f, err := pool.GetOrCreate(logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY)
	if err != nil {
		return err
	}

	// Serialize all messages upfront
	bins := make([][]byte, 0, len(msgs))
	for _, m := range msgs {
		b, err := message.Serialize(m)
		if err != nil {
			return err
		}
		bins = append(bins, b)
	}

	// Protocol framing adapter for the file pool's batch append method
	writeProto := func(w io.Writer, mb []byte) error {
		return protocol.WriteMessage(w, mb)
	}

	// Perform batch write to log file
	_, positions, err := f.AppendBatch(bins, writeProto)
	if err != nil {
		return err
	}

	// Update index with all message positions
	return idx.AppendIndices(idxFile, positions)
}

// AppendMessageSimple provides compatibility method without indexing.
// This is a simplified version of AppendMessage for scenarios where
// message indexing is not required. It still uses the file pool for
// efficient file handle management but skips index maintenance.
//
// Use this method when:
// - Only sequential access to messages is needed
// - Index overhead is not justified
// - Compatibility with legacy code is required
//
// Parameters:
//   - logFile: Path to the log file
//   - msg: Message to store
//
// Returns:
//   - error: Any error during serialization or file writing
func AppendMessageSimple(logFile string, msg message.Message) error {
	if pool == nil {
		return errors.New("file pool not initialized")
	}

	// Get file handle from pool
	f, err := pool.GetOrCreate(logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY)
	if err != nil {
		return err
	}

	// Append message without index update
	_, err = f.AppendWith(func(w io.Writer) error {
		b, serr := message.Serialize(msg)
		if serr != nil {
			return serr
		}
		return protocol.WriteMessage(w, b)
	})
	return err
}

// ReadAllMessages reads and deserializes all messages from a log file sequentially.
// This method is useful for full topic reconstruction, debugging, or migration
// scenarios. It reads the entire log file from beginning to end, deserializing
// each message in order.
//
// Note: This method loads all messages into memory, so it should be used
// carefully with large log files. Consider using ReadMessagesFromOffset
// for reading specific ranges of messages.
//
// Parameters:
//   - logFile: Path to the log file to read
//
// Returns:
//   - []message.Message: All messages from the log file in order
//   - error: Any error during file reading or message deserialization
func ReadAllMessages(logFile string) ([]message.Message, error) {
	var messages []message.Message

	// Open log file for reading
	f, err := os.Open(logFile)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// Use buffered reader for performance
	r := bufio.NewReader(f)

	// Read messages until EOF
	for {
		// Read protocol-framed message
		msgBytes, err := protocol.ReadMessage(r)
		if err != nil {
			if err == io.EOF {
				break // Normal end of file
			}
			return nil, err
		}

		// Deserialize message
		m, err := message.Deserialize(msgBytes)
		if err != nil {
			return nil, err
		}
		messages = append(messages, m)
	}
	return messages, nil
}

// ReadMessagesFromOffset reads messages starting from a specific logical offset.
// This method uses the index to seek directly to the specified offset without
// reading earlier messages, providing efficient random access to message ranges.
// It's commonly used by consumers to read from their last known position.
//
// The process:
// 1. Load the index from disk
// 2. Look up the byte position for the requested offset
// 3. Seek to that position in the log file
// 4. Read messages sequentially from that point to EOF
//
// Parameters:
//   - logFile: Path to the log file to read
//   - idxFile: Path to the index file for position lookup
//   - offset: Logical offset (0-based) to start reading from
//
// Returns:
//   - []message.Message: Messages from the specified offset to end of log
//   - error: Any error during index loading, file operations, or deserialization
func ReadMessagesFromOffset(logFile, idxFile string, offset uint64) ([]message.Message, error) {
	// Load index for position lookup
	idx, err := LoadIndex(idxFile)
	if err != nil {
		return nil, err
	}

	// Check if offset is beyond available messages
	if int(offset) >= len(idx.Positions) {
		return []message.Message{}, nil // Return empty slice for out-of-range offset
	}

	// Get byte position for the requested offset
	pos, err := idx.GetPosition(offset)
	if err != nil {
		return nil, err
	}

	// Open log file and seek to position
	f, err := os.Open(logFile)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	if _, err := f.Seek(pos, io.SeekStart); err != nil {
		return nil, err
	}

	// Read messages from offset position to EOF
	r := bufio.NewReader(f)
	var messages []message.Message
	for {
		// Read protocol-framed message
		msgBytes, err := protocol.ReadMessage(r)
		if err != nil {
			if err == io.EOF {
				break // Normal end of file
			}
			return nil, err
		}

		// Deserialize message
		m, err := message.Deserialize(msgBytes)
		if err != nil {
			return nil, err
		}
		messages = append(messages, m)
	}
	return messages, nil
}

// GetTopicMessageCount returns the total number of messages stored for a topic.
// This method loads the index file and returns the count of indexed positions,
// which corresponds to the number of messages that have been successfully
// written to the topic. This is useful for monitoring, statistics, and
// consumer positioning.
//
// Parameters:
//   - idxFile: Path to the topic's index file
//
// Returns:
//   - int64: Total number of messages in the topic
//   - error: Any error during index loading
func GetTopicMessageCount(idxFile string) (int64, error) {
	idx, err := LoadIndex(idxFile)
	if err != nil {
		return 0, err
	}
	return int64(len(idx.Positions)), nil
}
