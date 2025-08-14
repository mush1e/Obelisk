package storage

// Storage system with file pooling, offset indexing, and atomic batch operations.
// Uses log files (.log) for message data and index files (.idx) for fast lookup.

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	obeliskErrors "github.com/mush1e/obelisk/internal/errors"
	"github.com/mush1e/obelisk/internal/message"
	"github.com/mush1e/obelisk/pkg/protocol"
)

// OffsetIndex maps logical message offsets to byte positions for fast random access.
type OffsetIndex struct {
	Positions []int64
	mtx       sync.RWMutex
}

// LoadIndex loads an existing index from disk or creates a new empty index.
func LoadIndex(path string) (*OffsetIndex, error) {
	idx := &OffsetIndex{Positions: []int64{}}

	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return idx, nil // Empty index for new topics
		}
		return nil, categorizeFileError("load_index", err)
	}
	defer f.Close()

	// Try to load index
	var pos int64
	for {
		if err := binary.Read(f, binary.LittleEndian, &pos); err != nil {
			if err == io.EOF {
				break
			}
			// Index corrupted! Try to rebuild
			logFile := strings.TrimSuffix(path, ".idx") + ".log"
			fmt.Printf("Index corrupted, attempting rebuild: %s\n", path)

			if rebuildErr := RebuildIndex(logFile, path); rebuildErr != nil {
				return nil, obeliskErrors.NewDataError("load_index",
					"index corrupted and rebuild failed", rebuildErr)
			}

			// Retry loading the rebuilt index
			return LoadIndex(path)
		}
		idx.Positions = append(idx.Positions, pos)
	}
	return idx, nil
}

// AppendIndex appends a position to the index file and in-memory structure.
func (idx *OffsetIndex) AppendIndex(path string, pos int64) error {
	idx.mtx.Lock()
	defer idx.mtx.Unlock()

	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return categorizeFileError("open_index_for_append", err)
	}
	defer f.Close()

	if err := binary.Write(f, binary.LittleEndian, pos); err != nil {
		return categorizeFileError("write_index_entry", err)
	}

	idx.Positions = append(idx.Positions, pos)
	return nil
}

// AppendIndices appends multiple positions atomically to the index file.
func (idx *OffsetIndex) AppendIndices(path string, poses []int64) error {
	idx.mtx.Lock()
	defer idx.mtx.Unlock()

	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return categorizeFileError("open_index_for_batch_append", err)
	}
	defer f.Close()

	for i, p := range poses {
		if err := binary.Write(f, binary.LittleEndian, p); err != nil {
			return categorizeFileError(fmt.Sprintf("write_index_entry_%d", i), err)
		}
	}

	idx.Positions = append(idx.Positions, poses...)
	return nil
}

// GetPosition retrieves the byte position for a given logical offset.
func (idx *OffsetIndex) GetPosition(offset uint64) (int64, error) {
	idx.mtx.RLock()
	defer idx.mtx.RUnlock()

	if int(offset) >= len(idx.Positions) {
		return 0, obeliskErrors.NewPermanentError("get_position", "offset beyond end of index", nil)
	}
	return idx.Positions[offset], nil
}

// AppendMessage appends a single message atomically and updates its index.
func AppendMessage(pool *FilePool, logFile, idxFile string, msg message.Message, idx *OffsetIndex) error {
	if pool == nil {
		return obeliskErrors.NewConfigurationError("append_message", "file pool not initialized", nil)
	}

	msgBin, err := message.Serialize(msg)
	if err != nil {
		return obeliskErrors.NewPermanentError("serialize_message", "failed to serialize message", err)
	}

	f, err := pool.GetOrCreate(logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY)
	if err != nil {
		return categorizePoolError("get_file_for_append", err)
	}

	pos, err := f.AppendWith(func(w io.Writer) error {
		return protocol.WriteMessage(w, msgBin)
	})
	if err != nil {
		return categorizeFileError("write_message_to_file", err)
	}

	if err := idx.AppendIndex(idxFile, pos); err != nil {
		return err // Already categorized by AppendIndex
	}

	return nil
}

// AppendMessages performs batch append of multiple messages with index updates.
func AppendMessages(pool *FilePool, logFile, idxFile string, msgs []message.Message, idx *OffsetIndex) error {
	if pool == nil {
		return obeliskErrors.NewConfigurationError("append_messages", "file pool not initialized", nil)
	}

	f, err := pool.GetOrCreate(logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY)
	if err != nil {
		return categorizePoolError("get_file_from_pool", err)
	}

	bins := make([][]byte, 0, len(msgs))
	for i, m := range msgs {
		b, err := message.Serialize(m)
		if err != nil {
			return obeliskErrors.NewPermanentError("serialize_message",
				fmt.Sprintf("failed to serialize message %d", i), err)
		}
		bins = append(bins, b)
	}

	writeProto := func(w io.Writer, mb []byte) error {
		return protocol.WriteMessage(w, mb)
	}

	_, positions, err := f.AppendBatch(bins, writeProto)
	if err != nil {
		return categorizeFileError("write_batch_to_file", err)
	}

	if err := idx.AppendIndices(idxFile, positions); err != nil {
		return err // Already categorized by AppendIndices
	}

	return nil
}

// ReadAllMessages reads all messages from a log file sequentially.
func ReadAllMessages(logFile string) ([]message.Message, error) {
	var messages []message.Message

	f, err := os.Open(logFile)
	if err != nil {
		return nil, categorizeFileError("open_log_file", err)
	}
	defer f.Close()

	r := bufio.NewReader(f)

	for {
		msgBytes, err := protocol.ReadMessage(r)
		if err != nil {
			if err == io.EOF {
				break // Normal end of file
			}
			return nil, categorizeDataError("read_message_protocol", err)
		}

		m, err := message.Deserialize(msgBytes)
		if err != nil {
			return nil, categorizeDataError("deserialize_message", err)
		}
		messages = append(messages, m)
	}
	return messages, nil
}

// ReadMessagesFromOffset reads messages starting from a specific logical offset.
func ReadMessagesFromOffset(logFile, idxFile string, offset uint64) ([]message.Message, error) {
	idx, err := LoadIndex(idxFile)
	if err != nil {
		return nil, err // Already categorized by LoadIndex
	}

	if int(offset) >= len(idx.Positions) {
		return []message.Message{}, nil // No error for valid but empty result
	}

	pos, err := idx.GetPosition(offset)
	if err != nil {
		return nil, err // Already categorized by GetPosition
	}

	f, err := os.Open(logFile)
	if err != nil {
		return nil, categorizeFileError("open_log_file_for_offset", err)
	}
	defer f.Close()

	if _, err := f.Seek(pos, io.SeekStart); err != nil {
		return nil, categorizeFileError("seek_to_offset_position", err)
	}

	r := bufio.NewReader(f)
	var messages []message.Message
	for {
		msgBytes, err := protocol.ReadMessage(r)
		if err != nil {
			if err == io.EOF {
				break // Normal end of file
			}
			return nil, categorizeDataError("read_message_from_offset", err)
		}

		m, err := message.Deserialize(msgBytes)
		if err != nil {
			return nil, categorizeDataError("deserialize_message_from_offset", err)
		}
		messages = append(messages, m)
	}
	return messages, nil
}

// GetTopicMessageCount returns the total number of messages stored for a topic.
func GetTopicMessageCount(idxFile string) (int64, error) {
	idx, err := LoadIndex(idxFile)
	if err != nil {
		return 0, err // Already categorized by LoadIndex
	}
	return int64(len(idx.Positions)), nil
}

// RebuildIndex reconstructs the index file for a given log file by
// scanning each message and recording its byte position.
func RebuildIndex(logFile, idxFile string) error {
	f, err := os.Open(logFile)
	if err != nil {
		return categorizeFileError("open_log_file_for_rebuild", err)
	}
	defer f.Close()

	r := bufio.NewReader(f)
	var positions []int64 // Collect positions first
	var offset int64 = 0

	for {
		currentPos := offset

		// Try to read message
		msgBytes, err := protocol.ReadMessage(r)
		if err != nil {
			if err == io.EOF {
				break // Normal end
			}
			// Log file corrupted - stop at last good message
			fmt.Printf("Warning: Log file corrupted at offset %d, rebuilding partial index\n", offset)
			break
		}

		// Message is valid, record its position
		positions = append(positions, currentPos)

		// Advance offset
		offset += 4 + int64(len(msgBytes))
	}

	// Now write all valid positions atomically
	tempFile := idxFile + ".tmp"
	tmpF, err := os.Create(tempFile)
	if err != nil {
		return categorizeFileError("create_temp_index", err)
	}

	for _, pos := range positions {
		if err := binary.Write(tmpF, binary.LittleEndian, pos); err != nil {
			tmpF.Close()
			os.Remove(tempFile) // Clean up on error
			return categorizeFileError("write_index_entry", err)
		}
	}

	if err := tmpF.Close(); err != nil {
		os.Remove(tempFile)
		return categorizeFileError("close_temp_index", err)
	}

	// Atomic rename
	if err := os.Rename(tempFile, idxFile); err != nil {
		os.Remove(tempFile)
		return categorizeFileError("rename_index", err)
	}

	fmt.Printf("Index rebuilt: %d messages indexed\n", len(positions))
	return nil
}
