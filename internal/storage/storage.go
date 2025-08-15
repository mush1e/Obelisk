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
	"time"

	obeliskErrors "github.com/mush1e/obelisk/internal/errors"
	"github.com/mush1e/obelisk/internal/message"
	"github.com/mush1e/obelisk/pkg/protocol"
)

// Recovery statistics for corruption handling
type RecoveryStats struct {
	validMessages      int
	corruptionSections int
	bytesSkipped       int64
}

// Result from attempting to skip corrupted data
type SkipResult struct {
	found        bool
	bytesSkipped int64
}

// Tunable parameters for recovery scanning
const (
	recoveryMaxSkipBytes   = 1024 * 1024 // 1MB max scan while attempting recovery
	recoveryScanWindowSize = 1024        // 1KB chunks for scanning
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

	// RAII: File automatically protected from cleanup during callback
	return pool.WithFile(logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, func(f *File) error {
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
	})
}

// AppendMessages performs batch append of multiple messages with index updates.
func AppendMessages(pool *FilePool, logFile, idxFile string, msgs []message.Message, idx *OffsetIndex) error {
	if pool == nil {
		return obeliskErrors.NewConfigurationError("append_messages", "file pool not initialized", nil)
	}

	// Pre-serialize all messages
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

	// RAII: File automatically protected from cleanup during callback
	return pool.WithFile(logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, func(f *File) error {
		_, positions, err := f.AppendBatch(bins, writeProto)
		if err != nil {
			return categorizeFileError("write_batch_to_file", err)
		}

		if err := idx.AppendIndices(idxFile, positions); err != nil {
			return err // Already categorized by AppendIndices
		}

		return nil
	})
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

// AppendMessagesWithFile performs batch append using an already-acquired file handle
// This is for cases where you already have a file from WithFile
func AppendMessagesWithFile(logFile *File, idxFile string, msgs []message.Message, idx *OffsetIndex) error {
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

	_, positions, err := logFile.AppendBatch(bins, writeProto)
	if err != nil {
		return categorizeFileError("write_batch_to_file", err)
	}

	if err := idx.AppendIndices(idxFile, positions); err != nil {
		return err
	}

	return nil
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

	f, err := os.Open(logFile)
	if err != nil {
		return nil, categorizeFileError("open_log_file_for_offset", err)
	}
	defer f.Close()

	r := bufio.NewReader(f)
	var messages []message.Message

	for i := int(offset); i < len(idx.Positions); i++ {
		pos := idx.Positions[i]
		if _, err := f.Seek(pos, io.SeekStart); err != nil {
			// Seek failure: categorize and stop returning what we have so far
			return messages, categorizeFileError("seek_to_index_position", err)
		}
		r.Reset(f)

		msgBytes, readErr := protocol.ReadMessage(r)
		if readErr != nil {
			// Reading at an indexed position should succeed; if not, categorize and continue
			// to salvage subsequent messages when possible.
			continue
		}

		m, deserErr := message.Deserialize(msgBytes)
		if deserErr != nil {
			// Skip malformed message even if indexed; continue to next
			continue
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
// IMPROVED: Can skip over corrupted sections and continue recovery.
func RebuildIndex(logFile, idxFile string) error {
	// Backup corrupt index file before rebuilding
	if _, err := os.Stat(idxFile); err == nil {
		backupFile := idxFile + ".corrupt." + time.Now().Format("20060102-150405")
		os.Rename(idxFile, backupFile)
		fmt.Printf("Backed up corrupt index to: %s\n", backupFile)
	}

	f, err := os.Open(logFile)
	if err != nil {
		return categorizeFileError("open_log_file_for_rebuild", err)
	}
	defer f.Close()

	// Use our new smart recovery function
	positions, recoveryStats := recoverMessagesWithSkipping(f)

	// Log what we accomplished
	fmt.Printf("Recovery complete: %d valid messages indexed\n", len(positions))
	if recoveryStats.corruptionSections > 0 {
		fmt.Printf("Skipped %d corrupted sections (%d bytes total)\n",
			recoveryStats.corruptionSections, recoveryStats.bytesSkipped)
	}

	// Write the recovered positions to a new index file
	return writeIndexFile(idxFile, positions)
}

func recoverMessagesWithSkipping(f *os.File) ([]int64, RecoveryStats) {
	var positions []int64
	var stats RecoveryStats
	var offset int64 = 0

	// Create a buffered reader for efficient scanning
	r := bufio.NewReader(f)

	for {
		// Remember where we are in case we find a valid message
		currentPos := offset

		// Try to read message at current position
		msgBytes, err := protocol.ReadMessage(r)
		if err != nil {
			if err == io.EOF {
				break // Normal end of file - we're done
			}

			// Corruption detected - try to skip to next valid message
			fmt.Printf("Corruption at offset %d, attempting recovery...\n", offset)
			stats.corruptionSections++

			// Try to find the next valid message
			skipResult := skipToNextValidMessage(f, &offset)
			if skipResult.found {
				stats.bytesSkipped += skipResult.bytesSkipped
				fmt.Printf("Resumed scanning at offset %d (skipped %d bytes)\n",
					offset, skipResult.bytesSkipped)

				// Create new reader from the recovery position
				if _, err := f.Seek(offset, io.SeekStart); err != nil {
					break // Can't seek, give up
				}
				r = bufio.NewReader(f)
				continue // Try again from new position
			} else {
				fmt.Printf("Could not find valid messages after corruption, stopping\n")
				break // Could not recover
			}
		}

		// Validate deserialization to ensure the frame actually contains a valid message schema
		if _, deserErr := message.Deserialize(msgBytes); deserErr != nil {
			// Treat schema-invalid frame as corruption and attempt recovery from here
			fmt.Printf("Schema-invalid frame at offset %d, attempting recovery...\n", currentPos)
			stats.corruptionSections++

			// Reset file position to currentPos for skip search baseline
			offset = currentPos
			skipResult := skipToNextValidMessage(f, &offset)
			if skipResult.found {
				stats.bytesSkipped += skipResult.bytesSkipped
				fmt.Printf("Resumed scanning at offset %d (skipped %d bytes)\n",
					offset, skipResult.bytesSkipped)
				if _, err := f.Seek(offset, io.SeekStart); err != nil {
					break
				}
				r = bufio.NewReader(f)
				continue
			}
			fmt.Printf("Could not recover after schema-invalid frame, stopping\n")
			break
		}

		// Message is valid, record its position
		positions = append(positions, currentPos)
		stats.validMessages++

		// Advance offset for next message (4-byte length + message data)
		offset += 4 + int64(len(msgBytes))
	}

	return positions, stats
}

// skipToNextValidMessage attempts to find the next valid message after corruption
func skipToNextValidMessage(f *os.File, currentOffset *int64) SkipResult {
	startOffset := *currentOffset
	buffer := make([]byte, recoveryScanWindowSize)

	// Scan forward looking for a valid message pattern
	for bytesSkipped := int64(0); bytesSkipped < recoveryMaxSkipBytes; bytesSkipped += int64(recoveryScanWindowSize) {
		// Calculate position to scan
		scanPos := startOffset + bytesSkipped

		// Seek to next scan position
		if _, err := f.Seek(scanPos, io.SeekStart); err != nil {
			return SkipResult{found: false, bytesSkipped: bytesSkipped}
		}

		// Read a chunk to scan for message patterns
		n, err := f.Read(buffer)
		if err != nil {
			if err == io.EOF {
				return SkipResult{found: false, bytesSkipped: bytesSkipped}
			}
			continue // Try next chunk on other errors
		}

		// Look for potential message start patterns in this chunk
		for i := 0; i < n-4; i++ { // Need at least 4 bytes for length
			candidatePos := scanPos + int64(i)

			// Extract potential message length from these 4 bytes
			lengthBytes := buffer[i : i+4]
			messageLength := binary.LittleEndian.Uint32(lengthBytes)

			// Quick sanity check - is this a reasonable message length?
			if messageLength == 0 || messageLength > protocol.MaxMessageSize {
				continue // Skip this position
			}

			// Try to read a complete message from this position
			if _, err := f.Seek(candidatePos, io.SeekStart); err != nil {
				continue
			}

			// Create new buffered reader from this position
			testReader := bufio.NewReader(f)

			// Test if we can read a valid message
			if testBytes, testErr := protocol.ReadMessage(testReader); testErr == nil {
				// Try to deserialize to confirm it's truly a valid message
				if _, deserErr := message.Deserialize(testBytes); deserErr == nil {
					// SUCCESS! Found valid message
					*currentOffset = candidatePos
					return SkipResult{
						found:        true,
						bytesSkipped: candidatePos - startOffset,
					}
				}
			}
		}
	}

	// Couldn't find valid message within our search limit
	return SkipResult{found: false, bytesSkipped: recoveryMaxSkipBytes}
}

// writeIndexFile atomically writes positions to index file
func writeIndexFile(idxFile string, positions []int64) error {
	tempFile := idxFile + ".tmp"
	tmpF, err := os.Create(tempFile)
	if err != nil {
		return categorizeFileError("create_temp_index", err)
	}

	// Write all positions
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

	// Atomic rename to replace old index
	if err := os.Rename(tempFile, idxFile); err != nil {
		os.Remove(tempFile)
		return categorizeFileError("rename_index", err)
	}

	return nil
}
