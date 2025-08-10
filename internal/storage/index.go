package storage

// This package provides indexing functionality for Obelisk message log files.
// The index maps logical message offsets to byte positions in the log files,
// enabling efficient random access to messages without scanning the entire file.

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
)

// OffsetIndex maps message offsets → byte positions in the log file.
// Each position in the slice represents the byte offset where a message begins
// in the corresponding log file, allowing for O(1) lookup of message locations.
type OffsetIndex struct {
	Positions []int64 // Array of byte positions, indexed by message offset
}

// LoadIndex loads an index from a .idx file or creates an empty one if it doesn't exist.
// The index file format stores int64 byte positions sequentially in little-endian format.
func LoadIndex(idxFile string) (*OffsetIndex, error) {
	// Return empty index if file doesn't exist yet
	file, err := os.Open(idxFile)
	if os.IsNotExist(err) {
		return &OffsetIndex{Positions: []int64{}}, nil
	}
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// Read all int64 positions from the index file
	var positions []int64
	r := bufio.NewReader(file)
	for {
		var pos int64
		err := binary.Read(r, binary.LittleEndian, &pos)
		if err != nil {
			break // EOF reached
		}
		positions = append(positions, pos)
	}
	return &OffsetIndex{Positions: positions}, nil
}

// AppendIndex adds a new position to the index file and updates the in-memory index.
// This should be called after successfully writing a message to the log file.
func (idx *OffsetIndex) AppendIndex(idxFile string, pos int64) error {
	// Open index file for appending
	file, err := os.OpenFile(idxFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	// Write the byte position as int64 in little-endian format
	w := bufio.NewWriter(file)
	if err := binary.Write(w, binary.LittleEndian, pos); err != nil {
		return err
	}
	if err := w.Flush(); err != nil {
		return err
	}

	// Update the in-memory index to reflect the new position
	idx.Positions = append(idx.Positions, pos)
	return nil
}

// GetPosition returns the byte position for a given logical message offset.
// This allows seeking to the exact position in the log file where a message begins.
func (idx *OffsetIndex) GetPosition(offset uint64) (int64, error) {
	if int(offset) >= len(idx.Positions) {
		return 0, fmt.Errorf("offset %d out of range", offset)
	}
	return idx.Positions[offset], nil
}
