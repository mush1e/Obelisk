package storage

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
)

// OffsetIndex maps message offsets → byte positions in the log file
type OffsetIndex struct {
	Positions []int64
}

// LoadIndex loads an index from a .idx file or creates an empty one if it doesn't exist.
func LoadIndex(idxFile string) (*OffsetIndex, error) {
	file, err := os.Open(idxFile)
	if os.IsNotExist(err) {
		return &OffsetIndex{Positions: []int64{}}, nil
	}
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var positions []int64
	r := bufio.NewReader(file)
	for {
		var pos int64
		err := binary.Read(r, binary.LittleEndian, &pos)
		if err != nil {
			break // EOF
		}
		positions = append(positions, pos)
	}
	return &OffsetIndex{Positions: positions}, nil
}

// AppendIndex adds a new position to the index file.
func (idx *OffsetIndex) AppendIndex(idxFile string, pos int64) error {
	file, err := os.OpenFile(idxFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	w := bufio.NewWriter(file)
	if err := binary.Write(w, binary.LittleEndian, pos); err != nil {
		return err
	}
	if err := w.Flush(); err != nil {
		return err
	}

	idx.Positions = append(idx.Positions, pos)
	return nil
}

// GetPosition returns the byte position for a given offset.
func (idx *OffsetIndex) GetPosition(offset uint64) (int64, error) {
	if int(offset) >= len(idx.Positions) {
		return 0, fmt.Errorf("offset %d out of range", offset)
	}
	return idx.Positions[offset], nil
}
