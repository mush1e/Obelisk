package storage

// Storage system with file pooling, offset indexing, and atomic batch operations.
// Uses log files (.log) for message data and index files (.idx) for fast lookup.

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sync"

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
			return idx, nil
		}
		return nil, err
	}
	defer f.Close()

	var pos int64
	for {
		if err := binary.Read(f, binary.LittleEndian, &pos); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		idx.Positions = append(idx.Positions, pos)
	}
	return idx, nil
}

// AppendIndex appends a position to the index file and in-memory structure.
// TODO: Consider using file pool for index files.
func (idx *OffsetIndex) AppendIndex(path string, pos int64) error {
	idx.mtx.Lock()
	defer idx.mtx.Unlock()

	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	if err := binary.Write(f, binary.LittleEndian, pos); err != nil {
		return err
	}

	idx.Positions = append(idx.Positions, pos)
	return nil
}

// AppendIndices appends multiple positions atomically to the index file.
// TODO: Consider using file pool for index files.
func (idx *OffsetIndex) AppendIndices(path string, poses []int64) error {
	idx.mtx.Lock()
	defer idx.mtx.Unlock()

	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	for _, p := range poses {
		if err := binary.Write(f, binary.LittleEndian, p); err != nil {
			return err
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
		return 0, io.EOF
	}
	return idx.Positions[offset], nil
}

// AppendMessage appends a single message atomically and updates its index.
func AppendMessage(pool *FilePool, logFile, idxFile string, msg message.Message, idx *OffsetIndex) error {
	if pool == nil {
		return errors.New("file pool not initialized")
	}

	msgBin, err := message.Serialize(msg)
	if err != nil {
		return err
	}

	f, err := pool.GetOrCreate(logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY)
	if err != nil {
		return err
	}

	pos, err := f.AppendWith(func(w io.Writer) error {
		return protocol.WriteMessage(w, msgBin)
	})
	if err != nil {
		return err
	}

	return idx.AppendIndex(idxFile, pos)
}

// AppendMessages performs batch append of multiple messages with index updates.
func AppendMessages(pool *FilePool, logFile, idxFile string, msgs []message.Message, idx *OffsetIndex) error {
	if pool == nil {
		return errors.New("file pool not initialized")
	}

	f, err := pool.GetOrCreate(logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY)
	if err != nil {
		return err
	}

	bins := make([][]byte, 0, len(msgs))
	for _, m := range msgs {
		b, err := message.Serialize(m)
		if err != nil {
			return err
		}
		bins = append(bins, b)
	}

	writeProto := func(w io.Writer, mb []byte) error {
		return protocol.WriteMessage(w, mb)
	}

	_, positions, err := f.AppendBatch(bins, writeProto)
	if err != nil {
		return err
	}

	return idx.AppendIndices(idxFile, positions)
}

// ReadAllMessages reads all messages from a log file sequentially.
func ReadAllMessages(logFile string) ([]message.Message, error) {
	var messages []message.Message

	f, err := os.Open(logFile)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r := bufio.NewReader(f)

	for {

		msgBytes, err := protocol.ReadMessage(r)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		m, err := message.Deserialize(msgBytes)
		if err != nil {
			return nil, err
		}
		messages = append(messages, m)
	}
	return messages, nil
}

// ReadMessagesFromOffset reads messages starting from a specific logical offset.
func ReadMessagesFromOffset(logFile, idxFile string, offset uint64) ([]message.Message, error) {

	idx, err := LoadIndex(idxFile)
	if err != nil {
		return nil, err
	}

	if int(offset) >= len(idx.Positions) {
		return []message.Message{}, nil
	}

	pos, err := idx.GetPosition(offset)
	if err != nil {
		return nil, err
	}

	f, err := os.Open(logFile)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	if _, err := f.Seek(pos, io.SeekStart); err != nil {
		return nil, err
	}

	r := bufio.NewReader(f)
	var messages []message.Message
	for {

		msgBytes, err := protocol.ReadMessage(r)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		m, err := message.Deserialize(msgBytes)
		if err != nil {
			return nil, err
		}
		messages = append(messages, m)
	}
	return messages, nil
}

// GetTopicMessageCount returns the total number of messages stored for a topic.
func GetTopicMessageCount(idxFile string) (int64, error) {
	idx, err := LoadIndex(idxFile)
	if err != nil {
		return 0, err
	}
	return int64(len(idx.Positions)), nil
}
