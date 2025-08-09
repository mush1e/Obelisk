package storage

import (
	"bufio"
	"io"
	"os"

	"github.com/mush1e/obelisk/internal/message"
	"github.com/mush1e/obelisk/pkg/protocol"
)

// AppendMessage appends a serialized message to the log file and updates the index.
// This is the primary interface that should be used by all components.
func AppendMessage(logFile, idxFile string, msg message.Message, idx *OffsetIndex) error {
	msgBin, err := message.Serialize(msg)
	if err != nil {
		return err
	}

	file, err := os.OpenFile(logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	pos, err := file.Seek(0, io.SeekEnd) // Get byte position before writing
	if err != nil {
		return err
	}

	w := bufio.NewWriter(file)
	if err := protocol.WriteMessage(w, msgBin); err != nil {
		return err
	}
	if err := w.Flush(); err != nil {
		return err
	}

	return idx.AppendIndex(idxFile, pos)
}

// AppendMessageSimple provides backward compatibility for components that don't need indexing
// This should be deprecated in favor of the indexed version
func AppendMessageSimple(logFile string, msg message.Message) error {
	msgBin, err := message.Serialize(msg)
	if err != nil {
		return err
	}

	file, err := os.OpenFile(logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	w := bufio.NewWriter(file)
	if err := protocol.WriteMessage(w, msgBin); err != nil {
		return err
	}
	return w.Flush()
}

// ReadAllMessages reads all messages from the log file.
func ReadAllMessages(logFile string) ([]message.Message, error) {
	var messages []message.Message

	file, err := os.Open(logFile)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	r := bufio.NewReader(file)
	for {
		msgBytes, err := protocol.ReadMessage(r)
		if err != nil {
			break // EOF
		}

		msg, err := message.Deserialize(msgBytes)
		if err != nil {
			return nil, err
		}
		messages = append(messages, msg)
	}
	return messages, nil
}

// ReadMessagesFromOffset reads messages starting from a logical offset using the index.
func ReadMessagesFromOffset(logFile, idxFile string, offset uint64) ([]message.Message, error) {
	idx, err := LoadIndex(idxFile)
	if err != nil {
		return nil, err
	}

	pos, err := idx.GetPosition(offset)
	if err != nil {
		return nil, err
	}

	file, err := os.Open(logFile)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	if _, err := file.Seek(pos, os.SEEK_SET); err != nil {
		return nil, err
	}

	r := bufio.NewReader(file)
	var messages []message.Message

	for {
		msgBytes, err := protocol.ReadMessage(r)
		if err != nil {
			break // EOF
		}

		msg, err := message.Deserialize(msgBytes)
		if err != nil {
			return nil, err
		}
		messages = append(messages, msg)
	}

	return messages, nil
}

// GetTopicMessageCount returns the total number of messages stored for a topic
func GetTopicMessageCount(idxFile string) (int64, error) {
	idx, err := LoadIndex(idxFile)
	if err != nil {
		return 0, err
	}
	return int64(len(idx.Positions)), nil
}
