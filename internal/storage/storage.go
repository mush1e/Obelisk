package storage

import (
	"bufio"
	"os"

	"github.com/mush1e/obelisk/internal/message"
	"github.com/mush1e/obelisk/pkg/protocol"
)

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
