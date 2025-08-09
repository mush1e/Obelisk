package storage

import (
	"bufio"
	"io"
	"os"

	"github.com/mush1e/obelisk/internal/message"
	"github.com/mush1e/obelisk/pkg/protocol"
)

// AppendMessage appends a serialized message to the log file and updates the index.
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
