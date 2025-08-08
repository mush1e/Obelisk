package storage

// This package provides storage functionality for messages.
// It is responsible for appending and reading messages from a log file.
// This log file represents a sequence of messages, each prefixed with its length.
// It represents the single source of truth for all messages in the system.
//
import (
	"bufio"
	"os"

	"github.com/mush1e/obelisk/internal/message"
	"github.com/mush1e/obelisk/pkg/protocol"
)

// AppendMessage appends a serialized length-prefixed message to the file.
func AppendMessage(filename string, msg message.Message) error {
	msgBin, err := message.Serialize(msg)
	if err != nil {
		return err
	}

	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
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

// ReadAllMessages reads all messages from the file.
func ReadAllMessages(filename string) ([]message.Message, error) {
	var messages []message.Message

	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	r := bufio.NewReader(file)
	for {
		msgBytes, err := protocol.ReadMessage(r)
		if err != nil {
			if err.Error() == "EOF" || err == os.ErrClosed {
				break
			}
			return messages, err
		}

		msg, err := message.Deserialize(msgBytes)
		if err != nil {
			return messages, err
		}
		messages = append(messages, msg)
	}

	return messages, nil
}
