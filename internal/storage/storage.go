package storage

// This package provides a storage type for Obelisk.
// It is used to store messages that are received from the network.
// The storage type is a struct that contains a filename and a buffer.
// The storage type also provides methods for appending messages and reading all messages.

import (
	"encoding/binary"
	"io"
	"os"

	"github.com/mush1e/obelisk/internal/message"
)

func AppendMessage(filename string, msg message.Message) error {
	msgBin, err := message.Serialize(msg)
	if err != nil {
		return err
	}

	// Open in append mode (creates file if it doesn't exist)
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	// Write length first
	sizeBin := make([]byte, 4)
	binary.LittleEndian.PutUint32(sizeBin, uint32(len(msgBin)))

	if _, err := file.Write(sizeBin); err != nil {
		return err
	}

	// Then write message
	if _, err := file.Write(msgBin); err != nil {
		return err
	}

	return nil
}

func ReadAllMessages(filename string) ([]message.Message, error) {
	var messages []message.Message

	// Open file for reading
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	for {
		lengthBytes := make([]byte, 4)
		_, err := file.Read(lengthBytes)
		if err != nil {
			if err == io.EOF {
				break
			}
			return messages, err
		}

		messageLength := binary.LittleEndian.Uint32(lengthBytes)
		messageBytes := make([]byte, messageLength)

		_, err = file.Read(messageBytes)
		if err != nil {
			return messages, err
		}

		msg, err := message.Deserialize(messageBytes)
		if err != nil {
			return messages, err
		}

		messages = append(messages, msg)
	}

	return messages, nil
}
