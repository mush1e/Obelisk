package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"time"
)

type Message struct {
	Timestamp time.Time `json:"timestamp"`
	Key       string    `json:"key"`
	Value     string    `json:"value"`
}

// Binary Serialize
func Serialize(msg Message) ([]byte, error) {
	var buf bytes.Buffer

	// Write timestamp as int64 (Unix nanoseconds)
	if err := binary.Write(&buf, binary.LittleEndian, msg.Timestamp.UnixNano()); err != nil {
		return nil, err
	}

	// Key
	keyBytes := []byte(msg.Key)
	keyLen := uint32(len(keyBytes))
	if err := binary.Write(&buf, binary.LittleEndian, keyLen); err != nil {
		return nil, err
	}
	if _, err := buf.Write(keyBytes); err != nil {
		return nil, err
	}

	// Value
	valBytes := []byte(msg.Value)
	valLen := uint32(len(valBytes))
	if err := binary.Write(&buf, binary.LittleEndian, valLen); err != nil {
		return nil, err
	}
	if _, err := buf.Write(valBytes); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// Binary Decerialize
func Deserialize(data []byte) (Message, error) {
	var msg Message
	buf := bytes.NewReader(data)

	// Read timestamp
	var ts int64
	if err := binary.Read(buf, binary.LittleEndian, &ts); err != nil {
		return msg, err
	}
	msg.Timestamp = time.Unix(0, ts)

	// Read Key
	var keyLen uint32
	if err := binary.Read(buf, binary.LittleEndian, &keyLen); err != nil {
		return msg, err
	}
	keyBytes := make([]byte, keyLen)
	if _, err := buf.Read(keyBytes); err != nil {
		return msg, err
	}
	msg.Key = string(keyBytes)

	// Read Value
	var valLen uint32
	if err := binary.Read(buf, binary.LittleEndian, &valLen); err != nil {
		return msg, err
	}
	valBytes := make([]byte, valLen)
	if _, err := buf.Read(valBytes); err != nil {
		return msg, err
	}
	msg.Value = string(valBytes)

	return msg, nil
}

func AppendMessage(filename string, msg Message) error {
	msgBin, err := Serialize(msg)
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

func ReadAllMessages(filename string) ([]Message, error) {
	var messages []Message

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

		msg, err := Deserialize(messageBytes)
		if err != nil {
			return messages, err
		}

		messages = append(messages, msg)
	}

	return messages, nil
}

func main() {
	msg := Message{
		Timestamp: time.Now(),
		Key:       "user123",
		Value:     "Hello, Obelisk!",
	}

	// Test Binary
	binData, _ := Serialize(msg)
	msgFromBin, _ := Deserialize(binData)
	fmt.Printf("Binary size: %d bytes\n", len(binData))
	fmt.Printf("Binary works: %v\n", msg.Key == msgFromBin.Key)

	// Test the file operations
	msg1 := Message{time.Now(), "user1", "hello"}
	msg2 := Message{time.Now(), "user2", "world"}

	// Write messages
	AppendMessage("test.log", msg1)
	AppendMessage("test.log", msg2)

	// Read them back
	messages, err := ReadAllMessages("test.log")
	if err != nil {
		fmt.Printf("Error: %v\n", err)
	} else {
		fmt.Printf("Read %d messages:\n", len(messages))
		for i, msg := range messages {
			fmt.Printf("  %d: %s -> %s\n", i, msg.Key, msg.Value)
		}
	}
}
