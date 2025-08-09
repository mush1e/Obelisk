package message

// This package provides a message type for Obelisk.
// It is used to store messages that are received from the network.
// The message type is a struct that contains a timestamp, a key, and a value.
// The message type also provides methods for serializing and deserializing messages.

import (
	"bytes"
	"encoding/binary"
	"time"
)

// Message represents a message that is received from the network.
// It contains a timestamp, a key, and a value.
type Message struct {
	Timestamp time.Time `json:"timestamp"`
	Topic     string    `json:"topic"`
	Key       string    `json:"key"`
	Value     string    `json:"value"`
}

// Serialize serializes a message into a byte slice.
func Serialize(msg Message) ([]byte, error) {
	var buf bytes.Buffer

	// Write timestamp as int64 (Unix nanoseconds)
	if err := binary.Write(&buf, binary.LittleEndian, msg.Timestamp.UnixNano()); err != nil {
		return nil, err
	}
	// Topic
	topicBytes := []byte(msg.Topic)
	topicLen := uint32(len(topicBytes))
	if err := binary.Write(&buf, binary.LittleEndian, topicLen); err != nil {
		return nil, err
	}
	if err := binary.Write(&buf, binary.LittleEndian, topicBytes); err != nil {
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

// Deserialize converts a byte slice into a message.
func Deserialize(data []byte) (Message, error) {
	var msg Message
	buf := bytes.NewReader(data)

	// Read Timestamp
	var ts int64
	if err := binary.Read(buf, binary.LittleEndian, &ts); err != nil {
		return msg, err
	}
	msg.Timestamp = time.Unix(0, ts)

	// Read Topic
	var TopicLen uint32
	if err := binary.Read(buf, binary.LittleEndian, &TopicLen); err != nil {
		return msg, err
	}
	topicBytes := make([]byte, TopicLen)
	if err := binary.Read(buf, binary.LittleEndian, &topicBytes); err != nil {
		return msg, err
	}
	msg.Topic = string(topicBytes)

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
