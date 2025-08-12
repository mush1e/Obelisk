package message

// Message types and binary serialization utilities for the Obelisk message broker.
// - Cross-platform compatibility using little-endian byte ordering
// - Variable-length string encoding with explicit length prefixes
// - Nanosecond precision timestamps for high-resolution message ordering

import (
	"bytes"
	"encoding/binary"
	"time"
)

// Message represents a single message in the Obelisk message broker system.
// Each message contains both metadata (timestamp, topic) and payload data (key, value)
// necessary for routing, ordering, and processing within the broker infrastructure.
//
// Fields:
// - Timestamp: When the message was created, used for ordering and TTL operations
// - Topic: Routing destination that determines which consumers receive the message
// - Key: Optional identifier for message deduplication and partitioning logic
// - Value: The actual payload data that consumers will process
//
// Messages are immutable once created and are designed to be serialized for
// network transmission or storage in log files. The JSON tags enable REST API
// integration while the binary serialization provides efficient storage format.
type Message struct {
	Timestamp time.Time `json:"timestamp"` // Creation time with nanosecond precision
	Topic     string    `json:"topic"`     // Routing destination for message delivery
	Key       string    `json:"key"`       // Optional identifier for deduplication/partitioning
	Value     string    `json:"value"`     // Actual message payload data
}

// Serialize converts a message into a binary byte slice for storage or network transmission.
func Serialize(msg Message) ([]byte, error) {
	var buf bytes.Buffer

	// Write timestamp as int64 Unix nanoseconds for high precision
	// Nanosecond precision enables proper message ordering even with high throughput
	if err := binary.Write(&buf, binary.LittleEndian, msg.Timestamp.UnixNano()); err != nil {
		return nil, err
	}

	// Write topic with length prefix for variable-length string encoding
	topicBytes := []byte(msg.Topic)
	topicLen := uint32(len(topicBytes))
	if err := binary.Write(&buf, binary.LittleEndian, topicLen); err != nil {
		return nil, err
	}
	if err := binary.Write(&buf, binary.LittleEndian, topicBytes); err != nil {
		return nil, err
	}

	// Write key with length prefix for variable-length string encoding
	keyBytes := []byte(msg.Key)
	keyLen := uint32(len(keyBytes))
	if err := binary.Write(&buf, binary.LittleEndian, keyLen); err != nil {
		return nil, err
	}
	if _, err := buf.Write(keyBytes); err != nil {
		return nil, err
	}

	// Write value with length prefix for variable-length string encoding
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

// Deserialize converts a binary byte slice back into a Message struct.
func Deserialize(data []byte) (Message, error) {
	var msg Message
	buf := bytes.NewReader(data)

	// Read timestamp from first 8 bytes and convert to time.Time
	// Unix nanoseconds provide precise message ordering capabilities
	var ts int64
	if err := binary.Read(buf, binary.LittleEndian, &ts); err != nil {
		return msg, err
	}
	msg.Timestamp = time.Unix(0, ts)

	// Read topic using length-prefixed variable-length string format
	var TopicLen uint32
	if err := binary.Read(buf, binary.LittleEndian, &TopicLen); err != nil {
		return msg, err
	}
	topicBytes := make([]byte, TopicLen)
	if err := binary.Read(buf, binary.LittleEndian, &topicBytes); err != nil {
		return msg, err
	}
	msg.Topic = string(topicBytes)

	// Read key using length-prefixed variable-length string format
	var keyLen uint32
	if err := binary.Read(buf, binary.LittleEndian, &keyLen); err != nil {
		return msg, err
	}
	keyBytes := make([]byte, keyLen)
	if _, err := buf.Read(keyBytes); err != nil {
		return msg, err
	}
	msg.Key = string(keyBytes)

	// Read value using length-prefixed variable-length string format
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
