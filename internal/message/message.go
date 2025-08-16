package message

// Message types and binary serialization utilities for the Obelisk message broker.
// - Cross-platform compatibility using little-endian byte ordering
// - Variable-length string encoding with explicit length prefixes
// - Nanosecond precision timestamps for high-resolution message ordering

import (
	"bytes"
	"encoding/binary"
	"time"

	obeliskErrors "github.com/mush1e/obelisk/internal/errors"
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

	if err := binary.Write(&buf, binary.LittleEndian, msg.Timestamp.UnixNano()); err != nil {
		return nil, obeliskErrors.NewPermanentError("serialize_timestamp", "failed to serialize timestamp", err)
	}

	// Write topic with validation
	if len(msg.Topic) > 255 {
		return nil, obeliskErrors.NewPermanentError("serialize_topic", "topic name too long (max 255)", nil)
	}

	topicBytes := []byte(msg.Topic)
	topicLen := uint32(len(topicBytes))
	if err := binary.Write(&buf, binary.LittleEndian, topicLen); err != nil {
		return nil, obeliskErrors.NewPermanentError("serialize_topic_length", "failed to serialize topic length", err)
	}
	if err := binary.Write(&buf, binary.LittleEndian, topicBytes); err != nil {
		return nil, obeliskErrors.NewPermanentError("serialize_topic_data", "failed to serialize topic data", err)
	}

	// Similar for key and value...
	keyBytes := []byte(msg.Key)
	keyLen := uint32(len(keyBytes))
	if err := binary.Write(&buf, binary.LittleEndian, keyLen); err != nil {
		return nil, obeliskErrors.NewPermanentError("serialize_key_length", "failed to serialize key length", err)
	}
	if err := binary.Write(&buf, binary.LittleEndian, keyBytes); err != nil {
		return nil, obeliskErrors.NewPermanentError("serialize_key_data", "failed to serialize key data", err)
	}

	valBytes := []byte(msg.Value)
	valLen := uint32(len(valBytes))
	if err := binary.Write(&buf, binary.LittleEndian, valLen); err != nil {
		return nil, obeliskErrors.NewPermanentError("serialize_value_length", "failed to serialize value length", err)
	}
	if _, err := buf.Write(valBytes); err != nil {
		return nil, obeliskErrors.NewPermanentError("serialize_value_data", "failed to serialize value data", err)
	}

	return buf.Bytes(), nil
}

// Deserialize converts a binary byte slice back into a Message struct.
func Deserialize(data []byte) (Message, error) {
	var msg Message
	buf := bytes.NewReader(data)

	var ts int64
	if err := binary.Read(buf, binary.LittleEndian, &ts); err != nil {
		return msg, obeliskErrors.NewDataError("deserialize_timestamp", "failed to deserialize timestamp", err)
	}
	msg.Timestamp = time.Unix(0, ts)

	// Read topic
	var topicLen uint32
	if err := binary.Read(buf, binary.LittleEndian, &topicLen); err != nil {
		return msg, obeliskErrors.NewDataError("deserialize_topic_length", "failed to deserialize topic length", err)
	}
	if topicLen > 255 {
		return msg, obeliskErrors.NewDataError("deserialize_topic", "invalid topic length", nil)
	}
	topicBytes := make([]byte, topicLen)
	if err := binary.Read(buf, binary.LittleEndian, &topicBytes); err != nil {
		return msg, obeliskErrors.NewDataError("deserialize_topic_data", "failed to deserialize topic data", err)
	}
	msg.Topic = string(topicBytes)

	// Similar for key and value...
	var keyLen uint32
	if err := binary.Read(buf, binary.LittleEndian, &keyLen); err != nil {
		return msg, obeliskErrors.NewDataError("deserialize_key_length", "failed to deserialize key length", err)
	}
	keyBytes := make([]byte, keyLen)
	if _, err := buf.Read(keyBytes); err != nil {
		return msg, obeliskErrors.NewDataError("deserialize_key_data", "failed to deserialize key data", err)
	}
	msg.Key = string(keyBytes)

	var valLen uint32
	if err := binary.Read(buf, binary.LittleEndian, &valLen); err != nil {
		return msg, obeliskErrors.NewDataError("deserialize_value_length", "failed to deserialize value length", err)
	}
	valBytes := make([]byte, valLen)
	if _, err := buf.Read(valBytes); err != nil {
		return msg, obeliskErrors.NewDataError("deserialize_value_data", "failed to deserialize value data", err)
	}
	msg.Value = string(valBytes)

	return msg, nil
}
