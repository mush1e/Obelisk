package message

// This package provides message types and serialization utilities for the Obelisk message broker.
// Messages are the fundamental data units that flow through the system, containing metadata
// like timestamps and topics, along with key-value payload data. The package implements
// efficient binary serialization for network transmission and storage persistence.
//
// The message system handles:
// - Structured message representation with timestamp, topic, key, and value fields
// - Binary serialization protocol for efficient network transmission
// - Deserialization with proper error handling and data validation
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
// The serialization format uses little-endian byte ordering for cross-platform compatibility
// and encodes variable-length strings with explicit length prefixes to support proper
// deserialization without delimiters.
//
// Binary format layout:
// 1. Timestamp: 8 bytes (int64, Unix nanoseconds, little-endian)
// 2. Topic length: 4 bytes (uint32, little-endian)
// 3. Topic data: Variable length bytes (UTF-8 encoded string)
// 4. Key length: 4 bytes (uint32, little-endian)
// 5. Key data: Variable length bytes (UTF-8 encoded string)
// 6. Value length: 4 bytes (uint32, little-endian)
// 7. Value data: Variable length bytes (UTF-8 encoded string)
//
// This format ensures efficient parsing during deserialization while maintaining
// compact representation for storage efficiency. The length-prefixed approach
// eliminates the need for escape characters or delimiters in the payload data.
//
// Parameters:
//   - msg: Message instance to serialize into binary format
//
// Returns:
//   - []byte: Binary representation of the message
//   - error: Any error that occurred during serialization
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
// This method reverses the serialization process, reading the binary format
// created by Serialize() and reconstructing the original message data with
// proper error handling for corrupted or truncated data.
//
// The deserialization process follows the exact binary format specification:
// 1. Reads 8-byte timestamp and converts from Unix nanoseconds to time.Time
// 2. Reads 4-byte topic length, then reads topic string data
// 3. Reads 4-byte key length, then reads key string data
// 4. Reads 4-byte value length, then reads value string data
//
// Error handling covers scenarios like:
// - Truncated data that doesn't contain all expected fields
// - Invalid length fields that exceed remaining data
// - I/O errors during binary reading operations
//
// The method uses a bytes.Reader for efficient sequential reading and
// binary.Read for automatic endianness handling during numeric field parsing.
//
// Parameters:
//   - data: Binary byte slice containing serialized message data
//
// Returns:
//   - Message: Reconstructed message with all fields populated
//   - error: Any error that occurred during deserialization
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
