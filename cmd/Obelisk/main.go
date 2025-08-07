package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"time"
)

type Message struct {
	Timestamp time.Time `json:"timestamp"`
	Key       string    `json:"key"`
	Value     string    `json:"value"`
}

// Json Serialize
func SerializeJSON(msg Message) ([]byte, error) {
	return json.Marshal(msg)
}

// Json Deserialize
func DeserializeJSON(data []byte) (Message, error) {
	var msg Message
	err := json.Unmarshal(data, &msg)
	return msg, err
}

// Binary Serialize
func SerializeBin(msg Message) ([]byte, error) {
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
func DeserializeBin(data []byte) (Message, error) {
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

func main() {
	msg := Message{
		Timestamp: time.Now(),
		Key:       "user123",
		Value:     "Hello, Obelisk!",
	}

	// Test JSON
	jsonData, _ := SerializeJSON(msg)
	msgFromJSON, _ := DeserializeJSON(jsonData)
	fmt.Printf("JSON size: %d bytes\n", len(jsonData))
	fmt.Printf("JSON works: %v\n", msg.Key == msgFromJSON.Key)

	// Test Binary
	binData, _ := SerializeBin(msg)
	msgFromBin, _ := DeserializeBin(binData)
	fmt.Printf("Binary size: %d bytes\n", len(binData))
	fmt.Printf("Binary works: %v\n", msg.Key == msgFromBin.Key)
}
