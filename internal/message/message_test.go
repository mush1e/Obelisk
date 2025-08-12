package message

// Unit tests for message serialization and deserialization functionality.

import (
	"testing"
	"time"
)

// TestSerializeDeserialize validates round-trip serialization integrity.
//
// The test verifies:
// 1. Successful serialization of a complete message to binary format
// 2. Successful deserialization of binary data back to message struct
// 3. Preservation of all message fields (timestamp, topic, key, value)
// 4. Timestamp precision maintenance through the binary encoding process
// 5. String field integrity including special characters and encoding
//
// This round-trip test is essential for validating the binary protocol used
// for message persistence in log files and network transmission between
// broker components.
func TestSerializeDeserialize(t *testing.T) {
	// Create a test message with representative data including timestamp
	original := Message{
		Timestamp: time.Now(),
		Topic:     "test-topic",
		Key:       "user123",
		Value:     "Hello, Obelisk!",
	}

	// Serialize the message to binary format
	data, err := Serialize(original)
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	// Verify that serialization produces non-empty output
	if len(data) == 0 {
		t.Error("Serialize produced empty data")
	}

	// Deserialize the binary data back to a message struct
	result, err := Deserialize(data)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	// Validate that all string fields are preserved exactly
	if result.Key != original.Key {
		t.Errorf("Key mismatch after roundtrip: got %q, want %q", result.Key, original.Key)
	}
	if result.Value != original.Value {
		t.Errorf("Value mismatch after roundtrip: got %q, want %q", result.Value, original.Value)
	}
	if result.Topic != original.Topic {
		t.Errorf("Topic mismatch after roundtrip: got %q, want %q", result.Topic, original.Topic)
	}

	// Validate timestamp precision - should be preserved to nanosecond level
	if !result.Timestamp.Equal(original.Timestamp) {
		t.Errorf("Timestamp mismatch after roundtrip: got %v, want %v", result.Timestamp, original.Timestamp)
	}

	// Additional validation: ensure the timestamps are exactly equal in nanoseconds
	if result.Timestamp.UnixNano() != original.Timestamp.UnixNano() {
		t.Errorf("Timestamp nanoseconds mismatch: got %d, want %d",
			result.Timestamp.UnixNano(), original.Timestamp.UnixNano())
	}
}

// TestSerializeEmptyFields validates that messages with empty string fields
// are correctly serialized and deserialized, ensuring the binary protocol
// handles edge cases gracefully.
func TestSerializeEmptyFields(t *testing.T) {
	// Create message with empty fields to test edge case handling
	original := Message{
		Timestamp: time.Unix(1640995200, 123456789), // Fixed timestamp for reproducible tests
		Topic:     "",                               // Empty topic
		Key:       "",                               // Empty key
		Value:     "",                               // Empty value
	}

	// Test serialization of empty fields
	data, err := Serialize(original)
	if err != nil {
		t.Fatalf("Serialize with empty fields failed: %v", err)
	}

	// Verify serialization produces output even with empty fields
	if len(data) == 0 {
		t.Error("Serialize with empty fields produced no data")
	}

	// Test deserialization of empty fields
	result, err := Deserialize(data)
	if err != nil {
		t.Fatalf("Deserialize with empty fields failed: %v", err)
	}

	// Validate empty fields are preserved
	if result.Topic != "" {
		t.Errorf("Expected empty topic, got %q", result.Topic)
	}
	if result.Key != "" {
		t.Errorf("Expected empty key, got %q", result.Key)
	}
	if result.Value != "" {
		t.Errorf("Expected empty value, got %q", result.Value)
	}
	if !result.Timestamp.Equal(original.Timestamp) {
		t.Errorf("Timestamp mismatch with empty fields: got %v, want %v", result.Timestamp, original.Timestamp)
	}
}

// TestDeserializeInvalidData validates error handling when attempting to
// deserialize corrupted or incomplete binary data.
func TestDeserializeInvalidData(t *testing.T) {
	testCases := []struct {
		name string
		data []byte
	}{
		{"empty data", []byte{}},
		{"truncated timestamp", []byte{1, 2, 3, 4}},
		{"incomplete header", []byte{1, 2, 3, 4, 5, 6, 7, 8, 1, 2}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Attempt to deserialize invalid data
			_, err := Deserialize(tc.data)

			// Verify that deserialization properly fails with an error
			if err == nil {
				t.Errorf("Expected error when deserializing %s, but got none", tc.name)
			}
		})
	}
}

// TestSerializeLargeMessage validates that the binary protocol can handle
// messages with large string fields without corruption or performance issues.
func TestSerializeLargeMessage(t *testing.T) {
	// Create a large string for testing protocol limits
	largeValue := string(make([]byte, 10000)) // 10KB of null bytes
	for i := range largeValue {
		largeValue = largeValue[:i] + "A" + largeValue[i+1:]
	}

	original := Message{
		Timestamp: time.Now(),
		Topic:     "large-message-topic",
		Key:       "large-key",
		Value:     largeValue,
	}

	// Test serialization of large message
	data, err := Serialize(original)
	if err != nil {
		t.Fatalf("Serialize large message failed: %v", err)
	}

	// Verify serialization produces appropriately sized output
	if len(data) < len(largeValue) {
		t.Error("Serialized data appears smaller than input")
	}

	// Test deserialization of large message
	result, err := Deserialize(data)
	if err != nil {
		t.Fatalf("Deserialize large message failed: %v", err)
	}

	// Validate large value is preserved exactly
	if result.Value != original.Value {
		t.Error("Large value was corrupted during serialization roundtrip")
	}
	if len(result.Value) != len(original.Value) {
		t.Errorf("Large value length mismatch: got %d, want %d", len(result.Value), len(original.Value))
	}
}
