package message

import (
	"testing"
	"time"
)

func TestSerializeDeserialize(t *testing.T) {
	original := Message{
		Timestamp: time.Now(),
		Key:       "user123",
		Value:     "Hello, Obelisk!",
	}

	data, err := Serialize(original)
	if err != nil {
		t.Fatalf("Serialize failed : %v", err)
	}

	result, err := Deserialize(data)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	if result.Key != original.Key || result.Value != original.Value {
		t.Errorf("Mismatch after roundtrip: got %+v, want %+v", result, original)
	}
}
