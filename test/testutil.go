package testutil

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mush1e/obelisk/internal/batch"
	"github.com/mush1e/obelisk/internal/buffer"
	"github.com/mush1e/obelisk/internal/health"
	"github.com/mush1e/obelisk/internal/message"
	"github.com/mush1e/obelisk/internal/storage"
)

type TestHelper struct {
	tempDir string
	pool    *storage.FilePool
	t       *testing.T
}

func NewTestHelper(t *testing.T) *TestHelper {
	tempDir, err := os.MkdirTemp("", "obelisk-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	pool := storage.NewFilePool(time.Hour)

	return &TestHelper{
		tempDir: tempDir,
		pool:    pool,
		t:       t,
	}
}

func (h *TestHelper) Cleanup() {
	if err := h.pool.Stop(); err != nil {
		h.t.Errorf("Failed to stop pool: %v", err)
	}
	if err := os.RemoveAll(h.tempDir); err != nil {
		h.t.Errorf("Failed to remove temp dir: %v", err)
	}
}

func (h *TestHelper) TempDir() string {
	return h.tempDir
}

func (h *TestHelper) Pool() *storage.FilePool {
	return h.pool
}

func (h *TestHelper) NewBatcher() *batch.TopicBatcher {
	return batch.NewTopicBatcher(
		h.tempDir,
		5,                    // Small batch size for testing
		100*time.Millisecond, // Fast flush for testing
		h.pool,
		health.NewHealthTracker(),
	)
}

func (h *TestHelper) NewBuffer() *buffer.TopicBuffers {
	return buffer.NewTopicBuffers(10) // Small capacity for testing
}

func (h *TestHelper) CreateTestMessage(topic, key, value string) message.Message {
	return message.Message{
		Timestamp: time.Now(),
		Topic:     topic,
		Key:       key,
		Value:     value,
	}
}

func (h *TestHelper) LogFile(topic string) string {
	return filepath.Join(h.tempDir, topic+".log")
}

func (h *TestHelper) IdxFile(topic string) string {
	return filepath.Join(h.tempDir, topic+".idx")
}

func (h *TestHelper) WaitForFlush() {
	time.Sleep(150 * time.Millisecond) // Slightly longer than flush interval
}

func (h *TestHelper) AssertFileExists(path string) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		h.t.Errorf("Expected file to exist: %s", path)
	}
}

func (h *TestHelper) AssertMessageCount(topic string, expected int) {
	messages, err := storage.ReadAllMessages(h.LogFile(topic))
	if err != nil {
		h.t.Errorf("Failed to read messages: %v", err)
		return
	}
	if len(messages) != expected {
		h.t.Errorf("Expected %d messages, got %d", expected, len(messages))
	}
}
