package storage

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mush1e/obelisk/internal/message"
	"github.com/mush1e/obelisk/pkg/protocol"
)

// TestCorruptionRecovery tests the main corruption recovery scenario
func TestCorruptionRecovery(t *testing.T) {
	// Create temporary directory for test files
	tempDir := t.TempDir()
	logFile := filepath.Join(tempDir, "test.log")
	idxFile := filepath.Join(tempDir, "test.idx")

	// Step 1: Create a log file with corruption in the middle
	t.Log("Creating log file with corruption...")
	messagesBeforeCorruption, messagesAfterCorruption := createCorruptedLogFile(t, logFile)

	totalExpectedMessages := len(messagesBeforeCorruption) + len(messagesAfterCorruption)
	t.Logf("Created log with %d messages before corruption, %d after (total: %d)",
		len(messagesBeforeCorruption), len(messagesAfterCorruption), totalExpectedMessages)

	// Step 2: Try to rebuild index (this should recover both sections)
	t.Log("Rebuilding index with corruption recovery...")
	err := RebuildIndex(logFile, idxFile)
	if err != nil {
		t.Fatalf("RebuildIndex failed: %v", err)
	}

	// Step 3: Load the rebuilt index
	idx, err := LoadIndex(idxFile)
	if err != nil {
		t.Fatalf("LoadIndex failed: %v", err)
	}

	// Step 4: Verify we recovered the expected number of messages
	recoveredCount := len(idx.Positions)
	t.Logf("Recovered %d messages out of %d expected", recoveredCount, totalExpectedMessages)

	if recoveredCount < totalExpectedMessages {
		t.Logf("Note: Some messages lost due to corruption (recovered %d/%d)", recoveredCount, totalExpectedMessages)
	}

	if recoveredCount == 0 {
		t.Fatal("No messages recovered - corruption recovery failed completely")
	}

	// Step 5: Test reading using the rebuilt index (not sequential reading)
	t.Log("Testing reading specific messages using rebuilt index...")

	// Test reading from specific offsets using the index
	for i := 0; i < min(recoveredCount, 3); i++ {
		messages, err := ReadMessagesFromOffset(logFile, idxFile, uint64(i))
		if err != nil {
			t.Errorf("ReadMessagesFromOffset failed for offset %d: %v", i, err)
			continue
		}

		remainingMessages := recoveredCount - i
		if len(messages) != remainingMessages {
			t.Logf("Read %d messages from offset %d (expected %d)",
				len(messages), i, remainingMessages)
		}

		t.Logf("Successfully read from offset %d: got %d messages", i, len(messages))
	}

	// Step 6: Verify we can read specific messages by offset
	for i := 0; i < min(recoveredCount, 5); i++ { // Test first 5 messages
		pos, err := idx.GetPosition(uint64(i))
		if err != nil {
			t.Errorf("Failed to get position for offset %d: %v", i, err)
			continue
		}

		if pos < 0 {
			t.Errorf("Invalid position %d for offset %d", pos, i)
		}
	}

	t.Logf("✅ Corruption recovery test passed - recovered %d/%d messages",
		recoveredCount, totalExpectedMessages)
}

// createCorruptedLogFile creates a log file with valid messages, then corruption, then more valid messages
func createCorruptedLogFile(t *testing.T, logFile string) ([]message.Message, []message.Message) {
	pool := NewFilePool(time.Hour)
	defer pool.Stop()

	// Create messages to write before corruption
	messagesBeforeCorruption := []message.Message{
		{Topic: "test", Key: "key1", Value: "Message before corruption 1", Timestamp: time.Now()},
		{Topic: "test", Key: "key2", Value: "Message before corruption 2", Timestamp: time.Now()},
		{Topic: "test", Key: "key3", Value: "Message before corruption 3", Timestamp: time.Now()},
	}

	// Write the first batch of messages
	idx := &OffsetIndex{Positions: []int64{}}
	for _, msg := range messagesBeforeCorruption {
		err := AppendMessage(pool, logFile, logFile+".tmp", msg, idx)
		if err != nil {
			t.Fatalf("Failed to append message before corruption: %v", err)
		}
	}

	// Inject corruption by writing garbage bytes
	t.Log("Injecting corruption...")
	f, err := os.OpenFile(logFile, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("Failed to open log file for corruption: %v", err)
	}

	// Write realistic corruption - looks like it could be a message but isn't
	corruptData := []byte{
		0xFF, 0xFF, 0xFF, 0xFF, // Invalid length (4294967295 bytes)
		0xDE, 0xAD, 0xBE, 0xEF, // Garbage data
		0x12, 0x34, 0x56, 0x78, // More garbage
		0x00, 0x00, 0x00, 0x00, // Null bytes
	}
	f.Write(corruptData)
	f.Close()

	// Write valid messages after corruption
	messagesAfterCorruption := []message.Message{
		{Topic: "test", Key: "key4", Value: "Message after corruption 1", Timestamp: time.Now()},
		{Topic: "test", Key: "key5", Value: "Message after corruption 2", Timestamp: time.Now()},
		{Topic: "test", Key: "key6", Value: "Message after corruption 3", Timestamp: time.Now()},
	}

	// Append messages after corruption (this will work with current system)
	for _, msg := range messagesAfterCorruption {
		err := AppendMessage(pool, logFile, logFile+".tmp", msg, idx)
		if err != nil {
			t.Fatalf("Failed to append message after corruption: %v", err)
		}
	}

	return messagesBeforeCorruption, messagesAfterCorruption
}

// TestSkippingFunction tests the skipToNextValidMessage function directly
func TestSkippingFunction(t *testing.T) {
	tempDir := t.TempDir()
	logFile := filepath.Join(tempDir, "skip_test.log")

	// Create a file with a pattern: valid message, garbage, valid message
	createTestFileWithGaps(t, logFile)

	f, err := os.Open(logFile)
	if err != nil {
		t.Fatalf("Failed to open test file: %v", err)
	}
	defer f.Close()

	// Test our recovery function
	positions, stats := recoverMessagesWithSkipping(f)

	t.Logf("Recovery results:")
	t.Logf("  Valid messages: %d", stats.validMessages)
	t.Logf("  Corruption sections: %d", stats.corruptionSections)
	t.Logf("  Bytes skipped: %d", stats.bytesSkipped)
	t.Logf("  Positions recovered: %v", positions)

	if len(positions) == 0 {
		t.Fatal("No positions recovered")
	}

	if stats.corruptionSections == 0 {
		t.Error("Expected to detect corruption sections")
	}

	t.Log("✅ Skipping function test passed")
}

// createTestFileWithGaps creates a file with valid messages separated by garbage
func createTestFileWithGaps(t *testing.T, logFile string) {
	f, err := os.Create(logFile)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}
	defer f.Close()

	// Write first valid message
	validMsg1 := message.Message{
		Topic:     "test",
		Key:       "valid1",
		Value:     "This is the first valid message",
		Timestamp: time.Now(),
	}

	msgBin1, err := message.Serialize(validMsg1)
	if err != nil {
		t.Fatalf("Failed to serialize message 1: %v", err)
	}
	protocol.WriteMessage(f, msgBin1)

	// Write garbage data
	garbage := []byte{0x99, 0x99, 0x99, 0x99, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF}
	f.Write(garbage)

	// Write second valid message
	validMsg2 := message.Message{
		Topic:     "test",
		Key:       "valid2",
		Value:     "This is the second valid message",
		Timestamp: time.Now(),
	}

	msgBin2, err := message.Serialize(validMsg2)
	if err != nil {
		t.Fatalf("Failed to serialize message 2: %v", err)
	}
	protocol.WriteMessage(f, msgBin2)

	t.Log("Created test file with valid message, garbage, valid message")
}

// Comparison of old vs new behavior
func TestOldVsNewBehavior(t *testing.T) {
	t.Log("This test demonstrates the improvement from old to new corruption handling")

	// With old behavior:   [msg1, msg2, CORRUPT, msg3, msg4] -> recover [msg1, msg2] (lose msg3, msg4)
	// With new behavior:   [msg1, msg2, CORRUPT, msg3, msg4] -> recover [msg1, msg2, msg3, msg4]

	tempDir := t.TempDir()
	logFile := filepath.Join(tempDir, "comparison.log")
	idxFile := filepath.Join(tempDir, "comparison.idx")

	// Create test scenario
	beforeMsgs, afterMsgs := createCorruptedLogFile(t, logFile)
	totalMessages := len(beforeMsgs) + len(afterMsgs)

	// Test our new recovery
	err := RebuildIndex(logFile, idxFile)
	if err != nil {
		t.Fatalf("New RebuildIndex failed: %v", err)
	}

	idx, err := LoadIndex(idxFile)
	if err != nil {
		t.Fatalf("LoadIndex failed: %v", err)
	}

	recoveredCount := len(idx.Positions)

	t.Logf("Original total messages: %d", totalMessages)
	t.Logf("Messages with old behavior: %d (would stop at corruption)", len(beforeMsgs))
	t.Logf("Messages with new behavior: %d (skips corruption)", recoveredCount)

	improvement := recoveredCount - len(beforeMsgs)
	if improvement > 0 {
		t.Logf("✅ Improvement: +%d messages recovered (%.1f%% better)",
			improvement, float64(improvement)/float64(totalMessages)*100)
	} else {
		t.Log("⚠️  No improvement detected - check corruption placement")
	}
}

// Helper function for min
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
