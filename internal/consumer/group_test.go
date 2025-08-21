package consumer

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestBasicConsumerGroup(t *testing.T) {
	tempDir := t.TempDir()

	// Create mock partition files for the "orders" topic
	createMockPartitions(t, tempDir, "orders", 4)

	// Create group manager
	mgr := NewConsumerGroupManager(tempDir)

	// Create a group
	group, err := mgr.CreateGroup("test-group", []string{"orders"})
	if err != nil {
		t.Fatalf("Failed to create group: %v", err)
	}

	// Create some fake consumers
	consumer1 := NewConsumer(tempDir, "consumer1", "orders")
	consumer2 := NewConsumer(tempDir, "consumer2", "orders")

	// Join the group
	if err := group.JoinGroup("member1", consumer1); err != nil {
		t.Fatalf("Failed to join group: %v", err)
	}

	if err := group.JoinGroup("member2", consumer2); err != nil {
		t.Fatalf("Failed to join group: %v", err)
	}

	// Check assignments
	partitions1 := group.GetAssignedPartitions("member1")
	partitions2 := group.GetAssignedPartitions("member2")

	t.Logf("Member1 partitions: %v", partitions1)
	t.Logf("Member2 partitions: %v", partitions2)

	// Both should have some partitions
	if len(partitions1) == 0 {
		t.Error("Member1 should have partitions")
	}
	if len(partitions2) == 0 {
		t.Error("Member2 should have partitions")
	}

	// Total partitions should be 4 (assuming 4 partitions per topic)
	totalPartitions := len(partitions1) + len(partitions2)
	if totalPartitions != 4 {
		t.Errorf("Expected 4 total partitions, got %d", totalPartitions)
	}
}

func TestGroupRebalance(t *testing.T) {
	tempDir := t.TempDir()

	// Create mock partition files for the "orders" topic
	createMockPartitions(t, tempDir, "orders", 4)

	mgr := NewConsumerGroupManager(tempDir)

	group, err := mgr.CreateGroup("rebalance-test", []string{"orders"})
	if err != nil {
		t.Fatalf("Failed to create group: %v", err)
	}

	// Add first member
	consumer1 := NewConsumer(tempDir, "consumer1", "orders")
	group.JoinGroup("member1", consumer1)

	partitions1 := group.GetAssignedPartitions("member1")
	if len(partitions1) != 4 {
		t.Errorf("Single member should get all 4 partitions, got %d", len(partitions1))
	}

	// Add second member - should trigger rebalance
	consumer2 := NewConsumer(tempDir, "consumer2", "orders")
	group.JoinGroup("member2", consumer2)

	partitions1After := group.GetAssignedPartitions("member1")
	partitions2After := group.GetAssignedPartitions("member2")

	t.Logf("After rebalance - Member1: %v, Member2: %v", partitions1After, partitions2After)

	// Should be roughly equal
	if len(partitions1After)+len(partitions2After) != 4 {
		t.Error("Total partitions should still be 4 after rebalance")
	}
}

// createMockPartitions creates mock partition files for testing
func createMockPartitions(t *testing.T, baseDir, topic string, partitionCount int) {
	// The path resolution is:
	// 1. Test passes tempDir to NewConsumerGroupManager
	// 2. Manager creates its baseDir as tempDir/../groups
	// 3. Groups look for topics in manager.baseDir/../topics = tempDir/../groups/../topics = tempDir/../topics
	// So we need to create topics in tempDir/../topics
	parentDir := filepath.Dir(baseDir)
	topicsDir := filepath.Join(parentDir, "topics")
	topicDir := filepath.Join(topicsDir, topic)

	if err := os.MkdirAll(topicDir, 0755); err != nil {
		t.Fatalf("Failed to create topic directory: %v", err)
	}

	// Create partition files
	for i := 0; i < partitionCount; i++ {
		logFile := filepath.Join(topicDir, fmt.Sprintf("partition-%d.log", i))
		idxFile := filepath.Join(topicDir, fmt.Sprintf("partition-%d.idx", i))

		// Create empty log file
		if f, err := os.Create(logFile); err != nil {
			t.Fatalf("Failed to create partition log file: %v", err)
		} else {
			f.Close()
		}

		// Create empty index file
		if f, err := os.Create(idxFile); err != nil {
			t.Fatalf("Failed to create partition index file: %v", err)
		} else {
			f.Close()
		}
	}
}
