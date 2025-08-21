package consumer

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ConsumerGroup manages multiple consumers working together
type ConsumerGroup struct {
	mtx     sync.RWMutex
	ID      string             `json:"id"`
	Topics  []string           `json:"topics"`
	Members map[string]*Member `json:"members"`

	// Partition assignments: member_id -> list of partitions
	Assignments map[string][]int `json:"assignments"`

	// Rebalance state
	Generation    int64     `json:"generation"`
	LastRebalance time.Time `json:"last_rebalance"`

	baseDir string
}

// Member represents a consumer in the group
type Member struct {
	ID            string    `json:"id"`
	JoinedAt      time.Time `json:"joined_at"`
	LastHeartbeat time.Time `json:"last_heartbeat"`
	Partitions    []int     `json:"partitions"`

	// The actual consumer instance (not serialized)
	consumer *Consumer `json:"-"`
}

// ConsumerGroupManager manages all consumer groups
type ConsumerGroupManager struct {
	mtx     sync.RWMutex
	groups  map[string]*ConsumerGroup
	baseDir string
}

// NewConsumerGroupManager creates a new group manager
func NewConsumerGroupManager(baseDir string) *ConsumerGroupManager {
	groupsDir := filepath.Join(baseDir, "../groups")
	os.MkdirAll(groupsDir, 0755)

	return &ConsumerGroupManager{
		groups:  make(map[string]*ConsumerGroup),
		baseDir: groupsDir,
	}
}

// CreateGroup creates a new consumer group
func (mgr *ConsumerGroupManager) CreateGroup(groupID string, topics []string) (*ConsumerGroup, error) {
	mgr.mtx.Lock()
	defer mgr.mtx.Unlock()

	if _, exists := mgr.groups[groupID]; exists {
		return nil, fmt.Errorf("group %s already exists", groupID)
	}

	group := &ConsumerGroup{
		ID:            groupID,
		Topics:        topics,
		Members:       make(map[string]*Member),
		Assignments:   make(map[string][]int),
		Generation:    1,
		LastRebalance: time.Now(),
		baseDir:       mgr.baseDir,
	}

	mgr.groups[groupID] = group

	// Save to disk
	if err := group.saveToFile(); err != nil {
		delete(mgr.groups, groupID)
		return nil, fmt.Errorf("failed to save group: %v", err)
	}

	fmt.Printf("Created consumer group: %s for topics: %v\n", groupID, topics)
	return group, nil
}

// GetGroup retrieves an existing group
func (mgr *ConsumerGroupManager) GetGroup(groupID string) (*ConsumerGroup, error) {
	mgr.mtx.RLock()
	defer mgr.mtx.RUnlock()

	if group, exists := mgr.groups[groupID]; exists {
		return group, nil
	}

	return nil, fmt.Errorf("group %s not found", groupID)
}

// JoinGroup adds a consumer to the group
func (group *ConsumerGroup) JoinGroup(memberID string, consumer *Consumer) error {
	group.mtx.Lock()
	defer group.mtx.Unlock()

	if _, exists := group.Members[memberID]; exists {
		return fmt.Errorf("member %s already in group", memberID)
	}

	member := &Member{
		ID:            memberID,
		JoinedAt:      time.Now(),
		LastHeartbeat: time.Now(),
		consumer:      consumer,
	}

	group.Members[memberID] = member
	group.Generation++

	fmt.Printf("Member %s joined group %s (generation %d)\n",
		memberID, group.ID, group.Generation)

	// Trigger rebalance
	if err := group.rebalance(); err != nil {
		delete(group.Members, memberID)
		return fmt.Errorf("rebalance failed: %v", err)
	}

	return group.saveToFile()
}

// LeaveGroup removes a consumer from the group
func (group *ConsumerGroup) LeaveGroup(memberID string) error {
	group.mtx.Lock()
	defer group.mtx.Unlock()

	if _, exists := group.Members[memberID]; !exists {
		return fmt.Errorf("member %s not in group", memberID)
	}

	delete(group.Members, memberID)
	delete(group.Assignments, memberID)
	group.Generation++

	fmt.Printf("Member %s left group %s (generation %d)\n",
		memberID, group.ID, group.Generation)

	// Rebalance remaining members
	if len(group.Members) > 0 {
		if err := group.rebalance(); err != nil {
			return fmt.Errorf("rebalance failed: %v", err)
		}
	}

	return group.saveToFile()
}

// rebalance redistributes partitions among group members
func (group *ConsumerGroup) rebalance() error {
	// Clear existing assignments
	group.Assignments = make(map[string][]int)

	if len(group.Members) == 0 {
		return nil
	}

	// Get all partitions for all topics
	allPartitions := group.getAllPartitions()
	if len(allPartitions) == 0 {
		fmt.Printf("No partitions found for topics: %v\n", group.Topics)
		return nil
	}

	// Get member IDs
	memberIDs := make([]string, 0, len(group.Members))
	for memberID := range group.Members {
		memberIDs = append(memberIDs, memberID)
	}

	// Round-robin assignment (SIMPLE!)
	for i, partition := range allPartitions {
		memberIndex := i % len(memberIDs)
		memberID := memberIDs[memberIndex]
		group.Assignments[memberID] = append(group.Assignments[memberID], partition)
	}

	// Update member partition lists
	for memberID, partitions := range group.Assignments {
		if member, exists := group.Members[memberID]; exists {
			member.Partitions = partitions
		}
	}

	group.LastRebalance = time.Now()

	fmt.Printf("Rebalanced group %s: %d members, %d partitions\n",
		group.ID, len(group.Members), len(allPartitions))

	for memberID, partitions := range group.Assignments {
		fmt.Printf("   %s -> partitions %v\n", memberID, partitions)
	}

	return nil
}

// getAllPartitions discovers all partitions for the group's topics
func (group *ConsumerGroup) getAllPartitions() []int {
	var allPartitions []int

	for _, topic := range group.Topics {
		partitions, err := group.discoverTopicPartitions(topic)
		if err != nil {
			fmt.Printf("Warning: failed to discover partitions for topic %s: %v\n", topic, err)
			continue
		}
		allPartitions = append(allPartitions, partitions...)
	}

	return allPartitions
}

// discoverTopicPartitions discovers all partitions for a specific topic
// Uses the same logic as the existing consumer implementation
func (group *ConsumerGroup) discoverTopicPartitions(topic string) ([]int, error) {
	// We need to construct the path to the topic directory
	// Based on the consumer implementation, topics are stored in baseDir/../topics/
	topicsDir := filepath.Join(group.baseDir, "../topics")
	topicDir := filepath.Join(topicsDir, topic)

	// Check if this is a partitioned topic (directory exists)
	if stat, err := os.Stat(topicDir); err != nil || !stat.IsDir() {
		// Not a partitioned topic, might be legacy single-file topic
		// For consumer groups, we'll return empty slice for non-partitioned topics
		return []int{}, nil
	}

	entries, err := os.ReadDir(topicDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read topic directory %s: %v", topicDir, err)
	}

	var partitions []int
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "partition-") && strings.HasSuffix(entry.Name(), ".log") {
			name := strings.TrimPrefix(entry.Name(), "partition-")
			name = strings.TrimSuffix(name, ".log")
			if partition, parseErr := strconv.Atoi(name); parseErr == nil {
				partitions = append(partitions, partition)
			}
		}
	}

	// Sort partitions for consistent ordering
	sort.Ints(partitions)
	return partitions, nil
}

// saveToFile persists group state to disk
func (group *ConsumerGroup) saveToFile() error {
	groupFile := filepath.Join(group.baseDir, group.ID+".json")

	data, err := json.MarshalIndent(group, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal group: %v", err)
	}

	if err := os.WriteFile(groupFile, data, 0644); err != nil {
		return fmt.Errorf("failed to write group file: %v", err)
	}

	return nil
}

// loadFromFile loads group state from disk
func (mgr *ConsumerGroupManager) loadFromFile(groupID string) (*ConsumerGroup, error) {
	groupFile := filepath.Join(mgr.baseDir, groupID+".json")

	data, err := os.ReadFile(groupFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read group file: %v", err)
	}

	var group ConsumerGroup
	if err := json.Unmarshal(data, &group); err != nil {
		return nil, fmt.Errorf("failed to unmarshal group: %v", err)
	}

	group.baseDir = mgr.baseDir
	group.Members = make(map[string]*Member) // Consumer instances not persisted

	return &group, nil
}

// GetAssignedPartitions returns partitions assigned to a member
func (group *ConsumerGroup) GetAssignedPartitions(memberID string) []int {
	group.mtx.RLock()
	defer group.mtx.RUnlock()

	if partitions, exists := group.Assignments[memberID]; exists {
		return partitions
	}
	return []int{}
}
