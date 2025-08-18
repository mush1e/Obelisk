package main

import (
	"fmt"
	"hash/fnv"
)

func getPartition(key string, numPartitions int) int {
	if numPartitions <= 1 {
		return 0
	}

	if key == "" {
		return 0 // Would be random in real code
	}

	hash := fnv.New32a()
	hash.Write([]byte(key))
	return int(hash.Sum32()) % numPartitions
}

func main() {
	// Test keys from your test client
	keys := []string{
		"user-123",
		"user-456",
		"user-789",
		"user-111",
		"user-222",
		"user-333",
		"user-444",
		"user-555",
		"user-666",
		"user-777",
	}

	numPartitions := 8 // From your config for 'orders' topic

	fmt.Println("Key Distribution for 'orders' topic (8 partitions):")
	fmt.Println("================================================")

	// Track distribution
	partitionCounts := make(map[int][]string)

	for _, key := range keys {
		partition := getPartition(key, numPartitions)
		partitionCounts[partition] = append(partitionCounts[partition], key)
		fmt.Printf("%-12s -> partition %d\n", key, partition)
	}

	fmt.Println("\n\nPartition Summary:")
	fmt.Println("==================")
	for p := 0; p < numPartitions; p++ {
		keys := partitionCounts[p]
		fmt.Printf("Partition %d: %d keys", p, len(keys))
		if len(keys) > 0 {
			fmt.Printf(" (%v)", keys)
		}
		fmt.Println()
	}

	// Show that same key always goes to same partition
	fmt.Println("\n\nConsistency Check:")
	fmt.Println("==================")
	testKey := "user-123"
	for i := 0; i < 5; i++ {
		partition := getPartition(testKey, numPartitions)
		fmt.Printf("Attempt %d: '%s' -> partition %d\n", i+1, testKey, partition)
	}
}
