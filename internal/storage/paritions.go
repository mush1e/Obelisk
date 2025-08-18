package storage

import (
	"fmt"
	"hash/fnv"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
)

func GetPartition(key string, numPartitions int) int {
	if numPartitions <= 1 {
		return 0
	}

	// if no key is provited return random partition
	if key == "" {
		return rand.Intn(numPartitions)
	}

	// consistent hash based paritioning
	hash := fnv.New32a()
	hash.Write([]byte(key))
	return int(hash.Sum32()) % numPartitions
}

// GetPartitionedPaths returns the log and index file paths for a partition
func GetPartitionedPaths(baseDir, topic string, partition int) (string, string) {
	topicDir := filepath.Join(baseDir, topic)
	logFile := filepath.Join(topicDir, fmt.Sprintf("partition-%d.log", partition))
	idxFile := filepath.Join(topicDir, fmt.Sprintf("partition-%d.idx", partition))
	return logFile, idxFile
}

// EnsureTopicDirectory creates the topic directory if it doesn't exist
func EnsureTopicDirectory(baseDir, topic string) error {
	topicDir := filepath.Join(baseDir, topic)
	return os.MkdirAll(topicDir, 0755)
}

// IsPartitionedTopic checks if a topic uses the new partitioned structure
func IsPartitionedTopic(baseDir, topic string) bool {
	topicDir := filepath.Join(baseDir, topic)
	if _, err := os.Stat(topicDir); os.IsNotExist(err) {
		return false
	}

	// Check if directory exists and has partition files
	entries, err := os.ReadDir(topicDir)
	if err != nil {
		return false
	}

	for _, entry := range entries {
		if !entry.IsDir() && strings.HasPrefix(entry.Name(), "partition-") {
			return true
		}
	}
	return false
}
