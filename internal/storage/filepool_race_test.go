package storage

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestFilePoolRaceCondition attempts to reproduce the use-after-close race condition
func TestFilePoolRaceCondition(t *testing.T) {
	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "race-test.log")

	// Create a file pool with aggressive cleanup (short timeout)
	pool := NewFilePool(50 * time.Millisecond) // Very short timeout to trigger cleanup quickly
	defer pool.Stop()

	// Start cleanup with high frequency to increase race probability
	pool.StartCleanup(10 * time.Millisecond)

	var (
		raceDetected  int32
		successfulOps int32
		totalAttempts int32
		wg            sync.WaitGroup
	)

	// Number of goroutines and operations - tune this to increase race probability
	numGoroutines := 50
	opsPerGoroutine := 100

	t.Logf("Starting race condition test with %d goroutines, %d ops each",
		numGoroutines, opsPerGoroutine)

	// Launch multiple goroutines that repeatedly get files and use them
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			for j := 0; j < opsPerGoroutine; j++ {
				atomic.AddInt32(&totalAttempts, 1)

				// Get file from pool
				f, err := pool.GetOrCreate(testFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY)
				if err != nil {
					t.Errorf("Goroutine %d: Failed to get file: %v", goroutineID, err)
					continue
				}

				// Add a small delay to increase the chance of cleanup running
				// between getting the file and using it
				time.Sleep(time.Microsecond * time.Duration(j%10))

				// Try to use the file - this is where the race condition manifests
				_, err = f.AppendWith(func(w io.Writer) error {
					message := fmt.Sprintf("goroutine_%d_op_%d\n", goroutineID, j)
					_, writeErr := w.Write([]byte(message))
					return writeErr
				})

				if err != nil {
					// Check if this looks like a use-after-close error
					errStr := err.Error()
					if containsAny(errStr, []string{
						"use of closed",
						"bad file descriptor",
						"file already closed",
						"invalid argument",
					}) {
						atomic.AddInt32(&raceDetected, 1)
						t.Logf("🎯 RACE DETECTED in goroutine %d, op %d: %v", goroutineID, j, err)
					} else {
						t.Logf("Other error in goroutine %d, op %d: %v", goroutineID, j, err)
					}
				} else {
					atomic.AddInt32(&successfulOps, 1)
				}

				// Add another delay to let cleanup routine run
				if j%10 == 0 {
					time.Sleep(time.Microsecond * 100)
				}
			}
		}(i)
	}

	// Wait for all goroutines to complete
	wg.Wait()

	// Report results
	totalOps := atomic.LoadInt32(&totalAttempts)
	successful := atomic.LoadInt32(&successfulOps)
	races := atomic.LoadInt32(&raceDetected)

	t.Logf("Test Results:")
	t.Logf("  Total operations: %d", totalOps)
	t.Logf("  Successful: %d", successful)
	t.Logf("  Race conditions detected: %d", races)
	t.Logf("  Success rate: %.2f%%", float64(successful)/float64(totalOps)*100)

	if races > 0 {
		t.Logf("🔥 SUCCESS: Race condition reproduced! Found %d instances", races)
	} else {
		t.Logf("⚠️  No race detected - try increasing goroutines/operations or adjusting timing")
	}

	// This test is about demonstrating the race, not necessarily failing
	// In a real fix, you'd want races to be 0
}

// TestFilePoolRaceConditionWithStress is an even more aggressive version
func TestFilePoolRaceConditionWithStress(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	tempDir := t.TempDir()

	// Test multiple files to increase contention
	testFiles := make([]string, 5)
	for i := range testFiles {
		testFiles[i] = filepath.Join(tempDir, fmt.Sprintf("race-test-%d.log", i))
	}

	// Very aggressive cleanup settings
	pool := NewFilePool(20 * time.Millisecond)
	defer pool.Stop()
	pool.StartCleanup(5 * time.Millisecond)

	var (
		raceDetected  int32
		totalAttempts int32
		wg            sync.WaitGroup
	)

	// Even more goroutines and operations
	numGoroutines := 100
	opsPerGoroutine := 200

	t.Logf("Starting STRESS test with %d goroutines, %d ops each, %d files",
		numGoroutines, opsPerGoroutine, len(testFiles))

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			for j := 0; j < opsPerGoroutine; j++ {
				atomic.AddInt32(&totalAttempts, 1)

				// Randomly pick a file to increase pool contention
				testFile := testFiles[j%len(testFiles)]

				f, err := pool.GetOrCreate(testFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY)
				if err != nil {
					continue
				}

				// Varying delays to hit different timing windows
				delay := time.Duration(j%50) * time.Microsecond
				time.Sleep(delay)

				// Try to write - race condition should manifest here
				_, err = f.AppendWith(func(w io.Writer) error {
					data := fmt.Sprintf("stress_%d_%d\n", goroutineID, j)
					_, writeErr := w.Write([]byte(data))

					// Add delay during write to increase race window
					time.Sleep(time.Microsecond * 10)
					return writeErr
				})

				if err != nil && containsAny(err.Error(), []string{
					"use of closed", "bad file descriptor", "file already closed",
				}) {
					atomic.AddInt32(&raceDetected, 1)
					if atomic.LoadInt32(&raceDetected) <= 5 { // Don't spam logs
						t.Logf("🎯 STRESS RACE DETECTED: %v", err)
					}
				}

				// Occasionally force cleanup to run
				if j%20 == 0 {
					time.Sleep(time.Millisecond * 30) // Let cleanup run
				}
			}
		}(i)
	}

	wg.Wait()

	races := atomic.LoadInt32(&raceDetected)
	total := atomic.LoadInt32(&totalAttempts)

	t.Logf("Stress Test Results:")
	t.Logf("  Total operations: %d", total)
	t.Logf("  Race conditions: %d", races)
	t.Logf("  Race rate: %.4f%%", float64(races)/float64(total)*100)

	if races > 0 {
		t.Logf("🔥 STRESS SUCCESS: Reproduced %d race conditions!", races)
	}
}

// TestFilePoolWithReferenceCounting demonstrates how reference counting would fix the issue
func TestFilePoolWithReferenceCounting(t *testing.T) {
	t.Skip("This test would require implementing reference counting first")

	// This test would:
	// 1. Implement a FileWithRefCount wrapper
	// 2. Modify GetOrCreate to increment ref count
	// 3. Modify cleanup to check ref count
	// 4. Run the same stress test
	// 5. Verify that races == 0
}

// Helper function to check if error string contains any of the given substrings
func containsAny(s string, substrings []string) bool {
	for _, sub := range substrings {
		if len(s) >= len(sub) {
			for i := 0; i <= len(s)-len(sub); i++ {
				if s[i:i+len(sub)] == sub {
					return true
				}
			}
		}
	}
	return false
}

// Benchmark to measure the overhead of reference counting (when implemented)
func BenchmarkFilePoolConcurrentAccess(b *testing.B) {
	tempDir := b.TempDir()
	testFile := filepath.Join(tempDir, "benchmark.log")

	pool := NewFilePool(time.Hour) // Long timeout to avoid cleanup during benchmark
	defer pool.Stop()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			f, err := pool.GetOrCreate(testFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY)
			if err != nil {
				b.Fatalf("Failed to get file: %v", err)
			}

			_, err = f.AppendWith(func(w io.Writer) error {
				_, writeErr := w.Write([]byte("benchmark data\n"))
				return writeErr
			})

			if err != nil {
				b.Fatalf("Failed to write: %v", err)
			}
		}
	})
}
