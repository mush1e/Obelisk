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

// TestRAII_NoRaceConditions proves that the RAII pattern eliminates race conditions
func TestRAII_NoRaceConditions(t *testing.T) {
	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "raii-test.log")

	// Same aggressive settings that caused races before
	pool := NewFilePool(20 * time.Millisecond) // Very short timeout
	defer pool.Stop()
	pool.StartCleanup(5 * time.Millisecond) // Aggressive cleanup

	var (
		totalOps      int32
		successfulOps int32
		raceDetected  int32
		wg            sync.WaitGroup
	)

	// Same stress test parameters that found 758 races before
	numGoroutines := 100
	opsPerGoroutine := 200

	t.Logf("🚀 Testing RAII pattern with %d goroutines, %d ops each",
		numGoroutines, opsPerGoroutine)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			for j := 0; j < opsPerGoroutine; j++ {
				atomic.AddInt32(&totalOps, 1)

				// 🛡️ RAII PATTERN: File automatically protected from cleanup!
				err := pool.WithFile(testFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, func(f *File) error {
					// Add delay to give cleanup maximum chance to interfere
					time.Sleep(time.Microsecond * time.Duration(j%20))

					// Try to use file - should NEVER fail with "file already closed"
					_, writeErr := f.AppendWith(func(w io.Writer) error {
						message := fmt.Sprintf("raii_goroutine_%d_op_%d\n", goroutineID, j)
						_, err := w.Write([]byte(message))

						// Extra delay during write to stress test the protection
						time.Sleep(time.Microsecond * 10)
						return err
					})
					return writeErr
				})

				if err != nil {
					// Check for race condition indicators
					errStr := err.Error()
					if containsAny(errStr, []string{
						"use of closed",
						"bad file descriptor",
						"file already closed",
						"invalid argument",
					}) {
						atomic.AddInt32(&raceDetected, 1)
						t.Errorf("🔥 RACE DETECTED with RAII (should be impossible!): %v", err)
					} else {
						// Other errors are acceptable (disk full, permissions, etc.)
						t.Logf("Non-race error: %v", err)
					}
				} else {
					atomic.AddInt32(&successfulOps, 1)
				}

				// Force cleanup to run frequently
				if j%10 == 0 {
					time.Sleep(time.Millisecond * 25) // Longer than cleanup timeout
				}
			}
		}(i)
	}

	wg.Wait()

	// Results
	total := atomic.LoadInt32(&totalOps)
	successful := atomic.LoadInt32(&successfulOps)
	races := atomic.LoadInt32(&raceDetected)

	t.Logf("🏁 RAII Test Results:")
	t.Logf("  Total operations: %d", total)
	t.Logf("  Successful: %d", successful)
	t.Logf("  Race conditions: %d", races)
	t.Logf("  Success rate: %.2f%%", float64(successful)/float64(total)*100)

	// This should be 0 with RAII!
	if races == 0 {
		t.Logf("🎉 SUCCESS: RAII eliminated all race conditions!")
	} else {
		t.Errorf("❌ FAILURE: Found %d races (RAII should prevent ALL races)", races)
	}

	// Success rate should be very high (only legitimate errors allowed)
	successRate := float64(successful) / float64(total) * 100
	if successRate < 95.0 {
		t.Errorf("❌ Low success rate: %.2f%% (expected >95%%)", successRate)
	}
}

// TestRAII_vs_Manual compares RAII vs manual resource management
func TestRAII_vs_Manual(t *testing.T) {
	tempDir := t.TempDir()

	// Test 1: Manual resource management (legacy GetOrCreate)
	t.Run("Manual_Resource_Management", func(t *testing.T) {
		testFile := filepath.Join(tempDir, "manual-test.log")
		pool := NewFilePool(50 * time.Millisecond)
		defer pool.Stop()
		pool.StartCleanup(10 * time.Millisecond)

		var races int32
		var total int32

		// Smaller test to avoid spamming logs
		for i := 0; i < 50; i++ {
			atomic.AddInt32(&total, 1)

			// Legacy pattern (potentially racy)
			f, err := pool.GetOrCreate(testFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY)
			if err != nil {
				continue
			}

			time.Sleep(time.Microsecond * 100) // Let cleanup run

			_, err = f.AppendWith(func(w io.Writer) error {
				_, writeErr := w.Write([]byte("manual test\n"))
				return writeErr
			})

			if err != nil && containsAny(err.Error(), []string{"file already closed", "use of closed"}) {
				atomic.AddInt32(&races, 1)
			}

			time.Sleep(time.Millisecond * 60) // Force cleanup
		}

		manualRaces := atomic.LoadInt32(&races)
		manualTotal := atomic.LoadInt32(&total)
		t.Logf("Manual management: %d races out of %d ops", manualRaces, manualTotal)
	})

	// Test 2: RAII pattern
	t.Run("RAII_Pattern", func(t *testing.T) {
		testFile := filepath.Join(tempDir, "raii-test.log")
		pool := NewFilePool(50 * time.Millisecond)
		defer pool.Stop()
		pool.StartCleanup(10 * time.Millisecond)

		var races int32
		var total int32

		for i := 0; i < 50; i++ {
			atomic.AddInt32(&total, 1)

			// RAII pattern (should be race-free)
			err := pool.WithFile(testFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, func(f *File) error {
				time.Sleep(time.Microsecond * 100) // Let cleanup try to run

				_, writeErr := f.AppendWith(func(w io.Writer) error {
					_, err := w.Write([]byte("raii test\n"))
					return err
				})
				return writeErr
			})

			if err != nil && containsAny(err.Error(), []string{"file already closed", "use of closed"}) {
				atomic.AddInt32(&races, 1)
			}

			time.Sleep(time.Millisecond * 60) // Force cleanup
		}

		raiiRaces := atomic.LoadInt32(&races)
		raiiTotal := atomic.LoadInt32(&total)
		t.Logf("RAII pattern: %d races out of %d ops", raiiRaces, raiiTotal)

		// RAII should have 0 races
		if raiiRaces > 0 {
			t.Errorf("RAII should have 0 races, got %d", raiiRaces)
		}
	})
}

// BenchmarkRAII_vs_Manual compares performance of RAII vs manual management
func BenchmarkRAII_vs_Manual(b *testing.B) {
	tempDir := b.TempDir()

	b.Run("Manual", func(b *testing.B) {
		testFile := filepath.Join(tempDir, "bench-manual.log")
		pool := NewFilePool(time.Hour) // No cleanup during benchmark
		defer pool.Stop()

		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				f, err := pool.GetOrCreate(testFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY)
				if err != nil {
					b.Fatal(err)
				}

				_, err = f.AppendWith(func(w io.Writer) error {
					_, writeErr := w.Write([]byte("benchmark\n"))
					return writeErr
				})

				if err != nil {
					b.Fatal(err)
				}
			}
		})
	})

	b.Run("RAII", func(b *testing.B) {
		testFile := filepath.Join(tempDir, "bench-raii.log")
		pool := NewFilePool(time.Hour) // No cleanup during benchmark
		defer pool.Stop()

		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				err := pool.WithFile(testFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, func(f *File) error {
					_, writeErr := f.AppendWith(func(w io.Writer) error {
						_, err := w.Write([]byte("benchmark\n"))
						return err
					})
					return writeErr
				})

				if err != nil {
					b.Fatal(err)
				}
			}
		})
	})
}

// Helper function (reused from earlier test)
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
