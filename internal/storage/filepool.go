package storage

// This package provides a file pooling system for managing concurrent file operations
// in the Obelisk message broker. The file pool maintains a cache of open file handles
// to reduce the overhead of repeatedly opening and closing files for write operations.
// It implements thread-safe operations with per-file mutexes and automatic cleanup
// of idle files to prevent resource leaks.

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// Error definitions for file pool operations
var (
	ErrCouldNotOpenFile   = errors.New("could not open file")
	ErrCouldNotCloseFile  = errors.New("could not close file")
	ErrFileNotFoundInPool = errors.New("file not found in pool")
)

// File wraps *os.File and provides thread-safe operations with metadata tracking.
// Each File instance maintains its own mutex to allow concurrent access to different
// files while ensuring atomic operations on individual files. The dirty flag tracks
// whether the file has uncommitted changes, and lastAccessedNano is used for LRU
// cleanup decisions.
type File struct {
	mtx              sync.RWMutex // Per-file mutex for thread-safe operations
	lastAccessedNano int64        // Atomic timestamp for LRU cleanup (Unix nanoseconds)
	file             *os.File     // Underlying OS file handle
	dirty            int32        // Atomic dirty flag (0=clean, 1=dirty)
}

// NewFile creates a new File wrapper around an OS file handle.
// The file is opened with the specified flags and initialized with clean state.
// This is typically called by the FilePool when a new file needs to be opened.
//
// Parameters:
//   - path: filesystem path to the file
//   - flag: file opening flags (e.g., os.O_APPEND|os.O_CREATE|os.O_WRONLY)
//
// Returns:
//   - *File: wrapped file handle with metadata
//   - error: ErrCouldNotOpenFile if the underlying file operation fails
func NewFile(path string, flag int) (*File, error) {
	fptr, err := os.OpenFile(path, flag, 0644)
	if err != nil {
		return nil, ErrCouldNotOpenFile
	}
	f := &File{file: fptr}
	f.setDirty(false)
	f.SetLastAccessed(time.Now())
	return f, nil
}

// SetLastAccessed atomically updates the last access timestamp.
// This is called whenever the file is accessed to support LRU-based cleanup.
// Uses atomic operations to avoid the need for mutex locking on every access.
//
// Parameters:
//   - t: timestamp to set as the last access time
func (f *File) SetLastAccessed(t time.Time) {
	atomic.StoreInt64(&f.lastAccessedNano, t.UnixNano())
}

// GetLastAccessed atomically retrieves the last access timestamp.
// Used by the cleanup routine to determine which files are eligible for closure.
//
// Returns:
//   - time.Time: the last time this file was accessed
func (f *File) GetLastAccessed() time.Time {
	n := atomic.LoadInt64(&f.lastAccessedNano)
	return time.Unix(0, n)
}

// GetTimeSinceAccessed calculates how long it's been since the file was last accessed.
// This is used by the cleanup routine to identify idle files that can be closed.
//
// Returns:
//   - time.Duration: elapsed time since last access
func (f *File) GetTimeSinceAccessed() time.Duration {
	return time.Since(f.GetLastAccessed())
}

// IsDirty atomically checks if the file has uncommitted changes.
// A file is marked dirty when a write operation fails to sync to disk.
// Dirty files are excluded from cleanup to prevent data loss.
//
// Returns:
//   - bool: true if the file has uncommitted changes
func (f *File) IsDirty() bool {
	return atomic.LoadInt32(&f.dirty) != 0
}

// setDirty atomically sets the dirty flag state.
// This is called internally when write operations succeed (false) or fail (true).
// Uses atomic operations for thread-safety without mutex overhead.
//
// Parameters:
//   - v: true to mark dirty, false to mark clean
func (f *File) setDirty(v bool) {
	if v {
		atomic.StoreInt32(&f.dirty, 1)
	} else {
		atomic.StoreInt32(&f.dirty, 0)
	}
}

// AppendWith performs an atomic append operation using a writer function.
// This method ensures thread-safety by:
// 1. Acquiring an exclusive lock on the file
// 2. Seeking to the end of the file
// 3. Executing the provided writer function with a buffered writer
// 4. Flushing the buffer and syncing to disk
// 5. Updating metadata (dirty flag and access time)
//
// The writer function receives a buffered writer and should write all data
// that needs to be appended. The entire operation is atomic - either all
// data is written and synced, or an error is returned.
//
// Parameters:
//   - fn: function that writes data to the provided writer
//
// Returns:
//   - int64: byte offset where the write operation started
//   - error: any error that occurred during the operation
func (f *File) AppendWith(fn func(w io.Writer) error) (int64, error) {
	f.mtx.Lock()
	defer f.mtx.Unlock()

	// Get the starting position before writing
	start, err := f.file.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, err
	}

	// Use buffered writer for performance
	bw := bufio.NewWriter(f.file)
	if err := fn(bw); err != nil {
		return 0, err
	}
	if err := bw.Flush(); err != nil {
		return 0, err
	}

	// Sync to ensure data is persisted to disk before updating metadata
	// This ensures durability and prevents data loss on system crashes
	if err := f.file.Sync(); err != nil {
		f.setDirty(true) // Mark dirty on sync failure
		return 0, err
	}

	// Update metadata after successful write
	f.setDirty(false)
	f.SetLastAccessed(time.Now())
	return start, nil
}

// AppendBatch performs an atomic batch append operation for multiple messages.
// This is an optimized version of AppendWith for handling multiple messages
// efficiently. It pre-serializes all messages into buffers, then writes them
// sequentially while holding the file lock only once.
//
// The writeProto function should handle the protocol-specific framing for each
// message (e.g., length prefixes, delimiters, etc.). Each message is written
// with its protocol framing, and the absolute byte position of each message
// is tracked and returned.
//
// Parameters:
//   - msgBins: slice of serialized message bytes to write
//   - writeProto: function that writes a message with protocol framing
//
// Returns:
//   - int64: starting byte offset of the batch write operation
//   - []int64: absolute byte positions where each message starts
//   - error: any error that occurred during the operation
func (f *File) AppendBatch(msgBins [][]byte, writeProto func(w io.Writer, mb []byte) error) (int64, []int64, error) {
	f.mtx.Lock()
	defer f.mtx.Unlock()

	// Get starting position for the entire batch
	startPos, err := f.file.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, nil, err
	}

	// Pre-serialize each message with protocol framing into temporary buffers.
	// This allows us to know the exact size and position of each message
	// before committing to disk, enabling precise index generation.
	bw := bufio.NewWriter(f.file)
	var bytesWritten int64
	positions := make([]int64, 0, len(msgBins))

	for _, mb := range msgBins {
		// Use temporary buffer to determine message size after protocol framing
		var tmp bytes.Buffer
		if err := writeProto(&tmp, mb); err != nil {
			return 0, nil, err
		}

		// Record the absolute position where this message starts
		positions = append(positions, startPos+bytesWritten)

		// Write the framed message to the buffered writer
		if _, err := bw.Write(tmp.Bytes()); err != nil {
			return 0, nil, err
		}
		bytesWritten += int64(tmp.Len())
	}

	// Flush buffer and sync to disk atomically
	if err := bw.Flush(); err != nil {
		return 0, nil, err
	}

	if err := f.file.Sync(); err != nil {
		f.setDirty(true) // Mark dirty on sync failure
		return 0, nil, err
	}

	// Update metadata after successful batch write
	f.setDirty(false)
	f.SetLastAccessed(time.Now())
	return startPos, positions, nil
}

// Close safely closes the underlying file handle.
// This acquires the file lock to ensure no concurrent operations are in progress.
// Once closed, the File instance should not be used for further operations.
//
// Returns:
//   - error: any error that occurred during file closure
func (f *File) Close() error {
	f.mtx.Lock()
	defer f.mtx.Unlock()
	return f.file.Close()
}

// FilePool manages a collection of open file handles with automatic cleanup.
// It maintains a map of file paths to File instances, allowing efficient reuse
// of file handles across multiple operations. A background cleanup routine
// periodically closes idle files to prevent resource exhaustion.
//
// The pool is thread-safe and supports concurrent access to different files.
// Files are automatically opened on first access and kept open until they
// become idle for longer than the configured time limit.
type FilePool struct {
	mtx       sync.RWMutex     // Protects the files map
	files     map[string]*File // Map of file paths to File instances
	timeLimit time.Duration    // Idle timeout for file cleanup
	quit      chan struct{}    // Channel for signaling cleanup routine shutdown
}

// NewFilePool creates a new file pool with the specified idle timeout.
// Files that remain unused for longer than timeLimit will be automatically
// closed by the cleanup routine. The cleanup routine must be started
// separately using StartCleanup().
//
// Parameters:
//   - timeLimit: maximum idle time before a file is eligible for cleanup
//
// Returns:
//   - *FilePool: new file pool instance
func NewFilePool(timeLimit time.Duration) *FilePool {
	return &FilePool{
		files:     make(map[string]*File),
		timeLimit: timeLimit,
		quit:      make(chan struct{}),
	}
}

// GetOrCreate retrieves an existing file from the pool or creates a new one.
// This is the primary method for accessing files through the pool. If a file
// is already open, its access time is updated and the existing handle is returned.
// If the file is not in the pool, it's opened with the specified flags and added.
//
// This method is thread-safe and can be called concurrently for different files.
// However, it uses a write lock when adding new files to ensure consistency.
//
// Parameters:
//   - path: filesystem path to the file
//   - flag: file opening flags if a new file needs to be created
//
// Returns:
//   - *File: the file handle (existing or newly created)
//   - error: ErrCouldNotOpenFile if the file cannot be opened
func (fp *FilePool) GetOrCreate(path string, flag int) (*File, error) {
	fp.mtx.Lock()
	defer fp.mtx.Unlock()

	// Check if file already exists in pool
	if f, ok := fp.files[path]; ok {
		f.SetLastAccessed(time.Now()) // Update access time for LRU
		return f, nil
	}

	// File not in pool, create new one
	fptr, err := NewFile(path, flag)
	if err != nil {
		return nil, ErrCouldNotOpenFile
	}
	fp.files[path] = fptr
	return fptr, nil
}

// StartCleanup begins the background cleanup routine for idle files.
// The cleanup routine runs periodically (every checkInterval) and closes
// files that have been idle for longer than the pool's timeLimit.
// Only clean files (not dirty) are eligible for cleanup to prevent data loss.
//
// This should be called once after creating the FilePool and before using it.
// The cleanup routine runs in a separate goroutine and can be stopped using Stop().
//
// Parameters:
//   - checkInterval: how often to run the cleanup routine
func (fp *FilePool) StartCleanup(checkInterval time.Duration) {
	ticker := time.NewTicker(checkInterval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				fp.cleanupIdleFiles() // Perform cleanup on each tick
			case <-fp.quit:
				return // Shutdown signal received
			}
		}
	}()
}

// cleanupIdleFiles identifies and closes files that have been idle too long.
// This method uses a two-phase approach to avoid holding the pool lock
// while performing potentially slow file operations:
//
// Phase 1: Acquire read lock, identify files eligible for cleanup
// Phase 2: Release read lock, close identified files using their individual Close methods
//
// Only files that are both idle (beyond timeLimit) and clean (not dirty) are closed.
// This prevents data loss from uncommitted writes while still managing resources.
func (fp *FilePool) cleanupIdleFiles() {
	// Phase 1: Identify files to close (under read lock for minimal contention)
	fp.mtx.RLock()
	var toClose []string
	for path, file := range fp.files {
		if file.GetTimeSinceAccessed() >= fp.timeLimit && !file.IsDirty() {
			toClose = append(toClose, path)
		}
	}
	fp.mtx.RUnlock()

	// Phase 2: Close identified files (outside of pool lock)
	for _, path := range toClose {
		_ = fp.Close(path) // Ignore errors; Close returns ErrCouldNotCloseFile on failure
	}
}

// Close removes a specific file from the pool and closes its handle.
// This is used both by the cleanup routine and for explicit file closure.
// The file is removed from the pool map regardless of whether the close
// operation succeeds, preventing resource leaks.
//
// Parameters:
//   - path: filesystem path of the file to close
//
// Returns:
//   - error: ErrFileNotFoundInPool if file not in pool, ErrCouldNotCloseFile on close failure
func (fp *FilePool) Close(path string) error {
	fp.mtx.Lock()
	defer fp.mtx.Unlock()

	f, exists := fp.files[path]
	if !exists {
		return ErrFileNotFoundInPool
	}

	// Remove from pool first to prevent further access
	delete(fp.files, path)

	// Close the underlying file
	if err := f.Close(); err != nil {
		return ErrCouldNotCloseFile
	}
	return nil
}

// Stop gracefully shuts down the file pool.
// This method:
// 1. Signals the cleanup routine to stop
// 2. Closes all remaining files in the pool
// 3. Clears the pool map
//
// After calling Stop(), the FilePool should not be used for further operations.
// This is typically called during application shutdown.
//
// Returns:
//   - error: the first error encountered while closing files, or nil if all succeed
func (fp *FilePool) Stop() error {
	close(fp.quit) // Signal cleanup routine to stop
	return fp.closeAll()
}

// closeAll closes all files currently in the pool.
// This is an internal method used by Stop() to perform bulk cleanup.
// Files are removed from the map regardless of whether individual close
// operations succeed, ensuring complete cleanup even in error conditions.
//
// Returns:
//   - error: the first error encountered, or nil if all files close successfully
func (fp *FilePool) closeAll() error {
	fp.mtx.Lock()
	defer fp.mtx.Unlock()

	var firstErr error
	for path, file := range fp.files {
		if err := file.Close(); err != nil && firstErr == nil {
			firstErr = err // Capture first error but continue closing others
		}
		delete(fp.files, path)
	}
	return firstErr
}
