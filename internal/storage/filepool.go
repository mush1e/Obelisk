package storage

// This package provides a file pool for managing file handles efficiently.
// The file pool automatically closes idle files after a specified time limit
// to prevent resource leaks while maintaining performance for frequently accessed files.

import (
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrCouldNotOpenFile  = errors.New("could not open file")
	ErrCouldNotCloseFile = errors.New("could not close file")
)

// FilePool manages a collection of open file handles with automatic cleanup.
// It provides thread-safe access to files and closes idle files after a time limit
// to prevent resource exhaustion in long-running applications.
type FilePool struct {
	mtx       sync.RWMutex     // Protects the files map
	files     map[string]*File // Map of file path to File wrapper
	timeLimit time.Duration    // Time after which idle files are closed
	quit      chan struct{}    // Channel to signal cleanup goroutine to stop
}

// File wraps an os.File with metadata for pool management.
// It tracks access time atomically to support concurrent access patterns.
type File struct {
	lastAccessedNano int64    // Store as nanoseconds since epoch (atomic-friendly)
	file             *os.File // The actual file handle
	isDirty          bool     // Whether the file has pending writes
}

// NewFilePool creates a new FilePool with the specified idle time limit.
// Files that haven't been accessed for longer than timeLimit will be automatically closed.
func NewFilePool(timeLimit time.Duration) *FilePool {
	return &FilePool{
		files:     make(map[string]*File),
		timeLimit: timeLimit,
	}
}

// SetLastAccessed updates the last accessed time atomically.
// This is safe for concurrent use across multiple goroutines.
func (f *File) SetLastAccessed(t time.Time) {
	atomic.StoreInt64(&f.lastAccessedNano, t.UnixNano())
}

// GetLastAccessed reads the last accessed time atomically.
// This is safe for concurrent use across multiple goroutines.
func (f *File) GetLastAccessed() time.Time {
	nano := atomic.LoadInt64(&f.lastAccessedNano)
	return time.Unix(0, nano)
}

// GetTimeSinceAccessed returns how long since the file was last accessed.
// This is used by the cleanup routine to determine which files to close.
func (f *File) GetTimeSinceAccessed() time.Duration {
	return time.Since(f.GetLastAccessed())
}

// NewFile creates a new File wrapper around an os.File handle.
// It opens the file with the specified flags and initializes the access time.
func NewFile(path string, flag int) (*File, error) {
	fptr, err := os.OpenFile(path, flag, 0644)
	if err != nil {
		return nil, ErrCouldNotOpenFile
	}
	file := &File{
		file: fptr,
	}
	file.SetLastAccessed(time.Now())
	return file, nil
}

// GetOrCreate returns an existing file handle or creates a new one.
// If the file is already in the pool, it updates the access time.
// If not, it creates a new file handle and adds it to the pool.
func (fp *FilePool) GetOrCreate(path string, flag int) (*os.File, error) {
	fp.mtx.Lock()
	defer fp.mtx.Unlock()

	// Check if file is already in the pool
	f, exists := fp.files[path]
	if exists {
		f.SetLastAccessed(time.Now())
		return f.file, nil
	}

	// Create new file and add to pool
	fptr, err := NewFile(path, flag)
	if err != nil {
		return nil, ErrCouldNotOpenFile
	}

	fp.files[path] = fptr
	return fptr.file, nil
}

// StartCleanup starts a background goroutine that periodically closes idle files.
// It runs a cleanup check every checkInterval duration until the pool is stopped.
func (fp *FilePool) StartCleanup(checkInterval time.Duration) {
	ticker := time.NewTicker(checkInterval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				fp.cleanupIdleFiles() // Check and close idle files
			case <-fp.quit:
				// fp.closeAll() // TODO: implement closeAll if needed
				return // Exit cleanup goroutine
			}
		}
	}()
}

// cleanupIdleFiles identifies and closes files that have been idle for too long.
// It only closes files that aren't dirty to prevent data loss.
func (fp *FilePool) cleanupIdleFiles() {
	fp.mtx.RLock()
	var toClose []string

	// Find files that exceed the idle time limit and aren't dirty
	for path, file := range fp.files {
		if file.GetTimeSinceAccessed() >= fp.timeLimit && !file.isDirty {
			toClose = append(toClose, path)
		}
	}
	fp.mtx.RUnlock()

	// Close idle files (this acquires write lock internally)
	for _, path := range toClose {
		fp.Close(path)
	}
}

// Close closes the file handle for the specified path and removes it from the pool.
// Returns an error if the file is not in the pool or cannot be closed.
func (fp *FilePool) Close(path string) error {
	fp.mtx.Lock()
	defer fp.mtx.Unlock()

	f, exists := fp.files[path]
	if !exists {
		return errors.New("file not found in pool")
	}

	// Close the underlying file handle
	if err := f.file.Close(); err != nil {
		return ErrCouldNotCloseFile
	}

	// Remove from pool
	delete(fp.files, path)
	return nil
}
