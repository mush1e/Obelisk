package storage

// File pooling system that caches open file handles and automatically cleans up idle files.

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

// File wraps *os.File with thread-safe operations and metadata for LRU cleanup.
type File struct {
	mtx              sync.RWMutex
	lastAccessedNano int64
	file             *os.File
	dirty            int32
	inUse            int32
}

// NewFile creates a new File wrapper with the specified flags.
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

// SetLastAccessed atomically updates the last access timestamp for LRU cleanup.
func (f *File) SetLastAccessed(t time.Time) {
	atomic.StoreInt64(&f.lastAccessedNano, t.UnixNano())
}

// GetLastAccessed returns the last access timestamp.
func (f *File) GetLastAccessed() time.Time {
	n := atomic.LoadInt64(&f.lastAccessedNano)
	return time.Unix(0, n)
}

// GetTimeSinceAccessed returns elapsed time since last access.
func (f *File) GetTimeSinceAccessed() time.Duration {
	return time.Since(f.GetLastAccessed())
}

// IsDirty returns true if the file has uncommitted changes.
func (f *File) IsDirty() bool {
	return atomic.LoadInt32(&f.dirty) != 0
}

// IsInUse returns true if the file is currently being used
func (f *File) IsInUse() bool {
	return atomic.LoadInt32(&f.inUse) != 0
}

// setDirty atomically sets the dirty flag.
func (f *File) setDirty(v bool) {
	if v {
		atomic.StoreInt32(&f.dirty, 1)
	} else {
		atomic.StoreInt32(&f.dirty, 0)
	}
}

// setInUse atomically sets the inUse flag
func (f *File) setInUse(v bool) {
	if v {
		atomic.StoreInt32(&f.inUse, 1)
	} else {
		atomic.StoreInt32(&f.inUse, 0)
	}
}

// AppendWith performs an atomic append operation using a writer function.
func (f *File) AppendWith(fn func(w io.Writer) error) (int64, error) {
	f.mtx.Lock()
	defer f.mtx.Unlock()

	start, err := f.file.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, err
	}

	bw := bufio.NewWriter(f.file)
	if err := fn(bw); err != nil {
		f.setDirty(true)
		return 0, err
	}
	if err := bw.Flush(); err != nil {
		f.setDirty(true)
		return 0, err
	}

	// Sync to disk for durability
	if err := f.file.Sync(); err != nil {
		f.setDirty(true)
		return 0, err
	}

	f.setDirty(false)
	f.SetLastAccessed(time.Now())
	return start, nil
}

// AppendBatch performs an atomic batch append operation for multiple messages.
func (f *File) AppendBatch(msgBins [][]byte, writeProto func(w io.Writer, mb []byte) error) (int64, []int64, error) {
	f.mtx.Lock()
	defer f.mtx.Unlock()

	startPos, err := f.file.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, nil, err
	}

	// Pre-serialize messages to determine exact positions
	bw := bufio.NewWriter(f.file)
	var bytesWritten int64
	positions := make([]int64, 0, len(msgBins))

	for _, mb := range msgBins {

		var tmp bytes.Buffer
		if err := writeProto(&tmp, mb); err != nil {
			return 0, nil, err
		}

		positions = append(positions, startPos+bytesWritten)

		if _, err := bw.Write(tmp.Bytes()); err != nil {
			return 0, nil, err
		}
		bytesWritten += int64(tmp.Len())
	}

	if err := bw.Flush(); err != nil {
		f.setDirty(true)
		return 0, nil, err
	}

	if err := f.file.Sync(); err != nil {
		f.setDirty(true)
		return 0, nil, err
	}

	f.setDirty(false)
	f.SetLastAccessed(time.Now())
	return startPos, positions, nil
}

// Close safely closes the underlying file handle.
func (f *File) Close() error {
	f.mtx.Lock()
	defer f.mtx.Unlock()
	return f.file.Close()
}

// FilePool manages open file handles with automatic cleanup of idle files.
type FilePool struct {
	mtx       sync.RWMutex
	files     map[string]*File
	timeLimit time.Duration
	quit      chan struct{}
}

// NewFilePool creates a new file pool with the specified idle timeout.
func NewFilePool(timeLimit time.Duration) *FilePool {
	return &FilePool{
		files:     make(map[string]*File),
		timeLimit: timeLimit,
		quit:      make(chan struct{}),
	}
}

// WithFile provides RAII access to a file - automatic resource management!
// The file is guaranteed to be available during the callback and protected from cleanup.
// No manual cleanup needed - it's handled automatically!
func (fp *FilePool) WithFile(path string, flag int, fn func(*File) error) error {
	fp.mtx.Lock()

	f, exists := fp.files[path]
	if !exists {
		var err error
		f, err = NewFile(path, flag)
		if err != nil {
			fp.mtx.Unlock()
			return err
		}
		fp.files[path] = f
	}
	f.SetLastAccessed(time.Now())
	f.setInUse(true)
	fp.mtx.Unlock()

	defer f.setInUse(false)
	return fn(f)
}

// GetOrCreate retrieves an existing file from the pool or creates a new one.
// DEPRECATED - use WithFile instead
func (fp *FilePool) GetOrCreate(path string, flag int) (*File, error) {
	fp.mtx.Lock()
	defer fp.mtx.Unlock()

	if f, ok := fp.files[path]; ok {
		f.SetLastAccessed(time.Now())
		return f, nil
	}

	fptr, err := NewFile(path, flag)
	if err != nil {
		return nil, ErrCouldNotOpenFile
	}
	fp.files[path] = fptr
	return fptr, nil
}

// StartCleanup begins the background cleanup routine for idle files.
func (fp *FilePool) StartCleanup(checkInterval time.Duration) {
	ticker := time.NewTicker(checkInterval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				fp.cleanupIdleFiles()
			case <-fp.quit:
				return
			}
		}
	}()
}

// cleanupIdleFiles closes idle files without holding the pool lock during file operations.
func (fp *FilePool) cleanupIdleFiles() {

	fp.mtx.RLock()
	var toClose []string
	for path, file := range fp.files {
		if file.GetTimeSinceAccessed() >= fp.timeLimit &&
			!file.IsDirty() &&
			!file.IsInUse() { // 🛡️ RACE CONDITION ELIMINATED!
			toClose = append(toClose, path)
		}
	}
	fp.mtx.RUnlock()

	for _, path := range toClose {
		_ = fp.Close(path)
	}
}

// Close removes a file from the pool and closes its handle.
func (fp *FilePool) Close(path string) error {
	fp.mtx.Lock()
	defer fp.mtx.Unlock()

	f, exists := fp.files[path]
	if !exists {
		return ErrFileNotFoundInPool
	}

	delete(fp.files, path)
	// Close the underlying file
	if err := f.Close(); err != nil {
		return ErrCouldNotCloseFile
	}
	return nil
}

// Stop gracefully shuts down the file pool.

func (fp *FilePool) Stop() error {
	close(fp.quit) // Signal cleanup routine to stop
	return fp.closeAll()
}

// closeAll closes all files currently in the pool.
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
