package storage

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

var (
	ErrCouldNotOpenFile   = errors.New("could not open file")
	ErrCouldNotCloseFile  = errors.New("could not close file")
	ErrFileNotFoundInPool = errors.New("file not found in pool")
)

// File wraps *os.File and provides a per-file mutex and metadata
// so we can safely do seek+write+sync operations atomically.
type File struct {
	mtx              sync.RWMutex
	lastAccessedNano int64
	file             *os.File
	dirty            int32
}

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

// Atomic operation to update lastAccessedNano safely
func (f *File) SetLastAccessed(t time.Time) {
	atomic.StoreInt64(&f.lastAccessedNano, t.UnixNano())
}

func (f *File) GetLastAccessed() time.Time {
	n := atomic.LoadInt64(&f.lastAccessedNano)
	return time.Unix(0, n)
}

func (f *File) GetTimeSinceAccessed() time.Duration {
	return time.Since(f.GetLastAccessed())
}

func (f *File) IsDirty() bool {
	return atomic.LoadInt32(&f.dirty) != 0
}

func (f *File) setDirty(v bool) {
	if v {
		atomic.StoreInt32(&f.dirty, 1)
	} else {
		atomic.StoreInt32(&f.dirty, 0)
	}
}

// AppendWith acquires the file lock, seeks to end, runs the writer func,
// flushes, fsyncs, updates metadata and returns the starting offset.
func (f *File) AppendWith(fn func(w io.Writer) error) (int64, error) {
	f.mtx.Lock()
	defer f.mtx.Unlock()

	start, err := f.file.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, err
	}

	bw := bufio.NewWriter(f.file)
	if err := fn(bw); err != nil {
		return 0, err
	}
	if err := bw.Flush(); err != nil {
		return 0, err
	}

	// Sync to ensure data on disk before index update
	if err := f.file.Sync(); err != nil {
		f.setDirty(true)
		return 0, err
	}

	f.setDirty(false)
	f.SetLastAccessed(time.Now())
	return start, nil
}

// AppendBatch writes proto-encoded payloads and returns absolute positions for each item.
// writeProto should take an io.Writer and the raw message bytes and write the protocol framing.
func (f *File) AppendBatch(msgBins [][]byte, writeProto func(w io.Writer, mb []byte) error) (int64, []int64, error) {
	f.mtx.Lock()
	defer f.mtx.Unlock()

	startPos, err := f.file.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, nil, err
	}

	// Buffer everything into a bytes.Buffer per proto chunk so we know sizes/positions,
	// then write sequentially into the bufio.Writer under the same lock.
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

// Close underlying file
func (f *File) Close() error {
	f.mtx.Lock()
	defer f.mtx.Unlock()
	return f.file.Close()
}

// FilePool manages a map of path -> *File and a cleanup goroutine
type FilePool struct {
	mtx       sync.RWMutex
	files     map[string]*File
	timeLimit time.Duration
	quit      chan struct{}
}

func NewFilePool(timeLimit time.Duration) *FilePool {
	return &FilePool{
		files:     make(map[string]*File),
		timeLimit: timeLimit,
		quit:      make(chan struct{}),
	}
}

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

func (fp *FilePool) cleanupIdleFiles() {
	fp.mtx.RLock()
	var toClose []string
	for path, file := range fp.files {
		if file.GetTimeSinceAccessed() >= fp.timeLimit && !file.IsDirty() {
			toClose = append(toClose, path)
		}
	}
	fp.mtx.RUnlock()

	for _, path := range toClose {
		_ = fp.Close(path) // ignore close error; Close returns ErrCouldNotCloseFile in that case
	}
}

func (fp *FilePool) Close(path string) error {
	fp.mtx.Lock()
	defer fp.mtx.Unlock()

	f, exists := fp.files[path]
	if !exists {
		return ErrFileNotFoundInPool
	}
	if err := f.Close(); err != nil {
		return ErrCouldNotCloseFile
	}
	delete(fp.files, path)
	return nil
}

func (fp *FilePool) Stop() error {
	close(fp.quit)
	return fp.closeAll()
}

func (fp *FilePool) closeAll() error {
	fp.mtx.Lock()
	defer fp.mtx.Unlock()
	var firstErr error
	for path, file := range fp.files {
		if err := file.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(fp.files, path)
	}
	return firstErr
}
