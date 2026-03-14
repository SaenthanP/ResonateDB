package sim

import (
	"errors"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/saenthan/resonatedb/internal/wal"
)

type RealFS struct{}

func (fs *RealFS) OpenFile(path string, flag int, perm os.FileMode) (wal.File, error) {
	return os.OpenFile(path, flag, perm)
}

func (fs *RealFS) ReadDir(dir string) ([]os.DirEntry, error) {
	return os.ReadDir(dir)
}

func (fs *RealFS) MkdirAll(path string, perm os.FileMode) error {
	return os.MkdirAll(path, perm)
}

func (fs *RealFS) Remove(path string) error {
	return os.Remove(path)
}

type memFileInfo struct {
	name string
	size int64
}

func (fi memFileInfo) Name() string       { return fi.name }
func (fi memFileInfo) Size() int64        { return fi.size }
func (fi memFileInfo) Mode() os.FileMode  { return 0644 }
func (fi memFileInfo) ModTime() time.Time { return time.Time{} }
func (fi memFileInfo) IsDir() bool        { return false }
func (fi memFileInfo) Sys() any           { return nil }

type MemFile struct {
	mu   sync.Mutex
	name string
	data []byte
	pos  int

	// May move this out of this struct
	FailAfterByteCount int64
	FailNextFSync      bool
	bytesWritten       int64
}

func (f *MemFile) Read(b []byte) (int, error) {
	if f.pos >= len(f.data) {
		return 0, io.EOF
	}

	copyCount := copy(b, f.data[f.pos:])
	f.pos += copyCount
	return copyCount, nil
}

func (f *MemFile) Write(b []byte) (int, error) {
	if f.FailAfterByteCount > 0 && f.bytesWritten >= f.FailAfterByteCount {
		return 0, errors.New("test: write failiure")
	}

	f.data = append(f.data, b...)
	f.bytesWritten += int64(len(b))
	return len(b), nil
}

func (f *MemFile) Sync() error {
	if f.FailNextFSync {
		f.FailNextFSync = false
		return errors.New("test: sync failiure")
	}
	return nil
}

func (f *MemFile) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		f.pos = int(offset)
	case io.SeekCurrent:
		f.pos += int(offset)
	case io.SeekEnd:
		f.pos = len(f.data) + int(offset)
	}
	return int64(f.pos), nil
}

func (f *MemFile) Stat() (os.FileInfo, error) {
	return memFileInfo{
		name: f.name,
		size: int64(len(f.data)),
	}, nil
}
func (f *MemFile) Name() string { return f.name }
func (f *MemFile) Close() error { return nil }

type memDirEntry struct {
	info memFileInfo
}

func (e memDirEntry) Name() string               { return e.info.Name() }
func (e memDirEntry) IsDir() bool                { return false }
func (e memDirEntry) Type() os.FileMode          { return 0 }
func (e memDirEntry) Info() (os.FileInfo, error) { return e.info, nil }

type MemFS struct {
	mu    sync.Mutex
	files map[string]*MemFile
}

func NewMemFS() *MemFS {
	return &MemFS{files: make(map[string]*MemFile)}
}

func (fs *MemFS) OpenFile(path string, flag int, perm os.FileMode) (wal.File, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	f, exists := fs.files[path]
	if !exists {
		if flag&os.O_CREATE == 0 {
			return nil, &os.PathError{Op: "open", Path: path, Err: os.ErrNotExist}
		}
		f = &MemFile{name: path}
		fs.files[path] = f
	}

	if flag&os.O_APPEND != 0 {
		f.pos = len(f.data)
	} else {
		f.pos = 0
	}
	return f, nil
}

func (fs *MemFS) ReadDir(dir string) ([]os.DirEntry, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	prefix := dir + "/"
	var entries []os.DirEntry
	for path, f := range fs.files {
		if strings.HasPrefix(path, prefix) {
			entries = append(entries, memDirEntry{info: memFileInfo{name: f.name, size: int64(len(f.data))}})
		}
	}
	return entries, nil
}

func (fs *MemFS) MkdirAll(path string, perm os.FileMode) error { return nil }

func (fs *MemFS) Remove(path string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if _, exists := fs.files[path]; !exists {
		return &os.PathError{Op: "remove", Path: path, Err: os.ErrNotExist}
	}
	delete(fs.files, path)
	return nil
}
