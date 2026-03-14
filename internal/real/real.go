package real

import (
	"os"
	"time"

	"github.com/saenthan/resonatedb/internal/wal"
)

type RealFS struct{}

func (fs *RealFS) OpenFile(path string, flag int, perm os.FileMode) (wal.File, error) {
	return os.OpenFile(path, flag, perm)
}

func (fs *RealFS) ReadDir(dir string) ([]os.DirEntry, error) { return os.ReadDir(dir) }

func (fs *RealFS) MkdirAll(path string, perm os.FileMode) error { return os.MkdirAll(path, perm) }

func (fs *RealFS) Remove(path string) error { return os.Remove(path) }

type RealClock struct{}

func (RealClock) Now() time.Time                         { return time.Now() }
func (RealClock) NewTicker(d time.Duration) *time.Ticker { return time.NewTicker(d) }
