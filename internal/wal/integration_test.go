package wal_test

import (
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/saenthan/resonatedb/internal/wal"
	"github.com/saenthan/resonatedb/sim"
	"github.com/stretchr/testify/require"
)

type simFSWrapper struct{ inner *sim.MemFS }

func (w *simFSWrapper) OpenFile(path string, flag int, perm os.FileMode) (wal.File, error) {
	return w.inner.OpenFile(path, flag, perm)
}
func (w *simFSWrapper) ReadDir(dir string) ([]os.DirEntry, error) {
	return w.inner.ReadDir(dir)
}
func (w *simFSWrapper) MkdirAll(path string, perm os.FileMode) error {
	return w.inner.MkdirAll(path, perm)
}
func (w *simFSWrapper) Remove(path string) error { return w.inner.Remove(path) }

func newTestWal(t *testing.T, fs *sim.MemFS, clk wal.Clock, maxSegSize int64) *wal.Wal {
	t.Helper()
	w, err := wal.NewWal(wal.WalConfig{
		Dir:            "/wal",
		Clock:          clk,
		FileSystem:     &simFSWrapper{inner: fs},
		MaxSegmentSize: maxSegSize,
		BatchSize:      1,
	})
	require.NoError(t, err)
	return w
}

func readAllEntries(t *testing.T, w *wal.Wal, fs *sim.MemFS) []*wal.WalEntry {
	t.Helper()
	files, err := wal.ScanSegmentFiles(w)
	require.NoError(t, err)

	var entries []*wal.WalEntry
	for _, path := range files {
		f, err := (&simFSWrapper{inner: fs}).OpenFile(path, os.O_RDONLY, 0)
		require.NoError(t, err)
		for {
			entry, err := wal.DecodeEntry(f)
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			entries = append(entries, entry)
		}
	}
	return entries
}

func TestWAL_MultipleAppendsAccumulate(t *testing.T) {
	fs := sim.NewMemFS()
	clk := sim.NewSimClock(0)

	w := newTestWal(t, fs, clk, 4096)
	go w.Run()
	defer w.Close()

	written := []*wal.WalEntry{
		{Index: 0, Type: 1, Value: []byte("a1")},
		{Index: 1, Type: 1, Value: []byte("a2")},
		{Index: 2, Type: 1, Value: []byte("a3")},
	}
	for _, e := range written {
		require.NoError(t, w.Append(e))
	}

	entries := readAllEntries(t, w, fs)
	require.Len(t, entries, len(written))
	for i, e := range entries {
		require.Equal(t, written[i].Index, e.Index)
		require.Equal(t, written[i].Value, e.Value)
	}
}

func TestWal_AppendAfterReload(t *testing.T) {
	fs := sim.NewMemFS()
	clk := sim.NewSimClock(0)

	w := newTestWal(t, fs, clk, 4096)
	go w.Run()

	written := []*wal.WalEntry{
		{Index: 0, Type: 1, Value: []byte("a1")},
		{Index: 1, Type: 1, Value: []byte("a2")},
		{Index: 2, Type: 1, Value: []byte("a3")},
	}

	for _, e := range written {
		require.NoError(t, w.Append(e))
	}
	w.Close()

	w2 := newTestWal(t, fs, clk, 4096)
	written2 := []*wal.WalEntry{
		{Index: 3, Type: 1, Value: []byte("b1")},
		{Index: 4, Type: 1, Value: []byte("b2")},
		{Index: 5, Type: 1, Value: []byte("b3")},
	}

	go w2.Run()
	defer w2.Close()
	for _, e := range written2 {
		require.NoError(t, w2.Append(e))
	}

	totalWritten := append(written, written2...)
	entries := readAllEntries(t, w2, fs)
	require.Len(t, entries, len(totalWritten))
	for i, e := range entries {
		require.Equal(t, totalWritten[i].Index, e.Index)
		require.Equal(t, totalWritten[i].Value, e.Value)
	}
}

func TestWal_WriteFailure(t *testing.T) {
	fs := sim.NewMemFS()
	clk := sim.NewSimClock(0)

	w := newTestWal(t, fs, clk, 4096)
	go w.Run()
	defer w.Close()

	require.NoError(t, w.Append(&wal.WalEntry{Index: 0, Value: []byte("ok")}))

	files, err := wal.ScanSegmentFiles(w)
	require.NoError(t, err)
	require.Len(t, files, 1)


	f, err := fs.OpenFile(files[0], os.O_RDONLY, 0)
	require.NoError(t, err)
	f.FailAfterByteCount = 1 

	require.Error(t, w.Append(&wal.WalEntry{Index: 1, Value: []byte("fail")}))
}

func TestWal_SyncFailure(t *testing.T) {
	fs := sim.NewMemFS()
	clk := sim.NewSimClock(0)

	w := newTestWal(t, fs, clk, 4096)
	go w.Run()
	defer w.Close()

	require.NoError(t, w.Append(&wal.WalEntry{Index: 0, Value: []byte("ok")}))

	files, err := wal.ScanSegmentFiles(w)
	require.NoError(t, err)
	require.Len(t, files, 1)

	f, err := fs.OpenFile(files[0], os.O_RDONLY, 0)
	require.NoError(t, err)
	f.FailNextFSync = true

	require.Error(t, w.Append(&wal.WalEntry{Index: 1, Value: []byte("fail")}))
}

func TestWal_SegmentRotation(t *testing.T) {
	fs := sim.NewMemFS()
	clk := sim.NewSimClock(0)

	w := newTestWal(t, fs, clk, 50)
	go w.Run()
	defer w.Close()

	var written []*wal.WalEntry
	for i := range uint64(10) {
		entry := &wal.WalEntry{Index: i, Value: fmt.Append(nil, i)}
		written = append(written, entry)
		require.NoError(t, w.Append(entry))
	}

	files, err := wal.ScanSegmentFiles(w)
	require.NoError(t, err)
	require.Greater(t, len(files), 1)

	entries := readAllEntries(t, w, fs)
	require.Len(t, entries, len(written))
	for i, e := range entries {
		require.Equal(t, written[i].Index, e.Index)
		require.Equal(t, written[i].Value, e.Value)
	}
}
