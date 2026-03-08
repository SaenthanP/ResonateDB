package wal

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWalEncodeDecode(t *testing.T) {
	originalEntry := WalEntry{
		Index: 42,
		Type:  1,
		Value: []byte("testing"),
	}

	encoded, err := encodeEntry(&originalEntry)
	require.NoError(t, err)

	decodedEntry, err := decodeEntry(bytes.NewReader(encoded))
	require.NoError(t, err)
	require.NotNil(t, decodedEntry)

	require.Equal(t, originalEntry.Index, decodedEntry.Index)
	require.Equal(t, originalEntry.Type, decodedEntry.Type)
	require.Equal(t, originalEntry.Value, decodedEntry.Value)
}

func TestWalDecodeCorruptedCRC(t *testing.T) {
	originalEntry := WalEntry{
		Index: 42,
		Type:  1,
		Value: []byte("testing"),
	}

	encoded, err := encodeEntry(&originalEntry)
	require.NoError(t, err)

	// index 5 because it is after crc part of header
	encoded[5] = 0x3F
	_, err = decodeEntry(bytes.NewReader(encoded))
	require.Error(t, err)
}

func TestWalSegmentIDParse(t *testing.T) {
	cases := []struct {
		path string
		want uint64
		ok   bool
	}{
		{"/wal/segment-00000000000000000001.log", 1, true},
		{"/wal/segment-00000000000000000042.log", 42, true},
		{"/wal/segment-00000000000000000000.log", 0, true},
		{"/wal/notasegment.log", 0, false},
	}
	for _, c := range cases {
		id, err := parseSegmentID(c.path)
		if c.ok && err != nil {
			t.Errorf("parseSegmentID(%s): unexpected error: %v", c.path, err)
		}
		if !c.ok && err == nil {
			t.Errorf("parseSegmentID(%s): expected error, got id=%d", c.path, id)
		}
		if c.ok && id != c.want {
			t.Errorf("parseSegmentID(%s): got %d want %d", c.path, id, c.want)
		}
	}
}

func writeSegmentFile(t *testing.T, path string, entries []*WalEntry) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	for _, e := range entries {
		b, err := encodeEntry(e)
		require.NoError(t, err)

		_, err = f.Write(b)
		require.NoError(t, err)
	}
}

func TestLoadSegments_Empty(t *testing.T) {
	dir := t.TempDir()
	w := &Wal{dir: dir}
	if err := w.loadSegments(); err != nil {
		t.Fatalf("loadSegments: %v", err)
	}
	if len(w.segments) != 0 {
		t.Errorf("expected 0 segments, got %d", len(w.segments))
	}
	if w.activeSegment != nil {
		t.Error("expected nil activeSegment")
	}
}

func TestLoadSegments_SingleSegment(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "segment-00000000000000000001.log")

	entries := []*WalEntry{
		{Index: 10, Type: 1, Value: []byte("a")},
		{Index: 11, Type: 1, Value: []byte("b")},
		{Index: 12, Type: 1, Value: []byte("c")},
	}

	var totalSize int64
	for _, e := range entries {
		b, err := encodeEntry(e)
		require.NoError(t, err)
		totalSize += int64(len(b))
	}

	writeSegmentFile(t, path, entries)

	wal := &Wal{dir: dir}

	err := wal.loadSegments()
	require.NoError(t, err)

	require.Len(t, wal.segments, 1)

	segment := wal.segments[0]
	require.NotNil(t, segment)

	require.Equal(t, segment.id, uint64(1))
	require.Equal(t, segment.path, path)
	require.Equal(t, segment.size, totalSize)
	require.Equal(t, segment.startIndex, uint64(10))
	require.Equal(t, segment.endIndex, uint64(12))

	require.Equal(t, wal.activeSegment.path, segment.path)
}

func TestLoadSegments_MultipleSegments_LastIsActive(t *testing.T) {
	dir := t.TempDir()

	writeSegmentFile(t, filepath.Join(dir, "segment-00000000000000000001.log"), []*WalEntry{
		{Index: 1, Type: 1, Value: []byte("x")},
	})
	writeSegmentFile(t, filepath.Join(dir, "segment-00000000000000000002.log"), []*WalEntry{
		{Index: 2, Type: 1, Value: []byte("y")},
	})

	w := &Wal{dir: dir}
	err := w.loadSegments()
	require.Nil(t, err)

	require.Len(t, w.segments, 2)

	require.Equal(t, w.activeSegment.id, uint64(2))
}

/*
CRC
segment id parse
write segment file hlper
load segment empty
load single segment
*/
