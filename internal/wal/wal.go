package wal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type Wal struct {
	dir string

	activeSegment  *activeSegment
	segments       []*Segment
	activeQueue    chan *walRequest
	flushQueue     []*walRequest
	flushTicker    *time.Ticker
	maxSegmentSize int64
	batchSize      int
	done           chan struct{}
}

type Segment struct {
	id uint64

	path       string
	startIndex uint64
	endIndex   uint64
	size       int64
}

type activeSegment struct {
	fd *os.File
	*Segment
}

type WalEntry struct {
	Index uint64
	Type  uint8
	Value []byte
	CRC   uint32
}

type walRequest struct {
	entry *WalEntry
	done  chan error
}

type WalConfig struct {
	Dir string
}

func NewWal(cfg WalConfig) (*Wal, error) {
	err := os.MkdirAll(cfg.Dir, 0755)
	if err != nil {
		return nil, err
	}

	wal := &Wal{
		dir:         cfg.Dir,
		activeQueue: make(chan *walRequest, 4096),
		flushQueue:  make([]*walRequest, 0, 256),
		flushTicker: time.NewTicker(time.Second * 60),
		done:        make(chan struct{}),
	}

	err = wal.loadSegments()
	if err != nil {
		return nil, err
	}

	if wal.activeSegment == nil {
		err = wal.rotateSegment()
		if err != nil {
			return nil, err
		}
	}

	return wal, nil
}

func (w *Wal) Run() {
	for {
		select {
		case req := <-w.activeQueue:
			w.flushQueue = append(w.flushQueue, req)
			if len(w.flushQueue) >= w.batchSize {
				w.flush()
				w.flushQueue = w.flushQueue[:0]
			}
		case <-w.flushTicker.C:
			w.flush()
			w.flushQueue = w.flushQueue[:0]
		case <-w.done:
			w.flush()
			w.flushTicker.Stop()
			w.activeSegment.fd.Close()
			return
		}
	}
}

func (w *Wal) Close() {
	close(w.done)
}
func (w *Wal) appendEntry(entry *WalEntry) error {
	req := &walRequest{
		entry: entry,
		done:  make(chan error),
	}
	w.activeQueue <- req
	return <-req.done
}

func (w *Wal) flush() {
	batch := w.flushQueue
	if len(batch) == 0 {
		return
	}

	errFunc := func(batch []*walRequest, err error) {
		for _, r := range batch {
			r.done <- err
		}
	}

	buf := bytes.NewBuffer(nil)
	for _, req := range batch {
		data, err := encodeEntry(req.entry)
		if err != nil {
			errFunc(batch, err)
			return
		}
		_, err = buf.Write(data)
		if err != nil {
			errFunc(batch, err)
			return
		}
		if req.entry.Index > w.activeSegment.endIndex {
			w.activeSegment.endIndex = req.entry.Index
		}
	}

	bufLen, err := w.activeSegment.fd.Write(buf.Bytes())
	if err == nil {
		err = w.activeSegment.fd.Sync()
	}
	if err == nil {
		w.activeSegment.size += int64(bufLen)
	}
	if err == nil && w.activeSegment.size >= w.maxSegmentSize {
		err = w.rotateSegment()
	}
	errFunc(batch, err)
}

func scanSegmentFiles(dir string) ([]string, error) {
	var files []string
	entries, err := os.ReadDir(dir)
	if err != nil {
		return []string{}, err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if strings.HasSuffix(entry.Name(), ".log") && strings.HasPrefix(entry.Name(), "segment-") {
			files = append(files, filepath.Join(dir, entry.Name()))
		}
	}
	return files, nil
}

func (w *Wal) rotateSegment() error {
	var nextID uint64
	var startIndex uint64

	if len(w.segments) == 0 {
		nextID = 0
		startIndex = 0
	} else {
		last := w.segments[len(w.segments)-1]
		nextID = last.id + 1
		startIndex = last.endIndex + 1
	}

	fileName := fmt.Sprintf("segment-%020d.log", nextID)
	path := filepath.Join(w.dir, fileName)

	fd, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return err
	}

	seg := &Segment{
		id:         nextID,
		path:       path,
		startIndex: startIndex,
		endIndex:   startIndex,
		size:       0,
	}

	if w.activeSegment != nil {
		if err = w.activeSegment.fd.Close(); err != nil {
			return err
		}
	}
	w.segments = append(w.segments, seg)

	w.activeSegment = &activeSegment{
		fd:      fd,
		Segment: seg,
	}
	return nil
}

func parseSegmentID(path string) (uint64, error) {
	name := filepath.Base(path)
	name = strings.TrimPrefix(name, "segment-")
	name = strings.TrimSuffix(name, ".log")
	return strconv.ParseUint(name, 10, 64)
}

func (w *Wal) loadSegments() error {
	filePaths, err := scanSegmentFiles(w.dir)
	if err != nil {
		return err
	}

	var segments []*Segment
	for _, path := range filePaths {
		fd, err := os.OpenFile(path, os.O_RDWR, 0644)
		if err != nil {
			return err
		}
		info, err := fd.Stat()
		if err != nil {
			return err
		}

		var startIndex, endIndex uint64
		size := info.Size()
		first := true

		for {
			walEntry, err := decodeEntry(fd)
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}

			if first && walEntry != nil {
				startIndex = walEntry.Index
				first = false
			}

			endIndex = walEntry.Index
		}

		id, err := parseSegmentID(path)
		if err != nil {
			return err
		}
		segments = append(segments, &Segment{
			id:         id,
			path:       path,
			startIndex: startIndex,
			endIndex:   endIndex,
			size:       int64(size),
		})
		if err = fd.Close(); err != nil {
			return err
		}
	}

	w.segments = segments
	if len(segments) > 0 {
		segment := segments[len(segments)-1]

		fd, err := os.OpenFile(segment.path, os.O_APPEND|os.O_RDWR, 0644)
		if err != nil {
			return err
		}
		w.activeSegment = &activeSegment{
			fd:      fd,
			Segment: segment,
		}
	}

	return nil
}

func encodeEntry(entry *WalEntry) ([]byte, error) {
	buf := bytes.NewBuffer(nil)

	err := binary.Write(buf, binary.LittleEndian, entry.Index)
	if err != nil {
		return nil, err
	}

	err = binary.Write(buf, binary.LittleEndian, entry.Type)
	if err != nil {
		return nil, err
	}

	valLen := uint32(len(entry.Value))
	err = binary.Write(buf, binary.LittleEndian, valLen)
	if err != nil {
		return nil, err
	}

	_, err = buf.Write(entry.Value)
	if err != nil {
		return nil, err
	}

	payload := buf.Bytes()

	crc := crc32.ChecksumIEEE(payload)
	output := bytes.NewBuffer(nil)
	err = binary.Write(output, binary.LittleEndian, crc)
	if err != nil {
		return nil, err
	}
	_, err = output.Write(payload)
	if err != nil {
		return nil, err
	}

	return output.Bytes(), nil
}

/*
Header: CRC (4 bytes)| Index (8 bytes)| Type (1 byte)| Length (4 bytes)| Data (N Bytes)
*/
func decodeEntry(r io.Reader) (*WalEntry, error) {
	var crc uint32
	err := binary.Read(r, binary.LittleEndian, &crc)
	if err != nil {
		return nil, err
	}

	var index uint64
	err = binary.Read(r, binary.LittleEndian, &index)
	if err != nil {
		return nil, err
	}

	var entryType uint8
	err = binary.Read(r, binary.LittleEndian, &entryType)
	if err != nil {
		return nil, err
	}

	var length uint32
	err = binary.Read(r, binary.LittleEndian, &length)
	if err != nil {
		return nil, err
	}

	buf := make([]byte, length)
	_, err = io.ReadFull(r, buf)
	if err != nil {
		return nil, err
	}

	entry := WalEntry{
		Index: index,
		Type:  entryType,
		Value: buf,
		CRC:   crc,
	}

	crcBuf := bytes.NewBuffer(nil)
	err = binary.Write(crcBuf, binary.LittleEndian, entry.Index)
	if err != nil {
		return nil, err
	}

	err = binary.Write(crcBuf, binary.LittleEndian, entry.Type)
	if err != nil {
		return nil, err
	}

	err = binary.Write(crcBuf, binary.LittleEndian, length)
	if err != nil {
		return nil, err
	}

	err = binary.Write(crcBuf, binary.LittleEndian, entry.Value)
	if err != nil {
		return nil, err
	}

	if crc32.ChecksumIEEE(crcBuf.Bytes()) != entry.CRC {
		return nil, fmt.Errorf("checksum mismatch at index %d", entry.Index)
	}
	return &entry, nil
}
