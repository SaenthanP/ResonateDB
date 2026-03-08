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

	writer        *walWriter
	activeSegment *activeSegment
	segments      []*Segment
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

type walWriter struct {
	activeQueue    chan *walRequest
	flushQueue     []*walRequest
	flushTicker    *time.Ticker
	maxSegmentSize int64
	batchSize      int
}

func NewWal(cfg WalConfig) (*Wal, error) {
	activeQueue := make(chan *walRequest, 4096)
	flushQueue := []*walRequest{}

	flushDuration := time.Second * 60
	flushTicker := time.NewTicker(flushDuration)

	// Check if segments directory exists, if not, make one
	err := os.MkdirAll(cfg.Dir, 0755)
	if err != nil {
		return nil, err
	}
	// files, err := scanSegments(cfg.Dir)
	// if err != nil {
	// 	return nil, err
	// }

	// if len(files) == 0 {
	// 	// os.
	// }
	// segment := Segment{
	// 	id:         0,
	// 	path:       "",
	// 	startIndex: 0,
	// 	endIndex:   0,
	// 	size:       0,
	// }

	walWriter := walWriter{
		activeQueue: activeQueue,
		flushQueue:  flushQueue,
		flushTicker: flushTicker,
	}

	wal := &Wal{
		dir:    cfg.Dir,
		writer: new(walWriter),
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

func (w *Wal) newSegment(id uint64, startIndex int64) (*Segment, error) {
	fileName := fmt.Sprintf("segment-%020d.log", id)
	path := filepath.Join(w.dir, fileName)
	fd, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	defer fd.Close()

	seg := Segment{
		id:         id,
		path:       fileName,
		startIndex: uint64(startIndex),
		size:       0,
	}
	return new(seg), nil
}
func (w *Wal) rotateSegment() error {
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
		defer fd.Close()
		info, _ := fd.Stat()

		var startIndex, endIndex uint64
		size := info.Size()
		first := true
		for {
			_, err := fd.Seek(0, io.SeekCurrent)
			if err != nil {
				return err
			}

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

// func (wal *Wal) Append(data []byte) (index uint64, err error) {

// }
