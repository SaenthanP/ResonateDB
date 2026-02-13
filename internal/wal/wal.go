package wal

import (
	"bytes"
	"os"
	"strings"
	"time"
)

type Wal struct {
	dir string

	writer  *walWriter
	segment []*Segment
}

type Segment struct {
	id         uint64
	path       string
	startIndex uint64
	endIndex   uint64
	size       uint64
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
	reqs chan *walRequest

	activeQueue   *[]bytes.Buffer
	flushQueue    *[]bytes.Buffer
	flushDuration *time.Ticker
}

func NewWal(cfg WalConfig) (*Wal, error) {
	reqsChan := make(chan *walRequest, 4096)
	activeQueue := make([]bytes.Buffer, 4<<20)
	flushQueue := make([]bytes.Buffer, 4<<20)

	flushDuration := time.Second * 60
	flushTicker := time.NewTicker(flushDuration)

	// Check if segments directory exists, if not, make one
	err := os.MkdirAll(cfg.Dir, 0755)
	if err != nil {
		return nil, err
	}

	// segment := Segment{
	// 	id:         0,
	// 	path:       "",
	// 	startIndex: 0,
	// 	endIndex:   0,
	// 	size:       0,
	// }

	walWriter := walWriter{
		reqs:          reqsChan,
		activeQueue:   new(activeQueue),
		flushQueue:    new(flushQueue),
		flushDuration: flushTicker,
	}

	wal := Wal{
		dir:    cfg.Dir,
		writer: new(walWriter),
	}

	return new(wal), nil
}
func scanSegments(dir string) ([]string, error) {
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
			files = append(files, entry.Name())
		}
	}
	return files, nil
}

// func loadSegments(files []string) ([]Segment, error) {

// }

// func (wal *Wal) Append(data []byte) (index uint64, err error) {

// }
