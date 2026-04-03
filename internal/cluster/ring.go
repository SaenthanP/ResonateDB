package cluster

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"slices"
	"sort"
	"sync"
)

type Ring struct {
	vnodes      []uint32
	vnodeToAddr map[uint32]string
	vnodeCount  uint32
	addresses   map[string]struct{}
	mu          sync.RWMutex
}

func NewRing(vnodeCount uint32) *Ring {
	count := 0

	if vnodeCount == 0 {
		count = 200
	}
	return &Ring{
		vnodeToAddr: make(map[uint32]string),
		vnodeCount:  uint32(count),
		addresses:   make(map[string]struct{}),
	}
}

func (r *Ring) AddNode(addr string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.addresses[addr]; exists {
		return
	}

	for i := range r.vnodeCount {
		vnode := hash(fmt.Sprintf("%s:%d", addr, i))
		r.vnodeToAddr[vnode] = addr
		r.vnodes = append(r.vnodes, vnode)
	}
	r.addresses[addr] = struct{}{}

	slices.Sort(r.vnodes)
}

func (r *Ring) RemoveNode(addr string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.addresses[addr]; !exists {
		return
	}

	var filteredVNodes []uint32

	for _, v := range r.vnodes {
		if r.vnodeToAddr[v] != addr {
			filteredVNodes = append(filteredVNodes, v)
		} else {
			delete(r.vnodeToAddr, v)
		}
	}
	delete(r.addresses, addr)
	r.vnodes = filteredVNodes
}
func (r *Ring) GetNodes(key string, n uint32) []string {
	hash := hash(key)
	seen := make(map[string]struct{})
	records := make([]string, 0, n)

	r.mu.RLock()
	defer r.mu.RUnlock()

	// find the first position the value belongs to to prevent scanning the whole ring
	idx := sort.Search(len(r.vnodes), func(i int) bool { return r.vnodes[i] >= hash })

	for i := 0; len(records) < int(n) && i < int(r.vnodeCount); i++ {
		index := (idx + i) % int(r.vnodeCount)
		addr := r.vnodeToAddr[r.vnodes[index]]
		if _, exists := seen[addr]; !exists {
			seen[addr] = struct{}{}
			records = append(records, addr)
		}
	}
	return records
}

func hash(addr string) uint32 {
	data := sha256.Sum256([]byte(addr))
	return binary.LittleEndian.Uint32(data[:4])
}
