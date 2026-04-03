package cluster

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"slices"
)

type Ring struct {
	vnodes      []uint32
	vnodeToAddr map[uint32]string
	vnodeCount  uint32
	addresses   map[string]struct{}
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



func hash(addr string) uint32 {
	data := sha256.Sum256([]byte(addr))
	return binary.LittleEndian.Uint32(data[:4])
}
