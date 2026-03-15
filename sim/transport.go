package sim

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/saenthan/resonatedb/internal/cluster"
)

type SimTransport struct {
	mu         sync.Mutex
	nodes      map[string]*cluster.Node
	partitions map[[2]string]bool
}

func NewSimTransport() *SimTransport {
	return &SimTransport{
		nodes:      make(map[string]*cluster.Node),
		partitions: make(map[[2]string]bool),
	}
}

func (t *SimTransport) Register(addr string, node *cluster.Node) {
	t.nodes[addr] = node
}

func (t *SimTransport) Ping(ctx context.Context, to string, req cluster.PingRequest) (cluster.PingResponse, error) {
	t.mu.Lock()
	partitioned := t.isPartitioned(req.From, to)
	t.mu.Unlock()

	if partitioned {
		return cluster.PingResponse{}, errors.New("partitioned")
	}

	node, ok := t.nodes[to]
	if !ok {
		return cluster.PingResponse{}, fmt.Errorf("unknown node: %s", to)
	}

	return node.HandlePing(ctx, req.From, req.Updates)
}

func (t *SimTransport) Partition(a, b string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.partitions[[2]string{a, b}] = true
	t.partitions[[2]string{b, a}] = true
}

func (t *SimTransport) Heal(a, b string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.partitions, [2]string{a, b})
	delete(t.partitions, [2]string{b, a})
}

func (t *SimTransport) PingReq(ctx context.Context, to string, req cluster.PingRequest) (cluster.PingResponse, error) {
	t.mu.Lock()
	partitioned := t.isPartitioned(req.From, to)
	t.mu.Unlock()

	if partitioned {
		return cluster.PingResponse{}, errors.New("partitioned")
	}

	node, ok := t.nodes[to]
	if !ok {
		return cluster.PingResponse{}, fmt.Errorf("unknown node: %s", to)
	}

	return node.HandlePingReq(ctx, req.From, req.Target, req.Updates)
}

func (t *SimTransport) isPartitioned(a, b string) bool {
	return t.partitions[[2]string{a, b}]
}
