package cluster

import (
	"context"
	"fmt"
	"time"
)

const (
	PingInterval = 1 * time.Second
	PingTimeout  = 300 * time.Millisecond
)

func (n *Node) StartFailureDetector(ctx context.Context) {
	ticker := time.NewTicker(PingInterval)
	suspectNodeCleanupTicker := time.NewTicker(500 * time.Millisecond)

	go func() {
		defer ticker.Stop()
		defer suspectNodeCleanupTicker.Stop()

		for {
			select {
			case <-ticker.C:
				n.runPingRound(ctx)
			case <-suspectNodeCleanupTicker.C:
				n.runSuspectNodeCleanup(ctx)
			case <-ctx.Done():
				return
			}
		}
	}()
}

func (n *Node) runSuspectNodeCleanup(ctx context.Context) {
	for addr, update := range n.updates {
		if addr == n.Address {
			continue
		}

		if update.State == Suspect {
			if time.Since(update.SuspectedAt) > n.SuspectTimeout {
				fmt.Printf("Node %s failed to refute suspicion in time, marking as Fail\n", addr)
				update.State = Fail
				n.updates[addr] = update
			}
		}
	}
}
func (n *Node) runPingRound(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, PingTimeout)
	defer cancel()

	n.Ping(ctx)
}
