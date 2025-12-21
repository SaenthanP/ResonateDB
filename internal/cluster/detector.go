package cluster

import (
	"context"
	"time"
)

const (
	PingInterval = 1 * time.Second
	PingTimeout  = 300 * time.Millisecond
)

func (n *Node) StartFailureDetector(ctx context.Context) {
	ticker := time.NewTicker(PingInterval)

	go func() {
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				n.runPingRound(ctx)
			case <-ctx.Done():
				return
			}
		}
	}()
}

func (n *Node) runPingRound(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, PingTimeout)
	defer cancel()

	n.Ping(ctx)
}
