package cluster

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/saenthan/resonatedb/proto-gen/cluster"
)

type PingReqResult struct {
	Ack *cluster.Ack
	Err error
}

func (n *Node) Ping(ctx context.Context) {
	peer := n.getNodeToPing()
	if peer == nil {
		fmt.Println("Peer not available to Ping")
		return
	}
	updates := n.toProtoUpdates()
	req := cluster.Ping{
		From:    n.Address,
		Updates: updates,
	}

	// TODO add a timeout here that is predetermined
	ack, err := peer.Client.PingNode(ctx, &req)
	if err == nil {
		n.mergeUpdates(ack.Updates)
		return
	}

}

func (n *Node) PingReq(ctx context.Context, target string) []error {
	peers := n.getKNodesToPing()
	ctx, cancel := context.WithTimeout(ctx, time.Duration(2*time.Minute))
	defer cancel()

	updates := n.toProtoUpdates()
	var wg sync.WaitGroup

	resultChan := make(chan PingReqResult, len(peers))
	for _, peer := range peers {
		wg.Add(1)

		go func(peer *Peer) {
			defer wg.Done()
			ack, err := peer.Client.PingReqNode(ctx, &cluster.PingReq{
				From:    n.Address,
				Target:  target,
				Updates: updates,
			})
			resultChan <- PingReqResult{
				Ack: ack,
				Err: err,
			}
		}(peer)
	}

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	var errors []error
	for res := range resultChan {
		if res.Err != nil {
			errors = append(errors, res.Err)
		} else {
			n.mergeUpdates(res.Ack.Updates)
			cancel()
		}
	}
	
	n.markSuspect(target)
	return errors
}
