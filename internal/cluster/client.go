package cluster

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type PingReqResult struct {
	Ack PingResponse
	Err error
}

func (n *Node) Ping(ctx context.Context) {
	if len(n.Peers) == 0 {
		fmt.Println("no peers")
		return
	}

	peerAddress, peer := n.getNodeToPing()
	fmt.Printf("Start pinging node: %s\n", peerAddress)
	if peer == nil {
		fmt.Println("Peer not available to Ping")
		return
	}
	n.addToUpdate(peerAddress)
	req := PingRequest{
		From:    n.Address,
		Updates: n.updates,
	}

	// TODO add a timeout here that is predetermined
	ack, err := peer.Client.Ping(ctx, req)
	if err == nil {
		peer.State = Alive
		n.markAlive(peerAddress)
		// fmt.Println("reach success, ")
		n.mergeUpdates(ack.Updates)
		return
	}

	pingReqErr := n.PingReq(ctx, peerAddress)
	fmt.Println(pingReqErr)
}

func (n *Node) PingReq(ctx context.Context, target string) []error {
	fmt.Printf("Start pinging req node: %s\n", target)

	peers := n.getKNodesToPing()
	n.addToUpdate(target)
	ctx, cancel := context.WithTimeout(ctx, time.Duration(2*time.Minute))
	defer cancel()

	var wg sync.WaitGroup

	resultChan := make(chan PingReqResult, len(peers))
	for _, peer := range peers {
		wg.Add(1)

		go func(peer *Peer) {
			defer wg.Done()
			ack, err := peer.Client.PingReq(ctx, target, PingRequest{
				From:    n.Address,
				Updates: n.updates,
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
			n.markAlive(target)
			n.mergeUpdates(res.Ack.Updates)
			return nil
		}
	}

	n.markSuspect(target)
	return errors
}
