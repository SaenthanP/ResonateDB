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
	if len(n.updates) == 0 {
		fmt.Println("no peers")
		return
	}

	targetNodeAddr := n.getNodeToPing()
	if targetNodeAddr == "" {
		fmt.Println("Peer not available to Ping")
		return
	}
	fmt.Printf("Start pinging node: %s\n", targetNodeAddr)

	req := PingRequest{
		From:    n.Address,
		Updates: n.updates,
	}

	resp, err := n.Transport.Ping(ctx, targetNodeAddr, req)
	if err == nil {
		n.markAlive(targetNodeAddr)
		n.mergeUpdates(resp.Updates)
		return
	}

	pingReqErr := n.PingReq(ctx, targetNodeAddr)
	fmt.Println(pingReqErr)
}

func (n *Node) PingReq(ctx context.Context, targetAddr string) []error {
	fmt.Printf("Start pinging req node: %s\n", targetAddr)

	peers := n.getKNodesToPing()

	ctx, cancel := context.WithTimeout(ctx, time.Duration(2*time.Minute))
	defer cancel()

	var wg sync.WaitGroup

	resultChan := make(chan PingReqResult, len(peers))
	for _, peerAddr := range peers {
		wg.Add(1)

		go func(peerAddr string) {
			defer wg.Done()
			resp, err := n.Transport.PingReq(ctx, targetAddr, PingRequest{
				From:    n.Address,
				Updates: n.updates,
			})
			resultChan <- PingReqResult{
				Ack: resp,
				Err: err,
			}
		}(peerAddr)
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
			n.markAlive(targetAddr)
			n.mergeUpdates(res.Ack.Updates)
			return nil
		}
	}

	n.markSuspect(targetAddr)
	return errors
}
