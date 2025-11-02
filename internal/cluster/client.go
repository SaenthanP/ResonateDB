package cluster

import (
	"context"
	"fmt"

	"github.com/saenthan/resonatedb/proto-gen/cluster"
)

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

	ack, err := peer.Client.PingNode(ctx, &req)
	if err == nil {
		n.mergeUpdates(ack.Updates)
		return
	}

}

func (n *Node) PingReq(ctx context.Context) {

}
