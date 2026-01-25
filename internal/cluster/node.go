package cluster

import (
	"context"
	"fmt"
	"math/rand"

	pb "github.com/saenthan/resonatedb/proto-gen/cluster"
)

type ServerState int

const (
	Alive ServerState = iota
	Suspect
	Fail
)
const (
	K = 3
)

// This should be moved to the grpc handler/transport area
func (s ServerState) ToProto() pb.NodeState {
	switch s {
	case Alive:
		return pb.NodeState_ALIVE
	case Suspect:
		return pb.NodeState_SUSPECT
	case Fail:
		return pb.NodeState_DEAD
	default:
		return pb.NodeState_DEAD
	}
}

type NodeUpdate struct {
	Address        string
	Incarnation    int
	PiggyBackCount int
	State          ServerState
}

type Node struct {
	Address     string
	updates     map[string]NodeUpdate
	Incarnation int
	Transport   Transport
}

type Config struct {
	Address       string
	SeedAddresses []string
	Transport     Transport
}

func NewNode(cfg Config) *Node {
	node := &Node{
		Address:     cfg.Address,
		updates:     make(map[string]NodeUpdate),
		Incarnation: 0,
		Transport:   cfg.Transport,
	}

	node.updates[cfg.Address] = NodeUpdate{
		Address:        cfg.Address,
		Incarnation:    0,
		PiggyBackCount: 0,
		State:          Alive,
	}

	for _, addr := range cfg.SeedAddresses {
		node.updates[addr] = NodeUpdate{
			Address:        addr,
			Incarnation:    0,
			PiggyBackCount: 0,
			State:          Alive,
		}
	}

	return node
}

func (n *Node) toProtoUpdates() map[string]*pb.NodeUpdate {
	result := make(map[string]*pb.NodeUpdate, len(n.updates))
	for addr, u := range n.updates {
		result[addr] = &pb.NodeUpdate{
			Address:        u.Address,
			Incarnation:    int64(u.Incarnation),
			PiggyBackCount: int64(u.PiggyBackCount),
			State:          u.State.ToProto(),
		}
	}
	return result
}

func (n *Node) mergeUpdates(peerUpdates map[string]NodeUpdate) {
	for addr, u := range peerUpdates {
		if addr == n.Address {
			if (u.State == Suspect || u.State == Fail) && u.Incarnation >= n.Incarnation {
				n.Incarnation++
				n.updates[n.Address] = NodeUpdate{
					Address:        n.Address,
					Incarnation:    n.Incarnation,
					PiggyBackCount: 0,
					State:          Alive,
				}
			}
			continue
		}

		current, exists := n.updates[addr]
		incomingState := ServerState(u.State)
		// TODO this should be fixed after once I start to add cleanup of failed peers and piggyback count
		// Maybe add a isConnected flag to the peer
		if !exists {
			n.updates[addr] = NodeUpdate{
				Address:     u.Address,
				Incarnation: int(u.Incarnation),
				State:       incomingState,
			}
			continue
		}

		if u.Incarnation > current.Incarnation {
			n.updates[addr] = NodeUpdate{
				Address:        u.Address,
				Incarnation:    int(u.Incarnation),
				State:          incomingState,
				PiggyBackCount: 0,
			}
		} else if u.Incarnation == current.Incarnation && isStronger(incomingState, current.State) && current.State != Fail {
			current.State = incomingState
			current.PiggyBackCount = 0
			n.updates[addr] = current
		}
	}
}

func isStronger(a, b ServerState) bool {
	return a > b
}

func (n *Node) getNodeToPing() string {
	if len(n.updates) == 0 {
		return ""
	}

	keys := make([]string, 0, len(n.updates))
	for addr, node := range n.updates {
		if addr != n.Address && node.State != Fail {
			keys = append(keys, addr)
		}
	}

	randomAddr := keys[rand.Intn(len(keys))]
	return randomAddr
}

func (n *Node) getKNodesToPing() []string {
	candidates := make([]string, 0)

	for addr, peer := range n.updates {
		if peer.State == Fail || n.Address == addr {
			continue
		}
		candidates = append(candidates, addr)
	}
	rand.Shuffle(len(candidates), func(i, j int) { candidates[i], candidates[j] = candidates[j], candidates[i] })

	if len(candidates) < K {
		return candidates
	}

	return candidates[:K]
}
func (n *Node) markSuspect(target string) {

	_, exists := n.updates[target]
	if !exists {
		return
	}
	curr := n.updates[target]
	curr.State = Suspect
	n.updates[target] = curr
}

func (n *Node) markAlive(peerAddress string) {
	node, exists := n.updates[peerAddress]
	if !exists {
		n.updates[peerAddress] = NodeUpdate{
			Address:        peerAddress,
			Incarnation:    0,
			PiggyBackCount: 0,
			State:          Alive,
		}
		return
	}

	node.State = Alive
	n.updates[peerAddress] = node
}

func (n *Node) addToUpdate(address string) {
	if _, ok := n.updates[address]; ok {
		return
	}

	n.updates[address] = NodeUpdate{
		Address:        address,
		Incarnation:    0,
		PiggyBackCount: 0,
		State:          Alive,
	}
}

func (n *Node) HandlePing(ctx context.Context, req PingRequest) (PingResponse, error) {
	if len(req.Updates) > 0 {
		n.mergeUpdates(req.Updates)
	}

	return PingResponse{
		From:    req.From,
		Updates: n.updates,
	}, nil
}

func (n *Node) HandlePingReq(ctx context.Context, targetAddr string, req PingRequest) (PingResponse, error) {
	if len(req.Updates) > 0 {
		n.mergeUpdates(req.Updates)
	}

	// TODO: do you pass current node address or the parent caller
	// TODO: When switching to proper concurrency, n.updates should actually send a copy because maps are reference types
	input := PingRequest{
		From:    n.Address,
		Updates: n.updates,
	}

	// ack, err := n.Peers[targetAddr].Client.Ping(ctx, input)

	ack, err := n.Transport.Ping(ctx, targetAddr, input)
	if err != nil {
		return PingResponse{}, fmt.Errorf("failed to forward ping req to: %s from %s with error: %v", targetAddr, n.Address, err)
	}

	if len(ack.Updates) > 0 {
		n.mergeUpdates(ack.Updates)
	}

	return PingResponse{
		From:    n.Address,
		Updates: n.updates,
	}, nil
}
