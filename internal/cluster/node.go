package cluster

import (
	"context"
	"fmt"
	"log"
	"math/rand"

	pb "github.com/saenthan/resonatedb/proto-gen/cluster"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

type PeerFactory func(address string) (*Peer, error)

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

type Peer struct {
	Client Transport
	State  ServerState
}

type Node struct {
	Address     string
	Peers       map[string]*Peer
	updates     map[string]NodeUpdate
	Incarnation int

	newPeer PeerFactory
}

func NewNode(nodeAddress string, seedAddresses []string, newPeer PeerFactory,
) *Node {
	peers := make(map[string]*Peer)
	fmt.Println(seedAddresses)
	for _, addr := range seedAddresses {
		peer, err := newPeer(addr)
		if err != nil {
			log.Fatalf("failed to connect to peer %s: %v", addr, err)

		}
		peers[addr] = peer
	}
	updates := make(map[string]NodeUpdate)
	updates[nodeAddress] = NodeUpdate{
		Address:        nodeAddress,
		Incarnation:    0,
		PiggyBackCount: 0,
		State:          Alive,
	}

	return &Node{
		Address: nodeAddress,
		Peers:   peers,
		updates: updates,
		newPeer: newPeer,
	}
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
			newPeer, err := n.newPeer(addr)
			if err == nil {
				n.Peers[addr] = newPeer
			}

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

func (n *Node) getNodeToPing() (string, *Peer) {
	if len(n.Peers) == 0 {
		return "", nil
	}

	keys := make([]string, 0, len(n.Peers))
	for k := range n.Peers {
		keys = append(keys, k)
	}

	randomKey := keys[rand.Intn(len(keys))]
	return randomKey, n.Peers[randomKey]
}

func (n *Node) getKNodesToPing() []*Peer {
	candidates := make([]*Peer, 0)

	for _, peer := range n.Peers {
		if peer.State == Fail {
			continue
		}
		candidates = append(candidates, peer)
	}
	rand.Shuffle(len(candidates), func(i, j int) { candidates[i], candidates[j] = candidates[j], candidates[i] })

	if len(candidates) < K {
		return candidates
	}

	return candidates[:K]
}
func (n *Node) markSuspect(target string) {
	_, exists := n.Peers[target]
	if !exists {
		return
	}

	n.Peers[target].State = Suspect

	_, exists = n.updates[target]
	if !exists {
		n.updates[target] = NodeUpdate{
			Address:        target,
			Incarnation:    0,
			PiggyBackCount: 0,
			State:          Suspect,
		}
	} else {
		curr := n.updates[target]
		curr.State = Suspect
		n.updates[target] = curr
	}
}
func (n *Node) markAlive(peerAddress string) {
	if peer, ok := n.Peers[peerAddress]; ok {
		peer.State = Alive
	}

	if u, ok := n.updates[peerAddress]; ok {
		u.State = Alive
		u.PiggyBackCount = 0
		n.updates[peerAddress] = u
	}
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

func (n *Node) HandlePing(ctx context.Context, fromAddr string, updates map[string]NodeUpdate) (PingResponse, error) {
	_, exists := n.Peers[fromAddr]
	if !exists {
		peer, err := n.newPeer(fromAddr)
		if err != nil {
			return PingResponse{}, fmt.Errorf("failed to create new peer %w", err)
		}
		n.Peers[fromAddr] = peer
	}

	if len(updates) > 0 {
		n.mergeUpdates(updates)
	}

	return PingResponse{
		From:    fromAddr,
		Updates: n.updates,
	}, nil
}
func (n *Node) HandlePingReq(ctx context.Context, fromAddr string, targetAddr string, updates map[string]NodeUpdate) (PingResponse, error) {

	_, exists := n.Peers[fromAddr]

	if !exists {
		peer, err := n.newPeer(fromAddr)
		if err != nil {
			return PingResponse{}, status.Errorf(codes.Internal, "failed to create peer: %v", err)
		}
		n.Peers[fromAddr] = peer
	}

	if len(updates) > 0 {
		n.mergeUpdates(updates)
	}

	_, targetExists := n.Peers[targetAddr]
	if !targetExists {
		peer, err := n.newPeer(targetAddr)
		if err != nil {
			return PingResponse{}, status.Errorf(codes.Internal, "failed to create peer: %v", err)
		}
		n.Peers[targetAddr] = peer
	}

	// TODO: do you pass current node address or the parent caller
	input := PingRequest{
		From:    n.Address,
		Updates: updates,
	}

	ack, err := n.Peers[targetAddr].Client.Ping(ctx, input)
	if err != nil {
		return PingResponse{}, status.Errorf(codes.Internal, "failed to connect to target node: %v", err)
	}

	if len(ack.Updates) > 0 {
		n.mergeUpdates(ack.Updates)
	}

	return PingResponse{
		From:    n.Address,
		Updates: n.updates,
	}, nil
}
