package cluster

import (
	"log"
	"math/rand"

	pb "github.com/saenthan/resonatedb/proto-gen/cluster"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type ServerState int

const (
	Alive ServerState = iota
	Suspect
	Fail
)

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
	Client pb.ClusterServiceClient
	State  ServerState
}

type Node struct {
	Address string
	Peers   map[string]*Peer
	updates map[string]NodeUpdate
	Incarnation int
}

func NewNode(nodeAddress string, seedAddresses []string) *Node {
	peers := make(map[string]*Peer)

	for _, addr := range seedAddresses {
		peer, err := NewPeer(addr)
		if err != nil {
			log.Fatalf("failed to connect to peer %s: %v", addr, err)

		}
		peers[addr] = peer
	}

	return &Node{
		Address: nodeAddress,
		Peers:   peers,
		updates: make(map[string]NodeUpdate),
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

func (n *Node) mergeUpdates(peerUpdates map[string]*pb.NodeUpdate) {
	for addr, u := range peerUpdates {
		if u == nil {
			continue
		}
		
		if addr==n.Address{
			if (u.State==pb.NodeState(Suspect) ||u.State==pb.NodeState(Fail))&&u.Incarnation<int64(n.Incarnation){
				n.Incarnation++
				n.updates[n.Address]=NodeUpdate{
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

		if !exists {
			n.updates[addr] = NodeUpdate{
				Address:     u.Address,
				Incarnation: int(u.Incarnation),
				State:       incomingState,
			}
			continue
		}

		if u.Incarnation > int64(current.Incarnation) {
			n.updates[addr] = NodeUpdate{
				Address:        u.Address,
				Incarnation:    int(u.Incarnation),
				State:          incomingState,
				PiggyBackCount: 0,
			}
		} else if u.Incarnation == int64(current.Incarnation) && isStronger(incomingState, current.State) {
			current.State = incomingState
			current.PiggyBackCount = 0
			n.updates[addr] = current
		}
	}
}

func isStronger(a, b ServerState) bool {
	return a > b
}

func (n *Node) getNodeToPing() *Peer {
	if len(n.Peers) == 0 {
		return nil
	}

	keys := make([]string, 0, len(n.Peers))
	for k := range n.Peers {
		keys = append(keys, k)
	}

	randomKey := keys[rand.Intn(len(keys))]
	return n.Peers[randomKey]
}

func NewPeer(address string) (*Peer, error) {
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	client := pb.NewClusterServiceClient(conn)
	return &Peer{
		Client: client,
		State:  Alive,
	}, nil
}

/*
 SWIM PROTOCOL: https://www.cs.cornell.edu/projects/Quicksilver/public_pdfs/SWIM.pdf
-  Failure Detector
	- Protocol period
		- must be atleast 3 times the round trip estimate
		- Node Mi picks random member Mj selected, and pinged, waits for ack within specified timeout
		- If not within limit, Mi will indirectly pings Mj
			- Mi gets k random members, and send ping-req(Mj)
			- each will ping Mj and forward ack from Mj to Mi
			- at end of protocol period, Mi checks if it received acks from Mj or indirectly, if not will be set to failed state
			in local member list and hands off update to Dissemination Component.
- Dissemination Component
	- Upon detecting failiure, it will multi cast this info to rest of the group as failed
	- Member receiving this info will delete it from its membership list

- In updated model, instead of failed, set it to SUSPECT state, which the dissemntation component spreads this
	- after prespecified timeout, it is declared as faulty
	- seperate Dissemination not needed in the updated model

- Look into incarnation value, updates list, and life time priority
*/
