package cluster

import (
	"log"

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

type Node struct {
	Address string
	Peers   map[string]*Peer
}

type Peer struct {
	Client pb.ClusterServiceClient
	State  ServerState
}

func NewNode(nodeAddress string, seedAddresses []string) *Node {
	peers := make(map[string]*Peer)
	for _, peerAddress := range seedAddresses {
		// TODO checkback to do TLS
		conn, err := grpc.NewClient(peerAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("did not connect to server: %s %v", peerAddress, err)
		}

		client := pb.NewClusterServiceClient(conn)
		peers[peerAddress] = &Peer{Client: client}

	}
	return &Node{Address: nodeAddress, Peers: peers}
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
