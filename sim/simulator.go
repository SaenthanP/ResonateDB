package sim

import (
	"math/rand"
	"time"

	"github.com/saenthan/resonatedb/internal/cluster"
)

type SimNode struct {
	Node    *cluster.Node
	Address string
}

type Simulator struct {
	Nodes     []*SimNode
	Transport *SimTransport
	Clock     *SimClock
	rng       *rand.Rand
}

func NewSimulator(seed int64, addresses []string) *Simulator {
	clk := NewSimClock(seed)
	transport := NewSimTransport()
	rng := rand.New(rand.NewSource(seed))

	sim := &Simulator{
		Transport: transport,
		Clock:     clk,
		rng:       rng,
	}

	for _, addr := range addresses {
		node := cluster.NewNode(cluster.Config{
			Address:        addr,
			SeedAddresses:  addresses,
			Transport:      transport,
			SuspectTimeout: 3 * time.Second,
			Clock:          clk,
		})
		transport.Register(addr, node)
		sim.Nodes = append(sim.Nodes, &SimNode{
			Node:    node,
			Address: addr,
		})
	}
	return sim
}
