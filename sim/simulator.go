package sim

import (
	"context"
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
	Rng       *rand.Rand
}

func NewSimulator(seed int64, addresses []string) *Simulator {
	clk := NewSimClock(seed)
	transport := NewSimTransport()
	rng := rand.New(rand.NewSource(seed))

	sim := &Simulator{
		Transport: transport,
		Clock:     clk,
		Rng:       rng,
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

func (s *Simulator) Step(d time.Duration) {
	s.Clock.Advance(d)
	ctx := context.Background()
	node := s.Nodes[s.Rng.Intn(len(s.Nodes))]
	node.Node.RunPingRound(ctx)
	for _, n := range s.Nodes {
		n.Node.RunSuspectCleanup(ctx)
	}
}

func (s *Simulator) Run(steps int, d time.Duration) {
	for range steps {
		s.Step(d)
	}
}

func (s *Simulator) Partition(a, b string) { s.Transport.Partition(a, b) }
func (s *Simulator) Heal(a, b string)      { s.Transport.Heal(a, b) }

func (s *Simulator) NodeState(observer, target string) cluster.ServerState {
	for _, n := range s.Nodes {
		if n.Address == observer {
			return n.Node.GetUpdates()[target].State
		}
	}
	panic("unknown observer: " + observer)
}
