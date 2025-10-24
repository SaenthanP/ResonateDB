package cluster

import "sync"

type Node struct {
	address string
	peers   []*Node
	mu      sync.Mutex
}

func NewNode(address string) *Node {
	return &Node{address: address, peers: make([]*Node, 0), mu: sync.Mutex{}}
}

func (n *Node) seedNode(addresses []string) {

}
