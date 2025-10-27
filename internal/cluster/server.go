package cluster

import (
	pb "github.com/saenthan/resonatedb/proto-gen/cluster"
)

type Server struct {
	Node *Node
	pb.UnimplementedClusterServiceServer
}

func NewServer(node *Node) *Server {
	return &Server{Node: node}
}


// Handle incoming connections