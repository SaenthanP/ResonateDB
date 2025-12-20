package cluster

import (
	"context"

	pb "github.com/saenthan/resonatedb/proto-gen/cluster"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	Node *Node
	pb.UnimplementedClusterServiceServer
}

func NewServer(node *Node) *Server {
	return &Server{Node: node}
}

func (s *Server) PingNode(ctx context.Context, in *pb.Ping) (*pb.Ack, error) {
	peerAddress := in.From
	_, exists := s.Node.Peers[peerAddress]

	if !exists {
		peer, err := NewPeer(peerAddress)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to create peer: %v", err)
		}
		s.Node.Peers[peerAddress] = peer
	}

	if len(in.Updates) > 0 {
		s.Node.mergeUpdates(in.Updates)
	}

	return &pb.Ack{
		From:    s.Node.Address,
		Updates: s.Node.toProtoUpdates(),
	}, nil
}

func (s *Server) PingReqNode(ctx context.Context, in *pb.PingReq) (*pb.Ack, error) {
	peerAddress := in.From
	_, exists := s.Node.Peers[peerAddress]

	if !exists {
		peer, err := NewPeer(peerAddress)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to create peer: %v", err)
		}
		s.Node.Peers[peerAddress] = peer
	}

	if len(in.Updates) > 0 {
		s.Node.mergeUpdates(in.Updates)
	}

	targetAddress := in.Target
	_, targetExists := s.Node.Peers[targetAddress]
	if !targetExists {
		peer, err := NewPeer(targetAddress)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to create peer: %v", err)
		}
		s.Node.Peers[targetAddress] = peer
	}

	input := &pb.Ping{
		From:    s.Node.Address,
		Updates: s.Node.toProtoUpdates(),
	}

	ack, err := s.Node.Peers[targetAddress].Client.PingNode(ctx, input)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to connect to target node: %v", err)
	}

	if len(ack.Updates) > 0 {
		s.Node.mergeUpdates(ack.Updates)
	}

	return &pb.Ack{
		From:    s.Node.Address,
		Updates: s.Node.toProtoUpdates(),
	}, nil
}
