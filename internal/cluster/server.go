package cluster

import (
	"context"

	pb "github.com/saenthan/resonatedb/proto-gen/cluster"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Agent interface {
	HandlePing(ctx context.Context, fromAddr string, updates map[string]NodeUpdate) (PingResponse, error)
	HandlePingReq(ctx context.Context, fromAddr string, targetAddr string, updates map[string]NodeUpdate) (PingResponse, error)
}

type Server struct {
	Agent Agent
	pb.UnimplementedClusterServiceServer
}

func NewServer(Agent Agent) *Server {
	return &Server{Agent: Agent}
}

func (s *Server) PingNode(ctx context.Context, in *pb.Ping) (*pb.Ack, error) {
	resp, err := s.Agent.HandlePing(ctx, in.From, fromProtoUpdates(in.Updates))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to handle PingNode: %v", err)

	}

	return &pb.Ack{
		From:    resp.From,
		Updates: toProtoUpdates(resp.Updates),
	}, nil
}

func (s *Server) PingReqNode(ctx context.Context, in *pb.PingReq) (*pb.Ack, error) {
	resp, err := s.Agent.HandlePingReq(ctx, in.From, in.Target, fromProtoUpdates(in.Updates))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to handle PingReqNode: %v", err)

	}

	return &pb.Ack{
		From:    resp.From,
		Updates: toProtoUpdates(resp.Updates),
	}, nil
}
