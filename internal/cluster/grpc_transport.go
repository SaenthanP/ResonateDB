package cluster

import (
	"context"
	"fmt"

	pb "github.com/saenthan/resonatedb/proto-gen/cluster"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type GRPCTransport struct {
	conns map[string]*grpc.ClientConn
}

func NewGRPCTransport() *GRPCTransport {
	return &GRPCTransport{conns: make(map[string]*grpc.ClientConn)}
}

func (g *GRPCTransport) getConn(addr string) (*grpc.ClientConn, error) {
	conn, exists := g.conns[addr]
	if exists {
		return conn, nil
	}

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to create new peer connection for addr: %s with error %v", addr, err)
	}
	g.conns[addr] = conn
	return conn, nil
}

func (g *GRPCTransport) Ping(
	ctx context.Context,
	targetAddr string,
	req PingRequest,
) (PingResponse, error) {

	conn, err := g.getConn(targetAddr)
	if err != nil {
		return PingResponse{}, nil
	}

	//TODO look into if this is the best way to do connection pooling. Connection vs client pooling?
	client := pb.NewClusterServiceClient(conn)
	ack, err := client.PingNode(ctx, &pb.Ping{
		From:    req.From,
		Updates: toProtoUpdates(req.Updates),
	})
	if err != nil {
		return PingResponse{}, err
	}

	return PingResponse{
		From:    ack.From,
		Updates: fromProtoUpdates(ack.Updates),
	}, nil
}

func (g *GRPCTransport) PingReq(
	ctx context.Context,
	targetAddr string,
	req PingRequest,
) (PingResponse, error) {
	conn, err := g.getConn(targetAddr)
	if err != nil {
		return PingResponse{}, nil
	}

	client := pb.NewClusterServiceClient(conn)
	ack, err := client.PingReqNode(ctx, &pb.PingReq{
		From:    req.From,
		Updates: toProtoUpdates(req.Updates),
	})

	if err != nil {
		return PingResponse{}, err
	}

	return PingResponse{
		From:    ack.From,
		Updates: fromProtoUpdates(ack.Updates),
	}, nil
}

func toProtoUpdates(in map[string]NodeUpdate) map[string]*pb.NodeUpdate {
	out := make(map[string]*pb.NodeUpdate, len(in))
	for k, v := range in {
		out[k] = &pb.NodeUpdate{
			Address:        v.Address,
			Incarnation:    int64(v.Incarnation),
			PiggyBackCount: int64(v.PiggyBackCount),
			State:          v.State.ToProto(),
		}
	}
	return out
}

func fromProtoUpdates(in map[string]*pb.NodeUpdate) map[string]NodeUpdate {
	out := make(map[string]NodeUpdate, len(in))
	for k, v := range in {
		out[k] = NodeUpdate{
			Address:        v.Address,
			Incarnation:    int(v.Incarnation),
			PiggyBackCount: int(v.PiggyBackCount),
			State:          ServerState(v.State),
		}
	}
	return out
}
