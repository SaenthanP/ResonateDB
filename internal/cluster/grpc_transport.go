package cluster

import (
	"context"

	pb "github.com/saenthan/resonatedb/proto-gen/cluster"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type GRPCTransport struct {
	client pb.ClusterServiceClient
}

func NewGRPCTransport(client pb.ClusterServiceClient) *GRPCTransport {
	return &GRPCTransport{client: client}
}
func (g *GRPCTransport) Ping(
	ctx context.Context,
	target string,
	req PingRequest,
) (PingResponse, error) {

	ack, err := g.client.PingNode(ctx, &pb.Ping{
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
	via string,
	target string,
	req PingRequest,
) (PingResponse, error) {

	ack, err := g.client.PingReqNode(ctx, &pb.PingReq{
		From:    req.From,
		Target:  target,
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

func GrpcPeerFactory() PeerFactory {
	return func(address string) (*Peer, error) {
		conn, err := grpc.NewClient(
			address,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			return nil, err
		}

		client := pb.NewClusterServiceClient(conn)

		return &Peer{
			Client: NewGRPCTransport(client),
			State:  Alive,
		}, nil
	}
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
