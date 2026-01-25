package cluster

import "context"

type PingRequest struct {
	From    string
	Updates map[string]NodeUpdate
}

type PingResponse struct {
	From    string
	Updates map[string]NodeUpdate
}

type Transport interface {
	Ping(ctx context.Context, targetAddr string, req PingRequest) (PingResponse, error)
	PingReq(ctx context.Context, targetAddr string, req PingRequest) (PingResponse, error)
}
