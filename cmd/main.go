package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/saenthan/resonatedb/internal/cluster"
	"github.com/saenthan/resonatedb/internal/real"
	"github.com/saenthan/resonatedb/internal/wal"
	pb "github.com/saenthan/resonatedb/proto-gen/cluster"
	"google.golang.org/grpc"
)

func main() {
	cwd, err := os.Getwd()
	if err != nil {
		return
	}

	dir := filepath.Join(cwd, "wal_logs")

	cfg := wal.WalConfig{
		Dir:            dir,
		Clock:          real.RealClock{},
		FileSystem:     &real.RealFS{},
		MaxSegmentSize: 64 * 1024 * 1024,
		BatchSize:      256,
	}
	w, err := wal.NewWal(cfg)
	if err != nil {
		fmt.Printf("failed to init wal: %v\n", err)
		return
	}
	go w.Run()
	defer w.Close()

	//////////////////////////////////////////////////////
	address := flag.String("address", "", "address of node")
	peers := flag.String("peers", "", "list of addresses for peers seperated by commas")
	flag.Parse()

	if address == nil {
		fmt.Println("address missing")
		return
	}

	var parsedPeers []string
	if peers != nil {
		parsedPeers = strings.Split(*peers, ",")
	}
	fmt.Println(parsedPeers)
	if len(parsedPeers) == 1 && parsedPeers[0] == "" {
		parsedPeers = nil
	}
	lis, err := net.Listen("tcp", *address)
	if err != nil {
		fmt.Printf("failed to listen: %v. \n", err)
		return
	}

	s := grpc.NewServer()

	clusterCfg := cluster.Config{
		Address:        *address,
		SeedAddresses:  parsedPeers,
		Transport:      cluster.NewGRPCTransport(),
		SuspectTimeout: time.Millisecond * 5000,
		Clock:          real.RealClock{},
	}

	node := cluster.NewNode(clusterCfg)
	if node == nil {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	node.StartFailureDetector(ctx)

	srv := cluster.NewServer(node)
	pb.RegisterClusterServiceServer(s, srv)
	fmt.Printf("server started on address %s \n", *address)
	if err := s.Serve(lis); err != nil {
		fmt.Printf("failed to serve: %v", err)
	}
}
