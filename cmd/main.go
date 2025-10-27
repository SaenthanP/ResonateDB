package main

import (
	"flag"
	"fmt"
	"net"
	"strings"

	"github.com/saenthan/resonatedb/internal/cluster"
	pb "github.com/saenthan/resonatedb/proto-gen/cluster"
	"google.golang.org/grpc"
)

func main() {
	port := flag.String("port", "", "port of node")
	peers := flag.String("peers", "", "list of addresses for peers seperated by commas")
	flag.Parse()

	if port == nil {
		fmt.Println("Port missing")
		return
	}
	var parsedPeers []string
	if peers != nil {
		parsedPeers = strings.Split(*peers, ",")
	}

	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", *port))
	if err != nil {
		fmt.Printf("failed to listen: %v. \n", err)
		return
	}

	s := grpc.NewServer()
	node := cluster.NewNode(*port, parsedPeers)
	if node == nil {
		return
	}

	srv := cluster.NewServer(node)
	pb.RegisterClusterServiceServer(s, srv)
	fmt.Printf("server started on port %s \n", *port)
	if err := s.Serve(lis); err != nil {
		fmt.Printf("failed to serve: %v", err)
	}

}
