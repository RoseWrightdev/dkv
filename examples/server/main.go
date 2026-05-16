package main

import (
	"fmt"
	"net"

	"github.com/rosewrightdev/dkv"
)

func main() {
	// Initialize the Engine using the flat fluent API
	eng, err := dkv.NewEngineBuilder().
		Default().
		SetNodeID("node-1").
		SetBindPort(7946).
		SetGrpcPort(50051).
		SetWalPath("data/wal").
		SetSssPath("data/snapshot.bin").
		GetEngine()

	if err != nil {
		panic(err)
	}

	// Start background services
	eng.Start()
	defer eng.Stop()

	// Run the gRPC Server
	listener, err := net.Listen("tcp", ":50051")
	if err != nil {
		panic(err)
	}

	fmt.Println("dkv server listening on :50051...")
	s := dkv.NewServer(eng)
	if err := s.Run(listener); err != nil {
		panic(err)
	}
}
