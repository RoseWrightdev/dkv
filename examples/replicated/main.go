// Package main provides a minimal example of launching a 10-node distributed dkv cluster.
package main

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/rosewrightdev/dkv"
)

func main() {
	// Set log level to Warn to keep standard output clean for the example
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})))

	const numNodes = 10
	dataDir := filepath.Join("examples", "replicated", "data")
	_ = os.RemoveAll(dataDir) // Purge stale data to start a clean cluster run

	var servers []*dkv.Grpc
	var engines []dkv.Engine

	fmt.Printf("Starting %d-node DKV cluster...\n", numNodes)

	for i := range numNodes {
		name := fmt.Sprintf("node-%d", i+1)
		gossipPort := 7946 + i
		grpcPort := 50051 + i

		eb := dkv.NewEngineBuilder().
			Default().
			FastTest().
			SetWalPath(filepath.Join(dataDir, name, "wal")).
			SetSnpPath(filepath.Join(dataDir, name, "snp.gob")).
			SetNodeID(dkv.NodeID(name)).
			SetBindPort(gossipPort).
			SetGrpcPort(grpcPort).
			SetInsecure()

		// Join nodes 2..10 to node-1 as the primary bootstrap seed
		if i > 0 {
			eb.SetSeedNodes([]string{"127.0.0.1:7946"})
		}

		eng, err := eb.Build()
		if err != nil {
			panic(fmt.Errorf("failed to build engine for %s: %w", name, err))
		}
		engines = append(engines, eng)

		server := dkv.NewServer(eng)
		servers = append(servers, server)

		// Start background engines and gRPC server goroutine
		eng.Start()
		go func(s *dkv.Grpc) {
			_ = s.Run()
		}(server)

		fmt.Printf("  -> %s online (gRPC: 127.0.0.1:%d, Gossip: 127.0.0.1:%d)\n", name, grpcPort, gossipPort)
	}

	fmt.Println("\nCluster is fully operational!")
	fmt.Println("Connect to port 50051 to run the client examples:")
	fmt.Println("  go run examples/client/main.go")
	fmt.Println("  go run examples/cli/main.go")
	fmt.Println("\nPress Ctrl+C to terminate...")

	// Block until interrupted
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	fmt.Println("\nShutting down cluster gracefully...")
	for _, s := range servers {
		s.Stop()
	}
	for _, e := range engines {
		e.Stop()
	}
	fmt.Println("Cluster stopped successfully.")
}
