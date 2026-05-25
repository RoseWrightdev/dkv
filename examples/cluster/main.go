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
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})))

	const numNodes = 10
	dataDir := filepath.Join("examples", "replicated", "data")
	_ = os.RemoveAll(dataDir)

	fmt.Printf("Starting %d-node dkv cluster...\n", numNodes)

	cluster, err := dkv.NewCluster(numNodes, dataDir, insecure.NewCredentials())
	if err != nil {
		panic(fmt.Errorf("failed to start cluster: %w", err))
	}

	for i, engine := range cluster.Engines {
		name := fmt.Sprintf("node-%d", i+1)
		fmt.Printf("  -> %s online (gRPC: %s)\n", name, engine.Addr())
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
	cluster.Stop()
	fmt.Println("Cluster stopped successfully.")
}
