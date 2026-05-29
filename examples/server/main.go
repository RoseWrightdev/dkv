// Package main provides a simple example of starting a dkv server instance.
package main

import (
	"fmt"

	"github.com/rosewrightdev/dkv"
	"github.com/rosewrightdev/dkv/server"
)

func main() {
	// Initialize the Engine using the flat fluent API with sensible defaults
	eng, err := dkv.NewEngineBuilder().
		Default().
		SetInsecure().
		Build()

	if err != nil {
		panic(err)
	}

	// Start background services
	eng.Start()
	defer eng.Stop()

	// Run the gRPC Server using the address/port configured from the engine
	s := server.NewServer(eng)
	fmt.Printf("Starting DKV server on %s...\n", eng.Addr())
	if err := s.Run(); err != nil {
		panic(err)
	}
}
