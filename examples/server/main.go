// Package main provides a simple example of starting a dkv server instance.
package main

import (
	"fmt"

	"github.com/rosewrightdev/dkv"
	"github.com/rosewrightdev/dkv/server"
)

func main() {
	// Initialize the Database using the flat fluent API with sensible defaults
	db, err := dkv.NewDatabaseBuilder().
		Default().
		SetInsecure().
		Build()

	if err != nil {
		panic(err)
	}

	// Start background services
	db.Start()
	defer db.Stop()

	// Run the gRPC Server using the address/port configured from the database
	s := server.NewServer(db)
	fmt.Printf("Starting DKV server on %s...\n", db.Addr())
	if err := s.Run(); err != nil {
		panic(err)
	}
}
