// Package main is the entry point for the dkv server.
package main

import (
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/rosewrightdev/dkv"
	"google.golang.org/grpc/credentials"
)

func main() {
	builder := dkv.NewEngineBuilder().Default()

	certFile := os.Getenv("DKV_TLS_CERT_FILE")
	keyFile := os.Getenv("DKV_TLS_KEY_FILE")
	switch {
	case certFile != "" && keyFile != "":
		creds, err := credentials.NewServerTLSFromFile(certFile, keyFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to load TLS keys: %v\n", err)
			os.Exit(1)
		}
		builder.SetCreds(creds)
	case os.Getenv("DKV_INSECURE") == "true":
		builder.SetInsecure()
	default:
		fmt.Fprintf(os.Stderr, "TLS credentials are required (DKV_TLS_CERT_FILE, DKV_TLS_KEY_FILE). Use DKV_INSECURE=true for development.\n")
		os.Exit(1)
	}

	if id := os.Getenv("DKV_NODE_ID"); id != "" {
		builder.SetNodeID(dkv.NodeID(id))
	}

	if addr := os.Getenv("DKV_BIND_ADDR"); addr != "" {
		builder.SetBindAddr(addr)
	}

	if portStr := os.Getenv("DKV_BIND_PORT"); portStr != "" {
		if port, err := strconv.Atoi(portStr); err == nil {
			builder.SetBindPort(port)
		}
	}

	if portStr := os.Getenv("DKV_GRPC_PORT"); portStr != "" {
		if port, err := strconv.Atoi(portStr); err == nil {
			builder.SetGrpcPort(port)
		}
	}

	if addr := os.Getenv("DKV_ADVERTISE_ADDR"); addr != "" {
		builder.SetAdvertiseAddr(addr)
	}

	if seeds := os.Getenv("DKV_SEED_NODES"); seeds != "" {
		builder.SetSeedNodes(strings.Split(seeds, ","))
	}

	if path := os.Getenv("DKV_WAL_PATH"); path != "" {
		builder.SetWalPath(path)
	}

	if path := os.Getenv("DKV_SNP_PATH"); path != "" {
		builder.SetSnpPath(path)
	}

	if rfStr := os.Getenv("DKV_REPLICATION_FACTOR"); rfStr != "" {
		if rf, err := strconv.Atoi(rfStr); err == nil {
			builder.SetReplicationFactor(rf)
		}
	}

	if snpStr := os.Getenv("DKV_SNP_INTERVAL"); snpStr != "" {
		if d, err := time.ParseDuration(snpStr); err == nil {
			builder.SetSnpInterval(d)
		} else {
			fmt.Fprintf(os.Stderr, "invalid DKV_SNP_INTERVAL: %v\n", err)
			os.Exit(1)
		}
	}

	if walStr := os.Getenv("DKV_WAL_INTERVAL"); walStr != "" {
		if d, err := time.ParseDuration(walStr); err == nil {
			builder.SetWalInterval(d)
		} else {
			fmt.Fprintf(os.Stderr, "invalid DKV_WAL_INTERVAL: %v\n", err)
			os.Exit(1)
		}
	}

	if walBufStr := os.Getenv("DKV_WAL_BUFFER_SIZE"); walBufStr != "" {
		if size, err := strconv.ParseUint(walBufStr, 10, 32); err == nil {
			builder.SetWalBufferSize(uint32(size))
		} else {
			fmt.Fprintf(os.Stderr, "invalid DKV_WAL_BUFFER_SIZE: %v\n", err)
			os.Exit(1)
		}
	}

	if walSegStr := os.Getenv("DKV_WAL_SEGMENTS"); walSegStr != "" {
		if segs, err := strconv.Atoi(walSegStr); err == nil {
			builder.SetWalSegments(segs)
		} else {
			fmt.Fprintf(os.Stderr, "invalid DKV_WAL_SEGMENTS: %v\n", err)
			os.Exit(1)
		}
	}

	if gossipStr := os.Getenv("DKV_GOSSIP_INTERVAL"); gossipStr != "" {
		if d, err := time.ParseDuration(gossipStr); err == nil {
			builder.SetGossipInterval(d)
		} else {
			fmt.Fprintf(os.Stderr, "invalid DKV_GOSSIP_INTERVAL: %v\n", err)
			os.Exit(1)
		}
	}

	if os.Getenv("DKV_SINGLE_NODE") == "true" {
		builder.SingleNode()
	}

	eng, err := builder.Build()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to build engine: %v\n", err)
		os.Exit(1)
	}

	eng.Start()
	s := dkv.NewServer(eng)

	go func() {
		fmt.Printf("Starting DKV server on %s...\n", eng.Addr())
		if err := s.Run(); err != nil {
			fmt.Fprintf(os.Stderr, "server failed: %v\n", err)
			os.Exit(1)
		}
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	fmt.Println("Shutting down gracefully...")
	s.Stop()
}
