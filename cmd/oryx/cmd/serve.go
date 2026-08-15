package cmd

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/grpc/credentials"

	oryx "github.com/rosewrightdev/oryx"
	"github.com/rosewrightdev/oryx/cluster/mesh"
	"github.com/rosewrightdev/oryx/kv"
	"github.com/rosewrightdev/oryx/server"
)

var (
	serveNodeID                 string
	serveBindAddr               string
	serveBindPort               int
	serveGRPCPort               int
	serveRESPPort               string
	serveAdvertiseAddr          string
	serveSeeds                  string
	serveReplicationFactor      int
	serveWALPath                string
	serveSNPPath                string
	serveWALInterval            string
	serveSNPInterval            string
	serveWALBufferSize          uint32
	serveWALSegments            int
	serveGossipInterval         string
	serveSingleNode             bool
	serveTLSCert                string
	serveTLSKey                 string
	serveInsecure               bool
	serveVolatile               bool
	serveReplicationFailureMode string
)

var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "Start an oryx server node",
	Long: `Start an oryx server node with gRPC and RESP (Redis-compatible) listeners.

TLS is required unless --insecure is set. For development, use --insecure.
All flags can also be set via ORYX_* environment variables (flags take precedence).

Example:
  oryx serve --insecure --single-node
  oryx serve --tls-cert server.crt --tls-key server.key --seeds 10.0.0.1:7946
  ORYX_INSECURE=true ORYX_SINGLE_NODE=true oryx serve`,
	PreRunE: applyServeEnv,
	RunE:    runServe,
}

func init() {
	f := serveCmd.Flags()

	f.StringVar(&serveNodeID, "node-id", "", "unique node ID (default: random SHA-256) [ORYX_NODE_ID]")
	f.StringVar(&serveBindAddr, "addr", "127.0.0.1", "bind address for gossip/gRPC [ORYX_BIND_ADDR]")
	f.IntVar(&serveBindPort, "port", 7946, "gossip membership bind port [ORYX_BIND_PORT]")
	f.IntVar(&serveGRPCPort, "grpc-port", 50051, "gRPC API port [ORYX_GRPC_PORT]")
	f.StringVar(&serveRESPPort, "resp-port", "6379", "RESP (Redis) protocol port [ORYX_RESP_PORT]")
	f.StringVar(&serveAdvertiseAddr, "advertise-addr", "", "address advertised to peer nodes [ORYX_ADVERTISE_ADDR]")
	f.StringVar(&serveSeeds, "seeds", "", "comma-separated list of seed node addresses [ORYX_SEED_NODES]")
	f.IntVar(&serveReplicationFactor, "replication", 3, "replication factor [ORYX_REPLICATION_FACTOR]")
	f.StringVar(&serveReplicationFailureMode, "replication-failure-mode", "strict", "replication failure mode (strict or lenient) [ORYX_REPLICATION_FAILURE_MODE]")
	f.StringVar(&serveWALPath, "wal-path", "data/wal", "write-ahead log directory [ORYX_WAL_PATH]")
	f.StringVar(&serveSNPPath, "snp-path", "data/snapshot.bin", "snapshot file path [ORYX_SNP_PATH]")
	f.StringVar(&serveWALInterval, "wal-interval", "500ms", "WAL sync interval [ORYX_WAL_INTERVAL]")
	f.StringVar(&serveSNPInterval, "snp-interval", "5m", "snapshot interval [ORYX_SNP_INTERVAL]")
	f.Uint32Var(&serveWALBufferSize, "wal-buffer-size", 65536, "WAL buffer size in bytes [ORYX_WAL_BUFFER_SIZE]")
	f.IntVar(&serveWALSegments, "wal-segments", 16, "maximum number of WAL log segments [ORYX_WAL_SEGMENTS]")
	f.StringVar(&serveGossipInterval, "gossip-interval", "10s", "gossip communication interval [ORYX_GOSSIP_INTERVAL]")
	f.BoolVar(&serveSingleNode, "single-node", false, "run in single-node mode (no clustering) [ORYX_SINGLE_NODE]")
	f.StringVar(&serveTLSCert, "tls-cert", "", "path to TLS certificate file [ORYX_TLS_CERT_FILE]")
	f.StringVar(&serveTLSKey, "tls-key", "", "path to TLS key file [ORYX_TLS_KEY_FILE]")
	f.BoolVar(&serveInsecure, "insecure", false, "use insecure gRPC (no TLS) — dev only [ORYX_INSECURE]")
	f.BoolVar(&serveVolatile, "volatile", false, "volatile in-memory mode — disables WAL and snapshots")
}

// applyServeEnv reads ORYX_* environment variables for any flag that was not
// explicitly set on the command line (flag > env var > compiled default).
func applyServeEnv(cmd *cobra.Command, _ []string) error {
	f := cmd.Flags()

	strEnv := func(flag, env string) {
		if !f.Changed(flag) {
			if v := os.Getenv(env); v != "" {
				_ = f.Set(flag, v)
			}
		}
	}
	boolEnv := func(flag, env string) {
		if !f.Changed(flag) {
			if v := os.Getenv(env); v == "true" {
				_ = f.Set(flag, "true")
			}
		}
	}

	strEnv("node-id", "ORYX_NODE_ID")
	strEnv("addr", "ORYX_BIND_ADDR")
	strEnv("port", "ORYX_BIND_PORT")
	strEnv("grpc-port", "ORYX_GRPC_PORT")
	strEnv("resp-port", "ORYX_RESP_PORT")
	strEnv("advertise-addr", "ORYX_ADVERTISE_ADDR")
	strEnv("seeds", "ORYX_SEED_NODES")
	strEnv("replication", "ORYX_REPLICATION_FACTOR")
	strEnv("replication-failure-mode", "ORYX_REPLICATION_FAILURE_MODE")
	strEnv("wal-path", "ORYX_WAL_PATH")
	strEnv("snp-path", "ORYX_SNP_PATH")
	strEnv("wal-interval", "ORYX_WAL_INTERVAL")
	strEnv("snp-interval", "ORYX_SNP_INTERVAL")
	strEnv("wal-buffer-size", "ORYX_WAL_BUFFER_SIZE")
	strEnv("wal-segments", "ORYX_WAL_SEGMENTS")
	strEnv("gossip-interval", "ORYX_GOSSIP_INTERVAL")
	strEnv("tls-cert", "ORYX_TLS_CERT_FILE")
	strEnv("tls-key", "ORYX_TLS_KEY_FILE")
	boolEnv("single-node", "ORYX_SINGLE_NODE")
	boolEnv("insecure", "ORYX_INSECURE")

	return nil
}

func runServe(_ *cobra.Command, _ []string) error {
	// Restore INFO-level logging so server startup/shutdown messages are visible.
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	builder := oryx.NewDatabaseBuilder().Default()

	// --- TLS / credentials ---
	switch {
	case serveTLSCert != "" && serveTLSKey != "":
		creds, err := credentials.NewServerTLSFromFile(serveTLSCert, serveTLSKey)
		if err != nil {
			return fmt.Errorf("failed to load TLS credentials: %w", err)
		}
		builder.SetCreds(creds)
	case serveInsecure:
		builder.SetInsecure()
	default:
		return fmt.Errorf("TLS credentials required (--tls-cert/--tls-key). Use --insecure for development")
	}

	// --- Node identity & network ---
	if serveNodeID != "" {
		builder.SetNodeID(kv.NodeID(serveNodeID))
	}
	builder.SetBindAddr(serveBindAddr)
	builder.SetBindPort(serveBindPort)
	builder.SetGrpcPort(serveGRPCPort)

	if serveAdvertiseAddr != "" {
		builder.SetAdvertiseAddr(serveAdvertiseAddr)
	}
	if serveSeeds != "" {
		builder.SetSeedNodes(strings.Split(serveSeeds, ","))
	}
	builder.SetReplicationFactor(serveReplicationFactor)
	builder.SetReplicationFailureMode(mesh.ReplicationFailureMode(serveReplicationFailureMode))

	// --- Storage paths ---
	if serveVolatile {
		builder.VolatileMode()
	} else {
		builder.SetWalPath(serveWALPath)
		builder.SetSnpPath(serveSNPPath)
	}

	// --- Intervals ---
	if walInterval, err := time.ParseDuration(serveWALInterval); err == nil {
		builder.SetWalInterval(walInterval)
	} else {
		return fmt.Errorf("invalid --wal-interval %q: %w", serveWALInterval, err)
	}

	if snpInterval, err := time.ParseDuration(serveSNPInterval); err == nil {
		builder.SetSnpInterval(snpInterval)
	} else {
		return fmt.Errorf("invalid --snp-interval %q: %w", serveSNPInterval, err)
	}

	if gossipInterval, err := time.ParseDuration(serveGossipInterval); err == nil {
		builder.SetGossipInterval(gossipInterval)
	} else {
		return fmt.Errorf("invalid --gossip-interval %q: %w", serveGossipInterval, err)
	}

	// --- WAL tuning ---
	builder.SetWalBufferSize(serveWALBufferSize)
	builder.SetWalSegments(serveWALSegments)

	// --- Cluster mode ---
	if serveSingleNode {
		builder.SingleNode()
	}

	eng, err := builder.Build()
	if err != nil {
		return fmt.Errorf("failed to build engine: %w", err)
	}

	eng.Start()

	grpcSrv := server.NewServer(eng)
	respAddr := "0.0.0.0:" + serveRESPPort
	respSrv := server.NewRESPServer(eng, respAddr)

	go func() {
		slog.Info("Starting oryx gRPC server", "addr", eng.Addr())
		if err := grpcSrv.Run(); err != nil {
			slog.Error("gRPC server failed", "error", err)
			os.Exit(1)
		}
	}()

	go func() {
		slog.Info("Starting oryx RESP server", "addr", respAddr)
		if err := respSrv.Run(); err != nil {
			// A failed RESP server means the node is partially dead — it
			// still handles gRPC cluster traffic but drops all Redis client
			// connections. Exiting cleanly allows the process supervisor or
			// Kubernetes to restart the node in a known-good state (#80).
			slog.Error("RESP server failed", "error", err)
			os.Exit(1)
		}
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	slog.Info("Shutting down gracefully...")
	respSrv.Stop()
	grpcSrv.Stop()
	return nil
}
