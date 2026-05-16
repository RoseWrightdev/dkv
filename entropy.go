package dkv

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"time"

	pb "github.com/rosewrightdev/dkv/api"
)

// Reconciler defines the interface required by the anti-entropy service to perform state comparison and updates.
type Reconciler interface {
	Digests() map[int32]uint64
	SyncPush(sets []*pb.SetRequest, deletes []*pb.DeleteRequest) error
}

// AntiEntropyService performs periodic state reconciliation between nodes.
// It detects divergence in storage shards and pulls missing data from peers.
type AntiEntropyService struct {
	cluster  Cluster
	storage  Reconciler
	interval time.Duration
	stopChan chan struct{}
}

// newAntiEntropyService initializes a new AntiEntropyService instance.
func newAntiEntropyService(cluster Cluster, storage Reconciler, interval time.Duration) *AntiEntropyService {
	if cluster == nil {
		panic("anti-entropy requires a cluster implementation")
	}
	if storage == nil {
		panic("anti-entropy requires a reconciler implementation")
	}

	return &AntiEntropyService{
		cluster:  cluster,
		storage:  storage,
		interval: interval,
		stopChan: make(chan struct{}),
	}
}

// start begins the background reconciliation loop.
func (s *AntiEntropyService) start() {
	if s.interval <= 0 {
		panic(fmt.Sprintf("invalid anti-entropy interval: %v", s.interval))
	}
	go s.run()
}

// stop gracefully terminates the background reconciliation loop.
func (s *AntiEntropyService) stop() {
	select {
	case <-s.stopChan:
		return // Already stopped
	default:
		close(s.stopChan)
	}
}

func (s *AntiEntropyService) run() {
	slog.Info("Anti-entropy service started", "interval", s.interval)
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopChan:
			return
		case <-ticker.C:
			s.performSync()
		}
	}
}

func (s *AntiEntropyService) performSync() {
	members := s.cluster.Members()
	if len(members) == 0 {
		return
	}

	// Pick a random member to reconcile with.
	target := members[rand.Intn(len(members))]

	// We use the gRPC client to perform the pull operation.
	// We intentionally create a fresh client to avoid long-lived connection issues during membership churn.
	client, err := NewInsecureClient(string(target), 2*time.Second)
	if err != nil {
		slog.Debug("Failed to connect to peer for anti-entropy sync", "peer", target, "error", err)
		return
	}
	defer client.Close()

	// Compare shard digests to identify divergence.
	localDigests := s.storage.Digests()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := client.api.Pull(ctx, &pb.PullRequest{KnownDigests: localDigests})
	if err != nil {
		slog.Error("Anti-entropy pull failed", "peer", target, "error", err)
		return
	}

	if len(resp.Entries) > 0 || len(resp.Deletions) > 0 {
		slog.Info("Anti-entropy detected divergence and synced state",
			"peer", target,
			"sets", len(resp.Entries),
			"deletes", len(resp.Deletions))

		if err := s.storage.SyncPush(resp.Entries, resp.Deletions); err != nil {
			slog.Error("Failed to apply synced state from anti-entropy", "error", err)
		}
	}
}
