package dkv

import (
	"context"
	"log/slog"
	"math/rand"
	"time"

	pb "github.com/rosewrightdev/dkv/api"
)

// AntiEntropyService performs periodic state reconciliation between nodes.
type AntiEntropyService struct {
	eng      *engine
	interval time.Duration
	stopChan chan struct{}
}

func newAntiEntropyService(eng *engine, interval time.Duration) *AntiEntropyService {
	return &AntiEntropyService{
		eng:      eng,
		interval: interval,
		stopChan: make(chan struct{}),
	}
}

func (s *AntiEntropyService) start() {
	if s.interval <= 0 {
		slog.Debug("Anti-entropy disabled (interval <= 0)")
		return
	}
	go s.run()
}

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
	if s.eng.cluster == nil {
		return
	}

	members := s.eng.cluster.Members()
	if len(members) == 0 {
		return
	}

	// Pick a random member
	target := members[rand.Intn(len(members))]

	client, err := NewInsecureClient(target, 2*time.Second)
	if err != nil {
		slog.Debug("Failed to connect to peer for sync", "peer", target, "error", err)
		return
	}
	defer client.Close()

	// Get local digests
	localDigests := s.eng.hm.Digests()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := client.api.Pull(ctx, &pb.PullRequest{KnownDigests: localDigests})
	if err != nil {
		slog.Error("Anti-entropy pull failed", "peer", target, "error", err)
		return
	}

	if len(resp.Entries) > 0 || len(resp.Deletions) > 0 {
		slog.Info("Anti-entropy synced entries from peer", 
			"peer", target, 
			"sets", len(resp.Entries), 
			"deletes", len(resp.Deletions))
		
		_ = s.eng.SyncPush(resp.Entries, resp.Deletions)
	}
}
