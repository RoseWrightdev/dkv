package dkv

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"time"

	pb "github.com/rosewrightdev/dkv/api"
	"google.golang.org/grpc/credentials"
)

// Reconciler defines the interface required by the anti-entropy service for state comparison.
type Reconciler interface {
	RootDigest() RootDigest
	FillShardDigests(dst map[ShardID]Digest)
	FillDigests(dst map[ShardID]ShardDigest)
	SyncPush(sets []*pb.SetRequest, deletes []*pb.DeleteRequest) error
}

// EntropyService performs periodic state reconciliation between nodes.
// It detects divergence in storage shards and pulls missing data from peers.
type EntropyService struct {
	cluster  Cluster
	storage  Reconciler
	nodeID   NodeID
	pools    *resourcePools
	interval time.Duration
	creds    credentials.TransportCredentials
	stopChan chan struct{}
}

type EntropyServiceConfig struct {
	nodeID   NodeID
	cluster  Cluster
	storage  Reconciler
	pools    *resourcePools
	interval time.Duration
	creds    credentials.TransportCredentials
}

// newEntropyService initializes a new EntropyService instance.
func newEntropyService(config *EntropyServiceConfig) *EntropyService {
	if config.cluster == nil {
		panic("anti-entropy requires a cluster implementation")
	}
	if config.storage == nil {
		panic("anti-entropy requires a reconciler implementation")
	}

	return &EntropyService{
		cluster:  config.cluster,
		storage:  config.storage,
		nodeID:   config.nodeID,
		pools:    config.pools,
		interval: config.interval,
		creds:    config.creds,
		stopChan: make(chan struct{}),
	}
}

// start begins the background reconciliation loop until stopChan is closed.
func (s *EntropyService) start() {
	if s.interval <= 0 {
		panic(fmt.Sprintf("invalid anti-entropy interval: %v", s.interval))
	}
	go s.run()
}

// stop gracefully terminates the background reconciliation loop.
func (s *EntropyService) stop() {
	select {
	case <-s.stopChan:
		return // Already stopped
	default:
		close(s.stopChan)
	}
}

func (s *EntropyService) run() {
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

func (s *EntropyService) performSync() {
	members := s.cluster.Members()
	if len(members) == 0 {
		return
	}

	// #nosec G404
	target := members[rand.Intn(len(members))]
	client, err := NewClient(string(target), 2*time.Second, s.creds)
	if err != nil {
		slog.Debug("Failed to connect to peer for anti-entropy sync", "peer", target, "error", err)
		return
	}
	defer func() {
		_ = client.Close()
	}()

	req := s.pools.pullRequests.Get().(*pb.PullRequest)
	s.preparePullRequest(req)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := client.api.Pull(ctx, req)
	s.cleanupPullRequest(req)

	if err != nil {
		slog.Error("Anti-entropy pull failed", "peer", target, "error", err)
		return
	}

	if len(resp.Entries) > 0 || len(resp.Deletions) > 0 {
		slog.Info("Anti-entropy detected divergence and synced state",
			"peer", target, "sets", len(resp.Entries), "deletes", len(resp.Deletions))

		if err := s.storage.SyncPush(resp.Entries, resp.Deletions); err != nil {
			slog.Error("Failed to apply synced state from anti-entropy", "error", err)
		}
	}
}

func (s *EntropyService) preparePullRequest(req *pb.PullRequest) {
	localShards := s.pools.shardMaps.Get().(map[ShardID]Digest)
	localBuckets := s.pools.bucketMaps.Get().(map[ShardID]ShardDigest)
	defer func() {
		for k := range localShards {
			delete(localShards, k)
		}
		for k := range localBuckets {
			delete(localBuckets, k)
		}
		s.pools.shardMaps.Put(localShards)
		s.pools.bucketMaps.Put(localBuckets)
	}()

	s.storage.FillShardDigests(localShards)
	s.storage.FillDigests(localBuckets)

	req.NodeId = string(s.nodeID)
	req.RootDigest = uint64(s.storage.RootDigest())

	// Prepare Shard Digests
	for k := range req.ShardDigests {
		delete(req.ShardDigests, k)
	}
	for id, h := range localShards {
		// #nosec G115
		idU := uint32(id)
		req.ShardDigests[idU] = uint64(h)
	}

	// Prepare Sub-Bucket Digests
	for k := range req.SubDigests {
		delete(req.SubDigests, k)
	}
	for id, hashes := range localBuckets {
		sd := s.pools.shardDigests.Get().(*pb.ShardDigests)
		sd.SubHashes = hashes
		// #nosec G115
		idU := uint32(id)
		req.SubDigests[idU] = sd
	}
}

func (s *EntropyService) cleanupPullRequest(req *pb.PullRequest) {
	for _, sd := range req.SubDigests {
		sd.SubHashes = nil
		s.pools.shardDigests.Put(sd)
	}
	for k := range req.SubDigests {
		delete(req.SubDigests, k)
	}
	s.pools.pullRequests.Put(req)
}
