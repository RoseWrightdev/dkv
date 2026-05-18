package dkv

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"slices"
	"time"

	pb "github.com/rosewrightdev/dkv/api"
	"google.golang.org/grpc/credentials"
)

// Syncer performs periodic state reconciliation between nodes.
// It detects divergence in storage shards and pulls missing data from peers.
type Syncer struct {
	gossip        Gossiper
	mesh          Mesh
	clusterConfig *ClusterConfig
	hm            *shardedMap
	nodeID        NodeID
	pools         *pools
	interval      time.Duration
	creds         credentials.TransportCredentials
	stopChan      chan struct{}
}

type SyncerConfig struct {
	nodeID        NodeID
	gossip        Gossiper
	mesh          Mesh
	clusterConfig *ClusterConfig
	hm            *shardedMap
	pools         *pools
	interval      time.Duration
	creds         credentials.TransportCredentials
}

// newSyncer initializes a new Syncer instance.
func newSyncer(config *SyncerConfig) *Syncer {
	if config.mesh == nil {
		panic("Syncer requires a mesh implementation")
	}

	return &Syncer{
		gossip:        config.gossip,
		mesh:          config.mesh,
		clusterConfig: config.clusterConfig,
		hm:            config.hm,
		nodeID:        config.nodeID,
		pools:         config.pools,
		interval:      config.interval,
		creds:         config.creds,
		stopChan:      make(chan struct{}),
	}
}

// start begins the background reconciliation loop until stopChan is closed.
func (s *Syncer) start() {
	if s.interval <= 0 {
		panic(fmt.Sprintf("invalid Syncer interval: %v", s.interval))
	}
	go s.run()
}

// stop gracefully terminates the background reconciliation loop.
func (s *Syncer) stop() {
	select {
	case <-s.stopChan:
		return // Already stopped
	default:
		close(s.stopChan)
	}
}

func (s *Syncer) run() {
	slog.Info("Syncer service started", "interval", s.interval)
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

func (syn *Syncer) push(sets []*pb.SetRequest, deletes []*pb.DeleteRequest) error {
	for _, s := range sets {
		if err := syn.gossip.applyGossipSet(s); err != nil {
			return err
		}
	}
	for _, d := range deletes {
		if err := syn.gossip.applyGossipDelete(d); err != nil {
			return err
		}
	}
	return nil
}

type PullConfig struct {
	requesterID NodeID
	root        RootDigest
	shards      map[ShardID]Digest
	buckets     map[ShardID]ShardDigest
}

func (s *Syncer) pull(pullConfig *PullConfig) ([]*pb.SetRequest, []*pb.DeleteRequest, error) {
	// Level 1: Global check. If the root hash matches, the entire database is identical.
	if pullConfig.root == s.hm.RootDigest() {
		return nil, nil, nil
	}

	localShardDigests := s.pools.shardMaps.Get().(map[ShardID]Digest)
	localBuckets := s.pools.bucketMaps.Get().(map[ShardID]ShardDigest)
	defer func() {
		s.pools.shardMaps.Put(localShardDigests)
		s.pools.bucketMaps.Put(localBuckets)
	}()

	s.hm.FillShardDigests(localShardDigests)
	s.hm.FillDigests(localBuckets)

	var sets []*pb.SetRequest
	var deletes []*pb.DeleteRequest

	for shardID, localShardHash := range localShardDigests {
		remoteShardHash, hasShard := pullConfig.shards[shardID]

		// Level 2: Shard check
		if hasShard && localShardHash == remoteShardHash {
			continue
		}

		remoteBuckets, hasBuckets := pullConfig.buckets[shardID]
		localBucketHashes := localBuckets[shardID]

		// Level 3: Determine which sub-buckets need syncing using a bitmask. Each bit
		// corresponds to a sub-bucket index (0-63). To fit perfectly in a cache line.
		var mismatchMask uint64
		if !hasBuckets || len(remoteBuckets) != len(localBucketHashes) {
			// If the remote node is missing the bucket hashes entirely,
			// mark all 64 bits for synchronization.
			mismatchMask = ^uint64(0)
		} else {
			// Compare each sub-bucket hash and set the corresponding bit if they differ.
			for i := range subBucketCount {
				if localBucketHashes[i] != remoteBuckets[i] {
					mismatchMask |= (1 << i)
				}
			}
		}

		// I tried to refactor this out into another function, but saw small performance regresssions
		// this code is excepted from the usual codestyle requirements.
		if mismatchMask > 0 {
			shard := s.hm[int(shardID)]
			shard.mu.RLock()
			for b := range subBucketCount {
				if (mismatchMask & (1 << b)) != 0 {
					for k, v := range shard.buckets[b] {
						// Filter: Only send keys the requester is responsible for
						if !s.clusterConfig.SingleNode {
							isResponsible := false
							owners := s.mesh.GetOwners(Key(k), s.clusterConfig.ReplicationFactor)
							if slices.Contains(owners, pullConfig.requesterID) {
								isResponsible = true
							}
							s.mesh.PutOwners(owners)
							if !isResponsible {
								continue
							}
						}

						if v.Tombstone {
							req := s.pools.deleteRequests.Get().(*pb.DeleteRequest)
							req.Key = k
							req.Timestamp = v.Timestamp
							req.NodeId = v.NodeID
							deletes = append(deletes, req)
						} else {
							req := s.pools.setRequests.Get().(*pb.SetRequest)
							req.Key = k
							req.Value = v.Data
							req.Timestamp = v.Timestamp
							req.NodeId = v.NodeID
							sets = append(sets, req)
						}
					}
				}
			}
			shard.mu.RUnlock()
		}
	}
	return sets, deletes, nil
}

func (s *Syncer) performSync() {
	members := s.mesh.Members()
	if len(members) == 0 {
		return
	}

	// #nosec G404
	target := members[rand.Intn(len(members))]
	client, err := NewClient(string(target), 2*time.Second, s.creds)
	if err != nil {
		slog.Debug("Failed to connect to peer for Syncer sync", "peer", target, "error", err)
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
		slog.Error("Syncer pull failed", "peer", target, "error", err)
		return
	}

	if len(resp.Entries) > 0 || len(resp.Deletions) > 0 {
		slog.Info("Syncer detected divergence and synced state",
			"peer", target, "sets", len(resp.Entries), "deletes", len(resp.Deletions))

		if err := s.push(resp.Entries, resp.Deletions); err != nil {
			slog.Error("Failed to apply synced state from Syncer", "error", err)
		}
	}
}

func (s *Syncer) preparePullRequest(req *pb.PullRequest) {
	localShards := s.pools.shardMaps.Get().(map[ShardID]Digest)
	localBuckets := s.pools.bucketMaps.Get().(map[ShardID]ShardDigest)
	defer func() {
		s.pools.shardMaps.Put(localShards)
		s.pools.bucketMaps.Put(localBuckets)
	}()

	s.hm.FillShardDigests(localShards)
	s.hm.FillDigests(localBuckets)

	req.NodeId = string(s.nodeID)
	req.RootDigest = uint64(s.hm.RootDigest())

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

func (s *Syncer) cleanupPullRequest(req *pb.PullRequest) {
	for _, sd := range req.SubDigests {
		sd.SubHashes = nil
		s.pools.shardDigests.Put(sd)
	}
	for k := range req.SubDigests {
		delete(req.SubDigests, k)
	}
	s.pools.pullRequests.Put(req)
}
