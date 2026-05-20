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
	gossip     Gossiper
	mesh       Mesher
	creds      credentials.TransportCredentials
	meshConfig *MeshConfig
	hm         *shardedMap
	pools      *pools
	stopChan   chan struct{}
	nodeID     NodeID
	interval   time.Duration
}

type SyncerConfig struct {
	gossip     Gossiper
	mesh       Mesher
	creds      credentials.TransportCredentials
	meshConfig *MeshConfig
	hm         *shardedMap
	pools      *pools
	nodeID     NodeID
	interval   time.Duration
}

// newSyncer initializes a new Syncer instance.
func newSyncer(config *SyncerConfig) *Syncer {
	if config.mesh == nil {
		panic("Syncer requires a mesh implementation")
	}

	return &Syncer{
		gossip:     config.gossip,
		mesh:       config.mesh,
		meshConfig: config.meshConfig,
		hm:         config.hm,
		nodeID:     config.nodeID,
		pools:      config.pools,
		interval:   config.interval,
		creds:      config.creds,
		stopChan:   make(chan struct{}),
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
		if err := syn.gossip.applySet(s); err != nil {
			return err
		}
	}
	for _, d := range deletes {
		if err := syn.gossip.applyDelete(d); err != nil {
			return err
		}
	}
	return nil
}

type PullConfig struct {
	shards      map[ShardID]Digest
	buckets     map[ShardID]ShardDigest
	requesterID NodeID
	root        RootDigest
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

		remoteBucketHashes, hasBuckets := pullConfig.buckets[shardID]
		localBucketHashes := localBuckets[shardID]

		// Level 3: Determine which sub-buckets need syncing using a bitmask.
		mismatchMask := calculateMismatchMask(hasBuckets, remoteBucketHashes, localBucketHashes)

		if mismatchMask > 0 {
			sets, deletes = s.collectShardMismatches(shardID, mismatchMask, pullConfig.requesterID, sets, deletes)
		}
	}
	return sets, deletes, nil
}

// collectShardMismatches iterates over mismatched sub-buckets in a shard and collects changed keys.
func (s *Syncer) collectShardMismatches(
	shardID ShardID,
	mismatchMask uint64,
	requesterID NodeID,
	sets []*pb.SetRequest,
	deletes []*pb.DeleteRequest,
) ([]*pb.SetRequest, []*pb.DeleteRequest) {
	shard := s.hm[int(shardID)]
	shard.mu.RLock()
	for b := range subBucketCount {
		if (mismatchMask & (1 << b)) != 0 {
			for k, v := range shard.buckets[b] {
				if !s.isRequesterResponsible(k, requesterID) {
					continue
				}

				if v.Tombstone {
					deletes = append(deletes, s.buildDeleteRequest(k, v))
				} else {
					sets = append(sets, s.buildSetRequest(k, v))
				}
			}
		}
	}
	shard.mu.RUnlock()
	return sets, deletes
}

// calculateMismatchMask returns a bitmask representing mismatched sub-buckets (0-63).
func calculateMismatchMask(hasBuckets bool, remoteBucketHashes, localBucketHashes ShardDigest) uint64 {
	if !hasBuckets || len(remoteBucketHashes) != len(localBucketHashes) {
		return ^uint64(0)
	}
	var mismatchMask uint64
	for i := range subBucketCount {
		if localBucketHashes[i] != remoteBucketHashes[i] {
			mismatchMask |= (1 << i)
		}
	}
	return mismatchMask
}

// isRequesterResponsible returns true if the remote node is responsible for replicating the key.
func (s *Syncer) isRequesterResponsible(key string, requesterID NodeID) bool {
	if s.meshConfig.SingleNode {
		return true
	}
	owners := s.mesh.GetOwners(Key(key), s.meshConfig.ReplicationFactor)
	isResponsible := slices.Contains(owners, requesterID)
	s.mesh.PutOwners(owners)
	return isResponsible
}

// buildSetRequest leases a protobuf SetRequest from the sync pools and populates it.
func (s *Syncer) buildSetRequest(key string, val Value) *pb.SetRequest {
	req := s.pools.setRequests.Get().(*pb.SetRequest)
	req.Key = key
	req.Value = val.Data
	req.Timestamp = val.Timestamp
	req.NodeId = val.NodeID
	return req
}

// buildDeleteRequest leases a protobuf DeleteRequest from the sync pools and populates it.
func (s *Syncer) buildDeleteRequest(key string, val Value) *pb.DeleteRequest {
	req := s.pools.deleteRequests.Get().(*pb.DeleteRequest)
	req.Key = key
	req.Timestamp = val.Timestamp
	req.NodeId = val.NodeID
	return req
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
	localShards, localBuckets := s.preparePullRequest(req)
	defer func() {
		s.pools.shardMaps.Put(localShards)
		s.pools.bucketMaps.Put(localBuckets)
	}()

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

func (s *Syncer) preparePullRequest(req *pb.PullRequest) (map[ShardID]Digest, map[ShardID]ShardDigest) {
	localShards := s.pools.shardMaps.Get().(map[ShardID]Digest)
	localBuckets := s.pools.bucketMaps.Get().(map[ShardID]ShardDigest)

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

	return localShards, localBuckets
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
