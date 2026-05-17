package dkv

import (
	"encoding/gob"
	"fmt"
	"io"
	"log/slog"
	"os"
	"slices"

	pb "github.com/rosewrightdev/dkv/api"
)

func (eng *engine) streamToEncoder(enc *gob.Encoder) error {
	for i := range shardCount {
		shard := eng.hm[i]
		shard.mu.RLock()
		for b := range subBucketCount {
			for k, v := range shard.buckets[b] {
				entry := eng.pools.snapshotEntries.Get().(*snapshotEntry)
				entry.Key = k
				entry.Data = v.Data
				entry.Timestamp = v.Timestamp
				entry.Tombstone = v.Tombstone

				if err := enc.Encode(entry); err != nil {
					shard.mu.RUnlock()
					entry.Key = ""
					entry.Data = nil
					eng.pools.snapshotEntries.Put(entry)
					return err
				}
				entry.Key = ""
				entry.Data = nil
				eng.pools.snapshotEntries.Put(entry)
			}
		}
		shard.mu.RUnlock()
	}
	return nil
}

func (eng *engine) recover(sssPath string) error {
	if info, err := os.Stat(sssPath); err == nil && info.Size() > 0 {
		// #nosec G304
		file, err := os.Open(sssPath)
		if err != nil {
			return err
		}
		defer func() {
			_ = file.Close()
		}()

		dec := gob.NewDecoder(file)
		count := 0
		for {
			entry := eng.pools.snapshotEntries.Get().(*snapshotEntry)
			if err := dec.Decode(entry); err != nil {
				entry.Key = ""
				entry.Data = nil
				eng.pools.snapshotEntries.Put(entry)
				if err == io.EOF {
					break
				}
				return err
			}
			eng.hm.Store(entry.Key, hashFunc(entry.Key), Value{
				Data:      entry.Data,
				Timestamp: entry.Timestamp,
				Tombstone: entry.Tombstone,
			})
			entry.Key = ""
			entry.Data = nil
			eng.pools.snapshotEntries.Put(entry)
			count++
		}
		slog.Info("Loaded state from snapshot", "path", sssPath, "keys", count)
	}

	updates, err := eng.wal.replay()
	if err != nil {
		return err
	}
	for k, v := range updates {
		h := hashFunc(k)
		eng.hm.Store(k, h, v)
	}
	if len(updates) > 0 {
		slog.Info("Replayed updates from WAL", "count", len(updates))
	}

	return nil
}

func (eng *engine) RootDigest() RootDigest {
	return eng.hm.RootDigest()
}

func (eng *engine) FillShardDigests(dst map[ShardID]Digest) {
	eng.hm.FillShardDigests(dst)
}

func (eng *engine) FillDigests(dst map[ShardID]ShardDigest) {
	eng.hm.FillDigests(dst)
}

// SyncPull performs a hierarchical anti-entropy reconciliation against a remote node's state.
// It uses a 3-level Merkle-style comparison tree to pinpoint divergence with minimal bandwidth:
//
// 1. Root Level: Single global hash check (O(1)).
//
// 2. Shard Level: 128 intermediate shard hashes.
//
// 3. Bucket Level: 64 sub-bucket hashes per mismatched shard.
//
// It returns only the specific records (Sets/Deletes) needed to bring the remote node into sync.
func (eng *engine) SyncPull(requesterID NodeID, root RootDigest, shards map[ShardID]Digest, buckets map[ShardID]ShardDigest) ([]*pb.SetRequest, []*pb.DeleteRequest, error) {
	// Level 1: Global check. If the root hash matches, the entire database is identical.
	if root == eng.RootDigest() {
		return nil, nil, nil
	}

	localShardDigests := eng.pools.shardMaps.Get().(map[ShardID]Digest)
	localBuckets := eng.pools.bucketMaps.Get().(map[ShardID]ShardDigest)
	defer func() {
		eng.pools.shardMaps.Put(localShardDigests)
		eng.pools.bucketMaps.Put(localBuckets)
	}()

	eng.hm.FillShardDigests(localShardDigests)
	eng.hm.FillDigests(localBuckets)

	var sets []*pb.SetRequest
	var deletes []*pb.DeleteRequest

	for shardID, localShardHash := range localShardDigests {
		remoteShardHash, hasShard := shards[shardID]

		// Level 2: Shard check
		if hasShard && localShardHash == remoteShardHash {
			continue
		}

		remoteBuckets, hasBuckets := buckets[shardID]
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

		if mismatchMask > 0 {
			shard := eng.hm[int(shardID)]
			shard.mu.RLock()
			for b := range subBucketCount {
				if (mismatchMask & (1 << b)) != 0 {
					for k, v := range shard.buckets[b] {
						// Filter: Only send keys the requester is responsible for
						if !eng.clusterConfig.SingleNode {
							isResponsible := false
							if slices.Contains(eng.cluster.GetOwners(Key(k), eng.clusterConfig.ReplicationFactor), requesterID) {
								isResponsible = true
							}
							if !isResponsible {
								continue
							}
						}

						if v.Tombstone {
							req := eng.pools.deleteRequests.Get().(*pb.DeleteRequest)
							req.Key = k
							req.Timestamp = v.Timestamp
							req.NodeId = v.NodeID
							deletes = append(deletes, req)
						} else {
							req := eng.pools.setRequests.Get().(*pb.SetRequest)
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

func (eng *engine) SyncPush(sets []*pb.SetRequest, deletes []*pb.DeleteRequest) error {
	for _, s := range sets {
		if err := eng.applyGossipSet(s); err != nil {
			return err
		}
	}
	for _, d := range deletes {
		if err := eng.applyGossipDelete(d); err != nil {
			return err
		}
	}
	return nil
}

func (eng *engine) decodeFromReader(r io.Reader) error {
	dec := gob.NewDecoder(r)
	count := 0
	for {
		entry := eng.pools.snapshotEntries.Get().(*snapshotEntry)
		if err := dec.Decode(entry); err != nil {
			entry.Key = ""
			entry.Data = nil
			eng.pools.snapshotEntries.Put(entry)
			if err == io.EOF {
				break
			}
			return fmt.Errorf("failed to decode snapshot entry: %w", err)
		}

		if entry.Tombstone {
			req := eng.pools.deleteRequests.Get().(*pb.DeleteRequest)
			req.Key = entry.Key
			req.Timestamp = entry.Timestamp
			err := eng.applyGossipDelete(req)
			req.Reset()
			eng.pools.deleteRequests.Put(req)
			if err != nil {
				entry.Key = ""
				entry.Data = nil
				eng.pools.snapshotEntries.Put(entry)
				return err
			}
		} else {
			req := eng.pools.setRequests.Get().(*pb.SetRequest)
			req.Key = entry.Key
			req.Value = entry.Data
			req.Timestamp = entry.Timestamp
			err := eng.applyGossipSet(req)
			req.Reset()
			eng.pools.setRequests.Put(req)
			if err != nil {
				entry.Key = ""
				entry.Data = nil
				eng.pools.snapshotEntries.Put(entry)
				return err
			}
		}

		entry.Key = ""
		entry.Data = nil
		eng.pools.snapshotEntries.Put(entry)
		count++
	}

	if count > 0 {
		slog.Info("Merged remote state from cluster member", "entries", count)
	}
	return nil
}
