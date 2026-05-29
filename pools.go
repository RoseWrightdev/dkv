package dkv

import (
	"sync"

	pb "github.com/rosewrightdev/dkv/api"
	"github.com/rosewrightdev/dkv/snap"
)

type pools struct {
	setRequests       sync.Pool
	deleteRequests    sync.Pool
	snapshotEntries   sync.Pool
	walEntries        sync.Pool
	walSetWrappers    sync.Pool
	walDeleteWrappers sync.Pool
	pullRequests      sync.Pool
	shardDigests      sync.Pool
	shardMaps         sync.Pool
	bucketMaps        sync.Pool
}

func newPools() *pools {
	return &pools{
		setRequests: sync.Pool{
			New: func() any { return &pb.SetRequest{} },
		},
		deleteRequests: sync.Pool{
			New: func() any { return &pb.DeleteRequest{} },
		},
		snapshotEntries: sync.Pool{
			New: func() any { return &snap.SnapshotEntry{} },
		},
		walEntries: sync.Pool{
			New: func() any { return &pb.WalEntry{} },
		},
		walSetWrappers: sync.Pool{
			New: func() any { return &pb.WalEntry_Set{} },
		},
		walDeleteWrappers: sync.Pool{
			New: func() any { return &pb.WalEntry_Delete{} },
		},
		pullRequests: sync.Pool{
			New: func() any {
				return &pb.PullRequest{
					ShardDigests: make(map[uint32]uint64, shardCount),
					SubDigests:   make(map[uint32]*pb.ShardDigests, shardCount),
				}
			},
		},
		shardDigests: sync.Pool{
			New: func() any { return &pb.ShardDigests{} },
		},
		shardMaps: sync.Pool{
			New: func() any { return make(map[ShardID]Digest) },
		},
		bucketMaps: sync.Pool{
			New: func() any {
				m := make(map[ShardID]ShardDigest, shardCount)
				for i := range shardCount {
					m[ShardID(i)] = make([]Digest, subBucketCount)
				}
				return m
			},
		},
	}
}
