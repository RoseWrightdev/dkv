package dkv

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync"

	pb "github.com/rosewrightdev/dkv/api"
	"google.golang.org/grpc"
)

type server struct {
	pb.UnimplementedDkvServiceServer
	eng   Engine
	pools *serverPools
}

type serverPools struct {
	shards          sync.Pool
	buckets         sync.Pool
	getResponses    sync.Pool
	setResponses    sync.Pool
	deleteResponses sync.Pool
	pullResponses   sync.Pool
	pushResponses   sync.Pool
}

func newServerPools() *serverPools {
	return &serverPools{
		shards: sync.Pool{
			New: func() any { return make(map[ShardID]Digest) },
		},
		buckets: sync.Pool{
			New: func() any {
				m := make(map[ShardID]ShardDigest, shardCount)
				for i := range shardCount {
					m[ShardID(i)] = make([]Digest, subBucketCount)
				}
				return m
			},
		},
		getResponses: sync.Pool{
			New: func() any { return &pb.GetResponse{} },
		},
		setResponses: sync.Pool{
			New: func() any { return &pb.SetResponse{} },
		},
		deleteResponses: sync.Pool{
			New: func() any { return &pb.DeleteResponse{} },
		},
		pullResponses: sync.Pool{
			New: func() any { return &pb.PullResponse{} },
		},
		pushResponses: sync.Pool{
			New: func() any { return &pb.PushResponse{} },
		},
	}
}

func (s *server) Get(_ context.Context, in *pb.GetRequest) (*pb.GetResponse, error) {
	val, ok := s.eng.Get(Key(in.GetKey()))
	resp := s.pools.getResponses.Get().(*pb.GetResponse)
	resp.Value = val
	resp.Exists = ok
	return resp, nil
}

func (s *server) Set(_ context.Context, in *pb.SetRequest) (*pb.SetResponse, error) {
	if err := s.eng.Set(in.Key, in.Value); err != nil {
		return s.pools.setResponses.Get().(*pb.SetResponse), err
	}
	return s.pools.setResponses.Get().(*pb.SetResponse), nil
}

func (s *server) Delete(_ context.Context, in *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	if err := s.eng.Delete(in.Key); err != nil {
		return s.pools.deleteResponses.Get().(*pb.DeleteResponse), err
	}
	return s.pools.deleteResponses.Get().(*pb.DeleteResponse), nil
}

func (s *server) Pull(_ context.Context, in *pb.PullRequest) (*pb.PullResponse, error) {
	shards := s.pools.shards.Get().(map[ShardID]Digest)
	buckets := s.pools.buckets.Get().(map[ShardID]ShardDigest)

	// Clean up after use
	defer func() {
		for k := range shards {
			delete(shards, k)
		}
		for k := range buckets {
			delete(buckets, k)
		}
		s.pools.shards.Put(shards)
		s.pools.buckets.Put(buckets)
	}()

	for id, h := range in.ShardDigests {
		// #nosec G115
		shards[ShardID(id)] = h
	}

	for id, sd := range in.SubDigests {
		// #nosec G115
		buckets[ShardID(id)] = sd.SubHashes
	}

	sets, deletes, err := s.eng.SyncPull(NodeID(in.NodeId), in.RootDigest, shards, buckets)
	if err != nil {
		return nil, err
	}
	resp := s.pools.pullResponses.Get().(*pb.PullResponse)
	resp.Entries = sets
	resp.Deletions = deletes
	return resp, nil
}

func (s *server) Push(_ context.Context, in *pb.PushRequest) (*pb.PushResponse, error) {
	if err := s.eng.SyncPush(in.Entries, in.Deletions); err != nil {
		return nil, err
	}
	return s.pools.pushResponses.Get().(*pb.PushResponse), nil
}

// Grpc represents the gRPC server wrapper for the dkv service.
type Grpc struct {
	inner    *grpc.Server
	handlers *server
	eng      Engine
}

// NewServer creates a new Grpc server instance around a dkv Engine.
func NewServer(eng Engine) *Grpc {
	s := grpc.NewServer()
	h := &server{
		eng:   eng,
		pools: newServerPools(),
	}
	pb.RegisterDkvServiceServer(s, h)
	return &Grpc{inner: s, handlers: h, eng: eng}
}

// Run starts the gRPC server and serves requests on the provided net.Listener.
func (s *Grpc) Run(listener net.Listener) error {
	if listener == nil {
		return fmt.Errorf("dkv: cannot run server with nil listener")
	}
	slog.Info("Grpc server running on " + listener.Addr().String())
	err := s.inner.Serve(listener)
	return err
}

// Stop gracefully shuts down the gRPC server and stops the underlying engine.
func (s *Grpc) Stop() {
	s.handlers.eng.Stop()
	s.inner.GracefulStop()
}
