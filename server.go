package dkv

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync"

	pb "github.com/rosewrightdev/dkv/api"
	"github.com/rosewrightdev/dkv/entropy"
	"github.com/rosewrightdev/dkv/hashmap"
	"github.com/rosewrightdev/dkv/kv"
	"google.golang.org/grpc"
)

type server struct {
	pb.UnimplementedDkvServiceServer
	eng   Engine
	pools *serverPools
}

type serverPools struct {
	shards  sync.Pool
	buckets sync.Pool
}

func newServerPools() *serverPools {
	return &serverPools{
		shards: sync.Pool{
			New: func() any { return make(map[hashmap.ShardID]hashmap.Digest) },
		},
		buckets: sync.Pool{
			New: func() any {
				m := make(map[hashmap.ShardID]hashmap.ShardDigest, hashmap.ShardCount)
				for i := range hashmap.ShardCount {
					m[hashmap.ShardID(i)] = make([]hashmap.Digest, hashmap.SubBucketCount)
				}
				return m
			},
		},
	}
}

func (s *server) Get(_ context.Context, in *pb.GetRequest) (*pb.GetResponse, error) {
	val, ok := s.eng.Get(kv.Key(in.GetKey()))
	resp := &pb.GetResponse{
		Value:  val,
		Exists: ok,
	}
	return resp, nil
}

func (s *server) Set(_ context.Context, in *pb.SetRequest) (*pb.SetResponse, error) {
	if err := s.eng.Set(in.Key, in.Value); err != nil {
		return &pb.SetResponse{}, err
	}
	return &pb.SetResponse{}, nil
}

func (s *server) Delete(_ context.Context, in *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	if err := s.eng.Delete(in.Key); err != nil {
		return &pb.DeleteResponse{}, err
	}
	return &pb.DeleteResponse{}, nil
}

func (s *server) Pull(_ context.Context, in *pb.PullRequest) (*pb.PullResponse, error) {
	shards := s.pools.shards.Get().(map[hashmap.ShardID]hashmap.Digest)
	buckets := s.pools.buckets.Get().(map[hashmap.ShardID]hashmap.ShardDigest)

	// Clean up after use
	defer func() {
		for k := range shards {
			delete(shards, k)
		}
		// Reset/clear the elements of the pre-allocated slices without deleting the keys
		for _, slice := range buckets {
			clear(slice)
		}
		s.pools.shards.Put(shards)
		s.pools.buckets.Put(buckets)
	}()

	for id, h := range in.ShardDigests {
		// #nosec G115
		shards[hashmap.ShardID(id)] = h
	}

	for id, sd := range in.SubDigests {
		// #nosec G115
		copy(buckets[hashmap.ShardID(id)], sd.SubHashes)
	}

	sets, deletes, err := s.eng.SyncPull(&entropy.PullConfig{
		RequesterID: kv.NodeID(in.NodeId),
		Root:        hashmap.RootDigest(in.RootDigest),
		Shards:      shards,
		Buckets:     buckets,
	})
	if err != nil {
		return nil, err
	}
	resp := &pb.PullResponse{
		Entries:   sets,
		Deletions: deletes,
	}
	return resp, nil
}

func (s *server) Push(_ context.Context, in *pb.PushRequest) (*pb.PushResponse, error) {
	if err := s.eng.SyncPush(in.Entries, in.Deletions); err != nil {
		return nil, err
	}
	return &pb.PushResponse{}, nil
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

// Run starts the gRPC server and serves requests on the address/port configured by the engine.
func (s *Grpc) Run() error {
	addr := s.eng.Addr()
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("dkv: failed to create listener on %s: %w", addr, err)
	}
	slog.Info("Grpc server running on " + listener.Addr().String())
	return s.inner.Serve(listener)
}

// Stop gracefully shuts down the gRPC server and stops the underlying engine.
func (s *Grpc) Stop() {
	s.inner.GracefulStop()
	s.handlers.eng.Stop()
}

// HardStop immediately shuts down the gRPC server and stops the underlying engine.
func (s *Grpc) HardStop() {
	s.inner.Stop()
	s.handlers.eng.Stop()
}
