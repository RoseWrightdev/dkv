package dkv

import (
	"fmt"
	"testing"

	pb "github.com/rosewrightdev/dkv/api"
)

func BenchmarkStorageWriter_ApplySet(b *testing.B) {
	hm := newShardedMap()
	wal := &mockWal{}
	clock := NewHLC()
	mesh := &NopMesh{}
	meshConfig := &MeshConfig{SingleNode: true}

	sw := newStorageWriter(hm, wal, clock, mesh, meshConfig)

	req := &pb.SetRequest{
		Key:       "key",
		Value:     []byte("value"),
		Timestamp: 100,
		NodeId:    "node-1",
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		req.Key = fmt.Sprintf("key-%d", i)
		_ = sw.ApplySet(req)
	}
}

func BenchmarkStorageWriter_ApplyDelete(b *testing.B) {
	hm := newShardedMap()
	wal := &mockWal{}
	clock := NewHLC()
	mesh := &NopMesh{}
	meshConfig := &MeshConfig{SingleNode: true}

	sw := newStorageWriter(hm, wal, clock, mesh, meshConfig)

	req := &pb.DeleteRequest{
		Key:       "key",
		Timestamp: 100,
		NodeId:    "node-1",
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		req.Key = fmt.Sprintf("key-%d", i)
		_ = sw.ApplyDelete(req)
	}
}
