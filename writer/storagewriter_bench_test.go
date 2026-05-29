package writer

import (
	"fmt"
	"testing"

	pb "github.com/rosewrightdev/dkv/api"
	"github.com/rosewrightdev/dkv/hashmap"
	"github.com/rosewrightdev/dkv/mesh"
)

func BenchmarkStorageWriter_ApplySet(b *testing.B) {
	hm := hashmap.NewShardedMap()
	wal := &mockWal{}
	clock := &mockStorageClock{}
	meshObj := &mesh.NopMesh{}
	meshConfig := &mesh.MeshConfig{SingleNode: true}

	sw := NewStorageWriter(hm, wal, clock, meshObj, meshConfig)

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
	hm := hashmap.NewShardedMap()
	wal := &mockWal{}
	clock := &mockStorageClock{}
	meshObj := &mesh.NopMesh{}
	meshConfig := &mesh.MeshConfig{SingleNode: true}

	sw := NewStorageWriter(hm, wal, clock, meshObj, meshConfig)

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
