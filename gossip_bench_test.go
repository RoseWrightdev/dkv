package dkv

import (
	"testing"

	pb "github.com/rosewrightdev/dkv/api"
	"google.golang.org/protobuf/proto"
)

type mockStateWriter struct{}

func (m *mockStateWriter) ApplySet(_ *pb.SetRequest) error       { return nil }
func (m *mockStateWriter) ApplyDelete(_ *pb.DeleteRequest) error { return nil }

func BenchmarkGossip_OnGossip(b *testing.B) {
	pools := newPools()
	writer := &mockStateWriter{}
	g := newGossip(pools, writer)

	entry := &pb.WalEntry{
		Entry: &pb.WalEntry_Set{
			Set: &pb.SetRequest{
				Key:       "testkey",
				Value:     []byte("testvalue"),
				Timestamp: 12345,
				NodeId:    "node-1",
			},
		},
	}
	data, _ := proto.Marshal(entry)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		g.OnGossip(data)
	}
}
