package gossip

import (
	"testing"

	pb "github.com/rosewrightdev/oryx/api"
	"google.golang.org/protobuf/proto"
)

type fuzzNopStateWriter struct{}

func (f *fuzzNopStateWriter) ApplySet(_ *pb.SetRequest) error       { return nil }
func (f *fuzzNopStateWriter) ApplyDelete(_ *pb.DeleteRequest) error { return nil }

func FuzzOnGossip(f *testing.F) {
	// Seed corpus with valid WalEntry bytes and random noise
	validSetEntry, _ := proto.Marshal(&pb.WalEntry{
		Entry: &pb.WalEntry_Set{
			Set: &pb.SetRequest{
				Key:       "user:100",
				Value:     []byte("testvalue"),
				Timestamp: 1000,
				NodeId:    "node-1",
			},
		},
	})
	validDeleteEntry, _ := proto.Marshal(&pb.WalEntry{
		Entry: &pb.WalEntry_Delete{
			Delete: &pb.DeleteRequest{
				Key:       "user:100",
				Timestamp: 1001,
				NodeId:    "node-1",
			},
		},
	})

	f.Add([]byte{})
	f.Add(validSetEntry)
	f.Add(validDeleteEntry)
	f.Add([]byte{0x00, 0xff, 0xfe, 0xfd, 0x12, 0x34})

	g := NewGossip(&fuzzNopStateWriter{})

	f.Fuzz(func(t *testing.T, data []byte) {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("OnGossip panicked on input %x: %v", data, r)
			}
		}()
		g.OnGossip(data)
	})
}
