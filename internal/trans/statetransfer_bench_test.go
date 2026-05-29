package trans

import (
	"fmt"
	"testing"

	pb "github.com/rosewrightdev/dkv/api"
	"github.com/rosewrightdev/dkv/internal/hashmap"
	"github.com/rosewrightdev/dkv/kv"
	"github.com/rosewrightdev/dkv/security"
)

type mockStateWriter struct{}

func (m *mockStateWriter) ApplySet(_ *pb.SetRequest) error       { return nil }
func (m *mockStateWriter) ApplyDelete(_ *pb.DeleteRequest) error { return nil }

func BenchmarkStateTransfer_ExportState(b *testing.B) {
	hm := hashmap.NewShardedMap()
	writer := &mockStateWriter{}
	st := NewStateTransfer(hm, writer)

	// Populate map
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%d", i)
		hm.StoreLWW(key, security.HashFunc(key), kv.Value{
			Data:      []byte("value"),
			Timestamp: 100,
			NodeID:    "node-1",
		})
	}

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		_ = st.ExportState()
	}
}

func BenchmarkStateTransfer_ImportState(b *testing.B) {
	hm := hashmap.NewShardedMap()
	writer := &mockStateWriter{}
	st := NewStateTransfer(hm, writer)

	// Populate another transfer map to export state
	hm2 := hashmap.NewShardedMap()
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%d", i)
		hm2.StoreLWW(key, security.HashFunc(key), kv.Value{
			Data:      []byte("value"),
			Timestamp: 100,
			NodeID:    "node-1",
		})
	}
	st2 := NewStateTransfer(hm2, writer)
	stateBytes := st2.ExportState()

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		st.ImportState(stateBytes)
	}
}
