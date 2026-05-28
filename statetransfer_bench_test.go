package dkv

import (
	"fmt"
	"testing"
)

func BenchmarkStateTransfer_ExportState(b *testing.B) {
	pools := newPools()
	hm := newShardedMap()
	writer := &mockStateWriter{}
	st := newStateTransfer(pools, hm, writer)

	// Populate map
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%d", i)
		hm.StoreLWW(key, hashFunc(key), Value{
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
	pools := newPools()
	hm := newShardedMap()
	writer := &mockStateWriter{}
	st := newStateTransfer(pools, hm, writer)

	// Populate another transfer map to export state
	hm2 := newShardedMap()
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%d", i)
		hm2.StoreLWW(key, hashFunc(key), Value{
			Data:      []byte("value"),
			Timestamp: 100,
			NodeID:    "node-1",
		})
	}
	st2 := newStateTransfer(pools, hm2, writer)
	stateBytes := st2.ExportState()

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		st.ImportState(stateBytes)
	}
}
