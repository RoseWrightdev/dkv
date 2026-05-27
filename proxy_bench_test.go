package dkv

import "testing"

func BenchmarkGateway_GetReplicationFactor(b *testing.B) {
	gw := newGateway(&NopMesh{}, &MeshConfig{ReplicationFactor: 3}, newPools(), nil)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = gw.getReplicationFactor()
	}
}
