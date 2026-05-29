package gateway

import (
	"testing"

	"github.com/rosewrightdev/dkv/mesh"
)

func BenchmarkGateway_GetReplicationFactor(b *testing.B) {
	gw := NewGateway(&mesh.NopMesh{}, &mesh.MeshConfig{ReplicationFactor: 3}, nil)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = gw.getReplicationFactor()
	}
}
