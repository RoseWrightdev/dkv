package mesh

import "testing"

func BenchmarkNopMesh_GetOwners(b *testing.B) {
	mesh := &NopMesh{}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = mesh.GetOwners("key", 3)
	}
}

func BenchmarkNopMesh_Members(b *testing.B) {
	mesh := &NopMesh{}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = mesh.Members()
	}
}
