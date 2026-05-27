package dkv

import "testing"

func BenchmarkEngineBuilder_Build(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		builder := NewEngineBuilder().
			Default().
			SingleNode().
			SetInsecure()
		_, _ = builder.Build()
	}
}
