package dkv

import "testing"

func BenchmarkDatabaseBuilder_Build(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		builder := NewDatabaseBuilder().
			Default().
			SingleNode().
			SetInsecure()
		_, _ = builder.Build()
	}
}
