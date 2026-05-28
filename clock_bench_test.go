package dkv

import "testing"

func BenchmarkHLC_Now_Parallel(b *testing.B) {
	hlc := NewHLC()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = hlc.Now()
		}
	})
}
