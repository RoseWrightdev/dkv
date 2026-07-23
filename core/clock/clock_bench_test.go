package clock

import "testing"

func BenchmarkClock_Now_Parallel(b *testing.B) {
	hlc := NewClock()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = hlc.Now()
		}
	})
}
