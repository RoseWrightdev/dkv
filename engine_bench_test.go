package dkv

import (
	"fmt"
	"os"
	"testing"
	"time"
)

func setupBenchmarkEngine(b *testing.B, distributed bool) (Engine, func()) {
	tmpDir, err := os.MkdirTemp("", "dkv-bench-*")
	if err != nil {
		b.Fatal(err)
	}

	builder := NewEngineBuilder().
		Default().
		SetWalPath(tmpDir).
		SetSnpPath(tmpDir + "/snp.bin").
		SetWalInterval(time.Hour).
		SetSnpInterval(time.Hour).
		SetWalBufferSize(1024 * 1024).
		SetWalSegments(4).
		SetInsecure()

	if !distributed {
		builder.SingleNode()
	} else {
		builder.SetGossipInterval(10 * time.Second)
	}

	eng, err := builder.Build()
	if err != nil {
		b.Fatal(err)
	}

	eng.Start()
	cleanup := func() {
		eng.Stop()
		_ = os.RemoveAll(tmpDir)
	}
	return eng, cleanup
}

func BenchmarkEngine_Set(b *testing.B) {
	eng, cleanup := setupBenchmarkEngine(b, false)
	defer cleanup()
	val := []byte("value-data-12345")
	for b.Loop() {
		_ = eng.Set("key", val)
	}
}

func BenchmarkEngine_Get(b *testing.B) {
	eng, cleanup := setupBenchmarkEngine(b, false)
	defer cleanup()
	_ = eng.Set("key", []byte("val"))
	for b.Loop() {
		_, _ = eng.Get("key")
	}
}

func BenchmarkEngine_Delete(b *testing.B) {
	eng, cleanup := setupBenchmarkEngine(b, false)
	defer cleanup()
	_ = eng.Set("key", []byte("val"))
	for b.Loop() {
		_ = eng.Delete("key")
	}
}

func BenchmarkEngine_Set_Parallel(b *testing.B) {
	eng, cleanup := setupBenchmarkEngine(b, false)
	defer cleanup()

	const numKeys = 10000
	keys := make([]string, numKeys)
	for i := range numKeys {
		keys[i] = fmt.Sprintf("key-%d", i)
	}

	val := make([]byte, 512)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_ = eng.Set(keys[i%numKeys], val)
			i++
		}
	})
}

func BenchmarkEngine_Get_Parallel(b *testing.B) {
	eng, cleanup := setupBenchmarkEngine(b, false)
	defer cleanup()

	const numKeys = 10000
	keys := make([]string, numKeys)
	val := []byte("val")
	for i := range numKeys {
		keys[i] = fmt.Sprintf("key-%d", i)
		_ = eng.Set(keys[i], val)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, _ = eng.Get(keys[i%numKeys])
			i++
		}
	})
}

func BenchmarkEngine_Delete_Parallel(b *testing.B) {
	eng, cleanup := setupBenchmarkEngine(b, false)
	defer cleanup()

	const numKeys = 10000
	keys := make([]string, numKeys)
	for i := range numKeys {
		keys[i] = fmt.Sprintf("key-%d", i)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_ = eng.Delete(keys[i%numKeys])
			i++
		}
	})
}

func BenchmarkEngine_PayloadSizes(b *testing.B) {
	sizes := []int{128, 4096}
	if !testing.Short() {
		sizes = append(sizes, 1024*1024) // 1MB
	}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("Size-%d", size), func(b *testing.B) {
			eng, cleanup := setupBenchmarkEngine(b, false)
			defer cleanup()
			val := make([]byte, size)
			for b.Loop() {
				_ = eng.Set("key", val)
			}
		})
	}
}
