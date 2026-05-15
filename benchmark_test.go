package dkv

import (
	"fmt"
	"net"
	"os"
	"testing"
	"time"
)

func setupBenchmarkEngine(b *testing.B) (Engine, func()) {
	tmpDir, err := os.MkdirTemp("", "dkv-bench-*")
	if err != nil {
		b.Fatal(err)
	}

	eng, err := NewEngineBuilder().
		SetWalPath(tmpDir).
		SetSssPath(tmpDir + "/sss.gob").
		SetWalSyncInterval(time.Hour).
		SetSssInterval(time.Hour).
		SetWalBufferSize(1024 * 1024).
		SetEvictionService(NewLRU(LRUConfig{Capacity: 1000000, TTL: time.Hour, ShardCount: 16})).
		SetWalSegments(16).
		GetEngine()
	if err != nil {
		b.Fatal(err)
	}

	eng.Start()

	cleanup := func() {
		eng.Stop()
		os.RemoveAll(tmpDir)
	}

	return eng, cleanup
}

func BenchmarkEngine_Set(b *testing.B) {
	eng, cleanup := setupBenchmarkEngine(b)
	defer cleanup()

	const keyCount = 1000
	keys := make([]string, keyCount)
	for i := range keyCount {
		keys[i] = fmt.Sprintf("key-%d", i)
	}
	val := []byte("value-data-12345")

	
	for i := 0; b.Loop(); i++ {
		_ = eng.Set(keys[i%keyCount], val)
	}
}

func BenchmarkEngine_Get(b *testing.B) {
	eng, cleanup := setupBenchmarkEngine(b)
	defer cleanup()

	eng.Set("key", []byte("val"))
	
	for b.Loop() {
		_, _ = eng.Get("key")
	}
}

func BenchmarkLRU_Seen(b *testing.B) {
	lru := NewLRU(LRUConfig{Capacity: 1000000, TTL: time.Hour, ShardCount: 16})
	const keyCount = 1000
	keys := make([]string, keyCount)
	for i := range keyCount {
		keys[i] = fmt.Sprintf("key-%d", i)
	}
	
	for i := 0; b.Loop(); i++ {
		key := keys[i%keyCount]
		lru.seen(key, hashFunc(key))
	}
}

func BenchmarkEngine_Set_Parallel(b *testing.B) {
	eng, cleanup := setupBenchmarkEngine(b)
	defer cleanup()

	const keyCount = 100000
	keys := make([]string, keyCount)
	for i := range keyCount {
		keys[i] = fmt.Sprintf("key-%d", i)
	}
	val := make([]byte, 1024)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_ = eng.Set(keys[i%keyCount], val)
			i++
		}
	})
}

func BenchmarkEngine_Get_Parallel(b *testing.B) {
	eng, cleanup := setupBenchmarkEngine(b)
	defer cleanup()
	eng.Set("key", []byte("val"))

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = eng.Get("key")
		}
	})
}

func BenchmarkEngine_PayloadSizes(b *testing.B) {
	sizes := []int{128, 1024, 1024 * 1024}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("Size-%d", size), func(b *testing.B) {
			eng, cleanup := setupBenchmarkEngine(b)
			defer cleanup()
			val := make([]byte, size)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = eng.Set("key", val)
			}
		})
	}
}

func BenchmarkServer_GetSet_gRPC_Parallel(b *testing.B) {
	eng, cleanup := setupBenchmarkEngine(b)
	defer cleanup()
	s := NewServer(eng)
	lis, _ := net.Listen("tcp", "127.0.0.1:0")
	go s.Run(lis)
	defer s.Stop()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		client, _ := NewInsecureClient(lis.Addr().String(), time.Second)
		defer client.Close()
		val := []byte("val")
		for pb.Next() {
			_ = client.Set("key", val)
			_, _, _ = client.Get("key")
		}
	})
}
