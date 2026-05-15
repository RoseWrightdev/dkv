package dkv

import (
	"fmt"
	"net"
	"os"
	"testing"
	"time"
)

func BenchmarkEngine_Set(b *testing.B) {
	tmpDir, _ := os.MkdirTemp("", "dkv-bench-*")
	defer os.RemoveAll(tmpDir)
	eng, _ := newEngine(EngineConfig{
		walPath: tmpDir + "/wal.bin", sssPath: tmpDir + "/sss.gob",
		walSyncInterval: time.Hour, sssInterval: time.Hour,
		walBufferSize: 1024 * 1024, evictionService: NewLRU(1000000, time.Hour),
	})
	eng.Start()
	defer eng.Stop()

	const keyCount = 1000
	keys := make([]string, keyCount)
	for i := range keyCount {
		keys[i] = fmt.Sprintf("key-%d", i)
	}
	val := []byte("value-data-12345")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = eng.Set(keys[i%keyCount], val)
	}
}

func BenchmarkEngine_Get(b *testing.B) {
	tmpDir, _ := os.MkdirTemp("", "dkv-bench-*")
	defer os.RemoveAll(tmpDir)
	eng, _ := newEngine(EngineConfig{
		walPath: tmpDir + "/wal.bin", sssPath: tmpDir + "/sss.gob",
		walSyncInterval: time.Hour, sssInterval: time.Hour,
		evictionService: NewLRU(1000000, time.Hour),
	})
	eng.Start()
	defer eng.Stop()

	eng.Set("key", []byte("val"))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = eng.Get("key")
	}
}

func BenchmarkLRU_Seen(b *testing.B) {
	lru := NewLRU(1000000, time.Hour)
	const keyCount = 1000
	keys := make([]string, keyCount)
	for i := range keyCount {
		keys[i] = fmt.Sprintf("key-%d", i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lru.seen(keys[i%keyCount])
	}
}

func BenchmarkEngine_Set_Parallel(b *testing.B) {
	tmpDir, _ := os.MkdirTemp("", "dkv-bench-p-*")
	defer os.RemoveAll(tmpDir)
	eng, _ := newEngine(EngineConfig{
		walPath: tmpDir + "/wal.bin", sssPath: tmpDir + "/sss.gob",
		walSyncInterval: time.Hour, sssInterval: time.Hour,
		walBufferSize: 1024 * 1024, evictionService: NewLRU(1000000, time.Hour),
	})
	eng.Start()
	defer eng.Stop()

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
	tmpDir, _ := os.MkdirTemp("", "dkv-bench-p-*")
	defer os.RemoveAll(tmpDir)
	eng, _ := newEngine(EngineConfig{
		walPath: tmpDir + "/wal.bin", sssPath: tmpDir + "/sss.gob",
		walSyncInterval: time.Hour, sssInterval: time.Hour,
		evictionService: NewLRU(1000000, time.Hour),
	})
	eng.Start()
	defer eng.Stop()
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
			tmpDir, _ := os.MkdirTemp("", "dkv-size-*")
			defer os.RemoveAll(tmpDir)
			eng, _ := newEngine(EngineConfig{
				walPath: tmpDir + "/wal.bin", sssPath: tmpDir + "/sss.gob",
				walSyncInterval: time.Hour, sssInterval: time.Hour,
				evictionService: NewLRU(1000000, time.Hour),
			})
			eng.Start()
			defer eng.Stop()
			val := make([]byte, size)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = eng.Set("key", val)
			}
		})
	}
}

func BenchmarkServer_GetSet_gRPC_Parallel(b *testing.B) {
	tmpDir, _ := os.MkdirTemp("", "dkv-grpc-p-*")
	defer os.RemoveAll(tmpDir)
	eng, _ := newEngine(EngineConfig{
		walPath: tmpDir + "/wal.bin", sssPath: tmpDir + "/sss.gob",
		walSyncInterval: time.Hour, sssInterval: time.Hour,
		walBufferSize: 1024 * 1024, evictionService: NewLRU(100000, time.Hour),
	})
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
