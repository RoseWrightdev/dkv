package dkv

import (
	"fmt"
	"net"
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
		SetSssPath(tmpDir + "/sss.gob").
		SetWalSyncInterval(time.Hour).
		SetSssInterval(time.Hour).
		SetWalBufferSize(1024 * 1024).
		SetWalSegments(4)

	if !distributed {
		builder.SingleNode()
	} else {
		builder.SetGrpcPort(0).SetGossipInterval(10 * time.Second)
	}

	eng, err := builder.GetEngine()
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
	val := make([]byte, 512)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = eng.Set("key", val)
		}
	})
}

func BenchmarkEngine_Get_Parallel(b *testing.B) {
	eng, cleanup := setupBenchmarkEngine(b, false)
	defer cleanup()
	_ = eng.Set("key", []byte("val"))
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = eng.Get("key")
		}
	})
}

func BenchmarkEngine_Delete_Parallel(b *testing.B) {
	eng, cleanup := setupBenchmarkEngine(b, false)
	defer cleanup()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = eng.Delete("key")
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

func BenchmarkEngine_Snapshot(b *testing.B) {
	eng, cleanup := setupBenchmarkEngine(b, false)
	defer cleanup()
	count := 10000
	if !testing.Short() {
		count = 100000
	}
	for i := range count {
		_ = eng.Set(fmt.Sprintf("k-%d", i), []byte("v"))
	}
	b.ResetTimer()
	for b.Loop() {
		_ = eng.Snapshot()
	}
}

func BenchmarkEngine_Recover(b *testing.B) {
	tmpDir, _ := os.MkdirTemp("", "dkv-rec-*")
	defer os.RemoveAll(tmpDir)
	eng, _ := NewEngineBuilder().Default().SingleNode().SetWalPath(tmpDir).SetSssPath(tmpDir + "/s.bin").GetEngine()
	eng.Start()
	count := 5000
	if !testing.Short() {
		count = 20000
	}
	for i := range count {
		_ = eng.Set(fmt.Sprintf("k-%d", i), []byte("v"))
	}
	_ = eng.Snapshot()
	eng.Stop()

	b.ResetTimer()
	for b.Loop() {
		e, _ := NewEngineBuilder().Default().SingleNode().SetWalPath(tmpDir).SetSssPath(tmpDir + "/s.bin").GetEngine()
		e.Start()
		e.Stop()
	}
}

func BenchmarkServer_Get_gRPC(b *testing.B) {
	eng, cleanup := setupBenchmarkEngine(b, false)
	defer cleanup()
	s := NewServer(eng)
	lis, _ := net.Listen("tcp", "127.0.0.1:0")
	go s.Run(lis)
	defer s.Stop()

	client, _ := NewInsecureClient(lis.Addr().String(), time.Second)
	defer client.Close()
	_ = eng.Set("key", []byte("val"))

	b.ResetTimer()
	for b.Loop() {
		_, _, _ = client.Get("key")
	}
}

func BenchmarkServer_Delete_gRPC(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping heavy gRPC parallel benchmark")
	}
	eng, cleanup := setupBenchmarkEngine(b, false)
	defer cleanup()
	s := NewServer(eng)
	lis, _ := net.Listen("tcp", "127.0.0.1:0")
	go s.Run(lis)
	defer s.Stop()

	client, _ := NewInsecureClient(lis.Addr().String(), time.Second)
	defer client.Close()
	_ = eng.Set("key", []byte("val"))

	b.ResetTimer()
	for b.Loop() {
		_ = client.Delete("key")
	}
}

func BenchmarkServer_Set_gRPC_Parallel(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping heavy gRPC parallel benchmark")
	}
	eng, cleanup := setupBenchmarkEngine(b, true) // Distributed to measure marshaling overhead
	defer cleanup()
	s := NewServer(eng)
	lis, _ := net.Listen("tcp", "127.0.0.1:0")
	go s.Run(lis)
	defer s.Stop()

	val := []byte("val")
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		client, _ := NewInsecureClient(lis.Addr().String(), time.Second)
		defer client.Close()
		for pb.Next() {
			_ = client.Set("key", val)
		}
	})
}
