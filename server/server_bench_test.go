package server

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/rosewrightdev/dkv"
	"github.com/rosewrightdev/dkv/gateway"
)

func setupBenchmarkEngine(b *testing.B, distributed bool) (dkv.Engine, func()) {
	tmpDir, err := os.MkdirTemp("", "dkv-bench-*")
	if err != nil {
		b.Fatal(err)
	}

	builder := dkv.NewEngineBuilder().
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

func BenchmarkServer_Get_gRPC(b *testing.B) {
	eng, cleanup := setupBenchmarkEngine(b, false)
	defer cleanup()
	s := NewServer(eng)
	go func() {
		_ = s.Run()
	}()
	defer s.Stop()

	client, _ := gateway.NewInsecureClient(eng.Addr(), time.Second)
	defer func() {
		_ = client.Close()
	}()
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
	go func() {
		_ = s.Run()
	}()
	defer s.Stop()

	client, _ := gateway.NewInsecureClient(eng.Addr(), time.Second)
	defer func() {
		_ = client.Close()
	}()
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
	go func() {
		_ = s.Run()
	}()
	defer s.Stop()

	const numKeys = 10000
	keys := make([]string, numKeys)
	for i := range numKeys {
		keys[i] = fmt.Sprintf("key-%d", i)
	}

	val := []byte("val")
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		client, _ := gateway.NewInsecureClient(eng.Addr(), time.Second)
		defer func() {
			_ = client.Close()
		}()
		i := 0
		for pb.Next() {
			_ = client.Set(keys[i%numKeys], val)
			i++
		}
	})
}
