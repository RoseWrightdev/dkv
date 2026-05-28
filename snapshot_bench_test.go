package dkv

import (
	"fmt"
	"os"
	"testing"
)

func BenchmarkSnapshot(b *testing.B) {
	eng, cleanup := setupBenchmarkEngine(b, false)
	defer cleanup()
	count := 10000
	if !testing.Short() {
		count = 50000
	}
	for i := range count {
		_ = eng.Set(fmt.Sprintf("k-%d", i), []byte("v"))
	}
	b.ResetTimer()
	for b.Loop() {
		_ = eng.(*engine).snp.create()
	}
}

func BenchmarkRecover(b *testing.B) {
	tmpDir, _ := os.MkdirTemp("", "dkv-rec-*")
	defer func() {
		_ = os.RemoveAll(tmpDir)
	}()
	eng, _ := NewEngineBuilder().
		Default().
		SingleNode().
		SetWalPath(tmpDir).
		SetSnpPath(tmpDir + "/s.bin").
		SetInsecure().
		Build()
	eng.Start()
	count := 5000
	if !testing.Short() {
		count = 10000
	}
	for i := range count {
		_ = eng.Set(fmt.Sprintf("k-%d", i), []byte("v"))
	}
	_ = eng.(*engine).snp.create()
	eng.Stop()

	b.ResetTimer()
	for b.Loop() {
		e, _ := NewEngineBuilder().
			Default().
			SingleNode().
			SetWalPath(tmpDir).
			SetSnpPath(tmpDir + "/s.bin").
			SetInsecure().
			Build()
		e.Start()
		e.Stop()
	}
}
