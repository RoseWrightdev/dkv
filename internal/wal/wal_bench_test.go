package wal

import (
	"fmt"
	"os"
	"testing"
	"time"

	pb "github.com/rosewrightdev/dkv/api"
	"github.com/rosewrightdev/dkv/security"
)

func BenchmarkWAL_Publish(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "dkv-bench-wal-*")
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		_ = os.RemoveAll(tmpDir)
	}()

	wal, err := NewWal(tmpDir, time.Hour, 1024*1024, 4)
	if err != nil {
		b.Fatal(err)
	}
	wal.Start()
	defer wal.Stop()

	req := pb.SetRequest{Key: "key", Value: []byte("val"), Timestamp: 100}

	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		key := fmt.Sprintf("k-%d", i)
		req.Key = key
		_ = wal.Publish(key, security.HashFunc(key), &req)
	}
}

func BenchmarkWAL_Replay(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "dkv-bench-wal-replay-*")
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		_ = os.RemoveAll(tmpDir)
	}()

	wal, err := NewWal(tmpDir, time.Hour, 1024*1024, 4)
	if err != nil {
		b.Fatal(err)
	}
	wal.Start()

	req := pb.SetRequest{Key: "key", Value: []byte("val"), Timestamp: 100}
	for i := range 10000 {
		key := fmt.Sprintf("k-%d", i)
		req.Key = key
		_ = wal.Publish(key, security.HashFunc(key), &req)
	}
	wal.Stop()

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		w, _ := NewWal(tmpDir, time.Hour, 1024*1024, 4)
		_, _ = w.Replay()
		w.Stop()
	}
}

func BenchmarkWAL_Clear(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "dkv-bench-wal-clear-*")
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		_ = os.RemoveAll(tmpDir)
	}()

	wal, err := NewWal(tmpDir, time.Hour, 1024*1024, 4)
	if err != nil {
		b.Fatal(err)
	}
	wal.Start()
	defer wal.Stop()

	req := pb.SetRequest{Key: "key", Value: []byte("val"), Timestamp: 100}
	for i := range 1000 {
		key := fmt.Sprintf("k-%d", i)
		req.Key = key
		_ = wal.Publish(key, security.HashFunc(key), &req)
	}
	offsets, _ := wal.PrepareSnapshot()

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		_ = wal.Clear(offsets)
	}
}
