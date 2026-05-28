package dkv

import (
	"fmt"
	"os"
	"testing"
	"time"

	pb "github.com/rosewrightdev/dkv/api"
)

func BenchmarkWAL_Publish(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "dkv-bench-wal-*")
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		_ = os.RemoveAll(tmpDir)
	}()

	wal, err := newWal(tmpDir, time.Hour, 1024*1024, 4)
	if err != nil {
		b.Fatal(err)
	}
	wal.start()
	defer wal.stop()

	req := pb.SetRequest{Key: "key", Value: []byte("val"), Timestamp: 100}

	
	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		key := fmt.Sprintf("k-%d", i)
		req.Key = key
		_ = wal.publish(key, hashFunc(key), &req)
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

	wal, err := newWal(tmpDir, time.Hour, 1024*1024, 4)
	if err != nil {
		b.Fatal(err)
	}
	wal.start()

	req := pb.SetRequest{Key: "key", Value: []byte("val"), Timestamp: 100}
	for i := range 10000 {
		key := fmt.Sprintf("k-%d", i)
		req.Key = key
		_ = wal.publish(key, hashFunc(key), &req)
	}
	wal.stop()

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		w, _ := newWal(tmpDir, time.Hour, 1024*1024, 4)
		_, _ = w.replay()
		w.stop()
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

	wal, err := newWal(tmpDir, time.Hour, 1024*1024, 4)
	if err != nil {
		b.Fatal(err)
	}
	wal.start()
	defer wal.stop()

	req := pb.SetRequest{Key: "key", Value: []byte("val"), Timestamp: 100}
	for i := range 1000 {
		key := fmt.Sprintf("k-%d", i)
		req.Key = key
		_ = wal.publish(key, hashFunc(key), &req)
	}
	offsets, _ := wal.prepareSnapshot()

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		_ = wal.clear(offsets)
	}
}
