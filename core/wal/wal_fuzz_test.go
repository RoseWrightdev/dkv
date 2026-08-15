package wal

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	pb "github.com/rosewrightdev/oryx/api"
	"github.com/rosewrightdev/oryx/kv"
	"google.golang.org/protobuf/proto"
)

func FuzzWalReplay(f *testing.F) {
	// Build seed corpus with valid WAL segment data
	var buf bytes.Buffer

	entry1, _ := proto.Marshal(&pb.WalEntry{
		Entry: &pb.WalEntry_Set{
			Set: &pb.SetRequest{
				Key:       "user:1",
				Value:     []byte("value1"),
				Timestamp: 100,
				NodeId:    "node-1",
			},
		},
	})
	hdr1 := make([]byte, 4)
	binary.BigEndian.PutUint32(hdr1, uint32(len(entry1)))
	buf.Write(hdr1)
	buf.Write(entry1)

	entry2, _ := proto.Marshal(&pb.WalEntry{
		Entry: &pb.WalEntry_Delete{
			Delete: &pb.DeleteRequest{
				Key:       "user:2",
				Timestamp: 101,
				NodeId:    "node-1",
			},
		},
	})
	hdr2 := make([]byte, 4)
	binary.BigEndian.PutUint32(hdr2, uint32(len(entry2)))
	buf.Write(hdr2)
	buf.Write(entry2)

	f.Add([]byte{})
	f.Add(buf.Bytes())
	f.Add([]byte{0x00, 0x00, 0x00, 0x10, 0xff, 0xff, 0xff, 0xff})

	f.Fuzz(func(t *testing.T, data []byte) {
		tempDir := t.TempDir()
		segPath := filepath.Join(tempDir, "seg_00.log")

		if err := os.WriteFile(segPath, data, 0600); err != nil {
			return
		}

		wal, err := NewWal(tempDir, 100*time.Millisecond, 4096, 1)
		if err != nil {
			return
		}
		defer wal.Stop()

		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("replaySegment panicked on input %x: %v", data, r)
			}
		}()

		results := make(map[kv.Key]kv.Value)
		var mu sync.Mutex
		_ = wal.replaySegment(wal.segments[0], results, &mu)
	})
}
