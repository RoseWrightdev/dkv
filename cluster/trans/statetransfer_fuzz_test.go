package trans

import (
	"bytes"
	"encoding/gob"
	"testing"

	pb "github.com/rosewrightdev/dkv/api"
	"github.com/rosewrightdev/dkv/core/hashmap"
	"github.com/rosewrightdev/dkv/core/snap"
)

type fuzzNopWriter struct{}

func (f *fuzzNopWriter) ApplySet(_ *pb.SetRequest) error       { return nil }
func (f *fuzzNopWriter) ApplyDelete(_ *pb.DeleteRequest) error { return nil }

func FuzzDecodeFromReader(f *testing.F) {
	// Build seed corpus with valid gob encoded snapshot entries
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	_ = enc.Encode(&snap.SnapshotEntry{
		Key:       "key1",
		Data:      []byte("val1"),
		Timestamp: 100,
		NodeID:    "node-1",
		Tombstone: false,
	})
	_ = enc.Encode(&snap.SnapshotEntry{
		Key:       "key2",
		Timestamp: 101,
		NodeID:    "node-1",
		Tombstone: true,
	})

	f.Add([]byte{})
	f.Add(buf.Bytes())
	f.Add([]byte("invalid gob header stream data"))

	f.Fuzz(func(t *testing.T, data []byte) {
		hm := hashmap.NewShardedMap()
		st := NewStateTransfer(hm, &fuzzNopWriter{})
		reader := bytes.NewReader(data)

		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("DecodeFromReader panicked on input %x: %v", data, r)
			}
		}()

		_ = st.DecodeFromReader(reader)
	})
}
