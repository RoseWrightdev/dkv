package snap

import (
	"encoding/gob"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/rosewrightdev/dkv/kv"
)

func BenchmarkSnapshot(b *testing.B) {
	tmpDir, _ := os.MkdirTemp("", "dkv-bench-snap-*")
	defer func() {
		_ = os.RemoveAll(tmpDir)
	}()

	mw := &mockWal{}
	count := 10000
	if !testing.Short() {
		count = 50000
	}

	mockData := make([]SnapshotEntry, count)
	for i := range count {
		mockData[i] = SnapshotEntry{
			Key:       kv.Key(strconv.Itoa(i)),
			Data:      []byte("v"),
			Timestamp: int64(i),
		}
	}

	callBack := func(enc *gob.Encoder) error {
		for _, entry := range mockData {
			if err := enc.Encode(entry); err != nil {
				return err
			}
		}
		return nil
	}

	snp, _ := NewSnapshotter(tmpDir+"/s.bin", 5*time.Minute, mw, callBack)

	b.ResetTimer()
	for b.Loop() {
		_ = snp.Create()
	}
}
