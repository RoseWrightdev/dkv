package trans

import (
	"bytes"
	"encoding/gob"
	"errors"
	"testing"

	pb "github.com/rosewrightdev/dkv/api"
	"github.com/rosewrightdev/dkv/hashmap"
	"github.com/rosewrightdev/dkv/kv"
	"github.com/rosewrightdev/dkv/security"
	"github.com/stretchr/testify/assert"
)



type failingWriter struct{}

func (failingWriter) Write(_ []byte) (n int, err error) {
	return 0, errors.New("write error")
}

type mockStateTransferWriter struct {
	hm        *hashmap.ShardedMap
	setErr    error
	deleteErr error
}

func (m *mockStateTransferWriter) ApplySet(req *pb.SetRequest) error {
	if m.setErr != nil {
		return m.setErr
	}
	if m.hm != nil {
		m.hm.StoreLWW(req.Key, security.HashFunc(req.Key), kv.Value{Data: req.Value, Timestamp: req.Timestamp})
	}
	return nil
}

func (m *mockStateTransferWriter) ApplyDelete(req *pb.DeleteRequest) error {
	if m.deleteErr != nil {
		return m.deleteErr
	}
	if m.hm != nil {
		m.hm.StoreLWW(req.Key, security.HashFunc(req.Key), kv.Value{Timestamp: req.Timestamp, Tombstone: true})
	}
	return nil
}

func TestStateTransfer_All(t *testing.T) {
	// 1. Test empty import
	stEmpty := NewStateTransfer(hashmap.NewShardedMap(), &mockStateTransferWriter{})
	stEmpty.ImportState(nil)
	stEmpty.ImportState([]byte{})

	// 2. Test successful Export and Import
	hmSrc := hashmap.NewShardedMap()
	hmSrc.StoreLWW("user:1", security.HashFunc("user:1"), kv.Value{Data: []byte("val1"), Timestamp: 100})
	hmSrc.StoreLWW("user:2", security.HashFunc("user:2"), kv.Value{Data: []byte("val2"), Timestamp: 101, Tombstone: true})

	stSrc := NewStateTransfer(hmSrc, &mockStateTransferWriter{})
	data := stSrc.ExportState()
	assert.NotEmpty(t, data)

	hmDst := hashmap.NewShardedMap()
	swDst := &mockStateTransferWriter{hm: hmDst}
	stDst := NewStateTransfer(hmDst, swDst)

	errDecode := stDst.DecodeFromReader(bytes.NewReader(data))
	assert.NoError(t, errDecode)

	// Verify imported data
	v1, ok1 := hmDst.Load("user:1", security.HashFunc("user:1"))
	assert.True(t, ok1)
	assert.Equal(t, []byte("val1"), v1.Data)

	v2, ok2 := hmDst.Load("user:2", security.HashFunc("user:2"))
	assert.True(t, ok2)
	assert.True(t, v2.Tombstone)

	// 3. Test streamToEncoder error (using a failing writer)
	enc := gob.NewEncoder(failingWriter{})
	err := stSrc.StreamToEncoder(enc)
	assert.Error(t, err)

	// 4. Test decodeFromReader invalid gob data
	badReader := bytes.NewReader([]byte("this is not gob data at all!"))
	err = stDst.DecodeFromReader(badReader)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to decode snapshot entry")

	// 5. Test decodeFromReader ApplySet error
	hmErrSet := hashmap.NewShardedMap()
	swErrSet := &mockStateTransferWriter{hm: hmErrSet, setErr: errors.New("apply set error")}
	stErrSet := NewStateTransfer(hmErrSet, swErrSet)
	err = stErrSet.DecodeFromReader(bytes.NewReader(data))
	assert.Error(t, err)
	assert.Equal(t, swErrSet.setErr, err)

	// 6. Test decodeFromReader ApplyDelete error
	hmErrDel := hashmap.NewShardedMap()
	swErrDel := &mockStateTransferWriter{hm: hmErrDel, deleteErr: errors.New("apply delete error")}
	stErrDel := NewStateTransfer(hmErrDel, swErrDel)
	err = stErrDel.DecodeFromReader(bytes.NewReader(data))
	assert.Error(t, err)
	assert.Equal(t, swErrDel.deleteErr, err)
}
