package gossip

import (
	"errors"
	"testing"

	pb "github.com/rosewrightdev/dkv/api"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

type trackingStateWriter struct {
	appliedSets    []*pb.SetRequest
	appliedDeletes []*pb.DeleteRequest
	setErr         error
	deleteErr      error
}

func (m *trackingStateWriter) ApplySet(req *pb.SetRequest) error {
	m.appliedSets = append(m.appliedSets, req)
	return m.setErr
}

func (m *trackingStateWriter) ApplyDelete(req *pb.DeleteRequest) error {
	m.appliedDeletes = append(m.appliedDeletes, req)
	return m.deleteErr
}

func TestGossip_OnGossip(t *testing.T) {
	t.Run("ApplySet success", func(t *testing.T) {
		writer := &trackingStateWriter{}
		g := NewGossip(writer)

		setReq := &pb.SetRequest{
			Key:       "gossip-key-set",
			Value:     []byte("gossip-value"),
			Timestamp: 12345,
			NodeId:    "node-gossip",
		}
		entry := &pb.WalEntry{
			Entry: &pb.WalEntry_Set{
				Set: setReq,
			},
		}
		data, err := proto.Marshal(entry)
		assert.NoError(t, err)

		g.OnGossip(data)

		assert.Len(t, writer.appliedSets, 1)
		assert.Equal(t, "gossip-key-set", writer.appliedSets[0].Key)
		assert.Equal(t, []byte("gossip-value"), writer.appliedSets[0].Value)
		assert.Equal(t, int64(12345), writer.appliedSets[0].Timestamp)
	})

	t.Run("ApplyDelete success", func(t *testing.T) {
		writer := &trackingStateWriter{}
		g := NewGossip(writer)

		delReq := &pb.DeleteRequest{
			Key:       "gossip-key-del",
			Timestamp: 67890,
			NodeId:    "node-gossip-del",
		}
		entry := &pb.WalEntry{
			Entry: &pb.WalEntry_Delete{
				Delete: delReq,
			},
		}
		data, err := proto.Marshal(entry)
		assert.NoError(t, err)

		g.OnGossip(data)

		assert.Len(t, writer.appliedDeletes, 1)
		assert.Equal(t, "gossip-key-del", writer.appliedDeletes[0].Key)
		assert.Equal(t, int64(67890), writer.appliedDeletes[0].Timestamp)
	})

	t.Run("ApplySet error handling", func(t *testing.T) {
		writer := &trackingStateWriter{setErr: errors.New("write error")}
		g := NewGossip(writer)

		setReq := &pb.SetRequest{
			Key: "gossip-key-set-fail",
		}
		entry := &pb.WalEntry{
			Entry: &pb.WalEntry_Set{
				Set: setReq,
			},
		}
		data, err := proto.Marshal(entry)
		assert.NoError(t, err)

		g.OnGossip(data) // should log error and not panic
		assert.Len(t, writer.appliedSets, 1)
	})

	t.Run("ApplyDelete error handling", func(t *testing.T) {
		writer := &trackingStateWriter{deleteErr: errors.New("delete error")}
		g := NewGossip(writer)

		delReq := &pb.DeleteRequest{
			Key: "gossip-key-del-fail",
		}
		entry := &pb.WalEntry{
			Entry: &pb.WalEntry_Delete{
				Delete: delReq,
			},
		}
		data, err := proto.Marshal(entry)
		assert.NoError(t, err)

		g.OnGossip(data) // should log error and not panic
		assert.Len(t, writer.appliedDeletes, 1)
	})

	t.Run("Invalid data", func(t *testing.T) {
		writer := &trackingStateWriter{}
		g := NewGossip(writer)

		g.OnGossip([]byte("completely-invalid-proto-bytes"))

		assert.Empty(t, writer.appliedSets)
		assert.Empty(t, writer.appliedDeletes)
	})
}
