package dkv

import (
	"context"
	"testing"

	pb "github.com/rosewrightdev/dkv/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type mockEngine struct {
	mock.Mock
}

func (m *mockEngine) Get(key Key) ([]byte, bool) {
	args := m.Called(key)
	if v := args.Get(0); v != nil {
		return v.([]byte), args.Bool(1)
	}
	return nil, args.Bool(1)
}

func (m *mockEngine) Set(key Key, value []byte) error {
	args := m.Called(key, value)
	return args.Error(0)
}

func (m *mockEngine) Delete(key Key) error {
	args := m.Called(key)
	return args.Error(0)
}

func (m *mockEngine) Owner(key Key) NodeID {
	args := m.Called(key)
	return NodeID(args.String(0))
}

func (m *mockEngine) Start() { m.Called() }
func (m *mockEngine) Stop()  { m.Called() }
func (m *mockEngine) Addr() string {
	args := m.Called()
	return args.String(0)
}

func (m *mockEngine) Snapshot() error {
	args := m.Called()
	return args.Error(0)
}

func (m *mockEngine) SyncPull(pullConfig *PullConfig) ([]*pb.SetRequest, []*pb.DeleteRequest, error) {
	args := m.Called(pullConfig.requesterID, pullConfig.root, pullConfig.shards, pullConfig.buckets)
	return args.Get(0).([]*pb.SetRequest), args.Get(1).([]*pb.DeleteRequest), args.Error(2)
}

func (m *mockEngine) SyncPush(sets []*pb.SetRequest, deletes []*pb.DeleteRequest) error {
	args := m.Called(sets, deletes)
	return args.Error(0)
}

func (m *mockEngine) RootDigest() RootDigest {
	return m.Called().Get(0).(RootDigest)
}

func (m *mockEngine) FillShardDigests(dst map[ShardID]Digest) {
	m.Called(dst)
}

func (m *mockEngine) FillDigests(dst map[ShardID]ShardDigest) {
	m.Called(dst)
}

func TestServerHandlers(t *testing.T) {
	me := new(mockEngine)
	srv := &server{
		eng:   me,
		pools: newServerPools(),
	}

	t.Run("Pull_Success", func(t *testing.T) {
		root := RootDigest(12345)
		buckets := map[ShardID]ShardDigest{1: make([]Digest, 64)}
		buckets[1][0] = 123

		sets := []*pb.SetRequest{{Key: "k1", Value: []byte("v1")}}
		deletes := []*pb.DeleteRequest{{Key: "k2"}}

		me.On("SyncPull", mock.Anything, root, mock.Anything, mock.Anything).Return(sets, deletes, nil).Once()

		req := &pb.PullRequest{
			RootDigest:   root,
			ShardDigests: map[uint32]uint64{1: 123},
			SubDigests:   map[uint32]*pb.ShardDigests{1: {SubHashes: buckets[1]}},
		}
		resp, err := srv.Pull(context.Background(), req)
		assert.NoError(t, err)
		assert.Equal(t, sets, resp.Entries)
		assert.Equal(t, deletes, resp.Deletions)
		me.AssertExpectations(t)
	})

	t.Run("Push_Success", func(t *testing.T) {
		sets := []*pb.SetRequest{{Key: "k1"}}
		deletes := []*pb.DeleteRequest{{Key: "k2"}}

		me.On("SyncPush", sets, deletes).Return(nil).Once()

		_, err := srv.Push(context.Background(), &pb.PushRequest{Entries: sets, Deletions: deletes})
		assert.NoError(t, err)
		me.AssertExpectations(t)
	})

	t.Run("Pull_Error", func(t *testing.T) {
		me.On("SyncPull", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*pb.SetRequest{}, []*pb.DeleteRequest{}, assert.AnError).Once()

		_, err := srv.Pull(context.Background(), &pb.PullRequest{})
		assert.Error(t, err)
		me.AssertExpectations(t)
	})

	t.Run("Push_Error", func(t *testing.T) {
		me.On("SyncPush", mock.Anything, mock.Anything).Return(assert.AnError).Once()

		_, err := srv.Push(context.Background(), &pb.PushRequest{})
		assert.Error(t, err)
		me.AssertExpectations(t)
	})
}

func TestServer_PoolDegradation(t *testing.T) {
	me := new(mockEngine)
	srv := &server{
		eng:   me,
		pools: newServerPools(),
	}

	req1 := &pb.PullRequest{
		RootDigest: 12345,
		SubDigests: map[uint32]*pb.ShardDigests{
			1: {SubHashes: make([]Digest, 64)},
		},
	}
	me.On("SyncPull", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*pb.SetRequest{}, []*pb.DeleteRequest{}, nil).Once()
	_, err := srv.Pull(context.Background(), req1)
	assert.NoError(t, err)

	var capturedBuckets map[ShardID]ShardDigest
	me.On("SyncPull", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		capturedBuckets = args.Get(3).(map[ShardID]ShardDigest)
	}).Return([]*pb.SetRequest{}, []*pb.DeleteRequest{}, nil).Once()

	_, err = srv.Pull(context.Background(), &pb.PullRequest{})
	assert.NoError(t, err)

	assert.Equal(t, 128, len(capturedBuckets), "The recycled buckets map should contain all 128 pre-allocated shard entries!")
}
