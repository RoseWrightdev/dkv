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

func (m *mockEngine) Start() { m.Called() }
func (m *mockEngine) Stop()  { m.Called() }

func (m *mockEngine) Snapshot() error {
	args := m.Called()
	return args.Error(0)
}

func (m *mockEngine) SyncPull(knownDigests map[int32]uint64) ([]*pb.SetRequest, []*pb.DeleteRequest, error) {
	args := m.Called(knownDigests)
	return args.Get(0).([]*pb.SetRequest), args.Get(1).([]*pb.DeleteRequest), args.Error(2)
}

func (m *mockEngine) SyncPush(sets []*pb.SetRequest, deletes []*pb.DeleteRequest) error {
	args := m.Called(sets, deletes)
	return args.Error(0)
}

func TestServerHandlers(t *testing.T) {
	me := new(mockEngine)
	srv := &server{eng: me}

	t.Run("Pull_Success", func(t *testing.T) {
		digests := map[int32]uint64{1: 123}
		sets := []*pb.SetRequest{{Key: "k1", Value: []byte("v1")}}
		deletes := []*pb.DeleteRequest{{Key: "k2"}}

		me.On("SyncPull", digests).Return(sets, deletes, nil).Once()

		resp, err := srv.Pull(context.Background(), &pb.PullRequest{KnownDigests: digests})
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
		me.On("SyncPull", mock.Anything).Return([]*pb.SetRequest{}, []*pb.DeleteRequest{}, assert.AnError).Once()

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
