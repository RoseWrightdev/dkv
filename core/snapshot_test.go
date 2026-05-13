package core

import (
	"testing"

	"google.golang.org/protobuf/proto"
)

type MockWal struct{}

func (mw *MockWal) Publish(msg proto.Message) error {
	return nil
}
func (mw *MockWal) Replay() (*map[Key]Value, error) {
	return nil, nil
}
func (mw *MockWal) clear() error {
	return nil
}
func newMockWal() *MockWal {
	return &MockWal{}
}

func TestNewSnapShotService(t *testing.T) {}
