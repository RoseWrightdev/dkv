package dkv

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	pb "github.com/rosewrightdev/dkv/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// MockStateWriter implements StateWriter
type MockStateWriter struct {
	SetErr    error
	DeleteErr error
}

func (m *MockStateWriter) ApplySet(_ *pb.SetRequest) error {
	return m.SetErr
}

func (m *MockStateWriter) ApplyDelete(_ *pb.DeleteRequest) error {
	return m.DeleteErr
}

// MockMesher implements Mesher
type MockMesher struct {
	Owners          []NodeID
	AddrMap         map[NodeID]PeerAddress
	BroadcastCalled bool
}

func (m *MockMesher) Broadcast(_ []byte) {
	m.BroadcastCalled = true
}

func (m *MockMesher) Members() []PeerAddress {
	return nil
}

func (m *MockMesher) Owner(_ Key) NodeID {
	if len(m.Owners) > 0 {
		return m.Owners[0]
	}
	return ""
}

func (m *MockMesher) GetOwners(_ Key, _ int) []NodeID {
	return m.Owners
}

func (m *MockMesher) PutOwners(_ []NodeID) {}

func (m *MockMesher) AddressForNode(nodeID NodeID) PeerAddress {
	return m.AddrMap[nodeID]
}

func (m *MockMesher) start() error {
	return nil
}

func (m *MockMesher) stop() error {
	return nil
}

// MockGrpcServer implements pb.DkvServiceServer
type MockGrpcServer struct {
	pb.UnimplementedDkvServiceServer
	GetFunc  func(req *pb.GetRequest) (*pb.GetResponse, error)
	PushFunc func(req *pb.PushRequest) (*pb.PushResponse, error)
}

func (m *MockGrpcServer) Get(_ context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	if m.GetFunc != nil {
		return m.GetFunc(req)
	}
	return &pb.GetResponse{}, nil
}

func (m *MockGrpcServer) Push(_ context.Context, req *pb.PushRequest) (*pb.PushResponse, error) {
	if m.PushFunc != nil {
		return m.PushFunc(req)
	}
	return &pb.PushResponse{}, nil
}

func TestGateway_AllBranches(t *testing.T) {
	// Start a local gRPC server for testing remote proxying
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = lis.Close() }()

	grpcServer := grpc.NewServer()
	mockSrv := &MockGrpcServer{}
	pb.RegisterDkvServiceServer(grpcServer, mockSrv)

	go func() {
		_ = grpcServer.Serve(lis)
	}()
	defer grpcServer.Stop()

	addr := lis.Addr().String()

	p := newPools()
	// Trigger the uncovered new pool functions in pools.go
	_ = p.walSetWrappers.Get()
	_ = p.walDeleteWrappers.Get()

	// 1. Test Gateway getReplicationFactor edge cases
	mcZero := &MeshConfig{NodeID: "local-node", ReplicationFactor: 0}
	gwZero := newGateway(&NopMesh{}, mcZero, p, insecure.NewCredentials())
	assert.Equal(t, 1, gwZero.getReplicationFactor())

	mcNeg := &MeshConfig{NodeID: "local-node", ReplicationFactor: -5}
	gwNeg := newGateway(&NopMesh{}, mcNeg, p, insecure.NewCredentials())
	assert.Equal(t, 1, gwNeg.getReplicationFactor())

	// 2. Gateway setup with custom mocks
	mc := &MeshConfig{
		NodeID:            "local-node",
		ReplicationFactor: 3,
	}
	mesh := &MockMesher{
		Owners: []NodeID{"local-node", "remote-node"},
		AddrMap: map[NodeID]PeerAddress{
			"remote-node": PeerAddress(addr),
		},
	}
	sw := &MockStateWriter{}

	gw := newGateway(mesh, mc, p, insecure.NewCredentials())
	gw.sw = sw
	defer gw.Close()

	// 3. Test Set success (local and remote)
	mockSrv.PushFunc = func(_ *pb.PushRequest) (*pb.PushResponse, error) {
		return &pb.PushResponse{}, nil
	}
	err = gw.Set("test-key", []byte("val"), time.Now().UnixNano())
	assert.NoError(t, err)

	// 4. Test Set error local
	sw.SetErr = errors.New("local set error")
	err = gw.Set("test-key", []byte("val"), time.Now().UnixNano())
	// Should fail because local set fails, but remote might succeed (or not)
	// Actually, both are called in parallel, if all/any fail depending on complete failure logic:
	// "if len(errChan) == len(owners)" means all replicas must fail for Set to return an error.
	// Since we have 2 owners (local and remote), and only local fails, it shouldn't return error.
	assert.NoError(t, err)

	// 5. Test Set all replicas fail
	sw.SetErr = errors.New("local set error")
	mockSrv.PushFunc = func(_ *pb.PushRequest) (*pb.PushResponse, error) {
		return nil, errors.New("remote set error")
	}
	err = gw.Set("test-key", []byte("val"), time.Now().UnixNano())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "direct write replication failed on all replicas")

	// 6. Test Set with empty owners
	mesh.Owners = nil
	err = gw.Set("test-key", []byte("val"), time.Now().UnixNano())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no replica owners found")
	mesh.Owners = []NodeID{"local-node", "remote-node"} // restore

	// 7. Test Delete success
	sw.SetErr = nil
	sw.DeleteErr = nil
	mockSrv.PushFunc = func(_ *pb.PushRequest) (*pb.PushResponse, error) {
		return &pb.PushResponse{}, nil
	}
	err = gw.Delete("test-key", time.Now().UnixNano())
	assert.NoError(t, err)

	// 8. Test Delete all replicas fail
	sw.DeleteErr = errors.New("local delete error")
	mockSrv.PushFunc = func(_ *pb.PushRequest) (*pb.PushResponse, error) {
		return nil, errors.New("remote delete error")
	}
	err = gw.Delete("test-key", time.Now().UnixNano())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "direct delete replication failed on all replicas")

	// 9. Test Delete empty owners
	mesh.Owners = nil
	err = gw.Delete("test-key", time.Now().UnixNano())
	assert.Error(t, err)
	mesh.Owners = []NodeID{"local-node", "remote-node"} // restore

	// 10. Test Get (from remote peer)
	mockSrv.GetFunc = func(_ *pb.GetRequest) (*pb.GetResponse, error) {
		return &pb.GetResponse{Value: []byte("remote-val"), Exists: true}, nil
	}
	val, ok := gw.Get("test-key")
	assert.True(t, ok)
	assert.Equal(t, []byte("remote-val"), val)

	// 11. Test Get from remote returns not found
	mockSrv.GetFunc = func(_ *pb.GetRequest) (*pb.GetResponse, error) {
		return &pb.GetResponse{Exists: false}, nil
	}
	val, ok = gw.Get("test-key")
	assert.False(t, ok)
	assert.Nil(t, val)

	// 12. Test Gateway proxy methods with address not found
	mesh.AddrMap = nil // clear address mapping to trigger address not found error
	_, _, err = gw.proxyGetRemote("remote-node", "test-key")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "address not found for node")

	err = gw.applySetRemote("remote-node", "test-key", []byte("val"), 0)
	assert.Error(t, err)

	err = gw.applyDeleteRemote("remote-node", "test-key", 0)
	assert.Error(t, err)

	// 13. Test Gateway proxy methods with closed client cache
	mesh.AddrMap = map[NodeID]PeerAddress{
		"remote-node": PeerAddress(addr),
	}
	gw.Close() // close client cache!

	_, _, err = gw.proxyGetRemote("remote-node", "test-key")
	assert.Error(t, err)

	err = gw.applySetRemote("remote-node", "test-key", []byte("val"), 0)
	assert.Error(t, err)

	err = gw.applyDeleteRemote("remote-node", "test-key", 0)
	assert.Error(t, err)
}
