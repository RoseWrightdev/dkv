package gateway

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	pb "github.com/rosewrightdev/dkv/api"
	"github.com/rosewrightdev/dkv/internal/mesh"
	"github.com/rosewrightdev/dkv/kv"
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
	Owners          []kv.NodeID
	AddrMap         map[kv.NodeID]mesh.PeerAddress
	BroadcastCalled bool
}

func (m *MockMesher) Broadcast(_ []byte) {
	m.BroadcastCalled = true
}

func (m *MockMesher) Members() []mesh.PeerAddress {
	return nil
}

func (m *MockMesher) Owner(_ kv.Key) kv.NodeID {
	if len(m.Owners) > 0 {
		return m.Owners[0]
	}
	return ""
}

func (m *MockMesher) GetOwners(_ kv.Key, _ int) []kv.NodeID {
	return m.Owners
}

func (m *MockMesher) PutOwners(_ []kv.NodeID) {}

func (m *MockMesher) AddressForNode(nodeID kv.NodeID) mesh.PeerAddress {
	return m.AddrMap[nodeID]
}

func (m *MockMesher) Start() error {
	return nil
}

func (m *MockMesher) Stop() error {
	return nil
}

func (m *MockMesher) UpdateLocalWeight(_ int) {}

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

	// 1. Test Gateway getReplicationFactor edge cases
	mcZero := &mesh.MeshConfig{NodeID: "local-node", ReplicationFactor: 0}
	gwZero := NewGateway(&mesh.NopMesh{}, mcZero, insecure.NewCredentials())
	assert.Equal(t, 1, gwZero.getReplicationFactor())

	mcNeg := &mesh.MeshConfig{NodeID: "local-node", ReplicationFactor: -5}
	gwNeg := NewGateway(&mesh.NopMesh{}, mcNeg, insecure.NewCredentials())
	assert.Equal(t, 1, gwNeg.getReplicationFactor())

	// 2. Gateway setup with custom mocks
	mc := &mesh.MeshConfig{
		NodeID:            "local-node",
		ReplicationFactor: 3,
	}
	meshObj := &MockMesher{
		Owners: []kv.NodeID{"local-node", "remote-node"},
		AddrMap: map[kv.NodeID]mesh.PeerAddress{
			"remote-node": mesh.PeerAddress(addr),
		},
	}
	sw := &MockStateWriter{}

	gw := NewGateway(meshObj, mc, insecure.NewCredentials())
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
	meshObj.Owners = nil
	err = gw.Set("test-key", []byte("val"), time.Now().UnixNano())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no replica owners found")
	meshObj.Owners = []kv.NodeID{"local-node", "remote-node"} // restore

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
	meshObj.Owners = nil
	err = gw.Delete("test-key", time.Now().UnixNano())
	assert.Error(t, err)
	meshObj.Owners = []kv.NodeID{"local-node", "remote-node"} // restore

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
	meshObj.AddrMap = nil // clear address mapping to trigger address not found error
	_, _, err = gw.proxyGetRemote("remote-node", "test-key")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "address not found for node")

	err = gw.applySetRemote("remote-node", "test-key", []byte("val"), 0)
	assert.Error(t, err)

	err = gw.applyDeleteRemote("remote-node", "test-key", 0)
	assert.Error(t, err)

	// 13. Test Gateway proxy methods with closed client cache
	meshObj.AddrMap = map[kv.NodeID]mesh.PeerAddress{
		"remote-node": mesh.PeerAddress(addr),
	}
	gw.Close() // close client cache!

	_, _, err = gw.proxyGetRemote("remote-node", "test-key")
	assert.Error(t, err)

	err = gw.applySetRemote("remote-node", "test-key", []byte("val"), 0)
	assert.Error(t, err)

	err = gw.applyDeleteRemote("remote-node", "test-key", 0)
	assert.Error(t, err)
}
