package dkv

import (
	"context"
	"fmt"
	"sync"
	"time"

	pb "github.com/rosewrightdev/dkv/api"
	"google.golang.org/grpc/credentials"
)

// Gateway wraps a client cache and consistent hashing routing to proxy
// requests to the appropriate peer nodes.
type Gateway struct {
	cc         *ClientCache
	mesh       Mesher
	meshConfig *MeshConfig
	sw         StateWriter // Set during engine initialization
	pools      *pools      // Memory pools for request allocations
}

// newGateway initializes a new Gateway instance.
func newGateway(mesh Mesher, meshConfig *MeshConfig, pools *pools, creds credentials.TransportCredentials) *Gateway {
	return &Gateway{
		cc:         newClientCache(creds),
		mesh:       mesh,
		meshConfig: meshConfig,
		pools:      pools,
	}
}

// Get queries the consistent hash ring for owner nodes and routes
// the read request to the first reachable peer.
func (g *Gateway) Get(key Key) ([]byte, bool) {
	rf := g.getReplicationFactor()
	owners := g.mesh.GetOwners(key, rf)
	defer g.mesh.PutOwners(owners)

	for _, owner := range owners {
		if owner == g.meshConfig.NodeID {
			continue // We already checked local storage
		}
		if val, ok, err := g.proxyGetRemote(owner, key); err == nil && ok {
			return val, true
		}
	}
	return nil, false
}

// Set queries the hash ring for owners and executes parallel writes to replicas.
func (g *Gateway) Set(key Key, value []byte, ts int64) error {
	rf := g.getReplicationFactor()
	owners := g.mesh.GetOwners(key, rf)
	defer g.mesh.PutOwners(owners)

	if len(owners) == 0 {
		return fmt.Errorf("no replica owners found for key: %s", key)
	}

	var wg sync.WaitGroup
	errChan := make(chan error, len(owners))

	for _, owner := range owners {
		wg.Add(1)
		go func(node NodeID) {
			defer wg.Done()
			var err error
			if node == g.meshConfig.NodeID {
				err = g.applySetLocal(key, value, ts)
			} else {
				err = g.applySetRemote(node, key, value, ts)
			}
			if err != nil {
				errChan <- err
			}
		}(owner)
	}

	wg.Wait()
	close(errChan)

	if len(errChan) == len(owners) {
		return fmt.Errorf("direct write replication failed on all replicas: %v", <-errChan)
	}
	return nil
}

// Delete queries the hash ring for owners and executes parallel deletes to replicas.
func (g *Gateway) Delete(key Key, ts int64) error {
	rf := g.getReplicationFactor()
	owners := g.mesh.GetOwners(key, rf)
	defer g.mesh.PutOwners(owners)

	if len(owners) == 0 {
		return fmt.Errorf("no replica owners found for key: %s", key)
	}

	var wg sync.WaitGroup
	errChan := make(chan error, len(owners))

	for _, owner := range owners {
		wg.Add(1)
		go func(node NodeID) {
			defer wg.Done()
			var err error
			if node == g.meshConfig.NodeID {
				err = g.applyDeleteLocal(key, ts)
			} else {
				err = g.applyDeleteRemote(node, key, ts)
			}
			if err != nil {
				errChan <- err
			}
		}(owner)
	}

	wg.Wait()
	close(errChan)

	if len(errChan) == len(owners) {
		return fmt.Errorf("direct delete replication failed on all replicas: %v", <-errChan)
	}
	return nil
}

// Close gracefully closes all cached gRPC connections.
func (g *Gateway) Close() {
	g.cc.close()
}

// Helper methods for clean request/response proxy routing

func (g *Gateway) getReplicationFactor() int {
	rf := g.meshConfig.ReplicationFactor
	if rf <= 0 {
		return 1
	}
	return rf
}

func (g *Gateway) proxyGetRemote(node NodeID, key Key) ([]byte, bool, error) {
	addr := g.mesh.AddressForNode(node)
	if addr == "" {
		return nil, false, fmt.Errorf("address not found for node %s", node)
	}
	client, err := g.cc.get(addr)
	if err != nil {
		return nil, false, err
	}
	val, ok, err := client.Get(string(key))
	return val, ok, err
}

func (g *Gateway) applySetLocal(key Key, value []byte, ts int64) error {
	req := g.pools.setRequests.Get().(*pb.SetRequest)
	defer g.pools.setRequests.Put(req)
	req.Key = key
	req.Value = value
	req.Timestamp = ts
	req.NodeId = string(g.meshConfig.NodeID)

	err := g.sw.ApplySet(req)
	req.Reset()
	return err
}

func (g *Gateway) applySetRemote(node NodeID, key Key, value []byte, ts int64) error {
	addr := g.mesh.AddressForNode(node)
	if addr == "" {
		return fmt.Errorf("remote replica %s address not found", node)
	}
	client, err := g.cc.get(addr)
	if err != nil {
		return err
	}

	req := g.pools.setRequests.Get().(*pb.SetRequest)
	defer g.pools.setRequests.Put(req)
	req.Key = key
	req.Value = value
	req.Timestamp = ts
	req.NodeId = string(g.meshConfig.NodeID)

	pushReq := &pb.PushRequest{
		Entries: []*pb.SetRequest{req},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err = client.api.Push(ctx, pushReq)
	req.Reset()
	return err
}

func (g *Gateway) applyDeleteLocal(key Key, ts int64) error {
	req := g.pools.deleteRequests.Get().(*pb.DeleteRequest)
	defer g.pools.deleteRequests.Put(req)
	req.Key = key
	req.Timestamp = ts
	req.NodeId = string(g.meshConfig.NodeID)

	err := g.sw.ApplyDelete(req)
	req.Reset()
	return err
}

func (g *Gateway) applyDeleteRemote(node NodeID, key Key, ts int64) error {
	addr := g.mesh.AddressForNode(node)
	if addr == "" {
		return fmt.Errorf("remote replica %s address not found", node)
	}
	client, err := g.cc.get(addr)
	if err != nil {
		return err
	}

	req := g.pools.deleteRequests.Get().(*pb.DeleteRequest)
	defer g.pools.deleteRequests.Put(req)
	req.Key = key
	req.Timestamp = ts
	req.NodeId = string(g.meshConfig.NodeID)

	pushReq := &pb.PushRequest{
		Deletions: []*pb.DeleteRequest{req},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err = client.api.Push(ctx, pushReq)
	req.Reset()
	return err
}
