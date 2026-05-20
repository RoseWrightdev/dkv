package dkv

import "google.golang.org/grpc/credentials"

// Gateway wraps a client cache and consistent hashing routing to proxy
// requests to the appropriate peer nodes.
type Gateway struct {
	cc         *ClientCache
	mesh       Mesher
	meshConfig *MeshConfig
}

// newGateway initializes a new Gateway instance.
func newGateway(mesh Mesher, meshConfig *MeshConfig, creds credentials.TransportCredentials) *Gateway {
	return &Gateway{
		cc:         newClientCache(creds),
		mesh:       mesh,
		meshConfig: meshConfig,
	}
}

// Get queries the consistent hash ring for owner nodes and routes
// the read request to the first reachable peer.
func (g *Gateway) Get(key Key) ([]byte, bool) {
	rf := g.meshConfig.ReplicationFactor
	if rf <= 0 {
		rf = 1
	}
	owners := g.mesh.GetOwners(key, rf)
	defer g.mesh.PutOwners(owners)

	for _, owner := range owners {
		if owner == g.meshConfig.NodeID {
			continue // We already checked local storage
		}

		addr := g.mesh.AddressForNode(owner)
		if addr == "" {
			continue // Try next owner
		}

		// Proxy the read to the peer node
		client, err := g.cc.get(addr)
		if err != nil {
			continue // Try next owner
		}
		val, ok, err := client.Get(key)
		if err != nil || !ok {
			continue
		}
		return val, true
	}
	return nil, false
}

// Close gracefully closes all cached gRPC connections.
func (g *Gateway) Close() {
	g.cc.close()
}
