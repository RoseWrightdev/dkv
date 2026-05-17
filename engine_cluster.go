package dkv

import (
	"fmt"
	"slices"
	"time"
)

func (eng *engine) isLocal(key string) bool {
	if eng.clusterConfig.SingleNode {
		return true
	}
	rf := eng.clusterConfig.ReplicationFactor
	if rf <= 0 {
		rf = 1
	}

	// In a distributed cluster, we are responsible if we are one of the N owners
	owners := eng.cluster.GetOwners(key, rf)
	return slices.Contains(owners, eng.clusterConfig.NodeID)
}

func (eng *engine) Owner(key Key) NodeID {
	if eng.clusterConfig.SingleNode {
		return eng.clusterConfig.NodeID
	}
	return eng.cluster.Owner(key)
}

func (eng *engine) getCachedClient(addr string) (*Client, error) {
	eng.clientMu.RLock()
	if eng.clients == nil {
		eng.clientMu.RUnlock()
		return nil, fmt.Errorf("engine is stopped")
	}
	client, ok := eng.clients[addr]
	eng.clientMu.RUnlock()
	if ok {
		return client, nil
	}

	eng.clientMu.Lock()
	defer eng.clientMu.Unlock()
	if eng.clients == nil {
		return nil, fmt.Errorf("engine is stopped")
	}
	// Double check
	if client, ok = eng.clients[addr]; ok {
		return client, nil
	}

	client, err := NewClient(addr, 1*time.Second, eng.creds)
	if err != nil {
		return nil, err
	}
	eng.clients[addr] = client
	return client, nil
}

// Get retrieves the value associated with a key from the sharded map.
func (eng *engine) Get(key Key) ([]byte, bool) {
	hash := hashKey(hashFunc(key))
	iv, ok := eng.hm.Load(key, hash)
	if ok && !iv.Tombstone {
		eng.evictionService.publish(key, hash)
		return iv.Data, true
	} else if ok && iv.Tombstone {
		// We have a local tombstone. Do not proxy the read as the key is known to be deleted.
		return nil, false
	}

	// Gateway Proxy: If we don't have it locally, fetch it from an owner
	if !eng.clusterConfig.SingleNode {
		rf := eng.clusterConfig.ReplicationFactor
		if rf <= 0 {
			rf = 1
		}
		owners := eng.cluster.GetOwners(key, rf)

		for _, owner := range owners {
			if owner == eng.clusterConfig.NodeID {
				continue // We already checked local storage
			}

			addr := eng.cluster.AddressForNode(owner)
			if addr == "" {
				continue // Try next owner
			}

			// Proxy the read
			client, err := eng.getCachedClient(addr)
			if err != nil {
				continue // Try next owner
			}
			val, ok, err := client.Get(key)
			if err != nil || !ok {
				continue
			}
			return val, true
		}
	}

	return nil, false
}
