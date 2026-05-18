package dkv

import (
	"sync"
	"time"

	"google.golang.org/grpc/credentials"
)

type ClientCache struct {
	clientMu sync.RWMutex
	clients  map[PeerAddress]*Client
	creds    credentials.TransportCredentials
}

func newClientCache(creds credentials.TransportCredentials) *ClientCache {
	return &ClientCache{clientMu: sync.RWMutex{}, clients: make(map[PeerAddress]*Client), creds: creds}
}

func (cc *ClientCache) get(addr PeerAddress) (*Client, error) {
	cc.clientMu.RLock()
	client, ok := cc.clients[addr]
	cc.clientMu.RUnlock()
	if ok {
		return client, nil
	}

	cc.clientMu.Lock()
	defer cc.clientMu.Unlock()

	// Double check
	if client, ok = cc.clients[addr]; ok {
		return client, nil
	}

	client, err := NewClient(string(addr), 1*time.Second, cc.creds)
	if err != nil {
		return nil, err
	}
	cc.clients[addr] = client
	return client, nil
}

func (cc *ClientCache) close() {
	cc.clientMu.Lock()
	for _, client := range cc.clients {
		_ = client.Close()
	}
	cc.clients = nil
	cc.clientMu.Unlock()
}
