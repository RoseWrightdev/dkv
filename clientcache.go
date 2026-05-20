package dkv

import (
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc/credentials"
)

type ClientCache struct {
	creds    credentials.TransportCredentials
	clients  map[PeerAddress]*Client
	clientMu sync.RWMutex
	closed   bool
}

func newClientCache(creds credentials.TransportCredentials) *ClientCache {
	return &ClientCache{clientMu: sync.RWMutex{}, clients: make(map[PeerAddress]*Client), creds: creds}
}

func (cc *ClientCache) get(addr PeerAddress) (*Client, error) {
	cc.clientMu.RLock()
	if cc.closed {
		cc.clientMu.RUnlock()
		return nil, fmt.Errorf("client cache is closed")
	}
	client, ok := cc.clients[addr]
	cc.clientMu.RUnlock()
	if ok {
		return client, nil
	}

	cc.clientMu.Lock()
	defer cc.clientMu.Unlock()

	if cc.closed {
		return nil, fmt.Errorf("client cache is closed")
	}

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
	if cc.closed {
		cc.clientMu.Unlock()
		return
	}
	cc.closed = true
	for _, client := range cc.clients {
		_ = client.Close()
	}
	cc.clientMu.Unlock()
}
