// Package kv defines the core key-value types and structures for dkv.
package kv

// Key represents a unique identifier for a value in the dkv store.
type Key = string

// HashKey represents the hashed key value
type HashKey = uint64

// Value represents a single record in the database, including metadata for LWW.
type Value struct {
	NodeID    string
	Data      []byte
	Timestamp int64
	Tombstone bool
	ItemHash  uint64
}

// NodeID is a unique identifier for a node in the cluster.
type NodeID string

// SetRequest defines a domain-level write request for setting a key-value pair with timestamp metadata.
type SetRequest struct {
	Key       Key
	Value     []byte
	Timestamp int64
	NodeID    string
}

// DeleteRequest defines a domain-level delete request for marking a key as deleted with timestamp metadata.
type DeleteRequest struct {
	Key       Key
	Timestamp int64
	NodeID    string
}
