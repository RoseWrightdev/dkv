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
