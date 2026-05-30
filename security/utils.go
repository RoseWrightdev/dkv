// Package security provides secure hashing utilities for keys and state verification.
package security

import (
	"crypto/sha256"
	"encoding/binary"

	"github.com/cespare/xxhash/v2"
	"github.com/rosewrightdev/dkv/kv"
)

// HashFunc implements the extremely fast xxhash algorithm for strings.
func HashFunc(key string) kv.HashKey {
	return xxhash.Sum64String(key)
}

// HashFuncSecure implements a cryptographically secure SHA-256 hash function, returning a uint64 hash.
func HashFuncSecure(key string) uint64 {
	h := sha256.Sum256([]byte(key))
	return binary.BigEndian.Uint64(h[:8])
}

// HashBytes implements the extremely fast xxhash algorithm for byte arrays.
func HashBytes(data []byte) uint64 {
	return xxhash.Sum64(data)
}
