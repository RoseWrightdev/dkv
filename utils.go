package dkv

import (
	"crypto/sha256"
	"encoding/binary"

	"github.com/cespare/xxhash/v2"
)

// hashFunc implements the extremely fast xxhash algorithm for strings.
func hashFunc(key string) hashKey {
	return xxhash.Sum64String(key)
}

// hashFuncSecure implements a cryptographically secure SHA-256 hash function, returning a uint64 hash.
func hashFuncSecure(key string) uint64 {
	h := sha256.Sum256([]byte(key))
	return binary.BigEndian.Uint64(h[:8])
}

// hashBytes implements the extremely fast xxhash algorithm for byte arrays.
func hashBytes(data []byte) uint64 {
	return xxhash.Sum64(data)
}
