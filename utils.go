package dkv

import "github.com/cespare/xxhash/v2"

// hashFunc implements the extremely fast xxhash algorithm for strings.
func hashFunc(key string) hashKey {
	return xxhash.Sum64String(key)
}

// hashBytes implements the extremely fast xxhash algorithm for byte arrays.
func hashBytes(data []byte) uint64 {
	return xxhash.Sum64(data)
}
