package dkv

import "github.com/cespare/xxhash/v2"

// todo: consider cryptographic hashing algorithm for routing
// as it is now, dkv is vulnerable to a hashdos

// hashFunc implements the extremely fast xxhash algorithm for strings.
func hashFunc(key string) hashKey {
	return xxhash.Sum64String(key)
}

// hashBytes implements the extremely fast xxhash algorithm for byte arrays.
func hashBytes(data []byte) uint64 {
	return xxhash.Sum64(data)
}
