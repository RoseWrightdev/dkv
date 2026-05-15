package dkv

// hashFunc implements the FNV-1a hash algorithm.
func hashFunc(key string) hashKey {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	var hash hashKey = offset64
	for i := 0; i < len(key); i++ {
		hash ^= hashKey(key[i])
		hash *= prime64
	}
	return hash
}
