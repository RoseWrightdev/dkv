package core_test

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"

	pebble "github.com/cockroachdb/pebble"
	badger "github.com/dgraph-io/badger/v4"
	nutsdb "github.com/nutsdb/nutsdb"
	"github.com/rosewrightdev/oryx"
	"github.com/rosewrightdev/oryx/kv"
	bbolt "go.etcd.io/bbolt"
)

func BenchmarkComparative_Get_Parallel(b *testing.B) {
	keys := make([]kv.Key, 1000)
	for i := range keys {
		keys[i] = kv.Key(fmt.Sprintf("key_%d", i))
	}

	// 1. Setup Oryx Database (Sharded Core + WAL)
	oryxEng, err := oryx.NewDatabaseBuilder().Default().
		SetWalPath(b.TempDir() + "/wal").
		SetSnpPath(b.TempDir() + "/snapshot.bin").
		SetInsecure().
		SingleNode().
		Build()
	if err != nil {
		b.Fatalf("failed to build Oryx DB: %v", err)
	}
	oryxEng.Start()
	defer oryxEng.Stop()

	for i := range keys {
		_ = oryxEng.Set(keys[i], []byte(fmt.Sprintf("val_%d", i)))
	}

	// 2. Setup BadgerDB (Dgraph LSM Engine)
	badgerOpts := badger.DefaultOptions(b.TempDir() + "/badger").WithLogger(nil)
	badgerDB, err := badger.Open(badgerOpts)
	if err != nil {
		b.Fatalf("failed to open BadgerDB: %v", err)
	}
	defer func() { _ = badgerDB.Close() }()

	err = badgerDB.Update(func(txn *badger.Txn) error {
		for i := range keys {
			if err := txn.Set([]byte(keys[i]), []byte(fmt.Sprintf("val_%d", i))); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		b.Fatalf("failed to populate BadgerDB: %v", err)
	}

	// 3. Setup bbolt (etcd / Kubernetes B+Tree Engine)
	boltDB, err := bbolt.Open(b.TempDir()+"/bbolt.db", 0600, nil)
	if err != nil {
		b.Fatalf("failed to open bbolt: %v", err)
	}
	defer func() { _ = boltDB.Close() }()

	err = boltDB.Update(func(tx *bbolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte("bucket"))
		if err != nil {
			return err
		}
		for i := range keys {
			if err := bucket.Put([]byte(keys[i]), []byte(fmt.Sprintf("val_%d", i))); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		b.Fatalf("failed to populate bbolt: %v", err)
	}

	// 4. Setup NutsDB (In-Memory + WAL DB)
	nutsOpts := nutsdb.DefaultOptions
	nutsOpts.Dir = b.TempDir() + "/nuts"
	nutsDB, err := nutsdb.Open(nutsOpts)
	if err != nil {
		b.Fatalf("failed to open NutsDB: %v", err)
	}
	defer func() { _ = nutsDB.Close() }()

	_ = nutsDB.Update(func(tx *nutsdb.Tx) error {
		return tx.NewBucket(nutsdb.DataStructureBTree, "bucket")
	})

	err = nutsDB.Update(func(tx *nutsdb.Tx) error {
		for i := range keys {
			if err := tx.Put("bucket", []byte(keys[i]), []byte(fmt.Sprintf("val_%d", i)), 0); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		b.Fatalf("failed to populate NutsDB: %v", err)
	}

	// 5. Setup CockroachDB Pebble (Pebble LSM Engine)
	pebbleDB, err := pebble.Open(b.TempDir()+"/pebble", &pebble.Options{})
	if err != nil {
		b.Fatalf("failed to open Pebble: %v", err)
	}
	defer func() { _ = pebbleDB.Close() }()

	batch := pebbleDB.NewBatch()
	for i := range keys {
		if err := batch.Set([]byte(keys[i]), []byte(fmt.Sprintf("val_%d", i)), pebble.Sync); err != nil {
			b.Fatalf("failed to set pebble batch: %v", err)
		}
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		b.Fatalf("failed to commit pebble batch: %v", err)
	}

	// 6. Setup stdlib sync.Map baseline
	var syncMap sync.Map
	for i := range keys {
		syncMap.Store(string(keys[i]), []byte(fmt.Sprintf("val_%d", i)))
	}

	// Benchmark Oryx
	b.Run("Oryx_Database", func(b *testing.B) {
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			idx := rand.Intn(1000)
			for pb.Next() {
				k := keys[idx%1000]
				_, _ = oryxEng.Get(k)
				idx++
			}
		})
	})

	// Benchmark Go stdlib sync.Map
	b.Run("Stdlib_SyncMap_Baseline", func(b *testing.B) {
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			idx := rand.Intn(1000)
			for pb.Next() {
				k := string(keys[idx%1000])
				_, _ = syncMap.Load(k)
				idx++
			}
		})
	})

	// Benchmark BadgerDB
	b.Run("BadgerDB_Dgraph_GoDB", func(b *testing.B) {
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			idx := rand.Intn(1000)
			for pb.Next() {
				k := []byte(keys[idx%1000])
				_ = badgerDB.View(func(txn *badger.Txn) error {
					_, err := txn.Get(k)
					return err
				})
				idx++
			}
		})
	})

	// Benchmark bbolt
	b.Run("bbolt_Etcd_GoDB", func(b *testing.B) {
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			idx := rand.Intn(1000)
			for pb.Next() {
				k := []byte(keys[idx%1000])
				_ = boltDB.View(func(tx *bbolt.Tx) error {
					bkt := tx.Bucket([]byte("bucket"))
					_ = bkt.Get(k)
					return nil
				})
				idx++
			}
		})
	})

	// Benchmark CockroachDB Pebble
	b.Run("CockroachDB_Pebble_GoDB", func(b *testing.B) {
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			idx := rand.Intn(1000)
			for pb.Next() {
				k := []byte(keys[idx%1000])
				val, closer, err := pebbleDB.Get(k)
				if err == nil {
					_ = val
					_ = closer.Close()
				}
				idx++
			}
		})
	})

	// Benchmark NutsDB
	b.Run("NutsDB_Active_GoDB", func(b *testing.B) {
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			idx := rand.Intn(1000)
			for pb.Next() {
				k := []byte(keys[idx%1000])
				_ = nutsDB.View(func(tx *nutsdb.Tx) error {
					_, err := tx.Get("bucket", k)
					return err
				})
				idx++
			}
		})
	})
}
