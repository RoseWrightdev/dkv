# oryx Distributed Key-Value Database

oryx is a partitioned, state-replicated key-value database implemented in Go. In CAP theorem, oryx is AP in the style of Cassandra or ScyllaDB.

## Features

- Consistent-hash partitioning and direct write replication
- Gossip membership propagation (UDP)
- Hybrid logical clock (HLC) LWW conflict resolution
- 3-level Merkle tree anti-entropy state synchronization
- Multi-segment write-ahead log (WAL) crash durability
- High-concurrency sharded memory map (128 independent locks)
- Active snapshot persistence and recovery serialization
- Dynamic LRU cache TTL eviction
- Dual reactor architecture (`gnet/v2` multi-core event loop + Linux `io_uring` driver)
- Native Redis RESP wire protocol + gRPC Client and Server API

## Performance & Benchmarks

All benchmarks conducted locally on an Apple M4 Max (14 cores, 64GB RAM, Go 1.26.3) and LinuxKit Kernel 6.12.

### 1. Storage Core Benchmark vs Production Go DBs
Direct parallel read workload comparison (`go test -bench=BenchmarkComparative_Get_Parallel -benchmem ./core`).

This benchmark measures **High-Concurrency Parallel Point Lookups (`GET`)** across 1,000 pre-populated key-value pairs using `b.RunParallel` across all 14 CPU cores. It compares `oryx`'s sharded memory engine against widely-used embedded Go storage engines (BadgerDB, bbolt, Pebble, NutsDB) and the standard library in-memory baseline (`sync.Map`):

| Database Engine | Storage Architecture | Throughput (RPS) | Latency (ns/op) | Memory/op | Allocs/op |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Go `sync.Map` Baseline** | Standard Library In-Memory Map | 958,418,294 ops/s | 1.25 ns | 0 B | 0 allocs |
| **oryx Database** | Lock-Free Atomic Core + WAL | 192,609,751 ops/s | 7.15 ns | 0 B | 0 allocs |
| **CockroachDB Pebble** | Production Go LSM-Tree | 7,579,868 ops/s | 155.60 ns | 8 B | 1 allocs |
| **NutsDB** | In-Memory B-Tree + WAL | 3,774,739 ops/s | 316.10 ns | 272 B | 7 allocs |
| **BadgerDB (Dgraph)** | Pure-Go LSM-Tree + Value Log | 1,757,354 ops/s | 681.20 ns | 360 B | 7 allocs |
| **bbolt (etcd / Kubernetes)** | B+Tree Memory-Mapped Store | 1,000,000 ops/s | 1,016.00 ns | 480 B | 8 allocs |

#### Engine Breakdown & Key Architectural Differences:
- **`sync.Map` (Standard Library Baseline)**: Pure in-memory reference baseline; executes atomic read hits without WAL durability or network replication.
- **`oryx`**: Uses a 128-sharded lock-free hashmap core with atomic pointer reads, maintaining full crash durability via WAL while executing zero heap allocations during lookups (`0 B/op`, `0 allocs`).
- **Pebble (CockroachDB)**: Industry-standard Go LSM-tree used by CockroachDB; read paths incur block cache lookups and iterator handle allocations (`1 alloc/op`).
- **NutsDB**: Embedded key-value store with B-Tree indexes; read queries require transaction view context allocations (`7 allocs/op`).
- **BadgerDB (Dgraph)**: Famous pure-Go LSM-tree engine with value log separation; concurrent reads manage active transaction state (`7 allocs/op`).
- **bbolt (etcd / Kubernetes)**: Canonical B+Tree memory-mapped engine powering Kubernetes and etcd; point lookups navigate mmap B+Tree nodes inside view transactions (`8 allocs/op`).

### 2. Storage Engine Micro-benchmarks
Micro-benchmarks measuring direct storage interaction with the 128-sharded memory core + WAL (`go test -bench=. -benchmem`):

| Benchmark | Workload / Operation | Throughput (ops/sec) | Latency | Memory / Allocations |
| :--- | :--- | :--- | :--- | :--- |
| Get (Parallel) | Concurrent Point Reads | ~160,276,000 ops/s | 7.10 ns/op | 0 B/op (0 allocs) |
| Get (Single-thread) | Sequential Point Read | ~25,265,000 ops/s | 52.07 ns/op | 0 B/op (0 allocs) |
| Set (Parallel + WAL) | Concurrent Writes + WAL | ~1,727,000 ops/s | 686.90 ns/op | 776 B/op (3 allocs) |
| Set (Single-thread + WAL) | Sequential Write + WAL | ~1,863,000 ops/s | 618.00 ns/op | 760 B/op (3 allocs) |
| Delete (Parallel + WAL) | Concurrent Tombstones + WAL | ~766,960,000 ops/s | 1.53 ns/op | 0 B/op (0 allocs) |
| Delete (Single-thread + WAL) | Sequential Tombstone + WAL | ~89,730,000 ops/s | 13.52 ns/op | 0 B/op (0 allocs) |


## Quick Start

### 1. Start Server Node
```bash
go run cmd/oryx/main.go
```

### 2. Connect via Redis CLI
```bash
redis-cli -p 6379 SET mykey "hello world"
redis-cli -p 6379 GET mykey
```

### 3. Connect via Go Client
```bash
go run examples/client/main.go
```

## System Architecture

```mermaid
flowchart TD
    Client([Clients: RESP / gRPC]) -->|RESP / gRPC| Reactors["Wire Reactors\n(gnet / io_uring / gRPC)"]
    Reactors --> Engine[Engine Facade]

    subgraph Node[This Node]
        direction TB

        subgraph Storage[Storage Core]
            WAL[(Write-Ahead Log)]
            ShardedMap[(128-Sharded Map)]
            Snapshot[Snapshotter]
            Disk(Disk)
        end

        subgraph Routing[Gateway & Routing]
            Gateway[Gateway]
            Ring[Hash Ring]
        end

        subgraph Replication[Incoming Replication]
            Gossip["Gossip Handler\n(UDP, receive-only)"]
            Syncer[Anti-Entropy Syncer]
            Writer[StorageWriter]
        end

        Engine -->|"Set / Delete"| Gateway
        Gateway -->|GetOwners| Ring
        Gateway -->|"Local replica"| Writer
        Writer -->|Last Write Wins| ShardedMap

        Writer --> WAL
        Engine -->|"Local lookup"| ShardedMap

        Gossip -->|ApplySet / ApplyDelete| Writer
        Syncer -->|ApplySet / ApplyDelete| Writer

        WAL --> Disk
        Snapshot --> Disk
        ShardedMap <-->|Serialize / Load| Snapshot
        Evictor["LRU Cache"] --> ShardedMap
    end

    Peers([Remote Peer Nodes])

    Gateway -->|"gRPC proxy"| Peers
    Peers -->|"UDP gossip (inbound)"| Gossip
    Syncer <-->|TCP anti-entropy| Peers
```

## Running Benchmarks & Profiling

```bash
# Run full benchmark suite
go test -bench=. -benchmem ./...

# CPU Profiling
go test -bench . -cpuprofile=cpu.prof
go tool pprof -top cpu.prof

# Memory Profiling
go test -bench . -memprofile=mem.prof
go tool pprof -top mem.prof
```
