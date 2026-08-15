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

### 1. Wire Protocol Benchmark vs C Redis v8.10.0
`redis-benchmark` comparing oryx against C Redis v8.10.0 on local TCP socket listeners:

| Workload | `redis-benchmark` Command | oryx (Go) | C Redis v8.10.0 | % of C Redis Speed |
| :--- | :--- | :--- | :--- | :--- |
| `GET` (Single Conn) | `redis-benchmark -c 1 -n 50000` | 54,112 req/s | 58,892 req/s | 91.8% |
| `SET` (Single Conn + WAL) | `redis-benchmark -c 1 -n 50000` | 49,019 req/s | 56,179 req/s | 87.2% |
| `GET` (50 Parallel Conns) | `redis-benchmark -c 50 -n 200000` | 152,905 req/s | 228,832 req/s | 66.8% |
| `SET` (50 Parallel Conns + WAL)| `redis-benchmark -c 50 -n 200000` | 145,772 req/s | 211,640 req/s | 68.9% |
| `GET` (Pipelined Batch 16) | `redis-benchmark -c 50 -P 16 -n 500000` | 2,145,922 req/s | 2,732,240 req/s | 78.5% |
| `SET` (Pipelined Batch 16 + WAL)| `redis-benchmark -c 50 -P 16 -n 500000` | 1,785,714 req/s | 1,893,939 req/s | 94.3% |

* Native Linux `io_uring` Throughput: 2,475,247 req/sec (~2.48 Million ops/sec) with 0.173 ms average latency.

### 2. Storage Core Benchmark vs Production Go DBs
Direct storage core comparison (`go test -bench=BenchmarkComparative_Get_Parallel -benchmem`):

| Database Engine | Architecture | Throughput (RPS) | Latency (ns/op) | Memory/op | Allocs/op |
| :--- | :--- | :--- | :--- | :--- | :--- |
| oryx Database | Lock-Free Atomic Core + WAL | 214,141,518 ops/s | 6.08 ns | 0 B | 0 allocs |
| CockroachDB Pebble | Production Go KV Engine | 6,269,592 ops/s | 159.50 ns | 8 B | 1 allocs |
| NutsDB | Active Go In-Memory + WAL DB | 3,756,574 ops/s | 266.20 ns | 272 B | 7 allocs |

### 3. Storage Engine Micro-benchmarks
Micro-benchmarks measuring direct storage interaction with the 128-sharded memory core + WAL (`go test -bench=. -benchmem`):

| Benchmark | Throughput (ops/sec) | Latency | Memory / Allocations |
| :--- | :--- | :--- | :--- |
| Get (Parallel) | ~160,276,000 ops/s | 7.10 ns/op | 0 B/op (0 allocs) |
| Get (Single-thread) | ~25,265,000 ops/s | 52.07 ns/op | 0 B/op (0 allocs) |
| Set (Parallel + WAL) | ~1,727,000 ops/s | 686.90 ns/op | 776 B/op (3 allocs) |
| Set (Single-thread + WAL) | ~1,863,000 ops/s | 618.00 ns/op | 760 B/op (3 allocs) |
| Delete (Parallel + WAL) | ~766,960,000 ops/s | 1.53 ns/op | 0 B/op (0 allocs) |
| Delete (Single-thread + WAL) | ~89,730,000 ops/s | 13.52 ns/op | 0 B/op (0 allocs) |

### 4. Merkle Tree & Sharded Map State Integrity
Reconciliation and anti-entropy digest generation across 128 sharded map buckets (`go test -bench=. -benchmem ./core/hashmap`):

| Operation | Latency | Allocations | Key Insight |
| :--- | :--- | :--- | :--- |
| Root Digest Generation | 199.40 ns/op | 0 B/op (0 allocs) | Global state integrity checked in fraction of a microsecond |
| Fill Shard Digests | 722.70 ns/op | 0 B/op (0 allocs) | Builds intermediate 128-sharding bounds with zero allocations |
| Full State Digest (128 Shards) | 2.05 μs/op | 0 B/op (0 allocs) | Fast multi-bucket digest calculation across entire database state |


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
