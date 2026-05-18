# Distributed Key-Value Store

dkv is a partitioned, state-replicated key-value database implemented in Go. In CAP theorem, dkv is AP in the style of Cassandra or ScyhllaDB. 

## Features

* Consistent-hash partitioning
* Real-time gossip replication
* Hybrid logical clock (HLC) conflict resolution
* 3-level Merkle tree anti-entropy state synchronization
* Multi-segment write-ahead log (WAL) crash durability
* High-concurrency sharded memory map (128 independent locks)
* Active snapshot persistence and recovery serialization
* Dynamic LRU cache eviction (capacity and TTL modes)
* Strongly-typed gRPC communication API

## System Architecture

```mermaid
flowchart TD
    Client([gRPC Client]) -->|gRPC Requests| Server[gRPC Server]
    Server -->|Engine Interface| Engine[engine Facade]
    
    %% Routing Decisions
    Engine -->|Consistent Hashing| Ring[HashRing]
    Ring -->|GetOwners / Node Lookup| OwnerCheck{Is Key Local?}
    
    %% Proxy path
    OwnerCheck -->|No| ClientCache[ClientCache Pool]
    ClientCache -->|gRPC Proxy Call| PeerNode[Remote Peer Node]
    
    %% Local Path
    OwnerCheck -->|Yes| LocalStore[Local State Operations]
    Engine -->|Coordinates| LocalStore
    
    %% Storage Core Stack (Vertically Aligned)
    LocalStore -->|Durability| WAL[(Write-Ahead Log)]
    LocalStore -->|In-Memory Storage| MemMap[(128-Sharded Map)]
    LocalStore -->|Eviction Callback| LRU[LRU Cache Service]
    
    WAL -->|Publish / Replay| MemMap
    Snapshot[Snapshoter Service] -->|Periodic State Flush| MemMap
    HLC[Hybrid Logical Clock] -.->|Vector Timestamp| MemMap
    
    %% Replication & AE
    MemMap -.->|Mesh Interface| Gossip[Gossip Service]
    MemMap -.->|Syncer| AE[Anti-Entropy Service]
    
    Gossip <-->|memberlist Broadcast| PeerNode
    AE <-->|Merkle Tree Sync| PeerNode
```

## Quick Start

Start a dkv server node:
```bash
go run examples/server/main.go
```

In a separate terminal, run the client example to set, get, and delete values:
```bash
go run examples/client/main.go
```

## Performance & Benchmarks

The dkv engine is benchmarked locally using Go's built-in testing framework on an Apple M4 Max (darwin/arm64, 14 cores, Go 1.26.3):

### 1. Core Storage Engine (Direct CRUD)
Micro-benchmarks measuring direct storage interaction with the 128-sharded memory store and active Write-Ahead Logging (WAL):

| Benchmark | Throughput (ops/sec) | Latency | Allocations |
| :--- | :--- | :--- | :--- |
| Get (Parallel) | ~46,253,000 | 21.62 ns/op | 0 B/op (0 allocs) |
| Get (Single-thread) | ~22,321,000 | 44.80 ns/op | 0 B/op (0 allocs) |
| Set (Parallel + WAL) | ~2,647,000 | 377.70 ns/op | 1 B/op (0 allocs) |
| Set (Single-thread + WAL) | ~2,765,000 | 361.60 ns/op | 0 B/op (0 allocs) |
| Delete (Parallel + WAL) | ~2,182,000 | 458.20 ns/op | 1 B/op (0 allocs) |
| Delete (Single-thread + WAL) | ~2,657,000 | 376.20 ns/op | 0 B/op (0 allocs) |

### 2. Multi-tier Merkle Tree & Anti-Entropy Sync
Reconciliation and anti-entropy sync performance across node boundaries:

| Operation | Latency | Allocations | Key Insight |
| :--- | :--- | :--- | :--- |
| Root Digest Generation | 249.30 ns | 0 B/op (0 allocs) | Global state integrity checked in fraction of a microsecond |
| Fill Shard Digests | 720.80 ns | 0 B/op (0 allocs) | Builds intermediate 128-sharding bounds with zero allocations |
| Sync Pull (Identical States) | 4.35 μs | 0 B/op (0 allocs) | Zero-copy validation when nodes are fully synchronized |
| Sync Pull (Single Mismatch) | 4.85 μs | 480 B/op (5 allocs) | Rapid single-shard branch pruning for minor state drift |
| Sync Pull (Full Divergence) | 42.04 μs | 48,971 B/op (361 allocs) | High-concurrency heavy synchronization of heavily drifted states |

### 3. Payload Size Scalability
Measures direct `Set` latency scaling under varying key-value payload sizes:

* Small Payload (128 Bytes): 378.10 ns/op (Zero allocations)
* Medium Payload (4 KB): 1.52 μs/op (Zero allocations)
* Large Payload (1 MB): 203.41 μs/op (198 B/op, 0 allocs) — *Maintains zero heap allocation escaping!*

### 4. Snapshotting & Recovery Durability
Measures background disk serialization and startup WAL replay times:

* State Snapshotting: ~16.35 ms to serialize full memory database (2,088 B/op, 26 allocations)
* Full Crash Recovery: ~2.90 ms to load Gob snapshots and fully replay segment logs from disk (reconstitutes 65,507 memory allocations safely)

---

To run the full benchmark suite locally:
```bash
go test -bench=. -benchmem ./...
```


