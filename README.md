# Distributed Key-Value Store

dkv is a partitioned, state-replicated key-value database implemented in Go. In CAP theorem, dkv is AP in the style of Cassandra or ScyhllaDB. 

## Features

* Partitioning
* Gossip replication
* Hybrid logical clock (HLC) LWW conflict resolution
* 3-level Merkle tree anti-entropy state synchronization
* Multi-segment write-ahead log (WAL) crash durability
* High-concurrency sharded memory map (128 independent locks)
* Active snapshot persistence and recovery serialization
* Dynamic LRU cache TTL eviction
* gRPC API

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
    Snapshot[Snapshotter Service] -->|Periodic State Flush| MemMap
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
| Get (Parallel) | ~67,522,000 | 14.81 ns/op | 0 B/op (0 allocs) |
| Get (Single-thread) | ~20,157,000 | 49.61 ns/op | 0 B/op (0 allocs) |
| Set (Parallel + WAL) | ~3,032,000 | 329.80 ns/op | 1 B/op (0 allocs) |
| Set (Single-thread + WAL) | ~3,029,000 | 330.10 ns/op | 0 B/op (0 allocs) |
| Delete (Parallel + WAL) | ~2,587,000 | 386.50 ns/op | 1 B/op (0 allocs) |
| Delete (Single-thread + WAL) | ~2,242,000 | 445.90 ns/op | 0 B/op (0 allocs) |

### 2. Multi-tier Merkle Tree & Anti-Entropy Sync
Reconciliation and anti-entropy sync performance across node boundaries:

| Operation | Latency | Allocations | Key Insight |
| :--- | :--- | :--- | :--- |
| Root Digest Generation | 263.50 ns | 0 B/op (0 allocs) | Global state integrity checked in fraction of a microsecond |
| Fill Shard Digests | 738.00 ns | 0 B/op (0 allocs) | Builds intermediate 128-sharding bounds with zero allocations |
| Sync Pull (Identical States) | 271.60 ns | 0 B/op (0 allocs) | Zero-copy validation when nodes are fully synchronized |
| Sync Pull (Single Mismatch) | 5.08 μs | 480 B/op (5 allocs) | Rapid single-shard branch pruning for minor state drift |
| Sync Pull (Full Divergence) | 36.36 μs | 35,672 B/op (285 allocs) | High-concurrency heavy synchronization of heavily drifted states |

### 3. Payload Size Scalability
Measures direct `Set` latency scaling under varying key-value payload sizes:

* Small Payload (128 Bytes): 352.80 ns/op (Zero allocations)
* Medium Payload (4 KB): 1.10 μs/op (Zero allocations)
* Large Payload (1 MB): 186.43 μs/op (166 B/op, 0 allocs) — *Maintains zero heap allocation escaping!*

### 4. Snapshotting & Recovery Durability
Measures background disk serialization and startup WAL replay times:

* State Snapshotting: ~76.90 ms to serialize full memory database (2,210 B/op, 26 allocations)
* Full Crash Recovery: ~5.34 ms to load Gob snapshots and fully replay segment logs from disk (reconstitutes 115,639 memory allocations safely)

---

To run the full benchmark suite locally:
```bash
go test -bench=. -benchmem ./...
```


