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

The dkv engine is benchmarked locally using Go's built-in testing framework:

| Benchmark | Throughput | Latency / Allocations |
| :--- | :--- | :--- |
| **Engine Get** (Parallel) | ~60,400,000 ops/sec | 16.5 ns/op (0 B/op) |
| **Engine Set** (Parallel + WAL) | ~3,030,000 ops/sec | 330 ns/op (1 B/op) |
| **Consistent Hashing Node Lookup** | ~18,300,000 ops/sec | 54 ns/op (0 B/op) |
| **Merkle Tree Root Digest Generation** | ~3,860,000 ops/sec | 259 ns/op (0 B/op) |
| **Engine Get** (Single-threaded) | ~19,300,000 ops/sec | 52 ns/op (0 B/op) |
| **Engine Set** (Single-threaded + WAL) | ~3,060,000 ops/sec | 326 ns/op (0 B/op) |

To run the full benchmark suite:
```bash
go test -bench=. -benchmem
```
