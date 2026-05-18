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
    Client([gRPC Client]) -->|gRPC Requests| Server[gRPC Server Node]
    
    Server -->|Consistent Hash Ring| Ring{Is Key Local?}
    
    Ring -->|No: Proxy Path| Proxy[gRPC Proxy Reader]
    Proxy -->|Double-Checked Pool| ConnCache[(Client Connection Pool)]
    ConnCache -->|gRPC Read| PeerNode[Remote Peer Node]
    
    Ring -->|Yes: Local Path| Storage[Storage Engine Coordinator]
    
    Storage -->|Write Append| WAL[(Write-Ahead Log Segments)]
    Storage -->|Memory Map Store| MemMap[(128 Sharded Memory Map)]
    Storage -->|Eviction Callback| LRU[LRU Cache Service]
    
    HLC[Hybrid Logical Clock] -.->|Generate Vector Timestamp| Storage
    
    MemMap -.->|State Replication| Gossip[HashiCorp memberlist]
    MemMap -.->|Periodic State Sync| AE[Anti-Entropy Service]
    
    Gossip <-->|Real-Time Broadcast| PeerNode
    AE <-->|3-Level Merkle Tree Sync| PeerNode
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
