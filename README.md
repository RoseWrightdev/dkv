# Distributed Key-Value Store

dkv is a partitioned, state-replicated key-value database implemented in Go. In CAP theorem, dkv is AP in the style of Cassandra or ScyhllaDB. 

## Features

* Consistent-hash partitioning and direct write replication
* Gossip membership propagation
* Hybrid logical clock (HLC) LWW conflict resolution
* 3-level Merkle tree anti-entropy state synchronization
* Multi-segment write-ahead log (WAL) crash durability
* High-concurrency sharded memory map (128 independent locks)
* Active snapshot persistence and recovery serialization
* Dynamic LRU cache TTL eviction
* gRPC Client and Server API

## System Architecture

```mermaid
flowchart TD
    Client([gRPC Client]) -->|gRPC| Engine[Engine Facade]

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

    Gateway -->|"gRPC proxy"| Peers([Remote Peer Nodes])
    Peers -->|"UDP gossip (inbound)"| Gossip
    Syncer <-->|TCP anti-entropy| Peers
```

```mermaid
sequenceDiagram
    participant Client
    participant Engine
    participant Gateway
    participant Writer as StorageWriter
    participant Peers as Remote Peers

    %% Write
    Client->>Engine: Set(key, value)
    Engine->>Engine: HashFunc(key) + clock.Now()
    Engine->>Gateway: Set(key, value, ts)
    Gateway->>Gateway: HashRing.GetOwners(key, RF)
    par to each owner
        Gateway->>Writer: applySetLocal → ApplySet
    and
        Gateway->>Peers: gRPC Push(SetRequest) × (RF-1)
    end
    Gateway-->>Engine: success
    Engine-->>Client: nil

    %% Read
    Client->>Engine: Get(key)
    Engine->>Engine: hm.Load (local lookup)
    alt owner / local hit
        Engine-->>Client: value
    else not owner / miss
        Engine->>Gateway: Get(key)
        Gateway->>Peers: gRPC Get(key)
        Peers-->>Gateway: value
        Gateway-->>Engine: value
        Engine-->>Client: value
    end
```

### Incoming Replication & Conflict Resolution
How external updates are merged into the local node via `StorageWriter` using atomic Last-Write-Wins (LWW).

```mermaid
flowchart LR
    Gossip["Gossip Handler\n(UDP, receive-only)"] -->|Incoming| Writer[StorageWriter]
    Syncer["Anti-Entropy Syncer\n(TCP)"] -->|Incoming| Writer
    StateTransfer["Bulk State Transfer\n(TCP)"] -->|Incoming| Writer
    GatewayLocal["Gateway\n(local replica)"] -->|Incoming| Writer

    subgraph Resolution["Conflict Resolution (per write)"]
        direction TB
        Writer -->|1. Check Ownership| Ring[HashRing]
        Writer -->|2. StoreLWW| LWW["HLC Timestamp\n+ NodeID tiebreak"]
    end

    Writer -->|3. Persist| WAL[(Write-Ahead Log)]
    LWW -->|4. Apply| ShardedMap[(Sharded Map)]
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
| Get (Parallel) | ~68,870,000 | 14.52 ns/op | 0 B/op (0 allocs) |
| Set (Parallel + WAL) | ~2,801,000 | 356.9 ns/op | 1 B/op (0 allocs) |
| Delete (Parallel + WAL) | ~4,288,000 | 233.2 ns/op | 0 B/op (0 allocs) |
| Get (Single-thread) | ~18,729,000 | 53.39 ns/op | 0 B/op (0 allocs) |
| Set (Single-thread + WAL) | ~2,850,000 | 350.8 ns/op | 0 B/op (0 allocs) |
| Delete (Single-thread + WAL) | ~2,185,000 | 457.5 ns/op | 0 B/op (0 allocs) |

### 2. Multi-tier Merkle Tree & Anti-Entropy Sync
Reconciliation and anti-entropy sync performance across node boundaries:

| Operation | Latency | Allocations | Key Insight |
| :--- | :--- | :--- | :--- |
| Root Digest Generation | 257.70 ns | 0 B/op (0 allocs) | Global state integrity checked in fraction of a microsecond |
| Fill Shard Digests | 715.00 ns | 0 B/op (0 allocs) | Builds intermediate 128-sharding bounds with zero allocations |
| Sync Pull (Identical States) | 292.40 ns | 0 B/op (0 allocs) | Zero-copy validation when nodes are fully synchronized |
| Sync Pull (Single Mismatch) | 5.02 μs | 480 B/op (5 allocs) | Rapid single-shard branch pruning for minor state drift |
| Sync Pull (Full Divergence) | 38.48 μs | 36,022 B/op (282 allocs) | High-concurrency heavy synchronization of drifted states |

### 3. Payload Size Scalability
Measures direct `Set` latency scaling under varying key-value payload sizes:

* Small Payload (128 Bytes): 363.80 ns/op (0 B/op, 0 allocs)
* Medium Payload (4 KB): 1.10 μs/op (0 B/op, 0 allocs)
* Large Payload (1 MB): 185.20 μs/op (133 B/op, 0 allocs)

### 4. Snapshotting & Recovery Durability
Measures background disk serialization and startup WAL replay times:

* State Snapshotting: ~50.58 ms to serialize full memory database (2,170 B/op, 26 allocations)
* Full Crash Recovery: ~4.34 ms to load Gob snapshots and fully replay segment logs from disk (reconstitutes 64,885 memory allocations safely)

### 5. Client Caching & Proxy Routing
Measures the internal Gateway cache for multiplexed gRPC connections:

* Gateway Proxy Cache (sync.Map): 8.12 ns/op (16 B/op, 1 allocs)

---

To run the full benchmark suite locally:
```bash
go test -bench=. -benchmem ./...
```

## Profiling

### CPU
```bash
go test -bench . -cpuprofile=cpu.prof
go tool pprof -http=:8000 cpu.prof
```

### Memory
```bash
go test -bench . -memprofile=mem.prof
go tool pprof -http=:8000 mem.prof
