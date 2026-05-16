# DKV Distributed Evolution Audit (Deep Dive)

This document provides a comprehensive technical audit of the changes made to DKV to transform it from a single-node engine into a distributed, eventually consistent (AP) storage system.

---

## Phase 1: Distributed Foundations (Data Versioning)

### Objective
Establish a mechanism for nodes to distinguish between old and new data without a centralized coordinator.

### Changes & Implementation Process
1.  **Protocol Upgrade (`api/dkv.proto`)**:
    *   Injected `int64 timestamp` into `SetRequest` and `DeleteRequest`.
    *   Regenerated gRPC and Protobuf bindings.
    *   *Rationale*: Distributed systems require a common reference point for ordering events. Physical time (Wall Clock) was chosen for its simplicity in an AP system.
2.  **Internal Storage Refactoring (`shardedMap.go`)**:
    *   Changed storage type from `[]byte` to `Value` struct.
    *   Added `Tombstone` (Tombstone) flag to handle distributed deletions.
    *   *Rationale*: To correctly propagate a delete, we must store the fact that a key was deleted at a specific time; otherwise, an older `Set` might "resurrect" the key during a sync.
3.  **Clock Integration (`clock.go`)**:
    *   Defined `Clock` interface and `MonotonicClock` implementation.
    *   *Implementation*: Used `time.Now().UnixNano()` wrapped with logic to ensure that time never moves backward on a single node (even if the system clock drifts).
4.  **Conflict Resolution**:
    *   Implemented **Last-Writer-Wins (LWW)** in the `engine`.
    *   *Logic*: `if incoming.Timestamp > local.Timestamp { apply() }`.

---

## Phase 2: Cluster Membership & Discovery

### Objective
Allow nodes to form a decentralized cluster and discover each other's API endpoints.

### Changes & Implementation Process
1.  **Memberlist Integration**:
    *   Imported `github.com/hashicorp/memberlist`.
    *   Created `ClusterService` to manage the gossip lifecycle.
2.  **Node Discovery & Metadata**:
    *   *Problem*: `memberlist` uses its own port for gossip, but nodes need to know the gRPC API port of their peers.
    *   *Solution*: Implemented `NodeMeta` delegate to broadcast the gRPC port. Peer discovery now resolves gRPC addresses as `PeerIP:PeerMetaPort`.
3.  **Lifecycle Management**:
    *   Nodes now join the cluster via a `SeedNodes` configuration.
    *   Implemented graceful leave and shutdown to minimize "Dead Node" noise in the cluster.

---

## Phase 3: Replication Logic (Gossip Propagation)

### Objective
Ensure that every write is propagated to the rest of the cluster as quickly as possible.

### Changes & Implementation Process
1.  **Broadcast Pipeline**:
    *   Implemented `memberlist.TransmitLimitedQueue` in `ClusterService`.
    *   Hooked `engine.Set` and `engine.Delete` into the broadcast queue.
2.  **Asynchronous Handling**:
    *   Writes are committed locally to the WAL and memory, then a gossip packet is queued for background transmission.
3.  **Critical Fix: Memory Management & Pooling**:
    *   *Bug*: A race condition was discovered where `pb.SetRequest` objects were being returned to the `sync.Pool` before the gossip layer had finished marshaling them.
    *   *Fix*: Refactored the engine to delay `pool.Put()` until after the `cluster.Broadcast()` call completed.

---

## Phase 4: Anti-Entropy (Self-Healing)

### Objective
Reconcile state drifts caused by network partitions, packet loss, or node restarts.

### Changes & Implementation Process
1.  **Shard Digests**:
    *   Implemented an XOR-based hashing algorithm for shards in `shardedMap`.
    *   *Rationale*: XOR is commutative and associative, allowing for order-independent state summaries.
2.  **Pull/Push Synchronization**:
    *   Nodes periodically (default 10s) pick a random peer.
    *   They exchange shard digests. If a digest mismatch is detected, the node "pulls" the full state for that shard from the peer.
3.  **Conflict Resolution during Sync**:
    *   Synced data is merged using the same LWW/Tombstone logic used for gossip, ensuring that background sync never overwrites newer local data.

---

## Phase 5: Testing & Validation

### Objective
Verify the correctness and performance of the distributed system.

### New Test Suites
1.  **`entropy_test.go`**: Simulates "missed gossip" by writing to a node while another is down, then verifying the background sync heals the cluster upon restart.
2.  **`gossip_test.go`**: Validates rapid propagation of sets and deletes across live nodes.
3.  **`server_test.go`**: Unit tests for the new `Pull` and `Push` gRPC handlers using mocks.
4.  **`integration_test.go`**: Added `TestDistributedCluster` which orchestrates a multi-node DKV cluster in-process.

---

## Current Architecture State

| Property | Implementation |
| :--- | :--- |
| **Consistency Model** | Eventual Consistency (Last-Writer-Wins) |
| **Membership** | Decentralized (Swim/Gossip) |
| **Replication** | Push (Gossip) + Pull (Anti-Entropy) |
| **Data Integrity** | WAL + Snapshotting with Metadata |
| **Conflict Resolution** | Wall-clock timestamps + Tombstones |

### Potential Future Improvements
*   **Merkle Trees**: For more efficient anti-entropy on very large datasets.
*   **Vector Clocks**: For more sophisticated conflict resolution beyond LWW.
*   **Client-Side Load Balancing**: Updating the Go client to handle multiple node addresses natively.

---
*Deep-dive audit completed on May 15, 2026.*
