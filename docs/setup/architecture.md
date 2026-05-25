# Architecture Overview

Surgewave is designed as a modular, high-performance message broker with multiple protocol support and pluggable storage backends.

## High-Level Architecture

```mermaid
flowchart TB
  subgraph Clients["Clients"]
    C1[Kafka Clients]
    C2[.NET Native]
    C3[gRPC Clients]
    C4[CLI]
  end
  subgraph Transport["Transport Layer"]
    T1["TCP (Kafka)"]
    T2["TCP (Native)"]
    T3[gRPC]
    T4[SharedMemory]
  end
  subgraph Protocol["Protocol Layer"]
    P1[Kafka Protocol]
    P2[Native Binary]
    P3[gRPC/Protobuf]
  end
  subgraph Core["Broker Core"]
    K1[Topic Manager]
    K2[Consumer Groups]
    K3[Transactions]
    K4[ACLs]
    K5[Schema Registry]
    K6[Connect Workers]
    K7[Streams Processing]
  end
  subgraph Storage["Storage Layer"]
    S1[Memory]
    S2[FileSystem]
    S3[Apache Arrow]
    S4[Tiered]
  end

  Clients --> Transport --> Protocol --> Core --> Storage
```

## Core Components

### Protocol Layer

| Protocol | Purpose | Performance |
|----------|---------|-------------|
| **Kafka Protocol** | 100% compatibility with Kafka clients | Baseline |
| **Native Protocol** | Maximum performance for .NET clients | much lower latency |
| **gRPC** | Cross-platform, streaming support | 2-3x faster |
| **SharedMemory** | Same-machine IPC | ultra-low latency (target) |

### Storage Engines

| Engine | Persistence | Best For |
|--------|-------------|----------|
| **Memory** | No | Testing, caching |
| **FileSystem** | Yes | General purpose |
| **ZeroCopyWal** | Yes | High performance |
| **Apache Arrow** | Yes | Analytics workloads |
| **Tiered** | Yes | Cost-optimized retention |

### Clustering

Surgewave uses **KRaft** (Kafka Raft) for consensus:

```mermaid
flowchart TB
  subgraph Cluster["Surgewave Cluster"]
    B1["Broker 1 (Leader)"]
    B2["Broker 2 (Follower)"]
    B3["Broker 3 (Follower)"]
    B1 <-->|Raft Consensus| B2
    B2 <-->|Raft Consensus| B3
    B1 <-->|Raft Consensus| B3
  end
  Notes["Topic partitions replicated across brokers<br/>Automatic leader election on failure<br/>In-sync replica (ISR) tracking"]
  Cluster --- Notes
```

## Key Design Decisions

### Zero-Copy I/O

Surgewave minimizes memory copies using:
- `Span<T>` and `Memory<T>` for buffer management
- Memory-mapped files for storage
- `ArrayPool<T>` for allocation reuse

### Lock-Free Structures

Performance-critical paths use:
- `Channel<T>` for async queuing
- `ConcurrentDictionary` for shared state
- Interlocked operations for counters

### Source Generators

Compile-time code generation for:
- Protocol serialization (Kafka wire format)
- Configuration binding
- Regex patterns

## Request Flow

### Produce Request

```
1. Client sends ProduceRequest
2. Protocol layer deserializes
3. Broker validates (ACL, schema)
4. Storage engine appends to partition
5. Replication to followers (if clustered)
6. Response sent to client
```

### Consume Request

```
1. Client sends FetchRequest
2. Broker checks consumer group membership
3. Storage engine reads from partition
4. Optional: decompress, schema decode
5. Response with messages sent to client
6. Offset committed (if auto-commit)
```

## Extension Points

- **Storage Engines**: Implement `ISurgewaveStorageEngine`
- **Protocol Handlers**: Implement protocol-specific handlers
- **Connect Connectors**: Implement `ISourceConnector` / `ISinkConnector`
- **Schema Handlers**: Implement `ISchemaHandler` for new formats
