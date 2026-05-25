# Configuration Reference

Complete reference for all Surgewave broker configuration options.

## Configuration File

Surgewave uses `appsettings.json` for configuration:

```json
{
  "Surgewave": {
    // Configuration options here
  }
}
```

## Core Settings

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `BrokerId` | int | 1 | Unique broker identifier |
| `Host` | string | "localhost" | Bind address |
| `Port` | int | 9092 | Kafka protocol port |
| `GrpcPort` | int | 9093 | gRPC API port |
| `DataDirectory` | string | "./data" | Data storage path |
| `LogDirectory` | string | "./logs" | Log files path |

## Network Settings

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `EnableDualMode` | bool | true | Enable dual-stack IPv4+IPv6 binding (see below) |
| `MaxConnectionsPerIp` | int | 100 | Max connections per IP |
| `SocketSendBufferBytes` | int | 102400 | Socket send buffer (bytes) |
| `SocketReceiveBufferBytes` | int | 102400 | Socket receive buffer (bytes) |
| `MaxRequestSize` | int | 104857600 | Max request size (100MB) |

### IPv4/IPv6 Binding

When `Host` is `"localhost"`, the binding behavior depends on `EnableDualMode`:

| `EnableDualMode` | Bind Address | Metadata Host | Accepts |
|------------------|-------------|---------------|---------|
| `true` (default) | `[::]:port` (IPv6Any, DualMode) | `localhost` | IPv4 + IPv6 |
| `false` | `127.0.0.1:port` | `127.0.0.1` | IPv4 only |

When `EnableDualMode` is `false`, the broker automatically advertises `127.0.0.1` in metadata responses instead of `localhost`. This prevents clients (e.g., librdkafka) from resolving `localhost` to `::1` (IPv6) and failing to connect.

**When to disable dual-mode:**
- Environments without IPv6 support (some Docker/CI configurations)
- When IPv6 DNS resolution causes connection timeouts
- Windows environments where dual-stack sockets behave unexpectedly

```json
{
  "Surgewave": {
    "EnableDualMode": false
  }
}
```

Using `SurgewaveRuntime` builder:

```csharp
await using var runtime = await SurgewaveRuntime.CreateBuilder()
    .WithIPv4Only()  // Sets EnableDualMode = false
    .Build()
    .StartAsync();
```

## Storage Settings

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `StorageMode` | string | "File" | Storage backend: File, Memory, ZeroCopyWal, ZeroCopyMemory |
| `LogSegmentBytes` | long | 1073741824 | Segment size (1GB) |
| `LogRetentionHours` | int | 168 | Retention period (7 days) |
| `LogRetentionBytes` | long | -1 | Max bytes per topic (-1 = unlimited) |

## Topic Settings

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `AutoCreateTopics` | bool | true | Auto-create on first produce |
| `DefaultNumPartitions` | int | 1 | Default partition count |
| `DefaultReplicationFactor` | int | 1 | Default replication factor |

### Per-Topic Configuration (via `cleanup.policy`)

| Policy | Description |
|--------|-------------|
| `delete` | Time/size-based retention (default) |
| `compact` | Keep latest value per key |
| `delete,compact` | Compact and also delete old data |
| `ephemeral` | Ring-buffer, no persistence (see below) |

### Ephemeral Topic Settings

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `ephemeral.buffer.bytes` | long | 67108864 | Ring buffer size (64 MB). Supports units: `1MB`, `256MB`, `1GB` |

Ephemeral topics store messages in a fixed-size ring buffer. When full, oldest messages are evicted. Data does not survive broker restarts. Ideal for live dashboards, sensor telemetry, and cache invalidation.

## Producer Settings

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `ProducerBatchSizeBytes` | int | 16384 | Batch size (16KB) |
| `ProducerLingerMs` | int | 5 | Linger time (ms) |
| `ProducerMaxBatchMessages` | int | 10000 | Max messages per batch |

## Native Protocol Settings

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `NativeProtocolCompressionEnabled` | bool | true | Enable compression |
| `SimdBatchThreshold` | int | 1000 | SIMD optimization threshold |

## Channel Pipeline (High-Throughput)

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `UseChannelPipeline` | bool | false | Enable channel pipeline |
| `ChannelWriteWorkers` | int | 4 | Write worker threads |
| `ChannelReadWorkers` | int | 2 | Read worker threads |
| `ChannelWriteBufferSize` | int | 10000 | Write buffer capacity |
| `ChannelWriteBatchSize` | int | 100 | Write batch size |
| `ChannelWriteBatchDelayMs` | int | 1 | Batch delay (ms) |

## Cluster Settings

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `ClusterNodes` | string | "" | Comma-separated broker endpoints |
| `ClusterId` | string | "surgewave-cluster" | Cluster identifier |
| `Rack` | string | null | Rack awareness identifier |

## Replication Settings

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `ReplicationPort` | int | 10092 | Replication port |
| `MinInSyncReplicas` | int | 1 | Minimum ISR count |
| `ReplicaLagTimeMaxMs` | int | 10000 | Max replica lag time |
| `ReplicaLagMaxMessages` | long | 4000 | Max replica lag messages |
| `ReplicaFetchMaxBytes` | int | 1048576 | Max fetch size (1MB) |
| `ReplicaFetchWaitMaxMs` | int | 500 | Max fetch wait time |

## Raft Consensus (KRaft)

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `UseRaftConsensus` | bool | false | Enable Raft consensus |
| `RaftDataDirectory` | string | "./raft" | Raft log directory |
| `RaftElectionTimeoutMinMs` | int | 150 | Min election timeout |
| `RaftElectionTimeoutMaxMs` | int | 300 | Max election timeout |
| `RaftHeartbeatIntervalMs` | int | 50 | Heartbeat interval |

## Heartbeat Settings

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `HeartbeatIntervalMs` | int | 3000 | Heartbeat interval |
| `HeartbeatTimeoutMs` | int | 10000 | Heartbeat timeout |
| `MaxHeartbeatFailures` | int | 3 | Max failures before disconnect |

## Partition Reassignment

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `ReassignmentThrottleBytesPerSec` | long | 52428800 | Throttle rate (50MB/s) |
| `ReassignmentMaxConcurrent` | int | 4 | Max concurrent reassignments |

## Auto-Rebalancing

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `AutoRebalanceEnabled` | bool | false | Enable auto-rebalancing |
| `RebalanceCheckIntervalSeconds` | int | 300 | Check interval (5 min) |
| `RebalanceImbalanceThreshold` | double | 0.1 | Imbalance threshold (10%) |
| `AllowAutoLeaderRebalance` | bool | true | Allow leader rebalancing |
| `LeaderImbalanceCheckIntervalSeconds` | int | 300 | Leader check interval |

## Shutdown Settings

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `ShutdownTimeoutSeconds` | int | 30 | Graceful shutdown timeout |

## Shared Memory Transport

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `SharedMemory.Enabled` | bool | false | Enable shared memory IPC |
| `SharedMemory.BasePath` | string | (platform) | Shared memory base path |
| `SharedMemory.RingBufferCapacity` | int | 16777216 | Ring buffer size (16MB) |
| `SharedMemory.PollingStrategy` | string | "Adaptive" | BusySpin, Sleep, Adaptive |
| `SharedMemory.SpinCount` | int | 1000 | Spin iterations |
| `SharedMemory.IdleSleepMicroseconds` | int | 100 | Sleep duration |
| `SharedMemory.MaxClients` | int | 100 | Max concurrent clients |

## Quotas

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `Quotas.Enabled` | bool | false | Enable quotas |
| `Quotas.ProducerBytesPerSecond` | long | 10485760 | Producer rate (10MB/s) |
| `Quotas.ConsumerBytesPerSecond` | long | 52428800 | Consumer rate (50MB/s) |
| `Quotas.ProducerBurstBytes` | long | 20971520 | Producer burst (20MB) |
| `Quotas.ConsumerBurstBytes` | long | 104857600 | Consumer burst (100MB) |

## Kafka Connect

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `Connect.Enabled` | bool | false | Enable Connect framework |
| `Connect.GroupId` | string | "surgewave-connect" | Connect worker group |
| `Connect.ConfigTopic` | string | "surgewave-connect-configs" | Config storage topic |
| `Connect.OffsetsTopic` | string | "surgewave-connect-offsets" | Offsets topic |
| `Connect.StatusTopic` | string | "surgewave-connect-status" | Status topic |

## Example Configuration

```json
{
  "Surgewave": {
    "BrokerId": 1,
    "Host": "0.0.0.0",
    "Port": 9092,
    "GrpcPort": 9093,
    "DataDirectory": "/var/surgewave/data",
    "LogDirectory": "/var/surgewave/logs",

    "StorageMode": "File",
    "LogSegmentBytes": 1073741824,
    "LogRetentionHours": 168,

    "AutoCreateTopics": true,
    "DefaultNumPartitions": 3,
    "DefaultReplicationFactor": 1,

    "ProducerBatchSizeBytes": 16384,
    "ProducerLingerMs": 5,

    "UseChannelPipeline": true,
    "ChannelWriteWorkers": 4,

    "Quotas": {
      "Enabled": true,
      "ProducerBytesPerSecond": 10485760
    },

    "SharedMemory": {
      "Enabled": false
    },

    "Connect": {
      "Enabled": false
    }
  }
}
```

## Environment Variable Override

Any setting can be overridden via environment variables:

```bash
export Surgewave__Port=9092
export Surgewave__DataDirectory=/data/surgewave
export Surgewave__Quotas__Enabled=true
```

Use double underscores (`__`) to represent nested settings.

## Next Steps

- [Storage Backends](../storage/index.md) - Storage configuration details
- [Clustering](../clustering/index.md) - Multi-broker setup
- [Security](../security/index.md) - Authentication and authorization
