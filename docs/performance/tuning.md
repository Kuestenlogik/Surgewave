# Tuning Guide

Optimize Surgewave for your workload.

## Quick Wins

### Producer Throughput

```json
{
  "Surgewave": {
    "ProducerBatchSizeBytes": 65536,
    "ProducerLingerMs": 50,
    "ProducerMaxBatchMessages": 50000
  }
}
```

### Low Latency

```json
{
  "Surgewave": {
    "ProducerBatchSizeBytes": 1,
    "ProducerLingerMs": 0
  }
}
```

### High Throughput Pipeline

```json
{
  "Surgewave": {
    "UseChannelPipeline": true,
    "ChannelWriteWorkers": 8,
    "ChannelWriteBufferSize": 50000,
    "ChannelWriteBatchSize": 500,
    "ChannelWriteBatchDelayMs": 5
  }
}
```

## Storage Tuning

### Memory (Fastest)

```json
{
  "Surgewave": {
    "StorageMode": "Memory"
  }
}
```

### ZeroCopy WAL (Fast + Persistent)

```json
{
  "Surgewave": {
    "StorageMode": "ZeroCopyWal"
  }
}
```

### Segment Size

Large segments = fewer files, slower recovery:

```json
{
  "Surgewave": {
    "LogSegmentBytes": 1073741824
  }
}
```

## Network Tuning

### Socket Buffers

```json
{
  "Surgewave": {
    "SocketSendBufferBytes": 1048576,
    "SocketReceiveBufferBytes": 1048576
  }
}
```

### Max Request Size

```json
{
  "Surgewave": {
    "MaxRequestSize": 104857600
  }
}
```

## Compression

### Producer Side

```csharp
await using var client = new SurgewaveNativeClient("localhost", 9092);
await client.ConnectAsync();
client.CompressionEnabled = true;  // Enable compression for large payloads
```

### Broker Side

```json
{
  "Surgewave": {
    "NativeProtocolCompressionEnabled": true
  }
}
```

## SIMD Optimization

Enable SIMD for large batches:

```json
{
  "Surgewave": {
    "SimdBatchThreshold": 1000
  }
}
```

## Shared Memory

For co-located services:

```json
{
  "Surgewave": {
    "SharedMemory": {
      "Enabled": true,
      "PollingStrategy": "BusySpin",
      "RingBufferCapacity": 33554432
    }
  }
}
```

## Client Tuning

### Producer

```csharp
var producer = new SurgewaveProducer<string, string>(options =>
{
    options.BatchSizeBytes = 64 * 1024;
    options.LingerMs = 50;
    options.CompressionCodec = CompressionCodec.Lz4;
    options.Acks = Acks.One;  // or All for durability
});
```

### Consumer

```csharp
var consumer = new SurgewaveConsumer<string, string>(options =>
{
    options.MaxPollRecords = 1000;
    options.FetchMinBytes = 1024 * 1024;
    options.FetchMaxWaitMs = 500;
});
```

## GC Tuning

Reduce GC pauses:

```bash
export DOTNET_GCHeapHardLimit=8G
export DOTNET_GCConserveMemory=5
```

## Monitoring Performance

```bash
# Check throughput
surgewave benchmark -n 100000 -s 1024

# Check broker stats
surgewave broker info -f json | jq '.throughput'
```

## Common Bottlenecks

| Symptom | Cause | Solution |
|---------|-------|----------|
| High P99 latency | GC pauses | Increase heap, reduce allocs |
| Low throughput | Small batches | Increase batch size, linger |
| Disk I/O | Slow storage | Use SSD, ZeroCopyWal |
| CPU bound | Compression | Use LZ4, reduce level |
| Memory pressure | Large buffers | Reduce buffer sizes |

## Profiles

### Realtime

```json
{
  "Surgewave": {
    "StorageMode": "Memory",
    "ProducerBatchSizeBytes": 1,
    "ProducerLingerMs": 0,
    "SharedMemory": { "Enabled": true }
  }
}
```

### High Throughput

```json
{
  "Surgewave": {
    "StorageMode": "ZeroCopyWal",
    "UseChannelPipeline": true,
    "ChannelWriteWorkers": 8,
    "ProducerBatchSizeBytes": 65536,
    "ProducerLingerMs": 50
  }
}
```

### Balanced

```json
{
  "Surgewave": {
    "StorageMode": "File",
    "ProducerBatchSizeBytes": 16384,
    "ProducerLingerMs": 5
  }
}
```

## Next Steps

- [Monitoring](../monitoring/index.md) - Track performance
- [Storage](../storage/index.md) - Storage selection
