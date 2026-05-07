# Memory Storage

In-memory storage provides the lowest latency for testing and ephemeral workloads.

## Configuration

```json
{
  "Surgewave": {
    "StorageMode": "Memory"
  }
}
```

Or with zero-copy optimization:

```json
{
  "Surgewave": {
    "StorageMode": "ZeroCopyMemory"
  }
}
```

## Characteristics

| Property | Value |
|----------|-------|
| Persistence | No |
| Latency | ultra-low (target) (write) |
| Throughput | 625K+ msg/s |
| Memory Usage | High |

## Use Cases

### Testing

```csharp
await using var broker = new EmbeddedSurgewave(options =>
{
    options.Storage = StorageBackend.Memory;
});
await broker.StartAsync();
```

### Development

Fast iteration during development without disk I/O overhead.

### Caching Layer

Use as a high-speed cache in front of persistent storage.

## Memory Management

Memory storage uses pooled buffers to minimize allocations:

- `ArrayPool<byte>` for message buffers
- Segment-based memory organization
- Automatic cleanup on segment rotation

## Limitations

- **No Persistence** - Data lost on restart
- **Memory Bound** - Limited by available RAM
- **No Replication** - Can't replicate to disk-based followers

## Zero-Copy Memory

The `ZeroCopyMemory` mode provides additional optimizations:

```json
{
  "Surgewave": {
    "StorageMode": "ZeroCopyMemory"
  }
}
```

Features:
- `ISurgewaveBuffer` interface for direct memory access
- No copy on read operations
- Span-based APIs for allocation-free processing

## Performance Tips

1. **Pre-allocate Topics** - Create topics before high-load periods
2. **Monitor Memory** - Watch for OOM conditions
3. **Set Retention** - Even in-memory benefits from retention limits

```json
{
  "Surgewave": {
    "StorageMode": "Memory",
    "LogRetentionBytes": 1073741824  // 1 GB per topic
  }
}
```

## Ephemeral Topics

Ephemeral topics (`cleanup.policy=ephemeral`) use a dedicated `EphemeralPartitionLog` backed by a fixed-size ring buffer. This is distinct from Memory storage mode:

| Feature | Memory Storage Mode | Ephemeral Topics |
|---------|-------------------|------------------|
| Scope | All topics on broker | Per-topic setting |
| Storage class | `PartitionLog` (in-memory) | `EphemeralPartitionLog` (ring-buffer) |
| Retention | Time/size-based | Buffer window only |
| Wrap-around | No (grows until retention) | Yes (fixed buffer, evicts oldest) |
| Configuration | `StorageMode=Memory` | `cleanup.policy=ephemeral` |
| Use with File storage | No | Yes (mix persistent + ephemeral) |

Ephemeral topics can be created on any storage mode. On a File-backed broker, persistent and ephemeral topics coexist - persistent topics use disk while ephemeral topics use in-memory ring buffers.

```csharp
// Create ephemeral topic on a disk-backed broker
await client.Topics.Create("live-metrics")
    .WithPartitions(1)
    .WithConfig("cleanup.policy", "ephemeral")
    .WithConfig("ephemeral.buffer.bytes", "128MB")
    .ExecuteAsync();
```

## Next Steps

- [FileSystem Storage](filesystem.md) - Persistent alternative
- [Performance Tuning](../performance/tuning.md) - Optimization guide
