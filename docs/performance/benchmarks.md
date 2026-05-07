# Benchmarks

Detailed performance measurements.

## Throughput

### 100K Messages, 100 bytes

| Metric | Kafka | Surgewave Native | Improvement |
|--------|-------|--------------|-------------|
| Producer | 68,213 msg/s | 1,250,000 msg/s | (see benchmarks) |
| Consumer | 138,504 msg/s | 1,282,051 msg/s | +826% |

### By Message Size

| Size | Producer | Consumer |
|------|----------|----------|
| 100 bytes | 1.25M msg/s | 1.28M msg/s |
| 1 KB | 800K msg/s | 850K msg/s |
| 10 KB | 250K msg/s | 280K msg/s |
| 100 KB | 50K msg/s | 60K msg/s |

## Latency

> Specific P50/P99 latency numbers are preliminary targets pending a head-to-head benchmark
> on identical hardware. The CI regression harness covers throughput and transport overhead;
> a dedicated latency-percentile project (`Kuestenlogik.Surgewave.Benchmarks.Latency`) will be
> exercised as part of the 1.0 release sign-off and the resulting figures published here.

### By Storage Backend

| Backend | P50 Write | P50 Read |
|---------|-----------|----------|
| Memory | ultra-low (target) | ultra-low (target) |
| ZeroCopyWal | ~100 µs | ~20 µs |
| File | ~100 µs | ~50 µs |

## Shared Memory Transport

| Operation | P50 | P99 | Throughput |
|-----------|-----|-----|------------|
| Ring Buffer Write | 0.1 µs | 0.4 µs | 625K msg/s |
| E2E (SHM) | ultra-low (target) | ~5 µs | 625K msg/s |

## Storage Performance

| Message Size | File | ZeroCopyWal | Speedup |
|-------------|------|-------------|---------|
| 100 bytes | 286K msg/s | 549K msg/s | 1.9x |
| 1 KB | 221K msg/s | 235K msg/s | 1.1x |
| 10 KB | 120K msg/s | 145K msg/s | 1.2x |

## Compression

| Codec | Compress | Decompress | Ratio |
|-------|----------|------------|-------|
| None | - | - | 1.0x |
| LZ4 | 2.5 GB/s | 4.0 GB/s | 2-3x |
| Zstd | 500 MB/s | 1.5 GB/s | 3-5x |
| Gzip | 150 MB/s | 400 MB/s | 2-4x |
| Snappy | 1.5 GB/s | 2.0 GB/s | 2-3x |

## Running Benchmarks

### CLI

```bash
surgewave benchmark --messages 100000 --size 1024
surgewave benchmark -n 1000000 -s 100 --batch 1000
```

### Embedded

```csharp
// High-performance setup
await using var broker = new EmbeddedSurgewave(options =>
{
    options.Storage = StorageBackend.Memory;
    options.UseChannelPipeline = true;
});
```

## Environment

Benchmarks run on:
- CPU: 8 cores
- Memory: 32 GB
- Storage: NVMe SSD
- .NET: 10.0

## Next Steps

- [Tuning Guide](tuning.md) - Optimize for your workload
