# Performance Overview

This page summarises the throughput numbers Surgewave currently reaches in its embedded
benchmark harness, plus the optimisation areas the codebase is built around. For the full
benchmark methodology and per-platform comparison see [Benchmarks](benchmarks.md).

## Key Metrics

> Comparative latency numbers are preliminary targets and will be published alongside a head-to-head
> benchmark on identical hardware in the 1.0 release. Throughput numbers below come from the embedded
> benchmark harness (`benchmarks/baselines/benchmark-baseline.json`); see [Benchmarks](benchmarks.md)
> for the full setup.

| Metric | Surgewave Native | Kafka Protocol |
|--------|--------------|----------------|
| Producer Throughput | 1.25M msg/s | 68K msg/s |
| Consumer Throughput | 1.28M msg/s | 138K msg/s |

## Performance Advantages

### Native Protocol

- Lower latency than the Kafka wire path (target — public head-to-head benchmark pending)
- Single-pass serialization
- Zero-copy where possible
- SIMD optimizations

### Storage

| Backend | Throughput | Latency |
|---------|------------|---------|
| Memory | 625K msg/s | ultra-low (target) |
| ZeroCopyWal | 549K msg/s | ~20 µs |
| File | 286K msg/s | ~50 µs |

### Shared Memory

- Sub-microsecond latency
- Lock-free ring buffers
- No network overhead

## Optimization Techniques

1. **SIMD Operations** - Vectorized encoding
2. **ArrayPool** - Reduced allocations
3. **Span/Memory** - Zero-copy APIs
4. **Channel Pipelines** - Async batching
5. **Hardware CRC32** - SSE4.2/ARM acceleration

## Next Steps

- [Benchmarks](benchmarks.md) - Detailed measurements
- [Tuning Guide](tuning.md) - Configuration optimization
