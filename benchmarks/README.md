# Surgewave Benchmarks

Performance benchmarks for measuring Surgewave's throughput, latency, and comparison against Apache Kafka and Redpanda.

## Projects Overview

| Project | Description |
|---------|-------------|
| `Kuestenlogik.Surgewave.Benchmarks` | Main orchestrator - routes to appropriate benchmark projects |
| `Kuestenlogik.Surgewave.Benchmarks.Unit` | Microbenchmarks (serialization, compression, SIMD, protocol) |
| `Kuestenlogik.Surgewave.Benchmarks.Storage` | Storage engine benchmarks (File, Memory, Arrow backends) |
| `Kuestenlogik.Surgewave.Benchmarks.Integration` | End-to-end benchmarks (requires broker) |
| `Kuestenlogik.Surgewave.Benchmarks.Comparison` | Cross-system comparisons (Surgewave vs Kafka vs Redpanda) |
| `Kuestenlogik.Surgewave.Benchmarks.Latency` | P50/P90/P99/P99.9/P99.99 latency measurements |

## Quick Start

```bash
# Run all unit benchmarks
dotnet run --project benchmarks/Kuestenlogik.Surgewave.Benchmarks -- unit

# Run storage benchmarks
dotnet run --project benchmarks/Kuestenlogik.Surgewave.Benchmarks -- storage

# Run latency benchmarks (P50/P90/P99)
dotnet run --project benchmarks/Kuestenlogik.Surgewave.Benchmarks -- latency

# List all available benchmarks
dotnet run --project benchmarks/Kuestenlogik.Surgewave.Benchmarks -- --list tree
```

## Category-Based Filtering

Benchmarks are organized by categories using `[BenchmarkCategory]` attributes:

```bash
# By primary category
dotnet run -- unit                      # All unit benchmarks
dotnet run -- storage                   # All storage benchmarks
dotnet run -- simd                      # All SIMD benchmarks
dotnet run -- compression               # All compression benchmarks
dotnet run -- serialization             # All serialization benchmarks
dotnet run -- protocol                  # All protocol benchmarks

# Custom category filter
dotnet run -- --category=Latency        # Any category (custom)
dotnet run -- -c=P99                    # Short form

# BenchmarkDotNet native filtering
dotnet run -- --allCategories=Unit      # All benchmarks in category
dotnet run -- --anyCategories=P99,Latency   # Any matching category
dotnet run -- --filter *Serialization*  # Filter by name pattern
```

### Available Categories

| Type | Categories |
|------|------------|
| **Primary** | Unit, Storage, Integration, Comparison, Latency |
| **Features** | Serialization, Compression, Protocol, Simd, BufferPool, Throughput |
| **Latency Percentiles** | P50, P90, P99, P99.9, P99.99, EndToEnd |
| **Systems** | Kafka, Redpanda, Native, Embedded |

## Benchmark Types

### Unit Benchmarks (No Dependencies)

Microbenchmarks that don't require a running broker:

```bash
dotnet run -- unit
```

Includes:
- `SerializationBenchmarks` - RecordBatch serialization/deserialization
- `CompressionBenchmarks` - GZIP, LZ4, ZSTD codec comparison
- `ProtocolBenchmarks` - Kafka protocol parsing
- `SimdBigEndianBenchmarks` - SIMD-optimized byte operations
- `ByteArrayComparerBenchmarks` - SIMD key comparison
- `BufferPoolBenchmarks` - Memory pool vs heap allocation
- `StartupBenchmarks` - Initialization overhead

### Storage Benchmarks

Storage engine performance across backends:

```bash
dotnet run -- storage
```

Includes:
- `StorageBenchmarks` - Append/read across File, Memory, Arrow
- `MemoryMappedReadBenchmarks` - FileStream vs Memory-Mapped I/O
- `ThroughputBenchmarks` - Serialize/parse messages per second

### Latency Benchmarks (P50/P90/P99/P99.9/P99.99)

Detailed latency percentile measurements:

```bash
# Surgewave Native vs Kafka client (embedded broker)
dotnet run -- latency [msgCount] [msgSize] [storage]

# Surgewave vs Real Kafka broker
dotnet run -- latency-compare [msgCount] [msgSize] [kafkaBootstrap]
```

Example:
```bash
dotnet run -- latency 10000 100 memory
dotnet run -- latency-compare 5000 100 localhost:29092
```

### Comparison Benchmarks

Cross-system comparisons using Testcontainers:

```bash
# Four-way: Kafka vs Redpanda vs Surgewave+Kafka vs Surgewave Native
dotnet run -- vs-kafka [msgCount] [msgSize]

# Three-way comparison
dotnet run -- three-way [msgCount] [msgSize] [kafkaBootstrap]

# Native client vs Kafka client (embedded)
dotnet run -- compare [msgCount] [msgSize]
```

### Integration Benchmarks

End-to-end benchmarks requiring a running broker:

```bash
# Embedded broker throughput
dotnet run -- embedded [msgCount] [msgSize] [batchSize] [storage]

# Transport layer latency
dotnet run -- transport [msgCount] [msgSize]
```

## Output

All benchmark results are written to:
```
artifacts/benchmarks/
├── results/      # BenchmarkDotNet reports (JSON, Markdown)
└── reports/      # Custom benchmark reports
```

## Examples

```bash
# Quick SIMD performance check
dotnet run -- simd

# Full compression comparison (all codecs, all sizes)
dotnet run -- compression

# Latency percentiles against real Kafka
dotnet run -- latency-compare 5000 100 localhost:29092

# Four-way comparison (requires Docker for Testcontainers)
dotnet run -- vs-kafka 100000 1024

# Export specific benchmark results
dotnet run -- --filter *Serialization* --exporters json
```

## Adding New Benchmarks

1. Create benchmark class with `[BenchmarkCategory]` attribute:
   ```csharp
   [BenchmarkCategory("Unit", "MyFeature")]
   public class MyBenchmarks
   {
       [Benchmark(Baseline = true)]
       public void Baseline() { ... }

       [Benchmark]
       public void Improved() { ... }
   }
   ```

2. Add to appropriate project based on dependencies:
   - No broker needed → `Kuestenlogik.Surgewave.Benchmarks.Unit`
   - Storage focused → `Kuestenlogik.Surgewave.Benchmarks.Storage`
   - Needs broker → `Kuestenlogik.Surgewave.Benchmarks.Integration`
   - Cross-system → `Kuestenlogik.Surgewave.Benchmarks.Comparison`
   - Latency focused → `Kuestenlogik.Surgewave.Benchmarks.Latency`

3. Mark baseline methods with `[Benchmark(Baseline = true)]`

## Command Reference

| Command | Description |
|---------|-------------|
| `unit` | All unit microbenchmarks |
| `storage` | All storage benchmarks |
| `simd` | SIMD optimization benchmarks |
| `compression` | Compression codec benchmarks |
| `serialization` | Serialization benchmarks |
| `protocol` | Protocol parsing benchmarks |
| `latency` | P50/P90/P99 latency (embedded) |
| `latency-compare` | Latency vs real Kafka |
| `embedded` | Embedded broker throughput |
| `transport` | Transport layer latency |
| `compare` | Kafka vs Native client |
| `vs-kafka` | Surgewave vs Kafka (Testcontainers) |
| `three-way` | Kafka vs Surgewave+Kafka vs Native |
| `four-way` | + Redpanda comparison |
| `--list tree` | List all benchmarks |
| `--filter <pattern>` | Filter by name |
| `--category=<cat>` | Filter by category |
