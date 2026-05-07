# Benchmark Guide

Surgewave ships a multi-tier benchmark suite that covers everything from low-level buffer
primitives to full cross-platform competitor comparisons. All benchmark projects live
under `benchmarks/` in the repository root.

## Table of Contents

- [Overview](#overview)
- [Quick Start](#quick-start)
- [Competitor Benchmarks](#competitor-benchmarks)
- [Latency Benchmarks](#latency-benchmarks)
- [Storage Engine Benchmarks](#storage-engine-benchmarks)
- [Transport Benchmarks](#transport-benchmarks)
- [Throughput Benchmarks](#throughput-benchmarks)
- [BenchmarkDotNet Integration](#benchmarkdotnet-integration)
- [Regression Detection](#regression-detection)
- [Real-World Scenarios](#real-world-scenarios)

---

## Overview

### Benchmark Projects

| Project | Purpose |
|---------|---------|
| `Kuestenlogik.Surgewave.Benchmarks` | Main entry point — routes all commands to sub-projects |
| `Kuestenlogik.Surgewave.Benchmarks.Comparison` | Multi-platform competitor comparison (Kafka, Redpanda, Pulsar, NATS) |
| `Kuestenlogik.Surgewave.Benchmarks.Integration` | Embedded broker throughput (end-to-end, no mock) |
| `Kuestenlogik.Surgewave.Benchmarks.Latency` | P50/P90/P99/P99.9/P99.99 latency percentile measurement |
| `Kuestenlogik.Surgewave.Benchmarks.RealWorld` | Scenario-based tests: multi-broker, replication, failover |
| `Kuestenlogik.Surgewave.Benchmarks.Regression` | Regression detection against a stored baseline |
| `Kuestenlogik.Surgewave.Benchmarks.Storage` | All 7 storage engines (Memory, File, Arrow, NVMe, DuckDB, LMDB, Parquet) |
| `Kuestenlogik.Surgewave.Benchmarks.Streams` | Streams state stores and topology latency |
| `Kuestenlogik.Surgewave.Benchmarks.Transport` | SHM ring buffer, protocol serialization, transport comparison |
| `Kuestenlogik.Surgewave.Benchmarks.Unit` | Micro-benchmarks: serialization, SIMD, compression, buffer pools |
| `Kuestenlogik.Surgewave.Benchmarks.Streams` | Streams state stores and Streams latency percentiles |

### What Gets Measured

- **Throughput** — messages per second and MB/s for produce and consume paths
- **Latency percentiles** — P50, P90, P99, P99.9, P99.99 for produce, consume, and end-to-end
- **Storage engines** — append and read performance across all 7 backends
- **Protocol overhead** — MQTT, WebSocket JSON, Surgewave Native binary, and Kafka wire format
- **Transport primitives** — lock-free SPSC ring buffer vs TCP MemoryStream
- **Streams operations** — state store Put/Get/Delete/Range, topology, window, join, serde
- **Competitor comparison** — Surgewave vs Apache Kafka, Redpanda, Apache Pulsar (KoP), NATS JetStream

---

## Quick Start

### Prerequisites

- .NET 10 SDK
- Docker Desktop (for competitor benchmarks that start real brokers)

### Build

```bash
dotnet build benchmarks/Kuestenlogik.Surgewave.Benchmarks -c Release
```

Or build the whole solution:

```bash
dotnet build Kuestenlogik.Surgewave.slnx -c Release
```

### Run Your First Benchmark

The fastest way to get a number — embedded broker throughput using memory storage:

```bash
dotnet run --project benchmarks/Kuestenlogik.Surgewave.Benchmarks -c Release -- embedded 100000 100 1000 memory
```

This produces a table showing producer and consumer msg/s for a 100-byte message workload
with batch size 1000 against an in-process broker.

To see all available commands:

```bash
dotnet run --project benchmarks/Kuestenlogik.Surgewave.Benchmarks -c Release
```

---

## Competitor Benchmarks

These commands benchmark one system at a time and save the result as a JSON file to
`artifacts/benchmarks/results/`. After running several systems, the `compare` command
loads all saved results and prints a unified side-by-side table.

All competitor benchmarks are run from the main benchmark entry point:

```bash
dotnet run --project benchmarks/Kuestenlogik.Surgewave.Benchmarks -c Release -- <command> [options]
```

### `benchmark-surgewave` — Surgewave Baseline

Benchmarks the embedded Surgewave broker using both the native and Kafka wire protocols.
No Docker required; the broker runs in-process.

```bash
# Default: 100,000 messages, 100 bytes
dotnet run --project benchmarks/Kuestenlogik.Surgewave.Benchmarks -c Release -- benchmark-surgewave

# Custom message count and size
dotnet run --project benchmarks/Kuestenlogik.Surgewave.Benchmarks -c Release -- benchmark-surgewave 500000 1024
```

Result saved to: `artifacts/benchmarks/results/surgewave.json`

### `benchmark-kafka` — Apache Kafka

Benchmarks a running Apache Kafka broker. Start Kafka with Docker first:

```bash
docker run -d --name kafka \
  -p 29092:29092 \
  -e KAFKA_NODE_ID=1 \
  -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093,EXTERNAL://:29092 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092,EXTERNAL://localhost:29092 \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  confluentinc/cp-kafka:7.6.0
```

Then run the benchmark:

```bash
# Against the default host localhost:29092
dotnet run --project benchmarks/Kuestenlogik.Surgewave.Benchmarks -c Release -- benchmark-kafka

# Custom count, size, and bootstrap server
dotnet run --project benchmarks/Kuestenlogik.Surgewave.Benchmarks -c Release -- benchmark-kafka 100000 100 localhost:29092

# Also benchmark Surgewave in the same run for instant comparison
dotnet run --project benchmarks/Kuestenlogik.Surgewave.Benchmarks -c Release -- benchmark-kafka 100000 100 localhost:29092 --include-surgewave
```

Result saved to: `artifacts/benchmarks/results/kafka.json`

### `benchmark-redpanda` — Redpanda

```bash
docker run -d --name redpanda \
  -p 19092:19092 \
  redpandadata/redpanda:latest \
  redpanda start \
    --advertise-kafka-addr localhost:19092 \
    --kafka-addr 0.0.0.0:19092 \
    --overprovisioned \
    --smp 1 \
    --memory 512M \
    --reserve-memory 0M \
    --node-id 0 \
    --check=false
```

```bash
# Default: localhost:19092
dotnet run --project benchmarks/Kuestenlogik.Surgewave.Benchmarks -c Release -- benchmark-redpanda

# Custom bootstrap address
dotnet run --project benchmarks/Kuestenlogik.Surgewave.Benchmarks -c Release -- benchmark-redpanda 100000 100 localhost:19092

# With Surgewave comparison
dotnet run --project benchmarks/Kuestenlogik.Surgewave.Benchmarks -c Release -- benchmark-redpanda 100000 100 localhost:19092 --include-surgewave
```

Result saved to: `artifacts/benchmarks/results/redpanda.json`

### `benchmark-pulsar` — Apache Pulsar (via KoP)

Apache Pulsar with the Kafka-on-Pulsar (KoP) protocol handler exposes a Kafka-compatible endpoint:

```bash
docker run -d --name pulsar \
  -p 6650:6650 \
  -p 8080:8080 \
  -p 9092:9092 \
  apachepulsar/pulsar:latest \
  bin/pulsar standalone
```

```bash
# Default: localhost:9092 (KoP endpoint)
dotnet run --project benchmarks/Kuestenlogik.Surgewave.Benchmarks -c Release -- benchmark-pulsar

# Custom address
dotnet run --project benchmarks/Kuestenlogik.Surgewave.Benchmarks -c Release -- benchmark-pulsar 100000 100 localhost:9092

# With Surgewave comparison
dotnet run --project benchmarks/Kuestenlogik.Surgewave.Benchmarks -c Release -- benchmark-pulsar 100000 100 localhost:9092 --include-surgewave
```

Result saved to: `artifacts/benchmarks/results/pulsar.json`

### `benchmark-nats` — NATS JetStream

```bash
docker run -d --name nats \
  -p 4222:4222 \
  nats:latest \
  -js
```

```bash
# Default: nats://localhost:4222
dotnet run --project benchmarks/Kuestenlogik.Surgewave.Benchmarks -c Release -- benchmark-nats

# Custom NATS URL
dotnet run --project benchmarks/Kuestenlogik.Surgewave.Benchmarks -c Release -- benchmark-nats 100000 100 nats://localhost:4222

# With Surgewave comparison
dotnet run --project benchmarks/Kuestenlogik.Surgewave.Benchmarks -c Release -- benchmark-nats 100000 100 nats://localhost:4222 --include-surgewave
```

Result saved to: `artifacts/benchmarks/results/nats.json`

### `--include-surgewave` Flag

Any competitor command accepts `--include-surgewave` as the fourth positional argument.
When present, Surgewave is benchmarked in the same run so results are immediately comparable
without running `compare` separately.

```bash
dotnet run --project benchmarks/Kuestenlogik.Surgewave.Benchmarks -c Release -- benchmark-kafka 100000 100 localhost:29092 --include-surgewave
```

### `compare` — Cross-Platform Comparison Table

After running two or more competitor commands, generate a unified comparison table from
the saved JSON files:

```bash
dotnet run --project benchmarks/Kuestenlogik.Surgewave.Benchmarks -c Release -- compare
```

The command reads every `*.json` file from `artifacts/benchmarks/results/` and prints a
side-by-side table of throughput, latency, and relative performance ratios.

### Results Directory

All JSON result files are written to:

```
artifacts/benchmarks/results/
  surgewave.json
  kafka.json
  redpanda.json
  pulsar.json
  nats.json
```

---

## Latency Benchmarks

### `latency` — Surgewave Protocol Comparison (P50/P90/P99/P99.9/P99.99)

Measures produce, consume, and end-to-end latency percentiles for Surgewave Native protocol
and the Confluent Kafka client, both against the same embedded Surgewave broker.

```bash
# Signature: latency [msgCount] [msgSize] [storage]
# Defaults:  10000    100        memory

# Quick run with defaults
dotnet run --project benchmarks/Kuestenlogik.Surgewave.Benchmarks -c Release -- latency

# 50,000 messages, 1 KB payload, file storage
dotnet run --project benchmarks/Kuestenlogik.Surgewave.Benchmarks -c Release -- latency 50000 1024 file

# 10,000 messages, 100 bytes, Arrow columnar storage
dotnet run --project benchmarks/Kuestenlogik.Surgewave.Benchmarks -c Release -- latency 10000 100 arrow
```

Storage options: `memory` (default), `file`, `arrow`

Output includes a comparison table:

```
│    Operation   │   P50    │   P99    │  P99.9   │  P99.99  │ Native vs Kafka│
├────────────────┼──────────┼──────────┼──────────┼──────────┼───────────────┤
│ Produce Native │       45 │      180 │      420 │      850 │               │
│ Produce Kafka  │    15600 │    45000 │    58000 │    72000 │  345.6x slower │
```

### `latency-compare` — Surgewave vs Real Kafka Broker

Runs the same latency measurement but compares against a live Apache Kafka broker
rather than against Surgewave's Kafka protocol support. Requires a running Kafka instance.

```bash
# Signature: latency-compare [msgCount] [msgSize] [kafkaBootstrap]
# Defaults:  5000          100        localhost:29092

dotnet run --project benchmarks/Kuestenlogik.Surgewave.Benchmarks -c Release -- latency-compare

# Custom
dotnet run --project benchmarks/Kuestenlogik.Surgewave.Benchmarks -c Release -- latency-compare 10000 512 localhost:29092
```

### `streams-latency` — Streams Operations Latency

Measures P50/P90/P99/P99.9/P99.99 for Streams state store operations, topology
processing, join operations, window operations, and serde.

```bash
# Signature: streams-latency [operation] [recordCount] [storeType]
# Defaults:  all             10000         all

# All operations, all store types
dotnet run --project benchmarks/Kuestenlogik.Surgewave.Benchmarks -c Release -- streams-latency

# State store only, RocksDB
dotnet run --project benchmarks/Kuestenlogik.Surgewave.Benchmarks -c Release -- streams-latency statestore 10000 rocksdb

# Topology processing, 50K records
dotnet run --project benchmarks/Kuestenlogik.Surgewave.Benchmarks -c Release -- streams-latency topology 50000

# Window operations, SQLite
dotnet run --project benchmarks/Kuestenlogik.Surgewave.Benchmarks -c Release -- streams-latency window 10000 sqlite
```

**Operations:** `all`, `statestore`, `topology`, `join`, `window`, `serde`

**Store types:** `all`, `inmemory`, `rocksdb`, `sqlite`, `mappedfile`, `caching`

---

## Storage Engine Benchmarks

### `storage` — All 7 Storage Engines

Runs BenchmarkDotNet microbenchmarks for append (small/medium/large batch) and read
operations across all available segment storage backends.

```bash
dotnet run --project benchmarks/Kuestenlogik.Surgewave.Benchmarks -c Release -- storage
```

Or run the storage project standalone for a quick non-BDN test:

```bash
dotnet run --project benchmarks/Kuestenlogik.Surgewave.Benchmarks.Storage -c Release -- quick
```

**Engines benchmarked:**

| Engine | Class | Notes |
|--------|-------|-------|
| `Memory` | `MemoryLogSegment` | Fastest; no persistence |
| `File` | `FileStorageEngine` | Default persistence backend |
| `Arrow` | `ArrowLogSegment` | Columnar, analytics-friendly |
| `NvmeDirect` | `NvmeDirectStorageEngine` | Direct I/O, bypasses page cache |
| `DuckDB` | `DuckDbStorageEngine` | SQL-queryable storage |
| `LMDB` | `LmdbStorageEngine` | Memory-mapped B-tree |
| `Parquet` | `ParquetStorageEngine` | Columnar, offline analytics |

**Benchmark operations:**

- `Append_SmallBatch` — 100 bytes, 1 record (baseline)
- `Append_MediumBatch` — 1 KB, 10 records
- `Append_LargeBatch` — 10 KB, 100 records
- `Read_Single` — read up to 2 KB
- `Read_Multiple` — read up to 64 KB
- `Read_Large` — read up to 1 MB

Filter to a specific engine:

```bash
dotnet run --project benchmarks/Kuestenlogik.Surgewave.Benchmarks -c Release -- --filter "*StorageBenchmarks*Arrow*"
```

### `statestore` — Streams State Stores

BenchmarkDotNet microbenchmarks for Streams state store backends: Put (single and
batch-100), Get (existing and missing), Delete, and full enumeration.

```bash
dotnet run --project benchmarks/Kuestenlogik.Surgewave.Benchmarks -c Release -- statestore
```

**Backends:** `InMemory`, `RocksDb`, `Sqlite`, `MappedFile`, `Caching`

**Preload sizes:** 1,000 and 10,000 entries (both run for each backend).

Filter to one backend:

```bash
dotnet run --project benchmarks/Kuestenlogik.Surgewave.Benchmarks -c Release -- --filter "*StateStoreBenchmarks*RocksDb*"
```

---

## Transport Benchmarks

### `sharedmemory` / `ringbuffer` — SHM Ring Buffer

BenchmarkDotNet microbenchmarks for the lock-free SPSC ring buffer that backs the
shared-memory transport. Runs entirely in-process; no broker required.

```bash
dotnet run --project benchmarks/Kuestenlogik.Surgewave.Benchmarks -c Release -- sharedmemory

# Equivalent aliases
dotnet run --project benchmarks/Kuestenlogik.Surgewave.Benchmarks -c Release -- ringbuffer
dotnet run --project benchmarks/Kuestenlogik.Surgewave.Benchmarks -c Release -- shared-memory
```

**Operations measured:**

| Benchmark | Description |
|-----------|-------------|
| `SpscRingBuffer_Write` | Single claim+commit write (baseline) |
| `SpscRingBuffer_ReadWrite` | Write then immediately read (round-trip) |
| `SpscRingBuffer_BatchWrite` | 100 writes in a tight loop |

**Message sizes:** 64 B, 256 B, 1 KB, 4 KB (parameterized)

### `protocols` — Protocol Serialization Comparison

Compares MQTT topic matching, WebSocket JSON serialization, Surgewave Native binary encoding,
and Kafka wire protocol parsing — all without a running broker.

```bash
dotnet run --project benchmarks/Kuestenlogik.Surgewave.Benchmarks -c Release -- protocols
```

**Benchmark categories:**

| Category | What Is Measured |
|----------|-----------------|
| `TopicMapping` | MQTT topic filter matching (wildcard and exact), MQTT→Surgewave topic translation |
| `Serialization` | Encode an outbound message for WebSocket JSON and Surgewave Native |
| `Deserialization` | Decode WebSocket JSON, Surgewave Native binary, and Kafka wire requests |
| `MessageSize` | Total frame size (framing overhead) for each protocol at each payload size |

**Payload sizes:** 64 B, 256 B, 1 KB, 4 KB (parameterized)

### `transport-compare` — SHM Ring Buffer vs TCP

Compares the raw buffering cost of the shared-memory ring buffer against a `MemoryStream`
write (TCP send-buffer proxy) for Write, ReadWrite, and BatchWrite patterns.

```bash
dotnet run --project benchmarks/Kuestenlogik.Surgewave.Benchmarks -c Release -- transport-compare
```

**Message sizes:** 64 B, 256 B, 1 KB, 4 KB (parameterized)

---

## Throughput Benchmarks

### `embedded` — Embedded Broker Throughput

Measures producer and consumer throughput using an in-process Surgewave broker.
No Docker or external processes required.

```bash
# Signature: embedded [msgCount] [msgSize] [batchSize] [storageMode]
# Defaults:  100000    100        1000       both

# Defaults: 100K messages, 100 bytes, batch 1000, File + Memory comparison
dotnet run --project benchmarks/Kuestenlogik.Surgewave.Benchmarks -c Release -- embedded

# 1 million messages, 1 KB payload, batch 5000, memory-only
dotnet run --project benchmarks/Kuestenlogik.Surgewave.Benchmarks -c Release -- embedded 1000000 1024 5000 memory

# All storage modes
dotnet run --project benchmarks/Kuestenlogik.Surgewave.Benchmarks -c Release -- embedded 500000 100 1000 all
```

**Storage modes:** `file`, `memory`, `arrow`, `arrownocompress`, `both` (file + memory), `all`

Output includes a summary table comparing msg/s, MB/s, and startup time for each mode.

### Multi-Platform Scenario Comparison (Comparison Project)

The comparison project (`Kuestenlogik.Surgewave.Benchmarks.Comparison`) provides richer scenario-based
commands. Run it directly for access to platform presets and scenario selection:

```bash
cd benchmarks/Kuestenlogik.Surgewave.Benchmarks.Comparison
dotnet run -c Release -- [scenario] [options]
```

**Scenarios:**

| Scenario | Description |
|----------|-------------|
| `throughput` | Max throughput (msg/sec, MB/sec) |
| `latency` | Latency comparison (P50/P90/P99) |
| `batch-size` | Batch size impact on throughput |
| `message-size` | Message size impact on throughput |
| `multi-producer` | Producer scaling (1, 3, 5, 10 concurrent) |
| `all` | All scenarios (default) |

**Platform presets (`--platforms`):**

| Preset | Platforms Included |
|--------|-------------------|
| `all` | All 8 platform configurations |
| `containers` | Container variants (Surgewave Container + Kafka Container + Redpanda Container) |
| `fair` | Fair comparison — all container variants using Kafka wire protocol |
| `embedded` | Surgewave Embedded Native + Surgewave Embedded Kafka |
| `standalone` | Surgewave Standalone Native + Surgewave Standalone Kafka |
| `surgewave` | All 6 Surgewave variants |

**Named platforms:**

| Name | Description |
|------|-------------|
| `embedded-native` | Surgewave Embedded + Native Client |
| `embedded-kafka` | Surgewave Embedded + Kafka Client |
| `standalone-native` | Surgewave Standalone + Native Client |
| `standalone-kafka` | Surgewave Standalone + Kafka Client |
| `container-native` | Surgewave Container + Native Client |
| `container-kafka` | Surgewave Container + Kafka Client |
| `kafka` | Apache Kafka Container |
| `redpanda` | Redpanda Container |

**Examples:**

```bash
# Throughput: 1M messages
dotnet run -c Release -- throughput --messages 1000000

# Latency, embedded Native vs Kafka vs Redpanda
dotnet run -c Release -- latency --platforms embedded-native,kafka,redpanda

# Full comparison, all platforms, save results
dotnet run -c Release -- all --platforms all --output results.json --report report.md

# Fair comparison across all containers
dotnet run -c Release -- throughput --platforms fair

# Surgewave-only, skip all non-Surgewave platforms
dotnet run -c Release -- multi-producer --surgewave-only

# Custom Kafka image
dotnet run -c Release -- all --platforms containers --kafka-image confluentinc/cp-kafka:7.7.0
```

**All options:**

| Option | Default | Description |
|--------|---------|-------------|
| `--messages N` | 100000 | Messages to produce |
| `--message-size N` | 100 | Message size in bytes |
| `--batch-size N` | 1000 | Producer batch size |
| `--platforms SPEC` | embedded+kafka | Platform selection (name, preset, or comma list) |
| `--kafka HOST:PORT` | localhost:29092 | External Kafka bootstrap server |
| `--surgewave-standalone ADDR` | localhost:9092 | Surgewave standalone address |
| `--surgewave-image IMAGE` | surgewave:latest | Surgewave Docker image |
| `--kafka-image IMAGE` | confluentinc/cp-kafka:7.6.0 | Kafka Docker image |
| `--redpanda-image IMAGE` | redpandadata/redpanda:latest | Redpanda Docker image |
| `--output FILE` | — | Save JSON results |
| `--report FILE` | — | Generate markdown report |
| `--surgewave-only` | false | Skip all non-Surgewave platforms |

---

## BenchmarkDotNet Integration

All BenchmarkDotNet microbenchmarks are accessible through native BDN arguments passed
directly to the main benchmark entry point.

### List Available Benchmarks

```bash
dotnet run --project benchmarks/Kuestenlogik.Surgewave.Benchmarks -c Release -- --list tree
```

### Filter by Name

```bash
dotnet run --project benchmarks/Kuestenlogik.Surgewave.Benchmarks -c Release -- --filter "*Serialization*"
dotnet run --project benchmarks/Kuestenlogik.Surgewave.Benchmarks -c Release -- --filter "*StorageBenchmarks*"
dotnet run --project benchmarks/Kuestenlogik.Surgewave.Benchmarks -c Release -- --filter "*SpscRingBuffer*"
```

### Filter by Category

```bash
# Run all benchmarks in a single category
dotnet run --project benchmarks/Kuestenlogik.Surgewave.Benchmarks -c Release -- --allCategories=Storage
dotnet run --project benchmarks/Kuestenlogik.Surgewave.Benchmarks -c Release -- --allCategories=Latency

# Run benchmarks matching any listed category
dotnet run --project benchmarks/Kuestenlogik.Surgewave.Benchmarks -c Release -- --anyCategories=P99,Latency
```

### Category Shortcut Commands

The main entry point provides shorthand commands that expand to `--allCategories=<X>`:

| Command | Equivalent Category |
|---------|-------------------|
| `unit` | `Unit` |
| `storage` | `Storage` |
| `simd` | `Simd` |
| `compression` | `Compression` |
| `serialization` | `Serialization` |
| `protocol` | `Protocol` |
| `streams` | `Streams` |
| `sharedmemory` | `SharedMemory` |
| `protocols` | `ProtocolComparison` |
| `transport-compare` | `TransportComparison` |
| `statestore` | `StateStore` |

### Available BDN Categories

| Group | Categories |
|-------|-----------|
| Primary | `Unit`, `Storage`, `Integration`, `Comparison`, `Latency`, `Streams`, `SharedMemory` |
| Transport | `ProtocolComparison`, `TransportComparison` |
| Features | `Serialization`, `Compression`, `Protocol`, `Simd`, `BufferPool`, `Throughput` |
| Streams | `StateStore`, `Topology`, `Window`, `Join`, `Serde` |
| Latency | `P50`, `P90`, `P99`, `P99.9`, `P99.99`, `EndToEnd` |
| Systems | `Kafka`, `Redpanda`, `Native`, `Embedded` |

### JSON and Markdown Export

BenchmarkDotNet exports results automatically. To also get a markdown report:

```bash
dotnet run --project benchmarks/Kuestenlogik.Surgewave.Benchmarks -c Release -- \
  --allCategories=Storage \
  --exporters=json,markdown
```

BDN writes results to `BenchmarkDotNet.Artifacts/results/` by default. Surgewave overrides
the artifacts path to `artifacts/benchmarks/` for consistency.

### Custom Category Filter Shorthand

```bash
# Long form
dotnet run --project benchmarks/Kuestenlogik.Surgewave.Benchmarks -c Release -- --category=P99

# Short form
dotnet run --project benchmarks/Kuestenlogik.Surgewave.Benchmarks -c Release -- -c=P99
```

---

## Regression Detection

The `Kuestenlogik.Surgewave.Benchmarks.Regression` project detects performance regressions by comparing
BenchmarkDotNet JSON reports against a stored baseline. It is designed for CI integration.

### Commands

#### `compare` — Detect Regressions

```bash
dotnet run --project benchmarks/Kuestenlogik.Surgewave.Benchmarks.Regression -c Release -- \
  compare results.json benchmarks/benchmark-baseline.json \
  -o regression-report.md \
  --config benchmarks/regression-config.json \
  --fail-on-regression
```

| Argument | Description |
|----------|-------------|
| `<results-json>` | BDN JSON report from the current run |
| `<baseline-json>` | Baseline to compare against |
| `-o <file>` | Write markdown report to file (optional) |
| `--config <file>` | Custom thresholds config (optional) |
| `--fail-on-regression` | Exit code 1 if any regression detected |

Prints a markdown report to stdout. Exits 0 if no regressions; exits 1 with
`--fail-on-regression` when regressions are found.

#### `update-baseline` — Merge Results into Baseline

```bash
dotnet run --project benchmarks/Kuestenlogik.Surgewave.Benchmarks.Regression -c Release -- \
  update-baseline results.json benchmarks/benchmark-baseline.json
```

Merges new results into the baseline file. New benchmarks are added; existing ones are
updated. Use this after verifying a performance improvement is intentional.

#### `report` — Generate Markdown Report

```bash
dotnet run --project benchmarks/Kuestenlogik.Surgewave.Benchmarks.Regression -c Release -- \
  report results.json benchmarks/benchmark-baseline.json \
  -o reports/regression-2026-03-21.md \
  --config benchmarks/regression-config.json
```

Always writes the report to the `-o` file. Does not print to stdout.

### Regression Thresholds

Default thresholds are stored in `benchmarks/regression-config.json`:

```json
{
  "latencyThresholdPercent": 15.0,
  "throughputThresholdPercent": 10.0,
  "allocationThresholdPercent": 20.0,
  "excludedBenchmarks": [],
  "categoryOverrides": {}
}
```

A regression is flagged when a benchmark worsens beyond the relevant threshold.

### CI Integration

Example GitHub Actions step:

```yaml
- name: Run benchmarks
  run: |
    dotnet run --project benchmarks/Kuestenlogik.Surgewave.Benchmarks -c Release -- \
      --allCategories=Unit --exporters=json

- name: Check for regressions
  run: |
    dotnet run --project benchmarks/Kuestenlogik.Surgewave.Benchmarks.Regression -c Release -- \
      compare \
        "BenchmarkDotNet.Artifacts/results/report.json" \
        "benchmarks/benchmark-baseline.json" \
        -o "regression-report.md" \
        --fail-on-regression
```

The baseline file `benchmarks/benchmark-baseline.json` is committed to source control and
updated manually via `update-baseline` after deliberate performance changes.

---

## Real-World Scenarios

The `Kuestenlogik.Surgewave.Benchmarks.RealWorld` project runs scenario-based tests against a
multi-broker cluster under realistic conditions: disk I/O, replication, sustained load,
and failure recovery.

Unlike BenchmarkDotNet microbenchmarks, these are long-running scenario tests (up to
`--duration` seconds each) with configurable cluster topology.

```bash
dotnet run --project benchmarks/Kuestenlogik.Surgewave.Benchmarks.RealWorld -c Release -- [scenario] [options]
```

### Scenarios

| Scenario | Description |
|----------|-------------|
| `throughput` | Max throughput measurement (msg/s and MB/s) |
| `latency` | End-to-end latency percentiles (P50/P90/P99/P99.9/P99.99) |
| `scaling` | Linear scaling verification (1, 2, 3, 5 brokers) |
| `replication` | Replication overhead (RF=1 vs RF=3) |
| `consumer` | Consumer performance scaling (1, 3, 5 consumers) |
| `failover` | Failover impact measurement (crash + recovery time) |
| `storage` | Storage engine comparison (Memory/File/Arrow) |
| `all` | All scenarios in sequence (default) |

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `--brokers N` | 3 | Number of brokers in the cluster |
| `--messages N` | 100000 | Messages to produce |
| `--message-size N` | 100 | Message size in bytes |
| `--duration N` | 60 | Max duration per scenario in seconds |
| `--batch-size N` | 1000 | Producer batch size |
| `--output FILE` | — | Save results to JSON |
| `--compare FILE` | — | Compare against a baseline JSON |
| `--report FILE` | — | Generate markdown report |

### Examples

```bash
# Throughput: 1M messages, save results
dotnet run --project benchmarks/Kuestenlogik.Surgewave.Benchmarks.RealWorld -c Release -- \
  throughput --messages 1000000

# Latency: single broker, 10K messages
dotnet run --project benchmarks/Kuestenlogik.Surgewave.Benchmarks.RealWorld -c Release -- \
  latency --brokers 1 --messages 10000

# Scaling: verify linear scale-out
dotnet run --project benchmarks/Kuestenlogik.Surgewave.Benchmarks.RealWorld -c Release -- \
  scaling --messages 500000

# Replication overhead
dotnet run --project benchmarks/Kuestenlogik.Surgewave.Benchmarks.RealWorld -c Release -- \
  replication --messages 200000

# Storage engine comparison, 1 KB messages
dotnet run --project benchmarks/Kuestenlogik.Surgewave.Benchmarks.RealWorld -c Release -- \
  storage --message-size 1024

# All scenarios, 5-minute cap per scenario, save full report
dotnet run --project benchmarks/Kuestenlogik.Surgewave.Benchmarks.RealWorld -c Release -- \
  all \
  --messages 500000 \
  --duration 300 \
  --output results/realworld.json \
  --report results/realworld-report.md

# Compare current run against committed baseline
dotnet run --project benchmarks/Kuestenlogik.Surgewave.Benchmarks.RealWorld -c Release -- \
  throughput --compare benchmarks/baselines/realworld-baseline.json
```

### Multi-Broker Cluster Setup

The real-world benchmark suite starts `--brokers` Surgewave broker instances in-process
and wires them together as a cluster. No external infrastructure is required. The
cluster is torn down automatically when the benchmark finishes.

For the `failover` scenario, one broker is intentionally crashed mid-run and the suite
measures recovery time and message loss (expected: zero with RF >= 2).

---

## Reference: Baseline Performance

The committed baseline (`benchmarks/benchmark-baseline.json`) records expected numbers
for the current hardware. Numbers below are from an Intel Core i7-1360P (13th Gen,
12 cores/16 threads), 16 GB RAM, Windows 11, .NET 10.

### Embedded Throughput (1M messages, 100 bytes, batch 1000)

| Protocol | Producer | Consumer |
|----------|----------|----------|
| Surgewave Native | 1,500,000 msg/s (143 MB/s) | 2,500,000 msg/s (238 MB/s) |
| Kafka Protocol | 1,589,825 msg/s (152 MB/s) | 2,202,643 msg/s (210 MB/s) |

### Latency (Memory storage, Native protocol, 100 bytes)

| Percentile | Produce | Consume | End-to-End |
|-----------|---------|---------|------------|
| P50 | low (target) | — | ~low (target) |
| P99 | ~low (target) | — | ~15 ms |

### See Also

- [Performance Overview](performance/index.md)
- [Tuning Guide](performance/tuning.md)
- [Regression Suite](performance/regression-suite.md)
- [Why Surgewave over Kafka?](comparison.md)
