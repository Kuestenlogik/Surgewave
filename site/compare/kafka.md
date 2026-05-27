---
layout: page
title: Surgewave vs Kafka
subtitle: Same wire protocol. Single binary. Embeddable. No JVM.
description: A side-by-side comparison of Surgewave and Apache Kafka — deployment, coordination, latency, embeddability.
permalink: /compare/kafka/
---

Surgewave is designed for teams who need Kafka's capabilities without its operational overhead.

## The Problem with Kafka

Apache Kafka is powerful but comes with significant complexity:

- **ZooKeeper dependency** - Requires a separate distributed system to manage
- **JVM tuning** - Complex heap sizing, GC tuning, and memory management
- **Operational overhead** - Multiple processes, configuration files, and monitoring points
- **Testing friction** - Difficult to embed in unit tests; requires Docker or external services
- **Latency floor** - JVM warm-up and GC pauses limit achievable latency

## How Surgewave Solves This

| Challenge | Kafka | Surgewave |
|-----------|-------|-------|
| **Coordination** | Requires ZooKeeper or separate KRaft cluster | Built-in KRaft consensus |
| **Deployment** | Multiple JARs, configs, JVM tuning | Single binary, sensible defaults |
| **Testing** | Docker containers or external services | Embedded broker in-process |
| **Latency** | Kafka-protocol baseline | lower-latency native path |
| **Memory** | Requires heap tuning | Efficient .NET memory management |
| **Protocol** | Kafka protocol only | Kafka + Native + gRPC + SharedMemory |

## When to Use Surgewave

**Choose Surgewave when you:**

- Need Kafka protocol compatibility with simpler operations
- Want sub-millisecond latency for real-time workloads
- Need embedded testing without Docker
- Are building on .NET and want native integration
- Want a single binary deployment
- Need multiple transport options (gRPC, shared memory IPC)

**Consider staying with Kafka when you:**

- Have existing Kafka operational expertise and tooling
- Need mature multi-datacenter replication (MirrorMaker ecosystem)
- Require Kafka-specific integrations not yet available in Surgewave
- Are already running in a managed Kafka service (Confluent Cloud, AWS MSK)

## Feature Comparison

| Feature | Apache Kafka | Surgewave | Notes |
|---------|--------------|-------|-------|
| Kafka Protocol | 4.0 | 4.0 | Full compatibility |
| ZooKeeper-free | KRaft (separate) | Built-in | Surgewave includes KRaft |
| Transactions | Yes | Yes | Exactly-once semantics |
| Schema Registry | Separate service | Built-in | Avro, JSON, Protobuf |
| Kafka Connect | Yes | Yes | .NET-native connectors |
| Kafka Streams | Java only | .NET LINQ-style | Native C# API |
| Consumer Groups | Yes | Yes | Full rebalancing support |
| ACLs | Yes | Yes | Role-based access |
| TLS/SASL | Yes | Yes | PLAIN, SCRAM, mTLS |
| Log Compaction | Yes | Yes | Key-based deduplication |
| Tiered Storage | Confluent only | Built-in | S3, Azure Blob, GCP |
| Embedded Mode | No | Yes | In-process for testing |
| SharedMemory IPC | No | Yes | ultra-low (target) same-machine latency |
| License (core) | Apache 2.0 | Apache 2.0 | Same OSI-approved posture |

## Performance

Comparative numbers against Apache Kafka are preliminary (target) until
the 1.0 head-to-head publication; the CI regression harness tracks
absolute Surgewave throughput continuously. The table below shows a
representative run for orientation; reproduce via
`scripts/run-all-benchmarks.ps1`.

### Throughput &mdash; 100K messages, 100 bytes (target)

| Metric | Apache Kafka | Surgewave Native |
|--------|--------------|------------------|
| Producer | 68K msg/s | 1.25M msg/s |
| Consumer | 138K msg/s | 1.28M msg/s |

### Latency &mdash; Memory storage, 1KB messages (target)

| Protocol | P50 Produce | P99 Produce | P50 E2E |
|----------|-------------|-------------|---------|
| Surgewave Native | low (target) | low (target) | low (target) |
| Kafka Protocol | Kafka-protocol baseline | preliminary | Kafka-protocol baseline |

## Migration Path

Surgewave's Kafka protocol compatibility means you can:

1. **Drop-in replacement** - Point existing Kafka clients at Surgewave without code changes
2. **Gradual migration** - Run Surgewave alongside Kafka during transition
3. **Native upgrade** - Optionally switch to Surgewave's native protocol for better performance

```csharp
// Your existing Kafka client code works unchanged
var config = new ProducerConfig
{
    BootstrapServers = "surgewave-broker:9092"  // Just change the address
};
using var producer = new ProducerBuilder<string, string>(config).Build();
```

## Next Steps

- [Quickstart](/docs/quickstart/) &mdash; Get running in 5 minutes
- [Installation](/docs/setup/) &mdash; Deployment options
- [Architecture](/docs/architecture.html) &mdash; How Surgewave works internally

## Also see

- [Surgewave vs Redpanda](/compare/redpanda/) &mdash; both reject the JVM
- [Surgewave vs Aeron](/compare/aeron/) &mdash; library vs broker
