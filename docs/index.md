# Surgewave Documentation

Surgewave is a Kafka-wire-compatible message broker written in .NET 10. It implements the Kafka 4.x protocol, plus a native protocol for first-class .NET clients, and ships an embedded mode for in-process use in tests, CLI tools, and edge runtimes.

This is the technical reference. For a marketing overview, comparison tables, and use-case stories see the [Surgewave site](https://surgewave.io).

---

## Migrating from Apache Kafka

Surgewave accepts unmodified Kafka clients on port 9092. Three integration paths cover the typical scenarios:

| Path | Code Changes | Notes | Guide |
|------|--------------|-------|-------|
| Wire compatibility | None — point existing Confluent.Kafka at the broker | Kafka protocol baseline | — |
| API Wrapper | Swap NuGet package only | Native protocol under the same producer/consumer API | [Migration Guide](migration/index.md) |
| Native Client | Use `Kuestenlogik.Surgewave.Client` directly | Full native-protocol surface | [Surgewave.Client Docs](clients/dotnet.md) |

**Quick Start:**
```csharp
// Step 1: Replace NuGet package
// Confluent.Kafka → Kuestenlogik.Surgewave.Compatibility.Confluent.Kafka

// Step 2: Add one config line for low-latency native protocol
var config = new ProducerConfig
{
    BootstrapServers = "surgewave:9092",
    SurgewaveProtocol = "surgewave"  // Enable native protocol
};

// Step 3: Your existing code works unchanged!
```

**[Complete Migration Guide →](migration/index.md)**

---

## Choose Your Path

### Application Developers

Build applications that produce and consume messages.

| Guide | Description |
|-------|-------------|
| [Quickstart](quickstart/index.md) | Get started in 5 minutes |
| [.NET Client](clients/dotnet.md) | Native Surgewave client API |
| [Confluent.Kafka Wrapper](clients/confluent-kafka-wrapper.md) | Drop-in wrapper for existing Confluent.Kafka call sites |
| [Kafka Protocol Compatibility](clients/kafka-compat.md) | Use existing Kafka clients unchanged |
| [Producer API](clients/producer.md) | Sending messages |
| [Consumer API](clients/consumer.md) | Receiving messages |
| [Streams](features/streams.md) | Real-time stream processing |
| [AI & LLM](ai/index.md) | AI pipelines, guardrails, agent memory |
| [Connectors](connectors/index.md) | Pre-built data integrations |
| [Custom Connectors](connectors/custom-connectors.md) | Build your own connectors |

### DevOps / Administrators

Deploy, configure, and operate Surgewave in production.

| Guide | Description |
|-------|-------------|
| [Installation](setup/index.md) | All deployment options |
| [Docker](deployment/docker.md) | Container deployment |
| [Kubernetes](deployment/kubernetes.md) | K8s manifests and operators |
| [Helm Charts](deployment/helm.md) | Helm-based deployment |
| [Configuration](setup/configuration.md) | All configuration options |
| [Storage](storage/index.md) | Storage backend selection |
| [Clustering](clustering/index.md) | Multi-broker setup |
| [Security](security/index.md) | SASL, TLS, ACLs |
| [Monitoring](monitoring/index.md) | Metrics, tracing, dashboards |
| [Operations](operations/index.md) | Troubleshooting and maintenance |
| [CLI Reference](tools/cli-reference.md) | 65+ administrative commands |

### Contributors / Developers

Build Surgewave from source and contribute to the project.

| Guide | Description |
|-------|-------------|
| [Building from Source](setup/building.md) | Clone, build, and run |
| [Architecture](setup/architecture.md) | System design overview |
| [Testing](setup/testing.md) | Running tests and benchmarks |
| [Chaos Testing](testing/chaos-testing.md) | Resilience and fault injection testing |
| [Regression Suite](performance/regression-suite.md) | Automated performance regression detection |

---

## Key Features

| Feature | Description |
|---------|-------------|
| **Kafka 4.0 Wire** | Accepts unmodified Kafka 4.0 clients on port 9092 |
| **Native Protocol** | Binary protocol for .NET clients alongside the Kafka wire (target — public benchmarks pending) |
| **Multi-Backend Storage** | Memory, FileSystem, Arrow, Parquet, RocksDb, Lmdb, DuckDb, Sqlite, NvmeDirect, S3, Tiered |
| **Multiple Transports** | Kafka protocol, Native binary, gRPC, Shared Memory IPC |
| **Transactions & Streams** | Exactly-once semantics, Schema Registry, Kafka Streams compatibility |
| **Kafka Connect** | 10+ built-in connectors for S3, Azure, GCS, databases, MQTT, Redis, HTTP |
| **Clustering** | Multi-broker with KRaft consensus, automatic failover |
| **Security** | SASL (PLAIN, SCRAM), TLS, ACL authorization |
| **Single Binary** | No ZooKeeper, embedded option for in-process tests |

---

## Performance

Surgewave's CI regression harness tracks throughput and transport overhead; latency-percentile measurements (P50/P99) are part of the 1.0 release sign-off and will be published in [Benchmarks](performance/benchmarks.md) when available.

A representative throughput run on 100K × 100-byte messages (Surgewave native protocol vs. Apache Kafka, single broker, same hardware) is reproduced below; for the full methodology and per-backend numbers see [Benchmarks](performance/benchmarks.md).

| Metric | Apache Kafka | Surgewave Native |
|--------|--------------|------------------|
| Producer | 68K msg/s | 1.25M msg/s |
| Consumer | 138K msg/s | 1.28M msg/s |

---

## Architecture Overview

```mermaid
flowchart TB
  subgraph Broker["Surgewave Broker"]
    subgraph Protocols["Protocols"]
      P1[Kafka Wire]
      P2[Native Binary]
      P3[gRPC]
      P4[Shared Memory]
    end
    subgraph Features["Features"]
      F1[Transactions]
      F2[Schema Registry]
      F3[Kafka Connect]
      F4[Kafka Streams]
      F5[AI Pipelines]
    end
    subgraph Storage["Storage"]
      S1[Memory]
      S2[FileSystem]
      S3[Arrow/Parquet]
      S4[RocksDb/Lmdb]
      S5["Tiered (S3/Azure)"]
    end
  end
  subgraph Clustering["Clustering (KRaft)"]
    K1[Leader Election]
    K2[Replication]
    K3[Automatic Failover]
  end
  Broker --- Clustering
```

---

## Getting Help

- [Troubleshooting](operations/troubleshooting.md) - Common issues and solutions
- [Glossary](glossary.md) - Key terms and concepts
- [GitHub Issues](https://github.com/Kuestenlogik/Surgewave/issues) - Bug reports and feature requests
- [Roadmap](https://github.com/Kuestenlogik/Surgewave/blob/main/ROADMAP.md) - Development status and plans
