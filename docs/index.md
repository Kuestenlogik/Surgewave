# Surgewave Documentation

**Surgewave** is the complete event streaming platform for .NET 10 — native low-latency transport, signed plugin marketplace, built-in AI pipeline nodes, embedded broker for tests and edge. Full Kafka 4.0 wire compatibility ships in the box, so existing Kafka clients connect unchanged.

**[Why Surgewave?](comparison.md)** | **[Use Cases](use-cases.md)** | **[Get Started in 5 Minutes](quickstart/index.md)**

---

## Already on Kafka? Three migration paths

If you're moving from Apache Kafka, Surgewave meets you on the wire and gives you a gradient toward the native protocol:

| Path | Code Changes | Performance | Guide |
|------|--------------|-------------|-------|
| **Direct Compatibility** | None | Kafka baseline | Point existing Confluent.Kafka to Surgewave |
| **API Wrapper** | Package swap only | **much lower latency** | [Migration Guide](migration/index.md) |
| **Native Client** | Full rewrite | Optimal | [Surgewave.Client Docs](clients/dotnet.md) |

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
| [Use Cases](use-cases.md) | Real-world scenarios and patterns |
| [Quickstart](quickstart/index.md) | Get started in 5 minutes |
| [.NET Client](clients/dotnet.md) | Native Surgewave client API |
| [Confluent.Kafka Wrapper](clients/confluent-kafka-wrapper.md) | Zero-code migration from Kafka |
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
| **Kafka 4.0 Compatible** | 100% protocol compatibility with existing clients |
| **Native Protocol** | Lower-latency .NET path alongside the Kafka wire (target — public benchmarks pending) |
| **Multi-Backend Storage** | Memory, FileSystem, Arrow, Parquet, RocksDb, Lmdb, DuckDb, Sqlite, NvmeDirect, S3, Tiered |
| **Multiple Transports** | Kafka protocol, Native binary, gRPC, Shared Memory IPC |
| **Enterprise Features** | Transactions, Schema Registry, Kafka Streams |
| **Kafka Connect** | 10+ built-in connectors for S3, Azure, GCS, databases, MQTT, Redis, HTTP |
| **Clustering** | Multi-broker with KRaft consensus, automatic failover |
| **Security** | SASL (PLAIN, SCRAM), TLS, ACL authorization |
| **Easy Operations** | Single binary, zero ZooKeeper, embedded option |

---

## Performance

**Throughput (100K messages, 100 bytes):**

| Metric | Apache Kafka | Surgewave Native | Improvement |
|--------|--------------|--------------|-------------|
| Producer | 68K msg/s | 1.25M msg/s | (see benchmarks) |
| Consumer | 138K msg/s | 1.28M msg/s | +826% |

**Latency:** The Surgewave native protocol targets lower latency than the Kafka wire on the same broker; comparative head-to-head numbers will be published with the 1.0 release.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        Surgewave Broker                              │
├─────────────────────────────────────────────────────────────────┤
│  Protocols          │  Features           │  Storage             │
│  ─────────          │  ────────           │  ───────             │
│  • Kafka Wire       │  • Transactions     │  • Memory            │
│  • Native Binary    │  • Schema Registry  │  • FileSystem        │
│  • gRPC             │  • Kafka Connect    │  • Arrow/Parquet     │
│  • Shared Memory    │  • Kafka Streams    │  • RocksDb/Lmdb      │
│                     │  • AI Pipelines     │  • Tiered (S3/Azure) │
├─────────────────────────────────────────────────────────────────┤
│                     Clustering (KRaft)                           │
│  • Leader Election  • Replication  • Automatic Failover          │
└─────────────────────────────────────────────────────────────────┘
```

---

## Getting Help

- [Troubleshooting](operations/troubleshooting.md) - Common issues and solutions
- [Glossary](glossary.md) - Key terms and concepts
- [GitHub Issues](https://github.com/Kuestenlogik/Surgewave/issues) - Bug reports and feature requests
- [Roadmap](https://github.com/Kuestenlogik/Surgewave/blob/main/ROADMAP.md) - Development status and plans
