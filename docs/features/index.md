# Features

Advanced Surgewave features for enterprise deployments.

## Available Features

| Feature | Description |
|---------|-------------|
| [Kafka Connect](connect.md) | Source and sink connector framework with plugin system and AI pipeline support |
| [Kafka Streams](streams.md) | Stream processing library |
| [Schema Registry](schema-registry.md) | Schema management (Avro, JSON, Protobuf, FlatBuffers) |
| [Transactions](transactions.md) | Exactly-once semantics |
| [Quotas](quotas.md) | Rate limiting and throttling |
| [Log Compaction](compaction.md) | Automatic key-based deduplication |
| [Per-Message TTL](ttl.md) | Time-to-live with automatic expiration on fetch |
| [Dead Letter Queue](dlq.md) | Broker-level DLQ with native Nack protocol support |
| [SQL Query Engine](sql-engine.md) | Query Surgewave topics using SQL with windowed aggregations |
| [WASM Plugins](wasm-plugins.md) | Language-agnostic sandboxed plugins (Rust, Go, AssemblyScript) |
| [ML Scoring](ml-scoring.md) | Real-time ONNX model inference on streaming data |
| [AI Pipelines](../ai/index.md) | LLM integration, guardrails, agent memory, pipeline chat |
| [Bandwidth Quotas](bandwidth-quotas.md) | Per-client and per-user bandwidth throttling |
| [Cruise Control](cruise-control.md) | Automatic partition balancing across brokers |
| [Schema Linking](schema-linking.md) | Cross-cluster schema synchronization |
| [Schema Migration](schema-migration.md) | Zero-downtime on-read/on-write schema transformation |
| [Cluster Linking](cluster-linking.md) | Topic-level cross-cluster mirroring with offset translation |
| [Intent-Based Configuration](intent-based-config.md) | Natural-language topic configuration with 16 built-in rules |
| [Low-Code Data Mesh](data-mesh.md) | Topics as Data Products with contracts, SLOs, and lineage |
| [Cross-Topic Transactions](cross-topic-transactions.md) | Atomic writes across multiple topics with two-phase commit |
| [Exactly-Once Source Connectors](eos-connectors.md) | Source connectors with transactional offset tracking |
| [Agent Design Studio](../ai/agent-design-studio.md) | Visual 6-tab agent builder with test chat and deployment |
| [AMQP Adapter](../amqp-adapter.md) | AMQP 0.9.1 protocol support for RabbitMQ-compatible clients |
| [Streaming Consumer](../streaming-consumer.md) | Push-based consumer with credit-based flow control |
| [Priority Lanes](../priority-lanes.md) | Per-message priority routing with weighted consumer polling |
| [Interactive Query Service](../interactive-queries.md) | REST API for querying Streams state stores |
| [Circuit Breaker](../circuit-breaker.md) | Resilience pattern for stream processing external calls |

## Feature Comparison with Kafka

| Feature | Apache Kafka | Surgewave |
|---------|--------------|-------|
| Connect | Java-based | .NET-native with plugin system |
| Streams | Java DSL | .NET LINQ-style |
| Schema Registry | Separate service | Built-in |
| Transactions | Supported | Supported + cross-topic atomic writes |
| Quotas | Supported | Supported + bandwidth throttling |
| Per-Message TTL | Not built-in | Broker-native with `surgewave-ttl-ms` header |
| Dead Letter Queue | Client-side only | Broker-native with Nack protocol |
| SQL Engine | ksqlDB (separate) | Built-in with windowed aggregations |
| WASM Plugins | Not available | Sandboxed, hot-deployable, any language |
| ML Scoring | Not available | Built-in ONNX inference in pipelines |
| Schema Linking | Confluent only | Built-in cross-cluster schema sync |
| Cluster Linking | Confluent only | Built-in with offset translation and failover |
| Cruise Control | Separate Java service | Built-in with weighted scoring |
| Data Mesh | Not available | Built-in data product catalog with contracts |
| Agent Design Studio | Not available | Visual agent builder with test chat |
| AMQP 0.9.1 | RabbitMQ (separate) | Built-in adapter; same port (5672) |
| Streaming Consumer | Poll-only | Push-based with credit flow control |
| Priority Lanes | Not available | Three-tier partition layout with weighted polling |
| Interactive Queries | ksqlDB REST (separate) | Built-in REST API for Streams state stores |
| Circuit Breaker | Not available | Built-in Streams resilience with thread-safe state machine |

## Configuration

Features can be enabled/disabled in `appsettings.json`:

```json
{
  "Surgewave": {
    "SchemaRegistry": {
      "Enabled": true
    },
    "Connect": {
      "Enabled": true
    },
    "Transactions": {
      "Enabled": true,
      "TimeoutMs": 60000
    }
  }
}
```

## Next Steps

- [Connect](connect.md) - Build data pipelines
- [Streams](streams.md) - Real-time processing
- [Schema Registry](schema-registry.md) - Schema management
- [Per-Message TTL](ttl.md) - Automatic message expiration
- [Dead Letter Queue](dlq.md) - Failed message routing
- [SQL Query Engine](sql-engine.md) - Query topics with SQL
- [WASM Plugins](wasm-plugins.md) - Extend Surgewave with any language
- [ML Scoring](ml-scoring.md) - Real-time ONNX inference
- [Bandwidth Quotas](bandwidth-quotas.md) - Per-client bandwidth throttling
- [Cruise Control](cruise-control.md) - Automatic partition balancing
- [Schema Linking](schema-linking.md) - Cross-cluster schema sync
- [Schema Migration](schema-migration.md) - Zero-downtime schema evolution
- [Cluster Linking](cluster-linking.md) - Cross-cluster topic mirroring
- [Intent-Based Configuration](intent-based-config.md) - Natural-language topic config
- [Low-Code Data Mesh](data-mesh.md) - Data product catalog with contracts
- [Cross-Topic Transactions](cross-topic-transactions.md) - Atomic multi-topic writes
- [Exactly-Once Source Connectors](eos-connectors.md) - EOS for Connect sources
- [Agent Design Studio](../ai/agent-design-studio.md) - Visual agent builder
- [AMQP Adapter](../amqp-adapter.md) - RabbitMQ-compatible protocol support
- [Streaming Consumer](../streaming-consumer.md) - Push-based consumption
- [Priority Lanes](../priority-lanes.md) - Message priority routing
- [Interactive Query Service](../interactive-queries.md) - Query state stores over REST
- [Circuit Breaker](../circuit-breaker.md) - Resilience for stream processing
