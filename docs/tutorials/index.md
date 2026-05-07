# Surgewave Tutorials

Welcome to the Surgewave tutorials! These step-by-step guides will help you master Surgewave's features and capabilities.

## Getting Started

| # | Tutorial | Description |
|---|----------|-------------|
| 01 | [Getting Started](01-getting-started.md) | Install Surgewave, start a broker, send your first message |
| 02 | [Producers & Consumers](02-producers-consumers.md) | Message production and consumption patterns |
| 03 | [Consumer Groups](03-consumer-groups.md) | Load balancing, rebalancing, offset management |

## Advanced Topics

| # | Tutorial | Description |
|---|----------|-------------|
| 04 | [Kafka Connect](04-kafka-connect.md) | Building and running connectors |
| 05 | [Kafka Streams](05-kafka-streams.md) | Stream processing with joins and aggregations |
| 06 | [Schema Registry](06-schema-registry.md) | Avro, JSON Schema, Protobuf serialization |
| 07 | [Clustering](07-clustering.md) | Multi-broker setup, replication, failover |

## AI Integration

| # | Tutorial | Description |
|---|----------|-------------|
| 08 | [AI Agents](08-ai-agents.md) | Building agents with Surgewave Agent Framework |

## Sample Projects

Each tutorial has a companion project in the `tutorials/` folder that you can open and run:

```
tutorials/
├── 01-getting-started/
├── 02-producers-consumers/
├── 03-consumer-groups/
├── 04-kafka-connect/
├── 05-kafka-streams/
├── 06-schema-registry/
├── 07-clustering/
└── 08-ai-agents/
```

## Full Demo Applications

For complete, runnable applications that demonstrate Surgewave features, see the `samples/` folder:

- **StandaloneDemo** - Demonstrates Kafka protocol compatibility with Confluent.Kafka client
- **AgentDemo** - Example AI agents using the Surgewave Agent Framework

## Quick Reference

For API documentation and feature reference, see the main [documentation](../index.md).
