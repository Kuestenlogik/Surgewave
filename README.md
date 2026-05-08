[![CI](https://github.com/Kuestenlogik/Surgewave/actions/workflows/ci.yml/badge.svg)](https://github.com/Kuestenlogik/Surgewave/actions/workflows/ci.yml)
[![.NET 10](https://img.shields.io/badge/.NET-10.0-512BD4)](https://dotnet.microsoft.com/)
[![Kafka Compatible](https://img.shields.io/badge/Kafka-4.0%20Compatible-231F20)](https://kafka.apache.org/)
[![License](https://img.shields.io/badge/License-BSL%201.1-blue.svg)](LICENSE)

# Surgewave

**The complete event streaming platform for .NET 10.**

> *Named after the storm surge that haunts the North German coast of [Theodor Storm](https://en.wikipedia.org/wiki/Theodor_Storm), just as Apache Kafka was named after [Franz Kafka](https://en.wikipedia.org/wiki/Franz_Kafka). Taming the surgewave of events, at any scale, anywhere.*

Surgewave is a complete event streaming platform built end-to-end on .NET 10 — native low-latency transport, signed plugin marketplace, built-in AI pipeline nodes, embedded broker for tests and edge. Full Kafka 4.0 wire compatibility ships in the box, so any existing Kafka client (Confluent.Kafka, librdkafka, kafka-go, …) connects on day one. Adopt Surgewave as your streaming platform; keep your existing Kafka producers and consumers working unchanged.

## What Surgewave Does

Surgewave connects your services, devices, and data in real time. It distributes events reliably, processes streams on the fly, and stores them durably.

- Decouple microservices with pub/sub and request/reply
- Ingest IoT data from edge devices to the cloud
- Replicate database changes with built-in Change Data Capture (CDC)
- Transform, filter, join, and aggregate streams in real time. Use a fluent .NET API or SQL.
- Store events with configurable retention. Choose the storage engine for your use case: file-based, in-memory, shared-memory, database, or bring your own.
- Implement CQRS and event sourcing with ordered, replayable event logs
- Built-in dashboards and materialized views. Integrate into your existing monitoring via OpenTelemetry (OTEL).

## Why Surgewave?

**Easy to adopt**
- Wire-compatible with Kafka 4.0. Run alongside Kafka or replace it entirely.
- Your existing clients, tools, and monitoring keep working.
- Just swap the broker and keep your Kafka clients. Or use Surgewave's .NET client, which switches protocols at runtime.
- Migrate service by service, in either direction.

**Easy to operate**
- Pure .NET, no JVM, no ZooKeeper. One technology stack, one team.
- Run embedded in your app, as a standalone broker, or scaled out across a cluster.
- Your .NET team can build, deploy, and troubleshoot everything with the skills they already have.

**Built for performance**
- Designed from scratch on .NET 10 with zero-copy and io_uring.
- Lower latency, higher throughput, less hardware.

**Built for extensibility**
- Add storage engines, protocol adapters, or connectors as plugins.
- Package as `.swpkg` files, install at runtime, no fork required.

## Quick Start

**Start the broker:**
```bash
docker run -p 9092:9092 -p 5050:5050 ghcr.io/kuestenlogik/surgewave
```

Open the **Control UI** at [localhost:5050](http://localhost:5050). See the [Control UI guide](docs/quickstart/) for a walkthrough.

> No Docker? See [Building from Source](#building-from-source) below.

**Connect with any Kafka client:**
```csharp
var config = new ProducerConfig { BootstrapServers = "localhost:9092" };
```

## Learn More

| | |
|---|---|
| [Getting Started](docs/quickstart/) | Install, configure, and run your first producer/consumer |
| [.NET Client](docs/clients/dotnet.md) | Producer, consumer, and admin APIs with Source Link debugging |
| [Kafka Conformance](CONFORMANCE.md) | Per-RPC and per-KIP status table — what's wired, stubbed, and out of scope |
| [Schema Registry](docs/features/schema-registry.md) | 12 serialization formats with compatibility checking |
| [Stream Processing](docs/features/streams.md) | Real-time transforms, joins, aggregations, and SQL |
| [Plugin Development](docs/features/plugin-development.md) | Build and package your own storage engines, connectors, or protocol adapters |
| [CLI Reference](docs/tools/cli-reference.md) | Manage topics, groups, schemas, and plugins from the command line |

## Building from Source

Requires [.NET 10 SDK](https://dotnet.microsoft.com/download/dotnet/10.0).

```bash
git clone https://github.com/Kuestenlogik/Surgewave.git
cd Surgewave
dotnet build Kuestenlogik.Surgewave.slnx -c Release
dotnet run --project src/Kuestenlogik.Surgewave.Broker
```

For the full step-by-step guide — build, publish, and run in all variants (development, self-contained executables, Docker containers) — see [docs/setup/building.md](docs/setup/building.md).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). All contributions require signing a [CLA](.github/CLA.md).

## License

[Business Source License 1.1](LICENSE) — each version converts to [Apache 2.0](https://www.apache.org/licenses/LICENSE-2.0) five years after release. Internal use is free. For commercial redistribution: [licensing@kuestenlogik.com](mailto:licensing@kuestenlogik.com).
