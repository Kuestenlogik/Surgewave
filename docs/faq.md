# Frequently Asked Questions

Common questions about Surgewave.

## General

### What is Surgewave?

Surgewave is a Kafka-wire-compatible message broker written in .NET 10. It accepts unmodified Kafka 4.0 clients on port 9092 and adds a native binary protocol for lower-latency .NET workloads. Comparative benchmarks against Apache Kafka are tracked under [Benchmarks](performance/benchmarks.md).

### How does Surgewave compare to Kafka?

| Aspect | Apache Kafka | Surgewave |
|--------|--------------|-------|
| Protocol | Kafka 4.0 | Kafka 4.0 + Native |
| Latency (P50) | Kafka-protocol overhead | Native path targets lower latency |
| Dependencies | ZooKeeper/KRaft | None (built-in) |
| Deployment | Multiple JARs | Single binary |
| Testing | Docker required | Embedded/InMemory |

### Can I use my existing Kafka clients?

Yes. Surgewave is 100% compatible with the Kafka 4.0 protocol. Any Kafka client (Confluent.Kafka, librdkafka, etc.) works unchanged. Just point your `bootstrap.servers` to Surgewave.

### What .NET versions are supported?

Surgewave requires .NET 10 or later. It uses modern C# features and is optimized for performance with Span<T>, Memory<T>, and SIMD operations.

---

## Testing & Development

### How do I run integration tests without Docker?

Surgewave's embedded broker allows integration testing without external dependencies:

```csharp
[Fact]
public async Task MyIntegrationTest()
{
    await using var surgewave = new EmbeddedSurgewave();
    await surgewave.StartAsync();

    var client = surgewave.CreateClient();
    // Your test code here
}
```

### Can I use in-memory storage for tests?

Yes. Configure memory storage for fastest test execution:

```csharp
var surgewave = new EmbeddedSurgewave(options =>
{
    options.StorageMode = StorageMode.Memory;
});
```

### How fast do tests start?

Embedded Surgewave starts in milliseconds, not seconds. No JVM startup, no container initialization. This enables true unit-test-speed integration testing.

### Can I run multiple brokers in tests?

Yes, for testing clustering scenarios:

```csharp
await using var cluster = new EmbeddedSurgewaveCluster(nodeCount: 3);
await cluster.StartAsync();
```

---

## Compatibility

### Which Kafka APIs are supported?

All 75+ Kafka 4.0 APIs are implemented:
- Produce, Fetch, ListOffsets, Metadata
- Consumer Groups (JoinGroup, SyncGroup, Heartbeat, LeaveGroup)
- Admin APIs (CreateTopics, DeleteTopics, etc.)
- Transactions (InitProducerId, AddPartitionsToTxn, EndTxn)
- ACLs, Quotas, and more

### Does Surgewave support Kafka Streams?

Yes. Surgewave includes a Kafka Streams-compatible library with:
- Stream-Stream joins (inner, left, outer)
- Table-Table joins
- Windowed aggregations
- State stores with changelog

### Does Surgewave support Kafka Connect?

Yes. Surgewave includes a Kafka Connect-compatible framework with:
- Source and Sink connectors
- Distributed coordination
- Built-in connectors (FileStream, HTTP, Database, S3)
- MirrorMaker 2.0 for cross-cluster replication

### Does Surgewave support Schema Registry?

Yes. Confluent Schema Registry-compatible with support for:
- Avro, JSON Schema, Protobuf, FlatBuffers
- Compatibility checking
- Schema evolution

---

## Performance

### What latency can I expect?

The Surgewave native protocol targets sub-millisecond P50 produce on warm in-memory storage; the Kafka wire path matches upstream Kafka's protocol budget; Shared Memory IPC targets microsecond-class latency for same-host clients. Head-to-head numbers on identical hardware will be published with the 1.0 release.

### What throughput can I expect?

With 100-byte messages:
- **Producer**: 1.25M msg/s (Native protocol)
- **Consumer**: 1.28M msg/s (Native protocol)

### How do I get the best performance?

1. Use the **Native protocol** for .NET clients (much lower latency than Kafka protocol)
2. Use **Memory** or **ZeroCopyWal** storage for lowest latency
3. Use **Shared Memory IPC** for same-machine communication
4. Enable **batching** for throughput-oriented workloads

### What storage backend should I use?

| Backend | Use Case | Persistence |
|---------|----------|-------------|
| Memory | Testing, caching | No |
| FileSystem | General purpose | Yes |
| ZeroCopyWal | High performance | Yes |
| Arrow | Analytics | Yes |
| Tiered | Cost optimization | Yes (cloud) |

---

## Operations

### How do I deploy Surgewave?

Multiple options:
- **Single binary**: `surgewave-broker` executable
- **Docker**: `docker run ghcr.io/kuestenlogik/surgewave`
- **Kubernetes**: Helm chart or manifests
- **Embedded**: In-process for testing

### Does Surgewave support clustering?

Yes. Multi-broker clustering with:
- KRaft consensus (no ZooKeeper)
- Automatic leader election
- Configurable replication
- Rack-aware replica placement

### How do I monitor Surgewave?

- **Prometheus metrics**: Built-in `/metrics` endpoint
- **OpenTelemetry**: Distributed tracing support
- **Grafana dashboards**: Pre-built dashboards included
- **CLI**: `surgewave health`, `surgewave cluster status`

### How do I secure Surgewave?

- **SASL**: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512
- **TLS**: Encryption in transit
- **mTLS**: Mutual TLS for client auth
- **ACLs**: Fine-grained authorization
- **OAuth2/OIDC**: JWT validation with JWKS

---

## Troubleshooting

### Where are the logs?

Logs go to stdout by default. Configure in `appsettings.json`:

```json
{
  "Logging": {
    "LogLevel": {
      "Default": "Information",
      "Kuestenlogik.Surgewave": "Debug"
    }
  }
}
```

### How do I diagnose connection issues?

```bash
surgewave diagnose
surgewave health --verbose
```

### Where can I get help?

- [Troubleshooting Guide](operations/troubleshooting.md)
- [GitHub Issues](https://github.com/Kuestenlogik/Surgewave/issues)
- [Documentation](index.md)

---

## See Also

- [Quickstart](quickstart/index.md) - Get started in 5 minutes
- [Configuration](setup/configuration.md) - All configuration options
- [Troubleshooting](operations/troubleshooting.md) - Common issues and solutions
