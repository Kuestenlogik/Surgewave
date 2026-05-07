# API Reference

Auto-generated .NET API documentation.

## Namespaces

| Namespace | Description |
|-----------|-------------|
| `Kuestenlogik.Surgewave.Broker` | Broker implementation |
| `Kuestenlogik.Surgewave.Client` | Native .NET client |
| `Kuestenlogik.Surgewave.Core` | Core models and interfaces |
| `Kuestenlogik.Surgewave.Protocol` | Kafka protocol handlers |
| `Kuestenlogik.Surgewave.Protocol.Native` | Native protocol |
| `Kuestenlogik.Surgewave.Storage` | Storage engines |
| `Kuestenlogik.Surgewave.Grpc` | gRPC services |
| `Kuestenlogik.Surgewave.Connect` | Kafka Connect framework |
| `Kuestenlogik.Surgewave.Streams` | Kafka Streams library |

## Key Types

### Client

- `SurgewaveNativeClient` - Main native protocol client (low-level)
- `SurgewaveClient` - High-level client with protocol auto-detection
- `SurgewaveClientBuilder` - Fluent builder for creating clients
- `SurgewaveProducer<TKey, TValue>` - Typed producer
- `SurgewaveConsumer<TKey, TValue>` - Typed consumer
- `SurgewaveProducerOptions<TKey, TValue>` - Producer configuration
- `SurgewaveConsumerOptions<TKey, TValue>` - Consumer configuration

### Broker

- `SurgewaveBroker` - Main broker class
- `BrokerConfig` - Broker configuration
- `EmbeddedSurgewave` - Embedded broker for testing

### Storage

- `ISurgewaveStorageEngine` - Storage abstraction
- `ISurgewaveBuffer` - Zero-copy buffer interface
- `MemoryStorageEngine` - In-memory storage
- `WalStorageEngine` - Write-ahead log storage

### Protocol

- `SurgewavePayloadReader` - Protocol reading
- `SurgewavePayloadWriter` - Protocol writing
- `SurgewaveNativeProtocol` - Native protocol handling

## Building API Docs

```bash
cd docs
docfx metadata docfx.json
docfx build docfx.json
docfx serve ../artifacts/docs
```

## Next Steps

- [Clients](../clients/index.md) - Client usage guide
- [Samples](../samples/index.md) - Code examples
