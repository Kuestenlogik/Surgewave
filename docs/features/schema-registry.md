# Schema Registry

Surgewave includes a Confluent-compatible Schema Registry.

## Overview

Schema Registry manages schemas for:
- **Avro** - Apache Avro schemas
- **JSON Schema** - JSON-based schemas
- **Protobuf** - Protocol Buffers
- **FlatBuffers** - Memory-efficient serialization

## Configuration

```json
{
  "Surgewave": {
    "SchemaRegistry": {
      "Enabled": true,
      "DataPath": "./data/schemas",
      "DefaultCompatibility": "Backward"
    }
  }
}
```

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `Enabled` | bool | false | Enable Schema Registry |
| `DataPath` | string | ./data/schemas | Path to store schema data |
| `DefaultCompatibility` | string | Backward | Default compatibility for new subjects |

## CLI Usage

### List Subjects

```bash
surgewave schema list
surgewave schema list --include-deleted
```

### Register Schema

```bash
# Avro from string
surgewave schema register user-value --schema '{
  "type": "record",
  "name": "User",
  "fields": [
    {"name": "id", "type": "int"},
    {"name": "name", "type": "string"},
    {"name": "email", "type": "string"}
  ]
}'

# From file
surgewave schema register user-value --file user.avsc --type AVRO
surgewave schema register events-value --file events.proto --type PROTOBUF
```

### Describe Subject

```bash
surgewave schema describe user-value
```

### Get Schema

```bash
surgewave schema get --id 1
surgewave schema get --subject user-value --version latest
surgewave schema get --subject user-value --version 2
```

### Compatibility

```bash
# Check before registering
surgewave schema compatibility check user-value --file new-user.avsc

# Get current level
surgewave schema compatibility get --subject user-value

# Set compatibility
surgewave schema compatibility set BACKWARD --subject user-value
```

### Delete

```bash
surgewave schema delete-subject user-value
surgewave schema delete-version user-value 1
```

## Compatibility Levels

| Level | Description |
|-------|-------------|
| NONE | No compatibility checking |
| BACKWARD | New can read old |
| FORWARD | Old can read new |
| FULL | Both backward and forward |
| BACKWARD_TRANSITIVE | Backward with all versions |
| FORWARD_TRANSITIVE | Forward with all versions |
| FULL_TRANSITIVE | Full with all versions |

## REST API

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/subjects` | GET | List all subjects |
| `/subjects/{subject}/versions` | GET | List versions |
| `/subjects/{subject}/versions` | POST | Register schema |
| `/subjects/{subject}/versions/{version}` | GET | Get schema |
| `/schemas/ids/{id}` | GET | Get by ID |
| `/config` | GET/PUT | Global config |
| `/config/{subject}` | GET/PUT | Subject config |
| `/compatibility/subjects/{subject}/versions/{version}` | POST | Check compatibility |

## Client Usage

### .NET Producer

```csharp
using Kuestenlogik.Surgewave.Client;
using Kuestenlogik.Surgewave.Schema.Registry.Serdes.Avro;

await using var producer = new SurgewaveProducer<string, User>(options =>
{
    options.BootstrapServers = "localhost:9092";
    options.AsyncValueSerializer = new SchemaRegistryAvroSerializer<User>(
        new AvroSerializerConfig
        {
            SchemaRegistryUrl = "https://localhost:9093"
        });
});

await producer.ProduceAsync("users", "user-1", new User { Id = 1, Name = "Alice" });
```

### .NET Consumer

```csharp
await using var consumer = new SurgewaveConsumer<string, User>(options =>
{
    options.BootstrapServers = "localhost:9092";
    options.GroupId = "user-processor";
    options.AsyncValueDeserializer = new SchemaRegistryAvroDeserializer<User>(
        new AvroSerializerConfig
        {
            SchemaRegistryUrl = "https://localhost:9093"
        });
});

consumer.Subscribe("users");
while (true)
{
    var record = await consumer.ConsumeAsync();
    if (record != null)
        Console.WriteLine($"User: {record.Value.Name}");
}
```

## Schema Types

### Avro

```json
{
  "type": "record",
  "name": "Order",
  "namespace": "com.example",
  "fields": [
    {"name": "id", "type": "long"},
    {"name": "customerId", "type": "string"},
    {"name": "amount", "type": "double"},
    {"name": "status", "type": {"type": "enum", "name": "Status", "symbols": ["PENDING", "COMPLETED"]}}
  ]
}
```

### JSON Schema

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "properties": {
    "id": {"type": "integer"},
    "name": {"type": "string"},
    "email": {"type": "string", "format": "email"}
  },
  "required": ["id", "name"]
}
```

### Protobuf

```protobuf
syntax = "proto3";
package example;

message User {
  int32 id = 1;
  string name = 2;
  string email = 3;
}
```

### Additional Formats

Surgewave supports 8 additional serialization formats via Schema Registry handlers:

| Format | Type Name | Content Type | Use Case |
|--------|-----------|-------------|----------|
| Hyperion | HYPERION | application/x-hyperion | Akka.NET integration |
| MessagePack | MSGPACK | application/x-msgpack | High-performance .NET (SignalR) |
| CBOR | CBOR | application/cbor | IoT (CoAP/MQTT) |
| Bond | BOND | application/x-bond | Microsoft/Azure |
| Thrift | THRIFT | application/x-thrift | Apache/Meta |
| MemoryPack | MEMORYPACK | application/x-memorypack | Ultra-fast .NET |
| Cap'n Proto | CAPNPROTO | application/x-capnproto | Zero-copy RPC |
| Orleans | ORLEANS | application/x-orleans | Microsoft Orleans Grains |

Schemaless formats (Hyperion, MessagePack, CBOR, MemoryPack, Orleans) use the type name as a hint.
Schema-based formats (Bond, Thrift, Cap'n Proto) support schema validation and compatibility checking.

All formats use the Confluent wire format: `[0x00][4-byte schema ID][payload]`.

## Best Practices

1. **Use Backward Compatibility** - Consumers can handle new data
2. **Add Optional Fields** - Never remove or rename required fields
3. **Version Subjects** - Use `-value` and `-key` suffixes
4. **Cache Schemas** - Use `CachedSchemaRegistryClient`

## Next Steps

- [Transactions](transactions.md) - Exactly-once semantics
- [Clients](../clients/index.md) - Client integration
