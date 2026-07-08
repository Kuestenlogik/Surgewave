# Tutorial 06: Schema Registry

Manage schemas for your topics with validation, evolution, and compatibility checking.

## Prerequisites

- A Surgewave broker running on `localhost:9092` with Schema Registry enabled
- .NET 10 SDK installed

## What You Will Build

A schema-managed user event pipeline where:
1. You register a JSON Schema for user events
2. A producer validates messages against the schema before sending
3. A consumer deserializes with schema awareness
4. You evolve the schema safely with compatibility checks

## Step 1: Enable Schema Registry

Add the following to your Surgewave broker configuration (`appsettings.json`):

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

The Schema Registry API becomes available on the broker's gRPC port (default `9093`).

## Step 2: Register a Schema

### Using the CLI

Register a JSON Schema for user events:

```bash
surgewave schema register users-value --schema '{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "properties": {
    "id": { "type": "integer" },
    "name": { "type": "string" },
    "email": { "type": "string", "format": "email" }
  },
  "required": ["id", "name"]
}'
```

Expected output:

```
Schema registered: subject=users-value, id=1, version=1
```

### Using the REST API

```bash
curl -X POST https://localhost:9093/subjects/users-value/versions \
  -H "Content-Type: application/json" \
  -d '{
    "schemaType": "JSON",
    "schema": "{\"$schema\":\"http://json-schema.org/draft-07/schema#\",\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"integer\"},\"name\":{\"type\":\"string\"},\"email\":{\"type\":\"string\",\"format\":\"email\"}},\"required\":[\"id\",\"name\"]}"
  }'
```

### From a File

Save your schema to `user.json`:

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "properties": {
    "id": { "type": "integer" },
    "name": { "type": "string" },
    "email": { "type": "string", "format": "email" }
  },
  "required": ["id", "name"]
}
```

```bash
surgewave schema register users-value --file user.json --type JSON
```

## Step 3: Produce with Schema Validation

Create a project and add packages:

```bash
mkdir surgewave-schema-tutorial && cd surgewave-schema-tutorial
dotnet new console -n SchemaProducer
cd SchemaProducer
dotnet add package Kuestenlogik.Surgewave.Client
dotnet add package Kuestenlogik.Surgewave.Schema.Registry.Client
```

`SchemaProducer/Program.cs`:

```csharp
using Kuestenlogik.Surgewave.Client;
using Kuestenlogik.Surgewave.Schema.Registry.Client;

// Define the User type matching the schema
public record User(int Id, string Name, string Email);

// Create a producer with schema registry validation
await using var producer = new SurgewaveProducer<string, User>(options =>
{
    options.BootstrapServers = "localhost:9092";
    options.AsyncValueSerializer = new SchemaRegistryJsonSerializer<User>(
        new JsonSchemaSerializerConfig
        {
            SchemaRegistryUrl = "https://localhost:9093",
            AutoRegisterSchemas = true,   // Register schema if not exists
            SubjectNameStrategy = SubjectNameStrategy.TopicName
        });
});

// Produce valid users
var users = new[]
{
    new User(1, "Alice", "alice@example.com"),
    new User(2, "Bob", "bob@example.com"),
    new User(3, "Charlie", "charlie@example.com")
};

foreach (var user in users)
{
    var offset = await producer.ProduceAsync("users", $"user-{user.Id}", user);
    Console.WriteLine($"Produced user {user.Name} at offset {offset}");
}

await producer.FlushAsync();
Console.WriteLine("All users produced with schema validation.");
```

## Step 4: Consume with Schema Deserialization

```bash
dotnet new console -n SchemaConsumer
cd SchemaConsumer
dotnet add package Kuestenlogik.Surgewave.Client
dotnet add package Kuestenlogik.Surgewave.Schema.Registry.Client
```

`SchemaConsumer/Program.cs`:

```csharp
using Kuestenlogik.Surgewave.Client;
using Kuestenlogik.Surgewave.Schema.Registry.Client;

public record User(int Id, string Name, string Email);

await using var consumer = new SurgewaveConsumer<string, User>(options =>
{
    options.BootstrapServers = "localhost:9092";
    options.GroupId = "schema-consumer";
    options.AutoOffsetReset = AutoOffsetReset.Earliest;
    options.AsyncValueDeserializer = new SchemaRegistryJsonDeserializer<User>(
        new JsonSchemaSerializerConfig
        {
            SchemaRegistryUrl = "https://localhost:9093"
        });
});

consumer.Subscribe("users");

Console.WriteLine("Consuming users...");

using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

try
{
    while (!cts.Token.IsCancellationRequested)
    {
        var result = await consumer.ConsumeAsync(cts.Token);
        if (result != null)
        {
            Console.WriteLine(
                $"User #{result.Value.Id}: {result.Value.Name} ({result.Value.Email})");
        }
    }
}
catch (OperationCanceledException)
{
    Console.WriteLine("Done consuming.");
}
```

## Step 5: Evolve the Schema

Add a new optional field (`age`) to the schema. This is backward-compatible because existing consumers can ignore the new field.

### Check Compatibility First

```bash
surgewave schema compatibility check users-value --schema '{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "properties": {
    "id": { "type": "integer" },
    "name": { "type": "string" },
    "email": { "type": "string", "format": "email" },
    "age": { "type": "integer", "minimum": 0 }
  },
  "required": ["id", "name"]
}'
```

Expected output:

```
Compatible: true
```

### Register the New Version

```bash
surgewave schema register users-value --schema '{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "properties": {
    "id": { "type": "integer" },
    "name": { "type": "string" },
    "email": { "type": "string", "format": "email" },
    "age": { "type": "integer", "minimum": 0 }
  },
  "required": ["id", "name"]
}'
```

Expected output:

```
Schema registered: subject=users-value, id=2, version=2
```

### View Schema Versions

```bash
surgewave schema describe users-value
```

```
Subject: users-value
  Version 1: Schema ID 1 (JSON)
  Version 2: Schema ID 2 (JSON)
Compatibility: BACKWARD
```

## Step 6: Avro Schemas

Surgewave also supports Avro schemas for more compact serialization:

```bash
surgewave schema register orders-value --type AVRO --schema '{
  "type": "record",
  "name": "Order",
  "namespace": "com.example",
  "fields": [
    {"name": "id", "type": "long"},
    {"name": "customerId", "type": "string"},
    {"name": "amount", "type": "double"},
    {"name": "status", "type": {
      "type": "enum",
      "name": "Status",
      "symbols": ["PENDING", "COMPLETED", "CANCELLED"]
    }}
  ]
}'
```

Use with the Avro serializer:

```csharp
using Kuestenlogik.Surgewave.Schema.Registry.Serdes.Avro;

await using var producer = new SurgewaveProducer<string, Order>(options =>
{
    options.BootstrapServers = "localhost:9092";
    options.AsyncValueSerializer = new SchemaRegistryAvroSerializer<Order>(
        new AvroSerializerConfig
        {
            SchemaRegistryUrl = "https://localhost:9093"
        });
});
```

## Compatibility Levels Reference

| Level | Can Add Fields? | Can Remove Fields? | Use Case |
|-------|:-:|:-:|----------|
| **BACKWARD** | Optional only | Yes | New consumers read old data |
| **FORWARD** | Yes | Optional only | Old consumers read new data |
| **FULL** | Optional only | Optional only | Both directions |
| **NONE** | Any change | Any change | Development only |

Set the compatibility level per subject:

```bash
surgewave schema compatibility set FULL --subject users-value
```

Or set the global default:

```bash
surgewave schema compatibility set BACKWARD
```

## Managing Schemas

```bash
# List all subjects
surgewave schema list

# Get a specific version
surgewave schema get --subject users-value --version 1

# Get the latest version
surgewave schema get --subject users-value --version latest

# Delete a subject (soft delete)
surgewave schema delete-subject users-value

# Delete a specific version
surgewave schema delete-version users-value 1
```

## Next Steps

- [Tutorial 07: Clustering](07-clustering.md) -- set up a multi-broker cluster
- [Schema Registry Reference](../features/schema-registry.md) -- full API and configuration
- [Client Serializers](../clients/dotnet.md#serializers) -- built-in serializer reference
