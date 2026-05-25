# Recipe: Schema Registry

Register, validate, and evolve schemas. Surgewave's Schema Registry is Confluent-compatible.

---

## Enable Schema Registry

`appsettings.json`:

```json
{
  "Surgewave": {
    "SchemaRegistry": {
      "Enabled": true,
      "DefaultCompatibility": "Backward"
    }
  }
}
```

Schema Registry REST API is served on the same port as the broker HTTP API (default 9093).

---

## Register an Avro Schema

```bash
curl -X POST https://localhost:9093/subjects/orders-value/versions \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{
    "schema": "{\"type\":\"record\",\"name\":\"Order\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"amount\",\"type\":\"double\"},{\"name\":\"status\",\"type\":\"string\"}]}"
  }'
# Response: {"id": 1}
```

## Register a JSON Schema

```bash
curl -X POST https://localhost:9093/subjects/events-value/versions \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{
    "schemaType": "JSON",
    "schema": "{\"$schema\":\"http://json-schema.org/draft-07/schema\",\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"timestamp\":{\"type\":\"string\",\"format\":\"date-time\"}}}"
  }'
```

## Register a Protobuf Schema

```bash
curl -X POST https://localhost:9093/subjects/users-value/versions \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{
    "schemaType": "PROTOBUF",
    "schema": "syntax = \"proto3\"; message User { string id = 1; string name = 2; string email = 3; }"
  }'
```

---

## Produce with Schema Validation

```csharp
// Install: Kuestenlogik.Surgewave.Serialization.Avro
await using var producer = new SurgewaveProducer<string, Order>(options =>
{
    options.BootstrapServers = "localhost:9092";
    options.SchemaRegistryUrl = "https://localhost:9093";
    options.ValueSerializer = new AvroSerializer<Order>();
});

await producer.ProduceAsync("orders", "ord-1", new Order
{
    Id = "ord-1",
    Amount = 99.99,
    Status = "pending"
});
```

---

## Consume with Deserialization

```csharp
await using var consumer = new SurgewaveConsumer<string, Order>(options =>
{
    options.BootstrapServers = "localhost:9092";
    options.GroupId = "order-processor";
    options.SchemaRegistryUrl = "https://localhost:9093";
    options.ValueDeserializer = new AvroDeserializer<Order>();
});

consumer.Subscribe("orders");

while (true)
{
    var record = await consumer.ConsumeAsync(TimeSpan.FromSeconds(5));
    if (record is null) continue;

    // record.Value is already a typed Order object
    Console.WriteLine($"Order {record.Value.Id}: {record.Value.Amount}");
    await consumer.CommitAsync(record);
}
```

---

## Check Compatibility Before Registering

```bash
# Check if an evolved schema is backward compatible
curl -X POST \
  https://localhost:9093/compatibility/subjects/orders-value/versions/latest \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{
    "schema": "{\"type\":\"record\",\"name\":\"Order\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"amount\",\"type\":\"double\"},{\"name\":\"status\",\"type\":\"string\"},{\"name\":\"region\",\"type\":[\"null\",\"string\"],\"default\":null}]}"
  }'
# Response: {"is_compatible": true}
```

---

## Schema Evolution — Add a Nullable Field

Backward compatibility: new field must have a default value so old consumers still work.

**v1 schema (already registered):**

```json
{
  "type": "record",
  "name": "Order",
  "fields": [
    {"name": "id",     "type": "string"},
    {"name": "amount", "type": "double"}
  ]
}
```

**v2 schema — add optional `region` field:**

```json
{
  "type": "record",
  "name": "Order",
  "fields": [
    {"name": "id",     "type": "string"},
    {"name": "amount", "type": "double"},
    {"name": "region", "type": ["null", "string"], "default": null}
  ]
}
```

```bash
# Register v2 — will succeed under Backward compatibility
curl -X POST https://localhost:9093/subjects/orders-value/versions \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"schema": "<v2-schema-json>"}'
# Response: {"id": 2}
```

**Rules for safe evolution:**

| Compatibility Mode | Safe to add field | Safe to remove field | Safe to change type |
|--------------------|:-----------------:|:--------------------:|:-------------------:|
| Backward           | Yes (with default)| No                   | No                  |
| Forward            | No                | Yes (with default)   | No                  |
| Full               | Yes (with default)| Yes (with default)   | No                  |

---

## CLI Shortcuts

```bash
surgewave schema list
surgewave schema register orders-value --file order.avsc --type AVRO
surgewave schema get orders-value
surgewave schema delete orders-value
```

---

## See Also

- [Schema Registry Reference](../features/schema-registry.md)
- [Config Reference](../reference/config-reference.md)
