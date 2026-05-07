# First Messages Tutorial

This tutorial covers producing and consuming messages with Surgewave in detail.

## Producing Messages

### Single Message

```bash
# Simple message
surgewave produce my-topic --value "Hello, World!"

# With key
surgewave produce my-topic --key "order-123" --value '{"status": "created"}'

# To specific partition
surgewave produce my-topic --partition 0 --value "Partition 0 only"
```

### Piped Input

```bash
# From echo
echo "Message from pipe" | surgewave produce my-topic

# From file
cat messages.txt | surgewave produce my-topic

# With key parsing (key:value format)
echo "user-1:logged in" | surgewave produce my-topic --parse-key
```

### Interactive Mode

```bash
surgewave produce my-topic --interactive
```

Type messages line by line, press `Ctrl+D` to exit.

### Batch Production

```bash
# From file with multiple lines
surgewave produce my-topic < bulk-messages.txt
```

---

## Consuming Messages

### Basic Consumption

```bash
# From latest (default)
surgewave consume my-topic

# From beginning
surgewave consume my-topic --offset earliest

# From specific offset
surgewave consume my-topic --offset 100

# Limit messages
surgewave consume my-topic --max-messages 50
```

### Output Formats

```bash
# Table format (default)
surgewave consume my-topic -f table

# JSON format
surgewave consume my-topic -f json

# Plain text (for piping)
surgewave consume my-topic -f plain | jq '.value'
```

### With Metadata

```bash
# Show timestamps
surgewave consume my-topic --timestamps

# Show partition and offset
surgewave consume my-topic --print-offset
```

---

## Programmatic Access

### .NET Native Client

```csharp
using Kuestenlogik.Surgewave.Client;
using Kuestenlogik.Surgewave.Client.Native;

// Create client
await using var client = new SurgewaveNativeClient("localhost", 9092);
await client.ConnectAsync();

// Produce
await client.Messaging.Send("my-topic")
    .WithKey("order-123")
    .WithValue("Order created")
    .ExecuteAsync();

// Consume using typed consumer
await using var consumer = new SurgewaveConsumer<string, string>(options =>
{
    options.BootstrapServers = "localhost:9092";
});
consumer.Subscribe("my-topic");
var result = await consumer.ConsumeAsync();
if (result != null)
    Console.WriteLine($"Key: {result.Key}, Value: {result.Value}");
```

### Typed Producer/Consumer

```csharp
using Kuestenlogik.Surgewave.Client;

// Typed producer (complex types auto-serialize to JSON)
await using var producer = new SurgewaveProducer<string, Order>(options =>
{
    options.BootstrapServers = "localhost:9092";
});

await producer.ProduceAsync("orders", "order-123", new Order { Id = 123, Status = "new" });

// Typed consumer
await using var consumer = new SurgewaveConsumer<string, Order>(options =>
{
    options.BootstrapServers = "localhost:9092";
    options.GroupId = "order-processor";
});

consumer.Subscribe("orders");
while (true)
{
    var record = await consumer.ConsumeAsync();
    if (record != null)
        Console.WriteLine($"Order {record.Value.Id}: {record.Value.Status}");
}
```

### Confluent.Kafka (100% Compatible)

```csharp
using Confluent.Kafka;

// Producer
var producerConfig = new ProducerConfig { BootstrapServers = "localhost:9092" };
using var producer = new ProducerBuilder<string, string>(producerConfig).Build();
await producer.ProduceAsync("my-topic", new Message<string, string>
{
    Key = "key",
    Value = "value"
});

// Consumer
var consumerConfig = new ConsumerConfig
{
    BootstrapServers = "localhost:9092",
    GroupId = "my-group",
    AutoOffsetReset = AutoOffsetReset.Earliest
};
using var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
consumer.Subscribe("my-topic");
while (true)
{
    var result = consumer.Consume();
    Console.WriteLine($"{result.Message.Key}: {result.Message.Value}");
}
```

---

## Message Headers

### CLI

```bash
# Currently headers are set programmatically
```

### Programmatic

```csharp
await client.Messaging.Send("my-topic")
    .WithKey("order-123")
    .WithValue(orderData)
    .WithHeader("correlation-id", correlationId)
    .WithHeader("source", "order-service")
    .ExecuteAsync();
```

---

## Best Practices

1. **Use Keys for Ordering** - Messages with the same key go to the same partition
2. **Batch for Throughput** - Use piped input or batch APIs for high volume
3. **Set Appropriate Timeouts** - Use `--timeout` for slow networks
4. **Use JSON Format for Piping** - Easier to parse with `jq`

## Next Steps

- [Setup Guide](../setup/index.md) - Configure Surgewave for your environment
- [Client APIs](../clients/index.md) - Full API documentation
- [Performance Tuning](../performance/tuning.md) - Optimize for your workload
