# Recipe: Produce & Consume Messages

Minimal, copy-paste examples for common produce/consume patterns.

---

## Simplest Producer — Surgewave Native Client

```csharp
await using var producer = new SurgewaveProducer<string, string>(options =>
{
    options.BootstrapServers = "localhost:9092";
});

await producer.ProduceAsync("my-topic", "key1", "hello world");
```

## Simplest Consumer — Surgewave Native Client

```csharp
await using var consumer = new SurgewaveConsumer<string, string>(options =>
{
    options.BootstrapServers = "localhost:9092";
    options.GroupId = "my-group";
    options.AutoOffsetReset = AutoOffsetReset.Earliest;
});

consumer.Subscribe("my-topic");

while (true)
{
    var record = await consumer.ConsumeAsync(TimeSpan.FromSeconds(5));
    if (record is null) continue;

    Console.WriteLine($"[{record.Key}] {record.Value}");
    await consumer.CommitAsync(record);
}
```

---

## Confluent.Kafka Client (Drop-in Compatible)

Surgewave speaks the Kafka wire protocol. Point any Confluent.Kafka client at Surgewave's port.

### Producer

```csharp
using Confluent.Kafka;

var config = new ProducerConfig { BootstrapServers = "localhost:9092" };

using var producer = new ProducerBuilder<string, string>(config).Build();

var result = await producer.ProduceAsync("my-topic", new Message<string, string>
{
    Key = "key1",
    Value = "hello from confluent client"
});

Console.WriteLine($"Delivered to {result.TopicPartitionOffset}");
producer.Flush();
```

### Consumer

```csharp
using Confluent.Kafka;

var config = new ConsumerConfig
{
    BootstrapServers = "localhost:9092",
    GroupId = "my-group",
    AutoOffsetReset = AutoOffsetReset.Earliest,
    EnableAutoCommit = false
};

using var consumer = new ConsumerBuilder<string, string>(config).Build();
consumer.Subscribe("my-topic");

while (true)
{
    var result = consumer.Consume(TimeSpan.FromSeconds(5));
    if (result is null) continue;

    Console.WriteLine($"[{result.Key}] {result.Value}");
    consumer.Commit(result);
}
```

---

## Headers and Keys

```csharp
await using var producer = new SurgewaveProducer<string, Order>(options =>
{
    options.BootstrapServers = "localhost:9092";
});

var headers = new MessageHeaders
{
    { "source", "payment-service" },
    { "correlation-id", Guid.NewGuid().ToString() }
};

await producer.ProduceAsync("orders", new ProduceRecord<string, Order>
{
    Key = order.Id.ToString(),
    Value = order,
    Headers = headers,
    Timestamp = DateTimeOffset.UtcNow
});
```

---

## Batching for Throughput

```csharp
await using var producer = new SurgewaveProducer<string, Order>(options =>
{
    options.BootstrapServers = "localhost:9092";
    options.BatchSize = 1000;      // messages per batch
    options.LingerMs = 10;         // wait up to 10ms to fill batch
});

// Fire-and-forget approach — acknowledgement via callback
var tasks = orders.Select(o =>
    producer.ProduceAsync("orders", o.Id.ToString(), o));

await Task.WhenAll(tasks);
```

---

## Error Handling

```csharp
await using var producer = new SurgewaveProducer<string, string>(options =>
{
    options.BootstrapServers = "localhost:9092";
    options.RequestTimeoutMs = 10_000;
});

try
{
    await producer.ProduceAsync("orders", "key", "value");
}
catch (ProduceException ex)
{
    Console.Error.WriteLine($"Produce failed: {ex.Error.Code} - {ex.Message}");
    // Retry, DLQ, or alert based on ex.Error.Code
}

// Consumer with error handling
await using var consumer = new SurgewaveConsumer<string, string>(options =>
{
    options.BootstrapServers = "localhost:9092";
    options.GroupId = "my-group";
});

consumer.Subscribe("my-topic");

try
{
    while (!cts.IsCancellationRequested)
    {
        var record = await consumer.ConsumeAsync(cts.Token);
        if (record is null) continue;

        try
        {
            await ProcessAsync(record.Value);
            await consumer.CommitAsync(record);
        }
        catch (Exception ex)
        {
            // Do not commit — message will be redelivered
            logger.LogError(ex, "Processing failed for offset {Offset}", record.Offset);
        }
    }
}
catch (OperationCanceledException)
{
    // Clean shutdown
}
```

---

## See Also

- [Producer API](../clients/producer.md)
- [Consumer API](../clients/consumer.md)
- [Kafka Compatibility](../clients/kafka-compat.md)
