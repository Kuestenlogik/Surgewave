# Kafka Compatibility

Surgewave is wire-compatible with Apache Kafka clients. This page shows
per-language client snippets; the detailed conformance status — per-RPC
coverage, KIP table, Schema Registry compatibility, and the cross-client
test matrix — lives under [Conformance](../conformance/).

## Confluent.Kafka (.NET)

### Installation

```bash
dotnet add package Confluent.Kafka
```

### Producer

```csharp
using Confluent.Kafka;

var config = new ProducerConfig
{
    BootstrapServers = "localhost:9092",  // Point to Surgewave
    Acks = Acks.All,
    EnableIdempotence = true
};

using var producer = new ProducerBuilder<string, string>(config).Build();

var result = await producer.ProduceAsync("my-topic", new Message<string, string>
{
    Key = "key",
    Value = "value"
});

Console.WriteLine($"Delivered to {result.Partition}@{result.Offset}");
```

### Consumer

```csharp
var config = new ConsumerConfig
{
    BootstrapServers = "localhost:9092",
    GroupId = "my-consumer-group",
    AutoOffsetReset = AutoOffsetReset.Earliest,
    EnableAutoCommit = true
};

using var consumer = new ConsumerBuilder<string, string>(config).Build();
consumer.Subscribe("my-topic");

while (true)
{
    var result = consumer.Consume(TimeSpan.FromSeconds(1));
    if (result != null)
    {
        Console.WriteLine($"{result.Message.Key}: {result.Message.Value}");
    }
}
```

### Admin Client

```csharp
using var admin = new AdminClientBuilder(new AdminClientConfig
{
    BootstrapServers = "localhost:9092"
}).Build();

// Create topic
await admin.CreateTopicsAsync(new[]
{
    new TopicSpecification
    {
        Name = "new-topic",
        NumPartitions = 3,
        ReplicationFactor = 1
    }
});

// List topics
var metadata = admin.GetMetadata(TimeSpan.FromSeconds(10));
foreach (var topic in metadata.Topics)
{
    Console.WriteLine(topic.Topic);
}
```

## librdkafka Configuration

Surgewave supports all librdkafka configuration:

| Setting | Description | Supported |
|---------|-------------|-----------|
| `bootstrap.servers` | Broker addresses | Yes |
| `client.id` | Client identifier | Yes |
| `acks` | Acknowledgment level | Yes |
| `compression.type` | Compression codec | Yes |
| `batch.size` | Batch size | Yes |
| `linger.ms` | Linger time | Yes |
| `enable.idempotence` | Idempotent producer | Yes |
| `transactional.id` | Transaction ID | Yes |

## Other Kafka Clients

### kafka-python

```python
from kafka import KafkaProducer, KafkaConsumer

producer = KafkaProducer(bootstrap_servers='localhost:9092')
producer.send('my-topic', b'message')

consumer = KafkaConsumer('my-topic', bootstrap_servers='localhost:9092')
for message in consumer:
    print(message.value)
```

### kafka-go

```go
conn, _ := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", "my-topic", 0)
conn.WriteMessages(kafka.Message{Value: []byte("message")})
```

### Java

```java
Properties props = new Properties();
props.put("bootstrap.servers", "localhost:9092");
props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

KafkaProducer<String, String> producer = new KafkaProducer<>(props);
producer.send(new ProducerRecord<>("my-topic", "key", "value"));
```

## Protocol Version Compatibility

Surgewave supports Kafka 4.0 protocol versions:

| API | Version Range |
|-----|---------------|
| Produce | 3-13 |
| Fetch | 4-18 |
| Metadata | 0-13 |
| OffsetCommit | 2-10 |

## Migration from Kafka

1. **Update bootstrap.servers** - Point to Surgewave instead of Kafka
2. **No code changes** - Same client code works
3. **Verify features** - Test your specific usage patterns

```csharp
// Before (Kafka)
BootstrapServers = "kafka1:9092,kafka2:9092"

// After (Surgewave)
BootstrapServers = "surgewave:9092"
```

## Performance Comparison

When clients connect via the Kafka wire, Surgewave operates on the same wire-protocol budget as
upstream Kafka — librdkafka and Confluent.Kafka cannot tell the brokers apart. Switching the same
client to the Surgewave native protocol (where supported) reduces transport overhead substantially;
the head-to-head numbers will be published alongside the 1.0 release.

## Next Steps

- [.NET Client](dotnet.md) - Higher performance native client
- [Producer API](producer.md) - Producer patterns
- [Consumer API](consumer.md) - Consumer patterns
