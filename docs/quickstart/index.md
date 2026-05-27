# 5-Minute Quickstart

Get Surgewave running and send your first messages in under 5 minutes.

## Option 1: Docker (Recommended)

The fastest way to get started:

```bash
# Start Surgewave broker
docker run -d --name surgewave -p 9092:9092 -p 9093:9093 kuestenlogik/surgewave

# Verify it's running
docker logs surgewave
```

You should see:
```
Surgewave broker started on 0.0.0.0:9092
gRPC server started on 0.0.0.0:9093
```

## Option 2: Download Binary

Download the latest release from [GitHub Releases](https://github.com/Kuestenlogik/Surgewave/releases):

```bash
# Windows
surgewave-broker.exe

# Linux/macOS
./surgewave-broker
```

## Option 3: .NET Tool

```bash
dotnet tool install -g Kuestenlogik.Surgewave.Cli
surgewave broker start
```

---

## Send Your First Messages

### Using the CLI

```bash
# Create a topic
surgewave topics create my-first-topic --partitions 3

# Produce messages
surgewave produce my-first-topic --value "Hello, Surgewave!"
surgewave produce my-first-topic --key "user-1" --value "Welcome to Surgewave"

# Consume messages
surgewave consume my-first-topic --offset earliest --max-messages 10
```

Output:
```
Offset: 0  Key: (null)   Value: Hello, Surgewave!
Offset: 1  Key: user-1   Value: Welcome to Surgewave
```

### Using Kafka Clients

Surgewave is 100% compatible with Apache Kafka clients. No code changes required:

```csharp
// C# with Confluent.Kafka
var config = new ProducerConfig
{
    BootstrapServers = "localhost:9092"  // Point to Surgewave
};

using var producer = new ProducerBuilder<string, string>(config).Build();
await producer.ProduceAsync("my-topic", new Message<string, string>
{
    Key = "key",
    Value = "Hello from Kafka client!"
});
```

```python
# Python with kafka-python
from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers='localhost:9092')
producer.send('my-topic', b'Hello from Python!')
```

```java
// Java with kafka-clients
Properties props = new Properties();
props.put("bootstrap.servers", "localhost:9092");
KafkaProducer<String, String> producer = new KafkaProducer<>(props);
producer.send(new ProducerRecord<>("my-topic", "key", "Hello from Java!"));
```

---

## Quick CLI Reference

| Command | Description |
|---------|-------------|
| `surgewave topics list` | List all topics |
| `surgewave topics create <name>` | Create a topic |
| `surgewave produce <topic> -m "msg"` | Produce a message |
| `surgewave consume <topic>` | Consume messages |
| `surgewave groups list` | List consumer groups |
| `surgewave health` | Check broker health |

---

## Next Steps

- [First Messages Tutorial](first-messages.md) - Detailed produce/consume examples
- [Installation Guide](../setup/index.md) - All deployment options
- [Configuration](../setup/configuration.md) - Configure Surgewave for your needs
- [Client APIs](../clients/index.md) - Using Surgewave from your application
