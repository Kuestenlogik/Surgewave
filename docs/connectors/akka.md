# Akka.NET Connector

The Akka.NET connector enables bidirectional data flow between Surgewave/Kafka topics and Akka.NET actor systems, clusters, and streams.

## Features

- **Actor Mode**: Direct actor message passing (Tell/Ask patterns)
- **Cluster Mode**: Distributed pub/sub via Akka Cluster
- **Streams Mode**: Reactive streams with backpressure support
- Multiple operation modes (actor, cluster, streams)
- Configurable buffer sizes and overflow strategies
- Retry logic with exponential backoff
- HOCON configuration support

## Installation

Add the connector package to your project:

```xml
<PackageReference Include="Kuestenlogik.Surgewave.Connect.Akka" />
```

## Modes

### Actor Mode (Default)

Basic actor message passing:
- **Source**: Creates a receiver actor that queues incoming messages
- **Sink**: Sends messages to a target actor via Tell or Ask

### Cluster Mode

Distributed pub/sub via Akka Cluster:
- **Source**: Subscribes to cluster topics via DistributedPubSub
- **Sink**: Publishes to cluster topics via DistributedPubSub

### Streams Mode

Akka Streams with backpressure:
- **Source**: Actor-backed source that queues messages
- **Sink**: Streams-based processing with configurable parallelism

## Configuration

### Common Configuration

| Property | Type | Required | Default | Description |
|----------|------|----------|---------|-------------|
| `akka.system.name` | String | No | `surgewave-connect` | Actor system name |
| `akka.system.config` | String | No | | HOCON configuration |
| `akka.actor.path` | String | Yes* | `/user/surgewave-receiver` | Actor path |
| `akka.remote.address` | String | No | | Remote actor address |
| `akka.mode` | String | No | `actor` | Mode: actor, cluster, streams |

### Source Configuration

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `akka.topic.pattern` | String | `akka.${path}` | Topic naming pattern |
| `akka.poll.timeout.ms` | Int | `1000` | Poll timeout in milliseconds |
| `akka.max.messages.per.poll` | Int | `100` | Max messages per poll |
| `akka.include.metadata` | Boolean | `true` | Include metadata in output |
| `akka.message.type` | String | | Filter by message type name |

### Sink Configuration

| Property | Type | Required | Default | Description |
|----------|------|----------|---------|-------------|
| `topics` | String | Yes | | Surgewave topics to consume |
| `akka.ask.timeout.ms` | Int | | `5000` | Ask timeout in milliseconds |
| `akka.tell.only` | Boolean | | `true` | Use Tell instead of Ask |
| `akka.batch.size` | Int | | `32` | Batch size for processing |
| `akka.max.retry.count` | Int | | `3` | Max retry attempts |
| `akka.retry.delay.ms` | Int | | `1000` | Retry delay in milliseconds |

### Cluster Configuration

| Property | Type | Required | Description |
|----------|------|----------|-------------|
| `akka.cluster.seed.nodes` | String | Yes | Comma-separated seed nodes |
| `akka.cluster.publish.topic` | String | Yes* | Topic to publish to |
| `akka.cluster.subscribe.topic` | String | Yes* | Topic to subscribe to |
| `akka.cluster.receptionist.path` | String | | Cluster receptionist path |

### Streams Configuration

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `akka.stream.buffer.size` | Int | `1024` | Stream buffer size |
| `akka.stream.overflow.strategy` | String | `backpressure` | Overflow strategy |
| `akka.stream.parallelism` | Int | `4` | Processing parallelism |

**Overflow Strategies**: `dropHead`, `dropTail`, `dropBuffer`, `backpressure`, `fail`

## Usage Examples

### Actor Mode Source

```csharp
var config = new Dictionary<string, string>
{
    ["akka.system.name"] = "my-system",
    ["akka.actor.path"] = "/user/surgewave-receiver",
    ["akka.topic.pattern"] = "akka.messages",
    ["akka.max.messages.per.poll"] = "100"
};

var connector = new AkkaSourceConnector();
connector.Start(config);
```

### Actor Mode Sink

```csharp
var config = new Dictionary<string, string>
{
    ["akka.system.name"] = "my-system",
    ["akka.actor.path"] = "/user/processor",
    ["topics"] = "orders,payments",
    ["akka.tell.only"] = "true"
};

var connector = new AkkaSinkConnector();
connector.Start(config);
```

### Cluster Mode Source

```csharp
var config = new Dictionary<string, string>
{
    ["akka.mode"] = "cluster",
    ["akka.system.name"] = "my-cluster",
    ["akka.cluster.seed.nodes"] = "akka.tcp://my-cluster@node1:2552,akka.tcp://my-cluster@node2:2552",
    ["akka.cluster.subscribe.topic"] = "events"
};

var task = new AkkaClusterSourceTask();
task.Start(config);
```

### Cluster Mode Sink

```csharp
var config = new Dictionary<string, string>
{
    ["akka.mode"] = "cluster",
    ["akka.system.name"] = "my-cluster",
    ["akka.cluster.seed.nodes"] = "akka.tcp://my-cluster@node1:2552,akka.tcp://my-cluster@node2:2552",
    ["akka.cluster.publish.topic"] = "events"
};

var task = new AkkaClusterSinkTask();
task.Start(config);
```

### Streams Mode Sink with Backpressure

```csharp
var config = new Dictionary<string, string>
{
    ["akka.mode"] = "streams",
    ["akka.actor.path"] = "/user/stream-sink",
    ["akka.stream.buffer.size"] = "2048",
    ["akka.stream.overflow.strategy"] = "backpressure",
    ["akka.stream.parallelism"] = "8"
};

var task = new AkkaStreamsSinkTask();
task.Start(config);
```

### Custom HOCON Configuration

```csharp
var hocon = @"
akka {
    loglevel = INFO
    actor {
        provider = cluster
    }
    remote {
        dot-netty.tcp {
            hostname = ""127.0.0.1""
            port = 2552
        }
    }
}
";

var config = new Dictionary<string, string>
{
    ["akka.system.config"] = hocon,
    ["akka.actor.path"] = "/user/processor"
};
```

## Message Format

### Source Record Structure

```json
{
  "source": {
    "actor_path": "/user/surgewave-receiver",
    "sender_path": "akka://system/user/sender",
    "message_type": "OrderMessage",
    "timestamp": "2024-01-15T10:30:00.0000000Z"
  },
  "data": { /* original message content */ },
  "ts_ms": 1705315800000
}
```

### SurgewaveMessage (Sink Output)

Messages sent to actors are wrapped in `SurgewaveMessage`:

```csharp
public class SurgewaveMessage
{
    public string Topic { get; init; }
    public int Partition { get; init; }
    public long Offset { get; init; }
    public DateTimeOffset Timestamp { get; init; }
    public string? Key { get; init; }
    public object Data { get; init; }
    public Dictionary<string, string>? Headers { get; init; }
}
```

### Headers

| Header | Description |
|--------|-------------|
| `akka.actor.path` | Receiver actor path |
| `akka.sender.path` | Sender actor path |
| `akka.message.type` | Message type name |
| `akka.timestamp` | Message timestamp |
| `akka.cluster.topic` | Cluster pub/sub topic (cluster mode) |

## Integration Patterns

### Sending to Actor

```csharp
// The source task's receiver actor can be accessed
var task = new AkkaSourceTask();
task.Start(config);

// Send messages to the receiver
task.ReceiverActor.Tell(new MyMessage { Data = "test" });
```

### Custom Streams Processing

```csharp
// Access the materializer for custom stream processing
var task = new AkkaStreamsSourceTask();
task.Start(config);

// Use the materializer
var source = Source.From(Enumerable.Range(1, 100))
    .Select(x => new MyMessage { Value = x });

source.RunWith(Sink.ActorRef<MyMessage>(task.SourceActor, "complete"), task.Materializer);
```

## Error Handling

- **Actor Mode**: Messages use Tell (fire-and-forget) by default; Ask with timeout for request-response
- **Cluster Mode**: Retries on publish failures with configurable backoff
- **Streams Mode**: Configurable overflow strategies for backpressure handling

## Performance Considerations

- **Buffer Size**: Larger buffers improve throughput but increase memory usage
- **Parallelism**: Higher parallelism improves throughput for CPU-bound processing
- **Tell vs Ask**: Tell is faster (no response waiting); use Ask when confirmation is needed
- **Batch Size**: Larger batches reduce overhead but increase latency
