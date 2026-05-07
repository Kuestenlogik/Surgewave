# RabbitMQ Connector

The RabbitMQ Connector provides integration between Surgewave and RabbitMQ message broker. It supports both source (consuming from queues) and sink (publishing to exchanges) operations.

## Overview

RabbitMQ is a widely-used open source message broker that supports multiple messaging protocols. The Surgewave RabbitMQ Connector enables:

- **Source**: Consume messages from RabbitMQ queues and produce Surgewave records
- **Sink**: Publish Surgewave records to RabbitMQ exchanges with routing key templating

## Features

- Queue consumption with prefetch control
- Exchange publishing with multiple exchange types (direct, fanout, topic, headers)
- Routing key templating with placeholders
- Message persistence and delivery modes
- TLS/SSL encryption
- Username/password authentication
- Dead letter exchange (DLX) support
- Message TTL configuration
- Manual and auto-acknowledgment modes

## Installation

The RabbitMQ connector is included in the `Kuestenlogik.Surgewave.Connect.RabbitMQ` package.

## Configuration

### Common Configuration

| Property | Description | Default |
|----------|-------------|---------|
| `rabbitmq.host` | RabbitMQ host | `localhost` |
| `rabbitmq.port` | RabbitMQ port | `5672` |
| `rabbitmq.virtual.host` | Virtual host | `/` |
| `rabbitmq.username` | Username | `guest` |
| `rabbitmq.password` | Password | `guest` |
| `rabbitmq.connection.name` | Connection name for identification | (none) |

### TLS Configuration

| Property | Description | Default |
|----------|-------------|---------|
| `rabbitmq.tls.enabled` | Enable TLS encryption | `false` |
| `rabbitmq.tls.cert.file` | Path to client certificate file | (none) |
| `rabbitmq.tls.key.file` | Path to client private key file | (none) |
| `rabbitmq.tls.ca.file` | Path to CA certificate file | (none) |

### Source Connector Configuration

| Property | Description | Default |
|----------|-------------|---------|
| `rabbitmq.queue` | Queue to consume from (required) | - |
| `topic` | Target Surgewave topic (required) | - |
| `rabbitmq.queue.durable` | Whether queue survives broker restart | `true` |
| `rabbitmq.queue.exclusive` | Whether queue is exclusive to connection | `false` |
| `rabbitmq.queue.auto.delete` | Delete queue when last consumer disconnects | `false` |
| `rabbitmq.prefetch.count` | Number of messages to prefetch | `100` |
| `rabbitmq.auto.ack` | Auto-acknowledge messages | `false` |
| `rabbitmq.batch.size` | Maximum records per poll | `100` |
| `rabbitmq.poll.timeout.ms` | Poll timeout in milliseconds | `1000` |
| `rabbitmq.dead.letter.exchange` | Dead letter exchange name | (none) |
| `rabbitmq.dead.letter.routing.key` | Dead letter routing key | (none) |

### Sink Connector Configuration

| Property | Description | Default |
|----------|-------------|---------|
| `topics` | Surgewave topics to consume (required) | - |
| `rabbitmq.exchange` | Target exchange (empty for default) | `""` |
| `rabbitmq.exchange.type` | Exchange type | `direct` |
| `rabbitmq.exchange.durable` | Whether exchange survives broker restart | `true` |
| `rabbitmq.exchange.auto.delete` | Delete exchange when no longer used | `false` |
| `rabbitmq.routing.key.template` | Routing key template | `${topic}` |
| `rabbitmq.persistent` | Make messages persistent | `true` |
| `rabbitmq.mandatory` | Return unroutable messages | `false` |
| `rabbitmq.content.type` | Default content type | `application/octet-stream` |
| `rabbitmq.message.ttl.ms` | Message TTL in milliseconds (0 = no expiry) | `0` |

## Routing Key Templates

The sink connector supports routing key templating with the following placeholders:

- `${topic}` - The Surgewave topic name
- `${key}` - The record key (as string)
- `${partition}` - The partition number

### Examples

```
${topic}                    -> orders
orders.${key}               -> orders.order-123
${topic}.p${partition}      -> orders.p0
events.${topic}.${key}      -> events.orders.order-123
```

## Exchange Types

RabbitMQ supports four exchange types:

- **direct**: Route by exact routing key match
- **fanout**: Broadcast to all bound queues
- **topic**: Route by routing key pattern (wildcards: `*` single word, `#` zero or more words)
- **headers**: Route by message header attributes

## Usage Examples

### Source Connector: Consume from Queue

```csharp
var config = new Dictionary<string, string>
{
    ["rabbitmq.host"] = "localhost",
    ["rabbitmq.queue"] = "orders",
    ["topic"] = "surgewave-orders",
    ["rabbitmq.prefetch.count"] = "200"
};

var connector = new RabbitMQSourceConnector();
connector.Start(config);
```

### Sink Connector: Publish to Exchange

```csharp
var config = new Dictionary<string, string>
{
    ["rabbitmq.host"] = "localhost",
    ["topics"] = "processed-orders",
    ["rabbitmq.exchange"] = "events",
    ["rabbitmq.exchange.type"] = "topic",
    ["rabbitmq.routing.key.template"] = "events.${topic}.${key}"
};

var connector = new RabbitMQSinkConnector();
connector.Start(config);
```

### With Authentication

```csharp
var config = new Dictionary<string, string>
{
    ["rabbitmq.host"] = "rabbitmq.example.com",
    ["rabbitmq.queue"] = "secure-queue",
    ["topic"] = "secure-events",
    ["rabbitmq.username"] = "myuser",
    ["rabbitmq.password"] = "mypassword",
    ["rabbitmq.virtual.host"] = "/myapp"
};
```

### With TLS

```csharp
var config = new Dictionary<string, string>
{
    ["rabbitmq.host"] = "rabbitmq.example.com",
    ["rabbitmq.queue"] = "encrypted-queue",
    ["topic"] = "encrypted-events",
    ["rabbitmq.tls.enabled"] = "true",
    ["rabbitmq.tls.cert.file"] = "/path/to/client.crt",
    ["rabbitmq.tls.key.file"] = "/path/to/client.key"
};
```

### With Dead Letter Exchange

```csharp
var config = new Dictionary<string, string>
{
    ["rabbitmq.host"] = "localhost",
    ["rabbitmq.queue"] = "orders",
    ["topic"] = "surgewave-orders",
    ["rabbitmq.dead.letter.exchange"] = "dlx",
    ["rabbitmq.dead.letter.routing.key"] = "failed.orders"
};
```

## Header Mapping

### Source (RabbitMQ to Surgewave)

RabbitMQ message properties and headers are mapped to Surgewave record headers:

| RabbitMQ Property | Surgewave Header |
|-------------------|--------------|
| Exchange | `rabbitmq.exchange` |
| Routing Key | `rabbitmq.routing.key` |
| Delivery Tag | `rabbitmq.delivery.tag` |
| Redelivered | `rabbitmq.redelivered` |
| Content-Type | `rabbitmq.content.type` |
| Content-Encoding | `rabbitmq.content.encoding` |
| Correlation-ID | `rabbitmq.correlation.id` |
| Message-ID | `rabbitmq.message.id` |
| Reply-To | `rabbitmq.reply.to` |
| Custom Headers | `rabbitmq.header.{name}` |

### Sink (Surgewave to RabbitMQ)

Surgewave record headers are copied to RabbitMQ message headers (excluding `rabbitmq.*` prefixed headers). Additionally, metadata headers are added:

| Surgewave Metadata | RabbitMQ Header |
|----------------|-----------------|
| Topic | `surgewave.topic` |
| Partition | `surgewave.partition` |
| Offset | `surgewave.offset` |

## Acknowledgment Modes

### Auto-Acknowledgment (`rabbitmq.auto.ack=true`)

Messages are acknowledged immediately upon receipt. Use for high-throughput scenarios where message loss is acceptable.

### Manual Acknowledgment (default)

Messages are acknowledged after successful processing via `CommitAsync()`. Provides exactly-once semantics when combined with idempotent processing.

## Dead Letter Handling

Configure dead letter exchange (DLX) to capture failed messages:

1. Create DLX and DLQ:
   ```
   rabbitmqadmin declare exchange name=dlx type=direct
   rabbitmqadmin declare queue name=dlq durable=true
   rabbitmqadmin declare binding source=dlx destination=dlq routing_key=failed
   ```

2. Configure connector:
   ```csharp
   config["rabbitmq.dead.letter.exchange"] = "dlx";
   config["rabbitmq.dead.letter.routing.key"] = "failed";
   ```

## Error Handling

- Connection failures during Start() throw `BrokerUnreachableException`
- Authentication failures throw `AuthenticationFailureException`
- Empty record batches in PutAsync are handled gracefully
- Disposal is safe to call multiple times

## Performance Tuning

- **Prefetch**: Increase `rabbitmq.prefetch.count` for higher throughput (balance with memory usage)
- **Batch Size**: Adjust `rabbitmq.batch.size` to control poll batch sizes
- **Persistent**: Set `rabbitmq.persistent=false` for non-durable messages (faster but may lose on broker restart)

## Dependencies

- **RabbitMQ.Client** (v7.1.2) - Official RabbitMQ .NET client
