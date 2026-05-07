# NATS Connector

The NATS Connector provides seamless integration between Surgewave and NATS messaging system. It supports both source (subscribing to NATS subjects) and sink (publishing to NATS subjects) operations.

## Overview

NATS is a simple, secure, and high-performance open-source messaging system. The Surgewave NATS Connector enables:

- **Source**: Subscribe to NATS subjects and convert messages to Surgewave records
- **Sink**: Publish Surgewave records to NATS subjects with flexible subject templating

## Features

- Core NATS protocol support
- Wildcard subject subscriptions (`*` and `>`)
- Queue group support for load balancing
- Subject templating with placeholders
- TLS/SSL encryption
- Multiple authentication methods (username/password, token, credentials file)
- Header mapping between NATS and Surgewave

## Installation

The NATS connector is included in the `Kuestenlogik.Surgewave.Connect.Nats` package.

## Configuration

### Common Configuration

| Property | Description | Default |
|----------|-------------|---------|
| `nats.url` | NATS server URL | `nats://localhost:4222` |
| `nats.connection.name` | Connection name for identification | (none) |

### Authentication

| Property | Description |
|----------|-------------|
| `nats.username` | Username for basic authentication |
| `nats.password` | Password for basic authentication |
| `nats.token` | Authentication token |
| `nats.credentials.file` | Path to NATS credentials file (.creds) |

### TLS Configuration

| Property | Description | Default |
|----------|-------------|---------|
| `nats.tls.enabled` | Enable TLS encryption | `false` |
| `nats.tls.cert.file` | Path to client certificate file | (none) |
| `nats.tls.key.file` | Path to client private key file | (none) |
| `nats.tls.ca.file` | Path to CA certificate file | (none) |

### Source Connector Configuration

| Property | Description | Default |
|----------|-------------|---------|
| `nats.subject` | NATS subject to subscribe to (required) | - |
| `topic` | Target Surgewave topic for messages (required) | - |
| `nats.queue.group` | Queue group for load balancing | (none) |
| `nats.batch.size` | Maximum records per poll | `100` |
| `nats.poll.timeout.ms` | Poll timeout in milliseconds | `1000` |

### Sink Connector Configuration

| Property | Description | Default |
|----------|-------------|---------|
| `topics` | Surgewave topics to consume (required) | - |
| `nats.subject.template` | Subject template with placeholders | `surgewave.${topic}` |

## Subject Templates

The sink connector supports subject templating with the following placeholders:

- `${topic}` - The Surgewave topic name
- `${key}` - The record key (as string)
- `${partition}` - The partition number

### Examples

```
surgewave.${topic}           -> surgewave.orders
events.${topic}.${key}   -> events.orders.order-123
data.p${partition}       -> data.p0
```

## Wildcard Subjects

NATS supports two wildcard tokens in subject subscriptions:

- `*` - Matches a single token: `orders.*` matches `orders.new` but not `orders.new.item`
- `>` - Matches one or more tokens: `orders.>` matches `orders.new` and `orders.new.item`

## Usage Examples

### Source Connector: Subscribe to NATS

```csharp
var config = new Dictionary<string, string>
{
    ["nats.url"] = "nats://localhost:4222",
    ["nats.subject"] = "orders.>",
    ["topic"] = "surgewave-orders",
    ["nats.queue.group"] = "order-processors"
};

var connector = new NatsSourceConnector();
connector.Start(config);
```

### Sink Connector: Publish to NATS

```csharp
var config = new Dictionary<string, string>
{
    ["nats.url"] = "nats://localhost:4222",
    ["topics"] = "processed-orders",
    ["nats.subject.template"] = "results.${topic}.${key}"
};

var connector = new NatsSinkConnector();
connector.Start(config);
```

### With Authentication

```csharp
var config = new Dictionary<string, string>
{
    ["nats.url"] = "nats://localhost:4222",
    ["nats.subject"] = "secure.events",
    ["topic"] = "secure-events",
    ["nats.username"] = "myuser",
    ["nats.password"] = "mypassword"
};
```

### With TLS

```csharp
var config = new Dictionary<string, string>
{
    ["nats.url"] = "nats://localhost:4222",
    ["nats.subject"] = "encrypted.events",
    ["topic"] = "encrypted-events",
    ["nats.tls.enabled"] = "true",
    ["nats.tls.cert.file"] = "/path/to/client.crt",
    ["nats.tls.key.file"] = "/path/to/client.key",
    ["nats.tls.ca.file"] = "/path/to/ca.crt"
};
```

## Header Mapping

### Source (NATS to Surgewave)

NATS message headers are mapped to Surgewave record headers with the prefix `nats.header.`:

| NATS Header | Surgewave Header |
|-------------|--------------|
| `X-Trace-Id` | `nats.header.X-Trace-Id` |
| (subject) | `nats.subject` |
| (reply-to) | `nats.reply.to` |

### Sink (Surgewave to NATS)

Surgewave record headers are copied to NATS message headers (excluding `nats.*` prefixed headers). Additionally, metadata headers are added:

| Surgewave Metadata | NATS Header |
|----------------|-------------|
| Topic | `surgewave.topic` |
| Partition | `surgewave.partition` |
| Offset | `surgewave.offset` |

## Queue Groups

Queue groups enable load balancing across multiple consumers. When multiple subscribers use the same queue group, each message is delivered to only one subscriber in the group.

```csharp
var config = new Dictionary<string, string>
{
    ["nats.subject"] = "orders.new",
    ["topic"] = "orders",
    ["nats.queue.group"] = "order-workers"
};
```

## Offset Tracking

The source connector tracks offsets using:
- `nats.offset.subject` - The subject from which the message was received
- `nats.offset.sequence` - A sequence number for ordering
- `nats.offset.timestamp` - The timestamp when the message was received

## Error Handling

- Connection failures during Start() will throw exceptions
- Connection drops during operation will be logged
- Empty record batches in PutAsync are handled gracefully
- Disposal is safe to call multiple times

## Dependencies

- **NATS.Net** (v2.7.1) - Official NATS .NET client
