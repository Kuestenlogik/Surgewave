# QueueView

QueueView is Surgewave's queue semantics layer — a thin, stateful overlay that gives RabbitMQ/SQS-style
delivery guarantees on top of Surgewave's immutable log storage. Messages are never deleted from the log,
so log replay and consumer-group rewind remain fully available at the same time as at-least-once
queue delivery with visibility timeouts, requeue, and dead-letter routing.

## Table of Contents

- [Overview](#overview)
- [Concepts](#concepts)
- [Configuration](#configuration)
- [AMQP Integration](#amqp-integration)
- [REST API](#rest-api)
- [Architecture](#architecture)
- [Usage Examples](#usage-examples)
- [Comparison with Competitors](#comparison-with-competitors)

---

## Overview

Traditional message brokers force a choice: either a log (Kafka-style, immutable, replayable) or a
queue (RabbitMQ-style, destructive consume, ack/nack, dead-lettering). Surgewave removes that trade-off.

QueueView sits in front of an existing Surgewave partition log and adds a delivery-tracking layer:

- **Immutable log** — messages are appended once, never removed. Existing consumers and replay tools
  see everything as normal.
- **Queue overlay** — a second access path that tracks which offsets are in-flight, enforces a
  visibility timeout, re-delivers timed-out messages, and routes repeatedly-failed messages to a
  dead-letter topic.

The result: a single Surgewave topic can be consumed both by Kafka-style consumer groups (using offsets)
and by AMQP clients (using ack/nack/visibility) simultaneously.

---

## Concepts

### Log vs Queue Model

| Aspect | Log (Kafka-style) | Queue (QueueView-style) |
|--------|-------------------|------------------------|
| Message storage | Immutable, offset-addressed | Immutable, but delivery state is tracked |
| Consume | Advance offset, messages always re-readable | Deliver, hide, ack or re-deliver |
| Failure handling | Re-read from last committed offset | Visibility timeout expiry, re-queued automatically |
| Dead-lettering | Not built-in | Automatic DLQ topic after `MaxDeliveryCount` |
| Requeue | Not supported (log cannot re-insert) | Yes — `Nack(requeue: true)` |
| Log replay | Always available | Always available (QueueView state is separate) |

### Visibility Timeout

When a message is delivered via `ReceiveAsync`, QueueView records a timestamp
(`ExpiresAt = UtcNow + VisibilityTimeout`). The message is hidden from other consumers for the
duration of the timeout. If the consumer calls `Ack` before `ExpiresAt`, the message is removed from
in-flight tracking. If the consumer does not acknowledge in time, the background cleanup timer
re-queues the message for redelivery.

Default visibility timeout: **30 seconds**.

### Delivery Count

Each `InFlightMessage` tracks `DeliveryCount` — the number of times the message has been handed to a
consumer (starts at 1, increments on each redelivery). When `DeliveryCount` reaches
`MaxDeliveryCount` and the message has still not been acknowledged (visibility timeout expires), it is
routed to the dead-letter topic and removed from normal delivery.

Default maximum delivery count: **5 attempts**.

### In-Flight Messages

An in-flight message is one that has been delivered but not yet acknowledged. In-flight state is
tracked in a `ConcurrentDictionary<string, InFlightMessage>` keyed by a composite message ID:

```
{topic}-{partition}-{offset}
```

A second data structure, `ConcurrentQueue<InFlightMessage>`, holds messages that are eligible for
immediate redelivery (nacked with `requeue=true` or returned from timed-out visibility).

### Dead-Letter Queue

When a message exceeds `MaxDeliveryCount` or is rejected with `Basic.Reject(requeue=false)`, it is
routed to a dead-letter topic. The DLQ topic name is derived automatically:

```
{sourceTopic}{DlqTopicSuffix}
```

For example, the DLQ for topic `orders` is `orders.dlq` (using the default suffix `.dlq`).

QueueView creates the DLQ topic on demand using `LogManager.CreateTopicAsync` with:
- 1 partition, replication factor 1
- `cleanup.policy=delete`, 7-day retention

If no `LogManager` is registered or `DlqTopicSuffix` is empty, rejected messages are dropped with a
warning log rather than routed to a DLQ.

---

## Configuration

All QueueView settings live under the `Surgewave:QueueView` configuration section.

### Properties

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `Enabled` | `bool` | `false` | Enable QueueView semantics. Topics must also be explicitly enrolled. |
| `VisibilityTimeout` | `TimeSpan` | `00:00:30` | How long a delivered message is hidden before it becomes eligible for redelivery. |
| `MaxDeliveryCount` | `int` | `5` | Maximum delivery attempts before routing to the DLQ. |
| `DlqTopicSuffix` | `string` | `".dlq"` | Suffix appended to the source topic name to derive the DLQ topic name. Set to empty string to disable DLQ routing. |
| `CleanupInterval` | `TimeSpan` | `00:00:10` | How often the background timer checks for expired visibility timeouts. |
| `MaxInFlightPerConsumer` | `int` | `1000` | Maximum in-flight (delivered-but-not-acknowledged) messages per consumer. |

### appsettings.json Example

```json
{
  "Surgewave": {
    "QueueView": {
      "Enabled": true,
      "VisibilityTimeout": "00:00:30",
      "MaxDeliveryCount": 5,
      "DlqTopicSuffix": ".dlq",
      "CleanupInterval": "00:00:10",
      "MaxInFlightPerConsumer": 1000
    },
    "Amqp": {
      "Enabled": true,
      "Port": 5672
    }
  }
}
```

### Feature Toggle Pattern

QueueView follows Surgewave's standard feature toggle pattern. Enable it via the configuration section
or from the command line:

```bash
dotnet run --project src/Kuestenlogik.Surgewave.Broker -- \
  --Surgewave:QueueView:Enabled=true \
  --Surgewave:Amqp:Enabled=true
```

QueueView is only active for topics that are enrolled through `QueueViewManager.GetOrCreate`. The
AMQP adapter enrols topics automatically when a consumer registers via `Basic.Consume`.

---

## AMQP Integration

The AMQP 0.9.1 adapter (`Kuestenlogik.Surgewave.Protocol.Amqp`) uses QueueView as its delivery backend when
`IQueueViewManager` is registered in DI. Without QueueView, it falls back to log-based offset
tracking (no requeue support).

### How Topics Are Enrolled

When an AMQP client calls `Basic.Consume` on a queue, the adapter resolves the corresponding Surgewave
topic, then calls `QueueViewManager.GetOrCreate(topic, log)` to enrol the topic. From that point,
all messages delivered to that consumer go through the QueueView delivery path.

### AMQP Method Mapping

#### With QueueView (recommended)

| AMQP Method | QueueView Call | Effect |
|-------------|---------------|--------|
| `Basic.Ack` (single) | `IQueueView.Ack(messageId)` | Removes message from in-flight; advances committed offset |
| `Basic.Ack` (multiple) | `IQueueView.Ack(...)` for each tag ≤ deliveryTag | Batch acknowledgement |
| `Basic.Nack` (requeue=true) | `IQueueView.Nack(messageId, requeue: true)` | Message placed back in redelivery queue immediately |
| `Basic.Nack` (requeue=false) | `IQueueView.Nack(messageId, requeue: false)` | Message dropped silently (offset not advanced) |
| `Basic.Reject` (requeue=true) | `IQueueView.Nack(messageId, requeue: true)` | Message placed back in redelivery queue |
| `Basic.Reject` (requeue=false) | `IQueueView.RejectAsync(messageId)` | Message routed to DLQ topic |

#### Without QueueView (fallback mode)

When `IQueueViewManager` is not registered in DI, the adapter tracks delivery tags against Surgewave log
offsets directly. In this mode:

- `Basic.Ack` commits the offset for that delivery tag.
- `Basic.Nack(requeue=true)` logs a warning; the message is **not** requeued (log-based systems
  cannot re-insert messages at an earlier position).
- `Basic.Reject(requeue=true)` logs the same warning.
- `Basic.Nack(requeue=false)` and `Basic.Reject(requeue=false)` silently remove the delivery tag.

### Redelivery Flag

When QueueView delivers a message with `DeliveryCount > 1`, the AMQP adapter sets the `redelivered`
flag to `true` in the `Basic.Deliver` frame. RabbitMQ-compatible clients that inspect this flag will
correctly identify redelivered messages.

### Limitations

The following AMQP features are not currently supported:

| Feature | Status |
|---------|--------|
| Per-message TTL | Not supported — visibility timeout applies globally to all messages on the topic |
| Exchange routing (fanout, topic, headers) | Exchanges are accepted and mapped to Surgewave topics; full routing-key pattern matching is not implemented |
| Queue TTL / auto-delete | Not supported |
| Publisher confirms (`Basic.Confirm`) | Not supported |
| Per-consumer prefetch (`Basic.Qos`) | Not enforced — `MaxInFlightPerConsumer` is a soft cap |
| Multiple queues on one channel | The adapter tracks one active consumer tag per channel |
| Transaction semantics (`Tx.*`) | Not supported |

---

## REST API

The QueueView REST API is registered under `/api/queue` and tagged as `Queue Semantics` in the
OpenAPI schema.

### `GET /api/queue/topics`

Returns the list of topic names that currently have an active `QueueView`.

**Response:** `200 OK`

```json
["orders", "payments", "shipments"]
```

An empty array is returned when no topics have been enrolled.

---

### `GET /api/queue/{topic}/status`

Returns the current in-flight count for a single topic's `QueueView`.

**Path parameter:** `topic` — topic name (URL-encoded)

**Response:** `200 OK`

```json
{
  "topic": "orders",
  "inFlightCount": 42
}
```

**Response:** `404 Not Found` — when no `QueueView` exists for the topic

```json
{
  "message": "No QueueView found for topic 'orders'"
}
```

---

### `POST /api/queue/{topic}/purge`

Clears all in-flight messages for a topic by removing and disposing its `QueueView`. The view
is recreated transparently on the next `ReceiveAsync` call (e.g. when the next consumer connects).

**Path parameter:** `topic` — topic name

**Response:** `200 OK`

```json
{
  "topic": "orders",
  "message": "In-flight messages cleared. QueueView will be recreated on next receive call."
}
```

**Response:** `404 Not Found` — when no `QueueView` exists for the topic

> **Note:** Purging discards in-flight state only. The underlying Surgewave log is not modified and
> no messages are lost. Consumers that reconnect after a purge will receive messages from their
> last committed offset onward.

---

## Architecture

### Component Diagram

```mermaid
flowchart TB
    Client["AMQP Client<br/>(RabbitMQ SDK, etc.)"]
    Adapter["AmqpBrokerAdapter<br/>(Protocol.Amqp)<br/>BackgroundService, one per connection"]
    Manager["QueueViewManager<br/>(Broker.Queue)<br/>Registry: ConcurrentDictionary&lt;topic, QueueView&gt;<br/>IAsyncDisposable"]
    View["QueueView (Broker.Queue)<br/>IQueueView implementation<br/>_inFlight: ConcurrentDictionary&lt;messageId, InFlightMessage&gt;<br/>_redeliveryQueue: ConcurrentQueue&lt;InFlightMessage&gt;<br/>_committedOffsets: ConcurrentDictionary&lt;partition, long&gt;<br/>_readOffsets: ConcurrentDictionary&lt;partition, long&gt;<br/>_cleanupTimer: System.Threading.Timer"]
    Log["Surgewave Partition Log (Core.Storage)<br/>Immutable append-only log<br/>Unchanged by QueueView operations"]

    Client -->|AMQP 0.9.1 over TCP (port 5672)| Adapter
    Adapter -->|GetOrCreate(topic, log)<br/>ReceiveAsync / Ack / Nack / RejectAsync| Manager
    Manager -->|one QueueView per enrolled topic| View
    View -->|IPartitionLog.ReadBatchesAsync<br/>LogManager.AppendBatchAsync (DLQ writes)| Log
```

### Internal Data Structures

| Field | Type | Purpose |
|-------|------|---------|
| `_inFlight` | `ConcurrentDictionary<string, InFlightMessage>` | Tracks all messages currently delivered but not yet acknowledged. Keyed by `{topic}-{partition}-{offset}`. |
| `_redeliveryQueue` | `ConcurrentQueue<InFlightMessage>` | FIFO queue of messages eligible for immediate redelivery (nacked with requeue or returned from visibility timeout). Drained before reading new messages from the log. |
| `_committedOffsets` | `ConcurrentDictionary<int, long>` | Per-partition highest acknowledged offset. Only moves forward. |
| `_readOffsets` | `ConcurrentDictionary<int, long>` | Per-partition next log offset to read. Starts at `LogStartOffset`. |

### Cleanup Timer

A `System.Threading.Timer` fires every `CleanupInterval` (default 10 s). On each tick:

1. Snapshot all keys from `_inFlight` where `ExpiresAt <= UtcNow`.
2. For each expired key, remove from `_inFlight` atomically.
3. If `DeliveryCount >= MaxDeliveryCount`, add to a DLQ candidate list.
4. Otherwise, enqueue back into `_redeliveryQueue`.
5. Route DLQ candidates to the dead-letter topic via a fire-and-forget `Task.Run`.

The snapshot-then-remove pattern avoids modifying the dictionary while enumerating it.

### Thread Safety

All public methods are safe to call from concurrent request handlers:

- `_inFlight` and `_committedOffsets` use `ConcurrentDictionary` with lock-free read paths.
- `_redeliveryQueue` is a `ConcurrentQueue` — all enqueue/dequeue operations are atomic.
- `_readOffsets` updates are per-partition; concurrent single-partition access from the same consumer
  tag is single-threaded by design (one consume loop per consumer tag).
- `QueueViewManager._views` is a `ConcurrentDictionary` — `GetOrCreate` is atomic.

---

## Usage Examples

### RabbitMQ .NET Client Connecting to Surgewave

Any standard RabbitMQ 6.x or 7.x .NET client works with Surgewave's AMQP adapter without modification.

```csharp
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

// Point to Surgewave instead of RabbitMQ — no code change needed
var factory = new ConnectionFactory
{
    HostName = "localhost",
    Port = 5672,          // Surgewave AMQP port
    UserName = "guest",
    Password = "guest",
    VirtualHost = "/"
};

using var connection = await factory.CreateConnectionAsync();
using var channel = await connection.CreateChannelAsync();

// Declare the Surgewave topic as an AMQP queue
await channel.QueueDeclareAsync("orders", durable: true, exclusive: false, autoDelete: false);

// Consume with explicit acknowledgement
var consumer = new AsyncEventingBasicConsumer(channel);
consumer.ReceivedAsync += async (_, ea) =>
{
    try
    {
        var body = ea.Body.ToArray();
        // ... process message ...
        await channel.BasicAckAsync(ea.DeliveryTag, multiple: false);
    }
    catch (Exception)
    {
        // Nack with requeue — Surgewave QueueView re-delivers after visibility timeout
        await channel.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: true);
    }
};

await channel.BasicConsumeAsync("orders", autoAck: false, consumer: consumer);
```

### Docker: Start Surgewave with AMQP and QueueView Enabled

```bash
docker run -d \
  --name surgewave \
  -p 9092:9092 \
  -p 5672:5672 \
  -p 5050:5050 \
  -e Surgewave__QueueView__Enabled=true \
  -e Surgewave__QueueView__VisibilityTimeout=00:00:30 \
  -e Surgewave__QueueView__MaxDeliveryCount=5 \
  -e Surgewave__QueueView__DlqTopicSuffix=.dlq \
  -e Surgewave__Amqp__Enabled=true \
  -e Surgewave__Amqp__Port=5672 \
  surgewave:latest
```

### dotnet run: Start Surgewave with AMQP and QueueView Enabled

```bash
dotnet run --project src/Kuestenlogik.Surgewave.Broker -- \
  --Surgewave:QueueView:Enabled=true \
  --Surgewave:QueueView:VisibilityTimeout=00:00:30 \
  --Surgewave:QueueView:MaxDeliveryCount=5 \
  --Surgewave:QueueView:DlqTopicSuffix=.dlq \
  --Surgewave:Amqp:Enabled=true \
  --Surgewave:Amqp:Port=5672
```

### Complete appsettings.json for Production Use

```json
{
  "Surgewave": {
    "QueueView": {
      "Enabled": true,
      "VisibilityTimeout": "00:01:00",
      "MaxDeliveryCount": 3,
      "DlqTopicSuffix": ".dlq",
      "CleanupInterval": "00:00:05",
      "MaxInFlightPerConsumer": 500
    },
    "Amqp": {
      "Enabled": true,
      "Port": 5672,
      "MaxConnections": 200,
      "MaxFrameSize": 131072,
      "MaxChannels": 2047,
      "HeartbeatInterval": 60
    }
  },
  "Logging": {
    "LogLevel": {
      "Kuestenlogik.Surgewave.Broker.Queue": "Information",
      "Kuestenlogik.Surgewave.Protocol.Amqp": "Information"
    }
  }
}
```

### Inspect In-Flight State via REST

```bash
# List all topics that have an active QueueView
curl http://localhost:5050/api/queue/topics

# Get in-flight count for the 'orders' topic
curl http://localhost:5050/api/queue/orders/status

# Purge stuck in-flight messages (safe — log is not modified)
curl -X POST http://localhost:5050/api/queue/orders/purge
```

### Check the DLQ

The DLQ topic is a standard Surgewave topic. Consume it with any Kafka-compatible client or the Surgewave
.NET client:

```bash
# Using kcat (kafkacat)
kcat -b localhost:9092 -t orders.dlq -C -o beginning
```

---

## Comparison with Competitors

| Feature | Surgewave QueueView | RabbitMQ | Apache Kafka | Apache Pulsar |
|---------|-----------------|----------|--------------|---------------|
| At-least-once delivery | Yes | Yes | Yes (consumer groups) | Yes |
| Visibility timeout | Yes (configurable) | No (nack/reject instead) | No | Yes (ack timeout) |
| Requeue on nack | Yes (`requeue=true`) | Yes | No | Yes |
| Dead-letter routing | Yes (auto-created DLQ topic) | Yes (x-dead-letter-exchange) | No (manual) | Yes (configurable) |
| Log replay after consume | Yes (log untouched) | No (messages deleted) | Yes (offset reset) | Yes (subscription reset) |
| Log + queue simultaneously | Yes | No | No | No |
| AMQP 0.9.1 compatible | Yes | Yes (native) | No | Partial (KoP workaround) |
| Per-message TTL | No | Yes | No | Yes |
| Message priorities | No | Yes | No | No |
| Delivery count tracking | Yes | Yes | No | Yes |
| Broker-side filtering | No | Yes (header exchange) | No | Yes (SQL filter) |
| Storage backend | Pluggable (7 engines) | Mnesia / Quorum Queues | Log segments | BookKeeper |
| .NET 10 native | Yes | No (AMQP client only) | No | No |

### What Works Well

- **Migrating from RabbitMQ** — existing clients that use `Basic.Ack`, `Basic.Nack(requeue=true)`,
  and `Basic.Reject` work without code changes. Point the connection factory at Surgewave's AMQP port.
- **Dual-access topics** — produce via Kafka protocol (native clients, Kafka Streams), consume via
  AMQP — both against the same topic, simultaneously.
- **Zero-loss DLQ** — messages exceed `MaxDeliveryCount` and are automatically routed to a
  separate topic, still stored in the Surgewave log, still replayable.
- **Purge without data loss** — `POST /api/queue/{topic}/purge` clears in-flight state; the
  underlying log is untouched.

### Known Limitations

- **Per-message TTL** — not supported. The visibility timeout is a global setting per
  `QueueViewConfig`, not per message.
- **Exchange routing** — Surgewave accepts `Exchange.Declare` and `Queue.Bind` but does not implement
  fanout/topic/headers routing logic. Messages are routed to Surgewave topics by name mapping only.
- **Requeue in fallback mode** — without `QueueViewManager`, `Basic.Nack(requeue=true)` is a no-op
  (Surgewave's log cannot re-insert messages at earlier offsets).
- **Multiple queues per channel** — the AMQP adapter tracks one active consumer tag per channel;
  running multiple `Basic.Consume` calls on the same channel is not supported.

---

## See Also

- [AMQP Adapter](amqp-adapter.md) - Full AMQP 0.9.1 protocol support (RabbitMQ-compatible)
- [Kafka Compatibility](clients/kafka-compat.md)
- [Glossary](glossary.md)
