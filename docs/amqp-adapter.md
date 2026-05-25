# AMQP 0.9.1 Adapter

The AMQP 0.9.1 adapter allows any AMQP-compatible client (RabbitMQ client libraries, Spring AMQP,
Celery, etc.) to produce and consume messages through Surgewave topics without code changes.

## Overview

Surgewave's AMQP adapter is a `BackgroundService` that listens on a dedicated TCP port (default 5672)
and speaks the AMQP 0.9.1 wire protocol. Internally, messages are stored in Surgewave's immutable log,
so log replay and consumer-group rewind remain available alongside AMQP clients.

Delivery semantics depend on whether QueueView is enabled:

- **With QueueView** — full RabbitMQ-compatible semantics: visibility timeouts, `Nack(requeue:true)`,
  dead-letter routing after `MaxDeliveryCount`.
- **Without QueueView** — offset-based tracking similar to Kafka consumer groups.

## Enabling the Adapter

Add to `appsettings.json`:

```json
{
  "Surgewave": {
    "Amqp": {
      "Enabled": true,
      "Port": 5672
    }
  }
}
```

Register in DI (typically done once in `Program.cs`):

```csharp
builder.Services.AddSurgewaveAmqp(builder.Configuration);
```

## Configuration Reference

All settings live under the `Surgewave:Amqp` section.

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `Enabled` | bool | `false` | Enable the AMQP adapter |
| `Port` | int | `5672` | TCP port to listen on |
| `MaxChannels` | int | `256` | Max AMQP channels per connection |
| `HeartbeatInterval` | int (s) | `60` | Heartbeat interval; connections missing two beats are closed |
| `MaxConnections` | int | `1000` | Max concurrent AMQP connections |
| `MaxFrameSize` | int (bytes) | `131072` | Max AMQP frame size (128 KB) |
| `AllowAnonymous` | bool | `true` | Allow connections without SASL credentials |
| `VirtualHost` | string | `"/"` | Accepted AMQP virtual host |

## Exchange Types and Surgewave Topic Mapping

AMQP exchanges are virtual constructs. The adapter maps exchange+routing-key pairs to Surgewave
topic names according to the exchange type:

| Exchange Type | Surgewave Topic Name |
|---------------|-----------------|
| `direct` | Routing key (falls back to exchange name if key is empty) |
| `fanout` | Exchange name; all bound queues receive every message |
| `topic` | Routing key (pattern wildcards `*` and `#` are matched at bind time) |
| `headers` | Routing key (headers matching is not fully implemented; treated as direct) |

Name normalisation: dots (`.`) and slashes (`/`) in exchange names and routing keys are replaced
with hyphens (`-`).

**Examples:**

| Exchange | Routing Key | Surgewave Topic |
|----------|-------------|-------------|
| `""` (default) | `orders` | `orders` |
| `events` (fanout) | *(ignored)* | `events` |
| `app` (direct) | `user.created` | `user-created` |
| `logs` (topic) | `app.error.*` | `app-error-*` |

## Queue to Consumer Group Mapping

Each AMQP queue name is mapped to a Surgewave consumer-group name (same normalisation rules apply).
This gives each queue its own independent read offset, matching RabbitMQ semantics where
competing consumers within a queue share work.

```
AMQP Queue "invoice-service"  →  Surgewave consumer group "invoice-service"
AMQP Queue "audit.log"        →  Surgewave consumer group "audit-log"
```

## Delivery Semantics

### With QueueView

When `AddSurgewaveQueueView()` is registered in DI, the adapter uses full queue semantics:

| AMQP Method | QueueView Action |
|-------------|-----------------|
| `Basic.Ack` | `Ack` — marks message as processed |
| `Basic.Nack requeue=true` | `Nack(requeue:true)` — message redelivered after visibility timeout |
| `Basic.Nack requeue=false` | `Nack(requeue:false)` — message dropped |
| `Basic.Reject requeue=true` | `Nack(requeue:true)` — message redelivered |
| `Basic.Reject requeue=false` | `RejectAsync` — routes to Dead Letter Queue topic |

### Without QueueView (Fallback Mode)

Without QueueView the adapter falls back to offset-commit tracking:

| AMQP Method | Action |
|-------------|--------|
| `Basic.Ack` | Commits the consumer-group offset |
| `Basic.Nack requeue=true` | Logged as warning; not supported in log-based mode |
| `Basic.Nack requeue=false` | Offset is not committed; message will be redelivered on restart |
| `Basic.Reject` | Equivalent to `Nack` |

## Supported AMQP Methods

| Class | Methods |
|-------|---------|
| Connection | Start, StartOk, Tune, TuneOk, Open, OpenOk, Close, CloseOk |
| Channel | Open, OpenOk, Close, CloseOk |
| Exchange | Declare, DeclareOk |
| Queue | Declare, DeclareOk, Bind, BindOk |
| Basic | Publish, Consume, ConsumeOk, Deliver, Ack, Nack, Reject |
| Heartbeat | Both directions |

## RabbitMQ Client Example (.NET)

Connect to Surgewave using the standard RabbitMQ .NET client:

```csharp
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

var factory = new ConnectionFactory
{
    HostName = "localhost",
    Port = 5672,
    UserName = "guest",
    Password = "guest",
    VirtualHost = "/"
};

await using var connection = await factory.CreateConnectionAsync();
await using var channel = await connection.CreateChannelAsync();

// Declare a direct exchange (Surgewave maps routing key → topic)
await channel.ExchangeDeclareAsync("my-exchange", ExchangeType.Direct, durable: true);
await channel.QueueDeclareAsync("my-queue", durable: true, exclusive: false, autoDelete: false);
await channel.QueueBindAsync("my-queue", "my-exchange", routingKey: "orders");

// Produce a message
var body = Encoding.UTF8.GetBytes("""{"id": 1, "item": "widget"}""");
await channel.BasicPublishAsync("my-exchange", "orders", body);

// Consume messages
var consumer = new AsyncEventingBasicConsumer(channel);
consumer.ReceivedAsync += async (_, ea) =>
{
    var message = Encoding.UTF8.GetString(ea.Body.Span);
    Console.WriteLine($"Received: {message}");
    await channel.BasicAckAsync(ea.DeliveryTag, multiple: false);
};

await channel.BasicConsumeAsync("my-queue", autoAck: false, consumer);
```

## Python (pika) Example

```python
import pika

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost', port=5672))
channel = connection.channel()

channel.exchange_declare(exchange='events', exchange_type='fanout')
channel.queue_declare(queue='my-queue')
channel.queue_bind(exchange='events', queue='my-queue')

# Produce
channel.basic_publish(exchange='events', routing_key='', body=b'Hello Surgewave')

# Consume
def callback(ch, method, properties, body):
    print(f"Received: {body}")
    ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_consume(queue='my-queue', on_message_callback=callback)
channel.start_consuming()
```

## Docker / Docker Compose

Expose port 5672 and enable the adapter:

```yaml
services:
  surgewave:
    image: surgewave:latest
    ports:
      - "9092:9092"   # native Surgewave protocol
      - "5672:5672"   # AMQP
    environment:
      Surgewave__Amqp__Enabled: "true"
      Surgewave__Amqp__Port: "5672"
      Surgewave__Amqp__AllowAnonymous: "true"
```

## Limitations

- **Headers exchange** — routing based on header values is not implemented; the adapter falls
  back to direct-style routing on the routing key.
- **Exchange-to-exchange bindings** — not supported; exchanges bind directly to Surgewave topics.
- **Mandatory/immediate flags** — accepted but silently ignored.
- **Transactions (`tx.*`)** — AMQP transaction methods are not implemented; use Surgewave's native
  transaction API for exactly-once semantics.
- **QoS prefetch count** — `Basic.Qos` is accepted but the prefetch count is not enforced;
  use `MaxBytesPerPush` on the streaming consumer instead.
- **SSL/TLS** — the adapter currently accepts plain TCP only; place a TLS-terminating proxy
  (e.g., nginx, Envoy) in front for encrypted connections.

## Next Steps

- [QueueView](queue-view.md) — RabbitMQ-compatible delivery semantics on top of Surgewave logs
- [Dead Letter Queue](features/dlq.md) — Automatic DLQ routing for rejected messages
- [Transactions](features/transactions.md) — Exactly-once semantics
