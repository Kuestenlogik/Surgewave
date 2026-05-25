# AMQP 0.9.1

Surgewave speaks AMQP 0.9.1 on `:5672` (configurable), giving RabbitMQ
clients a drop-in target. Every routed message lands on a Surgewave topic
for replay, analytics, or fan-out to Kafka consumers.

## Supported features

- **Exchanges** — direct, topic, fanout, headers
- **Queues** — durable, exclusive, auto-delete
- **Routing** — bindings with routing keys
- **Acknowledgements** — basic.ack, basic.nack, basic.reject
- **Publisher confirms** — broker-side delivery guarantees

## Configuration

```yaml
Surgewave:
  Amqp:
    Enabled: true
    Port: 5672
    DefaultVhost: "/"
```

## Migration path

Point existing RabbitMQ-using applications at Surgewave's AMQP listener —
they connect unchanged. Behind the scenes, every message hits a Surgewave
topic, so you can replay history, attach a Kafka consumer for analytics,
or stream into AI pipelines without re-architecting.

See the [transport overview](index.md) for the protocol-bridge architecture
across AMQP, MQTT, Kafka wire, and the native protocol.
