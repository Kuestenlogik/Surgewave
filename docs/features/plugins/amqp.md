# AMQP 0.9.1 Protocol Plugin

`kuestenlogik.surgewave.protocol.amqp` — AMQP 0.9.1 wire-protocol adapter that lets
RabbitMQ-compatible clients (any language with an AMQP 0.9.1 library) talk
directly to Surgewave topics. Useful for migrating off RabbitMQ without
rewriting clients.

## Installation

```bash
surgewave plugin install kuestenlogik.surgewave.protocol.amqp-<version>.swpkg
```

## Configuration

Section: `Surgewave:Amqp`. Every field has a recommended default in
`pluginsettings.json`; only override what you need.

| Field | Default | Description |
|---|---|---|
| `Enabled` | `false` | Master switch. Set to `true` to start the AMQP listener. |
| `Port` | `5672` | TCP port for plain AMQP (AMQP-over-TLS uses port 5671 by convention but is not yet supported). |
| `MaxChannels` | `256` | Maximum channels per connection. AMQP clients multiplex requests over channels within a connection. |
| `HeartbeatInterval` | `60` | Heartbeat interval in seconds. Connections that miss two consecutive heartbeats are considered dead. |
| `MaxConnections` | `1000` | Maximum concurrent AMQP client connections. |
| `MaxFrameSize` | `131072` (128 KB) | Maximum AMQP frame size. Larger messages are split across frames automatically. |
| `AllowAnonymous` | `true` | Accept connections without SASL credentials. Set to `false` for production. |
| `VirtualHost` | `"/"` | Virtual host name accepted by the adapter. AMQP clients always connect to a vhost; the default `/` is the universal value. |

### Minimal config

```json
{
  "Surgewave": {
    "Amqp": { "Enabled": true }
  }
}
```

The AMQP listener starts on port 5672, accepts anonymous connections to
vhost `/`, and routes messages into Surgewave topics.

### Production-ish config

```json
{
  "Surgewave": {
    "Amqp": {
      "Enabled": true,
      "Port": 5672,
      "MaxConnections": 10000,
      "AllowAnonymous": false,
      "VirtualHost": "/surgewave"
    }
  }
}
```

## Compatibility notes

- **Exchanges and queues** — Surgewave uses its log abstraction underneath, so
  the classic AMQP exchange/queue/binding model is mapped onto Surgewave topics.
  See `Kuestenlogik.Surgewave.Protocol.Amqp` source for the exact mapping rules.
- **Acknowledgements** — auto-ack and manual-ack both work. Manual ack
  commits the offset; nack with requeue replays the message.
- **TLS** — not yet supported in this version.

## Operations

```bash
surgewave plugin show kuestenlogik.surgewave.protocol.amqp
surgewave config view appsettings.json --explain
surgewave config validate appsettings.json
```

## Reference

- Source: `src/Kuestenlogik.Surgewave.Protocol.Amqp/`
- Config class: `AmqpConfig.cs`
