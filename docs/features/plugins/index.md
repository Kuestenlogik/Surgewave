# Protocol Plugins

Surgewave ships five community protocol plugins that let third-party clients
talk to a Surgewave broker without using the native Surgewave or Kafka client
libraries. Every plugin ships as a `.swpkg` package, ships its recommended
defaults via `pluginsettings.json`, and is enabled with a single
`Enabled: true` line in the broker's `appsettings.json`.

| Plugin | Section | Default port | Use case |
|---|---|---|---|
| [MQTT](mqtt.md) | `Surgewave:Mqtt` | 1883 | IoT device connectivity (MQTTnet server) |
| [AMQP 0.9.1](amqp.md) | `Surgewave:Amqp` | 5672 | RabbitMQ-compatible clients |
| [WebSocket](websocket.md) | `Surgewave:WebSocket` | shared HTTP | Browser-based streaming clients |
| [PostgreSQL](postgresql.md) | `Surgewave:PostgreSql` | 5432 | psql / pgAdmin / JDBC for SQL queries against topics |
| Kafka | `Surgewave:` (broker) | 9092 | Native Kafka wire protocol — built into the broker, no plugin |

## Installing

```bash
surgewave plugin install kuestenlogik.surgewave.protocol.mqtt-0.1.0.swpkg
```

Each `.swpkg` extracts to `plugins/<id>/` and bundles a `pluginsettings.json`
with the recommended defaults — so the only required line in the operator's
`appsettings.json` to start an adapter is:

```json
{
  "Surgewave": {
    "Mqtt": { "Enabled": true }
  }
}
```

Every other field (port, max clients, topic prefix, ...) is inherited from
the plugin's `pluginsettings.json`. Override individual fields in
`appsettings.json` only when you need to deviate from the defaults.

## Inspecting

```bash
surgewave plugin show kuestenlogik.surgewave.protocol.mqtt          # manifest, assemblies, defaults
surgewave plugin defaults kuestenlogik.surgewave.protocol.mqtt      # just the defaults JSON
surgewave config view appsettings.json --explain      # source per leaf
surgewave config validate appsettings.json            # check the merged config
```

## Plugin-defaults model

See [plugin-defaults.md](../plugin-defaults.md) for the full 3-tier
configuration precedence model and how plugin authors ship their own
default values.
