# MQTT 3.1.1 / 5.0

Surgewave bridges MQTT clients into Kafka-compatible event streams. The MQTT
adapter listens on `:1883` (configurable), translating publish/subscribe
into produce/fetch on Surgewave topics.

## Supported features

- **MQTT 3.1.1 and 5.0** wire-compatible
- **QoS 0 / 1 / 2** with broker-side persistence
- **Retained messages** — last value preserved per topic
- **Last-will & testament** — connection-loss notifications
- **Topic mapping** — MQTT topic patterns (`sensors/+/temp`) map onto
  Surgewave topics with configurable transformation

## Configuration

```yaml
Surgewave:
  Mqtt:
    Enabled: true
    Port: 1883
    MaxClients: 10000
```

## Use cases

Most common: IoT fleets push readings via MQTT to the gateway, downstream
analytics consume the same data via the Kafka wire on `:9092` — no
protocol bridge, no data duplication.

See the [Multi-Protocol Gateway guide](../integrations/multi-protocol-gateway.md)
for a full end-to-end walkthrough.
