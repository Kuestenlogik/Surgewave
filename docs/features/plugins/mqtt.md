# MQTT Protocol Plugin

`kuestenlogik.surgewave.protocol.mqtt` — bundles an MQTT 3.1.1/5 broker (MQTTnet) on top
of Surgewave. IoT devices can publish to and consume from Surgewave topics using
any standard MQTT client (mosquitto, Eclipse Paho, ESP32 firmware, ...).

## Installation

```bash
surgewave plugin install kuestenlogik.surgewave.protocol.mqtt-<version>.swpkg
```

## Configuration

Section: `Surgewave:Mqtt`. Every field has a recommended default shipped via
`pluginsettings.json`; only override what you need.

| Field | Default | Description |
|---|---|---|
| `Enabled` | `false` | Master switch. Set to `true` to start the MQTT listener at broker startup. |
| `Port` | `1883` | TCP port for plain MQTT. Use a non-default if you also run a separate MQTT broker on the same host. |
| `TopicPrefix` | `"mqtt."` | Surgewave topic prefix applied to inbound MQTT topics. MQTT topic `sensors/temp` lands in Surgewave topic `mqtt.sensors.temp` (slashes converted to dots). |
| `MaxClients` | `1000` | Maximum concurrent MQTT client connections. |
| `MaxMessageSizeBytes` | `262144` (256 KB) | Maximum MQTT message payload size. |
| `AllowAnonymous` | `true` | Accept connections without username/password. Set to `false` for production. |
| `KeepAliveSeconds` | `60` | MQTT keep-alive interval. Clients that miss 1.5× this without a PINGREQ are disconnected. |

### Minimal config

```json
{
  "Surgewave": {
    "Mqtt": { "Enabled": true }
  }
}
```

That's it — every other field comes from the plugin's bundled defaults.
The MQTT listener starts on port 1883 with anonymous connections accepted
and the `mqtt.` topic prefix.

### Production-ish config

```json
{
  "Surgewave": {
    "Mqtt": {
      "Enabled": true,
      "Port": 8883,
      "TopicPrefix": "iot.",
      "AllowAnonymous": false,
      "MaxClients": 50000,
      "MaxMessageSizeBytes": 1048576
    }
  }
}
```

## Topic mapping

| MQTT topic | Surgewave topic |
|---|---|
| `sensors/temperature` | `mqtt.sensors.temperature` |
| `building/floor1/room2/light` | `mqtt.building.floor1.room2.light` |

The slash → dot conversion is irreversible from Surgewave's perspective: a
producer writing directly to `mqtt.sensors.temperature` is indistinguishable
from one going through the MQTT plugin. Choose your `TopicPrefix` so that
MQTT-sourced topics are recognisable.

## Operations

```bash
# Inspect the installed plugin
surgewave plugin show kuestenlogik.surgewave.protocol.mqtt

# View the effective MQTT config
surgewave config view appsettings.json --explain

# Validate the merged config
surgewave config validate appsettings.json
```

## Reference

- Source: `src/Kuestenlogik.Surgewave.Protocol.Mqtt/`
- Config class: `MqttConfig.cs`
- Bundled defaults: `pluginsettings.json` in the plugin source root
