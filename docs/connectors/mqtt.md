# MQTT Connector

The MQTT connector enables bidirectional streaming between Surgewave and MQTT brokers, supporting MQTT 3.1.1 and MQTT 5.0 protocols.

## Overview

- **Source**: Subscribe to MQTT topics and produce messages to Surgewave
- **Sink**: Consume from Surgewave and publish to MQTT topics

**Use Cases:**
- IoT data ingestion
- Edge device integration
- Home automation pipelines
- Industrial sensor networks

## Quick Start

### MQTT Source

Subscribe to MQTT and stream to Surgewave:

```json
{
  "name": "mqtt-source",
  "config": {
    "connector.class": "MqttSourceConnector",
    "mqtt.broker.url": "tcp://localhost:1883",
    "mqtt.topics": "sensors/+/temperature",
    "surgewave.topic": "sensor-data",
    "mqtt.qos": "1"
  }
}
```

### MQTT Sink

Publish Surgewave records to MQTT:

```json
{
  "name": "mqtt-sink",
  "config": {
    "connector.class": "MqttSinkConnector",
    "mqtt.broker.url": "tcp://localhost:1883",
    "mqtt.topic": "commands/${key}",
    "topics": "device-commands",
    "mqtt.qos": "1"
  }
}
```

## Configuration Reference

### Connection Settings

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `mqtt.broker.url` | string | Required | Broker URL (`tcp://` or `ssl://`) |
| `mqtt.client.id` | string | auto | Client identifier |
| `mqtt.username` | string | - | Authentication username |
| `mqtt.password` | password | - | Authentication password |
| `mqtt.clean.session` | bool | `true` | Start with clean session |
| `mqtt.keep.alive.seconds` | int | `60` | Keep-alive interval |
| `mqtt.connection.timeout.seconds` | int | `30` | Connection timeout |

### TLS Settings

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `mqtt.tls.enabled` | bool | `false` | Enable TLS |
| `mqtt.tls.allow.untrusted` | bool | `false` | Allow untrusted certificates |
| `mqtt.tls.client.cert.path` | string | - | Client certificate path |
| `mqtt.tls.client.cert.password` | password | - | Certificate password |

### Source Settings

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `mqtt.topics` | string | Required | MQTT topics (comma-separated) |
| `mqtt.qos` | int | `1` | QoS level (0, 1, 2) |
| `surgewave.topic` | string | Required | Destination Surgewave topic |
| `surgewave.topic.pattern` | string | - | Topic pattern with `${mqtt.topic}` |
| `mqtt.message.converter` | string | `bytes` | Converter: `bytes`, `string`, `json` |

### Sink Settings

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `topics` | string | Required | Source Surgewave topics |
| `mqtt.topic` | string | Required | MQTT topic to publish |
| `mqtt.topic.pattern` | string | - | Pattern: `${surgewave.topic}`, `${key}` |
| `mqtt.qos` | int | `1` | QoS level (0, 1, 2) |
| `mqtt.retain` | bool | `false` | Retain messages |
| `mqtt.message.expiry.seconds` | int | `0` | Message expiry (MQTT 5.0) |

## MQTT Topics

### Wildcards (Source)

MQTT wildcards for subscribing:

| Wildcard | Description | Example |
|----------|-------------|---------|
| `+` | Single level | `sensors/+/temp` matches `sensors/living-room/temp` |
| `#` | Multi level | `sensors/#` matches all under `sensors/` |

```json
{
  "mqtt.topics": "sensors/+/temperature,sensors/+/humidity"
}
```

### Topic Patterns (Sink)

Dynamic topic routing:

```json
{
  "mqtt.topic.pattern": "devices/${key}/commands"
}
```

Variables:
- `${surgewave.topic}` - Source Surgewave topic name
- `${key}` - Record key as string
- `${header.xxx}` - Header value

## QoS Levels

| Level | Description | Use Case |
|-------|-------------|----------|
| 0 | At most once | High-volume, loss-tolerant |
| 1 | At least once | Default, delivery guaranteed |
| 2 | Exactly once | Critical messages |

## Examples

### IoT Sensor Ingestion

Collect temperature data from all sensors:

```json
{
  "name": "sensor-collector",
  "config": {
    "connector.class": "MqttSourceConnector",
    "mqtt.broker.url": "tcp://mqtt.example.com:1883",
    "mqtt.username": "collector",
    "mqtt.password": "secret",
    "mqtt.topics": "sensors/#",
    "surgewave.topic.pattern": "iot.${mqtt.topic}",
    "mqtt.qos": "1",
    "mqtt.message.converter": "json"
  }
}
```

### Device Command Dispatch

Send commands to specific devices:

```json
{
  "name": "command-dispatcher",
  "config": {
    "connector.class": "MqttSinkConnector",
    "mqtt.broker.url": "ssl://mqtt.example.com:8883",
    "mqtt.username": "commander",
    "mqtt.password": "secret",
    "mqtt.tls.enabled": "true",
    "topics": "device-commands",
    "mqtt.topic.pattern": "devices/${key}/cmd",
    "mqtt.qos": "2",
    "mqtt.retain": "false"
  }
}
```

### Home Assistant Integration

Bridge Surgewave with Home Assistant:

```json
{
  "name": "homeassistant-bridge",
  "config": {
    "connector.class": "MqttSourceConnector",
    "mqtt.broker.url": "tcp://homeassistant.local:1883",
    "mqtt.topics": "homeassistant/+/+/state",
    "surgewave.topic": "home-events",
    "mqtt.message.converter": "json"
  }
}
```

### Mosquitto Local Development

```bash
# Start Mosquitto
docker run -p 1883:1883 eclipse-mosquitto

# Test publish
mosquitto_pub -h localhost -t "test/topic" -m '{"value": 42}'

# Test subscribe
mosquitto_sub -h localhost -t "test/topic"
```

```json
{
  "name": "local-mqtt",
  "config": {
    "connector.class": "MqttSourceConnector",
    "mqtt.broker.url": "tcp://localhost:1883",
    "mqtt.topics": "test/#",
    "surgewave.topic": "mqtt-test"
  }
}
```

## TLS Configuration

### Server Certificate Verification

```json
{
  "mqtt.broker.url": "ssl://mqtt.example.com:8883",
  "mqtt.tls.enabled": "true"
}
```

### Allow Self-Signed Certificates

```json
{
  "mqtt.tls.enabled": "true",
  "mqtt.tls.allow.untrusted": "true"
}
```

### Client Certificate Authentication

```json
{
  "mqtt.tls.enabled": "true",
  "mqtt.tls.client.cert.path": "/certs/client.pfx",
  "mqtt.tls.client.cert.password": "certpass"
}
```

## Message Converters

### Bytes (Default)

Raw bytes preserved:

```json
{
  "mqtt.message.converter": "bytes"
}
```

### String

UTF-8 encoded string:

```json
{
  "mqtt.message.converter": "string"
}
```

### JSON

Validates and processes JSON:

```json
{
  "mqtt.message.converter": "json"
}
```

## MQTT 5.0 Features

When using MQTT 5.0 brokers:

### Message Expiry

```json
{
  "mqtt.message.expiry.seconds": "3600"
}
```

### User Properties

MQTT 5.0 user properties map to Surgewave headers.

## Troubleshooting

### Common Issues

**Connection Refused**
- Verify broker is running
- Check port (1883 for TCP, 8883 for TLS)
- Ensure client ID is unique

**Authentication Failed**
- Verify username/password
- Check ACL permissions on broker
- Ensure client has publish/subscribe rights

**Message Loss**
- Increase QoS level
- Enable clean session = false for persistent sessions
- Check broker's max message size

### Monitoring

```bash
# Check connector status
surgewave connect status mqtt-source

# View metrics
surgewave connect describe mqtt-source
```

### Broker Logs

Monitor broker logs for connection issues:

```bash
# Mosquitto
docker logs mosquitto

# HiveMQ
docker logs hivemq
```

## See Also

- [Redis Connector](redis.md)
- [HTTP Webhook Connector](http.md)
- [Custom Connectors](custom-connectors.md)
