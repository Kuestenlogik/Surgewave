# Redis Connector

The Redis connector enables streaming data between Surgewave and Redis, supporting Streams, Pub/Sub, and key-value operations.

## Overview

- **Source**: Read from Redis Streams with consumer groups, or subscribe to Pub/Sub channels
- **Sink**: Write to Redis as strings, hashes, or stream entries

**Use Cases:**
- Real-time cache synchronization
- Event-driven architectures
- Pub/Sub message routing
- Session and state management

## Quick Start

### Redis Stream Source

Read from Redis Streams:

```json
{
  "name": "redis-stream-source",
  "config": {
    "connector.class": "RedisSourceConnector",
    "redis.connection.string": "localhost:6379",
    "redis.stream.name": "events",
    "topic": "redis-events",
    "source.mode": "stream"
  }
}
```

### Redis Sink

Write to Redis:

```json
{
  "name": "redis-sink",
  "config": {
    "connector.class": "RedisSinkConnector",
    "redis.connection.string": "localhost:6379",
    "topics": "user-sessions",
    "sink.mode": "string",
    "redis.key.prefix": "session:"
  }
}
```

## Configuration Reference

### Connection Settings

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `redis.connection.string` | string | Required | Redis connection string |
| `redis.password` | password | - | Redis password |
| `redis.database` | int | `0` | Database number |
| `redis.ssl` | bool | `false` | Enable SSL/TLS |

### Source Settings

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `topic` | string | Required | Destination Surgewave topic |
| `source.mode` | string | `stream` | Mode: `stream`, `pubsub` |
| `redis.stream.name` | string | - | Stream name (stream mode) |
| `redis.channel.pattern` | string | - | Channel pattern (pubsub mode) |
| `redis.consumer.group` | string | `surgewave` | Consumer group name |
| `redis.consumer.name` | string | auto | Consumer name |
| `poll.interval.ms` | long | `100` | Polling interval |
| `batch.size` | int | `100` | Messages per batch |

### Sink Settings

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `topics` | string | Required | Source Surgewave topics |
| `sink.mode` | string | `string` | Mode: `string`, `hash`, `stream` |
| `redis.key.prefix` | string | - | Key prefix for writes |
| `redis.key.field` | string | - | Field to use as key |
| `redis.stream.name` | string | - | Target stream (stream mode) |
| `redis.ttl.seconds` | int | `0` | TTL for keys (0 = no expiry) |
| `batch.size` | int | `100` | Operations per pipeline |

## Source Modes

### Stream Mode (Recommended)

Read from Redis Streams with consumer groups:

```json
{
  "source.mode": "stream",
  "redis.stream.name": "events",
  "redis.consumer.group": "surgewave-processors",
  "redis.consumer.name": "processor-1"
}
```

Features:
- Exactly-once semantics with acknowledgments
- Consumer group load balancing
- Automatic offset tracking
- Dead letter handling

### Pub/Sub Mode

Subscribe to Redis channels:

```json
{
  "source.mode": "pubsub",
  "redis.channel.pattern": "notifications:*"
}
```

Pattern matching:
- `channel` - Exact channel name
- `channel:*` - Wildcard matching

**Note:** Pub/Sub is fire-and-forget, messages are lost if connector is down.

## Sink Modes

### String Mode

Store as Redis strings (SET):

```json
{
  "sink.mode": "string",
  "redis.key.prefix": "user:",
  "redis.key.field": "userId",
  "redis.ttl.seconds": "3600"
}
```

Result: `SET user:123 "{...}" EX 3600`

### Hash Mode

Store as Redis hashes (HSET):

```json
{
  "sink.mode": "hash",
  "redis.key.prefix": "session:",
  "redis.key.field": "sessionId"
}
```

Result: `HSET session:abc field1 value1 field2 value2`

### Stream Mode

Append to Redis Streams (XADD):

```json
{
  "sink.mode": "stream",
  "redis.stream.name": "output-events"
}
```

Result: `XADD output-events * field1 value1 field2 value2`

## Examples

### Session Cache Sync

Keep Redis cache in sync:

```json
{
  "name": "session-cache",
  "config": {
    "connector.class": "RedisSinkConnector",
    "redis.connection.string": "redis-cluster.example.com:6379",
    "redis.password": "secret",
    "topics": "user-sessions",
    "sink.mode": "hash",
    "redis.key.prefix": "session:",
    "redis.key.field": "sessionId",
    "redis.ttl.seconds": "7200"
  }
}
```

### Event Stream Processing

Read events from Redis Stream:

```json
{
  "name": "event-processor",
  "config": {
    "connector.class": "RedisSourceConnector",
    "redis.connection.string": "localhost:6379",
    "redis.stream.name": "order-events",
    "redis.consumer.group": "order-processors",
    "topic": "orders",
    "source.mode": "stream",
    "batch.size": "50"
  }
}
```

### Pub/Sub Bridge

Bridge Redis Pub/Sub to Surgewave:

```json
{
  "name": "pubsub-bridge",
  "config": {
    "connector.class": "RedisSourceConnector",
    "redis.connection.string": "localhost:6379",
    "redis.channel.pattern": "notifications:*",
    "topic": "redis-notifications",
    "source.mode": "pubsub"
  }
}
```

### Redis Cluster

```json
{
  "redis.connection.string": "redis1:6379,redis2:6379,redis3:6379"
}
```

### Redis Sentinel

```json
{
  "redis.connection.string": "sentinel1:26379,sentinel2:26379,sentinel3:26379,serviceName=mymaster"
}
```

## Consumer Groups

### Create Consumer Group

```bash
redis-cli XGROUP CREATE events surgewave-group $ MKSTREAM
```

### Monitor Consumer Group

```bash
redis-cli XINFO GROUPS events
redis-cli XINFO CONSUMERS events surgewave-group
```

### Pending Messages

```bash
# View pending messages
redis-cli XPENDING events surgewave-group

# Claim stuck messages
redis-cli XCLAIM events surgewave-group consumer-1 3600000 message-id
```

## Pipelining

The connector uses Redis pipelining for high throughput:

```json
{
  "batch.size": "100"
}
```

Operations are batched and sent in single round-trip.

## Troubleshooting

### Common Issues

**Connection Refused**
- Verify Redis is running
- Check firewall rules
- Ensure bind address allows external connections

**Authentication Failed**
- Verify password in `redis.password`
- Check Redis ACL configuration
- Ensure user has required permissions

**Consumer Group Errors**
- Create consumer group before starting: `XGROUP CREATE`
- Check stream exists: `EXISTS stream-name`
- Verify group doesn't already exist

### Performance Tuning

```json
{
  "batch.size": "200",
  "poll.interval.ms": "50"
}
```

### Monitoring

```bash
# Check connector status
surgewave connect status redis-source

# Redis monitoring
redis-cli MONITOR
redis-cli INFO clients
```

## See Also

- [MQTT Connector](mqtt.md)
- [HTTP Webhook Connector](http.md)
- [Custom Connectors](custom-connectors.md)
