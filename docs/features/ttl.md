# Per-Message TTL

Broker-native time-to-live for individual messages.

## Overview

Surgewave supports per-message TTL (time-to-live) via the `surgewave-ttl-ms` header. Messages are stored immediately but filtered from fetch responses after their expiry time has elapsed. This is a broker-native feature that requires no client-side logic -- expired messages are simply invisible to consumers.

Key characteristics:

- **Header-based**: Set `surgewave-ttl-ms` on any message to control its lifetime
- **Per-topic defaults**: Configure a default TTL for topics where all messages should expire
- **Server-side filtering**: The broker filters expired batches during fetch, not on the client
- **Bounded TTL**: A configurable maximum TTL prevents unbounded retention
- **Index-based tracking**: A per-partition sorted index enables efficient expiry lookups

## How It Works

1. A producer sets the `surgewave-ttl-ms` header on a message (e.g., `60000` for 1 minute).
2. On produce, the broker records the message offset and expiry time (`timestamp + ttlMs`) in the `TtlIndex`.
3. On fetch, the `TtlFilter` checks each batch against the index and excludes batches whose TTL has elapsed.
4. A background sweep timer periodically removes expired entries from the index.

```
Producer                    Broker                         Consumer
   |                          |                              |
   |-- Produce (ttl=60s) ---->|                              |
   |                          | Store message                |
   |                          | Record in TtlIndex           |
   |                          |                              |
   |                          |<---- Fetch ------------------|
   |                          | if now < expiry: return msg  |
   |                          | if now >= expiry: skip msg   |
   |                          |------- Response ------------>|
```

## Configuration

### Broker Configuration

Enable TTL globally in `appsettings.json`:

```json
{
  "Surgewave": {
    "Ttl": {
      "Enabled": true,
      "DefaultTtlMs": 0,
      "MaxTtlMs": 604800000,
      "IndexCleanupIntervalMs": 1000
    }
  }
}
```

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `Enabled` | bool | `false` | Enable per-message TTL feature globally |
| `DefaultTtlMs` | long | `0` | Default TTL in ms when no header is present (0 = no default) |
| `MaxTtlMs` | long | `604800000` | Maximum allowed TTL (default: 7 days). Messages exceeding this are capped |
| `IndexCleanupIntervalMs` | int | `1000` | Interval for sweeping expired index entries (default: 1 second) |

### Per-Topic Configuration

Enable TTL for a specific topic:

```bash
surgewave topics create my-topic --config surgewave.ttl.enabled=true
```

Or set a default TTL for the topic so all messages expire even without the header:

```bash
surgewave topics create events --config surgewave.ttl.enabled=true --config surgewave.ttl.default.ms=3600000
```

## Usage

### Producing with TTL

Set the `surgewave-ttl-ms` header on your messages to specify the TTL in milliseconds:

```csharp
using var producer = await SurgewaveClient.Create("localhost:9092")
    .BuildProducerAsync<string, string>();

// Message expires after 60 seconds
await producer.ProduceAsync("my-topic", "key", "value", headers: new Dictionary<string, byte[]>
{
    ["surgewave-ttl-ms"] = BitConverter.GetBytes(60_000L)
});

// Message expires after 5 minutes
await producer.ProduceAsync("my-topic", "key", "value", headers: new Dictionary<string, byte[]>
{
    ["surgewave-ttl-ms"] = BitConverter.GetBytes(300_000L)
});
```

### Consuming with TTL

No special client-side handling is required. Expired messages are filtered at the broker before the fetch response is sent:

```csharp
using var consumer = await SurgewaveClient.Create("localhost:9092")
    .BuildConsumerAsync<string, string>(new ConsumerOptions
    {
        GroupId = "my-group",
        Topics = ["my-topic"]
    });

// Only non-expired messages are returned
await foreach (var result in consumer.ConsumeAsync())
{
    Console.WriteLine($"Key: {result.Key}, Value: {result.Value}");
}
```

## Architecture

### TtlIndex

The `TtlIndex` maintains a per-partition `SortedSet<TtlRecord>` ordered by expiry time. This enables:

- **O(1) existence check** via `HasTtlRecords()` to skip filtering when no TTL messages exist
- **Efficient sweep**: The sorted order allows early termination during cleanup -- once a non-expired entry is found, the rest are also non-expired

### TtlFilter

The `TtlFilter` is a static utility invoked during fetch response construction. It checks each record batch's base offset against the TTL index and excludes batches whose expiry time has passed.

### Max TTL Enforcement

If a producer sends a message with a TTL exceeding `MaxTtlMs`, the broker caps the expiry time to `now + MaxTtlMs`. This prevents messages from lingering indefinitely due to excessively large TTL values.

## Use Cases

- **Session data**: Expire user session records after inactivity timeout
- **Cache invalidation**: Temporary cache entries that should not persist
- **Rate-limited events**: Discard stale sensor readings or telemetry
- **Ephemeral notifications**: Short-lived alerts or status updates
- **Regulatory compliance**: Ensure PII data is automatically purged after a retention window

## Next Steps

- [Dead Letter Queue](dlq.md) - Route failed messages after max retries
- [Log Compaction](compaction.md) - Key-based deduplication
- [Transactions](transactions.md) - Exactly-once semantics
