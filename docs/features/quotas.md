# Quotas

Rate limiting for producers and consumers.

## Overview

Quotas control:
- **Producer rate** - Bytes per second
- **Consumer rate** - Bytes per second
- **Burst capacity** - Allow temporary spikes

## Configuration

### Enable Quotas

```json
{
  "Surgewave": {
    "Quotas": {
      "Enabled": true,
      "ProducerBytesPerSecond": 10485760,
      "ConsumerBytesPerSecond": 52428800,
      "ProducerBurstBytes": 20971520,
      "ConsumerBurstBytes": 104857600,
      "MaxThrottleTimeMs": 30000,
      "ClientInactivityTimeoutMs": 300000
    }
  }
}
```

### Default Settings

| Setting | Default | Description |
|---------|---------|-------------|
| `ProducerBytesPerSecond` | 10 MB/s | Producer rate limit |
| `ConsumerBytesPerSecond` | 50 MB/s | Consumer rate limit |
| `ProducerBurstBytes` | 20 MB | Producer burst capacity |
| `ConsumerBurstBytes` | 100 MB | Consumer burst capacity |

## CLI Usage

### Describe Quotas

```bash
surgewave quotas describe
surgewave quotas describe --user alice
```

### Set Quotas

```bash
# Per-user quota
surgewave quotas set --user alice --producer-rate 10485760

# Per-client quota
surgewave quotas set --client-id my-app --consumer-rate 52428800
```

## Token Bucket Algorithm

Quotas use token bucket for rate limiting:

```
┌─────────────────────────────────────────────────────────────┐
│                    Token Bucket                              │
├─────────────────────────────────────────────────────────────┤
│  Bucket Capacity: burst_bytes                                │
│  Refill Rate: bytes_per_second                               │
│                                                              │
│  Request arrives:                                            │
│    If tokens >= request_size:                                │
│      Consume tokens, proceed                                 │
│    Else:                                                     │
│      Calculate throttle_time                                 │
│      Delay response                                          │
└─────────────────────────────────────────────────────────────┘
```

## Throttling Behavior

When quota exceeded:

1. Server calculates throttle time
2. Response includes `throttle_time_ms`
3. Client waits before next request
4. Producer backs off automatically

## Client Handling

### Producer

```csharp
var producer = new SurgewaveProducer<string, string>(options =>
{
    options.BootstrapServers = "localhost:9092";
    // Producer automatically handles throttling
});

// If throttled, producer waits before sending
await producer.ProduceAsync("topic", "key", "value");
```

### Consumer

```csharp
// Consumer automatically respects throttle
while (!cancellationToken.IsCancellationRequested)
{
    var record = await consumer.ConsumeAsync(cancellationToken);
    if (record != null)
        Process(record);
    // Fetch rate limited by broker
}
```

## Quota Types

### User Quotas

Apply to authenticated users:

```bash
surgewave quotas set --user alice --producer-rate 5242880
surgewave quotas set --user bob --consumer-rate 26214400
```

### Client ID Quotas

Apply to specific client IDs:

```bash
surgewave quotas set --client-id producer-1 --producer-rate 10485760
```

### Default Quotas

Apply when no specific quota matches:

```json
{
  "Quotas": {
    "ProducerBytesPerSecond": 10485760,
    "ConsumerBytesPerSecond": 52428800
  }
}
```

## Monitoring

Metrics for quota enforcement:

| Metric | Description |
|--------|-------------|
| `surgewave_quota_throttle_time_ms` | Total throttle time |
| `surgewave_quota_violations_total` | Quota violations |
| `surgewave_quota_bytes_in` | Bytes in (producer) |
| `surgewave_quota_bytes_out` | Bytes out (consumer) |

## Best Practices

1. **Set burst capacity** - Allow temporary spikes
2. **Monitor throttling** - Watch throttle time metrics
3. **Size for peaks** - Consider peak vs average load
4. **Use per-user quotas** - For multi-tenant environments

## Next Steps

- [Compaction](compaction.md) - Log cleanup
- [Security](../security/index.md) - User authentication
