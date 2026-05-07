# Log Compaction

Keep only the latest value per key.

## Overview

Log compaction:
- Retains latest record per key
- Removes older duplicate keys
- Preserves ordering within key

## Use Cases

- **Database changelog** - Table snapshots
- **Configuration** - Latest config per key
- **State management** - Event sourcing

## Configuration

### Per-Topic

```bash
surgewave topics create my-topic --config cleanup.policy=compact
```

Or via alter:

```bash
surgewave topics alter-config my-topic --set cleanup.policy=compact
```

### Broker Defaults

```json
{
  "Surgewave": {
    "LogCleanerEnabled": true,
    "LogCleanerMinCleanableRatio": 0.5,
    "LogCleanerDeleteRetentionMs": 86400000
  }
}
```

## How It Works

```
Before Compaction:
┌─────┬─────┬─────┬─────┬─────┬─────┬─────┬─────┐
│ A:1 │ B:1 │ A:2 │ C:1 │ B:2 │ A:3 │ C:2 │ B:3 │
└─────┴─────┴─────┴─────┴─────┴─────┴─────┴─────┘

After Compaction:
┌─────┬─────┬─────┐
│ A:3 │ C:2 │ B:3 │
└─────┴─────┴─────┘
```

## Settings

| Setting | Default | Description |
|---------|---------|-------------|
| `cleanup.policy` | delete | delete, compact, delete,compact, ephemeral |
| `ephemeral.buffer.bytes` | 67108864 | Ring buffer size for ephemeral topics (64MB) |
| `min.cleanable.dirty.ratio` | 0.5 | Min dirty ratio to trigger |
| `delete.retention.ms` | 86400000 | Tombstone retention (24h) |
| `min.compaction.lag.ms` | 0 | Min age before compaction |
| `max.compaction.lag.ms` | ∞ | Max age before forced compaction |

## Tombstones

Delete a key by producing null value:

```csharp
await producer.ProduceAsync("topic", "key-to-delete", null);
```

Tombstones retained for `delete.retention.ms` then removed.

## Cleanup Policies

### Delete Only (Default)

Remove old segments based on retention:

```bash
surgewave topics create logs --config cleanup.policy=delete
```

### Compact Only

Keep latest per key, no time-based deletion:

```bash
surgewave topics create config --config cleanup.policy=compact
```

### Compact + Delete

Compact and also delete old data:

```bash
surgewave topics create events --config cleanup.policy=compact,delete
```

### Ephemeral (Ring-Buffer)

No persistence - messages stored in a fixed-size ring buffer that wraps around:

```bash
surgewave topics create sensor-data --config cleanup.policy=ephemeral --config ephemeral.buffer.bytes=67108864
```

Ephemeral topics use `EphemeralPartitionLog` instead of `PartitionLog`. Messages are only available within the ring-buffer window. When the buffer is full, the oldest messages are evicted and `LogStartOffset` advances.

| Feature | Delete/Compact | Ephemeral |
|---------|---------------|-----------|
| Persistence | Yes | No (ring-buffer only) |
| Survives restart | Yes | No |
| Consumer replay | Full history | Buffer window only |
| Retention/Compaction workers | Active | Skipped |
| Use case | Logs, state stores | Live dashboards, telemetry, cache invalidation |

Configuration:

| Setting | Default | Description |
|---------|---------|-------------|
| `ephemeral.buffer.bytes` | 64 MB | Ring buffer size (supports KB, MB, GB units) |

See also: [Memory Storage](../storage/memory.md#ephemeral-topics) for comparison with memory storage mode.

## Segment Management

Compaction works on closed segments:

```
┌─────────────────────────────────────────────────────────────┐
│ Active Segment (not compacted)                               │
├─────────────────────────────────────────────────────────────┤
│ Closed Segment 2 (eligible for compaction)                   │
├─────────────────────────────────────────────────────────────┤
│ Closed Segment 1 (already compacted)                         │
└─────────────────────────────────────────────────────────────┘
```

## Dirty Ratio

Compaction triggered when:

```
dirty_bytes / total_bytes >= min.cleanable.dirty.ratio
```

Lower ratio = more frequent compaction = fresher data

## Best Practices

1. **Use meaningful keys** - Keys determine deduplication
2. **Set appropriate retention** - For tombstones
3. **Monitor log size** - Compaction reduces size
4. **Consider min lag** - Avoid compacting recent data

## Monitoring

| Metric | Description |
|--------|-------------|
| `surgewave_compaction_rate` | Records compacted/sec |
| `surgewave_compaction_lag` | Oldest uncompacted offset |
| `surgewave_dirty_ratio` | Current dirty ratio |

## Next Steps

- [Storage](../storage/index.md) - Storage backends
- [Clustering](../clustering/index.md) - Replication
