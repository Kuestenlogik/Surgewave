# Maintenance

Routine maintenance tasks for Surgewave deployments.

## Regular Tasks

### Log Rotation

Surgewave automatically rotates log segments based on configuration:

```json
{
  "Surgewave": {
    "LogSegmentBytes": 1073741824,
    "LogRetentionHours": 168
  }
}
```

### Compaction

For compacted topics, check compaction status:

```bash
surgewave logs compaction-status
```

Trigger manual compaction if needed:

```bash
surgewave logs compact --force
```

### Disk Cleanup

Monitor disk usage regularly:

```bash
surgewave broker info
du -sh /var/surgewave/data/topics/*
```

## Broker Maintenance

### Rolling Restart

To restart brokers without downtime:

1. Ensure replication factor >= 2
2. Restart one broker at a time
3. Wait for ISR to recover before next

```bash
# On each broker
surgewave broker shutdown --graceful
# Wait for shutdown
surgewave broker start
# Wait for ISR recovery
surgewave cluster status
```

### Configuration Changes

Most configuration changes require restart:

```bash
# Check current config
surgewave broker config describe

# Modify config file
# Then restart broker
```

Some settings can be changed dynamically:

```bash
surgewave broker config alter --set log.retention.hours=72
```

## Topic Maintenance

### Adding Partitions

Increase parallelism by adding partitions:

```bash
surgewave topics alter my-topic --partitions 6
```

> Note: Partitions can only be increased, not decreased.

### Changing Retention

```bash
# Reduce retention to 1 day
surgewave topics alter-config my-topic --set retention.ms=86400000

# Unlimited retention
surgewave topics alter-config my-topic --delete retention.ms
```

### Deleting Topics

```bash
surgewave topics delete old-topic
```

## Consumer Group Maintenance

### Delete Inactive Groups

```bash
surgewave groups list
surgewave groups delete inactive-group
```

### Reset Offsets

```bash
# Reset to earliest
surgewave groups reset my-group --topic my-topic --to-earliest

# Reset to latest
surgewave groups reset my-group --topic my-topic --to-latest
```

## Cluster Maintenance

### Adding a Broker

1. Configure new broker with unique ID
2. Start the broker
3. Rebalance partitions

```bash
surgewave cluster balance
```

### Removing a Broker

1. Move all partitions off the broker
2. Wait for replication
3. Shutdown the broker

```bash
surgewave cluster balance --exclude broker-3
```

## Scheduled Maintenance

| Task | Frequency | Command |
|------|-----------|---------|
| Health check | Daily | `surgewave health` |
| Disk usage | Weekly | `surgewave broker info` |
| Consumer lag | Weekly | `surgewave groups list` |
| Compaction status | Weekly | `surgewave logs compaction-status` |
| Full backup | Weekly | See [Backup](backup.md) |

## See Also

- [Troubleshooting](troubleshooting.md) - Fix common issues
- [Backup & Recovery](backup.md) - Data protection
- [Monitoring](../monitoring/index.md) - Observability
