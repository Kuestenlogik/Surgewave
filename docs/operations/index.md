# Operations Guide

Guides for operating Surgewave in production environments.

## Topics

| Guide | Description |
|-------|-------------|
| [Troubleshooting](troubleshooting.md) | Common issues and solutions |
| [Maintenance](maintenance.md) | Routine maintenance tasks |
| [Backup & Recovery](backup.md) | Data protection strategies |

## Quick Health Check

```bash
# Check broker status
surgewave health

# Detailed diagnostics
surgewave health --verbose

# Check specific components
surgewave health --component storage
surgewave health --component clustering
```

## Common Operations

### Graceful Shutdown

```bash
# Stop broker gracefully (completes in-flight requests)
surgewave broker stop --graceful

# Force stop (immediate)
surgewave broker stop --force
```

### Log Management

```bash
# View recent logs
surgewave logs --tail 100

# Filter by level
surgewave logs --level error --since 1h

# Follow logs
surgewave logs --follow
```

### Performance Monitoring

```bash
# Real-time metrics
surgewave metrics

# Specific metric categories
surgewave metrics --category producer
surgewave metrics --category consumer
surgewave metrics --category storage
```
