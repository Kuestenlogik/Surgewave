# Prometheus Metrics

Complete metrics reference.

## Endpoint

```
https://localhost:9093/metrics
```

## Connection Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `surgewave_connections_total` | Counter | Total connections |
| `surgewave_connections_active` | Gauge | Active connections |
| `surgewave_connections_by_ip` | Gauge | Connections per IP |

## Request Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `surgewave_requests_total` | Counter | Total requests |
| `surgewave_request_duration_ms` | Histogram | Request latency |
| `surgewave_request_errors_total` | Counter | Failed requests |

Labels: `api`, `client_id`

## Produce Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `surgewave_produce_messages_total` | Counter | Messages produced |
| `surgewave_produce_bytes_total` | Counter | Bytes produced |
| `surgewave_produce_latency_ms` | Histogram | Produce latency |
| `surgewave_produce_batch_size` | Histogram | Batch size |

Labels: `topic`

## Consume Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `surgewave_fetch_messages_total` | Counter | Messages fetched |
| `surgewave_fetch_bytes_total` | Counter | Bytes fetched |
| `surgewave_fetch_latency_ms` | Histogram | Fetch latency |
| `surgewave_consumer_lag` | Gauge | Consumer lag |

Labels: `topic`, `group`, `partition`

## Storage Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `surgewave_storage_bytes` | Gauge | Total storage size |
| `surgewave_segments_total` | Gauge | Segment count |
| `surgewave_log_end_offset` | Gauge | Log end offset |

Labels: `topic`, `partition`

## Transaction Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `surgewave_transactions_total` | Counter | Total transactions |
| `surgewave_transaction_commits_total` | Counter | Committed |
| `surgewave_transaction_aborts_total` | Counter | Aborted |
| `surgewave_transaction_duration_ms` | Histogram | Duration |
| `surgewave_transaction_timeouts_total` | Counter | Timeouts |

## Replication Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `surgewave_isr_shrinks_total` | Counter | ISR shrinks |
| `surgewave_isr_expands_total` | Counter | ISR expands |
| `surgewave_under_replicated_partitions` | Gauge | Under-replicated |
| `surgewave_offline_partitions` | Gauge | Offline partitions |
| `surgewave_replica_lag_messages` | Gauge | Replica lag |

Labels: `broker_id`

## Shared Memory Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `surgewave_shm_connections_total` | Counter | SHM connections |
| `surgewave_shm_connections_active` | Gauge | Active SHM connections |
| `surgewave_shm_messages_received_total` | Counter | Messages via SHM |
| `surgewave_shm_messages_sent_total` | Counter | Messages sent SHM |
| `surgewave_shm_request_latency_us` | Histogram | SHM latency |

## Quota Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `surgewave_quota_throttle_time_ms` | Counter | Throttle time |
| `surgewave_quota_violations_total` | Counter | Violations |
| `surgewave_quota_bytes_in` | Counter | Producer bytes |
| `surgewave_quota_bytes_out` | Counter | Consumer bytes |

Labels: `user`, `client_id`

## Example Queries

### Throughput

```promql
# Messages per second
rate(surgewave_produce_messages_total[1m])

# Bytes per second
rate(surgewave_produce_bytes_total[1m])
```

### Latency

```promql
# P99 latency
histogram_quantile(0.99, rate(surgewave_request_duration_ms_bucket[5m]))

# Average latency
rate(surgewave_request_duration_ms_sum[5m]) / rate(surgewave_request_duration_ms_count[5m])
```

### Consumer Lag

```promql
# Total lag
sum(surgewave_consumer_lag) by (group)

# Lag per partition
surgewave_consumer_lag{topic="orders"}
```

## Grafana Dashboard

A pre-built Grafana dashboard is shipped at:

```
deploy/monitoring/grafana-surgewave-dashboard.json
```

### Import Dashboard

1. Open Grafana → Dashboards → Import
2. Upload the JSON file or paste its contents
3. Select your Prometheus datasource
4. Click Import

### Dashboard Sections

| Section | Panels |
|---------|--------|
| Overview | Active connections, topics, partitions, consumer groups, log size, transactions |
| Throughput | Messages/sec, bytes/sec for produce and fetch |
| Latency | P50/P95/P99 produce and fetch latency, request duration |
| Replication | ISR count, replication lag, transaction rate |
| Errors | Error rate, produce errors, throttled requests |

### Variables

The dashboard includes template variables for filtering:
- `instance` - Filter by broker instance
- `topic` - Filter by topic name
- `api` - Filter by API type

## Prometheus Alerts

Pre-configured alert rules are available at:

```
deploy/monitoring/prometheus-alerts.yaml
```

### Setup Alerts

Add to your Prometheus configuration:

```yaml
rule_files:
  - /path/to/prometheus-alerts.yaml
```

### Alert Categories

| Category | Alerts |
|----------|--------|
| Broker Health | `SurgewaveBrokerDown`, `SurgewaveBrokerHighCpuUsage` |
| Connections | `SurgewaveNoActiveConnections`, `SurgewaveConnectionSpike` |
| Replication | `SurgewaveUnderReplicatedPartitions`, `SurgewaveHighReplicationLag` |
| Latency | `SurgewaveHighProduceLatency`, `SurgewaveHighFetchLatency`, `SurgewaveHighRequestLatency` |
| Errors | `SurgewaveHighErrorRate`, `SurgewaveProduceErrors` |
| Consumer Groups | `SurgewaveFrequentRebalances` |
| Transactions | `SurgewaveTransactionTimeouts`, `SurgewaveTransactionFencing`, `SurgewaveHighTransactionAbortRate` |
| Throttling | `SurgewaveClientsThrottled` |
| Shared Memory | `SurgewaveShmHighLatency`, `SurgewaveShmNoTraffic` |

### Alert Severity Levels

- **critical** - Immediate action required (broker down, under-replicated)
- **warning** - Attention needed (high latency, errors)
- **info** - Informational (throttling, disk growth)

## Sample Grafana Panel

```json
{
  "title": "Message Throughput",
  "type": "graph",
  "targets": [
    {
      "expr": "sum(rate(surgewave_messages_produced_total[1m]))",
      "legendFormat": "Produce"
    },
    {
      "expr": "sum(rate(surgewave_messages_fetched_total[1m]))",
      "legendFormat": "Consume"
    }
  ]
}
```

## Next Steps

- [Tracing](tracing.md) - Distributed tracing
- [Performance](../performance/index.md) - Optimization
