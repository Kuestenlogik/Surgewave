# Monitoring Overview

Observability tools for Surgewave.

## Components

| Component | Purpose |
|-----------|---------|
| [Metrics](metrics.md) | Prometheus-compatible metrics |
| [Tracing](tracing.md) | OpenTelemetry distributed tracing |
| [Dashboard](dashboard.md) | Control UI dashboard with 14 customizable widgets |

## Health Check

```bash
surgewave health
surgewave health -f json
```

Endpoint: `https://localhost:9093/health`

## Quick Setup

### Prometheus

```yaml
scrape_configs:
  - job_name: surgewave
    static_configs:
      - targets: ['localhost:9093']
    metrics_path: /metrics
```

### Grafana Dashboard

Import Surgewave dashboard:
- Dashboard ID: (coming soon)
- Data source: Prometheus

## Key Metrics

| Metric | Description |
|--------|-------------|
| `surgewave_connections_total` | Total connections |
| `surgewave_messages_in_total` | Messages received |
| `surgewave_messages_out_total` | Messages sent |
| `surgewave_request_duration_ms` | Request latency |
| `surgewave_storage_bytes` | Storage usage |

## Alerts

Recommended alerts:

```yaml
groups:
- name: surgewave
  rules:
  - alert: SurgewaveDown
    expr: up{job="surgewave"} == 0
    for: 1m
    labels:
      severity: critical
    annotations:
      summary: Surgewave broker is down

  - alert: UnderReplicatedPartitions
    expr: surgewave_under_replicated_partitions > 0
    for: 5m
    labels:
      severity: warning

  - alert: HighConsumerLag
    expr: surgewave_consumer_lag > 10000
    for: 10m
    labels:
      severity: warning
```

## Next Steps

- [Metrics](metrics.md) - Complete metrics reference
- [Tracing](tracing.md) - Distributed tracing
- [Dashboard](dashboard.md) - Control UI dashboard and widgets
