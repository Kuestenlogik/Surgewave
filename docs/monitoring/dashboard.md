# Control Dashboard

Surgewave Control provides a customizable dashboard with drag-and-drop widgets, theme persistence, and real-time metrics.

## Dashboard Widgets

The dashboard includes 14 widgets organized in three rows:

### Summary Row (Top)

| Widget | Description |
|--------|-------------|
| Cluster Status | Health indicator (Healthy/Unhealthy) |
| Throughput | Messages per second with live updates |
| Brokers | Active broker count |
| Topics | Total topic count |
| Latency | Produce latency P50/P90/P99 bar chart |

### Metrics Row (Second)

| Widget | Description |
|--------|-------------|
| Consumer Groups | Consumer group count |
| Partitions | Total partition count |
| Errors | Error counter (produce + general errors) |
| Pipelines | Active pipeline count |
| Connectors | Active connector count |
| Cluster Information | Detailed cluster info table (cluster ID, controller, totals) |

### Data Row (Bottom)

| Widget | Description |
|--------|-------------|
| Topics Table | Top 5 topics with partition/replication info |
| Consumer Groups Table | Top 5 consumer groups with state and member count |
| Recent Pipelines | Top 5 pipelines with status and node count |

## Widget Customization

### Drag-and-Drop Reordering

Widgets can be rearranged by dragging the handle icon in the top-right corner of each card. Cards can be moved within a row or between rows. The layout is persisted to `localStorage` and restored on reload.

### Widget Visibility Toggle

Click the widget menu icon in the dashboard header to show or hide individual widgets. Hidden widgets are remembered across browser sessions.

### Auto-Refresh

Metrics widgets (throughput, latency, errors) refresh automatically every 5 seconds. The dashboard calculates message throughput by comparing snapshots across intervals.

## Theme Persistence

Surgewave Control supports dark and light themes:

- **System detection**: On first visit, the theme matches the browser's `prefers-color-scheme` setting
- **Manual toggle**: Click the sun/moon icon in the top navigation bar
- **Persistence**: The selected theme is stored in `localStorage` under `surgewave_theme_dark_mode`

The theme applies globally via MudBlazor's `MudThemeProvider`.

## Consumer Lag Monitoring

The Consumer Groups table and detail pages show:

- **Group State** - Stable, PreparingRebalance, CompletingRebalance, Empty, Dead
- **Member Count** - Active consumers in the group
- **Per-partition lag** - Available on the consumer group detail page at `/consumer-groups/{groupId}`

Consumer lag alerts can be configured in Prometheus:

```yaml
- alert: HighConsumerLag
  expr: surgewave_consumer_lag > 10000
  for: 10m
  labels:
    severity: warning
  annotations:
    summary: "Consumer group {{ $labels.group }} has high lag"
```

## Alerting Overview

Surgewave exposes metrics compatible with Prometheus alerting. Recommended alerts:

| Alert | Condition | Severity |
|-------|-----------|----------|
| SurgewaveDown | `up{job="surgewave"} == 0` for 1m | Critical |
| UnderReplicatedPartitions | `surgewave_under_replicated_partitions > 0` for 5m | Warning |
| HighConsumerLag | `surgewave_consumer_lag > 10000` for 10m | Warning |
| HighProduceLatency | `surgewave_request_duration_ms{quantile="0.99"} > 100` for 5m | Warning |
| HighErrorRate | `rate(surgewave_errors_total[5m]) > 10` for 5m | Warning |

See the [Monitoring Overview](index.md) for Prometheus scrape configuration and Grafana dashboard setup.

## Next Steps

- [Metrics Reference](metrics.md) - Complete Prometheus metrics reference
- [OpenTelemetry](tracing.md) - Distributed tracing setup
