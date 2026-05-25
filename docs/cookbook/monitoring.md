# Recipe: Monitoring with Prometheus & Grafana

End-to-end setup: scrape Surgewave metrics, visualize in Grafana, alert on consumer lag.

---

## 1. Prometheus Endpoint

Surgewave exposes Prometheus metrics at:

```
https://localhost:9093/metrics
```

No configuration required — the endpoint is always enabled.

---

## 2. Prometheus Scrape Config

`prometheus.yml`:

```yaml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: surgewave
    static_configs:
      - targets:
          - localhost:9093    # HTTP/metrics port
    metrics_path: /metrics
    scrape_interval: 10s

  # Service Discovery (optional) — Surgewave publishes targets at /sd-targets
  - job_name: surgewave-sd
    http_sd_configs:
      - url: https://localhost:9093/sd-targets
        refresh_interval: 30s
```

---

## 3. Docker Compose — Broker + Prometheus + Grafana

```yaml
version: "3.9"
services:
  surgewave:
    image: klsurgewave/surgewave:latest
    ports:
      - "9092:9092"   # Kafka protocol
      - "9093:9093"   # HTTP / metrics
    environment:
      Surgewave__BrokerId: "0"
      Surgewave__DataDirectory: "/data"
    volumes:
      - surgewave-data:/data

  prometheus:
    image: prom/prometheus:latest
    ports:
      - "9090:9090"
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml

  grafana:
    image: grafana/grafana:latest
    ports:
      - "3000:3000"
    environment:
      GF_SECURITY_ADMIN_PASSWORD: admin
    volumes:
      - grafana-data:/var/lib/grafana

volumes:
  surgewave-data:
  grafana-data:
```

---

## 4. Key Metrics to Watch

### Throughput

| Metric | Alert Condition | Notes |
|--------|----------------|-------|
| `surgewave_produce_messages_total` | Rate drop > 20% | Production throughput |
| `surgewave_fetch_messages_total` | Rate drop > 20% | Consumption throughput |
| `surgewave_produce_bytes_total` | Baseline + 3σ | Bytes/sec |

### Latency

| Metric | Alert Condition | Notes |
|--------|----------------|-------|
| `surgewave_produce_latency_ms` P99 | > 100ms | Broker-side produce latency |
| `surgewave_fetch_latency_ms` P99 | > 200ms | Broker-side fetch latency |
| `surgewave_request_duration_ms` P99 | > 500ms | All requests |

### Consumer Lag

| Metric | Alert Condition | Notes |
|--------|----------------|-------|
| `surgewave_consumer_lag` | > 10000 | Per topic/group/partition |

### Storage

| Metric | Alert Condition | Notes |
|--------|----------------|-------|
| `surgewave_storage_bytes` | > 80% disk | Total log size |
| `surgewave_segments_total` | Sudden spike | Segment leak indicator |

### Connections

| Metric | Alert Condition | Notes |
|--------|----------------|-------|
| `surgewave_connections_active` | Trend up | Connection leak |
| `surgewave_request_errors_total` | Rate > 0 | Any error |

---

## 5. Consumer Lag Alert — Prometheus Rule

`rules/surgewave.yml`:

```yaml
groups:
  - name: surgewave
    rules:
      - alert: HighConsumerLag
        expr: surgewave_consumer_lag{group="order-processor"} > 10000
        for: 2m
        labels:
          severity: warning
        annotations:
          summary: "Consumer lag too high"
          description: >
            Group {{ $labels.group }} on topic {{ $labels.topic }}
            partition {{ $labels.partition }} has lag {{ $value }}.

      - alert: BrokerDown
        expr: up{job="surgewave"} == 0
        for: 30s
        labels:
          severity: critical
        annotations:
          summary: "Surgewave broker unreachable"

      - alert: HighProduceLatency
        expr: histogram_quantile(0.99, rate(surgewave_produce_latency_ms_bucket[5m])) > 100
        for: 1m
        labels:
          severity: warning
        annotations:
          summary: "P99 produce latency > 100ms"
```

---

## 6. Grafana Dashboard Panels

Import the Surgewave dashboard JSON from `docs/monitoring/dashboard.md` or build manually:

### Panel: Messages/sec

```
PromQL: rate(surgewave_produce_messages_total[1m])
Visualization: Time series
Legend: {{ topic }}
```

### Panel: Consumer Lag Heatmap

```
PromQL: surgewave_consumer_lag
Visualization: Heatmap
Group by: group, topic
```

### Panel: P99 Produce Latency

```
PromQL: histogram_quantile(0.99, rate(surgewave_produce_latency_ms_bucket[5m]))
Visualization: Time series
Unit: ms
```

### Panel: Active Connections

```
PromQL: surgewave_connections_active
Visualization: Stat (current value)
```

---

## 7. OpenTelemetry Export (Alternative)

Surgewave also exports OTEL traces and metrics. Configure in `appsettings.json`:

```json
{
  "Surgewave": {
    "Telemetry": {
      "Enabled": true,
      "OtlpEndpoint": "http://otel-collector:4317",
      "ExportInterval": 10
    }
  }
}
```

---

## See Also

- [Monitoring Overview](../monitoring/index.md)
- [Metrics Reference](../monitoring/metrics.md)
- [Dashboard](../monitoring/dashboard.md)
