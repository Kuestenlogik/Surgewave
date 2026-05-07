# Bandwidth Quotas

Per-client and per-user bandwidth throttling to prevent network saturation.

## Overview

Surgewave supports fine-grained bandwidth quotas that limit how many bytes per second a client can produce or consume. This prevents a single client from monopolizing broker network resources.

Key characteristics:

- **Per-client and per-user**: Set limits by client ID or authenticated user
- **Sliding window measurement**: Uses a `SlidingWindowCounter` with configurable window and bucket granularity
- **Backoff-based throttling**: Excess traffic triggers a calculated delay, not a hard rejection
- **Priority resolution**: User overrides > client overrides > global defaults
- **Runtime management**: Add, update, or remove quotas via REST API without restart

## How It Works

1. A produce or fetch request arrives with a known client ID (and optional user).
2. The `BandwidthQuotaManager` resolves the effective quota (user override > client override > default).
3. The `BandwidthTracker` checks the client's current byte rate against the limit using a `SlidingWindowCounter`.
4. If the rate exceeds the limit, a throttle delay is calculated: `(excess / limit) * ThrottleDelayFactor`.
5. The broker delays the response by the computed amount (capped at 30 seconds).

```
Client                          Broker
  |                               |
  |-- Produce (50 KB) ----------->|
  |                               | BandwidthQuotaManager.CheckAndRecordProduce()
  |                               | SlidingWindowCounter: currentRate = 9.5 MB/s
  |                               | Limit: 10 MB/s => OK, no throttle
  |<-------- Ack -----------------|
  |                               |
  |-- Produce (800 KB) ---------->|
  |                               | currentRate = 10.2 MB/s => THROTTLED
  |                               | delay = (excess / limit) * 1.5 = 30ms
  |        (30ms delay)           |
  |<-------- Ack -----------------|
```

## Configuration

Enable bandwidth quotas in `appsettings.json`:

```json
{
  "Surgewave": {
    "BandwidthQuota": {
      "Enabled": true,
      "DefaultProduceBytesPerSec": 10485760,
      "DefaultConsumeBytesPerSec": 20971520,
      "EnforcementWindowMs": 1000,
      "ThrottleDelayFactor": 1.5,
      "ClientOverrides": {
        "high-priority-producer": {
          "ProduceBytesPerSec": 52428800,
          "ConsumeBytesPerSec": 0
        }
      },
      "UserOverrides": {
        "admin": {
          "ProduceBytesPerSec": 0,
          "ConsumeBytesPerSec": 0
        }
      }
    }
  }
}
```

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `Enabled` | bool | `false` | Enable bandwidth quota enforcement |
| `DefaultProduceBytesPerSec` | long | `0` | Default produce limit (0 = unlimited) |
| `DefaultConsumeBytesPerSec` | long | `0` | Default consume limit (0 = unlimited) |
| `EnforcementWindowMs` | int | `1000` | Sliding window duration in milliseconds |
| `ThrottleDelayFactor` | double | `1.5` | Backoff multiplier for throttle delay |
| `ClientOverrides` | dict | `{}` | Per-client ID quota overrides |
| `UserOverrides` | dict | `{}` | Per-user quota overrides (highest priority) |

## REST API

All endpoints are under `/api/quotas/bandwidth`.

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/quotas/bandwidth/` | List all quotas and current usage |
| `GET` | `/api/quotas/bandwidth/{clientId}` | Get usage for a specific client |
| `PUT` | `/api/quotas/bandwidth/client/{clientId}` | Set client quota override |
| `PUT` | `/api/quotas/bandwidth/user/{user}` | Set user quota override |
| `DELETE` | `/api/quotas/bandwidth/client/{clientId}` | Remove client quota override |
| `GET` | `/api/quotas/bandwidth/metrics` | Aggregate throttling metrics |
| `GET` | `/api/quotas/bandwidth/config` | Current configuration |

### Set a Client Quota

```bash
curl -X PUT http://localhost:9092/api/quotas/bandwidth/client/my-producer \
  -H "Content-Type: application/json" \
  -d '{"produceBytesPerSec": 5242880, "consumeBytesPerSec": 0}'
```

### Check Usage

```bash
curl http://localhost:9092/api/quotas/bandwidth/my-producer
```

Response:

```json
{
  "clientId": "my-producer",
  "produceBytesPerSec": 4800000,
  "consumeBytesPerSec": 0,
  "produceLimitBytesPerSec": 5242880,
  "consumeLimitBytesPerSec": 0,
  "produceUtilizationPercent": 91.6,
  "consumeUtilizationPercent": 0,
  "isThrottled": false,
  "lastActivityAt": "2026-03-19T10:15:00Z"
}
```

## Architecture

### SlidingWindowCounter

A lock-free, fixed-size bucket array where each bucket tracks bytes for a time slice (default: 100ms per bucket, 10 buckets = 1s window). Uses `Interlocked` operations for thread safety.

### BandwidthTracker

Maintains a `ConcurrentDictionary<string, ClientBandwidthState>` with per-client produce and consume counters. Automatically cleans up inactive clients after 10 minutes.

### BandwidthQuotaManager

The top-level coordinator that resolves effective quotas, delegates rate checks to the tracker, and exposes metrics. Supports runtime override management via the REST API.

## Use Cases

- **Multi-tenant clusters**: Prevent one tenant from consuming all bandwidth
- **Producer throttling**: Limit ingest rate for misbehaving producers
- **Consumer fairness**: Ensure all consumer groups get fair share of fetch bandwidth
- **Cost control**: Cap bandwidth for lower-tier service accounts

## Next Steps

- [Quotas](quotas.md) - Request-rate quotas (messages/sec)
- [Cruise Control](cruise-control.md) - Automatic partition balancing
- [Per-Message TTL](ttl.md) - Time-based message expiration
