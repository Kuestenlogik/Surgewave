# Intent-Based Configuration

Resolve natural-language descriptions into concrete topic configurations.

## Overview

The Intent-Based Configuration engine translates free-form descriptions or keywords into production-ready topic configurations. It uses 16 built-in rules with English and German keywords -- no LLM required. Context-aware adjustments automatically tune settings based on device count, message rate, environment, and data classification.

Key characteristics:

- **16 built-in rules**: High-availability, high-throughput, low-latency, GDPR, IoT, analytics, and more
- **Bilingual keywords**: All rules support both English and German keywords
- **Context-aware**: Device count, message rate, environment, and data classification influence the output
- **Stackable rules**: Multiple matching rules merge their configurations (higher priority wins)
- **Dry-run support**: Resolve an intent without creating the topic
- **REST + Control UI**: Both programmatic and visual access

## How It Works

1. The user submits a description like "high-throughput IoT telemetry for 5000 devices in production".
2. The `IntentConfigEngine` normalizes the text and matches keywords against all 16 rules.
3. Matching rules are sorted by priority and merged: higher-priority rules override lower ones.
4. Context-based adjustments are applied (e.g., device count > 1000 increases partitions to 12).
5. Environment-specific constraints are enforced (production requires replication factor 3, min ISR 2).
6. The result includes the resolved config, applied rules, confidence score, and human-readable explanation.

## Configuration

The engine is built-in and requires no configuration. It is automatically available when the broker starts.

## Built-in Rules

| Rule | Keywords (EN) | Keywords (DE) | Key Settings |
|------|---------------|---------------|--------------|
| high-availability | ha, reliable | hochverfuegbar, ausfallsicher | RF=3, min.isr=2, acks=all |
| high-throughput | fast, bulk, batch | hoher-durchsatz, schnell | 12 partitions, LZ4, batching |
| low-latency | realtime, instant | niedrige-latenz, echtzeit | 1 partition, ack=1, linger=0 |
| gdpr-compliance | gdpr, pii, privacy | dsgvo, datenschutz | 30-day TTL, DLQ enabled |
| iot-edge | iot, sensor, telemetry | geraet, telemetrie | LZ4, 7-day TTL |
| analytics | data-lake, reporting | analyse, warehouse | Compacted, infinite retention |
| temporary | temp, test, debug | temporaer, kurzlebig | 1h retention, RF=1 |
| event-sourcing | audit, ledger, immutable | | Infinite retention |
| financial | payment, transaction | zahlung, bestellung | HA + dedup + all acks |
| chat-messaging | chat, conversation | nachrichten, dialog | 30-day TTL |
| logging | syslog, application-log | protokoll | 6 partitions, Zstd, 7-day |
| machine-learning | ml, ai, prediction | | LZ4 compression |
| metrics | monitoring, tracing | metriken, observability | 6 partitions, LZ4, 3-day |
| cdc | debezium, database-sync | | Compacted, infinite retention |
| notification | alert, webhook, push | benachrichtigung, alarm | Fast delivery, 1-day TTL |
| work-queue | queue, job, worker | warteschlange | All acks, DLQ enabled |

## Context Adjustments

The engine applies additional adjustments based on the `IntentContext`:

| Context | Adjustment |
|---------|------------|
| `ExpectedDeviceCount > 100` | Increase partitions (6/12/24 based on scale) |
| `ExpectedMessagesPerSec > 10000` | LZ4 compression, larger batch size, more partitions |
| `ExpectedMessageSizeBytes > 100KB` | Set max.message.bytes |
| `Environment = "production"` | RF=3, min.isr=2, acks=all |
| `DataClassification = "pii"` | 30-day TTL, DLQ enabled |

## REST API

All endpoints are under `/api/intent`.

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/intent/resolve` | Resolve intent to config (dry-run) |
| `POST` | `/api/intent/create` | Resolve intent AND create the topic |
| `GET` | `/api/intent/keywords` | List all available keywords |
| `GET` | `/api/intent/rules` | List all rules with details |

### Resolve an Intent (Dry-Run)

```bash
curl -X POST http://localhost:9092/api/intent/resolve \
  -H "Content-Type: application/json" \
  -d '{
    "description": "high-throughput IoT telemetry for production",
    "topicName": "iot-telemetry",
    "context": {
      "expectedDeviceCount": 5000,
      "expectedMessagesPerSec": 50000,
      "environment": "production"
    }
  }'
```

Response:

```json
{
  "topicName": "iot-telemetry",
  "partitions": 24,
  "replicationFactor": 3,
  "topicConfig": {
    "compression.type": "lz4",
    "surgewave.ttl.enabled": "true",
    "surgewave.ttl.default-ms": "604800000",
    "min.insync.replicas": "2",
    "acks": "all",
    "batch.size": "65536"
  },
  "confidence": 1.0,
  "explanation": "Matched rules: high-throughput, iot-edge. ...",
  "appliedRules": [...],
  "warnings": []
}
```

### Create Topic from Intent

```bash
curl -X POST http://localhost:9092/api/intent/create \
  -H "Content-Type: application/json" \
  -d '{
    "description": "GDPR-compliant user events",
    "topicName": "user-events"
  }'
```

## Use Cases

- **Self-service topic creation**: Teams describe their needs, the engine generates optimal config
- **Guardrails**: Prevent misconfiguration by deriving settings from intent rather than raw values
- **Onboarding**: New users create topics without deep Kafka/Surgewave knowledge
- **Compliance automation**: PII classification automatically enables TTL and DLQ

## Next Steps

- [Data Mesh](data-mesh.md) - Data product catalog with contracts and SLOs
- [Schema Registry](schema-registry.md) - Schema management
- [Quotas](quotas.md) - Rate limiting
