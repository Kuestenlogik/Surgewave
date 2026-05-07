# Surgewave Connectors

Surgewave Connect provides a robust framework for streaming data between Surgewave and external systems. With 10+ built-in connectors and an extensible plugin architecture, you can integrate Surgewave with databases, cloud storage, messaging systems, and more.

## Overview

Connectors enable bidirectional data flow:

- **Source Connectors** - Import data from external systems into Surgewave topics
- **Sink Connectors** - Export data from Surgewave topics to external systems

All connectors support:
- Distributed execution across multiple tasks
- Offset tracking for exactly-once semantics
- Configuration validation with sensible defaults
- Graceful error handling and retry logic

## Available Connectors

### Cloud Storage

| Connector | Source | Sink | Description |
|-----------|:------:|:----:|-------------|
| [AWS S3](s3.md) | Yes | Yes | Amazon S3 and S3-compatible storage (MinIO, LocalStack) |
| [Azure Blob Storage](azure-blob.md) | Yes | Yes | Azure Blob Storage with Azurite emulator support |
| [Google Cloud Storage](gcs.md) | Yes | Yes | GCS with service account and ADC authentication |

### Databases

| Connector | Source | Sink | Description |
|-----------|:------:|:----:|-------------|
| [PostgreSQL CDC](postgresql.md) | Yes | Yes | Logical replication with pgoutput plugin |
| [MongoDB](mongodb.md) | Yes | Yes | Change streams and polling modes |
| [Elasticsearch](elasticsearch.md) | Yes | Yes | Bulk indexing with multiple strategies |
| [Generic Database](database.md) | Yes | Yes | Any ADO.NET-compatible database |

### File Formats

| Connector | Source | Sink | Description |
|-----------|:------:|:----:|-------------|
| [CSV](csv.md) | Yes | Yes | RFC 4180 compliant CSV files with rolling output |
| [Parquet](parquet.md) | Yes | Yes | Apache Parquet columnar files with compression |

### Messaging & Integration

| Connector | Source | Sink | Description |
|-----------|:------:|:----:|-------------|
| [MQTT](mqtt.md) | Yes | Yes | MQTT 3.1.1/5.0 with MQTTnet |
| [Redis](redis.md) | Yes | Yes | Streams, Pub/Sub, and key-value modes |
| [HTTP Webhook](http.md) | Yes | Yes | REST APIs and webhook endpoints |

## Quick Start

### 1. List Available Connectors

```bash
surgewave connect plugins list
```

### 2. Create a Connector

Using CLI:
```bash
surgewave connect create my-s3-sink --config s3-sink.json
```

Using REST API:
```bash
curl -X POST https://localhost:9093/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "my-s3-sink",
    "config": {
      "connector.class": "S3SinkConnector",
      "s3.bucket.name": "my-bucket",
      "s3.region": "us-east-1",
      "topics": "events"
    }
  }'
```

### 3. Monitor Status

```bash
surgewave connect status my-s3-sink
```

## Common Configuration

All connectors support these common options:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `connector.class` | string | Required | Connector class name |
| `tasks.max` | int | 1 | Maximum parallel tasks |
| `key.converter` | string | JSON | Key serialization format |
| `value.converter` | string | JSON | Value serialization format |
| `errors.tolerance` | string | none | Error handling: `none`, `all` |
| `errors.log.enable` | bool | false | Log errors to DLQ topic |

## Connector Management

### CLI Commands

```bash
# Lifecycle
surgewave connect create <name> --config <file>
surgewave connect delete <name>
surgewave connect pause <name>
surgewave connect resume <name>
surgewave connect restart <name>

# Status
surgewave connect list
surgewave connect status <name>
surgewave connect describe <name>

# Tasks
surgewave connect tasks list <name>
surgewave connect tasks restart <name> <task-id>
```

### REST API

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/connectors` | GET | List all connectors |
| `/connectors` | POST | Create connector |
| `/connectors/{name}` | GET | Get connector details |
| `/connectors/{name}` | DELETE | Delete connector |
| `/connectors/{name}/config` | GET/PUT | Get or update config |
| `/connectors/{name}/status` | GET | Get status |
| `/connectors/{name}/pause` | PUT | Pause connector |
| `/connectors/{name}/resume` | PUT | Resume connector |
| `/connectors/{name}/restart` | POST | Restart connector |
| `/connector-plugins` | GET | List available plugins |

## Building Custom Connectors

Surgewave's connector framework is fully extensible. You can build custom connectors for any data source or sink.

See the [Custom Connector Development Guide](custom-connectors.md) for:
- Connector architecture and lifecycle
- Implementing source and sink connectors
- Configuration validation
- Offset management
- Testing and deployment

## Next Steps

- [AWS S3 Connector](s3.md) - Cloud storage integration
- [PostgreSQL CDC](postgresql.md) - Database change data capture
- [MQTT Connector](mqtt.md) - IoT messaging
- [Custom Connectors](custom-connectors.md) - Build your own
