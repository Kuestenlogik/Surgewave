# Azure Blob Storage Connector

The Azure Blob Storage connector enables streaming data between Surgewave and Azure Blob Storage, with support for the Azurite emulator for local development.

## Overview

- **Source**: Poll Azure containers for new blobs and stream their contents to Surgewave topics
- **Sink**: Write Surgewave records to Azure blobs with configurable partitioning

**Use Cases:**
- Azure data lake integration
- Log archival to Azure Storage
- Cross-cloud data replication
- Backup and compliance storage

## Quick Start

### Azure Blob Source Connector

Read blobs from Azure and produce to Surgewave:

```json
{
  "name": "azure-blob-source",
  "config": {
    "connector.class": "AzureBlobSourceConnector",
    "azure.storage.connection.string": "DefaultEndpointsProtocol=https;AccountName=mystorageaccount;AccountKey=...",
    "azure.storage.container.name": "incoming-data",
    "azure.blob.prefix": "events/",
    "topic": "azure-events",
    "format": "jsonlines"
  }
}
```

### Azure Blob Sink Connector

Write Surgewave records to Azure:

```json
{
  "name": "azure-blob-sink",
  "config": {
    "connector.class": "AzureBlobSinkConnector",
    "azure.storage.connection.string": "DefaultEndpointsProtocol=https;AccountName=mystorageaccount;AccountKey=...",
    "azure.storage.container.name": "archived-events",
    "azure.blob.prefix": "data/",
    "topics": "events,logs",
    "format": "jsonlines",
    "flush.size": "1000"
  }
}
```

## Configuration Reference

### Connection Settings

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `azure.storage.connection.string` | password | - | Full connection string |
| `azure.storage.account.name` | string | - | Storage account name |
| `azure.storage.account.key` | password | - | Storage account key |
| `azure.storage.container.name` | string | Required | Container name |
| `azure.storage.endpoint` | string | - | Custom endpoint (for Azurite) |

### Source Settings

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `topic` | string | Required | Destination Surgewave topic |
| `azure.blob.prefix` | string | - | Blob name prefix filter |
| `format` | string | `json` | Format: `json`, `jsonlines`, `csv`, `raw` |
| `poll.interval.ms` | long | `10000` | Polling interval |
| `delete.after.read` | bool | `false` | Delete blobs after processing |

### Sink Settings

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `topics` | string | Required | Source Surgewave topics |
| `azure.blob.prefix` | string | - | Blob name prefix |
| `format` | string | `json` | Format: `json`, `jsonlines` |
| `partitioner` | string | `default` | Partitioning: `default`, `time`, `field` |
| `flush.size` | int | `1000` | Records per blob before flush |
| `rotate.interval.ms` | long | `3600000` | Max time before rotation |

## Authentication

### Connection String

The simplest method - includes all authentication info:

```json
{
  "azure.storage.connection.string": "DefaultEndpointsProtocol=https;AccountName=mystorageaccount;AccountKey=abc123...;EndpointSuffix=core.windows.net"
}
```

### Account Name and Key

Separate account credentials:

```json
{
  "azure.storage.account.name": "mystorageaccount",
  "azure.storage.account.key": "abc123..."
}
```

### Managed Identity (Azure-hosted)

When running in Azure (App Service, Functions, AKS), use managed identity by specifying only the account name:

```json
{
  "azure.storage.account.name": "mystorageaccount",
  "azure.storage.container.name": "my-container"
}
```

## Local Development with Azurite

Azurite is Microsoft's official Azure Storage emulator.

### Start Azurite

```bash
# Using npm
npm install -g azurite
azurite --silent --location ./azurite-data

# Using Docker
docker run -p 10000:10000 -p 10001:10001 -p 10002:10002 \
  mcr.microsoft.com/azure-storage/azurite
```

### Connector Configuration

```json
{
  "name": "azurite-sink",
  "config": {
    "connector.class": "AzureBlobSinkConnector",
    "azure.storage.connection.string": "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1",
    "azure.storage.container.name": "test-container",
    "topics": "test-topic"
  }
}
```

## Format Support

### Source Formats

| Format | Description |
|--------|-------------|
| `json` | JSON array or single object |
| `jsonlines` | Newline-delimited JSON |
| `csv` | CSV with header row |
| `raw` | Raw bytes as single record |

### Sink Formats

| Format | Content-Type |
|--------|--------------|
| `json` | `application/json` |
| `jsonlines` | `application/x-ndjson` |

## Partitioning Strategies

### Default Partitioner
```
container/prefix/topic/partition/timestamp.json
```

### Time Partitioner
```
container/prefix/topic/year=2024/month=01/day=15/hour=10/timestamp.json
```

### Field Partitioner

Partitions by a field value extracted from records.

## Examples

### Event Archival

Archive events with daily partitioning:

```json
{
  "name": "event-archive",
  "config": {
    "connector.class": "AzureBlobSinkConnector",
    "azure.storage.connection.string": "...",
    "azure.storage.container.name": "event-archive",
    "azure.blob.prefix": "events/",
    "topics": "user-events,system-events",
    "format": "jsonlines",
    "partitioner": "time",
    "flush.size": "5000",
    "rotate.interval.ms": "3600000"
  }
}
```

### Log Ingestion

Process logs uploaded to Azure:

```json
{
  "name": "log-ingestion",
  "config": {
    "connector.class": "AzureBlobSourceConnector",
    "azure.storage.connection.string": "...",
    "azure.storage.container.name": "application-logs",
    "azure.blob.prefix": "prod/",
    "topic": "log-events",
    "format": "jsonlines",
    "poll.interval.ms": "30000",
    "delete.after.read": "true"
  }
}
```

## Troubleshooting

### Common Issues

**AuthenticationFailed**
- Verify connection string or account key is correct
- Check if account key has been rotated
- Ensure SAS token hasn't expired (if using SAS)

**ContainerNotFound**
- Container names are case-sensitive and must be lowercase
- Create container before starting connector
- Check for typos in container name

**Slow Performance**
- Increase `flush.size` to batch more records per blob
- Use `jsonlines` format for streaming workloads
- Consider blob tier (Hot vs Cool vs Archive)

### Container Creation

```bash
# Azure CLI
az storage container create \
  --name my-container \
  --account-name mystorageaccount
```

## See Also

- [AWS S3 Connector](s3.md)
- [Google Cloud Storage Connector](gcs.md)
- [Custom Connectors](custom-connectors.md)
