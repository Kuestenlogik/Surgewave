# Google Cloud Storage Connector

The Google Cloud Storage (GCS) connector enables streaming data between Surgewave and GCS buckets.

## Overview

- **Source**: Poll GCS buckets for new objects and stream their contents to Surgewave topics
- **Sink**: Write Surgewave records to GCS as objects with configurable partitioning

**Use Cases:**
- BigQuery data lake ingestion
- GCP-native log aggregation
- Cross-cloud data replication
- Compliance archival

## Quick Start

### GCS Source Connector

Read objects from GCS and produce to Surgewave:

```json
{
  "name": "gcs-source",
  "config": {
    "connector.class": "GcsSourceConnector",
    "gcs.bucket.name": "my-data-bucket",
    "gcs.prefix": "incoming/",
    "gcs.project.id": "my-project",
    "topic": "gcs-events",
    "format": "jsonlines"
  }
}
```

### GCS Sink Connector

Write Surgewave records to GCS:

```json
{
  "name": "gcs-sink",
  "config": {
    "connector.class": "GcsSinkConnector",
    "gcs.bucket.name": "my-archive-bucket",
    "gcs.prefix": "events/",
    "gcs.project.id": "my-project",
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
| `gcs.bucket.name` | string | Required | GCS bucket name |
| `gcs.project.id` | string | - | GCP project ID (uses default if empty) |
| `gcs.credentials.json` | password | - | Service account JSON (inline) |
| `gcs.credentials.file` | string | - | Path to service account JSON file |
| `gcs.prefix` | string | - | Object prefix filter |

### Source Settings

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `topic` | string | Required | Destination Surgewave topic |
| `format` | string | `json` | Format: `json`, `jsonlines`, `csv`, `raw` |
| `poll.interval.ms` | long | `10000` | Polling interval |
| `delete.after.read` | bool | `false` | Delete objects after processing |

### Sink Settings

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `topics` | string | Required | Source Surgewave topics |
| `format` | string | `json` | Format: `json`, `jsonlines` |
| `partitioner` | string | `default` | Partitioning: `default`, `time`, `field` |
| `flush.size` | int | `1000` | Records per object before flush |
| `rotate.interval.ms` | long | `3600000` | Max time before rotation |

## Authentication

### Application Default Credentials (Recommended)

When running on GCP (GCE, GKE, Cloud Run), ADC automatically uses the attached service account:

```json
{
  "gcs.bucket.name": "my-bucket",
  "gcs.project.id": "my-project"
}
```

For local development, set up ADC:
```bash
gcloud auth application-default login
```

### Service Account JSON (Inline)

Embed credentials directly in configuration:

```json
{
  "gcs.bucket.name": "my-bucket",
  "gcs.credentials.json": "{\"type\":\"service_account\",\"project_id\":\"my-project\",...}"
}
```

### Service Account JSON File

Reference a credentials file:

```json
{
  "gcs.bucket.name": "my-bucket",
  "gcs.credentials.file": "/path/to/service-account.json"
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
| `jsonlines` | `text/plain` |

## Partitioning Strategies

### Default Partitioner
```
gs://bucket/prefix/topic/partition/timestamp.json
```

### Time Partitioner
```
gs://bucket/prefix/topic/year=2024/month=01/day=15/hour=10/timestamp.json
```

### Field Partitioner

Partitions by extracting `id` or `key` field from records.

## Examples

### BigQuery Data Lake

Stage data in GCS for BigQuery loading:

```json
{
  "name": "bigquery-staging",
  "config": {
    "connector.class": "GcsSinkConnector",
    "gcs.bucket.name": "bigquery-staging",
    "gcs.prefix": "events/",
    "gcs.project.id": "analytics-prod",
    "topics": "user-events",
    "format": "jsonlines",
    "partitioner": "time",
    "flush.size": "10000",
    "rotate.interval.ms": "3600000"
  }
}
```

### Multi-Cloud Replication

Read from GCS and process in Surgewave:

```json
{
  "name": "gcs-ingestion",
  "config": {
    "connector.class": "GcsSourceConnector",
    "gcs.bucket.name": "shared-data",
    "gcs.prefix": "exports/",
    "gcs.credentials.file": "/secrets/gcp-sa.json",
    "topic": "gcs-imports",
    "format": "jsonlines",
    "poll.interval.ms": "60000"
  }
}
```

### Local Development

Use a GCS emulator or test with real credentials:

```bash
# Set up ADC for local development
gcloud auth application-default login

# Create test bucket
gsutil mb gs://test-bucket
```

```json
{
  "name": "local-gcs-test",
  "config": {
    "connector.class": "GcsSinkConnector",
    "gcs.bucket.name": "test-bucket",
    "gcs.project.id": "my-dev-project",
    "topics": "test-topic",
    "format": "json"
  }
}
```

## IAM Permissions

### Minimum Required Roles

**For Source Connector:**
- `roles/storage.objectViewer` - Read objects
- `roles/storage.objectAdmin` - If using `delete.after.read`

**For Sink Connector:**
- `roles/storage.objectCreator` - Create objects
- `roles/storage.objectAdmin` - Recommended for overwrites

### Custom IAM Policy

```json
{
  "bindings": [
    {
      "role": "roles/storage.objectAdmin",
      "members": ["serviceAccount:surgewave-connector@project.iam.gserviceaccount.com"]
    }
  ]
}
```

## Troubleshooting

### Common Issues

**Permission Denied (403)**
- Verify service account has required IAM roles
- Check bucket-level permissions
- Ensure project ID is correct

**Bucket Not Found (404)**
- Verify bucket name (globally unique, lowercase)
- Check project ID matches bucket's project
- Ensure bucket exists before starting connector

**Credential Errors**
- For ADC: Run `gcloud auth application-default login`
- For service account: Verify JSON is valid and not expired
- Check `GOOGLE_APPLICATION_CREDENTIALS` environment variable

### Bucket Creation

```bash
# Create bucket
gsutil mb -p PROJECT_ID -l REGION gs://BUCKET_NAME

# Grant access to service account
gsutil iam ch serviceAccount:SA@PROJECT.iam.gserviceaccount.com:objectAdmin gs://BUCKET_NAME
```

## See Also

- [AWS S3 Connector](s3.md)
- [Azure Blob Storage Connector](azure-blob.md)
- [Custom Connectors](custom-connectors.md)
