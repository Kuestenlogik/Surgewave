# Qdrant Connector

The Qdrant connector stores vector embeddings in Qdrant vector database for similarity search, RAG applications, and semantic retrieval.

## Features

- **Vector Storage** - Store embeddings with metadata payloads
- **Auto-Collection** - Automatically create collections with correct dimensions
- **ID Strategies** - Auto-generate, use message key, or extract from field
- **Batch Upserts** - Efficient batched writes to Qdrant
- **Retry Logic** - Automatic retries with exponential backoff
- **TLS Support** - Secure connections to Qdrant Cloud

## Prerequisites

Run Qdrant locally or use Qdrant Cloud:

```bash
# Docker
docker run -p 6333:6333 -p 6334:6334 qdrant/qdrant

# Or use Qdrant Cloud
# Get API key from https://cloud.qdrant.io
```

## Configuration

### Required Options

| Option | Type | Description |
|--------|------|-------------|
| `topics` | string | Comma-separated list of input topics |
| `collection` | string | Qdrant collection name |

### Connection Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `qdrant.host` | string | localhost | Qdrant server hostname |
| `qdrant.port` | int | 6334 | Qdrant gRPC port |
| `qdrant.https` | bool | false | Use HTTPS/TLS |
| `qdrant.api.key` | string | | API key (for Qdrant Cloud) |

### Collection Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `collection.create` | bool | true | Auto-create collection if missing |
| `vector.size` | int | 1536 | Vector dimensions |
| `distance.metric` | string | cosine | Distance: `cosine`, `euclid`, `dot` |

### Field Mapping

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `vector.field` | string | embedding | JSON field containing vector |
| `id.field` | string | | Field for point ID (with `id.strategy=field`) |
| `id.strategy` | string | auto | ID generation: `auto`, `field`, `key` |
| `payload.fields` | string | | Comma-separated fields for payload |

### Batching & Retry

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `batch.size` | int | 100 | Records to batch before upsert |
| `retry.max` | int | 3 | Maximum retry attempts |
| `retry.backoff.ms` | int | 1000 | Initial backoff between retries |

## Examples

### Basic Vector Storage

Store embeddings from OpenAI connector:

```json
{
  "name": "store-embeddings",
  "config": {
    "connector.class": "QdrantSinkConnector",
    "qdrant.host": "localhost",
    "qdrant.port": "6334",
    "topics": "embeddings",
    "collection": "documents",
    "vector.field": "embedding",
    "vector.size": "1536"
  }
}
```

Input message (from OpenAI connector):
```json
{
  "id": "doc-123",
  "title": "Surgewave Overview",
  "content": "Surgewave is a high-performance...",
  "embedding": [0.023, -0.041, 0.018, ...]
}
```

### With Payload Fields

Store metadata alongside vectors:

```json
{
  "name": "vectors-with-metadata",
  "config": {
    "connector.class": "QdrantSinkConnector",
    "qdrant.host": "localhost",
    "topics": "embeddings",
    "collection": "documents",
    "vector.field": "embedding",
    "vector.size": "1536",
    "payload.fields": "title,source,timestamp,category",
    "id.strategy": "field",
    "id.field": "id"
  }
}
```

Stored in Qdrant:
```json
{
  "id": "doc-123",
  "vector": [0.023, -0.041, 0.018, ...],
  "payload": {
    "title": "Surgewave Overview",
    "source": "docs",
    "timestamp": "2024-01-15T10:30:00Z",
    "category": "technical"
  }
}
```

### Qdrant Cloud

Connect to Qdrant Cloud with API key:

```json
{
  "name": "qdrant-cloud",
  "config": {
    "connector.class": "QdrantSinkConnector",
    "qdrant.host": "abc123.us-east-1.aws.cloud.qdrant.io",
    "qdrant.port": "6334",
    "qdrant.https": "true",
    "qdrant.api.key": "${QDRANT_API_KEY}",
    "topics": "embeddings",
    "collection": "production-docs",
    "vector.size": "1536"
  }
}
```

### Using Message Key as ID

Use Kafka message key for point ID:

```json
{
  "name": "key-based-ids",
  "config": {
    "connector.class": "QdrantSinkConnector",
    "qdrant.host": "localhost",
    "topics": "embeddings",
    "collection": "documents",
    "vector.field": "embedding",
    "id.strategy": "key"
  }
}
```

### High-Dimensional Vectors

Store text-embedding-3-large vectors (3072 dimensions):

```json
{
  "name": "large-vectors",
  "config": {
    "connector.class": "QdrantSinkConnector",
    "qdrant.host": "localhost",
    "topics": "large-embeddings",
    "collection": "documents-hd",
    "vector.field": "vector",
    "vector.size": "3072",
    "distance.metric": "cosine"
  }
}
```

### Euclidean Distance

Use Euclidean distance for image embeddings:

```json
{
  "name": "image-vectors",
  "config": {
    "connector.class": "QdrantSinkConnector",
    "qdrant.host": "localhost",
    "topics": "image-embeddings",
    "collection": "images",
    "vector.size": "512",
    "distance.metric": "euclid"
  }
}
```

## Complete RAG Pipeline

End-to-end pipeline from documents to searchable vectors:

```json
{
  "connectors": [
    {
      "name": "generate-embeddings",
      "config": {
        "connector.class": "OpenAISinkConnector",
        "openai.api.key": "${OPENAI_API_KEY}",
        "mode": "embeddings",
        "embeddings.model": "text-embedding-3-small",
        "topics": "raw-documents",
        "input.field": "content",
        "output.field": "embedding",
        "webhook.url": "http://localhost:8080/to-qdrant"
      }
    },
    {
      "name": "store-in-qdrant",
      "config": {
        "connector.class": "QdrantSinkConnector",
        "qdrant.host": "localhost",
        "topics": "embeddings",
        "collection": "documents",
        "vector.field": "embedding",
        "vector.size": "1536",
        "payload.fields": "title,content,source,timestamp",
        "id.strategy": "field",
        "id.field": "doc_id"
      }
    }
  ]
}
```

## ID Strategies

### Auto (Default)

Generate UUID for each point:

```json
{ "id.strategy": "auto" }
```

### Field

Extract ID from message field:

```json
{
  "id.strategy": "field",
  "id.field": "document_id"
}
```

### Key

Use Kafka message key:

```json
{ "id.strategy": "key" }
```

## Distance Metrics

| Metric | Use Case |
|--------|----------|
| `cosine` | Text embeddings (default) |
| `euclid` | Image embeddings, geographic data |
| `dot` | When vectors are normalized |

## Error Handling

The connector implements automatic retry:

1. **Connection Errors** - Retried with backoff
2. **Timeout** - Retried with backoff
3. **Invalid Vector Size** - Not retried, logged as error
4. **Collection Not Found** - Created if `collection.create=true`

## Performance Tuning

### Batch Size

Larger batches improve throughput:

```json
{ "batch.size": "500" }
```

### Connection Pooling

Qdrant client maintains connection pool automatically.

### Indexing

For large collections, configure HNSW index in Qdrant:

```bash
# Via Qdrant API
curl -X PATCH 'http://localhost:6333/collections/documents' \
  -H 'Content-Type: application/json' \
  -d '{
    "hnsw_config": {
      "m": 16,
      "ef_construct": 100
    }
  }'
```

## Querying Vectors

After storing vectors, query from your application:

```csharp
using Qdrant.Client;

var client = new QdrantClient("localhost", 6334);

var results = await client.SearchAsync(
    "documents",
    queryVector,
    limit: 10,
    filter: new Filter
    {
        Must = new[]
        {
            new Condition { Field = "category", Match = "technical" }
        }
    }
);
```

## See Also

- [OpenAI Connector](openai.md) - Generate embeddings
- [Ollama Connector](ollama.md) - Local embeddings
- [AI Overview](index.md) - Architecture patterns
- [PostgreSQL pgvector](../connectors/postgresql.md) - Alternative vector storage
