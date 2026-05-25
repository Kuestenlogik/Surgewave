# OpenAI Connector

The OpenAI connector enables integration with OpenAI's APIs for embedding generation and chat completions. It also supports Azure OpenAI and other OpenAI-compatible providers.

## Features

- **Embeddings Mode** - Generate vector embeddings from text fields
- **Completions Mode** - Process messages through chat/completion API
- **Batching** - Efficient batch processing (up to 2048 inputs for embeddings)
- **Retry Logic** - Automatic retries with exponential backoff
- **Custom Endpoints** - Support for Azure OpenAI and compatible APIs

## Configuration

### Required Options

| Option | Type | Description |
|--------|------|-------------|
| `openai.api.key` | string | OpenAI API key (or set `OPENAI_API_KEY` env var) |
| `topics` | string | Comma-separated list of input topics |

### Mode Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `mode` | string | embeddings | Processing mode: `embeddings` or `completions` |

### Embeddings Configuration

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `embeddings.model` | string | text-embedding-3-small | Model for embeddings |
| `embeddings.dimensions` | int | 0 | Output dimensions (0 = model default) |
| `input.field` | string | text | JSON field containing text to embed |
| `output.field` | string | embedding | JSON field for embedding output |

### Completions Configuration

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `completions.model` | string | gpt-4o-mini | Chat model for completions |
| `system.prompt` | string | Required | System prompt for the model |
| `max.tokens` | int | 256 | Maximum tokens in response |
| `temperature` | double | 0.7 | Temperature (0.0 - 2.0) |
| `input.field` | string | text | JSON field containing input text |
| `output.field` | string | embedding | JSON field for completion output |

### Connection Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `openai.base.url` | string | | Custom base URL (for Azure OpenAI) |
| `openai.organization` | string | | OpenAI organization ID |
| `openai.project` | string | | OpenAI project ID |

### Batching & Retry

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `batch.size` | int | 100 | Records to batch (max 2048 for embeddings) |
| `batch.timeout.ms` | int | 5000 | Max wait time for batch to fill |
| `retry.max` | int | 3 | Maximum retry attempts |
| `retry.backoff.ms` | int | 1000 | Initial backoff between retries |

### Output Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `webhook.url` | string | | Webhook URL to POST results |
| `include.original` | bool | true | Include original fields in output |
| `output.format` | string | merge | Output format: `json` or `merge` |

## Examples

### Embeddings Mode

Generate embeddings for document indexing:

```json
{
  "name": "document-embeddings",
  "config": {
    "connector.class": "OpenAISinkConnector",
    "openai.api.key": "${OPENAI_API_KEY}",
    "topics": "documents",
    "mode": "embeddings",
    "embeddings.model": "text-embedding-3-small",
    "input.field": "content",
    "output.field": "embedding",
    "batch.size": "100"
  }
}
```

Input message:
```json
{
  "id": "doc-123",
  "content": "Surgewave is a high-performance message broker...",
  "metadata": { "source": "docs" }
}
```

Output message:
```json
{
  "id": "doc-123",
  "content": "Surgewave is a high-performance message broker...",
  "metadata": { "source": "docs" },
  "embedding": [0.023, -0.041, 0.018, ...]
}
```

### Completions Mode

Summarize articles using GPT:

```json
{
  "name": "article-summarizer",
  "config": {
    "connector.class": "OpenAISinkConnector",
    "openai.api.key": "${OPENAI_API_KEY}",
    "topics": "articles",
    "mode": "completions",
    "completions.model": "gpt-4o-mini",
    "system.prompt": "Summarize the following article in 2-3 sentences. Be concise and capture the main points.",
    "input.field": "body",
    "output.field": "summary",
    "max.tokens": "150",
    "temperature": "0.3"
  }
}
```

### Azure OpenAI

Connect to Azure OpenAI Service:

```json
{
  "name": "azure-embeddings",
  "config": {
    "connector.class": "OpenAISinkConnector",
    "openai.api.key": "${AZURE_OPENAI_KEY}",
    "openai.base.url": "https://my-resource.openai.azure.com",
    "topics": "documents",
    "mode": "embeddings",
    "embeddings.model": "text-embedding-ada-002"
  }
}
```

### High-Dimensional Embeddings

Use text-embedding-3-large with custom dimensions:

```json
{
  "name": "large-embeddings",
  "config": {
    "connector.class": "OpenAISinkConnector",
    "openai.api.key": "${OPENAI_API_KEY}",
    "topics": "documents",
    "mode": "embeddings",
    "embeddings.model": "text-embedding-3-large",
    "embeddings.dimensions": "3072",
    "input.field": "text",
    "output.field": "vector"
  }
}
```

## Models

### Embedding Models

| Model | Dimensions | Description |
|-------|------------|-------------|
| `text-embedding-3-small` | 1536 | Fast, cost-effective embeddings |
| `text-embedding-3-large` | 3072 | Higher quality, larger dimensions |
| `text-embedding-ada-002` | 1536 | Legacy model (Azure default) |

### Chat Models

| Model | Context | Description |
|-------|---------|-------------|
| `gpt-4o-mini` | 128K | Fast, cost-effective |
| `gpt-4o` | 128K | Latest flagship model |
| `gpt-4-turbo` | 128K | High capability |
| `gpt-3.5-turbo` | 16K | Legacy, fastest |

## Error Handling

The connector implements automatic retry with exponential backoff:

1. **Rate Limits (429)** - Retried with backoff
2. **Server Errors (5xx)** - Retried with backoff
3. **Invalid Requests (400)** - Not retried, logged as error
4. **Auth Errors (401/403)** - Not retried, connector fails

Configure error handling:

```json
{
  "retry.max": "5",
  "retry.backoff.ms": "2000"
}
```

## Best Practices

1. **Use Environment Variables** - Never hardcode API keys in config
2. **Batch Appropriately** - Use batch.size=100 for embeddings, lower for completions
3. **Monitor Costs** - Track token usage via OpenAI dashboard
4. **Choose Right Model** - Use text-embedding-3-small unless you need higher quality
5. **Set Temperature** - Use 0.0-0.3 for factual tasks, higher for creative

## See Also

- [Ollama Connector](ollama.md) - Free, local alternative
- [Qdrant Connector](qdrant.md) - Store embeddings
- [AI Overview](index.md) - Architecture patterns
