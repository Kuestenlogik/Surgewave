# Ollama Connector

The Ollama connector enables local LLM inference without external API calls. Run embeddings and completions entirely on-premises using models like Llama 3, Mistral, and Qwen.

## Features

- **Local Inference** - No API keys or external calls required
- **Embeddings Mode** - Generate vectors using nomic-embed-text, mxbai-embed-large
- **Completions Mode** - Process messages through local LLMs (llama3, mistral, qwen2)
- **Privacy-First** - Keep all data on-premises
- **Cost-Free** - No per-token API costs
- **Offline Operations** - Run AI pipelines without internet

## Prerequisites

Install and run Ollama:

```bash
# Install Ollama (Linux/macOS)
curl -fsSL https://ollama.com/install.sh | sh

# Pull embedding model
ollama pull nomic-embed-text

# Pull chat model
ollama pull llama3

# Start Ollama server (if not running as service)
ollama serve
```

## Configuration

### Required Options

| Option | Type | Description |
|--------|------|-------------|
| `topics` | string | Comma-separated list of input topics |

### Mode Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `mode` | string | embeddings | Processing mode: `embeddings` or `completions` |

### Connection Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `ollama.base.url` | string | http://localhost:11434 | Ollama server URL |

### Embeddings Configuration

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `embeddings.model` | string | nomic-embed-text | Model for embeddings |
| `input.field` | string | text | JSON field containing text to embed |
| `output.field` | string | embedding | JSON field for embedding output |

### Completions Configuration

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `completions.model` | string | llama3 | Chat model for completions |
| `system.prompt` | string | Required | System prompt for the model |
| `max.tokens` | int | 256 | Maximum tokens in response |
| `temperature` | double | 0.7 | Temperature (0.0 - 2.0) |
| `input.field` | string | text | JSON field containing input text |
| `output.field` | string | embedding | JSON field for completion output |

### Batching & Retry

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `batch.size` | int | 10 | Records to batch before processing |
| `batch.timeout.ms` | int | 5000 | Max wait time for batch to fill |
| `retry.max` | int | 3 | Maximum retry attempts |
| `retry.backoff.ms` | int | 1000 | Initial backoff between retries |

### Output Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `webhook.url` | string | | Webhook URL to POST results |
| `include.original` | bool | true | Include original fields in output |
| `keep.alive` | string | 5m | Keep model loaded in memory |

## Examples

### Local Embeddings

Generate embeddings using nomic-embed-text:

```json
{
  "name": "local-embeddings",
  "config": {
    "connector.class": "OllamaSinkConnector",
    "ollama.base.url": "http://localhost:11434",
    "topics": "documents",
    "mode": "embeddings",
    "embeddings.model": "nomic-embed-text",
    "input.field": "content",
    "output.field": "embedding",
    "batch.size": "10"
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

### Local Summarization

Summarize articles using Llama 3:

```json
{
  "name": "local-summarizer",
  "config": {
    "connector.class": "OllamaSinkConnector",
    "ollama.base.url": "http://localhost:11434",
    "topics": "articles",
    "mode": "completions",
    "completions.model": "llama3",
    "system.prompt": "Summarize the following article in 2-3 sentences. Be concise.",
    "input.field": "body",
    "output.field": "summary",
    "max.tokens": "150",
    "temperature": "0.3"
  }
}
```

### High-Quality Embeddings

Use mxbai-embed-large for higher quality vectors:

```json
{
  "name": "quality-embeddings",
  "config": {
    "connector.class": "OllamaSinkConnector",
    "topics": "documents",
    "mode": "embeddings",
    "embeddings.model": "mxbai-embed-large",
    "input.field": "text",
    "output.field": "vector"
  }
}
```

### Classification Pipeline

Classify support tickets:

```json
{
  "name": "ticket-classifier",
  "config": {
    "connector.class": "OllamaSinkConnector",
    "topics": "support-tickets",
    "mode": "completions",
    "completions.model": "mistral",
    "system.prompt": "Classify this support ticket into one category: billing, technical, general, urgent. Return only the category name.",
    "input.field": "description",
    "output.field": "category",
    "max.tokens": "10",
    "temperature": "0.0"
  }
}
```

### Remote Ollama Server

Connect to Ollama running on another machine:

```json
{
  "name": "remote-embeddings",
  "config": {
    "connector.class": "OllamaSinkConnector",
    "ollama.base.url": "http://gpu-server:11434",
    "topics": "documents",
    "mode": "embeddings",
    "embeddings.model": "nomic-embed-text"
  }
}
```

## Models

### Embedding Models

| Model | Dimensions | Description |
|-------|------------|-------------|
| `nomic-embed-text` | 768 | Fast, general-purpose embeddings |
| `mxbai-embed-large` | 1024 | Higher quality, larger dimensions |
| `all-minilm` | 384 | Lightweight, fast embeddings |
| `snowflake-arctic-embed` | 1024 | High-quality for search |

### Chat Models

| Model | Parameters | Description |
|-------|------------|-------------|
| `llama3` | 8B | Meta's latest, good balance |
| `llama3:70b` | 70B | Larger, higher quality |
| `mistral` | 7B | Fast, efficient |
| `mixtral` | 8x7B | MoE, high capability |
| `qwen2` | 7B | Strong multilingual |
| `phi3` | 3.8B | Microsoft's small model |
| `gemma2` | 9B | Google's open model |

Pull models before use:
```bash
ollama pull llama3
ollama pull nomic-embed-text
```

## Performance Tuning

### GPU Acceleration

Ollama automatically uses GPU if available. For multi-GPU:

```bash
# Use specific GPU
CUDA_VISIBLE_DEVICES=0 ollama serve

# Check GPU usage
nvidia-smi
```

### Memory Management

Control model memory usage:

```json
{
  "keep.alive": "5m"
}
```

Values:
- `5m` - Keep loaded for 5 minutes (default)
- `0` - Unload immediately after request
- `-1` - Keep loaded indefinitely

### Batch Size

Ollama processes one request at a time, so smaller batches reduce latency:

```json
{
  "batch.size": "10"
}
```

## Error Handling

The connector retries on transient failures:

1. **Connection Refused** - Retried (Ollama may be starting)
2. **Timeout** - Retried with backoff
3. **Model Not Found** - Not retried, logged as error

Configure retry behavior:

```json
{
  "retry.max": "5",
  "retry.backoff.ms": "2000"
}
```

## Ollama vs OpenAI

| Feature | Ollama | OpenAI |
|---------|--------|--------|
| Cost | Free | Per-token |
| Privacy | Data stays local | Data sent to API |
| Internet | Not required | Required |
| Setup | Install Ollama | Get API key |
| Speed | Depends on hardware | Consistent |
| Models | Open-source only | Proprietary + fine-tuned |

**Choose Ollama when:**
- Privacy is critical
- No API costs desired
- Internet unavailable
- Using open-source models

**Choose OpenAI when:**
- Need GPT-4 quality
- No GPU hardware
- Want managed service
- Need latest models

## Troubleshooting

### Ollama Not Running

```bash
# Check if Ollama is running
curl http://localhost:11434/api/tags

# Start Ollama
ollama serve
```

### Model Not Found

```bash
# List available models
ollama list

# Pull missing model
ollama pull nomic-embed-text
```

### Slow Performance

1. Check GPU is being used: `nvidia-smi`
2. Use smaller model (llama3 vs llama3:70b)
3. Reduce `max.tokens` for completions
4. Increase `keep.alive` to avoid model reloading

### Out of Memory

```bash
# Use smaller model
ollama pull phi3

# Or use quantized version
ollama pull llama3:8b-q4_0
```

## See Also

- [OpenAI Connector](openai.md) - Cloud LLM integration
- [Qdrant Connector](qdrant.md) - Store embeddings
- [AI Overview](index.md) - Architecture patterns
