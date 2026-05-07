# AI & LLM Integration

Surgewave provides native connectors for AI/LLM workloads, enabling real-time AI pipelines, RAG (Retrieval-Augmented Generation) systems, and multi-agent architectures.

## Overview

Surgewave's AI connectors enable:

- **Embedding Generation** - Convert text to vectors using OpenAI or local models (Ollama)
- **LLM Processing** - Enrich messages with AI-generated content
- **Vector Storage** - Store embeddings in vector databases (Qdrant, pgvector)
- **Agent Communication** - Event-driven multi-agent architectures

## Available Connectors

### LLM Providers

| Connector | Embeddings | Completions | Description |
|-----------|:----------:|:-----------:|-------------|
| [OpenAI](openai.md) | Yes | Yes | OpenAI API and compatible providers (Azure OpenAI) |
| [Ollama](ollama.md) | Yes | Yes | Local LLM inference with no API costs |

### Vector Databases

| Connector | Description |
|-----------|-------------|
| [Qdrant](qdrant.md) | High-performance vector database with filtering |
| [PostgreSQL pgvector](../connectors/postgresql.md) | Vector extension for PostgreSQL |

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Surgewave Cluster                             │
├─────────────────┬─────────────────┬─────────────────────────────┤
│  documents      │   embeddings    │      enriched-docs          │
│    topic        │     topic       │         topic               │
└────────┬────────┴────────┬────────┴──────────────┬──────────────┘
         │                 │                       │
         ▼                 ▼                       ▼
   ┌───────────┐    ┌───────────┐          ┌───────────┐
   │  OpenAI   │    │  Qdrant   │          │   App     │
   │ Connector │───▶│ Connector │          │  Consumer │
   │(embeddings)│    │ (vectors) │          │           │
   └───────────┘    └───────────┘          └───────────┘
```

## Use Cases

### RAG Pipeline

Stream documents through embedding generation and into a vector database for AI retrieval:

```json
{
  "connectors": [
    {
      "name": "embed-documents",
      "config": {
        "connector.class": "OpenAISinkConnector",
        "mode": "embeddings",
        "input.field": "content",
        "output.field": "embedding",
        "topics": "documents"
      }
    },
    {
      "name": "store-vectors",
      "config": {
        "connector.class": "QdrantSinkConnector",
        "collection": "documents",
        "vector.field": "embedding",
        "topics": "embeddings"
      }
    }
  ]
}
```

### Content Enrichment

Add AI-generated summaries, classifications, or translations to streaming data:

```json
{
  "name": "summarize-articles",
  "config": {
    "connector.class": "OllamaSinkConnector",
    "mode": "completions",
    "completions.model": "llama3",
    "system.prompt": "Summarize this article in 2 sentences:",
    "input.field": "body",
    "output.field": "summary",
    "topics": "articles"
  }
}
```

### Multi-Agent Systems

Use Surgewave as the communication backbone for AI agents:

```
Agent A ──publish──▶ Surgewave Topic ──consume──▶ Agent B
   │                                              │
   └──────────◀── Surgewave Topic ◀──publish──────────┘
```

See [Agent Integration](agent-integration.md) for detailed patterns.

## Quick Start

### 1. Generate Embeddings with OpenAI

```bash
surgewave connect create openai-embeddings --config '{
  "connector.class": "OpenAISinkConnector",
  "openai.api.key": "${OPENAI_API_KEY}",
  "mode": "embeddings",
  "topics": "documents"
}'
```

### 2. Use Local LLM with Ollama

```bash
# Start Ollama locally
ollama serve

# Create connector
surgewave connect create ollama-summaries --config '{
  "connector.class": "OllamaSinkConnector",
  "ollama.base.url": "http://localhost:11434",
  "mode": "completions",
  "completions.model": "llama3",
  "topics": "articles"
}'
```

### 3. Store Vectors in Qdrant

```bash
surgewave connect create qdrant-vectors --config '{
  "connector.class": "QdrantSinkConnector",
  "qdrant.host": "localhost",
  "qdrant.port": "6334",
  "collection": "documents",
  "topics": "embeddings"
}'
```

## Configuration Reference

### Common Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `topics` | string | Required | Input topics (comma-separated) |
| `mode` | string | embeddings | Processing mode: `embeddings` or `completions` |
| `input.field` | string | text | JSON field containing input text |
| `output.field` | string | embedding | JSON field for output |
| `batch.size` | int | 100 | Records to batch before processing |
| `retry.max` | int | 3 | Maximum retry attempts |
| `retry.backoff.ms` | int | 1000 | Backoff between retries |

## Next Steps

- [OpenAI Connector](openai.md) - Cloud LLM integration
- [Ollama Connector](ollama.md) - Local LLM inference
- [Qdrant Connector](qdrant.md) - Vector database storage
- [Guardrails](guardrails.md) - Content safety (PII, toxicity, prompt injection)
- [Agent Memory](agent-memory.md) - Persistent agent memory and tool caching
- [Pipeline Chat](pipeline-chat.md) - Interactive chat with AI pipelines
- [Agent Integration](agent-integration.md) - Multi-agent architectures
