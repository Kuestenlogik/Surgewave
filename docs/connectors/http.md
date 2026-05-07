# HTTP Webhook Connector

The HTTP connector enables REST API integration, supporting both webhook receivers and HTTP polling/posting.

## Overview

- **Source**: Receive webhooks or poll HTTP endpoints
- **Sink**: POST records to HTTP endpoints with configurable authentication

**Use Cases:**
- Webhook event ingestion (GitHub, Stripe, Slack)
- REST API data extraction
- Third-party service integration
- Outbound notifications

## Quick Start

### HTTP Webhook Source

Receive webhook events:

```json
{
  "name": "webhook-source",
  "config": {
    "connector.class": "HttpSourceConnector",
    "source.mode": "webhook",
    "webhook.path": "/webhooks/github",
    "webhook.secret": "my-secret",
    "webhook.signature.header": "X-Hub-Signature-256",
    "topic": "github-events"
  }
}
```

### HTTP Sink

POST records to an API:

```json
{
  "name": "http-sink",
  "config": {
    "connector.class": "HttpSinkConnector",
    "http.url": "https://api.example.com/events",
    "topics": "user-events",
    "http.auth.type": "bearer",
    "http.auth.token": "my-api-token"
  }
}
```

## Configuration Reference

### Source Settings (Poll Mode)

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `source.mode` | string | `poll` | Mode: `poll`, `webhook` |
| `http.url` | string | Required | URL to poll |
| `topic` | string | Required | Destination Surgewave topic |
| `poll.interval.ms` | long | `60000` | Polling interval |
| `http.method` | string | `GET` | HTTP method |
| `http.auth.type` | string | - | Auth: `basic`, `bearer`, `api_key` |

### Source Settings (Webhook Mode)

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `source.mode` | string | `webhook` | Set to `webhook` |
| `webhook.path` | string | Required | Webhook endpoint path |
| `webhook.secret` | password | - | HMAC signature secret |
| `webhook.signature.header` | string | - | Header containing signature |
| `webhook.signature.algorithm` | string | `sha256` | Algorithm: `sha256`, `sha1`, `sha512` |
| `webhook.timestamp.header` | string | - | Timestamp header for replay protection |
| `webhook.timestamp.tolerance.seconds` | int | `300` | Max timestamp age |

### Sink Settings

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `topics` | string | Required | Source Surgewave topics |
| `http.url` | string | Required | Target URL |
| `http.method` | string | `POST` | HTTP method |
| `http.content.type` | string | `application/json` | Content-Type header |
| `http.auth.type` | string | - | Auth: `basic`, `bearer`, `api_key`, `hmac` |
| `http.auth.username` | string | - | Basic auth username |
| `http.auth.password` | password | - | Basic auth password |
| `http.auth.token` | password | - | Bearer token |
| `http.auth.api.key.header` | string | - | API key header name |
| `http.auth.api.key.value` | password | - | API key value |
| `batch.size` | int | `1` | Records per request |
| `retry.max.attempts` | int | `3` | Max retry attempts |
| `retry.backoff.ms` | long | `1000` | Retry backoff |

## Source Modes

### Poll Mode

Periodically fetch data from an HTTP endpoint:

```json
{
  "source.mode": "poll",
  "http.url": "https://api.example.com/events?since=${last_timestamp}",
  "poll.interval.ms": "60000",
  "http.auth.type": "bearer",
  "http.auth.token": "api-token"
}
```

### Webhook Mode

Receive push notifications:

```json
{
  "source.mode": "webhook",
  "webhook.path": "/hooks/stripe",
  "webhook.secret": "whsec_...",
  "webhook.signature.header": "Stripe-Signature"
}
```

## Authentication

### Basic Auth

```json
{
  "http.auth.type": "basic",
  "http.auth.username": "user",
  "http.auth.password": "pass"
}
```

### Bearer Token

```json
{
  "http.auth.type": "bearer",
  "http.auth.token": "eyJhbGciOiJIUzI1NiIs..."
}
```

### API Key

```json
{
  "http.auth.type": "api_key",
  "http.auth.api.key.header": "X-API-Key",
  "http.auth.api.key.value": "my-api-key"
}
```

### HMAC Signature (Sink)

```json
{
  "http.auth.type": "hmac",
  "http.auth.hmac.secret": "shared-secret",
  "http.auth.hmac.header": "X-Signature",
  "http.auth.hmac.algorithm": "sha256"
}
```

## Webhook Security

### HMAC Signature Validation

Validate webhook authenticity:

```json
{
  "webhook.secret": "whsec_...",
  "webhook.signature.header": "X-Hub-Signature-256",
  "webhook.signature.algorithm": "sha256"
}
```

Supports signature formats:
- `sha256=<hex>` - GitHub format
- `v1=<hex>` - Stripe format
- `<hex>` - Raw signature

### Timestamp Validation

Prevent replay attacks:

```json
{
  "webhook.timestamp.header": "X-Timestamp",
  "webhook.timestamp.tolerance.seconds": "300"
}
```

## Examples

### GitHub Webhooks

```json
{
  "name": "github-webhooks",
  "config": {
    "connector.class": "HttpSourceConnector",
    "source.mode": "webhook",
    "webhook.path": "/webhooks/github",
    "webhook.secret": "your-webhook-secret",
    "webhook.signature.header": "X-Hub-Signature-256",
    "webhook.signature.algorithm": "sha256",
    "topic": "github-events"
  }
}
```

Configure in GitHub:
- Payload URL: `https://your-domain.com/webhooks/github`
- Content type: `application/json`
- Secret: `your-webhook-secret`

### Stripe Webhooks

```json
{
  "name": "stripe-webhooks",
  "config": {
    "connector.class": "HttpSourceConnector",
    "source.mode": "webhook",
    "webhook.path": "/webhooks/stripe",
    "webhook.secret": "whsec_...",
    "webhook.signature.header": "Stripe-Signature",
    "topic": "payment-events"
  }
}
```

### Slack Notifications

Send alerts to Slack:

```json
{
  "name": "slack-alerts",
  "config": {
    "connector.class": "HttpSinkConnector",
    "http.url": "https://hooks.slack.com/services/T.../B.../xxx",
    "topics": "alerts",
    "http.method": "POST",
    "http.content.type": "application/json"
  }
}
```

### REST API Polling

Poll paginated API:

```json
{
  "name": "api-poller",
  "config": {
    "connector.class": "HttpSourceConnector",
    "source.mode": "poll",
    "http.url": "https://api.example.com/events",
    "poll.interval.ms": "30000",
    "http.auth.type": "bearer",
    "http.auth.token": "api-token",
    "topic": "api-events"
  }
}
```

### Outbound API Integration

Send data to external service:

```json
{
  "name": "crm-sync",
  "config": {
    "connector.class": "HttpSinkConnector",
    "http.url": "https://api.crm.com/contacts",
    "topics": "contact-updates",
    "http.method": "POST",
    "http.auth.type": "api_key",
    "http.auth.api.key.header": "Authorization",
    "http.auth.api.key.value": "Bearer crm-api-key",
    "batch.size": "1",
    "retry.max.attempts": "5"
  }
}
```

## Batching

### Single Record (Default)

Each record sent as individual request:

```json
{
  "batch.size": "1"
}
```

### Batch Requests

Send multiple records in array:

```json
{
  "batch.size": "100"
}
```

Request body: `[{record1}, {record2}, ...]`

## Error Handling

### Retry Configuration

```json
{
  "retry.max.attempts": "5",
  "retry.backoff.ms": "1000"
}
```

Exponential backoff: 1s, 2s, 4s, 8s, 16s

### Response Handling

| Status | Behavior |
|--------|----------|
| 2xx | Success |
| 4xx | Fail (no retry) |
| 5xx | Retry with backoff |
| Timeout | Retry with backoff |

## Troubleshooting

### Common Issues

**Webhook Not Receiving**
- Verify endpoint is publicly accessible
- Check firewall/security group rules
- Validate webhook path configuration

**Signature Validation Failed**
- Verify secret matches sender configuration
- Check signature header name
- Ensure algorithm matches

**Rate Limiting**
- Reduce poll frequency
- Implement batching for sink
- Add retry backoff

### Testing Webhooks

Use ngrok for local testing:

```bash
ngrok http 8080
# Use the ngrok URL in webhook configuration
```

### Monitoring

```bash
# Check connector status
surgewave connect status webhook-source

# View recent errors
surgewave connect describe webhook-source
```

## See Also

- [MQTT Connector](mqtt.md)
- [Redis Connector](redis.md)
- [Custom Connectors](custom-connectors.md)
