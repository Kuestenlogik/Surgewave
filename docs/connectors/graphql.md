# GraphQL Connector

The GraphQL connector provides integration with GraphQL APIs for both reading data (queries and subscriptions) and writing data (mutations).

## Package

```
Kuestenlogik.Surgewave.Connect.GraphQL
```

## Features

### Source Connector
- **Query Polling**: Execute GraphQL queries at configurable intervals
- **Subscription Mode**: Real-time data via WebSocket (graphql-ws protocol)
- Incremental polling with timestamp/ID-based tracking
- JSON path navigation for data extraction
- Configurable record key field
- Authentication support (Bearer tokens, API keys)

### Sink Connector
- **Single Mutations**: Execute individual mutations per record
- **Batch Mutations**: Execute bulk mutations with `$inputs` array
- Variable mapping from record fields
- Retry with exponential backoff
- Configurable batch sizes

## Authentication Methods

- **Bearer Token**: Standard Authorization header
- **API Key**: Custom header name support
- **Custom Headers**: Additional headers for specialized auth

## Source Configuration

### Required Settings

| Setting | Type | Description |
|---------|------|-------------|
| `graphql.endpoint` | String | GraphQL HTTP endpoint URL |
| `graphql.query` | String | GraphQL query or subscription |
| `topic` | String | Destination topic for records |

### Optional Settings

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `graphql.websocket.endpoint` | String | | WebSocket endpoint (required for subscriptions) |
| `graphql.auth.header` | String | `Authorization` | Authentication header name |
| `graphql.auth.token` | Password | | Authentication token |
| `graphql.headers` | String | | Additional headers as key=value;key=value |
| `graphql.timeout.ms` | Int | `30000` | Request timeout in milliseconds |
| `graphql.source.mode` | String | `poll` | Mode: `poll` or `subscription` |
| `graphql.operation.name` | String | | Operation name for multi-operation documents |
| `graphql.variables` | String | | Query variables as JSON object |
| `graphql.poll.interval.ms` | Int | `10000` | Poll interval in milliseconds |
| `graphql.data.path` | String | | JSON path to data array (e.g., `users`) |
| `graphql.id.field` | String | | Field to use as record key |
| `graphql.timestamp.field` | String | | Field for incremental polling |

## Sink Configuration

### Required Settings

| Setting | Type | Description |
|---------|------|-------------|
| `graphql.endpoint` | String | GraphQL HTTP endpoint URL |
| `graphql.mutation` | String | GraphQL mutation template |
| `topics` | String | Comma-separated list of topics to consume |

### Optional Settings

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `graphql.auth.header` | String | `Authorization` | Authentication header name |
| `graphql.auth.token` | Password | | Authentication token |
| `graphql.headers` | String | | Additional headers |
| `graphql.timeout.ms` | Int | `30000` | Request timeout in milliseconds |
| `graphql.operation.name` | String | | Operation name |
| `graphql.variables.mapping` | String | | Variable mapping as field=jsonPath;... |
| `graphql.batch.size` | Int | `100` | Batch size for mutations |
| `graphql.max.retry.count` | Int | `3` | Maximum retry attempts |
| `graphql.retry.delay.ms` | Int | `1000` | Delay between retries |

## Examples

### Basic Query Polling

```json
{
  "name": "graphql-users-source",
  "connector.class": "Kuestenlogik.Surgewave.Connect.GraphQL.GraphQLSourceConnector",
  "graphql.endpoint": "https://api.example.com/graphql",
  "graphql.query": "query { users { id name email createdAt } }",
  "graphql.data.path": "users",
  "graphql.id.field": "id",
  "graphql.poll.interval.ms": 30000,
  "topic": "users"
}
```

### Query with Authentication

```json
{
  "name": "graphql-auth-source",
  "connector.class": "Kuestenlogik.Surgewave.Connect.GraphQL.GraphQLSourceConnector",
  "graphql.endpoint": "https://api.example.com/graphql",
  "graphql.auth.token": "Bearer ${secrets:graphql-token}",
  "graphql.query": "query { posts { id title content author { name } } }",
  "graphql.data.path": "posts",
  "topic": "posts"
}
```

### Incremental Polling with Variables

```json
{
  "name": "graphql-incremental-source",
  "connector.class": "Kuestenlogik.Surgewave.Connect.GraphQL.GraphQLSourceConnector",
  "graphql.endpoint": "https://api.example.com/graphql",
  "graphql.query": "query GetOrders($after: DateTime) { orders(after: $after) { id total createdAt } }",
  "graphql.data.path": "orders",
  "graphql.timestamp.field": "createdAt",
  "graphql.poll.interval.ms": 60000,
  "topic": "orders"
}
```

### Real-time Subscription

```json
{
  "name": "graphql-subscription-source",
  "connector.class": "Kuestenlogik.Surgewave.Connect.GraphQL.GraphQLSourceConnector",
  "graphql.endpoint": "https://api.example.com/graphql",
  "graphql.websocket.endpoint": "wss://api.example.com/graphql",
  "graphql.source.mode": "subscription",
  "graphql.query": "subscription { messageCreated { id text sender timestamp } }",
  "graphql.data.path": "messageCreated",
  "topic": "messages"
}
```

### Basic Mutation Sink

```json
{
  "name": "graphql-sink",
  "connector.class": "Kuestenlogik.Surgewave.Connect.GraphQL.GraphQLSinkConnector",
  "graphql.endpoint": "https://api.example.com/graphql",
  "graphql.auth.token": "Bearer ${secrets:graphql-token}",
  "graphql.mutation": "mutation($input: UserInput!) { createUser(input: $input) { id } }",
  "topics": "new-users"
}
```

### Batch Mutation Sink

```json
{
  "name": "graphql-batch-sink",
  "connector.class": "Kuestenlogik.Surgewave.Connect.GraphQL.GraphQLSinkConnector",
  "graphql.endpoint": "https://api.example.com/graphql",
  "graphql.mutation": "mutation($inputs: [EventInput!]!) { createEvents(inputs: $inputs) { count } }",
  "graphql.batch.size": 50,
  "topics": "events"
}
```

### Mutation with Variable Mapping

```json
{
  "name": "graphql-mapped-sink",
  "connector.class": "Kuestenlogik.Surgewave.Connect.GraphQL.GraphQLSinkConnector",
  "graphql.endpoint": "https://api.example.com/graphql",
  "graphql.mutation": "mutation($userId: ID!, $status: String!) { updateStatus(userId: $userId, status: $status) { success } }",
  "graphql.variables.mapping": "userId=user.id;status=newStatus",
  "topics": "status-updates"
}
```

### API Key Authentication

```json
{
  "name": "graphql-apikey-sink",
  "connector.class": "Kuestenlogik.Surgewave.Connect.GraphQL.GraphQLSinkConnector",
  "graphql.endpoint": "https://api.example.com/graphql",
  "graphql.auth.header": "X-API-Key",
  "graphql.auth.token": "${secrets:api-key}",
  "graphql.mutation": "mutation($input: DataInput!) { ingestData(input: $input) { id } }",
  "topics": "data-ingestion"
}
```

## Source Modes

| Mode | Description | Use Case |
|------|-------------|----------|
| `poll` | Execute queries at intervals | Periodic data sync, batch processing |
| `subscription` | WebSocket real-time | Live updates, event streaming |

## Variable Handling

### Source Variables
- `$after`: Automatically populated with last timestamp/ID for incremental polling
- Custom variables: Provide via `graphql.variables` as JSON object

### Sink Variables
- `$input`: Default variable containing entire record
- `$inputs`: Array of records for batch mutations
- Custom mapping: Use `graphql.variables.mapping` to extract specific fields

## WebSocket Protocol

The subscription mode uses the `graphql-ws` protocol:
1. Sends `connection_init` on connect
2. Waits for `connection_ack`
3. Sends `subscribe` with query
4. Receives `next` messages with data
5. Handles `error` and `complete` messages
6. Automatic reconnection with 5-second delay

## Performance Considerations

- **Poll Interval**: Balance freshness vs API load (10-60 seconds typical)
- **Batch Size**: 50-100 records for batch mutations
- **Timeout**: Increase for slow APIs or large responses
- **Data Path**: Use to extract nested arrays and reduce processing
- **Subscription**: Preferred for real-time when available

## Error Handling

- GraphQL errors are logged but don't stop the connector
- HTTP errors trigger retry with exponential backoff
- WebSocket disconnections trigger automatic reconnection
- Invalid JSON records are skipped in sink

## Limitations

- Subscription mode requires graphql-ws protocol support
- File uploads not supported
- Multipart requests not supported
- Custom scalars must be JSON-serializable
- Query complexity/depth limits depend on server configuration
