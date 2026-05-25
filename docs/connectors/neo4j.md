# Neo4j Connector

The Neo4j connector provides integration with Neo4j graph database for both reading graph data using Cypher queries and writing nodes/relationships using MERGE or CREATE operations.

## Package

```
Kuestenlogik.Surgewave.Connect.Neo4j
```

## Features

### Source Connector
- **Cypher Query Support**: Execute custom Cypher queries for flexible graph data extraction
- **Label Polling**: Poll specific node labels with automatic query generation
- Incremental polling with timestamp-based tracking
- Configurable ID property for record keys
- Rich metadata in record headers (database, label, element ID)
- Topic naming with pattern substitution

### Sink Connector
- **MERGE Operations**: Upsert nodes with identity properties for idempotent writes
- **CREATE Operations**: Insert new nodes without uniqueness constraints
- **Custom Cypher**: Execute custom Cypher queries for complex write operations
- Batch transactions with UNWIND for high throughput
- Retry with exponential backoff for transient failures
- Tombstone handling for deletes

## Authentication Methods

- **Basic Authentication**: Username/password (standard)
- **No Authentication**: For development environments
- **TLS/SSL**: Encrypted connections

## Source Configuration

### Required Settings

| Setting | Type | Description |
|---------|------|-------------|
| `neo4j.uri` | String | Neo4j connection URI (e.g., bolt://localhost:7687) |

### Optional Settings

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `neo4j.username` | String | | Neo4j username |
| `neo4j.password` | Password | | Neo4j password |
| `neo4j.database` | String | `neo4j` | Neo4j database name |
| `neo4j.encrypted` | Boolean | `false` | Enable TLS encryption |
| `neo4j.label` | String | | Node label to query (required if no custom query) |
| `neo4j.query` | String | | Custom Cypher query (overrides label) |
| `neo4j.topic` | String | | Destination topic (optional if using pattern) |
| `neo4j.topic.pattern` | String | `neo4j.${database}.${label}` | Topic naming pattern |
| `neo4j.poll.interval.ms` | Int | `10000` | Poll interval in milliseconds |
| `neo4j.max.rows.per.poll` | Int | `10000` | Maximum rows per poll |
| `neo4j.include.metadata` | Boolean | `true` | Include Neo4j metadata in headers |
| `neo4j.timestamp.property` | String | | Property for incremental polling |
| `neo4j.id.property` | String | | Property to use as record key |

## Sink Configuration

### Required Settings

| Setting | Type | Description |
|---------|------|-------------|
| `neo4j.uri` | String | Neo4j connection URI (e.g., bolt://localhost:7687) |
| `topics` | String | Comma-separated list of topics to consume |

### Optional Settings

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `neo4j.username` | String | | Neo4j username |
| `neo4j.password` | Password | | Neo4j password |
| `neo4j.database` | String | `neo4j` | Neo4j database name |
| `neo4j.encrypted` | Boolean | `false` | Enable TLS encryption |
| `neo4j.label` | String | | Node label (required if no custom Cypher) |
| `neo4j.write.mode` | String | `merge` | Write mode: `merge`, `create` |
| `neo4j.batch.size` | Int | `1000` | Batch size for bulk operations |
| `neo4j.max.retry.count` | Int | `3` | Maximum retry attempts |
| `neo4j.retry.delay.ms` | Int | `1000` | Delay between retries in milliseconds |
| `neo4j.merge.properties` | String | | Comma-separated properties for MERGE key |
| `neo4j.id.property` | String | | Property for MERGE identity |
| `neo4j.node.label.field` | String | | Field to use as node label |
| `neo4j.custom.cypher` | String | | Custom Cypher query for writes |
| `neo4j.unwind.parameter` | String | `events` | Parameter name for UNWIND |

## Record Headers

### Source Headers

| Header | Description |
|--------|-------------|
| `neo4j.database` | Database name |
| `neo4j.label` | Node label |
| `neo4j.element.id` | Neo4j element ID |
| `neo4j.node.id` | Legacy node ID (if available) |

## Examples

### Basic Source (Label Mode)

```json
{
  "name": "neo4j-source",
  "connector.class": "Kuestenlogik.Surgewave.Connect.Neo4j.Neo4jSourceConnector",
  "neo4j.uri": "bolt://localhost:7687",
  "neo4j.username": "neo4j",
  "neo4j.password": "${secrets:neo4j-password}",
  "neo4j.database": "neo4j",
  "neo4j.label": "Person",
  "neo4j.poll.interval.ms": 10000
}
```

### Custom Cypher Query Source

```json
{
  "name": "neo4j-cypher-source",
  "connector.class": "Kuestenlogik.Surgewave.Connect.Neo4j.Neo4jSourceConnector",
  "neo4j.uri": "bolt://neo4j.example.com:7687",
  "neo4j.username": "neo4j",
  "neo4j.password": "${secrets:neo4j-password}",
  "neo4j.query": "MATCH (p:Person)-[:WORKS_AT]->(c:Company) WHERE p.updated > $lastTimestamp RETURN p, c ORDER BY p.updated LIMIT 1000",
  "neo4j.timestamp.property": "updated",
  "neo4j.poll.interval.ms": 60000
}
```

### Source with Incremental Polling

```json
{
  "name": "neo4j-incremental-source",
  "connector.class": "Kuestenlogik.Surgewave.Connect.Neo4j.Neo4jSourceConnector",
  "neo4j.uri": "bolt://localhost:7687",
  "neo4j.username": "neo4j",
  "neo4j.password": "${secrets:neo4j-password}",
  "neo4j.label": "Event",
  "neo4j.timestamp.property": "createdAt",
  "neo4j.id.property": "eventId",
  "neo4j.max.rows.per.poll": 5000
}
```

### Basic Sink (MERGE Mode)

```json
{
  "name": "neo4j-sink",
  "connector.class": "Kuestenlogik.Surgewave.Connect.Neo4j.Neo4jSinkConnector",
  "neo4j.uri": "bolt://localhost:7687",
  "neo4j.username": "neo4j",
  "neo4j.password": "${secrets:neo4j-password}",
  "neo4j.label": "Person",
  "neo4j.write.mode": "merge",
  "neo4j.id.property": "id",
  "topics": "persons"
}
```

### Sink with Multiple Merge Properties

```json
{
  "name": "neo4j-merge-sink",
  "connector.class": "Kuestenlogik.Surgewave.Connect.Neo4j.Neo4jSinkConnector",
  "neo4j.uri": "bolt://localhost:7687",
  "neo4j.username": "neo4j",
  "neo4j.password": "${secrets:neo4j-password}",
  "neo4j.label": "Transaction",
  "neo4j.write.mode": "merge",
  "neo4j.merge.properties": "accountId,transactionDate,reference",
  "topics": "transactions",
  "neo4j.batch.size": 2000
}
```

### Custom Cypher Sink

```json
{
  "name": "neo4j-custom-sink",
  "connector.class": "Kuestenlogik.Surgewave.Connect.Neo4j.Neo4jSinkConnector",
  "neo4j.uri": "bolt://localhost:7687",
  "neo4j.username": "neo4j",
  "neo4j.password": "${secrets:neo4j-password}",
  "neo4j.custom.cypher": "UNWIND $events AS event MERGE (p:Person {id: event.personId}) MERGE (c:Company {id: event.companyId}) MERGE (p)-[:WORKS_AT {since: event.startDate}]->(c)",
  "topics": "employment"
}
```

### High-Throughput Sink

```json
{
  "name": "neo4j-high-throughput-sink",
  "connector.class": "Kuestenlogik.Surgewave.Connect.Neo4j.Neo4jSinkConnector",
  "neo4j.uri": "bolt://neo4j-cluster.example.com:7687",
  "neo4j.username": "neo4j",
  "neo4j.password": "${secrets:neo4j-password}",
  "neo4j.label": "Event",
  "neo4j.write.mode": "create",
  "topics": "events",
  "neo4j.batch.size": 5000,
  "neo4j.max.retry.count": 5,
  "neo4j.retry.delay.ms": 500
}
```

### Sink with TLS

```json
{
  "name": "neo4j-tls-sink",
  "connector.class": "Kuestenlogik.Surgewave.Connect.Neo4j.Neo4jSinkConnector",
  "neo4j.uri": "bolt+s://secure-neo4j.example.com:7687",
  "neo4j.username": "neo4j",
  "neo4j.password": "${secrets:neo4j-password}",
  "neo4j.encrypted": true,
  "neo4j.label": "SecureData",
  "neo4j.id.property": "dataId",
  "topics": "secure-data"
}
```

## Cypher Query Examples

### Match Nodes by Label

```cypher
MATCH (n:Person)
WHERE n.age > 21
RETURN n
LIMIT 1000
```

### Match with Relationships

```cypher
MATCH (p:Person)-[r:KNOWS]->(f:Person)
WHERE p.name = 'Alice'
RETURN p, r, f
```

### Aggregations

```cypher
MATCH (p:Person)-[:WORKS_AT]->(c:Company)
RETURN c.name AS company, count(p) AS employees
ORDER BY employees DESC
```

### Create with UNWIND (Batch)

```cypher
UNWIND $events AS event
CREATE (n:Event)
SET n = event
```

### MERGE with Properties

```cypher
UNWIND $events AS event
MERGE (p:Person {id: event.id})
ON CREATE SET p.created = datetime()
ON MATCH SET p.updated = datetime()
SET p += event
```

## Write Modes

| Mode | Description | Use Case |
|------|-------------|----------|
| `merge` | Create or update (idempotent) | Updates to existing data, deduplication |
| `create` | Always create new nodes | Event logging, audit trails |

## Connection URI Schemes

| Scheme | Description |
|--------|-------------|
| `bolt://` | Standard Bolt protocol (unencrypted) |
| `bolt+s://` | Bolt with TLS encryption |
| `bolt+ssc://` | Bolt with TLS (self-signed certificates) |
| `neo4j://` | Routing protocol for clusters |
| `neo4j+s://` | Routing protocol with TLS |

## Performance Considerations

- **Batch Size**: 1000-5000 nodes typically optimal; larger batches may cause memory issues
- **UNWIND**: Always use UNWIND for batch operations instead of individual queries
- **MERGE Properties**: Use minimal properties for MERGE identity (indexed properties recommended)
- **Indexes**: Create indexes on properties used in MERGE and WHERE clauses
- **Transactions**: Batch writes use single transactions for atomicity
- **Connection Pooling**: Neo4j driver handles connection pooling automatically

## Limitations

- No native CDC support (use timestamp-based polling for incremental reads)
- Complex relationship creation requires custom Cypher queries
- APOC procedures not directly supported (use custom Cypher)
- Maximum transaction size limited by Neo4j heap configuration
- Graph algorithms (GDS) not integrated (use custom queries)
