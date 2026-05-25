# Recipe: Connect Pipeline

Data integration with Surgewave Connect — source connectors, transforms, sink connectors.

---

## Enable Connect

`appsettings.json`:

```json
{
  "Surgewave": {
    "Connect": {
      "Enabled": true,
      "PluginsDirectory": "plugins"
    }
  }
}
```

Start the broker with Connect enabled:

```bash
dotnet run --project src/Kuestenlogik.Surgewave.Broker -- \
  --Surgewave:Connect:Enabled=true \
  --Surgewave:Connect:PluginsDirectory="C:/path/to/plugins"
```

Collect connector DLLs into the plugins directory:

```powershell
.\scripts\collect-connectors.ps1
```

---

## 1. CSV File Source → Surgewave Topic

Create a file source connector that reads CSV files and publishes records to a topic.

```bash
curl -X POST https://localhost:9093/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "csv-source",
    "config": {
      "connector.class": "Kuestenlogik.Surgewave.Connector.File.CsvSourceConnector",
      "tasks.max": "1",
      "file.path": "/data/input/*.csv",
      "topic": "raw-data",
      "batch.size": "500",
      "poll.interval.ms": "5000"
    }
  }'
```

---

## 2. Surgewave Topic → PostgreSQL Sink

Read from a topic and insert rows into a database table.

```bash
curl -X POST https://localhost:9093/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "postgres-sink",
    "config": {
      "connector.class": "Kuestenlogik.Surgewave.Connector.Database.JdbcSinkConnector",
      "tasks.max": "2",
      "topics": "processed-orders",
      "connection.url": "Host=localhost;Database=orders;Username=app;Password=secret",
      "table.name": "orders",
      "insert.mode": "upsert",
      "pk.mode": "record_key",
      "pk.fields": "id",
      "batch.size": "100"
    }
  }'
```

---

## 3. Source → Transform → Sink (Full Pipeline)

This example reads from PostgreSQL CDC, transforms the records, and writes to Elasticsearch.

```bash
# Step 1: Create CDC source
curl -X POST https://localhost:9093/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "pg-cdc-source",
    "config": {
      "connector.class": "Kuestenlogik.Surgewave.Connector.Cdc.PostgresCdcConnector",
      "tasks.max": "1",
      "database.host": "localhost",
      "database.port": "5432",
      "database.name": "mydb",
      "database.user": "replicator",
      "database.password": "secret",
      "table.include.list": "public.orders",
      "topic.prefix": "cdc"
    }
  }'

# Step 2: Create transform pipeline (via Pipeline API)
curl -X POST https://localhost:9093/api/pipelines \
  -H "Content-Type: application/json" \
  -d '{
    "name": "cdc-to-elasticsearch",
    "nodes": [
      {
        "id": "source",
        "type": "SurgewaveTopic",
        "config": { "topic": "cdc.public.orders" }
      },
      {
        "id": "filter",
        "type": "Filter",
        "config": { "expression": "record.op != \"d\"" }
      },
      {
        "id": "transform",
        "type": "FieldMapper",
        "config": {
          "mappings": {
            "doc_id": "after.id",
            "status": "after.status",
            "updated_at": "after.updated_at"
          }
        }
      },
      {
        "id": "sink",
        "type": "Elasticsearch",
        "config": {
          "url": "http://elasticsearch:9200",
          "index": "orders",
          "id.field": "doc_id"
        }
      }
    ],
    "edges": [
      {"from": "source", "to": "filter"},
      {"from": "filter", "to": "transform"},
      {"from": "transform", "to": "sink"}
    ]
  }'

# Step 3: Start the pipeline
curl -X POST https://localhost:9093/api/pipelines/{id}/start
```

---

## 4. Using the Pipeline Designer UI

The Control UI runs on port 5050 and includes a visual pipeline designer.

```bash
dotnet run --project src/Kuestenlogik.Surgewave.Control --urls "http://localhost:5050"
```

Navigate to `http://localhost:5050/pipelines` to:
- Drag-and-drop source, transform, and sink nodes
- Connect nodes with edges
- Preview data at each step with dry-run mode
- Deploy and monitor live pipelines

---

## Manage Connectors

```bash
# List all connectors
curl https://localhost:9093/connectors

# Get status
curl https://localhost:9093/connectors/csv-source/status

# Pause / resume
curl -X PUT https://localhost:9093/connectors/csv-source/pause
curl -X PUT https://localhost:9093/connectors/csv-source/resume

# Restart a failed task
curl -X POST https://localhost:9093/connectors/csv-source/tasks/0/restart

# Delete connector
curl -X DELETE https://localhost:9093/connectors/csv-source
```

---

## See Also

- [Kafka Connect Feature](../features/connect.md)
- [Connector Plugins](../connectors/index.md)
- [REST API Reference](../reference/rest-api-reference.md)
