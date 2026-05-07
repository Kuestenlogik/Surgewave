# Kafka Connect

Surgewave includes a Kafka Connect-compatible framework for building data integration pipelines.

## Overview

Kafka Connect enables streaming data between Surgewave and external systems:
- **Source Connectors** - Import data into Surgewave topics
- **Sink Connectors** - Export data from Surgewave topics to external systems
- **AI Pipelines** - Integrate with LLM providers (OpenAI, Ollama) and vector databases (Qdrant)
- **Pipeline Chat** - Interactive chat API for conversational AI pipelines (see [Pipeline Chat](../ai/pipeline-chat.md))

## Built-in Connectors

Surgewave provides a comprehensive set of production-ready connectors:

| Category | Connectors | Documentation |
|----------|------------|---------------|
| **Cloud Storage** | AWS S3, Azure Blob, Google Cloud Storage | [Connectors Guide](../connectors/index.md) |
| **Databases** | PostgreSQL (CDC), MongoDB, Elasticsearch, Generic ADO.NET | [Database Connectors](../connectors/database.md) |
| **Messaging** | MQTT, Redis, HTTP/Webhooks | [Messaging Connectors](../connectors/mqtt.md) |

**[View All Connectors →](../connectors/index.md)**

## Current Status

| Component | Status |
|-----------|--------|
| Connect Worker | Implemented |
| REST API | Implemented |
| Source/Sink base classes | Implemented |
| Cloud Storage Connectors | Implemented |
| Database Connectors | Implemented |
| Messaging Connectors | Implemented |
| AI/LLM Connectors | Implemented |
| Pipeline Chat API | Implemented |
| Vector Store Connector | Implemented |
| Connector Plugin System | Implemented |
| Distributed mode | Implemented |

## Connector Plugin System

Surgewave's Connector Plugin System enables distributed connector discovery, task assignment, and pipeline editor integration across a cluster of Connect workers.

### Plugin Manifest (`plugin.json`)

Each plugin package includes a `plugin.json` manifest with extended metadata for pipeline editor integration:

```json
{
  "id": "Kuestenlogik.Surgewave.Connector.MyPlugin",
  "name": "My Custom Connector",
  "version": "1.0.0",
  "description": "A custom connector for external system integration",
  "authors": ["Your Team"],
  "license": "Apache-2.0",
  "minRuntimeVersion": "1.0.0",
  "connectors": [
    {
      "class": "MyNamespace.MySourceConnector",
      "type": "source",
      "name": "My Source",
      "displayName": "My System Source",
      "icon": "Radar",
      "category": "Integration",
      "description": "Reads data from My System into Surgewave topics"
    },
    {
      "class": "MyNamespace.MySinkConnector",
      "type": "sink",
      "name": "My Sink",
      "displayName": "My System Sink",
      "icon": "Output",
      "category": "Integration",
      "description": "Writes data from Surgewave topics to My System"
    }
  ],
  "dependencies": {
    "MySystem.Client": "2.0.0"
  }
}
```

The extended connector fields (`displayName`, `icon`, `category`, `description`) are used by the Pipeline Editor UI to render nodes in the connector palette with proper grouping and visual identity.

### ConnectorCapability in Heartbeats

Connect workers advertise their available connector types in heartbeat messages. Each worker sends a list of `ConnectorCapability` records describing the connectors it can instantiate:

```csharp
// Sent as part of worker heartbeat
public sealed record ConnectorCapability(
    string ClassName,    // Fully qualified class name
    string Type,         // "source" or "sink"
    string DisplayName,  // Human-readable name
    string Version);     // Plugin version
```

This allows the broker to track which workers support which connector types without requiring every worker to have every plugin installed.

### Aggregated Connector Registry

The `AggregatedConnectorRegistry` combines local plugins (discovered from the broker's plugins directory) with remote worker capabilities (received via heartbeats) into a unified view:

- **Local-priority deduplication**: When the same connector class is available both locally and on remote workers, local metadata (which has richer fields like `icon` and `category`) takes precedence.
- **Worker availability tracking**: Each connector type tracks which worker IDs can instantiate it.
- The `/connector-plugins` REST endpoint returns the aggregated list with availability information.

```csharp
// Aggregated type returned by the registry
public sealed record AggregatedConnectorType
{
    public required string ClassName { get; init; }
    public required string Type { get; init; }         // "source" or "sink"
    public string? DisplayName { get; init; }
    public string? Icon { get; init; }
    public string? Category { get; init; }
    public string? Description { get; init; }
    public string? Version { get; init; }
    public IReadOnlyList<string> AvailableOnWorkers { get; init; }
    public bool IsLocal { get; init; }
}
```

### Remote Task Assignment

When a pipeline starts, the `PipelineOrchestrator` assigns connector tasks to the appropriate workers:

1. **Worker resolution**: `ResolveTargetWorker` selects the best worker for each connector type using load balancing across available workers.
2. **Task tracking**: `TaskAssignmentTracker` records which worker owns which connector task.
3. **Assignment publishing**: Task assignments are published to the config topic for worker consumption.
4. **Disconnect handling**: When a worker disconnects, its tasks are automatically reassigned to remaining workers.
5. **Request forwarding**: REST requests for remote connectors are forwarded to the owning worker via `ConnectorRequestForwarder`.

### Pipeline Editor Integration

The Pipeline Editor UI integrates with the plugin system to provide:

- **Worker availability badges**: Each node in the connector palette shows whether it is available locally, remotely, or both (Local / Remote / Both).
- **Worker selector**: The node configuration panel includes a dropdown to select which worker should run the connector task.
- **Worker badges on running nodes**: Running pipeline nodes display which worker is executing them.

## Configuration

### Enable Connect

```json
{
  "Surgewave": {
    "Connect": {
      "Enabled": true,
      "GroupId": "surgewave-connect",
      "ConfigTopic": "surgewave-connect-configs",
      "OffsetsTopic": "surgewave-connect-offsets",
      "StatusTopic": "surgewave-connect-status"
    }
  }
}
```

## Creating Custom Connectors

### Source Connector

A source connector imports data from an external system into Surgewave:

```csharp
public class MySourceConnector : SourceConnector
{
    private string _connectionString;

    public override void Start(Dictionary<string, string> config)
    {
        _connectionString = config["connection.string"];
    }

    public override void Stop()
    {
        // Cleanup resources
    }

    public override IEnumerable<SourceTask> CreateTasks(int maxTasks)
    {
        // Create tasks to distribute work
        for (int i = 0; i < maxTasks; i++)
        {
            yield return new MySourceTask(_connectionString, i);
        }
    }

    public override ConfigDef Config => new ConfigDef()
        .Define("connection.string", ConfigType.String, Importance.High, "Connection string");
}

public class MySourceTask : SourceTask
{
    private readonly string _connectionString;
    private readonly int _taskId;

    public MySourceTask(string connectionString, int taskId)
    {
        _connectionString = connectionString;
        _taskId = taskId;
    }

    public override void Start(Dictionary<string, string> config)
    {
        // Initialize task-specific resources
    }

    public override void Stop()
    {
        // Cleanup
    }

    public override IEnumerable<SourceRecord> Poll()
    {
        // Fetch records from external system
        var data = FetchFromExternalSystem();

        foreach (var item in data)
        {
            yield return new SourceRecord
            {
                Topic = "my-topic",
                Partition = 0,
                Key = Encoding.UTF8.GetBytes(item.Id),
                Value = JsonSerializer.SerializeToUtf8Bytes(item),
                Timestamp = DateTimeOffset.UtcNow
            };
        }
    }
}
```

### Sink Connector

A sink connector exports data from Surgewave to an external system:

```csharp
public class MySinkConnector : SinkConnector
{
    private string _endpoint;

    public override void Start(Dictionary<string, string> config)
    {
        _endpoint = config["endpoint"];
    }

    public override void Stop()
    {
        // Cleanup
    }

    public override IEnumerable<SinkTask> CreateTasks(int maxTasks)
    {
        for (int i = 0; i < maxTasks; i++)
        {
            yield return new MySinkTask(_endpoint);
        }
    }

    public override ConfigDef Config => new ConfigDef()
        .Define("endpoint", ConfigType.String, Importance.High, "Target endpoint URL");
}

public class MySinkTask : SinkTask
{
    private readonly string _endpoint;
    private HttpClient _client;

    public MySinkTask(string endpoint)
    {
        _endpoint = endpoint;
    }

    public override void Start(Dictionary<string, string> config)
    {
        _client = new HttpClient { BaseAddress = new Uri(_endpoint) };
    }

    public override void Stop()
    {
        _client?.Dispose();
    }

    public override void Put(IEnumerable<SinkRecord> records)
    {
        foreach (var record in records)
        {
            // Write to external system
            var content = new ByteArrayContent(record.Value);
            _client.PostAsync("/ingest", content).Wait();
        }
    }

    public override void Flush()
    {
        // Ensure all records are written
    }
}
```

## REST API

The Connect REST API is available when Connect is enabled:

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/connectors` | GET | List all connectors |
| `/connectors` | POST | Create connector |
| `/connectors/{name}` | GET | Get connector details |
| `/connectors/{name}` | DELETE | Delete connector |
| `/connectors/{name}/config` | GET | Get connector config |
| `/connectors/{name}/config` | PUT | Update connector config |
| `/connectors/{name}/status` | GET | Get connector status |
| `/connectors/{name}/pause` | PUT | Pause connector |
| `/connectors/{name}/resume` | PUT | Resume connector |
| `/connectors/{name}/restart` | POST | Restart connector |
| `/connectors/{name}/tasks` | GET | List tasks |
| `/connectors/{name}/tasks/{id}/restart` | POST | Restart specific task |
| `/connector-plugins` | GET | List available plugins |

### Example: Create Connector

```bash
curl -X POST https://localhost:9093/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "my-source",
    "config": {
      "connector.class": "MySourceConnector",
      "connection.string": "Server=localhost;Database=mydb",
      "tasks.max": "2"
    }
  }'
```

## CLI Usage

```bash
# List connectors
surgewave connect list

# Create connector from config file
surgewave connect create my-connector --config connector.json

# Connector operations
surgewave connect describe my-connector
surgewave connect status my-connector
surgewave connect pause my-connector
surgewave connect resume my-connector
surgewave connect restart my-connector
surgewave connect delete my-connector

# Task management
surgewave connect tasks list my-connector
surgewave connect tasks restart my-connector 0
```

## Connector Configuration

All connectors support these common configuration options:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `connector.class` | string | Required | Fully qualified connector class name |
| `tasks.max` | int | 1 | Maximum number of tasks |
| `key.converter` | string | JSON | Key converter class |
| `value.converter` | string | JSON | Value converter class |
| `errors.tolerance` | string | none | Error handling (none, all) |
| `errors.log.enable` | bool | false | Log errors |

## Building Custom Connectors

Surgewave's Connect framework is fully extensible. You can build custom connectors to integrate with any system.

**[Custom Connector Development Guide →](../connectors/custom-connectors.md)**

The guide covers:
- Connector architecture and design patterns
- SourceConnector and SinkConnector implementation
- Configuration with ConfigDef
- Offset management for exactly-once semantics
- Testing strategies
- Deployment and packaging

## Next Steps

- [All Connectors](../connectors/index.md) - Browse available connectors
- [Custom Connectors](../connectors/custom-connectors.md) - Build your own
- [AI Pipelines](../ai/index.md) - AI/LLM integration and pipelines
- [Pipeline Chat](../ai/pipeline-chat.md) - Interactive chat with AI pipelines
- [Streams](streams.md) - Stream processing
- [Schema Registry](schema-registry.md) - Schema management
