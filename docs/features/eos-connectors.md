# Exactly-Once Source Connectors

Source connectors with exactly-once delivery guarantees via atomic offset commits.

## Overview

Exactly-Once Source (EOS) Connectors extend the standard Connect source framework with transactional offset tracking. By using cross-topic transactions, the `ExactlyOnceSourcePipeline` atomically produces messages and commits source offsets in a single transaction. On crash or restart, the connector resumes from the last committed offset with zero duplicates.

Key characteristics:

- **Atomic produce + offset commit**: Messages and offsets are committed in a single cross-topic transaction
- **Crash-safe resume**: On restart, the task reads its last committed offset and resumes exactly where it left off
- **Topic-backed offset store**: Offsets are stored in the compacted `__connect_offsets` topic for durability
- **Backward compatible**: `ExactlyOnceSourceTask` extends `SourceTask`, so it works with the standard pipeline (at-least-once) as a fallback
- **Configurable batching**: Control batch size and poll intervals for throughput/latency tradeoffs

## How It Works

1. The `ExactlyOnceSourcePipeline` retrieves the last committed offset from `SurgewaveSourceOffsetStore`.
2. It calls `task.PollWithOffsetAsync(lastOffset)` -- the task reads from the external source starting at the given position.
3. The pipeline begins a **cross-topic transaction**.
4. All polled records are produced to their target topics within the transaction.
5. The new source offset is also produced to the `__connect_offsets` topic within the same transaction.
6. The transaction is committed atomically -- either all messages and the offset are written, or none are.
7. On crash, the offset store reflects the last successful transaction, so no duplicates occur.

```
ExactlyOnceSourcePipeline           SurgewaveSourceOffsetStore        Surgewave Topics
         |                                    |                       |
         |-- GetOffset(connector, partition)->|                       |
         |<-- lastOffset --------------------|                       |
         |                                    |                       |
         | PollWithOffsetAsync(lastOffset)                            |
         | (reads from external source)                               |
         |                                    |                       |
         |--------- Begin Transaction --------------------------->|
         |                                    |                       |
         |--------- Produce: orders, msg1 ---------------------->|
         |--------- Produce: orders, msg2 ---------------------->|
         |--------- Produce: __connect_offsets, newOffset ------>|
         |                                    |                       |
         |--------- Commit Transaction ------------------------->|
         |                                    |                       |
         |-- UpdateCache(newOffset) --------->|                       |
```

## Architecture

### ExactlyOnceSourceTask

Abstract base class that extends `SourceTask`. Instead of `PollAsync()`, subclasses implement `PollWithOffsetAsync()` which receives the last committed offset:

```csharp
public abstract class ExactlyOnceSourceTask : SourceTask
{
    public abstract Task<IReadOnlyList<ExactlyOnceSourceRecord>> PollWithOffsetAsync(
        Dictionary<string, string>? lastOffset,
        CancellationToken ct = default);
}
```

### ExactlyOnceSourceRecord

A source record with explicit partition and offset metadata for tracking:

```csharp
public sealed record ExactlyOnceSourceRecord
{
    public required string SourcePartition { get; init; } // e.g., table name, file path
    public required Dictionary<string, string> SourceOffset { get; init; } // e.g., {"lsn": "0/16B3748"}
    public required string Topic { get; init; }
    public int? Partition { get; init; }
    public byte[]? Key { get; init; }
    public required byte[] Value { get; init; }
    public DateTimeOffset? Timestamp { get; init; }
    public IDictionary<string, byte[]>? Headers { get; init; }
}
```

### SurgewaveSourceOffsetStore

Topic-backed offset store using the compacted `__connect_offsets` topic. Keys are `connectorName:sourcePartition`, values are JSON-serialized offset maps. Offsets are cached in a `ConcurrentDictionary` for fast lookup.

### ExactlyOnceSourcePipeline

The runtime pipeline that orchestrates the poll-produce-commit loop. Implements `ITaskRunner` for integration with the Connect worker.

## Configuration

```json
{
  "Surgewave": {
    "Connect": {
      "Enabled": true,
      "ExactlyOnce": {
        "Enabled": true,
        "OffsetTopic": "__connect_offsets",
        "MaxBatchSize": 1000,
        "TransactionTimeout": "00:01:00",
        "PollInterval": "00:00:01"
      }
    }
  }
}
```

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `Enabled` | bool | `true` | Enable exactly-once source semantics |
| `OffsetTopic` | string | `__connect_offsets` | Compacted topic for offset storage |
| `MaxBatchSize` | int | `1000` | Max records per transaction batch |
| `TransactionTimeout` | TimeSpan | `60s` | Timeout for cross-topic transactions |
| `PollInterval` | TimeSpan | `1s` | Delay between polls when no records are available |

## Writing an EOS Source Connector

### 1. Define the Task

```csharp
public class PostgresCdcTask : ExactlyOnceSourceTask
{
    public override string Version => "1.0.0";
    private string _connectionString = "";

    public override void Start(IDictionary<string, string> config)
    {
        _connectionString = config["connection.string"];
    }

    public override async Task<IReadOnlyList<ExactlyOnceSourceRecord>> PollWithOffsetAsync(
        Dictionary<string, string>? lastOffset, CancellationToken ct)
    {
        var lsn = lastOffset?.GetValueOrDefault("lsn") ?? "0/0";

        // Read changes from PostgreSQL WAL starting at the given LSN
        var changes = await ReadWalChanges(_connectionString, lsn, ct);

        return changes.Select(change => new ExactlyOnceSourceRecord
        {
            SourcePartition = change.TableName,
            SourceOffset = new Dictionary<string, string> { ["lsn"] = change.Lsn },
            Topic = $"cdc-{change.TableName}",
            Key = Encoding.UTF8.GetBytes(change.PrimaryKey),
            Value = JsonSerializer.SerializeToUtf8Bytes(change)
        }).ToList();
    }

    public override void Stop() { }
}
```

### 2. Define the Connector

```csharp
[ConnectorMetadata(Name = "postgres-cdc-eos", Description = "PostgreSQL CDC with exactly-once")]
public class PostgresCdcEosConnector : ExactlyOnceSourceConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(PostgresCdcTask);

    public override ConfigDef Config => new ConfigDef()
        .Define("connection.string", ConfigDef.Type.String,
            ConfigDef.Importance.High, "PostgreSQL connection string");

    public override void Start(IDictionary<string, string> config) { }
    public override void Stop() { }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
        => [new Dictionary<string, string>(StartConfig)];
}
```

## REST API for Offset Management

Offsets can be managed via the Connect REST API:

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/connect/connectors/{name}/offsets` | Get all committed offsets for a connector |
| `GET` | `/api/connect/connectors/{name}/offsets/{partition}` | Get offset for a specific partition |
| `DELETE` | `/api/connect/connectors/{name}/offsets` | Delete all offsets (reset connector) |
| `PUT` | `/api/connect/connectors/{name}/offsets/{partition}` | Set offset to a specific position |

### Check Offsets

```bash
curl http://localhost:9092/api/connect/connectors/postgres-cdc-eos/offsets
```

Response:

```json
{
  "connectorName": "postgres-cdc-eos",
  "offsets": {
    "orders": { "lsn": "0/16B3748" },
    "customers": { "lsn": "0/16B4200" }
  }
}
```

### Reset a Connector

```bash
# Stop the connector first
curl -X PUT http://localhost:9092/api/connect/connectors/postgres-cdc-eos/pause

# Delete all offsets
curl -X DELETE http://localhost:9092/api/connect/connectors/postgres-cdc-eos/offsets

# Restart -- will reprocess from the beginning
curl -X PUT http://localhost:9092/api/connect/connectors/postgres-cdc-eos/resume
```

## Use Cases

- **CDC (Change Data Capture)**: Replicate database changes with guaranteed exactly-once delivery
- **File ingestion**: Process files without duplicates even after crashes
- **API polling**: Cursor-based API polling with crash-safe position tracking
- **Log shipping**: Ship logs from external systems with no gaps or duplicates

## Next Steps

- [Cross-Topic Transactions](cross-topic-transactions.md) - The transaction engine underlying EOS
- [Connect](connect.md) - Surgewave Connect framework overview
- [Transactions](transactions.md) - Single-topic exactly-once semantics
