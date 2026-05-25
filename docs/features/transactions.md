# Transactions

Surgewave supports exactly-once semantics with transactions.

## Overview

Transactions provide:
- **Atomic writes** across partitions
- **Exactly-once** processing
- **Read committed** isolation

## Producer Transactions

### Using the TransactionBuilder

The `SurgewaveNativeClient` provides a fluent transaction API:

```csharp
await using var client = new SurgewaveNativeClient("localhost", 9092);
await client.ConnectAsync();

// Begin a transaction
var tx = client.Transactions.BeginTransaction("my-transactional-id");
await tx.InitAsync();
```

### Transaction Flow

```csharp
var tx = client.Transactions.BeginTransaction("my-transactional-id");
await tx.InitAsync();

try
{
    await client.Messaging.Send("orders").WithValue(orderJson).ExecuteAsync();
    await client.Messaging.Send("inventory").WithValue(inventoryUpdate).ExecuteAsync();
    await client.Messaging.Send("notifications").WithValue(notification).ExecuteAsync();

    await tx.CommitAsync();
}
catch
{
    await tx.AbortAsync();
    throw;
}
```

## Consume-Transform-Produce

Exactly-once pattern using the native client:

```csharp
await using var client = new SurgewaveNativeClient("localhost", 9092);
await client.ConnectAsync();

await using var consumer = new SurgewaveConsumer<string, string>(options =>
{
    options.BootstrapServers = "localhost:9092";
    options.GroupId = "processor-group";
    options.IsolationLevel = IsolationLevel.ReadCommitted;
    options.EnableAutoCommit = false;
});

consumer.Subscribe("input-topic");

var tx = client.Transactions.BeginTransaction("processor-producer");
await tx.InitAsync();

while (!cancellationToken.IsCancellationRequested)
{
    var record = await consumer.ConsumeAsync(cancellationToken);
    if (record == null) continue;

    try
    {
        var result = Transform(record.Value);
        await client.Messaging.Send("output-topic")
            .WithKey(record.Key)
            .WithValue(result)
            .ExecuteAsync();

        await tx.CommitAsync();
    }
    catch
    {
        await tx.AbortAsync();
    }
}
```

## Isolation Levels

### Read Uncommitted (Default)

See all messages including uncommitted:

```csharp
options.IsolationLevel = IsolationLevel.ReadUncommitted;
```

### Read Committed

Only see committed messages:

```csharp
options.IsolationLevel = IsolationLevel.ReadCommitted;
```

## Transaction APIs

| API | Description |
|-----|-------------|
| `InitProducerId` | Initialize transactional producer |
| `BeginTransaction` | Start transaction |
| `EndTransaction` | Commit or abort |
| `WriteTxnMarkers` | Cross-broker coordination |
| `AddPartitionsToTxn` | Add partitions to transaction |
| `AddOffsetsToTxn` | Include consumer offsets |

## Configuration

### Transaction Builder

| Setting | Default | Description |
|---------|---------|-------------|
| `transactionalId` | Required | Unique transaction ID (passed to `BeginTransaction()`) |
| `WithTimeout()` | 60000ms | Transaction timeout |

### Broker

```json
{
  "Surgewave": {
    "Transactions": {
      "TimeoutMs": 60000,
      "LogRetentionMs": 604800000,
      "MaxTimeoutMs": 900000
    }
  }
}
```

## Metrics

Prometheus metrics:

| Metric | Description |
|--------|-------------|
| `surgewave_transactions_total` | Total transactions |
| `surgewave_transaction_commits_total` | Committed transactions |
| `surgewave_transaction_aborts_total` | Aborted transactions |
| `surgewave_transaction_duration_ms` | Transaction duration |
| `surgewave_transaction_timeouts_total` | Timed out transactions |

## CLI Commands

```bash
# List transactions
surgewave transactions list

# Describe transaction
surgewave transactions describe <transaction-id>
```

## Error Handling

```csharp
try
{
    var errorCode = await tx.CommitAsync();
    if (errorCode != SurgewaveErrorCode.None)
    {
        Console.WriteLine($"Transaction failed: {errorCode}");
    }
}
catch (InvalidOperationException ex)
{
    // Transaction initialization failed or another producer with same ID is active
    Console.WriteLine($"Transaction error: {ex.Message}");
}
catch (ProtocolException ex)
{
    // Protocol-level error
    await tx.AbortAsync();
}
```

## Best Practices

1. **Use unique transactional.id** - Per producer instance
2. **Keep transactions short** - Avoid long-running transactions
3. **Handle fencing** - Recreate producer on fence exception
4. **Enable read_committed** - For consumers needing isolation

## Next Steps

- [Quotas](quotas.md) - Rate limiting
- [Clustering](../clustering/index.md) - Multi-broker transactions
