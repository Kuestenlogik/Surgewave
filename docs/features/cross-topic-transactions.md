# Cross-Topic Transactions

Atomic writes across multiple topics with two-phase commit.

## Overview

Cross-Topic Transactions enable atomic writes to multiple Surgewave topics in a single transaction. Either all messages across all topics are committed, or none are. This is built on a two-phase commit protocol with native wire-level opcodes, automatic timeout-based abort, and a fluent client API.

Key characteristics:

- **Atomic multi-topic writes**: All-or-nothing semantics across any number of topics
- **Two-phase commit**: Phase 1 writes messages, Phase 2 writes transaction markers to a log topic
- **Auto-abort on timeout**: Expired transactions are automatically cleaned up
- **Native protocol**: Dedicated opcodes (0x0E00-0x0E07) for zero-overhead transaction management
- **REST + native client API**: Both HTTP and high-performance binary protocol support
- **Configurable limits**: Max pending writes, timeout bounds, and cleanup intervals

## How It Works

1. The client calls `Begin()` to start a transaction, receiving a unique transaction ID.
2. The client adds writes to the transaction buffer via `AddWrite()` -- messages are held in-memory.
3. On `CommitAsync()`, the `CrossTopicTransactionManager` executes two-phase commit:
   - **Phase 1 (Committing)**: Write all buffered messages to their respective topic-partitions.
   - **Phase 2 (Markers)**: Write commit markers to the transaction log topic (`__cross_topic_txn_log`).
   - **Phase 3 (Committed)**: Mark the transaction as committed and release resources.
4. On failure at any phase, the transaction is aborted and pending writes are discarded.

```
Client                       CrossTopicTransactionManager            Topics
  |                                    |                               |
  |-- Begin() ----------------------->|                               |
  |<-- transactionId ----------------|                               |
  |                                    |                               |
  |-- AddWrite(orders, ...) --------->|                               |
  |-- AddWrite(inventory, ...) ------>|                               |
  |-- AddWrite(events, ...) --------->|  (buffered in memory)         |
  |                                    |                               |
  |-- CommitAsync() ----------------->|                               |
  |                                    |-- Phase 1: Write messages -->|
  |                                    |   orders: msg1               |
  |                                    |   inventory: msg2            |
  |                                    |   events: msg3               |
  |                                    |                               |
  |                                    |-- Phase 2: Write markers --->|
  |                                    |   __cross_topic_txn_log      |
  |                                    |                               |
  |<-- CommitResult (success) --------|                               |
```

## Configuration

```json
{
  "Surgewave": {
    "CrossTopicTransaction": {
      "Enabled": true,
      "DefaultTimeout": "00:01:00",
      "MaxTimeout": "00:15:00",
      "MaxPendingWrites": 10000,
      "CleanupIntervalSeconds": 30,
      "TransactionLogTopic": "__cross_topic_txn_log"
    }
  }
}
```

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `Enabled` | bool | `true` | Enable cross-topic transactions |
| `DefaultTimeout` | TimeSpan | `60s` | Default transaction timeout |
| `MaxTimeout` | TimeSpan | `15m` | Maximum allowed timeout (capped if exceeded) |
| `MaxPendingWrites` | int | `10000` | Max buffered writes per transaction |
| `CleanupIntervalSeconds` | int | `30` | Interval for cleaning up timed-out transactions |
| `TransactionLogTopic` | string | `__cross_topic_txn_log` | Internal topic for commit markers |

## Client API

### Native Client (High Performance)

```csharp
using var client = await SurgewaveClient.Create("localhost:9092").ConnectAsync();

// Begin a cross-topic transaction
await using var tx = await client.NativeClient!.CrossTopicTransactions
    .BeginAsync(timeout: TimeSpan.FromSeconds(30));

// Add writes to multiple topics
await tx.ProduceAsync("orders", orderKey, orderValue, partition: 0);
await tx.ProduceAsync("inventory", inventoryKey, inventoryValue, partition: 0);
await tx.ProduceAsync("audit-events", eventKey, eventValue, partition: 0);

// Commit atomically -- all or nothing
var result = await tx.CommitAsync();
if (result.ErrorCode != SurgewaveErrorCode.None)
{
    Console.WriteLine($"Transaction failed: {result.Error}");
}
```

### Auto-Abort on Dispose

If a transaction is disposed without committing, it is automatically aborted:

```csharp
await using var tx = await client.NativeClient!.CrossTopicTransactions
    .BeginAsync(timeout: TimeSpan.FromSeconds(10));

tx.ProduceAsync("orders", key, value, 0);

// Transaction is auto-aborted here when `using` block exits without CommitAsync()
```

## Native Protocol

Cross-topic transactions use 8 dedicated opcodes in the Surgewave native protocol:

| OpCode | Name | Direction | Description |
|--------|------|-----------|-------------|
| `0x0E00` | `CrossTopicTxnBegin` | Request | Begin a new transaction |
| `0x0E01` | `CrossTopicTxnBeginAck` | Response | Returns transaction ID |
| `0x0E02` | `CrossTopicTxnAddWrite` | Request | Buffer a write to a topic-partition |
| `0x0E03` | `CrossTopicTxnAddWriteAck` | Response | Confirms write buffered, returns pending count |
| `0x0E04` | `CrossTopicTxnCommit` | Request | Commit the transaction |
| `0x0E05` | `CrossTopicTxnCommitAck` | Response | Commit result with offsets and duration |
| `0x0E06` | `CrossTopicTxnAbort` | Request | Abort the transaction |
| `0x0E07` | `CrossTopicTxnAbortAck` | Response | Confirms abort |

## REST API

All endpoints are under `/api/transactions`.

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/transactions/` | Begin a new transaction |
| `POST` | `/api/transactions/{id}/write` | Add a write to the transaction |
| `POST` | `/api/transactions/{id}/commit` | Commit the transaction |
| `POST` | `/api/transactions/{id}/abort` | Abort the transaction |
| `GET` | `/api/transactions/` | List active transactions |
| `GET` | `/api/transactions/{id}` | Get transaction status |

### Begin a Transaction

```bash
curl -X POST http://localhost:9092/api/transactions/ \
  -H "Content-Type: application/json" \
  -d '{"producerId": "order-service", "timeoutSeconds": 60}'
```

Response:

```json
{
  "transactionId": "a1b2c3d4e5f6...",
  "state": "Open",
  "startedAt": "2026-03-19T10:00:00Z",
  "timeoutSeconds": 60,
  "pendingWrites": 0,
  "producerId": "order-service"
}
```

### Add Writes and Commit

```bash
# Add a write (values are base64-encoded)
curl -X POST http://localhost:9092/api/transactions/a1b2c3d4e5f6.../write \
  -H "Content-Type: application/json" \
  -d '{"topic": "orders", "partition": 0, "keyBase64": "b3JkZXIxMjM=", "valueBase64": "eyJpZCI6MTIzfQ=="}'

# Commit
curl -X POST http://localhost:9092/api/transactions/a1b2c3d4e5f6.../commit
```

## Transaction States

| State | Description |
|-------|-------------|
| `Open` | Accepting writes, not yet committed |
| `Committing` | Two-phase commit in progress |
| `Committed` | All writes applied successfully |
| `Aborting` | Abort in progress |
| `Aborted` | Transaction was aborted (explicit or on failure) |
| `TimedOut` | Transaction exceeded its timeout, auto-aborted |

## Use Cases

- **Order processing**: Atomically write to orders, inventory, and audit topics
- **Event sourcing**: Ensure consistent state across multiple event streams
- **Saga orchestration**: Coordinate distributed transactions via topic-based messaging
- **Data pipeline consistency**: Guarantee all-or-nothing writes in multi-topic ETL flows

## Next Steps

- [Transactions](transactions.md) - Single-topic exactly-once semantics
- [Exactly-Once Source Connectors](eos-connectors.md) - EOS for Connect sources
- [Data Mesh](data-mesh.md) - Data product catalog with contracts
