using System.Text.Json.Serialization;

namespace Kuestenlogik.Surgewave.Control.Models;

/// <summary>
/// One Kafka transaction from GET /v3/transactions (gRPC-JSON transcoding:
/// int64 fields arrive as strings, hence the number handling).
/// </summary>
public sealed record TxnListingModel(
    string TransactionalId,
    [property: JsonNumberHandling(JsonNumberHandling.AllowReadingFromString)] long ProducerId,
    string State);

/// <summary>Envelope of GET /v3/transactions.</summary>
public sealed record TxnListModel(IReadOnlyList<TxnListingModel>? Transactions);

/// <summary>Request body for POST /v3/transactions/describe.</summary>
public sealed record TxnDescribeRequest(IReadOnlyList<string> TransactionalIds);

/// <summary>Envelope of POST /v3/transactions/describe.</summary>
public sealed record TxnDescribeModel(IReadOnlyList<TxnDescriptionModel>? Transactions);

/// <summary>Detailed state of one Kafka transaction.</summary>
public sealed record TxnDescriptionModel(
    string TransactionalId,
    string State,
    [property: JsonNumberHandling(JsonNumberHandling.AllowReadingFromString)] long ProducerId,
    int ProducerEpoch,
    [property: JsonNumberHandling(JsonNumberHandling.AllowReadingFromString)] long TransactionTimeoutMs,
    [property: JsonNumberHandling(JsonNumberHandling.AllowReadingFromString)] long TransactionStartTimeMs,
    IReadOnlyList<TxnTopicPartitionModel>? Partitions);

/// <summary>A topic partition participating in a transaction.</summary>
public sealed record TxnTopicPartitionModel(string Topic, int Partition);

/// <summary>
/// One Surgewave cross-topic transaction from GET /api/transactions
/// (mirror of the broker's TransactionResponse).
/// </summary>
public sealed record CrossTopicTransactionModel(
    string TransactionId,
    string State,
    DateTimeOffset StartedAt,
    double TimeoutSeconds,
    int PendingWrites,
    long? ProducerId);
