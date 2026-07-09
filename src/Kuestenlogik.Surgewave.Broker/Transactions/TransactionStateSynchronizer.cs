using System.Buffers.Binary;
using System.Text.Json;
using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.Replication;
using Kuestenlogik.Surgewave.Core.Models;
using Microsoft.Extensions.Logging;
using TransactionState = Kuestenlogik.Surgewave.Core.KafkaConstants.TransactionState;

namespace Kuestenlogik.Surgewave.Broker.Transactions;

/// <summary>
/// Synchronizes transaction state between coordinators during leadership changes.
/// Ensures transaction metadata is transferred when a new broker becomes the coordinator.
/// </summary>
internal sealed partial class TransactionStateSynchronizer
{
    private readonly ConnectionPool _connectionPool;
    private readonly ClusterState _clusterState;
    private readonly ClusteredTransactionCoordinator? _coordinator;
    private readonly int _localBrokerId;
    private readonly ILogger<TransactionStateSynchronizer> _logger;
    private readonly TransactionStateSynchronizerOptions _options;
    private int _correlationId;

    public TransactionStateSynchronizer(
        ConnectionPool connectionPool,
        ClusterState clusterState,
        ClusteredTransactionCoordinator? coordinator,
        int localBrokerId,
        ILogger<TransactionStateSynchronizer> logger,
        TransactionStateSynchronizerOptions? options = null)
    {
        _connectionPool = connectionPool;
        _clusterState = clusterState;
        _coordinator = coordinator;
        _localBrokerId = localBrokerId;
        _logger = logger;
        _options = options ?? new TransactionStateSynchronizerOptions();
    }

    /// <summary>
    /// Called when this broker becomes the new transaction coordinator.
    /// Fetches transaction state from the previous coordinator.
    /// </summary>
    /// <param name="previousCoordinatorId">The broker ID of the previous coordinator.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>True if synchronization succeeded, false otherwise.</returns>
    public async Task<bool> SyncFromPreviousCoordinatorAsync(
        int previousCoordinatorId,
        CancellationToken cancellationToken)
    {
        if (_coordinator == null)
        {
            LogNoCoordinator();
            return false;
        }

        if (previousCoordinatorId == _localBrokerId)
        {
            LogSameCoordinator();
            return true;
        }

        var previousBroker = _clusterState.GetBroker(previousCoordinatorId);
        if (previousBroker == null)
        {
            LogPreviousCoordinatorNotFound(previousCoordinatorId);
            return false;
        }

        try
        {
            LogStartingSync(previousCoordinatorId);

            var transactions = await FetchTransactionStateAsync(
                previousBroker.Host,
                previousBroker.Port,
                cancellationToken);

            if (transactions == null)
            {
                LogFetchFailed(previousCoordinatorId);
                return false;
            }

            // Import each transaction
            int imported = 0;
            foreach (var txn in transactions)
            {
                _coordinator.ImportTransactionState(txn);
                imported++;
            }

            LogSyncCompleted(previousCoordinatorId, imported);
            return true;
        }
        catch (Exception ex)
        {
            LogSyncFailed(previousCoordinatorId, ex);
            return false;
        }
    }

    /// <summary>
    /// Called when this broker is losing coordinator role.
    /// Exports transaction state for the new coordinator to fetch.
    /// </summary>
    /// <returns>Serialized transaction state.</returns>
    public byte[] ExportTransactionState()
    {
        if (_coordinator == null)
        {
            return [];
        }

        var transactions = _coordinator.GetAllTransactions()
            .Select(t => new TransactionStateSnapshot
            {
                TransactionalId = t.TransactionalId,
                ProducerId = t.ProducerId,
                ProducerEpoch = t.ProducerEpoch,
                State = t.State.ToString(),
                TransactionTimeoutMs = t.TransactionTimeoutMs,
                LastActivityTime = t.LastActivityTime.ToUnixTimeMilliseconds(),
                Partitions = t.Partitions.Select(p => new PartitionSnapshot(p.Topic, p.Partition)).ToList(),
                ConsumerGroups = t.ConsumerGroups.ToList(),
                PendingOffsets = t.PendingOffsets.Select(o => new OffsetSnapshot(
                    o.GroupId, o.Topic, o.Partition, o.Offset, o.Metadata)).ToList()
            })
            .ToList();

        return JsonSerializer.SerializeToUtf8Bytes(transactions);
    }

    /// <summary>
    /// Fetches transaction state from a remote broker.
    /// </summary>
    private async Task<List<TransactionMetadata>?> FetchTransactionStateAsync(
        string host,
        int port,
        CancellationToken cancellationToken)
    {
        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeoutCts.CancelAfter(_options.SyncTimeoutMs);

        var connection = await _connectionPool.GetConnectionAsync(host, port, timeoutCts.Token);
        try
        {
            var stream = connection.Stream;

            // Build a custom request to fetch transaction state
            // In a full implementation, this would use a proper Kafka API
            // For now, we'll use a simple JSON-based protocol over the connection
            var request = new TransactionStateFetchRequest
            {
                CorrelationId = Interlocked.Increment(ref _correlationId),
                TargetBrokerId = _localBrokerId
            };

            var requestBytes = JsonSerializer.SerializeToUtf8Bytes(request);

            // Write with magic byte prefix to identify this as a state sync request
            var header = new byte[8];
            BinaryPrimitives.WriteInt32BigEndian(header.AsSpan(0, 4), requestBytes.Length + 4);
            BinaryPrimitives.WriteInt32BigEndian(header.AsSpan(4, 4), TransactionStateFetchMagic);
            await stream.WriteAsync(header, timeoutCts.Token);
            await stream.WriteAsync(requestBytes, timeoutCts.Token);
            await stream.FlushAsync(timeoutCts.Token);

            // Read response size
            var sizeBuffer = new byte[4];
            await ReadExactlyAsync(stream, sizeBuffer, timeoutCts.Token);
            var responseSize = BinaryPrimitives.ReadInt32BigEndian(sizeBuffer);

            if (responseSize <= 0 || responseSize > _options.MaxResponseSizeBytes)
            {
                LogInvalidResponseSize(responseSize);
                return null;
            }

            // Read response body
            var responseBuffer = new byte[responseSize];
            await ReadExactlyAsync(stream, responseBuffer, timeoutCts.Token);

            // Parse response
            var snapshots = JsonSerializer.Deserialize<List<TransactionStateSnapshot>>(responseBuffer);
            if (snapshots == null)
            {
                return [];
            }

            // Convert to TransactionMetadata
            return snapshots.Select(s => ConvertToMetadata(s)).ToList();
        }
        finally
        {
            connection.Return();
        }
    }

    private static TransactionMetadata ConvertToMetadata(TransactionStateSnapshot snapshot)
    {
        var metadata = new TransactionMetadata
        {
            TransactionalId = snapshot.TransactionalId,
            ProducerId = snapshot.ProducerId,
            ProducerEpoch = snapshot.ProducerEpoch,
            TransactionTimeoutMs = snapshot.TransactionTimeoutMs,
            LastActivityTime = DateTimeOffset.FromUnixTimeMilliseconds(snapshot.LastActivityTime)
        };

        if (Enum.TryParse<TransactionState>(snapshot.State, out var state))
        {
            metadata.State = state;
        }

        foreach (var p in snapshot.Partitions)
        {
            metadata.Partitions.Add(new TopicPartition { Topic = p.Topic, Partition = p.Partition });
        }

        foreach (var g in snapshot.ConsumerGroups)
        {
            metadata.ConsumerGroups.Add(g);
        }

        foreach (var o in snapshot.PendingOffsets)
        {
            metadata.PendingOffsets.Add(new PendingTxnOffset
            {
                GroupId = o.GroupId,
                Topic = o.Topic,
                Partition = o.Partition,
                Offset = o.Offset,
                Metadata = o.Metadata
            });
        }

        return metadata;
    }

    private static async Task ReadExactlyAsync(
        Stream stream,
        byte[] buffer,
        CancellationToken cancellationToken)
    {
        int totalRead = 0;
        while (totalRead < buffer.Length)
        {
            var read = await stream.ReadAsync(buffer.AsMemory(totalRead, buffer.Length - totalRead), cancellationToken);
            if (read == 0)
                throw new EndOfStreamException("Connection closed while reading response");
            totalRead += read;
        }
    }

    // Magic number to identify transaction state sync requests
    private const int TransactionStateFetchMagic = 0x54584E53; // "TXNS"

    [LoggerMessage(Level = LogLevel.Warning, Message = "No transaction coordinator available for sync")]
    private partial void LogNoCoordinator();

    [LoggerMessage(Level = LogLevel.Debug, Message = "Skipping sync - already the coordinator")]
    private partial void LogSameCoordinator();

    [LoggerMessage(Level = LogLevel.Warning, Message = "Previous coordinator {BrokerId} not found in cluster state")]
    private partial void LogPreviousCoordinatorNotFound(int brokerId);

    [LoggerMessage(Level = LogLevel.Information, Message = "Starting transaction state sync from broker {BrokerId}")]
    private partial void LogStartingSync(int brokerId);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Failed to fetch transaction state from broker {BrokerId}")]
    private partial void LogFetchFailed(int brokerId);

    [LoggerMessage(Level = LogLevel.Information, Message = "Transaction state sync from broker {BrokerId} completed: {TransactionCount} transactions imported")]
    private partial void LogSyncCompleted(int brokerId, int transactionCount);

    [LoggerMessage(Level = LogLevel.Error, Message = "Transaction state sync from broker {BrokerId} failed")]
    private partial void LogSyncFailed(int brokerId, Exception ex);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Invalid response size: {Size}")]
    private partial void LogInvalidResponseSize(int size);
}

/// <summary>
/// Configuration options for transaction state synchronization.
/// </summary>
internal sealed class TransactionStateSynchronizerOptions
{
    /// <summary>
    /// Timeout for synchronization in milliseconds.
    /// Default: 30000ms
    /// </summary>
    public int SyncTimeoutMs { get; init; } = 30000;

    /// <summary>
    /// Maximum response size in bytes.
    /// Default: 100MB
    /// </summary>
    public int MaxResponseSizeBytes { get; init; } = 100 * 1024 * 1024;
}

/// <summary>
/// Request to fetch transaction state from another coordinator.
/// </summary>
internal sealed class TransactionStateFetchRequest
{
    public int CorrelationId { get; init; }
    public int TargetBrokerId { get; init; }
}

/// <summary>
/// Snapshot of a transaction for serialization.
/// </summary>
internal sealed class TransactionStateSnapshot
{
    public required string TransactionalId { get; init; }
    public long ProducerId { get; init; }
    public short ProducerEpoch { get; init; }
    public required string State { get; init; }
    public int TransactionTimeoutMs { get; init; }
    public long LastActivityTime { get; init; }
    public required List<PartitionSnapshot> Partitions { get; init; }
    public required List<string> ConsumerGroups { get; init; }
    public required List<OffsetSnapshot> PendingOffsets { get; init; }
}

internal readonly record struct PartitionSnapshot(string Topic, int Partition);
internal readonly record struct OffsetSnapshot(string GroupId, string Topic, int Partition, long Offset, string? Metadata);
