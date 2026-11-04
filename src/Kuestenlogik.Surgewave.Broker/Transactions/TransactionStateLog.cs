using Kuestenlogik.Surgewave.Broker.Coordination;
using Kuestenlogik.Surgewave.Coordination.Coordinators;
using Kuestenlogik.Surgewave.Core.Replication;
using Kuestenlogik.Surgewave.Core.Storage;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Broker.Transactions;

/// <summary>
/// Transaction state in <c>__transaction_state</c> (#197): a transactional id's transaction is a record in the
/// topic's partition for the id — the partition whose leader coordinates the id — so a broker that takes over a
/// partition takes over its transactions from the log. Leading, loading and durable writes are the
/// <see cref="CoordinatorStateLog"/>'s; the coordinator keeps the transactions and is told when a partition's arrive
/// and when they go.
/// </summary>
public sealed class TransactionStateLog : ICoordinatorStateOwner
{
    private readonly CoordinatorStateLog _log;
    private Action<int, IReadOnlyList<TransactionMetadata>>? _loaded;
    private Action<int>? _unloaded;

    /// <summary>A log over the broker's <c>__transaction_state</c>; it leads nothing until it is told what this broker leads.</summary>
    public TransactionStateLog(
        LogManager logManager,
        RecordBatchSerializer serializer,
        int partitionCount,
        ILogger<TransactionStateLog> logger,
        TimeSpan? commitTimeout = null,
        TimeProvider? timeProvider = null)
    {
        _log = new CoordinatorStateLog(CoordinatorPartitioner.TransactionTopic, partitionCount, this, logManager, serializer, logger, commitTimeout, timeProvider);
    }

    private TransactionStateLog(ILogger? logger)
    {
        _log = CoordinatorStateLog.InMemory(CoordinatorPartitioner.TransactionTopic, this, logger);
    }

    /// <summary>A log without a topic that owns every transactional id, for a coordinator under test.</summary>
    public static TransactionStateLog InMemory(ILogger? logger = null) => new(logger);

    /// <summary>
    /// The coordinator's side: <paramref name="loaded"/> gets a partition's transactions once it has been read,
    /// <paramref name="unloaded"/> is told to let a partition's go. Both run under the log's lock.
    /// </summary>
    internal void Attach(Action<int, IReadOnlyList<TransactionMetadata>> loaded, Action<int> unloaded)
    {
        _loaded = loaded;
        _unloaded = unloaded;
    }

    /// <summary>The partition of <c>__transaction_state</c> a transactional id belongs to.</summary>
    public int PartitionFor(string transactionalId) => _log.PartitionFor(transactionalId);

    /// <summary>Whether this broker coordinates the transactional id now.</summary>
    public CoordinatorOwnership OwnershipOf(string transactionalId) => _log.OwnershipOf(_log.PartitionFor(transactionalId));

    /// <summary>Writes the transaction as it is now; answered once the in-sync replicas have it.</summary>
    /// <remarks>Called with the transaction's lock held, so the record is the state the caller decided on.</remarks>
    internal Task<StateWriteStatus> WriteAsync(TransactionMetadata txn, CancellationToken cancellationToken)
        => _log.WriteAsync(_log.PartitionFor(txn.TransactionalId),
            [(TransactionRecords.Key(txn.TransactionalId), TransactionRecords.Value(txn))], _ => { }, cancellationToken);

    /// <inheritdoc cref="CoordinatorStateLog.OnLeadershipChanged"/>
    public void OnLeadershipChanged(int partition, bool isLeader) => _log.OnLeadershipChanged(partition, isLeader);

    /// <inheritdoc cref="CoordinatorStateLog.FollowLeadershipOf"/>
    public void FollowLeadershipOf(Kuestenlogik.Surgewave.Clustering.Replication.ReplicaManager replicas, IPartitionCommitGate commitGate)
        => _log.FollowLeadershipOf(replicas, commitGate);

    /// <summary>Completes once the id's partition has been read, or at once when it is not loading.</summary>
    public Task WhenLoadedAsync(string transactionalId) => _log.WhenLoadedAsync(_log.PartitionFor(transactionalId));

    /// <inheritdoc cref="CoordinatorStateLog.StartLocalAsync"/>
    public Task StartLocalAsync(CancellationToken cancellationToken) => _log.StartLocalAsync(cancellationToken);

    void ICoordinatorStateOwner.Loaded(int partition, IReadOnlyList<StateRecord> records)
    {
        // The last record of an id is its state; a tombstone ends it.
        var latest = new Dictionary<string, StateRecord>(StringComparer.Ordinal);
        foreach (var record in records)
        {
            if (TransactionRecords.ReadKey(record.Key.Span) is not { } id)
                continue;
            if (record.Value.IsEmpty)
                latest.Remove(id);
            else
                latest[id] = record;
        }

        _loaded?.Invoke(partition, [.. latest.Select(kv => TransactionRecords.ReadValue(kv.Key, kv.Value.Value.Span))]);
    }

    void ICoordinatorStateOwner.Unloaded(int partition) => _unloaded?.Invoke(partition);
}
