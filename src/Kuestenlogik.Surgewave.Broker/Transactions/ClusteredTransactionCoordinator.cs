using System.Collections.Concurrent;
using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.Replication;
using Kuestenlogik.Surgewave.Coordination.Transactions;
using Kuestenlogik.Surgewave.Core;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Microsoft.Extensions.Logging;
using TransactionState = Kuestenlogik.Surgewave.Core.KafkaConstants.TransactionState;

namespace Kuestenlogik.Surgewave.Broker.Transactions;

/// <summary>
/// Cluster-aware transaction coordinator that mirrors the base TransactionCoordinator's
/// lifecycle on the protocol-neutral contracts (#59) and adds marker replication for
/// durability across replicas. Not on the Kafka wire path (only cluster state sync + tests
/// use it directly), so it neutralises its method signatures without implementing the adapter
/// contract. <see cref="ValidateProduceBatch"/> stays Kafka-coupled (produce hot path).
/// </summary>
public sealed class ClusteredTransactionCoordinator : IAsyncDisposable
{
    private readonly ProducerStateManager _producerStateManager;
    private readonly TransactionIndex _transactionIndex;
    private readonly OffsetStore _offsetStore;
    private readonly LogManager _logManager;
    private readonly ILogger<ClusteredTransactionCoordinator> _logger;
    private readonly ConcurrentDictionary<string, TransactionMetadata> _transactionsByTxnId = new();
    private readonly TransactionMarkerWriter _markerWriter;
    private readonly TransactionMarkerReplicator _markerReplicator;
    private readonly TransactionStatePersistence _statePersistence;
    private readonly TransactionTimeoutManager _timeoutManager;
    private readonly ClusterState _clusterState;
    private readonly int _localBrokerId;
    private int _coordinatorEpoch;

    public ClusteredTransactionCoordinator(
        ProducerStateManager producerStateManager,
        LogManager logManager,
        TransactionIndex transactionIndex,
        OffsetStore offsetStore,
        TransactionStateStore stateStore,
        ClusterState clusterState,
        ConnectionPool connectionPool,
        int localBrokerId,
        ILoggerFactory loggerFactory)
    {
        _producerStateManager = producerStateManager;
        _logManager = logManager;
        _transactionIndex = transactionIndex;
        _offsetStore = offsetStore;
        _clusterState = clusterState;
        _localBrokerId = localBrokerId;
        _logger = loggerFactory.CreateLogger<ClusteredTransactionCoordinator>();

        // Create helper components
        _markerWriter = new TransactionMarkerWriter(logManager, _logger);
        _markerReplicator = new TransactionMarkerReplicator(
            connectionPool,
            clusterState,
            localBrokerId,
            loggerFactory.CreateLogger<TransactionMarkerReplicator>());
        _statePersistence = new TransactionStatePersistence(stateStore, producerStateManager, _logger);

        // Load persisted transaction state on startup
        _statePersistence.LoadPersistedState(_transactionsByTxnId);

        // Start timeout manager
        _timeoutManager = new TransactionTimeoutManager(
            _transactionsByTxnId,
            _transactionIndex,
            _markerWriter,
            _logger);
    }

    /// <summary>
    /// Gets or sets the coordinator epoch for fencing stale coordinators.
    /// </summary>
    public int CoordinatorEpoch
    {
        get => _coordinatorEpoch;
        set => _coordinatorEpoch = value;
    }

    public async ValueTask DisposeAsync()
    {
        await _timeoutManager.DisposeAsync();
    }

    /// <summary>
    /// Get the TransactionIndex for fetch filtering.
    /// </summary>
    public TransactionIndex TransactionIndex => _transactionIndex;

    /// <summary>
    /// Records a transactional batch being written. Called from produce path.
    /// Also implicitly adds the partition to the transaction metadata.
    /// </summary>
    public void RecordTransactionalBatch(TopicPartition partition, long producerId, long baseOffset)
    {
        _transactionIndex.RecordTransactionalBatch(partition, producerId, baseOffset);

        // Also track this partition in our transaction metadata
        foreach (var txnMeta in _transactionsByTxnId.Values)
        {
            if (txnMeta.ProducerId == producerId &&
                (txnMeta.State == TransactionState.Ongoing || txnMeta.State == TransactionState.Empty))
            {
                txnMeta.Partitions.Add(partition);
                txnMeta.LastActivityTime = DateTimeOffset.UtcNow;
                if (txnMeta.State == TransactionState.Empty)
                {
                    txnMeta.State = TransactionState.Ongoing;
                }
                break;
            }
        }
    }

    /// <summary>
    /// Validates a produce batch for idempotence (sequence number validation).
    /// </summary>
    public ErrorCode ValidateProduceBatch(long producerId, short epoch, int baseSequence, TopicPartition topicPartition)
    {
        return _producerStateManager.ValidateSequence(producerId, epoch, baseSequence, topicPartition);
    }

    /// <summary>
    /// Handles InitProducerId request for transactional producers.
    /// </summary>
    public async Task<InitProducerIdResult> InitProducerIdAsync(InitProducerIdCommand request, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(request.TransactionalId))
        {
            // Non-transactional idempotent producer
            var (producerId, epoch) = _producerStateManager.AllocateProducerId();
            return new InitProducerIdResult(TxnErrorStatus.None, producerId, epoch);
        }

        // Transactional producer
        var txnMetadata = _transactionsByTxnId.GetOrAdd(request.TransactionalId, txnId =>
        {
            var (pid, ep) = _producerStateManager.AllocateProducerId();
            return new TransactionMetadata
            {
                TransactionalId = txnId,
                ProducerId = pid,
                ProducerEpoch = ep,
                State = TransactionState.Empty,
                TransactionTimeoutMs = request.TransactionTimeoutMs
            };
        });

        // Check for existing transaction that needs cleanup (fencing scenario)
        if (txnMetadata.State == TransactionState.Ongoing ||
            txnMetadata.State == TransactionState.PrepareCommit ||
            txnMetadata.State == TransactionState.PrepareAbort)
        {
            // Producer crash recovery / fencing - abort pending transaction
            if (txnMetadata.Partitions.Count > 0)
            {
                _logger.LogInformation(
                    "Fencing producer {TransactionalId}: aborting in-flight transaction with {PartitionCount} partitions",
                    txnMetadata.TransactionalId,
                    txnMetadata.Partitions.Count);

                // Write markers locally
                var abortOffset = await _markerWriter.WriteMarkersAsync(txnMetadata, commit: false, cancellationToken);

                // Replicate to followers
                var replicationResult = await _markerReplicator.ReplicateMarkersAsync(
                    txnMetadata,
                    commit: false,
                    _coordinatorEpoch,
                    cancellationToken);

                if (!replicationResult.IsSuccess)
                {
                    _logger.LogWarning(
                        "Failed to replicate abort markers for fenced transaction {TransactionalId}: {FailedBrokers}",
                        txnMetadata.TransactionalId,
                        string.Join(", ", replicationResult.FailedBrokers.Keys));
                }

                _transactionIndex.AbortTransaction(txnMetadata.ProducerId, txnMetadata.Partitions, abortOffset);
            }

            // Bump epoch and reset state
            txnMetadata.ProducerEpoch++;
            txnMetadata.State = TransactionState.Empty;
            txnMetadata.Partitions.Clear();
            _producerStateManager.UpdateEpoch(txnMetadata.ProducerId, txnMetadata.ProducerEpoch);
        }

        // Update epoch if client provided one
        if (request.ProducerId == txnMetadata.ProducerId && request.ProducerEpoch >= txnMetadata.ProducerEpoch)
        {
            txnMetadata.ProducerEpoch = (short)(request.ProducerEpoch + 1);
            _producerStateManager.UpdateEpoch(txnMetadata.ProducerId, txnMetadata.ProducerEpoch);
        }

        _statePersistence.PersistState(txnMetadata);

        return new InitProducerIdResult(TxnErrorStatus.None, txnMetadata.ProducerId, txnMetadata.ProducerEpoch);
    }

    /// <summary>
    /// Handles AddPartitionsToTxn request.
    /// </summary>
    public AddPartitionsToTxnResult AddPartitionsToTxn(AddPartitionsToTxnCommand request)
    {
        // Validate request
        var status = ValidateTransactionRequest(request.TransactionalId, request.ProducerId, request.ProducerEpoch, out var txnMetadata);
        if (status != TxnErrorStatus.None)
        {
            var errorTopics = new List<AddPartitionsTopicResult>(request.Topics.Count);
            foreach (var topic in request.Topics)
            {
                var parts = new List<TxnPartitionStatus>(topic.Partitions.Count);
                foreach (var p in topic.Partitions)
                {
                    parts.Add(new TxnPartitionStatus(p, status));
                }
                errorTopics.Add(new AddPartitionsTopicResult(topic.Topic, parts));
            }
            return new AddPartitionsToTxnResult(errorTopics);
        }

        // Start transaction if not already started
        if (txnMetadata!.State == TransactionState.Empty ||
            txnMetadata.State == TransactionState.CompleteCommit ||
            txnMetadata.State == TransactionState.CompleteAbort)
        {
            txnMetadata.State = TransactionState.Ongoing;
            txnMetadata.Partitions.Clear();
            txnMetadata.LastActivityTime = DateTimeOffset.UtcNow;
        }
        else
        {
            txnMetadata.LastActivityTime = DateTimeOffset.UtcNow;
        }

        // Add partitions
        var results = new List<AddPartitionsTopicResult>(request.Topics.Count);
        foreach (var topic in request.Topics)
        {
            var partitionResults = new List<TxnPartitionStatus>(topic.Partitions.Count);
            foreach (var partition in topic.Partitions)
            {
                var tp = new TopicPartition { Topic = topic.Topic, Partition = partition };
                txnMetadata.Partitions.Add(tp);
                partitionResults.Add(new TxnPartitionStatus(partition, TxnErrorStatus.None));
            }
            results.Add(new AddPartitionsTopicResult(topic.Topic, partitionResults));
        }

        _statePersistence.PersistState(txnMetadata);

        return new AddPartitionsToTxnResult(results);
    }

    /// <summary>
    /// Handles AddOffsetsToTxn request - adds consumer group offsets to a transaction.
    /// </summary>
    public AddOffsetsToTxnResult AddOffsetsToTxn(AddOffsetsToTxnCommand request)
    {
        var status = ValidateTransactionRequest(request.TransactionalId, request.ProducerId, request.ProducerEpoch, out var txnMetadata);
        if (status != TxnErrorStatus.None)
        {
            return new AddOffsetsToTxnResult(status);
        }

        // Transaction must be ongoing or empty
        if (txnMetadata!.State != TransactionState.Ongoing && txnMetadata.State != TransactionState.Empty)
        {
            return new AddOffsetsToTxnResult(TxnErrorStatus.InvalidTxnState);
        }

        txnMetadata.ConsumerGroups.Add(request.GroupId);
        if (txnMetadata.State == TransactionState.Empty)
        {
            txnMetadata.State = TransactionState.Ongoing;
        }
        txnMetadata.LastActivityTime = DateTimeOffset.UtcNow;

        _logger.LogDebug("Added consumer group {GroupId} to transaction {TransactionalId}",
            request.GroupId, request.TransactionalId);

        _statePersistence.PersistState(txnMetadata);

        return new AddOffsetsToTxnResult(TxnErrorStatus.None);
    }

    /// <summary>
    /// Handles TxnOffsetCommit request - commits consumer offsets as part of a transaction.
    /// </summary>
    public TxnOffsetCommitResult TxnOffsetCommit(TxnOffsetCommitCommand request)
    {
        // KIP-1319 (v6): resolve each topic's identity once so the
        // pending-offset store sees a Name and the response wire can echo
        // back either Name (v0-5) or TopicId (v6+).
        var resolved = new List<(string? Name, Guid TopicId, TxnErrorStatus ResolveError, IReadOnlyList<TxnOffsetCommitPartition> Partitions)>(request.Topics.Count);
        foreach (var t in request.Topics)
        {
            string? name = t.Name;
            var topicId = t.TopicId;
            var resolveError = TxnErrorStatus.None;
            if (topicId != Guid.Empty && string.IsNullOrEmpty(name))
            {
                var meta = _logManager.GetTopicMetadataById(topicId);
                if (meta is null)
                {
                    resolveError = TxnErrorStatus.UnknownTopicId;
                }
                else
                {
                    name = meta.Name;
                }
            }
            else if (!string.IsNullOrEmpty(name) && topicId == Guid.Empty)
            {
                topicId = _logManager.GetTopicId(name);
            }
            resolved.Add((name, topicId, resolveError, t.Partitions));
        }

        var status = ValidateTransactionRequest(request.TransactionalId, request.ProducerId, request.ProducerEpoch, out var txnMetadata);
        if (status != TxnErrorStatus.None)
        {
            var errorTopics = new List<TxnOffsetCommitTopicResult>(resolved.Count);
            foreach (var r in resolved)
            {
                var parts = new List<TxnOffsetCommitPartitionResult>(r.Partitions.Count);
                foreach (var p in r.Partitions)
                {
                    parts.Add(new TxnOffsetCommitPartitionResult(p.Partition, status));
                }
                errorTopics.Add(new TxnOffsetCommitTopicResult { Name = r.Name, TopicId = r.TopicId, Partitions = parts });
            }
            return new TxnOffsetCommitResult(errorTopics);
        }

        var topics = new List<TxnOffsetCommitTopicResult>(resolved.Count);
        var stagedCount = 0;
        foreach (var r in resolved)
        {
            var partitionResults = new List<TxnOffsetCommitPartitionResult>(r.Partitions.Count);
            foreach (var partition in r.Partitions)
            {
                if (r.ResolveError != TxnErrorStatus.None)
                {
                    partitionResults.Add(new TxnOffsetCommitPartitionResult(partition.Partition, r.ResolveError));
                    continue;
                }
                txnMetadata!.PendingOffsets.Add(new PendingTxnOffset
                {
                    GroupId = request.GroupId,
                    Topic = r.Name ?? string.Empty,
                    Partition = partition.Partition,
                    Offset = partition.CommittedOffset,
                    Metadata = partition.Metadata,
                });
                stagedCount++;
                partitionResults.Add(new TxnOffsetCommitPartitionResult(partition.Partition, TxnErrorStatus.None));
            }
            topics.Add(new TxnOffsetCommitTopicResult { Name = r.Name, TopicId = r.TopicId, Partitions = partitionResults });
        }

        _logger.LogDebug("Staged {Count} offsets for transaction {TransactionalId}, group {GroupId}",
            stagedCount, request.TransactionalId, request.GroupId);

        _statePersistence.PersistState(txnMetadata!);

        return new TxnOffsetCommitResult(topics);
    }

    /// <summary>
    /// Handles EndTxn request (commit or abort) with cluster-aware marker replication.
    /// </summary>
    public async Task<EndTxnResult> EndTxnAsync(EndTxnCommand request, CancellationToken cancellationToken)
    {
        var status = ValidateTransactionRequest(request.TransactionalId, request.ProducerId, request.ProducerEpoch, out var txnMetadata);
        if (status != TxnErrorStatus.None)
        {
            return new EndTxnResult(status);
        }

        if (txnMetadata!.State != TransactionState.Ongoing)
        {
            return new EndTxnResult(TxnErrorStatus.InvalidTxnState);
        }

        // Transition to prepare state
        txnMetadata.State = request.Committed ? TransactionState.PrepareCommit : TransactionState.PrepareAbort;
        _statePersistence.PersistState(txnMetadata);

        // Write transaction markers locally
        var markerOffset = await _markerWriter.WriteMarkersAsync(txnMetadata, request.Committed, cancellationToken);

        // Replicate markers to followers (critical for durability)
        var replicationResult = await _markerReplicator.ReplicateMarkersAsync(
            txnMetadata,
            request.Committed,
            _coordinatorEpoch,
            cancellationToken);

        if (!replicationResult.IsSuccess && txnMetadata.Partitions.Count > 0)
        {
            _logger.LogWarning(
                "Transaction {TransactionalId}: Marker replication incomplete. Succeeded: {SuccessCount}, Failed: {FailedCount}",
                txnMetadata.TransactionalId,
                replicationResult.SuccessfulBrokers.Count,
                replicationResult.FailedBrokers.Count);

            // In strict mode, we might want to fail the transaction here
            // For now, proceed with best-effort (markers are written locally)
        }

        // Update TransactionIndex and commit/discard offsets
        if (request.Committed)
        {
            _transactionIndex.CommitTransaction(txnMetadata.ProducerId, txnMetadata.Partitions, markerOffset);

            foreach (var pendingOffset in txnMetadata.PendingOffsets)
            {
                _offsetStore.CommitOffset(pendingOffset.GroupId, pendingOffset.Topic, pendingOffset.Partition, pendingOffset.Offset);
                _logger.LogDebug("Committed transactional offset for group {GroupId}, {Topic}-{Partition} at offset {Offset}",
                    pendingOffset.GroupId, pendingOffset.Topic, pendingOffset.Partition, pendingOffset.Offset);
            }
        }
        else
        {
            _transactionIndex.AbortTransaction(txnMetadata.ProducerId, txnMetadata.Partitions, markerOffset);
            if (txnMetadata.PendingOffsets.Count > 0)
            {
                _logger.LogDebug("Discarded {Count} pending offsets due to transaction abort", txnMetadata.PendingOffsets.Count);
            }
        }

        // Complete transaction
        txnMetadata.State = request.Committed ? TransactionState.CompleteCommit : TransactionState.CompleteAbort;
        txnMetadata.Partitions.Clear();
        txnMetadata.ConsumerGroups.Clear();
        txnMetadata.PendingOffsets.Clear();

        _statePersistence.PersistState(txnMetadata);

        return new EndTxnResult(TxnErrorStatus.None);
    }

    /// <summary>
    /// Validates a transaction request's transactional ID, producer ID, and epoch.
    /// </summary>
    private TxnErrorStatus ValidateTransactionRequest(string? transactionalId, long producerId, short producerEpoch, out TransactionMetadata? txnMetadata)
    {
        txnMetadata = null;

        if (string.IsNullOrEmpty(transactionalId))
            return TxnErrorStatus.InvalidTxnState;

        if (!_transactionsByTxnId.TryGetValue(transactionalId, out txnMetadata))
            return TxnErrorStatus.UnknownProducerId;

        if (producerId != txnMetadata.ProducerId || producerEpoch != txnMetadata.ProducerEpoch)
        {
            return producerEpoch < txnMetadata.ProducerEpoch
                ? TxnErrorStatus.InvalidProducerEpoch
                : TxnErrorStatus.UnknownProducerId;
        }

        return TxnErrorStatus.None;
    }

    /// <summary>
    /// Lists transactions with optional filters.
    /// </summary>
    public IReadOnlyList<TransactionListing> ListTransactions(IEnumerable<string>? statesFilter = null, IEnumerable<long>? producerIdFilter = null)
    {
        var stateSet = statesFilter?.Select(s => Enum.TryParse<TransactionState>(s, ignoreCase: true, out var state) ? state : TransactionState.Empty)
            .Where(s => s != TransactionState.Empty)
            .ToHashSet();
        var producerIdSet = producerIdFilter?.ToHashSet();

        return _transactionsByTxnId.Values
            .Where(t => stateSet == null || stateSet.Count == 0 || stateSet.Contains(t.State))
            .Where(t => producerIdSet == null || producerIdSet.Count == 0 || producerIdSet.Contains(t.ProducerId))
            .Select(t => new TransactionListing(t.TransactionalId, t.ProducerId, t.State.ToString()))
            .ToList();
    }

    /// <summary>
    /// Describes transactions by transactional IDs.
    /// </summary>
    public IReadOnlyList<TransactionDescription> DescribeTransactions(IEnumerable<string> transactionalIds)
    {
        var result = new List<TransactionDescription>();
        foreach (var txnId in transactionalIds)
        {
            if (_transactionsByTxnId.TryGetValue(txnId, out var txn))
            {
                result.Add(new TransactionDescription(
                    txn.TransactionalId,
                    txn.State.ToString(),
                    txn.ProducerId,
                    txn.ProducerEpoch,
                    txn.TransactionTimeoutMs,
                    txn.LastActivityTime.ToUnixTimeMilliseconds(),
                    txn.Partitions.Select(p => (p.Topic, p.Partition)).ToList(),
                    0));
            }
            else
            {
                result.Add(new TransactionDescription(
                    txnId,
                    "Unknown",
                    -1,
                    -1,
                    0,
                    0,
                    new List<(string, int)>(),
                    59)); // UNKNOWN_PRODUCER_ID
            }
        }
        return result;
    }

    /// <summary>
    /// Gets all active transaction metadata. Used for state synchronization.
    /// </summary>
    internal IEnumerable<TransactionMetadata> GetAllTransactions()
    {
        return _transactionsByTxnId.Values;
    }

    /// <summary>
    /// Imports transaction state from another coordinator (used during leader handoff).
    /// </summary>
    internal void ImportTransactionState(TransactionMetadata metadata)
    {
        _transactionsByTxnId[metadata.TransactionalId] = metadata;
        _statePersistence.PersistState(metadata);
    }
}
