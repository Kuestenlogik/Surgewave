using System.Collections.Concurrent;
using Kuestenlogik.Surgewave.Coordination.Transactions;
using Kuestenlogik.Surgewave.Core;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Broker.Transactions;
using Microsoft.Extensions.Logging;
using TransactionState = Kuestenlogik.Surgewave.Core.KafkaConstants.TransactionState;

namespace Kuestenlogik.Surgewave.Broker;

/// <summary>
/// Coordinates transactions for transactional producers.
/// Manages transaction state and implements the protocol-neutral <see cref="ITransactionCoordinator"/>
/// wire-API surface (#59); the Kafka DTO conversion lives in the <c>TransactionApiHandler</c> adapter.
/// The produce-hot-path helpers (<see cref="ValidateProduceBatch"/> / <see cref="RecordTransactionalBatch"/>)
/// are protocol-neutral (b2): the former returns <c>ProduceSequenceStatus</c>, the latter takes
/// only neutral types; the Kafka Produce handler maps to the wire error code at the boundary.
/// </summary>
public sealed class TransactionCoordinator : IAsyncDisposable, ITransactionCoordinator, IProduceTransactionCoordinator, ITransactionMarkerSink
{
    private readonly ProducerStateManager _producerStateManager;
    private readonly TransactionIndex _transactionIndex;
    private readonly OffsetStore _offsetStore;
    private readonly ILogger<TransactionCoordinator> _logger;
    private readonly LogManager _logManager;
    private readonly ConcurrentDictionary<string, TransactionMetadata> _transactionsByTxnId = new();
    private readonly TransactionMarkerWriter _markerWriter;
    private readonly TransactionStatePersistence _statePersistence;
    private readonly TransactionTimeoutManager _timeoutManager;

    // #72 Inc7 — best-effort inter-broker marker replication (merged from the now-deleted
    // ClusteredTransactionCoordinator). Set once during startup wiring, before the coordinator serves
    // requests; null → markers are written locally only (single broker / no cluster transport),
    // preserving the prior behavior.
    private ITransactionMarkerReplicator? _markerReplicator;
    private int _coordinatorEpoch;

    // #72 Inc8 — bounded, partition-scoped retry of the best-effort marker replication.
    private const int MarkerReplicationMaxAttempts = 3;
    private static readonly TimeSpan MarkerReplicationRetryBackoff = TimeSpan.FromMilliseconds(50);

    public TransactionCoordinator(
        ProducerStateManager producerStateManager,
        LogManager logManager,
        TransactionIndex transactionIndex,
        OffsetStore offsetStore,
        TransactionStateStore stateStore,
        ILogger<TransactionCoordinator> logger)
    {
        _producerStateManager = producerStateManager;
        _transactionIndex = transactionIndex;
        _offsetStore = offsetStore;
        _logger = logger;
        _logManager = logManager;

        // Create helper components
        _markerWriter = new TransactionMarkerWriter(logManager, logger);
        _statePersistence = new TransactionStatePersistence(stateStore, producerStateManager, logger);

        // Load persisted transaction state on startup
        _statePersistence.LoadPersistedState(_transactionsByTxnId);

        // Start timeout manager: aborts expired transactions and finishes prepared ones
        _timeoutManager = new TransactionTimeoutManager(ExpireTransactionsAsync, logger);
    }

    public async ValueTask DisposeAsync()
    {
        await _timeoutManager.DisposeAsync();
    }

    /// <summary>
    /// #72 Inc7 — the coordinator epoch used to fence stale WriteTxnMarkers on the inter-broker wire.
    /// Set by the controller wiring; defaults to 0.
    /// </summary>
    public int CoordinatorEpoch
    {
        get => _coordinatorEpoch;
        set => _coordinatorEpoch = value;
    }

    /// <summary>
    /// #72 Inc7 — wire the best-effort inter-broker marker replicator (the gated native / Kafka-wire
    /// transport). Called once during startup before the coordinator serves requests; a null replicator
    /// keeps markers local-only.
    /// </summary>
    public void SetMarkerReplicator(ITransactionMarkerReplicator? markerReplicator)
        => _markerReplicator = markerReplicator;

    /// <summary>
    /// #72 Inc7/Inc8 — replicate the just-written commit/abort markers to follower brokers, best-effort
    /// (merged from the deleted ClusteredTransactionCoordinator), with a bounded retry of ONLY the
    /// not-yet-acked partitions (#72 Inc8). EndTxn latency now depends on the remote transport, but the
    /// replication NEVER fails the transaction — the markers are already durable in the local log, so a
    /// failed result, a transport throw, a cancelled replication, or exhausting the retries are all
    /// swallowed and logged. No-op when no replicator is wired or there are no partitions.
    /// <para>
    /// The retry is partition-scoped because the receive side does NOT dedup markers: re-sending a
    /// partition whose marker was already applied would double-write it, so only partitions the transport
    /// did not confirm (per the Inc6 per-partition outcomes) are retried. The happy path (all acked on the
    /// first attempt) adds no latency; the retry only engages on an actual replication shortfall and is
    /// bounded so EndTxn latency stays capped.
    /// </para>
    /// </summary>
    private async Task ReplicateMarkersBestEffortAsync(TransactionMetadata txnMetadata, bool commit, CancellationToken ct)
    {
        if (_markerReplicator is null || txnMetadata.Partitions.Count == 0)
            return;

        IReadOnlyList<TopicPartition> pending = [.. txnMetadata.Partitions];
        for (var attempt = 1; ; attempt++)
        {
            MarkerReplicationResult result;
            try
            {
                result = await _markerReplicator.ReplicateMarkersAsync(
                    txnMetadata.TransactionalId, txnMetadata.ProducerId, txnMetadata.ProducerEpoch,
                    pending, commit, _coordinatorEpoch, ct);
            }
            catch (Exception ex)
            {
                // Best-effort: the local markers are already durable, so a transport throw or a cancelled
                // replication must NEVER fail a commit. Swallow and log.
                _logger.LogWarning(ex,
                    "Transaction {TransactionalId}: marker replication threw on attempt {Attempt} — proceeding best-effort, markers are durable locally",
                    txnMetadata.TransactionalId, attempt);
                return;
            }

            var stillPending = NotAcked(result, pending);
            if (stillPending.Count == 0)
            {
                // A coarse transport (e.g. the Kafka wire) may report incomplete without a per-partition
                // failure target; surface it, but there is nothing dedup-safe to retry.
                if (!result.IsSuccess)
                    _logger.LogWarning(
                        "Transaction {TransactionalId}: marker replication reported incomplete with no per-partition retry target — proceeding best-effort, markers are durable locally",
                        txnMetadata.TransactionalId);
                return;
            }

            if (attempt >= MarkerReplicationMaxAttempts)
            {
                _logger.LogWarning(
                    "Transaction {TransactionalId}: {Count} partition(s) still unreplicated after {Attempts} attempt(s) — proceeding best-effort, markers are durable locally",
                    txnMetadata.TransactionalId, stillPending.Count, attempt);
                return;
            }

            pending = stillPending;
            try
            {
                await Task.Delay(MarkerReplicationRetryBackoff, ct);
            }
            catch (Exception)
            {
                return; // cancelled during backoff — best-effort
            }
        }
    }

    /// <summary>
    /// #72 Inc8 — the partitions a retry may re-send: ONLY those the transport EXPLICITLY reported as not
    /// applied (<see cref="MarkerPartitionOutcome.Failed"/> / <see cref="MarkerPartitionOutcome.SkippedNoLeader"/>
    /// / <see cref="MarkerPartitionOutcome.SkippedUnknownPartition"/>). A partition marked
    /// <see cref="MarkerPartitionOutcome.Replicated"/> or <see cref="MarkerPartitionOutcome.LocalLeader"/> —
    /// or ABSENT from the outcomes (e.g. a trivial <c>Success()</c> with no per-partition detail) — is
    /// treated as acked and never re-sent, so the receive side (which does not dedup) never double-writes a
    /// marker the transport did not report as failed.
    /// </summary>
    private static List<TopicPartition> NotAcked(MarkerReplicationResult result, IReadOnlyList<TopicPartition> attempted)
    {
        var notAcked = new List<TopicPartition>();
        foreach (var tp in attempted)
        {
            if (result.PartitionOutcomes.TryGetValue(tp, out var outcome)
                && outcome is MarkerPartitionOutcome.Failed
                    or MarkerPartitionOutcome.SkippedNoLeader
                    or MarkerPartitionOutcome.SkippedUnknownPartition)
            {
                notAcked.Add(tp);
            }
        }
        return notAcked;
    }

    /// <summary>
    /// Get the TransactionIndex for fetch filtering.
    /// </summary>
    public TransactionIndex TransactionIndex => _transactionIndex;

    /// <summary>
    /// Filters record batches for READ_COMMITTED isolation. Forwards to the concrete
    /// <see cref="TransactionIndex"/> so the neutral <see cref="IProduceTransactionCoordinator"/>
    /// keeps that type off the data-plane handler's surface (#59 b4-tier2).
    /// </summary>
    public List<byte[]> FilterForReadCommitted(TopicPartition partition, List<byte[]> batches, long highWatermark)
        => _transactionIndex.FilterForReadCommitted(partition, batches, highWatermark);

    /// <summary>
    /// Filters record batches for READ_UNCOMMITTED isolation. Forwards to the concrete
    /// <see cref="TransactionIndex"/>.
    /// </summary>
    public List<byte[]> FilterForReadUncommitted(List<byte[]> batches)
        => _transactionIndex.FilterForReadUncommitted(batches);

    /// <summary>
    /// Commits the producer's transaction across the given partitions (inter-broker
    /// WriteTxnMarkers path). Forwards to the concrete <see cref="TransactionIndex"/> so the
    /// neutral <see cref="ITransactionMarkerSink"/> keeps that type off the moved handler (#59 b5).
    /// </summary>
    public void CommitTransaction(long producerId, IEnumerable<TopicPartition> partitions, long commitOffset)
        => _transactionIndex.CommitTransaction(producerId, partitions, commitOffset);

    /// <summary>
    /// Aborts the producer's transaction across the given partitions. Forwards to the concrete
    /// <see cref="TransactionIndex"/>.
    /// </summary>
    public void AbortTransaction(long producerId, IEnumerable<TopicPartition> partitions, long abortOffset)
        => _transactionIndex.AbortTransaction(producerId, partitions, abortOffset);

    /// <summary>
    /// Records a transactional batch being written. Called from produce path, after
    /// <see cref="VerifyTransactionalBatch"/> has put the partition into its transaction.
    /// </summary>
    public void RecordTransactionalBatch(TopicPartition partition, long producerId, long baseOffset)
        => _transactionIndex.RecordTransactionalBatch(partition, producerId, baseOffset);

    /// <inheritdoc />
    public TxnErrorStatus VerifyTransactionalBatch(
        string transactionalId, long producerId, short producerEpoch, TopicPartition partition, TransactionVersion version)
        => WithTransaction(transactionalId, producerId, producerEpoch, txn =>
        {
            if (version >= TransactionVersion.Tv2)
            {
                txn.ClientTransactionVersion = version;
                return AddToTransaction(txn, partition, groupId: null);
            }

            // Kafka's verification (KIP-890 part 1): the partition was added with AddPartitionsToTxn.
            if (txn.State is TransactionState.PrepareCommit or TransactionState.PrepareAbort)
                return TxnErrorStatus.ConcurrentTransactions;
            return txn.State == TransactionState.Ongoing && txn.Partitions.Contains(partition)
                ? TxnErrorStatus.None
                : TxnErrorStatus.TransactionAbortable;
        });

    /// <summary>
    /// Validates a produce batch for idempotence (sequence number validation). Produce-hot-path;
    /// returns the neutral <c>ProduceSequenceStatus</c> — the Kafka Produce handler maps it to a
    /// wire <c>ErrorCode</c> at the boundary (b2).
    /// </summary>
    public ProduceSequenceCheck ValidateProduceBatch(
        long producerId, short epoch, int baseSequence, int lastOffsetDelta, TopicPartition topicPartition,
        bool partitionNeverWritten)
    {
        return _producerStateManager.ValidateSequence(producerId, epoch, baseSequence, lastOffsetDelta, topicPartition, partitionNeverWritten);
    }

    public void CommitProduceBatch(
        long producerId, short epoch, int baseSequence, int lastOffsetDelta,
        TopicPartition topicPartition, long baseOffset)
    {
        _producerStateManager.CommitSequence(
            producerId, epoch, baseSequence, lastOffsetDelta, topicPartition, baseOffset);
    }

    /// <summary>
    /// Handles InitProducerId for transactional producers.
    /// </summary>
    public async Task<InitProducerIdResult> InitProducerIdAsync(InitProducerIdCommand request, CancellationToken cancellationToken)
    {
        // KIP-939 two-phase commit is not offered: answered as Kafka answers it with
        // transaction.two.phase.commit.enable=false, and keeping a prepared transaction as Kafka does not yet.
        if (request.Enable2Pc)
            return new InitProducerIdResult(TxnErrorStatus.TransactionalIdAuthorizationFailed, -1, -1);
        if (request.KeepPreparedTxn)
            return new InitProducerIdResult(TxnErrorStatus.UnsupportedVersion, -1, -1);

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

        await txnMetadata.Lock.WaitAsync(cancellationToken);
        try
        {
            // KIP-892 Server-Side Defense: the client never raises the epoch. If the request
            // names a producer-id at all, it must match the current incarnation; an older
            // epoch from the same producer-id means a zombie that recovered after we already
            // fenced it.
            //
            // A request with ProducerId == -1 (no prior identity) is the "fresh init" path;
            // we re-allocate / bump on top of the current state instead of trusting client
            // input. A retry with the exact current (pid, epoch) is idempotent and returns
            // the current values.
            const long NoProducerId = -1;
            const short NoProducerEpoch = -1;
            if (request.ProducerId != NoProducerId)
            {
                if (request.ProducerId != txnMetadata.ProducerId
                    || request.ProducerEpoch < txnMetadata.ProducerEpoch)
                {
                    _logger.LogInformation(
                        "KIP-892 fence: rejecting InitProducerId for {TransactionalId} (zombie pid={ZombiePid}, epoch={ZombieEpoch}; current pid={Pid}, epoch={Epoch})",
                        txnMetadata.TransactionalId,
                        request.ProducerId,
                        request.ProducerEpoch,
                        txnMetadata.ProducerId,
                        txnMetadata.ProducerEpoch);

                    return new InitProducerIdResult(TxnErrorStatus.InvalidProducerEpoch, txnMetadata.ProducerId, txnMetadata.ProducerEpoch);
                }

                if (request.ProducerEpoch == txnMetadata.ProducerEpoch
                    && txnMetadata.State == TransactionState.Empty)
                {
                    // Idempotent retry from the current incarnation, nothing in flight — return as-is.
                    return new InitProducerIdResult(TxnErrorStatus.None, txnMetadata.ProducerId, txnMetadata.ProducerEpoch);
                }
            }

            // Markers of an ending transaction are being written; the client retries once it completed.
            if (txnMetadata.State is TransactionState.PrepareCommit or TransactionState.PrepareAbort)
            {
                return new InitProducerIdResult(TxnErrorStatus.ConcurrentTransactions, NoProducerId, NoProducerEpoch);
            }

            if (txnMetadata.State == TransactionState.Ongoing)
            {
                // Producer crash recovery / fencing: abort the in-flight transaction. The epoch is bumped
                // first, so the abort markers carry the epoch that fences the previous incarnation; a producer
                // id rotation (epoch exhausted) applies only after them, since the markers must name the id
                // the transaction's batches were written with.
                PrepareEpochBump(txnMetadata);

                if (txnMetadata.Partitions.Count > 0)
                {
                    _logger.LogInformation(
                        "Fencing producer {TransactionalId}: aborting in-flight transaction with {PartitionCount} partitions",
                        txnMetadata.TransactionalId,
                        txnMetadata.Partitions.Count);

                    var abortOffset = await _markerWriter.WriteMarkersAsync(txnMetadata, commit: false, cancellationToken);
                    _transactionIndex.AbortTransaction(txnMetadata.ProducerId, txnMetadata.Partitions, abortOffset);

                    // #72 Inc7 — replicate the fencing abort markers best-effort before the partitions are cleared.
                    await ReplicateMarkersBestEffortAsync(txnMetadata, commit: false, cancellationToken);
                }

                ClearTransactionContents(txnMetadata);
                ApplyEpochBump(txnMetadata);
            }
            else if (request.ProducerId == NoProducerId && request.ProducerEpoch == NoProducerEpoch)
            {
                // Fresh-init from a brand-new producer over an existing transactional-id
                // with no in-flight txn: bump the epoch so any zombie still using the
                // previous epoch is fenced on its very next request. Server-owned, not
                // client-driven.
                BumpEpoch(txnMetadata);
            }

            txnMetadata.State = TransactionState.Empty;
            txnMetadata.TransactionTimeoutMs = request.TransactionTimeoutMs;
            _statePersistence.PersistState(txnMetadata);

            return new InitProducerIdResult(TxnErrorStatus.None, txnMetadata.ProducerId, txnMetadata.ProducerEpoch);
        }
        finally
        {
            txnMetadata.Lock.Release();
        }
    }

    /// <summary>
    /// Handles AddPartitionsToTxn.
    /// </summary>
    public AddPartitionsToTxnResult AddPartitionsToTxn(AddPartitionsToTxnCommand request)
    {
        TxnErrorStatus Add(TransactionMetadata txn)
        {
            foreach (var topic in request.Topics)
            {
                foreach (var partition in topic.Partitions)
                {
                    var status = AddToTransaction(txn, new TopicPartition { Topic = topic.Topic, Partition = partition }, groupId: null);
                    if (status != TxnErrorStatus.None)
                        return status;
                }
            }
            return TxnErrorStatus.None;
        }

        var status = WithTransaction(request.TransactionalId, request.ProducerId, request.ProducerEpoch, Add);

        var results = new List<AddPartitionsTopicResult>(request.Topics.Count);
        foreach (var topic in request.Topics)
        {
            var partitionResults = new List<TxnPartitionStatus>(topic.Partitions.Count);
            foreach (var partition in topic.Partitions)
            {
                partitionResults.Add(new TxnPartitionStatus(partition, status));
            }
            results.Add(new AddPartitionsTopicResult(topic.Topic, partitionResults));
        }
        return new AddPartitionsToTxnResult(results);
    }

    /// <summary>
    /// Handles AddOffsetsToTxn - adds consumer group offsets to a transaction.
    /// </summary>
    public AddOffsetsToTxnResult AddOffsetsToTxn(AddOffsetsToTxnCommand request)
    {
        var status = WithTransaction(request.TransactionalId, request.ProducerId, request.ProducerEpoch,
            txn => AddToTransaction(txn, partition: null, request.GroupId));

        if (status == TxnErrorStatus.None)
        {
            _logger.LogDebug("Added consumer group {GroupId} to transaction {TransactionalId}",
                request.GroupId, request.TransactionalId);
        }
        return new AddOffsetsToTxnResult(status);
    }

    /// <summary>
    /// Handles TxnOffsetCommit - commits consumer offsets as part of a transaction.
    /// </summary>
    public TxnOffsetCommitResult TxnOffsetCommit(TxnOffsetCommitCommand request)
    {
        // Resolve each topic to its (Name, TopicId) pair once so both the
        // pending-offset store (name-keyed) and the v6 response wire (id-keyed)
        // see consistent values. KIP-1319 (v6) sends only TopicId on the wire;
        // pre-v6 sends only Name.
        var resolved = new List<(string? Name, Guid TopicId, TxnErrorStatus ResolveError, IReadOnlyList<TxnOffsetCommitPartition> Partitions)>(request.Topics.Count);
        foreach (var t in request.Topics)
        {
            string? name = t.Name;
            var topicId = t.TopicId;
            var resolveError = TxnErrorStatus.None;
            if (topicId != Guid.Empty && string.IsNullOrEmpty(name))
            {
                // v6 path — resolve TopicId → Name via the log manager.
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
                // Pre-v6 path — fill TopicId from the log manager so the
                // response side can echo it back at v6 if needed.
                topicId = _logManager.GetTopicId(name);
            }
            resolved.Add((name, topicId, resolveError, t.Partitions));
        }

        var stagedCount = 0;
        var partitionStatus = new Dictionary<(int Topic, int Partition), TxnErrorStatus>();

        TxnErrorStatus Stage(TransactionMetadata txn)
        {
            if (txn.State is TransactionState.PrepareCommit or TransactionState.PrepareAbort)
                return TxnErrorStatus.ConcurrentTransactions;

            if (request.TransactionVersion >= TransactionVersion.Tv2)
            {
                // V2 sends no AddOffsetsToTxn: the commit puts the group into the transaction.
                txn.ClientTransactionVersion = request.TransactionVersion;
                var added = AddToTransaction(txn, partition: null, request.GroupId);
                if (added != TxnErrorStatus.None)
                    return added;
            }
            else if (txn.State != TransactionState.Ongoing || !txn.ConsumerGroups.Contains(request.GroupId))
            {
                // Kafka's verification: the group was added with AddOffsetsToTxn.
                return TxnErrorStatus.TransactionAbortable;
            }

            for (var i = 0; i < resolved.Count; i++)
            {
                var r = resolved[i];
                foreach (var partition in r.Partitions)
                {
                    if (r.ResolveError != TxnErrorStatus.None)
                    {
                        partitionStatus[(i, partition.Partition)] = r.ResolveError;
                        continue;
                    }
                    txn.PendingOffsets.Add(new PendingTxnOffset
                    {
                        GroupId = request.GroupId,
                        Topic = r.Name ?? string.Empty,
                        Partition = partition.Partition,
                        Offset = partition.CommittedOffset,
                        Metadata = partition.Metadata,
                    });
                    stagedCount++;
                }
            }
            _statePersistence.PersistState(txn);
            return TxnErrorStatus.None;
        }

        var status = WithTransaction(request.TransactionalId, request.ProducerId, request.ProducerEpoch, Stage);

        var topics = new List<TxnOffsetCommitTopicResult>(resolved.Count);
        for (var i = 0; i < resolved.Count; i++)
        {
            var r = resolved[i];
            var partitionResults = new List<TxnOffsetCommitPartitionResult>(r.Partitions.Count);
            foreach (var p in r.Partitions)
            {
                var partitionResult = status != TxnErrorStatus.None
                    ? status
                    : partitionStatus.GetValueOrDefault((i, p.Partition), TxnErrorStatus.None);
                partitionResults.Add(new TxnOffsetCommitPartitionResult(p.Partition, partitionResult));
            }
            topics.Add(new TxnOffsetCommitTopicResult { Name = r.Name, TopicId = r.TopicId, Partitions = partitionResults });
        }

        if (status == TxnErrorStatus.None)
        {
            _logger.LogDebug("Staged {Count} offsets for transaction {TransactionalId}, group {GroupId}",
                stagedCount, request.TransactionalId, request.GroupId);
        }
        return new TxnOffsetCommitResult(topics);
    }

    /// <summary>
    /// Handles EndTxn (commit or abort), with Kafka's answers for transactions V1 and V2
    /// (<see cref="EndTxnTransition"/>). Under V2 the epoch is bumped and returned.
    /// </summary>
    public async Task<EndTxnResult> EndTxnAsync(EndTxnCommand request, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(request.TransactionalId))
            return new EndTxnResult(TxnErrorStatus.InvalidRequest);
        if (!_transactionsByTxnId.TryGetValue(request.TransactionalId, out var txn))
            return new EndTxnResult(TxnErrorStatus.InvalidProducerIdMapping);

        await txn.Lock.WaitAsync(cancellationToken);
        try
        {
            var decision = EndTxnTransition.Decide(txn, request.ProducerId, request.ProducerEpoch, request.Committed, request.TransactionVersion);
            switch (decision.Outcome)
            {
                case EndTxnOutcome.Refuse:
                    return new EndTxnResult(decision.Error);
                case EndTxnOutcome.AlreadyDone:
                    return new EndTxnResult(TxnErrorStatus.None, txn.ProducerId, txn.ProducerEpoch);
            }

            if (decision.Empty)
            {
                _logger.LogDebug("Ending empty transaction {TransactionalId}", txn.TransactionalId);
                txn.TxnStartTime = DateTimeOffset.UtcNow;
            }

            txn.ClientTransactionVersion = request.TransactionVersion;
            if (request.TransactionVersion >= TransactionVersion.Tv2)
            {
                PrepareEpochBump(txn);
            }
            Prepare(txn, request.Committed);
        }
        finally
        {
            txn.Lock.Release();
        }

        var (producerId, producerEpoch) = await WriteMarkersAndCompleteAsync(txn, request.Committed, cancellationToken);
        return new EndTxnResult(TxnErrorStatus.None, producerId, producerEpoch);
    }

    /// <summary>
    /// Aborts the transactions that outlived their timeout and finishes the ones left in PrepareCommit /
    /// PrepareAbort (a restart, or an EndTxn whose marker writing was cancelled). The timeout abort fences
    /// the producer: the epoch is bumped, so its next produce is refused and a commit still in flight is
    /// answered PRODUCER_FENCED.
    /// </summary>
    internal async Task ExpireTransactionsAsync(DateTimeOffset now, CancellationToken cancellationToken)
    {
        foreach (var txn in _transactionsByTxnId.Values)
        {
            bool commit;
            await txn.Lock.WaitAsync(cancellationToken);
            try
            {
                if ((txn.State is TransactionState.PrepareCommit or TransactionState.PrepareAbort) && !txn.CompletionInProgress)
                {
                    commit = txn.State == TransactionState.PrepareCommit;
                    txn.CompletionInProgress = true;
                    _logger.LogInformation("Completing transaction {TransactionalId} left in {State}", txn.TransactionalId, txn.State);
                }
                else if (txn.IsTimedOut(now))
                {
                    commit = false;
                    _logger.LogWarning(
                        "Transaction {TransactionalId} timed out after {TimeoutMs}ms, aborting",
                        txn.TransactionalId,
                        txn.TransactionTimeoutMs);
                    PrepareEpochBump(txn);
                    Prepare(txn, commit: false);
                }
                else
                {
                    continue;
                }
            }
            finally
            {
                txn.Lock.Release();
            }

            try
            {
                await WriteMarkersAndCompleteAsync(txn, commit, cancellationToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _logger.LogError(ex, "Failed to complete transaction {TransactionalId}", txn.TransactionalId);
                await txn.Lock.WaitAsync(CancellationToken.None);
                txn.CompletionInProgress = false; // the next pass retries
                txn.Lock.Release();
            }
        }
    }

    /// <summary>
    /// Runs <paramref name="action"/> on the transaction under its lock, after Kafka's checks for a request
    /// from the producer: the transactional id must be known and belong to the producer id, and the epoch must
    /// be the current one.
    /// </summary>
    private TxnErrorStatus WithTransaction(string? transactionalId, long producerId, short producerEpoch, Func<TransactionMetadata, TxnErrorStatus> action)
    {
        if (string.IsNullOrEmpty(transactionalId))
            return TxnErrorStatus.InvalidRequest;
        if (!_transactionsByTxnId.TryGetValue(transactionalId, out var txn))
            return TxnErrorStatus.InvalidProducerIdMapping;

        txn.Lock.Wait();
        try
        {
            if (producerId != txn.ProducerId)
                return TxnErrorStatus.InvalidProducerIdMapping;
            if (producerEpoch != txn.ProducerEpoch)
                return TxnErrorStatus.ProducerFenced;
            return action(txn);
        }
        finally
        {
            txn.Lock.Release();
        }
    }

    /// <summary>
    /// Adds a partition or a consumer group to the transaction, starting it when none is ongoing. Called with the
    /// transaction's lock held.
    /// </summary>
    private TxnErrorStatus AddToTransaction(TransactionMetadata txn, TopicPartition? partition, string? groupId)
    {
        switch (txn.State)
        {
            case TransactionState.PrepareCommit or TransactionState.PrepareAbort:
                return TxnErrorStatus.ConcurrentTransactions;
            case TransactionState.Dead:
                return TxnErrorStatus.InvalidTxnState;
            case TransactionState.Empty or TransactionState.CompleteCommit or TransactionState.CompleteAbort:
                ClearTransactionContents(txn);
                txn.State = TransactionState.Ongoing;
                txn.TxnStartTime = DateTimeOffset.UtcNow;
                break;
        }

        var changed = false;
        if (partition is not null)
            changed |= txn.Partitions.Add(partition.Value);
        if (groupId is not null)
            changed |= txn.ConsumerGroups.Add(groupId);

        txn.LastActivityTime = DateTimeOffset.UtcNow;
        if (changed)
            _statePersistence.PersistState(txn);
        return TxnErrorStatus.None;
    }

    /// <summary>
    /// The epoch bump of an ending transaction under transactions V2 and of a timeout abort. When the epoch is
    /// exhausted, the transaction completes onto a fresh producer id instead (<see cref="TransactionMetadata.NextProducerId"/>).
    /// Called with the lock held.
    /// </summary>
    private void PrepareEpochBump(TransactionMetadata txn)
    {
        txn.LastProducerEpoch = txn.ProducerEpoch;
        if (TransactionMetadata.IsEpochExhausted(txn.ProducerEpoch))
        {
            txn.NextProducerId = _producerStateManager.AllocateProducerId().ProducerId;
        }
        if (txn.ProducerEpoch < short.MaxValue)
        {
            txn.ProducerEpoch++;
        }
    }

    /// <summary>
    /// The epoch bump of InitProducerId: the new incarnation gets the next epoch, or a fresh producer id at epoch 0
    /// when the epoch is exhausted. Called with the lock held.
    /// </summary>
    private void BumpEpoch(TransactionMetadata txn)
    {
        txn.LastProducerEpoch = txn.ProducerEpoch;
        if (TransactionMetadata.IsEpochExhausted(txn.ProducerEpoch))
        {
            txn.PreviousProducerId = txn.ProducerId;
            txn.ProducerId = _producerStateManager.AllocateProducerId().ProducerId;
            txn.ProducerEpoch = 0;
        }
        else
        {
            txn.ProducerEpoch++;
            _producerStateManager.AdvanceEpoch(txn.ProducerId, txn.ProducerEpoch);
        }
    }

    /// <summary>Moves the transaction to PrepareCommit / PrepareAbort and persists it. Called with the lock held.</summary>
    private void Prepare(TransactionMetadata txn, bool commit)
    {
        txn.State = commit ? TransactionState.PrepareCommit : TransactionState.PrepareAbort;
        txn.CompletionInProgress = true;
        txn.LastActivityTime = DateTimeOffset.UtcNow;
        _statePersistence.PersistState(txn);
    }

    /// <summary>
    /// Writes the markers of a prepared transaction, commits or discards its offsets, and completes it.
    /// Runs without the lock — the transaction is visibly in PrepareCommit / PrepareAbort meanwhile, so a
    /// concurrent request is answered CONCURRENT_TRANSACTIONS — and takes it to complete. Returns the producer
    /// id and epoch the client continues with.
    /// </summary>
    private async Task<(long ProducerId, short ProducerEpoch)> WriteMarkersAndCompleteAsync(
        TransactionMetadata txn, bool commit, CancellationToken cancellationToken)
    {
        if (txn.Partitions.Count > 0)
        {
            var markerOffset = await _markerWriter.WriteMarkersAsync(txn, commit, cancellationToken);

            // #72 Inc7 — replicate the markers to follower brokers (best-effort). EndTxn latency now depends
            // on the remote transport, but a replication failure does not fail the commit (markers are
            // durable locally). No-op for a single broker (no remote leaders) or when no transport is wired.
            await ReplicateMarkersBestEffortAsync(txn, commit, cancellationToken);

            if (commit)
                _transactionIndex.CommitTransaction(txn.ProducerId, txn.Partitions, markerOffset);
            else
                _transactionIndex.AbortTransaction(txn.ProducerId, txn.Partitions, markerOffset);
        }

        if (commit)
        {
            foreach (var pendingOffset in txn.PendingOffsets)
            {
                _offsetStore.CommitOffset(pendingOffset.GroupId, pendingOffset.Topic, pendingOffset.Partition, pendingOffset.Offset);
                _logger.LogDebug("Committed transactional offset for group {GroupId}, {Topic}-{Partition} at offset {Offset}",
                    pendingOffset.GroupId, pendingOffset.Topic, pendingOffset.Partition, pendingOffset.Offset);
            }
        }
        else if (txn.PendingOffsets.Count > 0)
        {
            _logger.LogDebug("Discarded {Count} pending offsets due to transaction abort", txn.PendingOffsets.Count);
        }

        // The markers are written: completing must not be cancelled, or the transaction stays prepared.
        await txn.Lock.WaitAsync(CancellationToken.None);
        try
        {
            txn.State = commit ? TransactionState.CompleteCommit : TransactionState.CompleteAbort;
            ClearTransactionContents(txn);
            txn.CompletionInProgress = false;

            ApplyEpochBump(txn);

            _statePersistence.PersistState(txn);
            return (txn.ProducerId, txn.ProducerEpoch);
        }
        finally
        {
            txn.Lock.Release();
        }
    }

    /// <summary>
    /// Makes a prepared epoch bump effective: the producer continues on the fresh producer id at epoch 0 when the
    /// epoch was exhausted, otherwise at the transaction's epoch — with fresh sequences when that epoch is new to
    /// the produce path (transactions V2, a timeout abort). Called with the lock held.
    /// </summary>
    private void ApplyEpochBump(TransactionMetadata txn)
    {
        if (txn.NextProducerId != -1)
        {
            txn.PreviousProducerId = txn.ProducerId;
            txn.ProducerId = txn.NextProducerId;
            txn.ProducerEpoch = 0;
            txn.NextProducerId = -1;
        }
        _producerStateManager.AdvanceEpoch(txn.ProducerId, txn.ProducerEpoch);
    }

    private static void ClearTransactionContents(TransactionMetadata txn)
    {
        txn.Partitions.Clear();
        txn.ConsumerGroups.Clear();
        txn.PendingOffsets.Clear();
    }

    /// <summary>
    /// Lists transactions with optional filters.
    /// </summary>
    /// <param name="statesFilter">Names of <see cref="TransactionState"/> values to match. Empty / null means all states.</param>
    /// <param name="producerIdFilter">Producer ids to match. Empty / null means all producers.</param>
    /// <param name="minDurationMs">KIP-994 (v1+): only return transactions whose last-activity timestamp is older than this many ms. Negative or zero means no duration filter.</param>
    /// <param name="transactionalIdPattern">KIP-1152 (v2+): regular expression that the transactional id must match. Null or empty means no pattern filter. Invalid regex falls through silently — Surgewave prefers a permissive listing over a hard error here, matching librdkafka's behaviour.</param>
    public IReadOnlyList<TransactionListing> ListTransactions(
        IEnumerable<string>? statesFilter = null,
        IEnumerable<long>? producerIdFilter = null,
        long minDurationMs = -1,
        string? transactionalIdPattern = null)
    {
        var stateSet = statesFilter?.Select(s => Enum.TryParse<TransactionState>(s, ignoreCase: true, out var state) ? state : TransactionState.Empty)
            .Where(s => s != TransactionState.Empty)
            .ToHashSet();
        var producerIdSet = producerIdFilter?.ToHashSet();

        var durationCutoff = minDurationMs > 0
            ? (long?)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - minDurationMs
            : null;

        System.Text.RegularExpressions.Regex? regex = null;
        if (!string.IsNullOrEmpty(transactionalIdPattern))
        {
            try
            {
                regex = new System.Text.RegularExpressions.Regex(
                    transactionalIdPattern,
                    System.Text.RegularExpressions.RegexOptions.CultureInvariant,
                    TimeSpan.FromMilliseconds(50));
            }
            catch (ArgumentException)
            {
                // Bad pattern: degrade to no filter rather than dropping all results.
                regex = null;
            }
        }

        return _transactionsByTxnId.Values
            .Where(t => stateSet == null || stateSet.Count == 0 || stateSet.Contains(t.State))
            .Where(t => producerIdSet == null || producerIdSet.Count == 0 || producerIdSet.Contains(t.ProducerId))
            .Where(t => durationCutoff == null || t.LastActivityTime.ToUnixTimeMilliseconds() <= durationCutoff.Value)
            .Where(t => regex == null || RegexMatchSafe(regex, t.TransactionalId))
            .Select(t => new TransactionListing(t.TransactionalId, t.ProducerId, t.State.ToString()))
            .ToList();
    }

    /// <summary>
    /// Apply a regex with the per-pattern <see cref="System.Text.RegularExpressions.RegexMatchTimeoutException"/>
    /// safety net. The Regex constructor sets a 50 ms MatchTimeout to prevent
    /// a malicious or pathological pattern (Cox-style backtracking bait, e.g.
    /// <c>(a+)+$</c> against a long pure-'a' input) from blocking the listing.
    /// When the timeout fires we treat the id as <c>not matched</c> rather
    /// than aborting the entire listing — the same defensive shape as the
    /// invalid-pattern-degrades-to-no-filter case at construction time.
    /// </summary>
    private static bool RegexMatchSafe(System.Text.RegularExpressions.Regex regex, string input)
    {
        try
        {
            return regex.IsMatch(input);
        }
        catch (System.Text.RegularExpressions.RegexMatchTimeoutException)
        {
            return false;
        }
    }

    /// <summary>
    /// KIP-664 <c>DescribeProducers</c>: builds a per-partition producer snapshot by asking the
    /// producer-state manager which producer ids have written to (or hold a transaction on) each
    /// requested partition. Per Kafka the response is per-partition; the neutral status is None
    /// on every row (as before) — unknown-topic rows still surface so admin tools can correlate
    /// request and response indices.
    /// </summary>
    public DescribeProducersResult DescribeProducers(DescribeProducersCommand request)
    {
        var topicResults = new List<DescribeProducersTopicResult>(request.Topics.Count);
        foreach (var topic in request.Topics)
        {
            var partitionResults = new List<DescribeProducersPartitionResult>(topic.PartitionIndexes.Count);
            foreach (var partitionIndex in topic.PartitionIndexes)
            {
                var partition = new TopicPartition { Topic = topic.Name, Partition = partitionIndex };
                var producers = _producerStateManager.GetActiveProducersForPartition(partition);

                var producerStates = new List<TxnProducerState>(producers.Count);
                foreach (var p in producers)
                {
                    // Surgewave doesn't track per-partition LastTimestamp on the
                    // producer state directly — fall back to -1 (KIP-664 says
                    // "may be -1 if not tracked"). CoordinatorEpoch / TxnStartOffset
                    // are also returned as -1 unless the producer is mid-txn.
                    producerStates.Add(new TxnProducerState
                    {
                        ProducerId = p.ProducerId,
                        ProducerEpoch = p.Epoch,
                        LastSequence = p.LastSequence,
                        LastTimestamp = -1,
                        CoordinatorEpoch = -1,
                        CurrentTxnStartOffset = p.HasOngoingTransaction ? 0 : -1,
                    });
                }

                partitionResults.Add(new DescribeProducersPartitionResult
                {
                    PartitionIndex = partitionIndex,
                    Status = TxnErrorStatus.None,
                    ErrorMessage = null,
                    ActiveProducers = producerStates,
                });
            }
            topicResults.Add(new DescribeProducersTopicResult(topic.Name, partitionResults));
        }

        return new DescribeProducersResult(topicResults);
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
}
