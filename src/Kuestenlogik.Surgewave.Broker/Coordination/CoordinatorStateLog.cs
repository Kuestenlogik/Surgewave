using Kuestenlogik.Surgewave.Coordination.Coordinators;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Replication;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Core.Util;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Broker.Coordination;

/// <summary>
/// A coordinator's state topic — <c>__consumer_offsets</c> (#205) or <c>__transaction_state</c> (#197): which of its
/// partitions this broker leads, reading a partition when the broker takes it over, and writing records that are
/// answered once the in-sync replicas have them. What the records mean is the owner's business.
/// </summary>
/// <remarks>
/// <para>
/// A partition this broker leads is <see cref="CoordinatorOwnership.Loading"/> until it has been read from its start
/// to its end, then <see cref="CoordinatorOwnership.Owned"/>; one it does not lead is
/// <see cref="CoordinatorOwnership.NotOwned"/> — a coordinator's COORDINATOR_LOAD_IN_PROGRESS and NOT_COORDINATOR.
/// Every change of hands bumps the partition's epoch, so a load or a write that started before it never lands after it.
/// </para>
/// <para>
/// The owner's state is guarded by <see cref="SyncRoot"/>; the owner's callbacks run under it.
/// </para>
/// </remarks>
public sealed class CoordinatorStateLog
{
    private static readonly TimeSpan DefaultCommitTimeout = TimeSpan.FromSeconds(5);

    private readonly ICoordinatorStateOwner _owner;
    private readonly LogManager? _logManager;
    private readonly RecordBatchSerializer? _serializer;
    private readonly ILogger _logger;
    private readonly TimeSpan _commitTimeout;
    private readonly TimeProvider _time;
    private readonly Partition[] _partitions;
    private IPartitionCommitGate? _commitGate;
    private long _memoryOffset;

    /// <summary>A log over <paramref name="topic"/>; it leads nothing until it is told what this broker leads.</summary>
    public CoordinatorStateLog(
        string topic,
        int partitionCount,
        ICoordinatorStateOwner owner,
        LogManager logManager,
        RecordBatchSerializer serializer,
        ILogger logger,
        TimeSpan? commitTimeout = null,
        TimeProvider? timeProvider = null)
        : this(topic, partitionCount, owner, logger, timeProvider)
    {
        _logManager = logManager;
        _serializer = serializer;
        _commitTimeout = commitTimeout ?? DefaultCommitTimeout;
    }

    private CoordinatorStateLog(string topic, int partitionCount, ICoordinatorStateOwner owner, ILogger logger, TimeProvider? timeProvider)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(partitionCount, 1);
        Topic = topic;
        _owner = owner;
        _logger = logger;
        _time = timeProvider ?? TimeProvider.System;
        _commitTimeout = DefaultCommitTimeout;
        _partitions = new Partition[partitionCount];
        for (var i = 0; i < partitionCount; i++)
            _partitions[i] = new Partition();
    }

    /// <summary>A log without a topic that owns its one partition: what is written is what is kept, in memory.</summary>
    public static CoordinatorStateLog InMemory(string topic, ICoordinatorStateOwner owner, ILogger? logger = null)
    {
        var log = new CoordinatorStateLog(topic, 1, owner, logger ?? Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance, null);
        log._partitions[0].Ownership = CoordinatorOwnership.Owned;
        return log;
    }

    /// <summary>The state topic.</summary>
    public string Topic { get; }

    /// <summary>The partitions keys are hashed over.</summary>
    public int PartitionCount => _partitions.Length;

    /// <summary>Guards the owner's state; the owner's callbacks run under it.</summary>
    public Lock SyncRoot { get; } = new();

    /// <summary>The partition <paramref name="key"/> belongs to.</summary>
    public int PartitionFor(string key) => CoordinatorPartitioner.PartitionFor(key, _partitions.Length);

    /// <summary>Whether this broker serves the partition now.</summary>
    public CoordinatorOwnership OwnershipOf(int partition)
    {
        lock (SyncRoot)
            return _partitions[partition].Ownership;
    }

    /// <summary>Completes once the partition's current load has finished, or at once when it is not loading.</summary>
    public Task WhenLoadedAsync(int partition)
    {
        lock (SyncRoot)
            return _partitions[partition].Loaded.Task;
    }

    /// <summary>
    /// Writes <paramref name="records"/> into the partition as one batch. Once the in-sync replicas have it,
    /// <paramref name="apply"/> runs under <see cref="SyncRoot"/> with the log offset of the first record — unless the
    /// partition changed hands meanwhile, when the next owner reads the records from the log instead.
    /// </summary>
    /// <param name="transaction">
    /// The producer whose transaction the batch belongs to, or null: a transactional batch counts once the marker of
    /// its transaction follows it in the partition (Kafka's transactional offset commits).
    /// </param>
    public async Task<StateWriteStatus> WriteAsync(
        int partition, IReadOnlyList<(byte[] Key, byte[] Value)> records, Action<long> apply, CancellationToken cancellationToken,
        (long ProducerId, short ProducerEpoch)? transaction = null)
    {
        int epoch;
        lock (SyncRoot)
        {
            var owner = _partitions[partition];
            switch (owner.Ownership)
            {
                case CoordinatorOwnership.NotOwned:
                    return StateWriteStatus.NotCoordinator;
                case CoordinatorOwnership.Loading:
                    return StateWriteStatus.LoadInProgress;
            }
            epoch = owner.Epoch;

            // Without a log the state is the store.
            if (_logManager is null)
            {
                apply(_memoryOffset);
                _memoryOffset += records.Count;
                return StateWriteStatus.Ok;
            }
        }

        var tp = new TopicPartition { Topic = Topic, Partition = partition };
        if (_commitGate is not null && !_commitGate.CanAdmitDurableWrite(tp))
            return StateWriteStatus.NotAvailable;

        // Offsets relative to the batch: each record its own delta, so the log reserves one offset per record.
        var timestamp = _time.GetUtcNow().ToUnixTimeMilliseconds();
        var messages = new List<Message>(records.Count);
        for (var i = 0; i < records.Count; i++)
            messages.Add(new Message { Offset = i, Timestamp = timestamp, Key = records[i].Key, Value = records[i].Value, Headers = ReadOnlyMemory<byte>.Empty });

        var batch = _serializer!.SerializeMessages(messages);
        if (transaction is { } producer)
            TransactionalBatches.MarkTransactional(batch, producer.ProducerId, producer.ProducerEpoch);

        long baseOffset;
        try
        {
            baseOffset = await _logManager.AppendBatchAsync(tp, batch, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            Log.StateAppendFailed(_logger, Topic, partition, ex);
            return StateWriteStatus.NotAvailable;
        }

        if (_commitGate is not null
            && !await _commitGate.WaitForDurableCommitAsync(tp, baseOffset + records.Count, _commitTimeout, cancellationToken).ConfigureAwait(false))
        {
            return StateWriteStatus.NotAvailable;
        }

        lock (SyncRoot)
        {
            var owner = _partitions[partition];
            if (owner.Epoch != epoch || owner.Ownership != CoordinatorOwnership.Owned)
                return StateWriteStatus.NotCoordinator;
            apply(baseOffset);
        }
        return StateWriteStatus.Ok;
    }

    /// <summary>
    /// This broker leads <paramref name="partition"/> now, or no longer does. Leading starts its load; following lets
    /// the owner drop its state at once.
    /// </summary>
    public void OnLeadershipChanged(int partition, bool isLeader)
    {
        if (partition < 0 || partition >= _partitions.Length || _logManager is null)
            return;

        int epoch;
        lock (SyncRoot)
        {
            var owner = _partitions[partition];
            if (isLeader && owner.Ownership != CoordinatorOwnership.NotOwned)
                return; // already leading it; a repeated signal starts no second load

            owner.Epoch++;
            epoch = owner.Epoch;
            _owner.Unloaded(partition);
            owner.Ownership = isLeader ? CoordinatorOwnership.Loading : CoordinatorOwnership.NotOwned;
            if (isLeader)
                owner.Loaded = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            else
                owner.Loaded.TrySetResult();
        }

        if (isLeader)
        {
            Log.StatePartitionLoading(_logger, Topic, partition);
            _ = LoadAsync(partition, epoch);
        }
        else
        {
            Log.StatePartitionUnloaded(_logger, Topic, partition);
        }
    }

    /// <summary>
    /// Follows this broker's leadership of the topic: the partitions it leads already are loaded at once, every later
    /// change as the replica manager makes it. A write is answered once <paramref name="commitGate"/> says the in-sync
    /// replicas have it.
    /// </summary>
    public void FollowLeadershipOf(Kuestenlogik.Surgewave.Clustering.Replication.ReplicaManager replicas, IPartitionCommitGate commitGate)
    {
        _commitGate = commitGate;
        replicas.LeadershipChanged += (tp, isLeader) =>
        {
            if (string.Equals(tp.Topic, Topic, StringComparison.Ordinal))
                OnLeadershipChanged(tp.Partition, isLeader);
        };

        for (var p = 0; p < _partitions.Length; p++)
        {
            if (replicas.IsLeader(new TopicPartition { Topic = Topic, Partition = p }))
                OnLeadershipChanged(p, isLeader: true);
        }
    }

    /// <summary>
    /// For a broker that is not part of a cluster: creates the topic, compacted, if it is missing and leads every
    /// partition of it. Completes when every partition is loaded.
    /// </summary>
    public async Task StartLocalAsync(CancellationToken cancellationToken)
    {
        if (_logManager is null)
            return;

        if (_logManager.GetTopicMetadata(Topic) is null)
        {
            await _logManager.CreateTopicAsync(Topic, _partitions.Length, 1,
                new Dictionary<string, string> { ["cleanup.policy"] = "compact" }, cancellationToken).ConfigureAwait(false);
        }

        for (var p = 0; p < _partitions.Length; p++)
            OnLeadershipChanged(p, isLeader: true);

        Task[] loads;
        lock (SyncRoot)
            loads = [.. _partitions.Select(p => p.Loaded.Task)];
        await Task.WhenAll(loads).WaitAsync(cancellationToken).ConfigureAwait(false);
    }

    // Reads the partition from its start to its end; the owner gets every record in log order, and the partition is
    // served once the end is reached — unless the leadership moved on in the meantime.
    private async Task LoadAsync(int partition, int epoch)
    {
        var tp = new TopicPartition { Topic = Topic, Partition = partition };
        var records = new List<StateRecord>();
        try
        {
            var log = _logManager!.GetOrCreateLog(tp);
            var end = log.NextOffset;
            var position = log.LogStartOffset;
            while (position < end)
            {
                var batches = await _logManager.ReadBatchesAsync(tp, position, cancellationToken: CancellationToken.None).ConfigureAwait(false);
                if (batches.Count == 0)
                    break;
                var readFrom = position;

                foreach (var batch in batches)
                {
                    // A transaction marker carries its producer's outcome; the records of a transactional batch count
                    // once it follows them.
                    var producerId = TransactionalBatches.TransactionalProducerOf(batch);
                    var isMarker = CompressionCodec.IsControlBatch(batch);
                    foreach (var message in _serializer!.ParseRecordBatch(batch))
                    {
                        if (message.Offset < readFrom)
                            continue; // a batch that starts before where this read began was read already
                        records.Add(isMarker
                            ? new StateRecord(ReadOnlyMemory<byte>.Empty, ReadOnlyMemory<byte>.Empty, message.Offset, producerId,
                                TransactionalBatches.IsCommitMarker(message.Value.Span))
                            : new StateRecord(message.Key, message.Value, message.Offset, producerId));
                        position = Math.Max(position, message.Offset + 1);
                    }
                }

                // A read that brought nothing past where it started would be read again forever.
                if (position <= readFrom)
                {
                    Log.StatePartitionLoadStalled(_logger, Topic, partition, position, end);
                    break;
                }
            }
        }
        catch (Exception ex)
        {
            Log.StatePartitionLoadFailed(_logger, Topic, partition, ex);
        }

        lock (SyncRoot)
        {
            var owner = _partitions[partition];
            if (owner.Epoch != epoch)
                return; // a newer change of leadership owns this partition now

            _owner.Loaded(partition, records);
            owner.Ownership = CoordinatorOwnership.Owned;
            owner.Loaded.TrySetResult();
        }
        Log.StatePartitionLoaded(_logger, Topic, partition, records.Count);
    }

    private sealed class Partition
    {
        public CoordinatorOwnership Ownership { get; set; } = CoordinatorOwnership.NotOwned;
        public int Epoch { get; set; }
        public TaskCompletionSource Loaded { get; set; } = Completed();

        private static TaskCompletionSource Completed()
        {
            var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            done.TrySetResult();
            return done;
        }
    }
}

/// <summary>What a coordinator keeps of its state topic, told by the <see cref="CoordinatorStateLog"/>.</summary>
public interface ICoordinatorStateOwner
{
    /// <summary>Under the log's lock: the partition was read; <paramref name="records"/> are in log order.</summary>
    void Loaded(int partition, IReadOnlyList<StateRecord> records);

    /// <summary>Under the log's lock: this broker no longer serves the partition, or is about to read it afresh.</summary>
    void Unloaded(int partition);
}

/// <summary>A record of a state topic; an empty value is a tombstone.</summary>
/// <param name="ProducerId">The producer whose transaction the record was written in; -1 outside any transaction.</param>
/// <param name="Marker">
/// For the marker of a transaction rather than a record of the owner's: true when it commits, false when it aborts.
/// </param>
public readonly record struct StateRecord(
    ReadOnlyMemory<byte> Key, ReadOnlyMemory<byte> Value, long Offset, long ProducerId = -1, bool? Marker = null)
{
    /// <summary>Whether this is the marker of a transaction rather than a record of the owner's.</summary>
    public bool IsMarker => ProducerId >= 0 && Key.IsEmpty && Value.IsEmpty && Marker.HasValue;
}

/// <summary>Whether this broker serves a partition of a coordinator's state topic.</summary>
public enum CoordinatorOwnership
{
    /// <summary>Another broker leads the partition.</summary>
    NotOwned,

    /// <summary>This broker leads the partition and is still reading it.</summary>
    Loading,

    /// <summary>This broker leads the partition and has read it.</summary>
    Owned,
}

/// <summary>How a write to a coordinator's state topic went.</summary>
public enum StateWriteStatus
{
    /// <summary>In the log and on the in-sync replicas.</summary>
    Ok,

    /// <summary>This broker does not lead the partition.</summary>
    NotCoordinator,

    /// <summary>This broker leads the partition and is still loading it.</summary>
    LoadInProgress,

    /// <summary>The partition could not take a durable write now: too few in-sync replicas, or the append failed.</summary>
    NotAvailable,
}
