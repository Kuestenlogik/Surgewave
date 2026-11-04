using Kuestenlogik.Surgewave.Broker.GroupOffsets;
using Kuestenlogik.Surgewave.Coordination.Coordinators;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Replication;
using Kuestenlogik.Surgewave.Core.Storage;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Broker;

/// <summary>
/// Committed group offsets, kept in <c>__consumer_offsets</c> (#205). A group's offsets and metadata are records in
/// the topic's partition for the group — the partition whose leader coordinates the group (#204) — so a broker that
/// takes over a partition takes over its groups from the log, and nothing of them lives in a local file any more.
/// </summary>
/// <remarks>
/// <para>
/// This broker serves the groups of the partitions it leads. Becoming leader starts a load of the partition, during
/// which its groups answer <see cref="GroupOwnership.Loading"/>; losing the leadership drops them, and they answer
/// <see cref="GroupOwnership.NotOwned"/> — the coordinator's COORDINATOR_LOAD_IN_PROGRESS and NOT_COORDINATOR.
/// </para>
/// <para>
/// A commit is answered once its records are in the leader's log and, through the commit gate, on the in-sync
/// replicas — the acks=all of Kafka's offsets topic. The cache then takes the commit, unless a later record for the
/// same key got there first.
/// </para>
/// </remarks>
public sealed class OffsetStore : IDisposable
{
    private static readonly TimeSpan DefaultCommitTimeout = TimeSpan.FromSeconds(5);

    private readonly LogManager? _logManager;
    private readonly RecordBatchSerializer? _serializer;
    private IPartitionCommitGate? _commitGate;
    private readonly ILogger _logger;
    private readonly TimeSpan _commitTimeout;
    private readonly TimeProvider _time;
    private readonly Lock _lock = new();
    private readonly OffsetsPartition[] _partitions;

    /// <summary>
    /// A store over the broker's <c>__consumer_offsets</c>. It serves nothing until it is told what this broker
    /// leads: <see cref="FollowLeadershipOf"/> in a cluster, <see cref="StartLocalAsync"/> for a broker on its own.
    /// </summary>
    /// <param name="partitionCount">The topic's partition count — the one groups are hashed over.</param>
    public OffsetStore(
        LogManager logManager,
        RecordBatchSerializer serializer,
        int partitionCount,
        ILogger<OffsetStore> logger,
        TimeSpan? commitTimeout = null,
        TimeProvider? timeProvider = null)
        : this(partitionCount, logger, timeProvider)
    {
        _logManager = logManager;
        _serializer = serializer;
        _commitTimeout = commitTimeout ?? DefaultCommitTimeout;
    }

    private OffsetStore(int partitionCount, ILogger logger, TimeProvider? timeProvider)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(partitionCount, 1);
        _logger = logger;
        _time = timeProvider ?? TimeProvider.System;
        _commitTimeout = DefaultCommitTimeout;
        _partitions = new OffsetsPartition[partitionCount];
        for (var i = 0; i < partitionCount; i++)
            _partitions[i] = new OffsetsPartition();
    }

    /// <summary>
    /// A store without a log that owns every group, for a coordinator under test: what it is told is what it keeps,
    /// for as long as it lives.
    /// </summary>
    public static OffsetStore InMemory(ILogger? logger = null)
    {
        var store = new OffsetStore(1, logger ?? Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance, null);
        store._partitions[0].Ownership = GroupOwnership.Owned;
        return store;
    }

    /// <summary>The partitions groups are hashed over.</summary>
    public int PartitionCount => _partitions.Length;

    /// <summary>Whether this broker serves the group's offsets now.</summary>
    public GroupOwnership OwnershipOf(string groupId)
    {
        lock (_lock)
            return PartitionOf(groupId).Ownership;
    }

    /// <summary>
    /// Commits the group's offsets: one batch in the group's partition, answered once it is durable. Nothing is
    /// written unless this broker owns the group and its partition is loaded.
    /// </summary>
    public async Task<OffsetWriteStatus> CommitOffsetsAsync(
        string groupId, IReadOnlyList<CommittedOffset> offsets, CancellationToken cancellationToken)
    {
        if (offsets.Count == 0)
            return OffsetWriteStatus.Ok;

        var now = _time.GetUtcNow().ToUnixTimeMilliseconds();
        var records = new List<(byte[] Key, byte[] Value)>(offsets.Count);
        foreach (var o in offsets)
            records.Add((GroupRecords.OffsetCommitKey(groupId, o.Topic, o.Partition), GroupRecords.OffsetCommitValue(o.Offset, now)));

        return await WriteAsync(groupId, records, (entry, logOffset, i) =>
        {
            var o = offsets[i];
            entry.Apply(new TopicPartitionKey(o.Topic, o.Partition), o.Offset, logOffset);
        }, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Commits one offset; <see cref="CommitOffsetsAsync"/> for several.</summary>
    public Task<OffsetWriteStatus> CommitOffsetAsync(string groupId, string topic, int partition, long offset, CancellationToken cancellationToken)
        => CommitOffsetsAsync(groupId, [new CommittedOffset(topic, partition, offset)], cancellationToken);

    /// <summary>Removes one committed offset: a tombstone for its key.</summary>
    public Task<OffsetWriteStatus> DeleteOffsetAsync(string groupId, string topic, int partition, CancellationToken cancellationToken)
        => WriteAsync(groupId, [(GroupRecords.OffsetCommitKey(groupId, topic, partition), [])],
            (entry, logOffset, _) => entry.Remove(new TopicPartitionKey(topic, partition), logOffset), cancellationToken);

    /// <summary>Removes the group: tombstones for every offset it committed and for its metadata.</summary>
    public Task<OffsetWriteStatus> DeleteGroupAsync(string groupId, CancellationToken cancellationToken)
    {
        List<TopicPartitionKey> keys;
        lock (_lock)
        {
            keys = PartitionOf(groupId).Groups.TryGetValue(groupId, out var entry) ? [.. entry.Offsets.Keys] : [];
        }

        var records = new List<(byte[] Key, byte[] Value)>(keys.Count + 1);
        foreach (var key in keys)
            records.Add((GroupRecords.OffsetCommitKey(groupId, key.Topic, key.Partition), []));
        records.Add((GroupRecords.GroupMetadataKey(groupId), []));

        return WriteAsync(groupId, records, (entry, logOffset, i) =>
        {
            if (i < keys.Count)
                entry.Remove(keys[i], logOffset);
            else
                entry.Generation = 0;
        }, cancellationToken, removesGroup: true);
    }

    /// <summary>
    /// Records the generation a rebalance of the group completed, so a coordinator that takes the group over counts
    /// on from it rather than starting at one again.
    /// </summary>
    public Task<OffsetWriteStatus> RecordGenerationAsync(
        string groupId, string protocolType, int generation, string? protocol, string? leader, CancellationToken cancellationToken)
    {
        var now = _time.GetUtcNow().ToUnixTimeMilliseconds();
        return WriteAsync(groupId,
            [(GroupRecords.GroupMetadataKey(groupId), GroupRecords.GroupMetadataValue(protocolType, generation, protocol, leader, now))],
            (entry, _, _) => entry.Generation = Math.Max(entry.Generation, generation), cancellationToken);
    }

    /// <summary>The last generation recorded for the group, 0 when none is.</summary>
    public int GetGeneration(string groupId)
    {
        lock (_lock)
            return PartitionOf(groupId).Groups.TryGetValue(groupId, out var entry) ? entry.Generation : 0;
    }

    /// <summary>The committed offset, -1 when there is none or this broker does not serve the group.</summary>
    public long GetCommittedOffset(string groupId, string topic, int partition)
    {
        lock (_lock)
        {
            var owner = PartitionOf(groupId);
            return owner.Ownership == GroupOwnership.Owned
                && owner.Groups.TryGetValue(groupId, out var entry)
                && entry.Offsets.TryGetValue(new TopicPartitionKey(topic, partition), out var committed)
                ? committed.Offset
                : -1;
        }
    }

    /// <summary>Every committed offset of the group, keyed "topic:partition".</summary>
    public Dictionary<string, long> GetAllOffsets(string groupId)
    {
        lock (_lock)
        {
            var owner = PartitionOf(groupId);
            if (owner.Ownership != GroupOwnership.Owned || !owner.Groups.TryGetValue(groupId, out var entry))
                return [];
            return entry.Offsets.ToDictionary(kv => string.Concat(kv.Key.Topic, ":", kv.Key.Partition.ToString()), kv => kv.Value.Offset);
        }
    }

    /// <summary>Whether the group has committed any offset this broker serves.</summary>
    public bool HasCommittedOffsets(string groupId) => GetAllOffsets(groupId).Count > 0;

    /// <summary>The groups with committed offsets among those this broker serves.</summary>
    public IReadOnlyList<string> GetGroupIds()
    {
        lock (_lock)
        {
            return [.. _partitions
                .Where(p => p.Ownership == GroupOwnership.Owned)
                .SelectMany(p => p.Groups)
                .Where(g => g.Value.Offsets.Count > 0)
                .Select(g => g.Key)];
        }
    }

    /// <summary>
    /// This broker leads <paramref name="partition"/> of <c>__consumer_offsets</c> now, or no longer does. Leading
    /// starts the load of its groups; following drops them.
    /// </summary>
    public void OnLeadershipChanged(int partition, bool isLeader)
    {
        if (partition < 0 || partition >= _partitions.Length || _logManager is null)
            return;

        int epoch;
        lock (_lock)
        {
            var owner = _partitions[partition];
            if (isLeader && owner.Ownership != GroupOwnership.NotOwned)
                return; // already leading it; a repeated signal starts no second load

            owner.Epoch++;
            epoch = owner.Epoch;
            owner.Groups.Clear();
            owner.Ownership = isLeader ? GroupOwnership.Loading : GroupOwnership.NotOwned;
            owner.Loaded = isLeader ? new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously) : owner.Loaded;
            if (!isLeader)
                owner.Loaded.TrySetResult();
        }

        if (isLeader)
        {
            Log.OffsetsPartitionLoading(_logger, partition);
            _ = LoadAsync(partition, epoch);
        }
        else
        {
            Log.OffsetsPartitionUnloaded(_logger, partition);
        }
    }

    /// <summary>
    /// Follows this broker's leadership of <c>__consumer_offsets</c>: the partitions it leads already are loaded at
    /// once, and every later change as the replica manager makes it. A commit is answered once
    /// <paramref name="commitGate"/> says the in-sync replicas have it.
    /// </summary>
    public void FollowLeadershipOf(Kuestenlogik.Surgewave.Clustering.Replication.ReplicaManager replicas, IPartitionCommitGate commitGate)
    {
        _commitGate = commitGate;
        replicas.LeadershipChanged += (tp, isLeader) =>
        {
            if (string.Equals(tp.Topic, CoordinatorPartitioner.GroupTopic, StringComparison.Ordinal))
                OnLeadershipChanged(tp.Partition, isLeader);
        };

        for (var p = 0; p < _partitions.Length; p++)
        {
            if (replicas.IsLeader(new TopicPartition { Topic = CoordinatorPartitioner.GroupTopic, Partition = p }))
                OnLeadershipChanged(p, isLeader: true);
        }
    }

    /// <summary>Completes once the partition's current load has finished, or at once when it is not loading.</summary>
    public Task WhenLoadedAsync(string groupId)
    {
        lock (_lock)
            return PartitionOf(groupId).Loaded.Task;
    }

    /// <summary>
    /// For a broker that is not part of a cluster: creates <c>__consumer_offsets</c> if it is missing and leads every
    /// partition of it. Completes when every partition is loaded.
    /// </summary>
    public async Task StartLocalAsync(CancellationToken cancellationToken)
    {
        if (_logManager is null)
            return;

        if (_logManager.GetTopicMetadata(CoordinatorPartitioner.GroupTopic) is null)
        {
            await _logManager.CreateTopicAsync(CoordinatorPartitioner.GroupTopic, _partitions.Length, 1,
                new Dictionary<string, string> { ["cleanup.policy"] = "compact" }, cancellationToken).ConfigureAwait(false);
        }

        for (var p = 0; p < _partitions.Length; p++)
            OnLeadershipChanged(p, isLeader: true);

        Task[] loads;
        lock (_lock)
            loads = [.. _partitions.Select(p => p.Loaded.Task)];
        await Task.WhenAll(loads).WaitAsync(cancellationToken).ConfigureAwait(false);
    }

    private async Task<OffsetWriteStatus> WriteAsync(
        string groupId,
        IReadOnlyList<(byte[] Key, byte[] Value)> records,
        Action<GroupEntry, long, int> apply,
        CancellationToken cancellationToken,
        bool removesGroup = false)
    {
        var partition = CoordinatorPartitioner.PartitionFor(groupId, _partitions.Length);
        int epoch;
        lock (_lock)
        {
            var owner = _partitions[partition];
            switch (owner.Ownership)
            {
                case GroupOwnership.NotOwned:
                    return OffsetWriteStatus.NotCoordinator;
                case GroupOwnership.Loading:
                    return OffsetWriteStatus.LoadInProgress;
            }
            epoch = owner.Epoch;

            // Without a log there is nothing to wait for: the cache is the store.
            if (_logManager is null)
            {
                var entry = owner.Entry(groupId);
                for (var i = 0; i < records.Count; i++)
                    apply(entry, long.MaxValue, i);
                if (removesGroup)
                    owner.Groups.Remove(groupId);
                return OffsetWriteStatus.Ok;
            }
        }

        var tp = new TopicPartition { Topic = CoordinatorPartitioner.GroupTopic, Partition = partition };
        if (_commitGate is not null && !_commitGate.CanAdmitDurableWrite(tp))
            return OffsetWriteStatus.NotAvailable;

        var timestamp = _time.GetUtcNow().ToUnixTimeMilliseconds();
        // Offsets relative to the batch: each record its own delta, so the log reserves one offset per record.
        var messages = new List<Message>(records.Count);
        for (var i = 0; i < records.Count; i++)
            messages.Add(new Message { Offset = i, Timestamp = timestamp, Key = records[i].Key, Value = records[i].Value, Headers = ReadOnlyMemory<byte>.Empty });

        long baseOffset;
        try
        {
            baseOffset = await _logManager.AppendBatchAsync(tp, _serializer!.SerializeMessages(messages), cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            Log.OffsetsAppendFailed(_logger, groupId, partition, ex);
            return OffsetWriteStatus.NotAvailable;
        }

        if (_commitGate is not null
            && !await _commitGate.WaitForDurableCommitAsync(tp, baseOffset + records.Count, _commitTimeout, cancellationToken).ConfigureAwait(false))
        {
            return OffsetWriteStatus.NotAvailable;
        }

        lock (_lock)
        {
            var owner = _partitions[partition];
            // The partition changed hands while the write was on its way: the next owner reads it from the log.
            if (owner.Epoch != epoch || owner.Ownership != GroupOwnership.Owned)
                return OffsetWriteStatus.NotCoordinator;

            var entry = owner.Entry(groupId);
            for (var i = 0; i < records.Count; i++)
                apply(entry, baseOffset + i, i);
            if (removesGroup && entry.IsEmpty)
                owner.Groups.Remove(groupId);
        }
        return OffsetWriteStatus.Ok;
    }

    // Reads the partition from its start to its end and applies every record in log order; the partition's groups
    // are served once the end is reached, unless the leadership moved on in the meantime.
    private async Task LoadAsync(int partition, int epoch)
    {
        var tp = new TopicPartition { Topic = CoordinatorPartitioner.GroupTopic, Partition = partition };
        var loaded = new OffsetsPartition();
        var records = 0;
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
                    foreach (var message in _serializer!.ParseRecordBatch(batch))
                    {
                        Apply(loaded, GroupRecords.Read(message.Key.Span, message.Value.Span), message.Offset);
                        position = Math.Max(position, message.Offset + 1);
                        records++;
                    }
                }

                // A read that brought nothing past where it started would be read again forever.
                if (position <= readFrom)
                {
                    Log.OffsetsPartitionLoadStalled(_logger, partition, position, end);
                    break;
                }
            }
        }
        catch (Exception ex)
        {
            Log.OffsetsPartitionLoadFailed(_logger, partition, ex);
        }

        lock (_lock)
        {
            var owner = _partitions[partition];
            if (owner.Epoch != epoch)
                return; // a newer change of leadership owns this partition now

            foreach (var (group, entry) in loaded.Groups)
                owner.Groups[group] = entry;
            owner.Ownership = GroupOwnership.Owned;
            owner.Loaded.TrySetResult();
        }
        Log.OffsetsPartitionLoaded(_logger, partition, loaded.Groups.Count, records);
    }

    private static void Apply(OffsetsPartition partition, GroupRecord record, long logOffset)
    {
        switch (record)
        {
            case GroupRecord.OffsetCommitted c:
                partition.Entry(c.Group).Apply(new TopicPartitionKey(c.Topic, c.Partition), c.Offset, logOffset);
                break;
            case GroupRecord.OffsetRemoved r:
                partition.Entry(r.Group).Remove(new TopicPartitionKey(r.Topic, r.Partition), logOffset);
                break;
            case GroupRecord.GroupMetadata m:
                partition.Entry(m.Group).Generation = m.Generation;
                break;
            case GroupRecord.GroupRemoved g:
                partition.Groups.Remove(g.Group);
                break;
        }
    }

    // Under _lock.
    private OffsetsPartition PartitionOf(string groupId)
        => _partitions[CoordinatorPartitioner.PartitionFor(groupId, _partitions.Length)];

    public void Dispose()
    {
        // Nothing to flush: every commit was in the log before it was answered.
    }

    private sealed class OffsetsPartition
    {
        public GroupOwnership Ownership { get; set; } = GroupOwnership.NotOwned;
        public int Epoch { get; set; }
        public Dictionary<string, GroupEntry> Groups { get; } = new(StringComparer.Ordinal);
        public TaskCompletionSource Loaded { get; set; } = Completed();

        public GroupEntry Entry(string groupId)
        {
            if (!Groups.TryGetValue(groupId, out var entry))
                Groups[groupId] = entry = new GroupEntry();
            return entry;
        }

        private static TaskCompletionSource Completed()
        {
            var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            done.TrySetResult();
            return done;
        }
    }

    private sealed class GroupEntry
    {
        public Dictionary<TopicPartitionKey, (long Offset, long LogOffset)> Offsets { get; } = new();

        // The log position of the last tombstone per key, so an older commit that lands after it does not revive it.
        private readonly Dictionary<TopicPartitionKey, long> _removedAt = new();

        public int Generation { get; set; }

        public bool IsEmpty => Offsets.Count == 0;

        public void Apply(TopicPartitionKey key, long offset, long logOffset)
        {
            if (Offsets.TryGetValue(key, out var current) && current.LogOffset > logOffset)
                return;
            if (_removedAt.TryGetValue(key, out var removed) && removed > logOffset)
                return;
            Offsets[key] = (offset, logOffset);
        }

        public void Remove(TopicPartitionKey key, long logOffset)
        {
            if (Offsets.TryGetValue(key, out var current) && current.LogOffset > logOffset)
                return;
            Offsets.Remove(key);
            _removedAt[key] = logOffset;
        }
    }
}

/// <summary>Whether this broker serves a group's offsets.</summary>
public enum GroupOwnership
{
    /// <summary>Another broker leads the group's partition of <c>__consumer_offsets</c>.</summary>
    NotOwned,

    /// <summary>This broker leads the partition and is still reading it.</summary>
    Loading,

    /// <summary>This broker leads the partition and has read it.</summary>
    Owned,
}

/// <summary>How a write to <c>__consumer_offsets</c> went.</summary>
public enum OffsetWriteStatus
{
    /// <summary>In the log and on the in-sync replicas.</summary>
    Ok,

    /// <summary>This broker does not lead the group's partition.</summary>
    NotCoordinator,

    /// <summary>This broker leads the partition and is still loading it.</summary>
    LoadInProgress,

    /// <summary>The partition could not take a durable write now: too few in-sync replicas, or the append failed.</summary>
    NotAvailable,
}

/// <summary>An offset to commit.</summary>
public readonly record struct CommittedOffset(string Topic, int Partition, long Offset);

/// <summary>Value-type key for topic-partition lookups (avoids string allocation).</summary>
internal readonly record struct TopicPartitionKey(string Topic, int Partition);
