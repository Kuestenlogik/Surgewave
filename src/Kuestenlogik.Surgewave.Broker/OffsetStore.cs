using Kuestenlogik.Surgewave.Broker.Coordination;
using Kuestenlogik.Surgewave.Broker.GroupOffsets;
using Kuestenlogik.Surgewave.Coordination.Coordinators;
using Kuestenlogik.Surgewave.Core.Replication;
using Kuestenlogik.Surgewave.Core.Storage;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Broker;

/// <summary>
/// Committed group offsets, kept in <c>__consumer_offsets</c> (#205). A group's offsets and metadata are records in
/// the topic's partition for the group — the partition whose leader coordinates the group (#204) — so a broker that
/// takes over a partition takes over its groups from the log, and nothing of them lives in a local file any more.
/// Leading, loading and durable writes are the <see cref="CoordinatorStateLog"/>'s; this is what the records mean.
/// </summary>
/// <remarks>
/// A commit's cache entry is taken once the commit is durable, unless a later record for the same key got there first.
/// </remarks>
public sealed class OffsetStore : IDisposable, ICoordinatorStateOwner
{
    private readonly CoordinatorStateLog _log;
    private readonly TimeProvider _time;
    private readonly Dictionary<string, GroupEntry>[] _groups;

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
    {
        _time = timeProvider ?? TimeProvider.System;
        _groups = NewGroups(partitionCount);
        _log = new CoordinatorStateLog(CoordinatorPartitioner.GroupTopic, partitionCount, this, logManager, serializer, logger, commitTimeout, _time);
    }

    private OffsetStore(ILogger? logger)
    {
        _time = TimeProvider.System;
        _groups = NewGroups(1);
        _log = CoordinatorStateLog.InMemory(CoordinatorPartitioner.GroupTopic, this, logger);
    }

    /// <summary>
    /// A store without a log that owns every group, for a coordinator under test: what it is told is what it keeps,
    /// for as long as it lives.
    /// </summary>
    public static OffsetStore InMemory(ILogger? logger = null) => new(logger);

    /// <summary>The partitions groups are hashed over.</summary>
    public int PartitionCount => _log.PartitionCount;

    /// <summary>Whether this broker serves the group's offsets now.</summary>
    public CoordinatorOwnership OwnershipOf(string groupId) => _log.OwnershipOf(_log.PartitionFor(groupId));

    /// <summary>
    /// Commits the group's offsets: one batch in the group's partition, answered once it is durable. Nothing is
    /// written unless this broker owns the group and its partition is loaded.
    /// </summary>
    public Task<StateWriteStatus> CommitOffsetsAsync(string groupId, IReadOnlyList<CommittedOffset> offsets, CancellationToken cancellationToken)
    {
        if (offsets.Count == 0)
            return Task.FromResult(StateWriteStatus.Ok);

        var now = _time.GetUtcNow().ToUnixTimeMilliseconds();
        var records = new List<(byte[] Key, byte[] Value)>(offsets.Count);
        foreach (var o in offsets)
            records.Add((GroupRecords.OffsetCommitKey(groupId, o.Topic, o.Partition), GroupRecords.OffsetCommitValue(o.Offset, now)));

        return WriteAsync(groupId, records, (entry, baseOffset) =>
        {
            for (var i = 0; i < offsets.Count; i++)
                entry.Apply(new TopicPartitionKey(offsets[i].Topic, offsets[i].Partition), offsets[i].Offset, baseOffset + i);
        }, cancellationToken);
    }

    /// <summary>Commits one offset; <see cref="CommitOffsetsAsync"/> for several.</summary>
    public Task<StateWriteStatus> CommitOffsetAsync(string groupId, string topic, int partition, long offset, CancellationToken cancellationToken)
        => CommitOffsetsAsync(groupId, [new CommittedOffset(topic, partition, offset)], cancellationToken);

    /// <summary>Removes one committed offset: a tombstone for its key.</summary>
    public Task<StateWriteStatus> DeleteOffsetAsync(string groupId, string topic, int partition, CancellationToken cancellationToken)
        => WriteAsync(groupId, [(GroupRecords.OffsetCommitKey(groupId, topic, partition), [])],
            (entry, baseOffset) => entry.Remove(new TopicPartitionKey(topic, partition), baseOffset), cancellationToken);

    /// <summary>Removes the group: tombstones for every offset it committed and for its metadata.</summary>
    public Task<StateWriteStatus> DeleteGroupAsync(string groupId, CancellationToken cancellationToken)
    {
        var partition = _log.PartitionFor(groupId);
        List<TopicPartitionKey> keys;
        lock (_log.SyncRoot)
            keys = _groups[partition].TryGetValue(groupId, out var entry) ? [.. entry.Offsets.Keys] : [];

        var records = new List<(byte[] Key, byte[] Value)>(keys.Count + 1);
        foreach (var key in keys)
            records.Add((GroupRecords.OffsetCommitKey(groupId, key.Topic, key.Partition), []));
        records.Add((GroupRecords.GroupMetadataKey(groupId), []));

        return _log.WriteAsync(partition, records, baseOffset =>
        {
            if (!_groups[partition].TryGetValue(groupId, out var entry))
                return;
            for (var i = 0; i < keys.Count; i++)
                entry.Remove(keys[i], baseOffset + i);
            entry.Generation = 0;
            if (entry.IsEmpty)
                _groups[partition].Remove(groupId);
        }, cancellationToken);
    }

    /// <summary>
    /// Records the generation a rebalance of the group completed, so a coordinator that takes the group over counts
    /// on from it rather than starting at one again.
    /// </summary>
    public Task<StateWriteStatus> RecordGenerationAsync(
        string groupId, string protocolType, int generation, string? protocol, string? leader, CancellationToken cancellationToken)
    {
        var now = _time.GetUtcNow().ToUnixTimeMilliseconds();
        return WriteAsync(groupId,
            [(GroupRecords.GroupMetadataKey(groupId), GroupRecords.GroupMetadataValue(protocolType, generation, protocol, leader, now))],
            (entry, _) => entry.Generation = Math.Max(entry.Generation, generation), cancellationToken);
    }

    /// <summary>The last generation recorded for the group, 0 when none is.</summary>
    public int GetGeneration(string groupId)
    {
        var partition = _log.PartitionFor(groupId);
        lock (_log.SyncRoot)
            return _groups[partition].TryGetValue(groupId, out var entry) ? entry.Generation : 0;
    }

    /// <summary>The committed offset, -1 when there is none or this broker does not serve the group.</summary>
    public long GetCommittedOffset(string groupId, string topic, int partition)
    {
        var owner = _log.PartitionFor(groupId);
        lock (_log.SyncRoot)
        {
            return _log.OwnershipOf(owner) == CoordinatorOwnership.Owned
                && _groups[owner].TryGetValue(groupId, out var entry)
                && entry.Offsets.TryGetValue(new TopicPartitionKey(topic, partition), out var committed)
                ? committed.Offset
                : -1;
        }
    }

    /// <summary>Every committed offset of the group, keyed "topic:partition".</summary>
    public Dictionary<string, long> GetAllOffsets(string groupId)
    {
        var owner = _log.PartitionFor(groupId);
        lock (_log.SyncRoot)
        {
            if (_log.OwnershipOf(owner) != CoordinatorOwnership.Owned || !_groups[owner].TryGetValue(groupId, out var entry))
                return [];
            return entry.Offsets.ToDictionary(kv => string.Concat(kv.Key.Topic, ":", kv.Key.Partition.ToString()), kv => kv.Value.Offset);
        }
    }

    /// <summary>Whether the group has committed any offset this broker serves.</summary>
    public bool HasCommittedOffsets(string groupId) => GetAllOffsets(groupId).Count > 0;

    /// <summary>The groups with committed offsets among those this broker serves.</summary>
    public IReadOnlyList<string> GetGroupIds()
    {
        lock (_log.SyncRoot)
        {
            return [.. Enumerable.Range(0, _groups.Length)
                .Where(p => _log.OwnershipOf(p) == CoordinatorOwnership.Owned)
                .SelectMany(p => _groups[p])
                .Where(g => g.Value.Offsets.Count > 0)
                .Select(g => g.Key)];
        }
    }

    /// <inheritdoc cref="CoordinatorStateLog.OnLeadershipChanged"/>
    public void OnLeadershipChanged(int partition, bool isLeader) => _log.OnLeadershipChanged(partition, isLeader);

    /// <inheritdoc cref="CoordinatorStateLog.FollowLeadershipOf"/>
    public void FollowLeadershipOf(Kuestenlogik.Surgewave.Clustering.Replication.ReplicaManager replicas, IPartitionCommitGate commitGate)
        => _log.FollowLeadershipOf(replicas, commitGate);

    /// <summary>Completes once the group's partition has been read, or at once when it is not loading.</summary>
    public Task WhenLoadedAsync(string groupId) => _log.WhenLoadedAsync(_log.PartitionFor(groupId));

    /// <inheritdoc cref="CoordinatorStateLog.StartLocalAsync"/>
    public Task StartLocalAsync(CancellationToken cancellationToken) => _log.StartLocalAsync(cancellationToken);

    public void Dispose()
    {
        // Nothing to flush: every commit was in the log before it was answered.
    }

    void ICoordinatorStateOwner.Loaded(int partition, IReadOnlyList<StateRecord> records)
    {
        var groups = _groups[partition];
        foreach (var record in records)
        {
            switch (GroupRecords.Read(record.Key.Span, record.Value.Span))
            {
                case GroupRecord.OffsetCommitted c:
                    Entry(groups, c.Group).Apply(new TopicPartitionKey(c.Topic, c.Partition), c.Offset, record.Offset);
                    break;
                case GroupRecord.OffsetRemoved r:
                    Entry(groups, r.Group).Remove(new TopicPartitionKey(r.Topic, r.Partition), record.Offset);
                    break;
                case GroupRecord.GroupMetadata m:
                    Entry(groups, m.Group).Generation = m.Generation;
                    break;
                case GroupRecord.GroupRemoved g:
                    groups.Remove(g.Group);
                    break;
            }
        }
    }

    void ICoordinatorStateOwner.Unloaded(int partition) => _groups[partition].Clear();

    private Task<StateWriteStatus> WriteAsync(
        string groupId, IReadOnlyList<(byte[] Key, byte[] Value)> records, Action<GroupEntry, long> apply, CancellationToken cancellationToken)
    {
        var partition = _log.PartitionFor(groupId);
        return _log.WriteAsync(partition, records, baseOffset => apply(Entry(_groups[partition], groupId), baseOffset), cancellationToken);
    }

    private static GroupEntry Entry(Dictionary<string, GroupEntry> groups, string groupId)
    {
        if (!groups.TryGetValue(groupId, out var entry))
            groups[groupId] = entry = new GroupEntry();
        return entry;
    }

    private static Dictionary<string, GroupEntry>[] NewGroups(int partitionCount)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(partitionCount, 1);
        return [.. Enumerable.Range(0, partitionCount).Select(_ => new Dictionary<string, GroupEntry>(StringComparer.Ordinal))];
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

/// <summary>An offset to commit.</summary>
public readonly record struct CommittedOffset(string Topic, int Partition, long Offset);

/// <summary>Value-type key for topic-partition lookups (avoids string allocation).</summary>
internal readonly record struct TopicPartitionKey(string Topic, int Partition);
