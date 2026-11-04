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
/// <para>
/// A commit's cache entry is taken once the commit is durable, unless a later record for the same key got there first.
/// </para>
/// <para>
/// A transactional commit (TxnOffsetCommit, #197) is a batch of the producer's transaction in the group's partition,
/// as in Kafka: its offsets are pending until the transaction's marker lands in the partition — committed by a commit
/// marker, dropped by an abort marker. A broker that takes the partition over finds both in the log.
/// </para>
/// </remarks>
public sealed class OffsetStore : IDisposable, ICoordinatorStateOwner
{
    private readonly CoordinatorStateLog _log;
    private readonly TimeProvider _time;
    private readonly Dictionary<string, GroupEntry>[] _groups;

    // Per partition: the offsets each producer's ongoing transaction commits, and the markers that landed while the
    // partition was still being read.
    private readonly Dictionary<long, List<PendingOffset>>[] _pending;
    private readonly List<(long ProducerId, bool Commit)>[] _markersWhileLoading;

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
        (_pending, _markersWhileLoading) = NewPending(partitionCount);
        _log = new CoordinatorStateLog(CoordinatorPartitioner.GroupTopic, partitionCount, this, logManager, serializer, logger, commitTimeout, _time);
    }

    private OffsetStore(ILogger? logger)
    {
        _time = TimeProvider.System;
        _groups = NewGroups(1);
        (_pending, _markersWhileLoading) = NewPending(1);
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

    /// <summary>The partition of <c>__consumer_offsets</c> the group's offsets are in.</summary>
    public int PartitionFor(string groupId) => _log.PartitionFor(groupId);

    /// <summary>
    /// Commits the group's offsets in the transaction of <paramref name="producerId"/> (TxnOffsetCommit): one batch of
    /// that transaction in the group's partition, answered once durable. The offsets count once the transaction's
    /// commit marker lands in the partition (<see cref="CompleteTransaction"/>).
    /// </summary>
    public Task<StateWriteStatus> CommitTransactionalOffsetsAsync(
        string groupId, long producerId, short producerEpoch, IReadOnlyList<CommittedOffset> offsets, CancellationToken cancellationToken)
    {
        if (offsets.Count == 0)
            return Task.FromResult(StateWriteStatus.Ok);

        var now = _time.GetUtcNow().ToUnixTimeMilliseconds();
        var records = new List<(byte[] Key, byte[] Value)>(offsets.Count);
        foreach (var o in offsets)
            records.Add((GroupRecords.OffsetCommitKey(groupId, o.Topic, o.Partition), GroupRecords.OffsetCommitValue(o.Offset, now)));

        var partition = _log.PartitionFor(groupId);
        return _log.WriteAsync(partition, records, baseOffset =>
        {
            var pending = Pending(_pending[partition], producerId);
            for (var i = 0; i < offsets.Count; i++)
                pending.Add(new PendingOffset(groupId, new TopicPartitionKey(offsets[i].Topic, offsets[i].Partition), offsets[i].Offset, baseOffset + i));
        }, cancellationToken, (producerId, producerEpoch));
    }

    /// <summary>
    /// The marker of <paramref name="producerId"/>'s transaction is in the partition: its pending offsets are
    /// committed, or dropped when it aborted. A marker that lands while the partition is still being read is applied
    /// once it is read.
    /// </summary>
    public void CompleteTransaction(int partition, long producerId, bool commit)
    {
        if (partition < 0 || partition >= _pending.Length)
            return;
        lock (_log.SyncRoot)
        {
            switch (_log.OwnershipOf(partition))
            {
                case CoordinatorOwnership.Owned:
                    Complete(partition, producerId, commit);
                    break;
                case CoordinatorOwnership.Loading:
                    _markersWhileLoading[partition].Add((producerId, commit));
                    break;
            }
        }
    }

    /// <summary>Whether a transaction has offsets of the group that are not committed yet.</summary>
    public bool HasPendingOffsets(string groupId)
    {
        var partition = _log.PartitionFor(groupId);
        lock (_log.SyncRoot)
            return _pending[partition].Values.Any(offsets => offsets.Exists(o => o.Group == groupId));
    }

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
            (entry, _) =>
            {
                entry.Generation = Math.Max(entry.Generation, generation);
                entry.ProtocolType = protocolType;
            }, cancellationToken);
    }

    /// <summary>The last generation recorded for the group, 0 when none is.</summary>
    public int GetGeneration(string groupId)
    {
        var partition = _log.PartitionFor(groupId);
        lock (_log.SyncRoot)
            return _groups[partition].TryGetValue(groupId, out var entry) ? entry.Generation : 0;
    }

    /// <summary>
    /// The groups this broker serves that the log knows — by committed offsets or a recorded generation — with the
    /// protocol type of their last metadata record. A group taken over from another coordinator is known here before
    /// any of its members joined this broker (#224).
    /// </summary>
    public IReadOnlyList<(string GroupId, string? ProtocolType)> GetKnownGroups()
    {
        lock (_log.SyncRoot)
        {
            return [.. Enumerable.Range(0, _groups.Length)
                .Where(p => _log.OwnershipOf(p) == CoordinatorOwnership.Owned)
                .SelectMany(p => _groups[p])
                .Where(g => g.Value.Offsets.Count > 0 || g.Value.Generation > 0)
                .Select(g => (g.Key, g.Value.ProtocolType))];
        }
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
            if (record.IsMarker)
            {
                Complete(partition, record.ProducerId, record.Marker!.Value);
                continue;
            }
            if (record.ProducerId >= 0)
            {
                // An offset commit in a transaction: pending until the transaction's marker follows.
                if (GroupRecords.Read(record.Key.Span, record.Value.Span) is GroupRecord.OffsetCommitted pending)
                {
                    Pending(_pending[partition], record.ProducerId).Add(
                        new PendingOffset(pending.Group, new TopicPartitionKey(pending.Topic, pending.Partition), pending.Offset, record.Offset));
                }
                continue;
            }

            switch (GroupRecords.Read(record.Key.Span, record.Value.Span))
            {
                case GroupRecord.OffsetCommitted c:
                    Entry(groups, c.Group).Apply(new TopicPartitionKey(c.Topic, c.Partition), c.Offset, record.Offset);
                    break;
                case GroupRecord.OffsetRemoved r:
                    Entry(groups, r.Group).Remove(new TopicPartitionKey(r.Topic, r.Partition), record.Offset);
                    break;
                case GroupRecord.GroupMetadata m:
                    var described = Entry(groups, m.Group);
                    described.Generation = m.Generation;
                    described.ProtocolType = m.ProtocolType;
                    break;
                case GroupRecord.GroupRemoved g:
                    groups.Remove(g.Group);
                    break;
            }
        }

        // Markers that landed while the partition was read; one that is in the log already finds nothing pending.
        foreach (var (producerId, commit) in _markersWhileLoading[partition])
            Complete(partition, producerId, commit);
        _markersWhileLoading[partition].Clear();
    }

    void ICoordinatorStateOwner.Unloaded(int partition)
    {
        _groups[partition].Clear();
        _pending[partition].Clear();
        _markersWhileLoading[partition].Clear();
    }

    // Under the log's lock.
    private void Complete(int partition, long producerId, bool commit)
    {
        if (!_pending[partition].Remove(producerId, out var offsets) || !commit)
            return;
        foreach (var o in offsets)
            Entry(_groups[partition], o.Group).Apply(o.Key, o.Offset, o.LogOffset);
    }

    private static List<PendingOffset> Pending(Dictionary<long, List<PendingOffset>> pending, long producerId)
    {
        if (!pending.TryGetValue(producerId, out var offsets))
            pending[producerId] = offsets = [];
        return offsets;
    }

    private static (Dictionary<long, List<PendingOffset>>[], List<(long, bool)>[]) NewPending(int partitionCount) =>
        ([.. Enumerable.Range(0, partitionCount).Select(_ => new Dictionary<long, List<PendingOffset>>())],
         [.. Enumerable.Range(0, partitionCount).Select(_ => new List<(long, bool)>())]);

    private readonly record struct PendingOffset(string Group, TopicPartitionKey Key, long Offset, long LogOffset);

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

        // The protocol type of the group's last metadata record, null before one.
        public string? ProtocolType { get; set; }

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
