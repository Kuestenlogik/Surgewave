using System.Collections.Concurrent;
using Kuestenlogik.Surgewave.Coordination.ShareGroups;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;

namespace Kuestenlogik.Surgewave.Broker.ShareGroups;

/// <summary>
/// The share partitions this broker serves (#228): one per share group and partition this broker leads, read from the
/// share coordinator when it is first needed here. A group's members all fetch a partition from its leader, so its share
/// partition — what they hold, what waits — is in one place; two groups of one topic each have their own.
/// </summary>
internal sealed class SharePartitionManager(IShareStatePersister persister, TimeSpan recordLockDuration, TimeProvider? timeProvider = null)
{
    private readonly ConcurrentDictionary<(string GroupId, TopicPartition Partition), SharePartition> _partitions = new();

    /// <summary>Where the share partitions' state is kept.</summary>
    public IShareStatePersister Persister => persister;

    /// <summary>
    /// The group's share partition: read from the share coordinator when it is new here, and initialized there at
    /// <paramref name="startOffset"/> when the coordinator has none. Null, with why, when the state could not be had.
    /// </summary>
    public async Task<(SharePartition? Partition, ShareStateStatus Status)> GetOrCreateAsync(
        string groupId, TopicPartition partition, Guid topicId, IPartitionLog log, Func<long> startOffset, CancellationToken cancellationToken)
    {
        if (_partitions.TryGetValue((groupId, partition), out var held))
            return (held, ShareStateStatus.Ok);

        var key = new ShareStateKey(groupId, topicId, partition.Partition);
        var (status, state) = await persister.ReadAsync(key, cancellationToken).ConfigureAwait(false);
        if (status == ShareStateStatus.NotInitialized)
        {
            var start = startOffset();
            var (initialized, stateEpoch) = await persister.InitializeAsync(key, start, cancellationToken).ConfigureAwait(false);
            if (initialized != ShareStateStatus.Ok)
                return (null, initialized);
            state = new ShareStateSnapshot(stateEpoch, -1, start, []);
        }
        else if (status != ShareStateStatus.Ok || state is null)
        {
            return (null, status);
        }

        return (_partitions.GetOrAdd((groupId, partition), _ => new SharePartition(log, key, state, recordLockDuration, timeProvider)), ShareStateStatus.Ok);
    }

    /// <summary>The group's share partition when this broker has one.</summary>
    public SharePartition? Get(string groupId, TopicPartition partition) =>
        _partitions.GetValueOrDefault((groupId, partition));

    /// <summary>This broker no longer leads the partition: its share partitions go, every group's.</summary>
    public void Drop(TopicPartition partition)
    {
        foreach (var key in _partitions.Keys.Where(k => k.Partition.Equals(partition)).ToList())
            _partitions.TryRemove(key, out _);
    }

    /// <summary>The group's share partition is stale — its state was initialized afresh: the next request reads it again.</summary>
    public void Drop(string groupId, TopicPartition partition) => _partitions.TryRemove((groupId, partition), out _);

    /// <summary>A member left its group: what it holds goes back to the group, on every partition here.</summary>
    public void ReleaseMember(string groupId, string memberId, int deliveryLimit)
    {
        foreach (var (key, partition) in _partitions)
        {
            if (string.Equals(key.GroupId, groupId, StringComparison.Ordinal))
                partition.ReleaseAll(memberId, deliveryLimit);
        }
    }
}
