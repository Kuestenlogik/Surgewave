using System.Collections.Concurrent;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;

namespace Kuestenlogik.Surgewave.Broker.ShareGroups;

/// <summary>
/// The share partitions this broker serves (#228): one per share group and partition this broker leads. A group's
/// members all fetch a partition from its leader, so its share partition — what they hold, what waits — is in one
/// place; two groups of one topic each have their own.
/// </summary>
internal sealed class SharePartitionManager(TimeSpan recordLockDuration, TimeProvider? timeProvider = null)
{
    private readonly ConcurrentDictionary<(string GroupId, TopicPartition Partition), SharePartition> _partitions = new();

    /// <summary>The group's share partition, started at <paramref name="startOffset"/> when it is new here.</summary>
    public SharePartition GetOrCreate(string groupId, TopicPartition partition, IPartitionLog log, Func<long> startOffset) =>
        _partitions.GetOrAdd((groupId, partition), _ => new SharePartition(log, startOffset(), recordLockDuration, timeProvider));

    /// <summary>The group's share partition when this broker has one.</summary>
    public SharePartition? Get(string groupId, TopicPartition partition) =>
        _partitions.GetValueOrDefault((groupId, partition));

    /// <summary>This broker no longer leads the partition: its share partitions go, every group's.</summary>
    public void Drop(TopicPartition partition)
    {
        foreach (var key in _partitions.Keys.Where(k => k.Partition.Equals(partition)).ToList())
            _partitions.TryRemove(key, out _);
    }

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
