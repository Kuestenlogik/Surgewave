namespace Kuestenlogik.Surgewave.Coordination.ShareGroups;

// The persisted state of share partitions (KIP-932, #228): what the partition's leader keeps of a share group's view of
// a partition in __share_group_state, through the share coordinator — the leader of the key's partition of that topic.

/// <summary>One share group's view of one partition.</summary>
public readonly record struct ShareStateKey(string GroupId, Guid TopicId, int Partition)
{
    /// <summary>The key the share coordinator is found by, as Kafka's <c>SharePartitionKey</c> names it.</summary>
    public string CoordinatorKey => string.Concat(GroupId, ":", TopicId.ToString(), ":", Partition.ToString(System.Globalization.CultureInfo.InvariantCulture));
}

/// <summary>A persisted record state, with Kafka's values.</summary>
public enum ShareRecordState : sbyte
{
    /// <summary>Waiting for a member.</summary>
    Available = 0,

    /// <summary>Held by a member; written as available, since a lock does not outlive its leader.</summary>
    Acquired = 1,

    /// <summary>Done: a member acknowledged it.</summary>
    Acknowledged = 2,

    /// <summary>Done: rejected, or delivered as often as the group allows.</summary>
    Archived = 4,
}

/// <summary>Consecutive offsets of one state and delivery count.</summary>
public sealed record ShareStateBatch(long FirstOffset, long LastOffset, ShareRecordState State, short DeliveryCount);

/// <summary>
/// A share partition's state: its start offset and the records from there on that are not plain available, as of a
/// state epoch — which an AlterShareGroupOffsets moves — and the leader epoch of the leader that wrote it.
/// </summary>
public sealed record ShareStateSnapshot(int StateEpoch, int LeaderEpoch, long StartOffset, IReadOnlyList<ShareStateBatch> Batches);

/// <summary>How a share-state operation went.</summary>
public enum ShareStateStatus
{
    Ok,

    /// <summary>No state for the key: the share partition was never initialized.</summary>
    NotInitialized,

    /// <summary>The state was initialized afresh since the writer read it (Kafka's FENCED_STATE_EPOCH).</summary>
    FencedStateEpoch,

    /// <summary>A newer leader of the partition wrote since (Kafka's FENCED_LEADER_EPOCH).</summary>
    FencedLeaderEpoch,

    /// <summary>This broker is not the key's share coordinator.</summary>
    NotCoordinator,

    /// <summary>The share coordinator is still reading its partition.</summary>
    CoordinatorLoadInProgress,

    /// <summary>No share coordinator could be asked, or the write could not be made durable.</summary>
    CoordinatorNotAvailable,
}

/// <summary>
/// Reads and writes share-partition state at the key's share coordinator (Kafka's Persister): this broker when it leads
/// the key's partition of <c>__share_group_state</c>, another one over the inter-broker channel otherwise.
/// </summary>
public interface IShareStatePersister
{
    /// <summary>The key's state.</summary>
    Task<(ShareStateStatus Status, ShareStateSnapshot? State)> ReadAsync(ShareStateKey key, CancellationToken cancellationToken);

    /// <summary>
    /// Writes the key's state, answered once durable: refused when its state epoch is not the stored one, or its leader
    /// epoch older than the stored one.
    /// </summary>
    Task<ShareStateStatus> WriteAsync(ShareStateKey key, ShareStateSnapshot state, CancellationToken cancellationToken);

    /// <summary>Starts the key's state afresh at <paramref name="startOffset"/>, under the next state epoch.</summary>
    Task<(ShareStateStatus Status, int StateEpoch)> InitializeAsync(ShareStateKey key, long startOffset, CancellationToken cancellationToken);

    /// <summary>Removes the key's state.</summary>
    Task<ShareStateStatus> DeleteAsync(ShareStateKey key, CancellationToken cancellationToken);
}
