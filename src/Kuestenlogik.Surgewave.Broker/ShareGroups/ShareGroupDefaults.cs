namespace Kuestenlogik.Surgewave.Broker.ShareGroups;

/// <summary>
/// What a share group runs with where it set no config of its own (KIP-1240), and how long a member holds a record it
/// fetched: the broker's configuration (#229).
/// </summary>
public sealed record ShareGroupDefaults
{
    /// <summary>How often a record is delivered before it is archived; <c>group.share.delivery.count.limit</c>.</summary>
    public int DeliveryCountLimit { get; init; } = 5;

    /// <summary>How many records of a share partition may be locked at once; <c>group.share.partition.max.record.locks</c>.</summary>
    public int MaxRecordLocks { get; init; } = 2000;

    /// <summary>Whether a member may renew the lock on a record it holds; <c>share.renew.acknowledge.enable</c>.</summary>
    public bool RenewAcknowledgeEnable { get; init; } = true;

    /// <summary>How long a member holds a record it fetched; <c>group.share.record.lock.duration.ms</c>.</summary>
    public TimeSpan RecordLockDuration { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>The defaults of the broker's configuration.</summary>
    public static ShareGroupDefaults From(BrokerConfig config) => new()
    {
        DeliveryCountLimit = config.ShareDeliveryCountLimit,
        MaxRecordLocks = config.SharePartitionMaxRecordLocks,
        RenewAcknowledgeEnable = config.ShareRenewAcknowledgeEnable,
        RecordLockDuration = TimeSpan.FromMilliseconds(config.ShareRecordLockDurationMs),
    };
}
