using Kuestenlogik.Surgewave.Broker.Coordination;
using Kuestenlogik.Surgewave.Coordination.Coordinators;
using Kuestenlogik.Surgewave.Coordination.ShareGroups;
using Kuestenlogik.Surgewave.Core.Replication;
using Kuestenlogik.Surgewave.Core.Storage;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Broker.ShareGroups;

/// <summary>
/// The share coordinator (#228): share-partition state in <c>__share_group_state</c>, one snapshot record per share
/// group and partition in the topic's partition for its key — the partition whose leader is the key's share
/// coordinator. A write is answered once the in-sync replicas have it; a broker that takes a partition over reads its
/// snapshots from the log. Leading, loading and durable writes are the <see cref="CoordinatorStateLog"/>'s.
/// </summary>
/// <remarks>
/// As a persister it answers for the keys this broker coordinates only; one that reaches another broker's key gets
/// <see cref="ShareStateStatus.NotCoordinator"/>.
/// </remarks>
public sealed class ShareStateStore : ICoordinatorStateOwner, IShareStatePersister
{
    private readonly CoordinatorStateLog _log;
    private readonly TimeProvider _time;
    private readonly Dictionary<ShareStateKey, (int SnapshotEpoch, ShareStateSnapshot State)>[] _states;

    /// <summary>A store over the broker's <c>__share_group_state</c>; it leads nothing until it is told what this broker leads.</summary>
    public ShareStateStore(
        LogManager logManager,
        RecordBatchSerializer serializer,
        int partitionCount,
        ILogger<ShareStateStore> logger,
        TimeSpan? commitTimeout = null,
        TimeProvider? timeProvider = null)
    {
        _time = timeProvider ?? TimeProvider.System;
        _states = NewStates(partitionCount);
        _log = new CoordinatorStateLog(CoordinatorPartitioner.ShareStateTopic, partitionCount, this, logManager, serializer, logger, commitTimeout, _time);
    }

    private ShareStateStore(ILogger? logger)
    {
        _time = TimeProvider.System;
        _states = NewStates(1);
        _log = CoordinatorStateLog.InMemory(CoordinatorPartitioner.ShareStateTopic, this, logger);
    }

    /// <summary>A store without a log that coordinates every key, for a broker under test.</summary>
    public static ShareStateStore InMemory(ILogger? logger = null) => new(logger);

    /// <summary>Whether this broker is the key's share coordinator now.</summary>
    public CoordinatorOwnership OwnershipOf(ShareStateKey key) => _log.OwnershipOf(_log.PartitionFor(key.CoordinatorKey));

    /// <inheritdoc />
    public Task<(ShareStateStatus Status, ShareStateSnapshot? State)> ReadAsync(ShareStateKey key, CancellationToken cancellationToken)
    {
        var partition = _log.PartitionFor(key.CoordinatorKey);
        lock (_log.SyncRoot)
        {
            if (Here(partition) is { } refused)
                return Task.FromResult<(ShareStateStatus, ShareStateSnapshot?)>((refused, null));
            return Task.FromResult<(ShareStateStatus, ShareStateSnapshot?)>(_states[partition].TryGetValue(key, out var held)
                ? (ShareStateStatus.Ok, held.State)
                : (ShareStateStatus.NotInitialized, null));
        }
    }

    /// <inheritdoc />
    public async Task<ShareStateStatus> WriteAsync(ShareStateKey key, ShareStateSnapshot state, CancellationToken cancellationToken)
    {
        var partition = _log.PartitionFor(key.CoordinatorKey);
        int snapshotEpoch;
        lock (_log.SyncRoot)
        {
            if (Here(partition) is { } refused)
                return refused;
            if (!_states[partition].TryGetValue(key, out var held))
                return ShareStateStatus.NotInitialized;
            if (state.StateEpoch != held.State.StateEpoch)
                return ShareStateStatus.FencedStateEpoch;
            if (state.LeaderEpoch < held.State.LeaderEpoch)
                return ShareStateStatus.FencedLeaderEpoch;
            snapshotEpoch = held.SnapshotEpoch + 1;
        }
        return await PutAsync(partition, key, snapshotEpoch, state, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<(ShareStateStatus Status, int StateEpoch)> InitializeAsync(ShareStateKey key, long startOffset, CancellationToken cancellationToken)
    {
        var partition = _log.PartitionFor(key.CoordinatorKey);
        ShareStateSnapshot state;
        int snapshotEpoch;
        lock (_log.SyncRoot)
        {
            if (Here(partition) is { } refused)
                return (refused, -1);
            var held = _states[partition].TryGetValue(key, out var h) ? h : (SnapshotEpoch: -1, State: (ShareStateSnapshot?)null);
            state = new ShareStateSnapshot((held.State?.StateEpoch ?? -1) + 1, held.State?.LeaderEpoch ?? -1, startOffset, []);
            snapshotEpoch = held.SnapshotEpoch + 1;
        }
        var written = await PutAsync(partition, key, snapshotEpoch, state, cancellationToken).ConfigureAwait(false);
        return (written, written == ShareStateStatus.Ok ? state.StateEpoch : -1);
    }

    /// <inheritdoc />
    public async Task<ShareStateStatus> DeleteAsync(ShareStateKey key, CancellationToken cancellationToken)
    {
        var partition = _log.PartitionFor(key.CoordinatorKey);
        lock (_log.SyncRoot)
        {
            if (Here(partition) is { } refused)
                return refused;
        }
        var written = await _log.WriteAsync(partition, [(ShareStateRecords.Key(key), [])],
            _ => _states[partition].Remove(key), cancellationToken).ConfigureAwait(false);
        return StatusOf(written);
    }

    /// <inheritdoc cref="CoordinatorStateLog.OnLeadershipChanged"/>
    public void OnLeadershipChanged(int partition, bool isLeader) => _log.OnLeadershipChanged(partition, isLeader);

    /// <inheritdoc cref="CoordinatorStateLog.FollowLeadershipOf"/>
    public void FollowLeadershipOf(Kuestenlogik.Surgewave.Clustering.Replication.ReplicaManager replicas, IPartitionCommitGate commitGate)
        => _log.FollowLeadershipOf(replicas, commitGate);

    /// <inheritdoc cref="CoordinatorStateLog.StartLocalAsync"/>
    public Task StartLocalAsync(CancellationToken cancellationToken) => _log.StartLocalAsync(cancellationToken);

    void ICoordinatorStateOwner.Loaded(int partition, IReadOnlyList<StateRecord> records)
    {
        var states = _states[partition];
        foreach (var record in records)
        {
            if (ShareStateRecords.ReadKey(record.Key.Span) is not { } key)
                continue;
            if (record.Value.IsEmpty)
                states.Remove(key);
            else
                states[key] = ShareStateRecords.ReadValue(record.Value.Span);
        }
    }

    void ICoordinatorStateOwner.Unloaded(int partition) => _states[partition].Clear();

    private async Task<ShareStateStatus> PutAsync(int partition, ShareStateKey key, int snapshotEpoch, ShareStateSnapshot state, CancellationToken cancellationToken)
    {
        var value = ShareStateRecords.Value(state, snapshotEpoch, _time.GetUtcNow().ToUnixTimeMilliseconds());
        var written = await _log.WriteAsync(partition, [(ShareStateRecords.Key(key), value)],
            _ => _states[partition][key] = (snapshotEpoch, state), cancellationToken).ConfigureAwait(false);
        return StatusOf(written);
    }

    // Under the log's lock: why this broker does not answer for the partition, null when it does.
    private ShareStateStatus? Here(int partition) => _log.OwnershipOf(partition) switch
    {
        CoordinatorOwnership.Owned => null,
        CoordinatorOwnership.Loading => ShareStateStatus.CoordinatorLoadInProgress,
        _ => ShareStateStatus.NotCoordinator,
    };

    private static ShareStateStatus StatusOf(StateWriteStatus written) => written switch
    {
        StateWriteStatus.Ok => ShareStateStatus.Ok,
        StateWriteStatus.NotCoordinator => ShareStateStatus.NotCoordinator,
        StateWriteStatus.LoadInProgress => ShareStateStatus.CoordinatorLoadInProgress,
        _ => ShareStateStatus.CoordinatorNotAvailable,
    };

    private static Dictionary<ShareStateKey, (int, ShareStateSnapshot)>[] NewStates(int partitionCount)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(partitionCount, 1);
        return [.. Enumerable.Range(0, partitionCount).Select(_ => new Dictionary<ShareStateKey, (int, ShareStateSnapshot)>())];
    }
}
