using Kuestenlogik.Surgewave.Broker.Coordination;
using Kuestenlogik.Surgewave.Broker.GroupOffsets;
using Kuestenlogik.Surgewave.Coordination.ShareGroups;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Broker.Abstractions.Routing;
using Kuestenlogik.Surgewave.Coordination.Coordinators;
using Kuestenlogik.Surgewave.Core.Storage;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Broker.ShareGroups;

/// <summary>
/// Manages share group lifecycle, membership, record acquisition, and acknowledgements (KIP-932).
/// Share groups deliver every record to exactly one member at a time, with per-message acknowledgement.
/// Unlike consumer groups, ALL partitions of subscribed topics are assigned to ALL members.
/// Speaks the protocol-neutral <see cref="IShareGroupCoordinator"/> contract; the Kafka DTO conversion
/// lives in the <c>ShareGroupApiHandler</c> adapter (#59). The fetched record bytes are passed through
/// by reference to keep the share-consume path zero-copy.
/// </summary>
public sealed class ShareGroupCoordinator : IShareGroupCoordinator, IShareGroupConfigStore
{
    private readonly ILogger<ShareGroupCoordinator> logger;
    private readonly LogManager logManager;
    private readonly SharePartitionManager _sharePartitions;
    private IPartitionLeadership? _leadership;
    // The groups' records live in their partitions of __consumer_offsets (#219).
    private readonly OffsetStore _offsets;
    private readonly GroupStateLog<ShareGroupState> _state;
    private readonly Dictionary<string, ShareGroupState> _shareGroups;
    private readonly Lock _groupLock;

    private const int DefaultHeartbeatIntervalMs = 5000;
    private static readonly TimeSpan StaleHeartbeatTimeout = TimeSpan.FromSeconds(30);

    public ShareGroupCoordinator(
        ILogger<ShareGroupCoordinator> logger,
        LogManager logManager,
        OffsetStore? offsets = null,
        TimeSpan? recordLockDuration = null,
        TimeProvider? timeProvider = null,
        IShareStatePersister? shareState = null)
    {
        this.logger = logger;
        this.logManager = logManager;
        _sharePartitions = new SharePartitionManager(
            shareState ?? ShareStateStore.InMemory(), recordLockDuration ?? TimeSpan.FromSeconds(30), timeProvider);
        _offsets = offsets ?? OffsetStore.InMemory();
        _state = new GroupStateLog<ShareGroupState>(
            _offsets, k => ShareGroupRecords.IsShareGroupKey(k), ShareGroupRecords.Read, ShareGroupRecords.Of);
        _shareGroups = _state.Groups;
        _groupLock = _state.SyncRoot;
    }

    /// <summary>
    /// Who leads which partition (#228): a share partition is served by its partition's leader only. Without it — a
    /// broker on its own — every partition is this broker's.
    /// </summary>
    public void SetPartitionLeadership(IPartitionLeadership? leadership) => _leadership = leadership;

    // Whether this broker serves the group's partition of __consumer_offsets now (#219).
    private ShareGroupErrorStatus Ownership(string groupId) => _offsets.OwnershipOf(groupId) switch
    {
        CoordinatorOwnership.Owned => ShareGroupErrorStatus.None,
        CoordinatorOwnership.Loading => ShareGroupErrorStatus.CoordinatorLoadInProgress,
        _ => ShareGroupErrorStatus.NotCoordinator,
    };

    private static ShareGroupErrorStatus StatusOf(StateWriteStatus written) => written switch
    {
        StateWriteStatus.NotCoordinator => ShareGroupErrorStatus.NotCoordinator,
        StateWriteStatus.LoadInProgress => ShareGroupErrorStatus.CoordinatorLoadInProgress,
        _ => ShareGroupErrorStatus.CoordinatorNotAvailable,
    };

    // ─────────────────────────────────────────────────────────────
    // ShareGroupHeartbeat (API Key 76)
    // ─────────────────────────────────────────────────────────────

    /// <remarks>
    /// Served by the broker that leads the group's partition of <c>__consumer_offsets</c>, and answered once the group's
    /// changed records are durable there (#219).
    /// </remarks>
    public Task<ShareGroupHeartbeatResult> HeartbeatAsync(ShareGroupHeartbeatCommand request, CancellationToken cancellationToken)
    {
        var here = Ownership(request.GroupId);
        if (here != ShareGroupErrorStatus.None)
            return Task.FromResult(Refused(request, here));
        return _state.ChangeAsync(request.GroupId, () => HeartbeatLocked(request), written => Refused(request, StatusOf(written)), cancellationToken);
    }

    private static ShareGroupHeartbeatResult Refused(ShareGroupHeartbeatCommand request, ShareGroupErrorStatus status) => new()
    {
        Status = status,
        MemberId = request.MemberId,
        MemberEpoch = request.MemberEpoch,
        HeartbeatIntervalMs = DefaultHeartbeatIntervalMs,
    };

    private ShareGroupHeartbeatResult HeartbeatLocked(ShareGroupHeartbeatCommand request)
    {
        {
            logger.LogDebug("ShareGroupHeartbeat: GroupId={GroupId}, MemberId={MemberId}, MemberEpoch={MemberEpoch}",
                request.GroupId, request.MemberId, request.MemberEpoch);

            // MemberEpoch -1 means leave
            if (request.MemberEpoch == -1)
            {
                return HandleShareGroupLeave(request);
            }

            var group = _state.Current(request.GroupId);
            if (group is null)
            {
                group = new ShareGroupState { GroupId = request.GroupId };
                _state.Add(request.GroupId, group);
                logger.LogInformation("ShareGroup created: {GroupId}", request.GroupId);
            }

            // Clean stale members
            CleanStaleMembers(group);

            // Generate member ID if epoch is 0 (join)
            var memberId = request.MemberId;
            if (request.MemberEpoch == 0 && string.IsNullOrEmpty(memberId))
            {
                memberId = $"{request.ClientId}-{Guid.NewGuid()}";
            }

            // Add or update member
            if (!group.Members.TryGetValue(memberId, out var member))
            {
                member = new ShareGroupMember
                {
                    MemberId = memberId,
                    RackId = request.RackId,
                    ClientId = request.ClientId,
                    ClientHost = "*"
                };
                group.Members[memberId] = member;
                group.GroupEpoch++;
                logger.LogInformation("ShareGroup {GroupId}: member {MemberId} joined (epoch={Epoch})",
                    request.GroupId, memberId, group.GroupEpoch);
            }

            member.LastHeartbeat = DateTime.UtcNow;

            if (request.RackId != null)
            {
                member.RackId = request.RackId;
            }

            // Update subscriptions if provided
            bool subscriptionChanged = false;
            if (request.SubscribedTopicNames != null)
            {
                member.SubscribedTopicNames = [.. request.SubscribedTopicNames];
                subscriptionChanged = true;
            }

            if (subscriptionChanged)
            {
                RebuildSubscribedTopics(group);
                group.GroupEpoch++;
            }

            member.MemberEpoch = group.GroupEpoch;

            // Build assignment: all partitions of all subscribed topics for this member
            var assignment = BuildAssignment(group, member);

            return new ShareGroupHeartbeatResult
            {
                MemberId = memberId,
                MemberEpoch = member.MemberEpoch,
                HeartbeatIntervalMs = DefaultHeartbeatIntervalMs,
                Assignment = assignment
            };
        }
    }

    private ShareGroupHeartbeatResult HandleShareGroupLeave(ShareGroupHeartbeatCommand request)
    {
        if (_state.Current(request.GroupId) is { } group)
        {
            if (group.Members.Remove(request.MemberId))
            {
                RebuildSubscribedTopics(group);
                group.GroupEpoch++;
                logger.LogInformation("ShareGroup {GroupId}: member {MemberId} left (epoch={Epoch})",
                    request.GroupId, request.MemberId, group.GroupEpoch);
            }
        }

        return new ShareGroupHeartbeatResult
        {
            MemberId = request.MemberId,
            MemberEpoch = -1,
            HeartbeatIntervalMs = DefaultHeartbeatIntervalMs
        };
    }

    // ─────────────────────────────────────────────────────────────
    // ShareGroupDescribe (API Key 77)
    // ─────────────────────────────────────────────────────────────

    public IReadOnlyList<ShareGroupDescription> Describe(IReadOnlyList<string> groupIds)
    {
        lock (_groupLock)
        {
            var groups = new List<ShareGroupDescription>(groupIds.Count);

            foreach (var groupId in groupIds)
            {
                if (_state.Current(groupId) is not { } group)
                {
                    groups.Add(new ShareGroupDescription
                    {
                        Status = ShareGroupErrorStatus.InvalidGroupId,
                        GroupId = groupId,
                        GroupState = "",
                        AssignorName = "uniform",
                        Members = []
                    });
                    continue;
                }

                var members = new List<ShareGroupMemberDescription>(group.Members.Count);
                foreach (var m in group.Members.Values)
                {
                    var assignment = BuildDescribeAssignment(group, m);
                    members.Add(new ShareGroupMemberDescription
                    {
                        MemberId = m.MemberId,
                        RackId = m.RackId,
                        MemberEpoch = m.MemberEpoch,
                        ClientId = m.ClientId ?? "",
                        ClientHost = m.ClientHost ?? "*",
                        SubscribedTopicNames = m.SubscribedTopicNames,
                        Assignment = assignment
                    });
                }

                groups.Add(new ShareGroupDescription
                {
                    Status = ShareGroupErrorStatus.None,
                    GroupId = groupId,
                    GroupState = GetGroupState(group),
                    GroupEpoch = group.GroupEpoch,
                    // The ShareGroup state keeps NO separate AssignmentEpoch field (unlike
                    // Java/Kafka). AssignmentEpoch == GroupEpoch is therefore guaranteed by
                    // construction — the KAFKA-20442 invariant (empty group => epochs equal) holds
                    // trivially.
                    AssignmentEpoch = group.GroupEpoch,
                    AssignorName = "uniform",
                    Members = members
                });
            }

            return groups;
        }
    }

    // ─────────────────────────────────────────────────────────────
    // ShareFetch (API Key 78)
    // ─────────────────────────────────────────────────────────────

    /// <remarks>
    /// Served by the partition's leader, which holds the group's share partition (#228): a broker that does not lead the
    /// partition answers NOT_LEADER_OR_FOLLOWER and names the leader. Acknowledgements carried by the fetch are applied
    /// first, then the member gets records — those waiting for a member, then new ones — as the group's limits allow.
    /// </remarks>
    public async Task<ShareFetchResult> ShareFetchAsync(ShareFetchCommand request, CancellationToken cancellationToken)
    {
        var responses = new List<ShareFetchTopicResult>(request.Topics.Count);
        var settings = SettingsOf(request.GroupId);
        var memberId = request.MemberId ?? string.Empty;

        foreach (var fetchTopic in request.Topics)
        {
            var topicName = logManager.ResolveTopicId(fetchTopic.TopicId);
            var partitions = new List<ShareFetchPartitionResult>(fetchTopic.Partitions.Count);

            foreach (var fetchPartition in fetchTopic.Partitions)
            {
                // KAFKA-20533: a topic deleted during ShareFetch is UnknownTopicId for the partition.
                if (topicName == null)
                {
                    partitions.Add(new ShareFetchPartitionResult
                    {
                        PartitionIndex = fetchPartition.PartitionIndex,
                        Status = ShareGroupErrorStatus.UnknownTopicId,
                        AcknowledgeStatus = ShareGroupErrorStatus.UnknownTopicId,
                        CurrentLeader = NoLeader,
                        AcquiredRecords = []
                    });
                    continue;
                }

                var tp = new TopicPartition { Topic = topicName, Partition = fetchPartition.PartitionIndex };
                if (NotLeaderHere(tp) is { } leader)
                {
                    partitions.Add(new ShareFetchPartitionResult
                    {
                        PartitionIndex = fetchPartition.PartitionIndex,
                        Status = ShareGroupErrorStatus.NotLeaderOrFollower,
                        AcknowledgeStatus = ShareGroupErrorStatus.NotLeaderOrFollower,
                        CurrentLeader = leader,
                        AcquiredRecords = []
                    });
                    continue;
                }

                var log = logManager.GetLog(tp);
                if (log == null || string.IsNullOrEmpty(request.GroupId))
                {
                    partitions.Add(new ShareFetchPartitionResult
                    {
                        PartitionIndex = fetchPartition.PartitionIndex,
                        Status = ShareGroupErrorStatus.UnknownTopicOrPartition,
                        AcknowledgeStatus = ShareGroupErrorStatus.UnknownTopicOrPartition,
                        CurrentLeader = NoLeader,
                        AcquiredRecords = []
                    });
                    continue;
                }

                // The share partition's state from the share coordinator; the log's start where it has none yet.
                var (share, shareStatus) = await _sharePartitions.GetOrCreateAsync(
                    request.GroupId, tp, fetchTopic.TopicId, log, () => log.LogStartOffset, cancellationToken);
                if (share is null)
                {
                    partitions.Add(new ShareFetchPartitionResult
                    {
                        PartitionIndex = fetchPartition.PartitionIndex,
                        Status = StatusOf(shareStatus),
                        AcknowledgeStatus = StatusOf(shareStatus),
                        CurrentLeader = LeaderOf(tp),
                        AcquiredRecords = []
                    });
                    continue;
                }

                var acknowledgeStatus = fetchPartition.AcknowledgementBatches.Count > 0
                    ? Acknowledge(share, memberId, fetchPartition.AcknowledgementBatches, settings)
                    : ShareGroupErrorStatus.None;

                try
                {
                    var maxRecords = request.MaxRecords > 0 ? request.MaxRecords : 100;
                    var acquired = await share.AcquireAsync(memberId, maxRecords, settings.DeliveryLimit, settings.MaxRecordLocks, cancellationToken);

                    // What changed — acknowledgements, new deliveries — is durable before the answer.
                    var persisted = await PersistAsync(share, tp, cancellationToken);
                    if (persisted != ShareGroupErrorStatus.None)
                    {
                        partitions.Add(new ShareFetchPartitionResult
                        {
                            PartitionIndex = fetchPartition.PartitionIndex,
                            Status = persisted,
                            AcknowledgeStatus = acknowledgeStatus == ShareGroupErrorStatus.None ? persisted : acknowledgeStatus,
                            CurrentLeader = LeaderOf(tp),
                            AcquiredRecords = []
                        });
                        continue;
                    }

                    partitions.Add(new ShareFetchPartitionResult
                    {
                        PartitionIndex = fetchPartition.PartitionIndex,
                        Status = ShareGroupErrorStatus.None,
                        AcknowledgeStatus = acknowledgeStatus,
                        CurrentLeader = LeaderOf(tp),
                        Records = acquired.Batches,
                        AcquiredRecords = [.. acquired.Ranges.Select(r => new ShareAcquiredRecords(r.FirstOffset, r.LastOffset, r.DeliveryCount))],
                    });
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    logger.LogError(ex, "ShareFetch error for {Topic}-{Partition}", topicName, fetchPartition.PartitionIndex);
                    partitions.Add(new ShareFetchPartitionResult
                    {
                        PartitionIndex = fetchPartition.PartitionIndex,
                        Status = ShareGroupErrorStatus.Unknown,
                        AcknowledgeStatus = acknowledgeStatus,
                        CurrentLeader = NoLeader,
                        AcquiredRecords = []
                    });
                }
            }

            responses.Add(new ShareFetchTopicResult(fetchTopic.TopicId, partitions));
        }

        return new ShareFetchResult(responses);
    }

    // ─────────────────────────────────────────────────────────────
    // ShareAcknowledge (API Key 79)
    // ─────────────────────────────────────────────────────────────

    /// <remarks>
    /// Served by the partition's leader, as ShareFetch, and answered once the acknowledgements are durable with the
    /// share coordinator (#228).
    /// </remarks>
    public async Task<ShareAcknowledgeResult> ShareAcknowledgeAsync(ShareAcknowledgeCommand request, CancellationToken cancellationToken)
    {
        var responses = new List<ShareAcknowledgeTopicResult>(request.Topics.Count);
        var settings = SettingsOf(request.GroupId);

        foreach (var ackTopic in request.Topics)
        {
            var topicName = logManager.ResolveTopicId(ackTopic.TopicId);
            var partitions = new List<ShareAcknowledgePartitionResult>(ackTopic.Partitions.Count);

            foreach (var ackPartition in ackTopic.Partitions)
            {
                if (topicName == null)
                {
                    partitions.Add(new ShareAcknowledgePartitionResult
                    {
                        PartitionIndex = ackPartition.PartitionIndex,
                        Status = ShareGroupErrorStatus.UnknownTopicId,
                        CurrentLeader = NoLeader
                    });
                    continue;
                }

                var tp = new TopicPartition { Topic = topicName, Partition = ackPartition.PartitionIndex };
                if (NotLeaderHere(tp) is { } leader)
                {
                    partitions.Add(new ShareAcknowledgePartitionResult
                    {
                        PartitionIndex = ackPartition.PartitionIndex,
                        Status = ShareGroupErrorStatus.NotLeaderOrFollower,
                        CurrentLeader = leader
                    });
                    continue;
                }

                // Nothing acquired here — no share partition of the group — is nothing a member can acknowledge.
                var share = string.IsNullOrEmpty(request.GroupId) ? null : _sharePartitions.Get(request.GroupId, tp);
                // The group's renew gate first (KIP-1240): a Renew it forbids is an invalid request wherever it lands.
                var status = RenewForbidden(ackPartition.AcknowledgementBatches, settings) ? ShareGroupErrorStatus.InvalidRequest
                    : share is null ? ShareGroupErrorStatus.InvalidRecordState
                    : Acknowledge(share, request.MemberId, ackPartition.AcknowledgementBatches, settings);
                if (status == ShareGroupErrorStatus.None && share is not null)
                    status = await PersistAsync(share, tp, cancellationToken);
                partitions.Add(new ShareAcknowledgePartitionResult
                {
                    PartitionIndex = ackPartition.PartitionIndex,
                    Status = status,
                    CurrentLeader = LeaderOf(tp)
                });
            }

            responses.Add(new ShareAcknowledgeTopicResult(ackTopic.TopicId, partitions));
        }

        return new ShareAcknowledgeResult(responses);
    }

    // A member's acknowledgements on a share partition, with the group's renew gate (KIP-1240).
    private static ShareGroupErrorStatus Acknowledge(
        SharePartition share, string? memberId, IReadOnlyList<ShareAcknowledgementBatch> batches, ShareSettings settings)
    {
        if (RenewForbidden(batches, settings))
            return ShareGroupErrorStatus.InvalidRequest;
        return share.Acknowledge(memberId, [.. batches.Select(b => (b.FirstOffset, b.LastOffset, b.AcknowledgeTypes))], settings.DeliveryLimit) switch
        {
            ShareGroupAckOutcome.Ok => ShareGroupErrorStatus.None,
            _ => ShareGroupErrorStatus.InvalidRecordState,
        };
    }

    private static bool RenewForbidden(IReadOnlyList<ShareAcknowledgementBatch> batches, ShareSettings settings) =>
        !settings.RenewEnabled && batches.Any(b => b.AcknowledgeTypes.Contains((sbyte)4));

    private static readonly ShareLeader NoLeader = new(-1, -1);

    // The leader when another broker leads the partition; null when this one does, or nobody knows another.
    private ShareLeader? NotLeaderHere(TopicPartition tp)
    {
        if (_leadership?.IsLedByAnotherBroker(tp) != true)
            return null;
        _sharePartitions.Drop(tp);
        return LeaderOf(tp);
    }

    private ShareLeader LeaderOf(TopicPartition tp) =>
        _leadership?.LeaderOf(tp) is { } leader ? new ShareLeader(leader.BrokerId, leader.LeaderEpoch) : NoLeader;

    // The group's limits as its coordinator holds them; the defaults where this broker is not its coordinator.
    private ShareSettings SettingsOf(string? groupId)
    {
        var defaults = new ShareGroupState();
        if (string.IsNullOrEmpty(groupId))
            return new ShareSettings(defaults.MaxDeliveryCount, defaults.MaxRecordLocks, defaults.RenewAcknowledgeEnabled);
        lock (_groupLock)
        {
            var group = _offsets.OwnershipOf(groupId) == CoordinatorOwnership.Owned ? _state.Current(groupId) : null;
            group ??= defaults;
            return new ShareSettings(group.MaxDeliveryCount, group.MaxRecordLocks, group.RenewAcknowledgeEnabled);
        }
    }

    // Writes the share partition's state with the share coordinator; a fenced write leaves it stale, read again next time.
    private async Task<ShareGroupErrorStatus> PersistAsync(SharePartition share, TopicPartition tp, CancellationToken cancellationToken)
    {
        var leaderEpoch = _leadership?.LeaderOf(tp)?.LeaderEpoch ?? 0;
        var written = await share.PersistAsync(_sharePartitions.Persister, leaderEpoch, cancellationToken);
        if (written is ShareStateStatus.FencedStateEpoch or ShareStateStatus.FencedLeaderEpoch or ShareStateStatus.NotInitialized)
            _sharePartitions.Drop(share.Key.GroupId, tp);
        return StatusOf(written);
    }

    private static ShareGroupErrorStatus StatusOf(ShareStateStatus status) => status switch
    {
        ShareStateStatus.Ok => ShareGroupErrorStatus.None,
        ShareStateStatus.FencedLeaderEpoch => ShareGroupErrorStatus.NotLeaderOrFollower,
        ShareStateStatus.CoordinatorLoadInProgress => ShareGroupErrorStatus.CoordinatorLoadInProgress,
        // A state initialized afresh, a coordinator moved or unreachable: the client retries and the state is read again.
        _ => ShareGroupErrorStatus.CoordinatorNotAvailable,
    };

    private readonly record struct ShareSettings(int DeliveryLimit, int MaxRecordLocks, bool RenewEnabled);

    // ─────────────────────────────────────────────────────────────
    // DescribeShareGroupOffsets (API Key 90)
    // ─────────────────────────────────────────────────────────────

    /// <remarks>
    /// The start offsets are the share partitions' own, kept with their share coordinators in __share_group_state
    /// (#228); a partition whose share partition was never initialized has none (-1).
    /// </remarks>
    public async Task<DescribeShareOffsetsResult> DescribeOffsetsAsync(DescribeShareOffsetsCommand request, CancellationToken cancellationToken)
    {
        var groups = new List<DescribeShareOffsetsGroupResult>(request.Groups.Count);
        foreach (var groupReq in request.Groups)
        {
            IReadOnlyList<(string Topic, IReadOnlyList<int> Partitions)>? wanted;
            lock (_groupLock)
            {
                if (_state.Current(groupReq.GroupId) is not { } group)
                {
                    wanted = null;
                }
                else
                {
                    wanted = groupReq.Topics is { } topics
                        ? [.. topics.Select(t => (t.TopicName, t.Partitions))]
                        : [.. group.SubscribedTopics
                            .Select(t => (Topic: t, Metadata: logManager.GetTopicMetadata(t)))
                            .Where(t => t.Metadata is not null)
                            .Select(t => (t.Topic, (IReadOnlyList<int>)[.. Enumerable.Range(0, t.Metadata!.PartitionCount)]))];
                }
            }
            if (wanted is null)
            {
                groups.Add(new DescribeShareOffsetsGroupResult
                {
                    GroupId = groupReq.GroupId,
                    Topics = [],
                    Status = ShareGroupErrorStatus.InvalidGroupId,
                    ErrorMessage = "Share group not found"
                });
                continue;
            }

            var topicResults = new List<DescribeShareOffsetsTopicResult>(wanted.Count);
            foreach (var (topicName, partitionIndexes) in wanted)
            {
                var topicId = logManager.GetTopicId(topicName);
                var partitions = new List<DescribeShareOffsetsPartitionResult>(partitionIndexes.Count);
                foreach (var partitionIndex in partitionIndexes)
                {
                    var (status, state) = await _sharePartitions.Persister.ReadAsync(
                        new ShareStateKey(groupReq.GroupId, topicId, partitionIndex), cancellationToken);
                    var startOffset = status == ShareStateStatus.Ok && state is not null ? state.StartOffset : -1;
                    var log = logManager.GetLog(new TopicPartition { Topic = topicName, Partition = partitionIndex });
                    var lag = startOffset < 0 || log is null ? -1 : Math.Max(log.HighWatermark - startOffset, 0);
                    partitions.Add(new DescribeShareOffsetsPartitionResult
                    {
                        PartitionIndex = partitionIndex,
                        StartOffset = startOffset,
                        LeaderEpoch = state?.LeaderEpoch ?? -1,
                        Lag = lag,
                        Status = status is ShareStateStatus.Ok or ShareStateStatus.NotInitialized ? ShareGroupErrorStatus.None : StatusOf(status),
                    });
                }
                topicResults.Add(new DescribeShareOffsetsTopicResult { TopicName = topicName, TopicId = topicId, Partitions = partitions });
            }

            groups.Add(new DescribeShareOffsetsGroupResult { GroupId = groupReq.GroupId, Topics = topicResults, Status = ShareGroupErrorStatus.None });
        }
        return new DescribeShareOffsetsResult(groups);
    }

    // ─────────────────────────────────────────────────────────────
    // AlterShareGroupOffsets (API Key 91)
    // ─────────────────────────────────────────────────────────────

    /// <remarks>
    /// Starts each share partition afresh at its new start offset with its share coordinator (#228): under a new state
    /// epoch, so the partition's leader finds its share partition stale on its next write and reads it again.
    /// </remarks>
    public async Task<AlterShareOffsetsResult> AlterOffsetsAsync(AlterShareOffsetsCommand request, CancellationToken cancellationToken)
    {
        if (EmptyGroupStatus(request.GroupId) is { } refused)
            return new AlterShareOffsetsResult { Status = refused.Status, ErrorMessage = refused.Message, Responses = [] };

        var responses = new List<AlterShareOffsetsTopicResult>(request.Topics.Count);
        foreach (var topic in request.Topics)
        {
            var topicId = logManager.GetTopicId(topic.TopicName);
            var partitions = new List<AlterShareOffsetsPartitionResult>(topic.Partitions.Count);
            foreach (var partition in topic.Partitions)
            {
                var (status, _) = await _sharePartitions.Persister.InitializeAsync(
                    new ShareStateKey(request.GroupId, topicId, partition.PartitionIndex), partition.StartOffset, cancellationToken);
                _sharePartitions.Drop(request.GroupId, new TopicPartition { Topic = topic.TopicName, Partition = partition.PartitionIndex });
                partitions.Add(new AlterShareOffsetsPartitionResult(partition.PartitionIndex, StatusOf(status)));
            }
            responses.Add(new AlterShareOffsetsTopicResult { TopicName = topic.TopicName, TopicId = topicId, Partitions = partitions });
        }

        logger.LogInformation("ShareGroup {GroupId}: altered offsets for {TopicCount} topics", request.GroupId, request.Topics.Count);
        return new AlterShareOffsetsResult { Status = ShareGroupErrorStatus.None, Responses = responses };
    }

    // ─────────────────────────────────────────────────────────────
    // DeleteShareGroupOffsets (API Key 92)
    // ─────────────────────────────────────────────────────────────

    /// <remarks>Removes the share partitions' state with their share coordinators (#228).</remarks>
    public async Task<DeleteShareOffsetsResult> DeleteOffsetsAsync(DeleteShareOffsetsCommand request, CancellationToken cancellationToken)
    {
        if (EmptyGroupStatus(request.GroupId) is { } refused)
            return new DeleteShareOffsetsResult { Status = refused.Status, ErrorMessage = refused.Message, Responses = [] };

        var responses = new List<DeleteShareOffsetsTopicResult>(request.Topics.Count);
        foreach (var topic in request.Topics)
        {
            var topicId = logManager.GetTopicId(topic.TopicName);
            var status = ShareGroupErrorStatus.None;
            var partitionCount = logManager.GetTopicMetadata(topic.TopicName)?.PartitionCount ?? 0;
            for (var p = 0; p < partitionCount; p++)
            {
                var deleted = await _sharePartitions.Persister.DeleteAsync(new ShareStateKey(request.GroupId, topicId, p), cancellationToken);
                _sharePartitions.Drop(request.GroupId, new TopicPartition { Topic = topic.TopicName, Partition = p });
                if (deleted != ShareStateStatus.Ok)
                    status = StatusOf(deleted);
            }
            responses.Add(new DeleteShareOffsetsTopicResult { TopicName = topic.TopicName, TopicId = topicId, Status = status });
        }

        logger.LogInformation("ShareGroup {GroupId}: deleted offsets for {TopicCount} topics", request.GroupId, request.Topics.Count);
        return new DeleteShareOffsetsResult { Status = ShareGroupErrorStatus.None, Responses = responses };
    }

    // Why the group's offsets cannot be changed now: not this broker's, unknown, or with members (Kafka's rule).
    private (ShareGroupErrorStatus Status, string Message)? EmptyGroupStatus(string groupId)
    {
        var here = Ownership(groupId);
        if (here != ShareGroupErrorStatus.None)
            return (here, $"This broker does not coordinate share group {groupId} now.");
        lock (_groupLock)
        {
            if (_state.Current(groupId) is not { } group)
                return (ShareGroupErrorStatus.InvalidGroupId, "Share group not found");
            if (group.Members.Count > 0)
                return (ShareGroupErrorStatus.NonEmptyGroup, "Cannot alter or delete offsets while the group has active members");
        }
        return null;
    }

    // ─────────────────────────────────────────────────────────────
    // Internal helpers
    // ─────────────────────────────────────────────────────────────



    /// <summary>
    /// KIP-1240 — mutate a share-group config via IncrementalAlterConfigs.
    /// Returns null on success or an error message describing why the
    /// mutation was rejected. The group is auto-created with default state
    /// if it doesn't exist yet (matches upstream's "configs precede group"
    /// behaviour — an admin may set retention before the first consumer joins).
    /// Supported configs:
    /// <list type="bullet">
    ///   <item><c>share.delivery.count.limit</c> — int, clamp [2, 10]</item>
    ///   <item><c>share.partition.max.record.locks</c> — int, clamp [100, 10000]</item>
    ///   <item><c>share.renew.acknowledge.enable</c> — bool</item>
    /// </list>
    /// </summary>
    /// <remarks>Answered once the config is durable in the group's records (#219).</remarks>
    public Task<string?> SetShareGroupConfigAsync(string groupId, string name, string? value, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(groupId)) return Task.FromResult<string?>("Group name must not be empty.");
        var here = Ownership(groupId);
        if (here != ShareGroupErrorStatus.None)
            return Task.FromResult<string?>($"This broker does not coordinate share group {groupId} now ({here}).");
        return _state.ChangeAsync(groupId, () => SetShareGroupConfigLocked(groupId, name, value),
            written => (string?)$"The config of share group {groupId} could not be made durable now ({StatusOf(written)}).", cancellationToken);
    }

    private string? SetShareGroupConfigLocked(string groupId, string name, string? value)
    {
        {
            var group = _state.Current(groupId);
            if (group is null)
            {
                group = new ShareGroupState { GroupId = groupId };
                _state.Add(groupId, group);
            }

            switch (name)
            {
                case "share.delivery.count.limit":
                    if (!int.TryParse(value, out var deliveryCount))
                        return $"Config '{name}' must be an integer.";
                    if (deliveryCount < 2 || deliveryCount > 10)
                        return $"Config '{name}' must be between 2 and 10 (KIP-1240 clamp).";
                    group.MaxDeliveryCount = deliveryCount;
                    return null;

                case "share.partition.max.record.locks":
                    if (!int.TryParse(value, out var maxLocks))
                        return $"Config '{name}' must be an integer.";
                    if (maxLocks < 100 || maxLocks > 10000)
                        return $"Config '{name}' must be between 100 and 10000 (KIP-1240 clamp).";
                    group.MaxRecordLocks = maxLocks;
                    return null;

                case "share.renew.acknowledge.enable":
                    if (!bool.TryParse(value, out var renewEnabled))
                        return $"Config '{name}' must be 'true' or 'false'.";
                    group.RenewAcknowledgeEnabled = renewEnabled;
                    return null;

                default:
                    return $"Config '{name}' is not a recognized share-group config (KIP-1240).";
            }
        }
    }

    /// <summary>
    /// Removes members whose last heartbeat exceeds the stale timeout.
    /// </summary>
    /// <remarks>An eviction is written to the group's records like any other change (#219).</remarks>
    public async Task SweepStaleMembersAsync(CancellationToken cancellationToken)
    {
        List<string> groupIds;
        lock (_groupLock)
            groupIds = [.. _shareGroups.Keys];

        foreach (var groupId in groupIds)
        {
            await _state.ChangeAsync(groupId, () =>
            {
                if (_state.Current(groupId) is { } group)
                    CleanStaleMembers(group);
                return true;
            }, _ => false, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Removes members whose last heartbeat exceeds the stale timeout.
    /// </summary>
    private void CleanStaleMembers(ShareGroupState group)
    {
        var now = DateTime.UtcNow;
        List<string>? staleMembers = null;

        foreach (var kvp in group.Members)
        {
            if (now - kvp.Value.LastHeartbeat > StaleHeartbeatTimeout)
            {
                staleMembers ??= [];
                staleMembers.Add(kvp.Key);
            }
        }

        if (staleMembers != null)
        {
            foreach (var staleMember in staleMembers)
            {
                group.Members.Remove(staleMember);
                logger.LogInformation("ShareGroup {GroupId}: removed stale member {MemberId}", group.GroupId, staleMember);
            }

            RebuildSubscribedTopics(group);
            group.GroupEpoch++;
        }
    }

    /// <summary>
    /// Rebuilds the group-level SubscribedTopics set from all members' subscriptions.
    /// </summary>
    private static void RebuildSubscribedTopics(ShareGroupState group)
    {
        group.SubscribedTopics.Clear();
        foreach (var member in group.Members.Values)
        {
            foreach (var topic in member.SubscribedTopicNames)
            {
                group.SubscribedTopics.Add(topic);
            }
        }
    }

    /// <summary>
    /// Builds the heartbeat assignment for a member: all partitions of all topics the member subscribed to.
    /// </summary>
    private ShareAssignment? BuildAssignment(ShareGroupState group, ShareGroupMember member)
    {
        if (member.SubscribedTopicNames.Count == 0)
        {
            return null;
        }

        var topicPartitions = new List<ShareTopicPartitions>();

        foreach (var topicName in member.SubscribedTopicNames)
        {
            var metadata = logManager.GetTopicMetadata(topicName);
            if (metadata == null) continue;

            var partitions = new List<int>(metadata.PartitionCount);
            for (int i = 0; i < metadata.PartitionCount; i++)
            {
                partitions.Add(i);
            }

            topicPartitions.Add(new ShareTopicPartitions(metadata.TopicId, partitions));
        }

        return topicPartitions.Count > 0
            ? new ShareAssignment(topicPartitions)
            : null;
    }

    /// <summary>
    /// Builds the describe assignment for a member (includes topic names in addition to IDs).
    /// </summary>
    private ShareDescribeAssignment BuildDescribeAssignment(ShareGroupState group, ShareGroupMember member)
    {
        var topicPartitions = new List<ShareDescribeTopicPartitions>();

        foreach (var topicName in member.SubscribedTopicNames)
        {
            var metadata = logManager.GetTopicMetadata(topicName);
            if (metadata == null) continue;

            var partitions = new List<int>(metadata.PartitionCount);
            for (int i = 0; i < metadata.PartitionCount; i++)
            {
                partitions.Add(i);
            }

            topicPartitions.Add(new ShareDescribeTopicPartitions
            {
                TopicId = metadata.TopicId,
                TopicName = topicName,
                Partitions = partitions
            });
        }

        return new ShareDescribeAssignment(topicPartitions);
    }

    private static string GetGroupState(ShareGroupState group)
    {
        if (group.Members.Count == 0) return "Empty";
        return "Stable";
    }
}
