using Kuestenlogik.Surgewave.Broker.Coordination;
using Kuestenlogik.Surgewave.Broker.Security;
using Kuestenlogik.Surgewave.Coordination.Consumer;
using Kuestenlogik.Surgewave.Core.Observability;
using Kuestenlogik.Surgewave.Core.Storage;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Broker;

/// <summary>
/// Manages consumer group lifecycle, membership, and offset commits for the classic
/// (rebalance-based) consumer-group protocol. Speaks the protocol-neutral
/// <see cref="IConsumerGroupCoordinator"/> contract — the Kafka DTO conversion and wire
/// envelope live in the <c>ConsumerGroupApiHandler</c> adapter (#59).
/// When wired with an <see cref="IConsumerGroupV2Coordinator"/> reference,
/// OffsetCommit requests that target a KIP-848 v2 group fall through to a server-side
/// epoch validation against that coordinator.
/// </summary>
public sealed partial class ConsumerGroupCoordinator(
    ILogger<ConsumerGroupCoordinator> logger,
    OffsetStore? offsetStore = null,
    LogManager? logManager = null,
    AclAuthorizer? aclAuthorizer = null,
    SurgewaveBrokerObservability? observability = null,
    IConsumerGroupV2Coordinator? v2Coordinator = null,
    TimeProvider? timeProvider = null) : IConsumerGroupCoordinator
{
    private readonly Dictionary<string, ConsumerGroupState> _consumerGroups = [];
    private readonly Lock _groupLock = new();

    // Every offset and the generation a group reached live in __consumer_offsets (#205); without a store, in memory.
    private readonly OffsetStore _offsets = offsetStore ?? OffsetStore.InMemory();

    public OffsetFetchResult FetchOffsets(OffsetFetchCommand request)
    {
        var groups = new List<OffsetFetchGroupResult>(request.Groups.Count);
        foreach (var groupRequest in request.Groups)
        {
            // A group of a partition this broker is still reading has no answer yet (#205).
            if (_offsets.OwnershipOf(groupRequest.GroupId) != CoordinatorOwnership.Owned)
            {
                groups.Add(new OffsetFetchGroupResult
                {
                    GroupId = groupRequest.GroupId,
                    Topics = [],
                    Status = ConsumerGroupErrorStatus.CoordinatorLoadInProgress,
                });
                continue;
            }

            var topics = FetchOffsetsForGroup(groupRequest.GroupId, groupRequest.Topics, request.UseTopicId);
            groups.Add(new OffsetFetchGroupResult { GroupId = groupRequest.GroupId, Topics = topics });
        }

        return new OffsetFetchResult(groups);
    }

    private List<OffsetFetchTopicResult> FetchOffsetsForGroup(string groupId, IReadOnlyList<OffsetFetchTopicRequest>? topicRequests, bool useTopicId)
    {
        topicRequests ??= AllCommittedTopics(groupId);
        var topics = new List<OffsetFetchTopicResult>(topicRequests.Count);
        Log.OffsetFetchRequest(logger, groupId, topicRequests.Count);

        foreach (var topicRequest in topicRequests)
        {
            // v10+ names topics by id; older versions get the id from the name for the response.
            string topicName = topicRequest.Topic;
            Guid topicId = topicRequest.TopicId;
            if (useTopicId && topicRequest.TopicId != Guid.Empty)
            {
                var resolved = logManager?.ResolveTopicId(topicRequest.TopicId);
                if (resolved == null)
                {
                    topics.Add(new OffsetFetchTopicResult
                    {
                        Topic = string.Empty,
                        TopicId = topicRequest.TopicId,
                        Partitions = [.. topicRequest.PartitionIndexes.Select(p =>
                            new OffsetFetchPartitionResult(p, -1, ConsumerGroupErrorStatus.UnknownTopicId))],
                    });
                    continue;
                }
                topicName = resolved;
            }
            else if (!useTopicId && !string.IsNullOrEmpty(topicRequest.Topic))
            {
                topicId = logManager?.GetTopicId(topicRequest.Topic) ?? Guid.Empty;
            }

            var partitions = new List<OffsetFetchPartitionResult>(topicRequest.PartitionIndexes.Count);
            foreach (var partitionIndex in topicRequest.PartitionIndexes)
            {
                var offset = _offsets.GetCommittedOffset(groupId, topicName, partitionIndex);
                Log.OffsetFetchFromStore(logger, groupId, topicName, partitionIndex, offset);
                partitions.Add(new OffsetFetchPartitionResult(partitionIndex, offset, ConsumerGroupErrorStatus.None));
            }

            topics.Add(new OffsetFetchTopicResult { Topic = topicName, TopicId = topicId, Partitions = partitions });
        }

        return topics;
    }

    public async Task<OffsetCommitResult> CommitOffsetsAsync(OffsetCommitCommand request, CancellationToken cancellationToken)
    {
        if (_offsets.OwnershipOf(request.GroupId) != CoordinatorOwnership.Owned)
            return BuildOffsetCommitErrorResponse(request, ConsumerGroupErrorStatus.CoordinatorLoadInProgress);

        bool isV2Group;
        lock (_groupLock)
        {
            if (_consumerGroups.ContainsKey(request.GroupId))
            {
                isV2Group = false;
            }
            else
            {
                // KIP-848 path: the group may live in the v2 coordinator. Validate the member and epoch there; v2
                // groups have no entry here but share the offsets.
                var v2Status = v2Coordinator?.ValidateMemberForOffsetOperation(
                    request.GroupId, request.MemberId, request.GenerationIdOrMemberEpoch) ?? ConsumerGroupFenceStatus.NotAV2Group;

                // A consumer that assigns its partitions itself commits outside any generation (-1) for a group nobody
                // joined; Kafka takes such commits, and so does this coordinator.
                if (v2Status == ConsumerGroupFenceStatus.NotAV2Group && request.GenerationIdOrMemberEpoch < 0)
                {
                    isV2Group = false;
                }
                else if (v2Status != ConsumerGroupFenceStatus.Ok)
                {
                    // NotAV2Group: not a v2 group either, so the group is unknown. Anything else is the v2 fence.
                    return BuildOffsetCommitErrorResponse(request, v2Status switch
                    {
                        ConsumerGroupFenceStatus.FencedEpoch => ConsumerGroupErrorStatus.FencedMemberEpoch,
                        ConsumerGroupFenceStatus.StaleEpoch => ConsumerGroupErrorStatus.StaleMemberEpoch,
                        _ => ConsumerGroupErrorStatus.UnknownMember,
                    });
                }
                else
                {
                    isV2Group = true;
                }
            }
        }

        // Resolve every partition to its answer; the ones that pass are written in one batch and get its verdict.
        var responseTopics = new List<(string Topic, Guid TopicId, List<(int Partition, ConsumerGroupErrorStatus? Status)> Partitions)>();
        var toWrite = new List<CommittedOffset>();
        foreach (var topic in request.Topics)
        {
            string topicName = topic.Topic;
            Guid topicId = topic.TopicId;
            if (request.UseTopicId && topic.TopicId != Guid.Empty)
            {
                var resolved = logManager?.ResolveTopicId(topic.TopicId);
                if (resolved == null)
                {
                    responseTopics.Add((string.Empty, topic.TopicId,
                        [.. topic.Partitions.Select(p => (p.PartitionIndex, (ConsumerGroupErrorStatus?)ConsumerGroupErrorStatus.UnknownTopicId))]));
                    continue;
                }
                topicName = resolved;
            }
            else if (!request.UseTopicId && !string.IsNullOrEmpty(topic.Topic))
            {
                topicId = logManager?.GetTopicId(topic.Topic) ?? Guid.Empty;
            }

            var partitions = new List<(int Partition, ConsumerGroupErrorStatus? Status)>(topic.Partitions.Count);
            foreach (var partition in topic.Partitions)
            {
                // KIP-1251: a v2 member's epoch must be at least the per-partition assignment epoch. Older commits for
                // partitions it still owns are accepted; for partitions assigned elsewhere since, STALE_MEMBER_EPOCH.
                if (isV2Group && v2Coordinator is not null)
                {
                    var partitionTopicId = topicId != Guid.Empty ? topicId : (logManager?.GetTopicId(topicName) ?? Guid.Empty);
                    if (partitionTopicId != Guid.Empty && !v2Coordinator.IsPartitionAssignmentValid(
                            request.GroupId, request.MemberId, request.GenerationIdOrMemberEpoch, partitionTopicId, partition.PartitionIndex))
                    {
                        partitions.Add((partition.PartitionIndex, ConsumerGroupErrorStatus.StaleMemberEpoch));
                        continue;
                    }
                }

                toWrite.Add(new CommittedOffset(topicName, partition.PartitionIndex, partition.CommittedOffset));
                partitions.Add((partition.PartitionIndex, null)); // the write's verdict
            }
            responseTopics.Add((topicName, topicId, partitions));
        }

        var written = ToStatus(await _offsets.CommitOffsetsAsync(request.GroupId, toWrite, cancellationToken).ConfigureAwait(false));

        return new OffsetCommitResult([.. responseTopics.Select(t => new OffsetCommitTopicResult
        {
            Topic = t.Topic,
            TopicId = t.TopicId,
            Partitions = [.. t.Partitions.Select(p => new OffsetCommitPartitionResult(p.Partition, p.Status ?? written))],
        })]);
    }

    // What a write to __consumer_offsets means for the request that asked for it.
    private static ConsumerGroupErrorStatus ToStatus(StateWriteStatus status) => status switch
    {
        StateWriteStatus.Ok => ConsumerGroupErrorStatus.None,
        StateWriteStatus.NotCoordinator => ConsumerGroupErrorStatus.NotCoordinator,
        StateWriteStatus.LoadInProgress => ConsumerGroupErrorStatus.CoordinatorLoadInProgress,
        _ => ConsumerGroupErrorStatus.CoordinatorNotAvailable,
    };

    /// <summary>
    /// Builds an error result that flat-fills every requested partition with the
    /// supplied status. Centralised to avoid the inline-loop variants we used to
    /// have at every error branch.
    /// </summary>
    private static OffsetCommitResult BuildOffsetCommitErrorResponse(OffsetCommitCommand request, ConsumerGroupErrorStatus status)
    {
        var errorTopics = new List<OffsetCommitTopicResult>(request.Topics.Count);
        foreach (var topic in request.Topics)
        {
            var partitions = new List<OffsetCommitPartitionResult>(topic.Partitions.Count);
            foreach (var partition in topic.Partitions)
            {
                partitions.Add(new OffsetCommitPartitionResult(partition.PartitionIndex, status));
            }
            errorTopics.Add(new OffsetCommitTopicResult
            {
                Topic = topic.Topic,
                TopicId = topic.TopicId,
                Partitions = partitions,
            });
        }

        return new OffsetCommitResult(errorTopics);
    }

    public IReadOnlyList<GroupDescription> DescribeGroups(IReadOnlyList<string> groupIds)
    {
        var known = KnownWithoutMembers();
        lock (_groupLock)
        {
            var groups = new List<GroupDescription>();

            foreach (var groupId in groupIds)
            {
                if (!_consumerGroups.TryGetValue(groupId, out var group) && known.TryGetValue(groupId, out var protocolType))
                {
                    // Known from __consumer_offsets, no member here since: an Empty group, as Kafka loads it (#224).
                    groups.Add(new GroupDescription
                    {
                        Status = ConsumerGroupErrorStatus.None,
                        GroupId = groupId,
                        GroupState = "Empty",
                        ProtocolType = protocolType,
                        ProtocolData = "",
                        Members = [],
                        AuthorizedOperations = ComputeGroupAuthorizedOperations(groupId),
                    });
                    continue;
                }
                if (group is null)
                {
                    groups.Add(new GroupDescription
                    {
                        Status = ConsumerGroupErrorStatus.InvalidGroupId,
                        GroupId = groupId,
                        GroupState = "",
                        ProtocolType = "",
                        ProtocolData = "",
                        Members = []
                    });
                    continue;
                }

                // Inline loop avoids LINQ closure allocations
                var members = new List<GroupDescriptionMember>(group.Members.Count);
                foreach (var m in group.Members.Values)
                {
                    members.Add(new GroupDescriptionMember
                    {
                        MemberId = m.MemberId,
                        GroupInstanceId = m.GroupInstanceId,
                        ClientId = m.ClientId,
                        ClientHost = "/", // We don't track client host
                        MemberMetadata = m.Metadata,
                        MemberAssignment = m.Assignment
                    });
                }

                // Compute authorized operations for this group
                var authorizedOps = ComputeGroupAuthorizedOperations(groupId);

                groups.Add(new GroupDescription
                {
                    Status = ConsumerGroupErrorStatus.None,
                    GroupId = groupId,
                    GroupState = GetGroupState(group),
                    ProtocolType = group.ProtocolType,
                    ProtocolData = group.ProtocolName,
                    Members = members,
                    AuthorizedOperations = authorizedOps
                });
            }

            return groups;
        }
    }

    /// <summary>
    /// Snapshot of all Kafka-protocol consumer groups for lag reporting:
    /// (GroupId, State, MemberCount). State follows the ListGroups semantics.
    /// </summary>
    public IReadOnlyList<(string GroupId, string State, int MemberCount)> GetGroupSummaries()
    {
        lock (_groupLock)
        {
            var result = new List<(string, string, int)>(_consumerGroups.Count);
            foreach (var group in _consumerGroups.Values)
            {
                result.Add((group.GroupId, GetGroupState(group), group.Members.Count));
            }
            return result;
        }
    }

    public IReadOnlyList<GroupListing> ListGroups(IReadOnlyList<string>? statesFilter)
    {
        var known = KnownWithoutMembers();
        lock (_groupLock)
        {
            var groups = new List<GroupListing>();

            // Known from __consumer_offsets, no member here since: Empty (#224).
            if (statesFilter is not { Count: > 0 } || statesFilter.Contains("Empty", StringComparer.OrdinalIgnoreCase))
            {
                foreach (var (groupId, protocolType) in known)
                {
                    if (!_consumerGroups.ContainsKey(groupId))
                        groups.Add(new GroupListing(groupId, protocolType, "Empty", "classic"));
                }
            }

            foreach (var (groupId, group) in _consumerGroups)
            {
                var state = GetGroupState(group);

                // Filter by state if requested
                if (statesFilter != null && statesFilter.Count > 0)
                {
                    if (!statesFilter.Contains(state, StringComparer.OrdinalIgnoreCase))
                    {
                        continue;
                    }
                }

                groups.Add(new GroupListing(groupId, group.ProtocolType, state, "classic"));
            }

            return groups;
        }
    }

    // The groups of the partitions this broker serves that the log knows, by protocol type ("consumer" when the log
    // has offsets but no metadata record: a group of simple consumers).
    private Dictionary<string, string> KnownWithoutMembers() =>
        _offsets.GetKnownGroups().ToDictionary(g => g.GroupId, g => g.ProtocolType ?? "consumer", StringComparer.Ordinal);

    public async Task<IReadOnlyList<DeleteGroupResult>> DeleteGroupsAsync(IReadOnlyList<string> groupIds, CancellationToken cancellationToken)
    {
        Log.DeleteGroupsRequest(logger, groupIds.Count);
        var results = new List<DeleteGroupResult>(groupIds.Count);

        foreach (var groupId in groupIds)
        {
            if (_offsets.OwnershipOf(groupId) != CoordinatorOwnership.Owned)
            {
                results.Add(new DeleteGroupResult(groupId, ConsumerGroupErrorStatus.CoordinatorLoadInProgress));
                continue;
            }

            lock (_groupLock)
            {
                if (_consumerGroups.TryGetValue(groupId, out var group) && group.Members.Count > 0)
                {
                    Log.DeleteGroupsNotEmpty(logger, groupId, group.Members.Count);
                    results.Add(new DeleteGroupResult(groupId, ConsumerGroupErrorStatus.NonEmptyGroup));
                    continue;
                }
                _consumerGroups.Remove(groupId);
            }

            // A group without members may still have offsets, from before a restart or a change of coordinator; they
            // go with it. Deleting a group that is not there is no error (an idempotent delete).
            var status = ToStatus(await _offsets.DeleteGroupAsync(groupId, cancellationToken).ConfigureAwait(false));
            if (status == ConsumerGroupErrorStatus.None)
                Log.DeleteGroupsDeleted(logger, groupId);
            results.Add(new DeleteGroupResult(groupId, status));
        }

        return results;
    }

    /// <summary>
    /// KIP-496: delete committed offsets for a consumer group on the listed
    /// (topic, partition) tuples. Per spec a partition can be deleted only when
    /// the group has no active member subscribed to that topic — otherwise the
    /// group could later commit a new offset and the delete would silently
    /// race. Per-partition status:
    /// <list type="bullet">
    ///   <item><see cref="ConsumerGroupErrorStatus.GroupIdNotFound"/> — group is unknown.</item>
    ///   <item><see cref="ConsumerGroupErrorStatus.GroupSubscribedToTopic"/> — at least one
    ///         active member of the group still subscribes to this topic.</item>
    ///   <item><see cref="ConsumerGroupErrorStatus.None"/> — offset deleted (or wasn't there
    ///         to begin with — KIP-496 makes the operation idempotent).</item>
    /// </list>
    /// </summary>
    public async Task<OffsetDeleteResult> DeleteOffsetsAsync(OffsetDeleteCommand request, CancellationToken cancellationToken)
    {
        ConsumerGroupErrorStatus groupError;
        if (_offsets.OwnershipOf(request.GroupId) != CoordinatorOwnership.Owned)
        {
            groupError = ConsumerGroupErrorStatus.CoordinatorLoadInProgress;
        }
        else
        {
            lock (_groupLock)
            {
                // Classic groups carry member subscriptions as opaque Kafka bytes, which are not decoded here, so
                // "does an active member subscribe to THIS topic" has no precise answer: the conservative reading of
                // KIP-496 treats every topic as subscribed while the group has any member. A group known only by
                // its offsets has none.
                _consumerGroups.TryGetValue(request.GroupId, out var group);
                groupError = group?.Members.Count > 0
                    ? ConsumerGroupErrorStatus.GroupSubscribedToTopic
                    : group is null && !_offsets.HasCommittedOffsets(request.GroupId)
                        ? ConsumerGroupErrorStatus.GroupIdNotFound
                        : ConsumerGroupErrorStatus.None;
            }
        }

        var topicResults = new List<OffsetDeleteTopicResult>(request.Topics.Count);
        foreach (var topicReq in request.Topics)
        {
            var partitionResults = new List<OffsetDeletePartitionResult>(topicReq.Partitions.Count);
            foreach (var partitionIndex in topicReq.Partitions)
            {
                var status = groupError;
                if (status == ConsumerGroupErrorStatus.None)
                {
                    status = ToStatus(await _offsets.DeleteOffsetAsync(request.GroupId, topicReq.Name, partitionIndex, cancellationToken)
                        .ConfigureAwait(false));
                }
                partitionResults.Add(new OffsetDeletePartitionResult(partitionIndex, status));
            }

            topicResults.Add(new OffsetDeleteTopicResult { Name = topicReq.Name, Partitions = partitionResults });
        }

        return new OffsetDeleteResult(topicResults);
    }

    private static string GetGroupState(ConsumerGroupState group) => StateName(group.Phase);

    /// <summary>
    /// Get information about all consumer groups for lag monitoring.
    /// </summary>
    public IEnumerable<(string GroupId, string State, int MemberCount)> GetAllGroupInfo()
    {
        lock (_groupLock)
        {
            // Materialize inside the lock — Lock.Scope (C# 13) can't span yield boundaries.
            return _consumerGroups.Select(kvp => (kvp.Key, GetGroupState(kvp.Value), kvp.Value.Members.Count)).ToList();
        }
    }

    /// <summary>
    /// Every topic partition the group has committed an offset for — what an OffsetFetch with null topics answers.
    /// </summary>
    private List<OffsetFetchTopicRequest> AllCommittedTopics(string groupId)
    {
        // Keys are "topic:partition"; a Kafka topic name cannot contain ':'.
        var partitionsByTopic = new SortedDictionary<string, List<int>>(StringComparer.Ordinal);
        foreach (var key in _offsets.GetAllOffsets(groupId).Keys)
        {
            var colon = key.LastIndexOf(':');
            if (colon <= 0 || !int.TryParse(key.AsSpan(colon + 1), out var partition))
                continue;
            var topic = key[..colon];
            if (!partitionsByTopic.TryGetValue(topic, out var partitions))
                partitionsByTopic[topic] = partitions = [];
            partitions.Add(partition);
        }

        var requests = new List<OffsetFetchTopicRequest>(partitionsByTopic.Count);
        foreach (var (topic, partitions) in partitionsByTopic)
        {
            partitions.Sort();
            requests.Add(new OffsetFetchTopicRequest
            {
                Topic = topic,
                TopicId = logManager?.GetTopicId(topic) ?? Guid.Empty,
                PartitionIndexes = partitions,
            });
        }
        return requests;
    }

    /// <summary>The group's committed offsets, keyed "topic:partition".</summary>
    public Dictionary<string, long> GetGroupCommittedOffsets(string groupId) => _offsets.GetAllOffsets(groupId);

    /// <summary>
    /// Compute authorized operations bitmask for a consumer group.
    /// If no ACL authorizer is configured, returns all group operations as allowed.
    /// </summary>
    private int ComputeGroupAuthorizedOperations(string groupId)
    {
        if (aclAuthorizer == null)
        {
            // No ACL authorizer configured - return all group operations allowed
            // Group operations: Read (3), Delete (6), Describe (8)
            return (1 << (int)AclOperation.Read) |
                   (1 << (int)AclOperation.Delete) |
                   (1 << (int)AclOperation.Describe);
        }

        // Use ACL authorizer to compute actual permissions
        // Note: In a real implementation, we'd pass the actual principal and host from the request context
        // For now, use wildcard to check what's allowed for anonymous access
        return aclAuthorizer.GetAuthorizedOperations(
            "User:*",  // Would come from authenticated connection
            "*",       // Would come from client host
            AclResourceType.Group,
            groupId);
    }
}

internal sealed class ConsumerGroupState
{
    public required string GroupId { get; init; }
    public required int GenerationId { get; set; }
    public required string ProtocolType { get; init; }
    public required string ProtocolName { get; init; }
    public Dictionary<string, GroupMember> Members { get; } = new();

    // The rebalance cycle (#216): where the group is, who leads, and who is waiting for the next step.
    public ClassicGroupPhase Phase { get; set; } = ClassicGroupPhase.Empty;
    public string? LeaderId { get; set; }
    public Dictionary<string, TaskCompletionSource<JoinGroupResult>> PendingJoins { get; } = new();
    public Dictionary<string, TaskCompletionSource<SyncGroupResult>> PendingSyncs { get; } = new();
    public ITimer? RebalanceDeadline { get; set; }
    public int RebalanceRound { get; set; }
}

internal sealed class GroupMember
{
    public required string MemberId { get; init; }
    public string? GroupInstanceId { get; init; }
    public required string ClientId { get; init; }
    public required byte[] Metadata { get; set; }
    public byte[] Assignment { get; set; } = Array.Empty<byte>();
    public DateTime LastHeartbeat { get; set; } = DateTime.UtcNow;
    public int SessionTimeoutMs { get; set; }
    public int RebalanceTimeoutMs { get; set; }
    public ITimer? SessionDeadline { get; set; }
    public int SessionRound { get; set; }
}
