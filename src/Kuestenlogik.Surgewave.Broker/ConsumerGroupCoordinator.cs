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
public sealed class ConsumerGroupCoordinator(
    ILogger<ConsumerGroupCoordinator> logger,
    OffsetStore? offsetStore = null,
    LogManager? logManager = null,
    AclAuthorizer? aclAuthorizer = null,
    SurgewaveBrokerObservability? observability = null,
    IConsumerGroupV2Coordinator? v2Coordinator = null) : IConsumerGroupCoordinator
{
    private readonly Dictionary<string, ConsumerGroupState> _consumerGroups = [];
    private readonly Lock _groupLock = new();

    public JoinGroupResult JoinGroup(JoinGroupCommand request)
    {
        lock (_groupLock)
        {
            // Only the first join protocol is consulted (Kafka picks one protocol per group).
            var firstProtocol = request.Protocols.Count > 0 ? request.Protocols[0] : null;

            Log.JoinGroupRequest(logger, request.GroupId, request.MemberId,
                request.Protocols.Count, firstProtocol?.Metadata?.Length ?? 0);

            if (!_consumerGroups.TryGetValue(request.GroupId, out var group))
            {
                group = new ConsumerGroupState
                {
                    GroupId = request.GroupId,
                    GenerationId = 1,
                    ProtocolType = request.ProtocolType,
                    ProtocolName = firstProtocol?.Name ?? "range"
                };
                _consumerGroups[request.GroupId] = group;
                Log.JoinGroupCreated(logger, request.GroupId);
            }

            // Clean up stale members who haven't sent a heartbeat within session timeout
            var sessionTimeout = TimeSpan.FromMilliseconds(request.SessionTimeoutMs > 0 ? request.SessionTimeoutMs : 10000);
            var now = DateTime.UtcNow;

            // Inline loop avoids LINQ closure allocations
            List<string>? staleMembers = null;
            foreach (var kvp in group.Members)
            {
                if (now - kvp.Value.LastHeartbeat > sessionTimeout)
                {
                    staleMembers ??= new List<string>();
                    staleMembers.Add(kvp.Key);
                }
            }

            if (staleMembers != null)
            foreach (var staleMember in staleMembers)
            {
                group.Members.Remove(staleMember);
                Log.JoinGroupRemovedStaleMember(logger, staleMember, sessionTimeout.TotalMilliseconds);
            }

            if (staleMembers is { Count: > 0 })
            {
                // Bump generation when members are removed
                group.GenerationId++;
            }

            // Generate member ID if empty
            var memberId = string.IsNullOrEmpty(request.MemberId)
                ? $"{request.ClientId}-{Guid.NewGuid()}"
                : request.MemberId;

            // Add or update member (preserve assignment if member already exists)
            if (!group.Members.TryGetValue(memberId, out var member))
            {
                member = new GroupMember
                {
                    MemberId = memberId,
                    GroupInstanceId = request.GroupInstanceId,
                    ClientId = request.ClientId,
                    Metadata = firstProtocol?.Metadata ?? Array.Empty<byte>(),
                    Assignment = Array.Empty<byte>()
                };
                group.Members[memberId] = member;
                Log.JoinGroupMemberAdded(logger, memberId, member.Metadata.Length);
            }
            else
            {
                // Update metadata for existing member, but preserve assignment
                member.Metadata = firstProtocol?.Metadata ?? Array.Empty<byte>();
                Log.JoinGroupMemberUpdated(logger, memberId, member.Metadata.Length);
            }

            // First member becomes the leader
            var leaderId = group.Members.Keys.First();
            var isLeader = memberId == leaderId;

            Log.JoinGroupLeaderInfo(logger, memberId, leaderId, isLeader, group.Members.Count);

            // Prepare member list (only for leader) - inline loop avoids LINQ closures
            JoinGroupMemberInfo[] members;
            int totalMetadataLength = 0;
            if (isLeader)
            {
                members = new JoinGroupMemberInfo[group.Members.Count];
                int i = 0;
                foreach (var m in group.Members.Values)
                {
                    members[i++] = new JoinGroupMemberInfo(m.MemberId, m.GroupInstanceId, m.Metadata ?? []);
                    totalMetadataLength += m.Metadata?.Length ?? 0;
                }
            }
            else
            {
                members = [];
            }

            Log.JoinGroupResponse(logger, memberId, members.Length, totalMetadataLength);

            return new JoinGroupResult
            {
                GenerationId = group.GenerationId,
                ProtocolName = group.ProtocolName,
                LeaderId = leaderId,
                MemberId = memberId,
                Members = members
            };
        }
    }

    public SyncGroupResult SyncGroup(SyncGroupCommand request)
    {
        lock (_groupLock)
        {
            Log.SyncGroupRequest(logger, request.GroupId, request.MemberId, request.GenerationId);
            Log.SyncGroupAssignmentsCount(logger, request.Assignments.Count);

            if (!_consumerGroups.TryGetValue(request.GroupId, out var group))
            {
                Log.SyncGroupNotFound(logger, request.GroupId);
                return new SyncGroupResult(ConsumerGroupErrorStatus.UnknownMember, Array.Empty<byte>());
            }

            // Store assignments if this is the leader
            if (request.Assignments.Count > 0)
            {
                Log.SyncGroupLeaderAssignments(logger, request.Assignments.Count);
                foreach (var assignment in request.Assignments)
                {
                    Log.SyncGroupMemberAssignment(logger, assignment.MemberId, assignment.Assignment.Length);
                    if (group.Members.TryGetValue(assignment.MemberId, out var member))
                    {
                        member.Assignment = assignment.Assignment;
                    }
                }

                // Rebalance is "done" the moment the leader distributes
                // assignments — followers' subsequent SyncGroup calls just
                // read back the stored bytes, so emitting here gives one
                // event per rebalance (not one per member). Topic stays
                // empty because assignments span every topic the group
                // subscribes to and decoding the assignment bytes to
                // enumerate them is more work than the tap needs.
                if (observability?.HasSubscribers == true)
                {
                    observability.Publish(new SurgewaveBrokerEvent(
                        SurgewaveBrokerEventKind.Rebalanced,
                        Topic: string.Empty, Partition: -1, Offset: null,
                        Principal: null, RejectReason: null,
                        Consumers: [request.GroupId],
                        Key: null, Value: null,
                        Timestamp: DateTimeOffset.UtcNow));
                }
            }

            // Return the assignment for this member
            var memberAssignment = group.Members.TryGetValue(request.MemberId, out var m)
                ? m.Assignment
                : Array.Empty<byte>();

            Log.SyncGroupReturningAssignment(logger, request.MemberId, memberAssignment.Length);

            return new SyncGroupResult(ConsumerGroupErrorStatus.None, memberAssignment);
        }
    }

    public GroupHeartbeatResult Heartbeat(GroupHeartbeatCommand request)
    {
        lock (_groupLock)
        {
            if (!_consumerGroups.TryGetValue(request.GroupId, out var group))
            {
                return new GroupHeartbeatResult(ConsumerGroupErrorStatus.UnknownMember);
            }

            if (!group.Members.TryGetValue(request.MemberId, out var member))
            {
                return new GroupHeartbeatResult(ConsumerGroupErrorStatus.UnknownMember);
            }

            // Update heartbeat timestamp
            member.LastHeartbeat = DateTime.UtcNow;

            // Heartbeat successful
            return new GroupHeartbeatResult(ConsumerGroupErrorStatus.None);
        }
    }

    public LeaveGroupResult LeaveGroup(LeaveGroupCommand request)
    {
        lock (_groupLock)
        {
            if (!_consumerGroups.TryGetValue(request.GroupId, out var group))
            {
                return new LeaveGroupResult(ConsumerGroupErrorStatus.UnknownMember, null);
            }

            // Handle V3+ batch leave (members array)
            if (request.Members.Count > 0)
            {
                var memberResponses = new List<LeaveGroupMemberResult>();
                foreach (var member in request.Members)
                {
                    var removed = group.Members.Remove(member.MemberId);
                    memberResponses.Add(new LeaveGroupMemberResult(
                        member.MemberId,
                        member.GroupInstanceId,
                        removed ? ConsumerGroupErrorStatus.None : ConsumerGroupErrorStatus.UnknownMember));
                }

                return new LeaveGroupResult(ConsumerGroupErrorStatus.None, memberResponses);
            }

            // Handle V0-2 single member leave
            if (!string.IsNullOrEmpty(request.MemberId))
            {
                group.Members.Remove(request.MemberId);
            }

            return new LeaveGroupResult(ConsumerGroupErrorStatus.None, null);
        }
    }

    public OffsetFetchResult FetchOffsets(OffsetFetchCommand request)
    {
        lock (_groupLock)
        {
            var groups = new List<OffsetFetchGroupResult>(request.Groups.Count);
            foreach (var groupRequest in request.Groups)
            {
                var topics = FetchOffsetsForGroup(groupRequest.GroupId, groupRequest.Topics, request.UseTopicId);
                groups.Add(new OffsetFetchGroupResult { GroupId = groupRequest.GroupId, Topics = topics });
            }

            return new OffsetFetchResult(groups);
        }
    }

    private List<OffsetFetchTopicResult> FetchOffsetsForGroup(string groupId, IReadOnlyList<OffsetFetchTopicRequest> topicRequests, bool useTopicId)
    {
        var topics = new List<OffsetFetchTopicResult>();

        Log.OffsetFetchRequest(logger, groupId, topicRequests.Count);

        if (!_consumerGroups.TryGetValue(groupId, out var group))
        {
            Log.OffsetFetchGroupNotInMemory(logger, groupId);
            // Group doesn't exist in memory - try to get from persisted store
            foreach (var topicRequest in topicRequests)
            {
                // For v10+, resolve TopicId to topic name
                string topicName = topicRequest.Topic;
                Guid topicId = topicRequest.TopicId;

                if (useTopicId && topicRequest.TopicId != Guid.Empty)
                {
                    var resolved = logManager?.ResolveTopicId(topicRequest.TopicId);
                    if (resolved == null)
                    {
                        // Unknown TopicId - return error for all partitions (inline loop)
                        var errorPartitions = new List<OffsetFetchPartitionResult>(topicRequest.PartitionIndexes.Count);
                        foreach (var p in topicRequest.PartitionIndexes)
                        {
                            errorPartitions.Add(new OffsetFetchPartitionResult(p, -1, ConsumerGroupErrorStatus.UnknownTopicId));
                        }

                        topics.Add(new OffsetFetchTopicResult
                        {
                            Topic = string.Empty,
                            TopicId = topicRequest.TopicId,
                            Partitions = errorPartitions
                        });
                        continue;
                    }
                    topicName = resolved;
                }
                else if (!useTopicId && !string.IsNullOrEmpty(topicRequest.Topic))
                {
                    // For older versions, get TopicId from topic name for response
                    topicId = logManager?.GetTopicId(topicRequest.Topic) ?? Guid.Empty;
                }

                var partitions = new List<OffsetFetchPartitionResult>(topicRequest.PartitionIndexes.Count);
                foreach (var partitionIndex in topicRequest.PartitionIndexes)
                {
                    // Try to get from persisted store
                    var offset = offsetStore?.GetCommittedOffset(groupId, topicName, partitionIndex) ?? -1;
                    Log.OffsetFetchFromStore(logger, groupId, topicName, partitionIndex, offset);

                    partitions.Add(new OffsetFetchPartitionResult(partitionIndex, offset, ConsumerGroupErrorStatus.None));
                }

                topics.Add(new OffsetFetchTopicResult
                {
                    Topic = topicName,
                    TopicId = topicId,
                    Partitions = partitions
                });
            }

            return topics;
        }

        Log.OffsetFetchGroupInMemory(logger, groupId, group.CommittedOffsets.Count);

        // Group exists in memory - return committed offsets (check store as fallback)
        foreach (var topicRequest in topicRequests)
        {
            // For v10+, resolve TopicId to topic name
            string topicName = topicRequest.Topic;
            Guid topicId = topicRequest.TopicId;

            if (useTopicId && topicRequest.TopicId != Guid.Empty)
            {
                var resolved = logManager?.ResolveTopicId(topicRequest.TopicId);
                if (resolved == null)
                {
                    // Unknown TopicId - return error for all partitions (inline loop)
                    var errorPartitions = new List<OffsetFetchPartitionResult>(topicRequest.PartitionIndexes.Count);
                    foreach (var p in topicRequest.PartitionIndexes)
                    {
                        errorPartitions.Add(new OffsetFetchPartitionResult(p, -1, ConsumerGroupErrorStatus.UnknownTopicId));
                    }

                    topics.Add(new OffsetFetchTopicResult
                    {
                        Topic = string.Empty,
                        TopicId = topicRequest.TopicId,
                        Partitions = errorPartitions
                    });
                    continue;
                }
                topicName = resolved;
            }
            else if (!useTopicId && !string.IsNullOrEmpty(topicRequest.Topic))
            {
                // For older versions, get TopicId from topic name for response
                topicId = logManager?.GetTopicId(topicRequest.Topic) ?? Guid.Empty;
            }

            var partitions = new List<OffsetFetchPartitionResult>(topicRequest.PartitionIndexes.Count);
            foreach (var partitionIndex in topicRequest.PartitionIndexes)
            {
                var key = string.Concat(topicName, ":", partitionIndex.ToString());
                var offset = group.CommittedOffsets.GetValueOrDefault(key, -1);
                Log.OffsetFetchFromMemory(logger, key, offset);

                // If not in memory, check persisted store
                if (offset == -1 && offsetStore != null)
                {
                    offset = offsetStore.GetCommittedOffset(groupId, topicName, partitionIndex);
                    Log.OffsetFetchFallbackToStore(logger, groupId, topicName, partitionIndex, offset);
                }

                partitions.Add(new OffsetFetchPartitionResult(partitionIndex, offset, ConsumerGroupErrorStatus.None));
            }

            topics.Add(new OffsetFetchTopicResult
            {
                Topic = topicName,
                TopicId = topicId,
                Partitions = partitions
            });
        }

        return topics;
    }

    public OffsetCommitResult CommitOffsets(OffsetCommitCommand request)
    {
        lock (_groupLock)
        {
            bool useTopicId = request.UseTopicId;

            if (!_consumerGroups.TryGetValue(request.GroupId, out var group))
            {
                // KIP-848 path: the group may live in the v2 coordinator. Validate the
                // member + epoch there, and if it checks out, route the commit straight
                // to the offset store — v2 groups don't carry their own _consumerGroups
                // entry but they share the same offset persistence.
                if (v2Coordinator != null)
                {
                    var v2Status = v2Coordinator.ValidateMemberForOffsetOperation(
                        request.GroupId, request.MemberId, request.GenerationIdOrMemberEpoch);

                    if (v2Status == ConsumerGroupFenceStatus.Ok)
                    {
                        return CommitOffsetsForV2Group(request, useTopicId);
                    }

                    // NotAV2Group is the sentinel for "not a v2 group, fall through to the
                    // classic coordinator" — anything else is an authoritative fence, mapped
                    // from the neutral v2 fence status onto the classic error status.
                    if (v2Status != ConsumerGroupFenceStatus.NotAV2Group)
                    {
                        return BuildOffsetCommitErrorResponse(request, v2Status switch
                        {
                            ConsumerGroupFenceStatus.UnknownMember => ConsumerGroupErrorStatus.UnknownMember,
                            ConsumerGroupFenceStatus.FencedEpoch => ConsumerGroupErrorStatus.FencedMemberEpoch,
                            ConsumerGroupFenceStatus.StaleEpoch => ConsumerGroupErrorStatus.StaleMemberEpoch,
                            _ => ConsumerGroupErrorStatus.UnknownMember,
                        });
                    }
                }

                // Group genuinely unknown.
                return BuildOffsetCommitErrorResponse(request, ConsumerGroupErrorStatus.UnknownMember);
            }

            // Commit the offsets
            var responseTopics = new List<OffsetCommitTopicResult>();
            foreach (var topic in request.Topics)
            {
                // For v10+, resolve TopicId to topic name
                string topicName = topic.Topic;
                Guid topicId = topic.TopicId;

                if (useTopicId && topic.TopicId != Guid.Empty)
                {
                    var resolved = logManager?.ResolveTopicId(topic.TopicId);
                    if (resolved == null)
                    {
                        // Unknown TopicId - return error for all partitions (inline loop)
                        var errorPartitions = new List<OffsetCommitPartitionResult>(topic.Partitions.Count);
                        foreach (var p in topic.Partitions)
                        {
                            errorPartitions.Add(new OffsetCommitPartitionResult(p.PartitionIndex, ConsumerGroupErrorStatus.UnknownTopicId));
                        }

                        responseTopics.Add(new OffsetCommitTopicResult
                        {
                            Topic = string.Empty,
                            TopicId = topic.TopicId,
                            Partitions = errorPartitions
                        });
                        continue;
                    }
                    topicName = resolved;
                }
                else if (!useTopicId && !string.IsNullOrEmpty(topic.Topic))
                {
                    // For older versions, get TopicId from topic name for response
                    topicId = logManager?.GetTopicId(topic.Topic) ?? Guid.Empty;
                }

                var partitions = new List<OffsetCommitPartitionResult>(topic.Partitions.Count);
                foreach (var partition in topic.Partitions)
                {
                    var key = string.Concat(topicName, ":", partition.PartitionIndex.ToString());
                    group.CommittedOffsets[key] = partition.CommittedOffset;

                    // Persist to offset store
                    offsetStore?.CommitOffset(request.GroupId, topicName, partition.PartitionIndex, partition.CommittedOffset);

                    partitions.Add(new OffsetCommitPartitionResult(partition.PartitionIndex, ConsumerGroupErrorStatus.None));
                }

                responseTopics.Add(new OffsetCommitTopicResult
                {
                    Topic = topicName,
                    TopicId = topicId,
                    Partitions = partitions
                });
            }

            return new OffsetCommitResult(responseTopics);
        }
    }

    /// <summary>
    /// Persists offsets for a KIP-848 v2 group through the shared <see cref="OffsetStore"/>.
    /// The v2 coordinator already validated member identity and epoch; this method just
    /// resolves topic IDs (for v10+) and writes the offsets.
    /// </summary>
    private OffsetCommitResult CommitOffsetsForV2Group(OffsetCommitCommand request, bool useTopicId)
    {
        var responseTopics = new List<OffsetCommitTopicResult>(request.Topics.Count);
        foreach (var topic in request.Topics)
        {
            string topicName = topic.Topic;
            Guid topicId = topic.TopicId;

            if (useTopicId && topic.TopicId != Guid.Empty)
            {
                var resolved = logManager?.ResolveTopicId(topic.TopicId);
                if (resolved == null)
                {
                    var errorPartitions = new List<OffsetCommitPartitionResult>(topic.Partitions.Count);
                    foreach (var p in topic.Partitions)
                    {
                        errorPartitions.Add(new OffsetCommitPartitionResult(p.PartitionIndex, ConsumerGroupErrorStatus.UnknownTopicId));
                    }
                    responseTopics.Add(new OffsetCommitTopicResult
                    {
                        Topic = string.Empty,
                        TopicId = topic.TopicId,
                        Partitions = errorPartitions,
                    });
                    continue;
                }
                topicName = resolved;
            }
            else if (!useTopicId && !string.IsNullOrEmpty(topic.Topic))
            {
                topicId = logManager?.GetTopicId(topic.Topic) ?? Guid.Empty;
            }

            var partitions = new List<OffsetCommitPartitionResult>(topic.Partitions.Count);
            foreach (var partition in topic.Partitions)
            {
                // KIP-1251 — per-partition fence. The member's claimed
                // memberEpoch must be at least the per-partition
                // assignment epoch for (topicId, partition). Older
                // commits for partitions the member still owns are
                // accepted (the KIP's whole point); older commits for
                // partitions that got re-assigned are rejected as
                // STALE_MEMBER_EPOCH.
                var partitionTopicId = topicId != Guid.Empty
                    ? topicId
                    : (logManager?.GetTopicId(topicName) ?? Guid.Empty);
                if (v2Coordinator is not null && partitionTopicId != Guid.Empty
                    && !v2Coordinator.IsPartitionAssignmentValid(
                        request.GroupId,
                        request.MemberId,
                        request.GenerationIdOrMemberEpoch,
                        partitionTopicId,
                        partition.PartitionIndex))
                {
                    partitions.Add(new OffsetCommitPartitionResult(partition.PartitionIndex, ConsumerGroupErrorStatus.StaleMemberEpoch));
                    continue;
                }

                offsetStore?.CommitOffset(request.GroupId, topicName, partition.PartitionIndex, partition.CommittedOffset);
                partitions.Add(new OffsetCommitPartitionResult(partition.PartitionIndex, ConsumerGroupErrorStatus.None));
            }

            responseTopics.Add(new OffsetCommitTopicResult
            {
                Topic = topicName,
                TopicId = topicId,
                Partitions = partitions,
            });
        }

        return new OffsetCommitResult(responseTopics);
    }

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
        lock (_groupLock)
        {
            var groups = new List<GroupDescription>();

            foreach (var groupId in groupIds)
            {
                if (!_consumerGroups.TryGetValue(groupId, out var group))
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
        lock (_groupLock)
        {
            var groups = new List<GroupListing>();

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

                groups.Add(new GroupListing(groupId, group.ProtocolType, state));
            }

            return groups;
        }
    }

    public IReadOnlyList<DeleteGroupResult> DeleteGroups(IReadOnlyList<string> groupIds)
    {
        lock (_groupLock)
        {
            Log.DeleteGroupsRequest(logger, groupIds.Count);

            var results = new List<DeleteGroupResult>();

            foreach (var groupId in groupIds)
            {
                ConsumerGroupErrorStatus status;

                if (!_consumerGroups.TryGetValue(groupId, out var group))
                {
                    // Group doesn't exist - report as success per Kafka protocol
                    // (idempotent delete - deleting non-existent group is not an error)
                    Log.DeleteGroupsNotFound(logger, groupId);
                    status = ConsumerGroupErrorStatus.None;
                }
                else if (group.Members.Count > 0)
                {
                    // Group has active members - cannot delete
                    Log.DeleteGroupsNotEmpty(logger, groupId, group.Members.Count);
                    status = ConsumerGroupErrorStatus.NonEmptyGroup;
                }
                else
                {
                    // Group is empty - safe to delete
                    _consumerGroups.Remove(groupId);

                    // Also clear any committed offsets from the offset store
                    offsetStore?.DeleteGroup(groupId);

                    Log.DeleteGroupsDeleted(logger, groupId);
                    status = ConsumerGroupErrorStatus.None;
                }

                results.Add(new DeleteGroupResult(groupId, status));
            }

            return results;
        }
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
    public OffsetDeleteResult DeleteOffsets(OffsetDeleteCommand request)
    {
        lock (_groupLock)
        {
            var topicResults = new List<OffsetDeleteTopicResult>(request.Topics.Count);

            // Group-level pre-check: if the group is unknown every partition
            // result inherits GroupIdNotFound. We still emit per-partition rows
            // so the response shape matches what the client decoded.
            _consumerGroups.TryGetValue(request.GroupId, out var group);

            // Classic-protocol groups store member subscriptions as opaque
            // Kafka-encoded bytes; Surgewave does not decode them, so the precise
            // "is THIS topic subscribed by any active member" question can't
            // be answered without parsing the Subscription protocol. We pick
            // the conservative reading of KIP-496: if the group has any
            // active member, every topic is treated as potentially subscribed
            // and the delete is rejected with GroupSubscribedToTopic. The
            // operator is then expected to LeaveGroup / DeleteGroup first,
            // matching the librdkafka admin-client's typical retry path. If
            // the group has zero members the offsets are stale by definition
            // and deletion is safe.
            var groupHasActiveMembers = group?.Members.Count > 0;

            foreach (var topicReq in request.Topics)
            {
                var partitionResults = new List<OffsetDeletePartitionResult>(topicReq.Partitions.Count);

                ConsumerGroupErrorStatus topicError;
                if (group is null)
                {
                    topicError = ConsumerGroupErrorStatus.GroupIdNotFound;
                }
                else if (groupHasActiveMembers)
                {
                    topicError = ConsumerGroupErrorStatus.GroupSubscribedToTopic;
                }
                else
                {
                    topicError = ConsumerGroupErrorStatus.None;
                }

                foreach (var partitionIndex in topicReq.Partitions)
                {
                    if (topicError == ConsumerGroupErrorStatus.None)
                    {
                        offsetStore?.DeleteOffset(request.GroupId, topicReq.Name, partitionIndex);
                    }
                    partitionResults.Add(new OffsetDeletePartitionResult(partitionIndex, topicError));
                }

                topicResults.Add(new OffsetDeleteTopicResult
                {
                    Name = topicReq.Name,
                    Partitions = partitionResults,
                });
            }

            return new OffsetDeleteResult(topicResults);
        }
    }

    private static string GetGroupState(ConsumerGroupState group)
    {
        if (group.Members.Count == 0)
        {
            return "Empty";
        }

        // Check if any member has an assignment (inline loop avoids LINQ closure)
        foreach (var m in group.Members.Values)
        {
            if (m.Assignment.Length > 0)
                return "Stable";
        }
        return "PreparingRebalance";
    }

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
    /// Get all committed offsets for a group from memory.
    /// Returns empty dictionary if group doesn't exist.
    /// </summary>
    public Dictionary<string, long> GetGroupCommittedOffsets(string groupId)
    {
        lock (_groupLock)
        {
            if (!_consumerGroups.TryGetValue(groupId, out var group))
            {
                return [];
            }

            return new Dictionary<string, long>(group.CommittedOffsets);
        }
    }

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
    public Dictionary<string, long> CommittedOffsets { get; } = new(); // "topic:partition" -> offset
}

internal sealed class GroupMember
{
    public required string MemberId { get; init; }
    public string? GroupInstanceId { get; init; }
    public required string ClientId { get; init; }
    public required byte[] Metadata { get; set; }
    public byte[] Assignment { get; set; } = Array.Empty<byte>();
    public DateTime LastHeartbeat { get; set; } = DateTime.UtcNow;
}
