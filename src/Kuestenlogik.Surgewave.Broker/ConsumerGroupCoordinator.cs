using Kuestenlogik.Surgewave.Broker.ConsumerGroupV2;
using Kuestenlogik.Surgewave.Broker.Security;
using Kuestenlogik.Surgewave.Coordination.Consumer;
using Kuestenlogik.Surgewave.Core.Observability;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Broker;

/// <summary>
/// Manages consumer group lifecycle, membership, and offset commits.
/// When wired with a <see cref="ConsumerGroupV2Coordinator"/> reference,
/// OffsetCommit/OffsetFetch requests that target a KIP-848 v2 group fall
/// through to a server-side epoch validation against that coordinator.
/// </summary>
public sealed class ConsumerGroupCoordinator(
    ILogger<ConsumerGroupCoordinator> logger,
    OffsetStore? offsetStore = null,
    LogManager? logManager = null,
    AclAuthorizer? aclAuthorizer = null,
    SurgewaveBrokerObservability? observability = null,
    ConsumerGroupV2Coordinator? v2Coordinator = null)
{
    private readonly Dictionary<string, ConsumerGroupState> _consumerGroups = [];
    private readonly Lock _groupLock = new();

    public JoinGroupResponse HandleJoinGroup(JoinGroupRequest request)
    {
        lock (_groupLock)
        {
            Log.JoinGroupRequest(logger, request.ApiVersion, request.GroupId, request.MemberId,
                request.Protocols.Length, request.Protocols.FirstOrDefault()?.Metadata?.Length ?? 0);

            if (!_consumerGroups.TryGetValue(request.GroupId, out var group))
            {
                group = new ConsumerGroupState
                {
                    GroupId = request.GroupId,
                    GenerationId = 1,
                    ProtocolType = request.ProtocolType,
                    ProtocolName = request.Protocols.FirstOrDefault()?.Name ?? "range"
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
                    Metadata = request.Protocols.FirstOrDefault()?.Metadata ?? Array.Empty<byte>(),
                    Assignment = Array.Empty<byte>()
                };
                group.Members[memberId] = member;
                Log.JoinGroupMemberAdded(logger, memberId, member.Metadata.Length);
            }
            else
            {
                // Update metadata for existing member, but preserve assignment
                member.Metadata = request.Protocols.FirstOrDefault()?.Metadata ?? Array.Empty<byte>();
                Log.JoinGroupMemberUpdated(logger, memberId, member.Metadata.Length);
            }

            // First member becomes the leader
            var leaderId = group.Members.Keys.First();
            var isLeader = memberId == leaderId;

            Log.JoinGroupLeaderInfo(logger, memberId, leaderId, isLeader, group.Members.Count);

            // Prepare member list (only for leader) - inline loop avoids LINQ closures
            JoinGroupResponse.JoinGroupMember[] members;
            int totalMetadataLength = 0;
            if (isLeader)
            {
                members = new JoinGroupResponse.JoinGroupMember[group.Members.Count];
                int i = 0;
                foreach (var m in group.Members.Values)
                {
                    members[i++] = new JoinGroupResponse.JoinGroupMember
                    {
                        MemberId = m.MemberId,
                        GroupInstanceId = m.GroupInstanceId,
                        Metadata = m.Metadata ?? []
                    };
                    totalMetadataLength += m.Metadata?.Length ?? 0;
                }
            }
            else
            {
                members = [];
            }

            Log.JoinGroupResponse(logger, memberId, members.Length, totalMetadataLength);

            return new JoinGroupResponse
            {
                CorrelationId = request.CorrelationId,
                ApiVersion = request.ApiVersion,
                ErrorCode = ErrorCode.None,
                GenerationId = group.GenerationId,
                ProtocolName = group.ProtocolName,
                Leader = leaderId,
                MemberId = memberId,
                Members = members
            };
        }
    }

    public SyncGroupResponse HandleSyncGroup(SyncGroupRequest request)
    {
        lock (_groupLock)
        {
            Log.SyncGroupRequest(logger, request.ApiVersion, request.GroupId, request.MemberId, request.GenerationId);
            Log.SyncGroupAssignmentsCount(logger, request.Assignments.Length);

            if (!_consumerGroups.TryGetValue(request.GroupId, out var group))
            {
                Log.SyncGroupNotFound(logger, request.GroupId);
                return new SyncGroupResponse
                {
                    CorrelationId = request.CorrelationId,
                    ApiVersion = request.ApiVersion,
                    ErrorCode = ErrorCode.UnknownMemberId,
                    Assignment = Array.Empty<byte>()
                };
            }

            // Store assignments if this is the leader
            if (request.Assignments.Length > 0)
            {
                Log.SyncGroupLeaderAssignments(logger, request.Assignments.Length);
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

            return new SyncGroupResponse
            {
                CorrelationId = request.CorrelationId,
                ApiVersion = request.ApiVersion,
                ErrorCode = ErrorCode.None,
                Assignment = memberAssignment
            };
        }
    }

    public HeartbeatResponse HandleHeartbeat(HeartbeatRequest request)
    {
        lock (_groupLock)
        {
            if (!_consumerGroups.TryGetValue(request.GroupId, out var group))
            {
                return new HeartbeatResponse
                {
                    CorrelationId = request.CorrelationId,
                    ApiVersion = request.ApiVersion,
                    ErrorCode = ErrorCode.UnknownMemberId
                };
            }

            if (!group.Members.TryGetValue(request.MemberId, out var member))
            {
                return new HeartbeatResponse
                {
                    CorrelationId = request.CorrelationId,
                    ApiVersion = request.ApiVersion,
                    ErrorCode = ErrorCode.UnknownMemberId
                };
            }

            // Update heartbeat timestamp
            member.LastHeartbeat = DateTime.UtcNow;

            // Heartbeat successful
            return new HeartbeatResponse
            {
                CorrelationId = request.CorrelationId,
                ApiVersion = request.ApiVersion,
                ErrorCode = ErrorCode.None
            };
        }
    }

    public LeaveGroupResponse HandleLeaveGroup(LeaveGroupRequest request)
    {
        lock (_groupLock)
        {
            if (!_consumerGroups.TryGetValue(request.GroupId, out var group))
            {
                return new LeaveGroupResponse
                {
                    CorrelationId = request.CorrelationId,
                    ApiVersion = request.ApiVersion,
                    ErrorCode = ErrorCode.UnknownMemberId
                };
            }

            // Handle V3+ batch leave (members array)
            if (request.Members.Length > 0)
            {
                var memberResponses = new List<LeaveGroupResponse.MemberResponse>();
                foreach (var member in request.Members)
                {
                    var removed = group.Members.Remove(member.MemberId);
                    memberResponses.Add(new LeaveGroupResponse.MemberResponse
                    {
                        MemberId = member.MemberId,
                        GroupInstanceId = member.GroupInstanceId,
                        ErrorCode = removed ? ErrorCode.None : ErrorCode.UnknownMemberId
                    });
                }

                return new LeaveGroupResponse
                {
                    CorrelationId = request.CorrelationId,
                    ApiVersion = request.ApiVersion,
                    ErrorCode = ErrorCode.None,
                    Members = memberResponses.ToArray()
                };
            }

            // Handle V0-2 single member leave
            if (!string.IsNullOrEmpty(request.MemberId))
            {
                group.Members.Remove(request.MemberId);
            }

            return new LeaveGroupResponse
            {
                CorrelationId = request.CorrelationId,
                ApiVersion = request.ApiVersion,
                ErrorCode = ErrorCode.None
            };
        }
    }

    public OffsetFetchResponse HandleOffsetFetch(OffsetFetchRequest request)
    {
        lock (_groupLock)
        {
            // v8+: Multi-group batch fetch using Groups array
            if (request.ApiVersion >= 8 && request.Groups != null)
            {
                return HandleOffsetFetchV8Plus(request);
            }

            // v1-7: Single group fetch
            return HandleOffsetFetchSingleGroup(request, request.GroupId ?? string.Empty, request.Topics);
        }
    }

    private OffsetFetchResponse HandleOffsetFetchV8Plus(OffsetFetchRequest request)
    {
        var groups = new List<OffsetFetchResponseGroup>();

        foreach (var groupRequest in request.Groups!)
        {
            var topics = FetchOffsetsForGroup(groupRequest.GroupId, groupRequest.Topics, request.ApiVersion);
            groups.Add(new OffsetFetchResponseGroup
            {
                GroupId = groupRequest.GroupId,
                Topics = topics,
                ErrorCode = ErrorCode.None
            });
        }

        return new OffsetFetchResponse
        {
            CorrelationId = request.CorrelationId,
            ApiVersion = request.ApiVersion,
            Groups = groups,
            ErrorCode = ErrorCode.None
        };
    }

    private OffsetFetchResponse HandleOffsetFetchSingleGroup(OffsetFetchRequest request, string groupId, List<TopicPartitionRequest>? topicRequests)
    {
        var topics = FetchOffsetsForGroup(groupId, topicRequests, request.ApiVersion);

        return new OffsetFetchResponse
        {
            CorrelationId = request.CorrelationId,
            ApiVersion = request.ApiVersion,
            Topics = topics,
            ErrorCode = ErrorCode.None
        };
    }

    private List<TopicPartitionOffset> FetchOffsetsForGroup(string groupId, List<TopicPartitionRequest>? topicRequests, short apiVersion)
    {
        var topics = new List<TopicPartitionOffset>();
        bool useTopicId = apiVersion >= 10;

        Log.OffsetFetchRequest(logger, groupId, topicRequests?.Count ?? 0);

        if (!_consumerGroups.TryGetValue(groupId, out var group))
        {
            Log.OffsetFetchGroupNotInMemory(logger, groupId);
            // Group doesn't exist in memory - try to get from persisted store
            if (topicRequests != null)
            {
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
                            var errorPartitions = new List<PartitionOffsetMetadata>(topicRequest.PartitionIndexes.Count);
                            foreach (var p in topicRequest.PartitionIndexes)
                            {
                                errorPartitions.Add(new PartitionOffsetMetadata
                                {
                                    PartitionIndex = p,
                                    CommittedOffset = -1,
                                    ErrorCode = ErrorCode.UnknownTopicId
                                });
                            }

                            topics.Add(new TopicPartitionOffset
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

                    var partitions = new List<PartitionOffsetMetadata>(topicRequest.PartitionIndexes.Count);
                    foreach (var partitionIndex in topicRequest.PartitionIndexes)
                    {
                        // Try to get from persisted store
                        var offset = offsetStore?.GetCommittedOffset(groupId, topicName, partitionIndex) ?? -1;
                        Log.OffsetFetchFromStore(logger, groupId, topicName, partitionIndex, offset);

                        partitions.Add(new PartitionOffsetMetadata
                        {
                            PartitionIndex = partitionIndex,
                            CommittedOffset = offset,
                            ErrorCode = ErrorCode.None
                        });
                    }

                    topics.Add(new TopicPartitionOffset
                    {
                        Topic = topicName,
                        TopicId = topicId,
                        Partitions = partitions
                    });
                }
            }

            return topics;
        }

        Log.OffsetFetchGroupInMemory(logger, groupId, group.CommittedOffsets.Count);

        // Group exists in memory - return committed offsets (check store as fallback)
        if (topicRequests != null)
        {
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
                        var errorPartitions = new List<PartitionOffsetMetadata>(topicRequest.PartitionIndexes.Count);
                        foreach (var p in topicRequest.PartitionIndexes)
                        {
                            errorPartitions.Add(new PartitionOffsetMetadata
                            {
                                PartitionIndex = p,
                                CommittedOffset = -1,
                                ErrorCode = ErrorCode.UnknownTopicId
                            });
                        }

                        topics.Add(new TopicPartitionOffset
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

                var partitions = new List<PartitionOffsetMetadata>(topicRequest.PartitionIndexes.Count);
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

                    partitions.Add(new PartitionOffsetMetadata
                    {
                        PartitionIndex = partitionIndex,
                        CommittedOffset = offset,
                        ErrorCode = ErrorCode.None
                    });
                }

                topics.Add(new TopicPartitionOffset
                {
                    Topic = topicName,
                    TopicId = topicId,
                    Partitions = partitions
                });
            }
        }

        return topics;
    }

    public OffsetCommitResponse HandleOffsetCommit(OffsetCommitRequest request)
    {
        lock (_groupLock)
        {
            bool useTopicId = request.ApiVersion >= 10;

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
                    // from the neutral status back to the wire error code.
                    if (v2Status != ConsumerGroupFenceStatus.NotAV2Group)
                    {
                        return BuildOffsetCommitErrorResponse(request, v2Status switch
                        {
                            ConsumerGroupFenceStatus.UnknownMember => ErrorCode.UnknownMemberId,
                            ConsumerGroupFenceStatus.FencedEpoch => ErrorCode.FencedMemberEpoch,
                            ConsumerGroupFenceStatus.StaleEpoch => ErrorCode.StaleMemberEpoch,
                            _ => ErrorCode.UnknownMemberId,
                        });
                    }
                }

                // Group genuinely unknown.
                return BuildOffsetCommitErrorResponse(request, ErrorCode.UnknownMemberId);
            }

            // Commit the offsets
            var responseTopics = new List<TopicPartitionCommitResult>();
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
                        var errorPartitions = new List<PartitionCommitResult>(topic.Partitions.Count);
                        foreach (var p in topic.Partitions)
                        {
                            errorPartitions.Add(new PartitionCommitResult
                            {
                                PartitionIndex = p.PartitionIndex,
                                ErrorCode = ErrorCode.UnknownTopicId
                            });
                        }

                        responseTopics.Add(new TopicPartitionCommitResult
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

                var partitions = new List<PartitionCommitResult>(topic.Partitions.Count);
                foreach (var partition in topic.Partitions)
                {
                    var key = string.Concat(topicName, ":", partition.PartitionIndex.ToString());
                    group.CommittedOffsets[key] = partition.CommittedOffset;

                    // Persist to offset store
                    offsetStore?.CommitOffset(request.GroupId, topicName, partition.PartitionIndex, partition.CommittedOffset);

                    partitions.Add(new PartitionCommitResult
                    {
                        PartitionIndex = partition.PartitionIndex,
                        ErrorCode = ErrorCode.None
                    });
                }

                responseTopics.Add(new TopicPartitionCommitResult
                {
                    Topic = topicName,
                    TopicId = topicId,
                    Partitions = partitions
                });
            }

            return new OffsetCommitResponse
            {
                CorrelationId = request.CorrelationId,
                ApiVersion = request.ApiVersion,
                Topics = responseTopics
            };
        }
    }

    /// <summary>
    /// Persists offsets for a KIP-848 v2 group through the shared <see cref="OffsetStore"/>.
    /// The v2 coordinator already validated member identity and epoch; this method just
    /// resolves topic IDs (for v10+) and writes the offsets.
    /// </summary>
    private OffsetCommitResponse CommitOffsetsForV2Group(OffsetCommitRequest request, bool useTopicId)
    {
        var responseTopics = new List<TopicPartitionCommitResult>(request.Topics.Count);
        foreach (var topic in request.Topics)
        {
            string topicName = topic.Topic;
            Guid topicId = topic.TopicId;

            if (useTopicId && topic.TopicId != Guid.Empty)
            {
                var resolved = logManager?.ResolveTopicId(topic.TopicId);
                if (resolved == null)
                {
                    var errorPartitions = new List<PartitionCommitResult>(topic.Partitions.Count);
                    foreach (var p in topic.Partitions)
                    {
                        errorPartitions.Add(new PartitionCommitResult
                        {
                            PartitionIndex = p.PartitionIndex,
                            ErrorCode = ErrorCode.UnknownTopicId,
                        });
                    }
                    responseTopics.Add(new TopicPartitionCommitResult
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

            var partitions = new List<PartitionCommitResult>(topic.Partitions.Count);
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
                    partitions.Add(new PartitionCommitResult
                    {
                        PartitionIndex = partition.PartitionIndex,
                        ErrorCode = ErrorCode.StaleMemberEpoch,
                    });
                    continue;
                }

                offsetStore?.CommitOffset(request.GroupId, topicName, partition.PartitionIndex, partition.CommittedOffset);
                partitions.Add(new PartitionCommitResult
                {
                    PartitionIndex = partition.PartitionIndex,
                    ErrorCode = ErrorCode.None,
                });
            }

            responseTopics.Add(new TopicPartitionCommitResult
            {
                Topic = topicName,
                TopicId = topicId,
                Partitions = partitions,
            });
        }

        return new OffsetCommitResponse
        {
            CorrelationId = request.CorrelationId,
            ApiVersion = request.ApiVersion,
            Topics = responseTopics,
        };
    }

    /// <summary>
    /// Builds an error response that flat-fills every requested partition with the
    /// supplied error code. Centralised to avoid the inline-loop variants we used to
    /// have at every error branch.
    /// </summary>
    private static OffsetCommitResponse BuildOffsetCommitErrorResponse(OffsetCommitRequest request, ErrorCode errorCode)
    {
        var errorTopics = new List<TopicPartitionCommitResult>(request.Topics.Count);
        foreach (var topic in request.Topics)
        {
            var partitions = new List<PartitionCommitResult>(topic.Partitions.Count);
            foreach (var partition in topic.Partitions)
            {
                partitions.Add(new PartitionCommitResult
                {
                    PartitionIndex = partition.PartitionIndex,
                    ErrorCode = errorCode,
                });
            }
            errorTopics.Add(new TopicPartitionCommitResult
            {
                Topic = topic.Topic,
                TopicId = topic.TopicId,
                Partitions = partitions,
            });
        }

        return new OffsetCommitResponse
        {
            CorrelationId = request.CorrelationId,
            ApiVersion = request.ApiVersion,
            Topics = errorTopics,
        };
    }

    public DescribeGroupsResponse HandleDescribeGroups(DescribeGroupsRequest request)
    {
        lock (_groupLock)
        {
            var groups = new List<DescribeGroupsResponse.DescribedGroup>();

            foreach (var groupId in request.GroupIds)
            {
                if (!_consumerGroups.TryGetValue(groupId, out var group))
                {
                    groups.Add(new DescribeGroupsResponse.DescribedGroup
                    {
                        ErrorCode = ErrorCode.InvalidGroupId,
                        GroupId = groupId,
                        GroupState = "",
                        ProtocolType = "",
                        ProtocolData = "",
                        Members = new List<DescribeGroupsResponse.GroupMember>()
                    });
                    continue;
                }

                // Inline loop avoids LINQ closure allocations
                var members = new List<DescribeGroupsResponse.GroupMember>(group.Members.Count);
                foreach (var m in group.Members.Values)
                {
                    members.Add(new DescribeGroupsResponse.GroupMember
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

                groups.Add(new DescribeGroupsResponse.DescribedGroup
                {
                    ErrorCode = ErrorCode.None,
                    GroupId = groupId,
                    GroupState = GetGroupState(group),
                    ProtocolType = group.ProtocolType,
                    ProtocolData = group.ProtocolName,
                    Members = members,
                    AuthorizedOperations = authorizedOps
                });
            }

            return new DescribeGroupsResponse
            {
                CorrelationId = request.CorrelationId,
                ApiVersion = request.ApiVersion,
                Groups = groups
            };
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

    public ListGroupsResponse HandleListGroups(ListGroupsRequest request)
    {
        lock (_groupLock)
        {
            var groups = new List<ListGroupsResponse.ListedGroup>();

            foreach (var (groupId, group) in _consumerGroups)
            {
                var state = GetGroupState(group);

                // Filter by state if requested
                if (request.StatesFilter != null && request.StatesFilter.Count > 0)
                {
                    if (!request.StatesFilter.Contains(state, StringComparer.OrdinalIgnoreCase))
                    {
                        continue;
                    }
                }

                groups.Add(new ListGroupsResponse.ListedGroup
                {
                    GroupId = groupId,
                    ProtocolType = group.ProtocolType,
                    GroupState = state
                });
            }

            return new ListGroupsResponse
            {
                CorrelationId = request.CorrelationId,
                ApiVersion = request.ApiVersion,
                ErrorCode = ErrorCode.None,
                Groups = groups
            };
        }
    }

    public DeleteGroupsResponse HandleDeleteGroups(DeleteGroupsRequest request)
    {
        lock (_groupLock)
        {
            Log.DeleteGroupsRequest(logger, request.GroupIds.Count);

            var results = new List<DeleteGroupsResponse.DeletableGroupResult>();

            foreach (var groupId in request.GroupIds)
            {
                ErrorCode errorCode;

                if (!_consumerGroups.TryGetValue(groupId, out var group))
                {
                    // Group doesn't exist - report as success per Kafka protocol
                    // (idempotent delete - deleting non-existent group is not an error)
                    Log.DeleteGroupsNotFound(logger, groupId);
                    errorCode = ErrorCode.None;
                }
                else if (group.Members.Count > 0)
                {
                    // Group has active members - cannot delete
                    Log.DeleteGroupsNotEmpty(logger, groupId, group.Members.Count);
                    errorCode = ErrorCode.NonEmptyGroup;
                }
                else
                {
                    // Group is empty - safe to delete
                    _consumerGroups.Remove(groupId);

                    // Also clear any committed offsets from the offset store
                    offsetStore?.DeleteGroup(groupId);

                    Log.DeleteGroupsDeleted(logger, groupId);
                    errorCode = ErrorCode.None;
                }

                results.Add(new DeleteGroupsResponse.DeletableGroupResult
                {
                    GroupId = groupId,
                    ErrorCode = errorCode
                });
            }

            return new DeleteGroupsResponse
            {
                CorrelationId = request.CorrelationId,
                ApiVersion = request.ApiVersion,
                ThrottleTimeMs = 0,
                Results = results
            };
        }
    }

    /// <summary>
    /// KIP-496: delete committed offsets for a consumer group on the listed
    /// (topic, partition) tuples. Per spec a partition can be deleted only when
    /// the group has no active member subscribed to that topic — otherwise the
    /// group could later commit a new offset and the delete would silently
    /// race. Per-partition error codes:
    /// <list type="bullet">
    ///   <item><see cref="ErrorCode.GroupIdNotFound"/> — group is unknown.</item>
    ///   <item><see cref="ErrorCode.GroupSubscribedToTopic"/> — at least one
    ///         active member of the group still subscribes to this topic.</item>
    ///   <item><see cref="ErrorCode.None"/> — offset deleted (or wasn't there
    ///         to begin with — KIP-496 makes the operation idempotent).</item>
    /// </list>
    /// </summary>
    public OffsetDeleteResponse HandleOffsetDelete(OffsetDeleteRequest request)
    {
        lock (_groupLock)
        {
            var topicResults = new List<OffsetDeleteResponse.OffsetDeleteTopicResponse>(request.Topics.Count);

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
                var partitionResults = new List<OffsetDeleteResponse.OffsetDeletePartitionResponse>(topicReq.Partitions.Count);

                ErrorCode topicError;
                if (group is null)
                {
                    topicError = ErrorCode.GroupIdNotFound;
                }
                else if (groupHasActiveMembers)
                {
                    topicError = ErrorCode.GroupSubscribedToTopic;
                }
                else
                {
                    topicError = ErrorCode.None;
                }

                foreach (var partitionReq in topicReq.Partitions)
                {
                    if (topicError == ErrorCode.None)
                    {
                        offsetStore?.DeleteOffset(request.GroupId, topicReq.Name, partitionReq.PartitionIndex);
                    }
                    partitionResults.Add(new OffsetDeleteResponse.OffsetDeletePartitionResponse
                    {
                        PartitionIndex = partitionReq.PartitionIndex,
                        ErrorCode = topicError,
                    });
                }

                topicResults.Add(new OffsetDeleteResponse.OffsetDeleteTopicResponse
                {
                    Name = topicReq.Name,
                    Partitions = partitionResults,
                });
            }

            return new OffsetDeleteResponse
            {
                CorrelationId = request.CorrelationId,
                ApiVersion = request.ApiVersion,
                ErrorCode = ErrorCode.None,
                ThrottleTimeMs = 0,
                Topics = topicResults,
            };
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
