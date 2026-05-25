using Kuestenlogik.Surgewave.Broker.GroupStatePersistence;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Queue;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Broker.ShareGroups;

/// <summary>
/// Manages share group lifecycle, membership, record acquisition, and acknowledgements (KIP-932).
/// Share groups deliver every record to exactly one member at a time, with per-message acknowledgement.
/// Unlike consumer groups, ALL partitions of subscribed topics are assigned to ALL members.
/// </summary>
public sealed class ShareGroupCoordinator
{
    private readonly ILogger<ShareGroupCoordinator> logger;
    private readonly LogManager logManager;
    private readonly IQueueViewManager queueViewManager;
    private readonly Dictionary<string, ShareGroupState> _shareGroups;
    private readonly Lock _groupLock = new();
    private readonly IGroupStateStore<ShareGroupState>? _persistence;

    private const int DefaultHeartbeatIntervalMs = 5000;
    private static readonly TimeSpan StaleHeartbeatTimeout = TimeSpan.FromSeconds(30);

    public ShareGroupCoordinator(
        ILogger<ShareGroupCoordinator> logger,
        LogManager logManager,
        IQueueViewManager queueViewManager,
        IGroupStateStore<ShareGroupState>? persistence = null)
    {
        this.logger = logger;
        this.logManager = logManager;
        this.queueViewManager = queueViewManager;
        _persistence = persistence;
        _shareGroups = persistence is null
            ? []
            : persistence.LoadAll().ToDictionary(kv => kv.Key, kv => kv.Value);

        if (_shareGroups.Count > 0)
        {
            logger.LogInformation("ShareGroupCoordinator: recovered {Count} share group(s) from persistence", _shareGroups.Count);
        }
    }

    // ─────────────────────────────────────────────────────────────
    // ShareGroupHeartbeat (API Key 76)
    // ─────────────────────────────────────────────────────────────

    public ShareGroupHeartbeatResponse HandleShareGroupHeartbeat(ShareGroupHeartbeatRequest request)
    {
        lock (_groupLock)
        {
            logger.LogDebug("ShareGroupHeartbeat: GroupId={GroupId}, MemberId={MemberId}, MemberEpoch={MemberEpoch}",
                request.GroupId, request.MemberId, request.MemberEpoch);

            // MemberEpoch -1 means leave
            if (request.MemberEpoch == -1)
            {
                return HandleShareGroupLeave(request);
            }

            if (!_shareGroups.TryGetValue(request.GroupId, out var group))
            {
                group = new ShareGroupState { GroupId = request.GroupId };
                _shareGroups[request.GroupId] = group;
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
                member.SubscribedTopicNames = request.SubscribedTopicNames;
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

            _persistence?.Save(group.GroupId, group);

            return new ShareGroupHeartbeatResponse
            {
                CorrelationId = request.CorrelationId,
                ApiVersion = request.ApiVersion,
                ErrorCode = ErrorCode.None,
                MemberId = memberId,
                MemberEpoch = member.MemberEpoch,
                HeartbeatIntervalMs = DefaultHeartbeatIntervalMs,
                Assignment = assignment
            };
        }
    }

    private ShareGroupHeartbeatResponse HandleShareGroupLeave(ShareGroupHeartbeatRequest request)
    {
        if (_shareGroups.TryGetValue(request.GroupId, out var group))
        {
            if (group.Members.Remove(request.MemberId))
            {
                RebuildSubscribedTopics(group);
                group.GroupEpoch++;
                logger.LogInformation("ShareGroup {GroupId}: member {MemberId} left (epoch={Epoch})",
                    request.GroupId, request.MemberId, group.GroupEpoch);

                if (group.Members.Count == 0 && group.StartOffsets.Count == 0)
                {
                    _persistence?.Delete(group.GroupId);
                }
                else
                {
                    _persistence?.Save(group.GroupId, group);
                }
            }
        }

        return new ShareGroupHeartbeatResponse
        {
            CorrelationId = request.CorrelationId,
            ApiVersion = request.ApiVersion,
            ErrorCode = ErrorCode.None,
            MemberId = request.MemberId,
            MemberEpoch = -1,
            HeartbeatIntervalMs = DefaultHeartbeatIntervalMs
        };
    }

    // ─────────────────────────────────────────────────────────────
    // ShareGroupDescribe (API Key 77)
    // ─────────────────────────────────────────────────────────────

    public ShareGroupDescribeResponse HandleShareGroupDescribe(ShareGroupDescribeRequest request)
    {
        lock (_groupLock)
        {
            var groups = new List<ShareGroupDescribeResponse.DescribedGroup>(request.GroupIds.Count);

            foreach (var groupId in request.GroupIds)
            {
                if (!_shareGroups.TryGetValue(groupId, out var group))
                {
                    groups.Add(new ShareGroupDescribeResponse.DescribedGroup
                    {
                        ErrorCode = ErrorCode.InvalidGroupId,
                        GroupId = groupId,
                        GroupState = "",
                        AssignorName = "uniform",
                        Members = []
                    });
                    continue;
                }

                var members = new List<ShareGroupDescribeResponse.Member>(group.Members.Count);
                foreach (var m in group.Members.Values)
                {
                    var assignment = BuildDescribeAssignment(group, m);
                    members.Add(new ShareGroupDescribeResponse.Member
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

                groups.Add(new ShareGroupDescribeResponse.DescribedGroup
                {
                    ErrorCode = ErrorCode.None,
                    GroupId = groupId,
                    GroupState = GetGroupState(group),
                    GroupEpoch = group.GroupEpoch,
                    // ShareGroup-State haelt KEIN separates AssignmentEpoch-Feld (anders als
                    // Java/Kafka). AssignmentEpoch == GroupEpoch ist damit by-construction
                    // garantiert — die KAFKA-20442-Invariante (empty group => epochs gleich) gilt
                    // trivial.
                    AssignmentEpoch = group.GroupEpoch,
                    AssignorName = "uniform",
                    Members = members
                });
            }

            return new ShareGroupDescribeResponse
            {
                CorrelationId = request.CorrelationId,
                ApiVersion = request.ApiVersion,
                Groups = groups
            };
        }
    }

    // ─────────────────────────────────────────────────────────────
    // ShareFetch (API Key 78)
    // ─────────────────────────────────────────────────────────────

    public async Task<ShareFetchResponse> HandleShareFetch(ShareFetchRequest request, CancellationToken cancellationToken)
    {
        var responses = new List<ShareFetchResponse.ShareFetchableTopicResponse>(request.Topics.Count);

        foreach (var fetchTopic in request.Topics)
        {
            var topicName = logManager.ResolveTopicId(fetchTopic.TopicId);
            var partitions = new List<ShareFetchResponse.PartitionData>(fetchTopic.Partitions.Count);

            foreach (var fetchPartition in fetchTopic.Partitions)
            {
                var acknowledgeErrorCode = ErrorCode.None;

                // Process inline acknowledgement batches first
                if (fetchPartition.AcknowledgementBatches.Count > 0 && topicName != null)
                {
                    acknowledgeErrorCode = ProcessAcknowledgementBatches(
                        topicName, fetchPartition.PartitionIndex, fetchPartition.AcknowledgementBatches);
                }

                // KAFKA-20533: Bei Topic-Loeschung waehrend ShareFetch muss der Per-Partition-
                // Error-Code korrekt UnknownTopicId zurueckgegeben werden (Java's Bug war ein
                // NPE in brokerTopicStats.updateBytesOut(null, ...) und Mapping auf
                // UNKNOWN_SERVER_ERROR). Surgewave testet topicName==null hier direkt und
                // emittiert keine Metrics fuer null-Topics, also kein NPE-Pfad.
                if (topicName == null)
                {
                    partitions.Add(new ShareFetchResponse.PartitionData
                    {
                        PartitionIndex = fetchPartition.PartitionIndex,
                        ErrorCode = ErrorCode.UnknownTopicId,
                        AcknowledgeErrorCode = acknowledgeErrorCode,
                        CurrentLeader = new ShareFetchResponse.LeaderIdAndEpoch { LeaderId = -1, LeaderEpoch = -1 },
                        AcquiredRecords = []
                    });
                    continue;
                }

                var tp = new TopicPartition { Topic = topicName, Partition = fetchPartition.PartitionIndex };
                var log = logManager.GetLog(tp);
                if (log == null)
                {
                    partitions.Add(new ShareFetchResponse.PartitionData
                    {
                        PartitionIndex = fetchPartition.PartitionIndex,
                        ErrorCode = ErrorCode.UnknownTopicOrPartition,
                        AcknowledgeErrorCode = acknowledgeErrorCode,
                        CurrentLeader = new ShareFetchResponse.LeaderIdAndEpoch { LeaderId = -1, LeaderEpoch = -1 },
                        AcquiredRecords = []
                    });
                    continue;
                }

                // Get or create queue view for this topic+partition
                var queueView = queueViewManager.GetOrCreate(topicName, log);

                // Determine max messages to fetch
                var maxMessages = request.MaxRecords > 0 ? request.MaxRecords : 100;

                try
                {
                    var messages = await queueView.ReceiveAsync(
                        fetchPartition.PartitionIndex,
                        maxMessages,
                        request.MemberId,
                        cancellationToken);

                    // Build records from in-flight messages
                    byte[]? records = null;
                    var acquiredRecords = new List<ShareFetchResponse.AcquiredRecords>();

                    if (messages.Count > 0)
                    {
                        // Concatenate record bodies into a single byte array
                        var totalSize = 0;
                        foreach (var msg in messages)
                        {
                            totalSize += msg.Body.Length;
                        }

                        var buffer = new byte[totalSize];
                        var offset = 0;
                        foreach (var msg in messages)
                        {
                            Buffer.BlockCopy(msg.Body, 0, buffer, offset, msg.Body.Length);
                            offset += msg.Body.Length;
                        }
                        records = buffer;

                        // Group consecutive messages into acquired record ranges
                        var rangeStart = messages[0];
                        var rangeLast = messages[0];

                        for (int i = 1; i < messages.Count; i++)
                        {
                            var current = messages[i];
                            if (current.Offset == rangeLast.Offset + 1 && current.DeliveryCount == rangeStart.DeliveryCount)
                            {
                                rangeLast = current;
                            }
                            else
                            {
                                acquiredRecords.Add(new ShareFetchResponse.AcquiredRecords
                                {
                                    FirstOffset = rangeStart.Offset,
                                    LastOffset = rangeLast.Offset,
                                    DeliveryCount = (short)rangeStart.DeliveryCount
                                });
                                rangeStart = current;
                                rangeLast = current;
                            }
                        }

                        acquiredRecords.Add(new ShareFetchResponse.AcquiredRecords
                        {
                            FirstOffset = rangeStart.Offset,
                            LastOffset = rangeLast.Offset,
                            DeliveryCount = (short)rangeStart.DeliveryCount
                        });
                    }

                    partitions.Add(new ShareFetchResponse.PartitionData
                    {
                        PartitionIndex = fetchPartition.PartitionIndex,
                        ErrorCode = ErrorCode.None,
                        AcknowledgeErrorCode = acknowledgeErrorCode,
                        CurrentLeader = new ShareFetchResponse.LeaderIdAndEpoch { LeaderId = 0, LeaderEpoch = 0 },
                        Records = records,
                        AcquiredRecords = acquiredRecords
                    });
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "ShareFetch error for {Topic}-{Partition}", topicName, fetchPartition.PartitionIndex);
                    partitions.Add(new ShareFetchResponse.PartitionData
                    {
                        PartitionIndex = fetchPartition.PartitionIndex,
                        ErrorCode = ErrorCode.Unknown,
                        AcknowledgeErrorCode = acknowledgeErrorCode,
                        CurrentLeader = new ShareFetchResponse.LeaderIdAndEpoch { LeaderId = -1, LeaderEpoch = -1 },
                        AcquiredRecords = []
                    });
                }
            }

            responses.Add(new ShareFetchResponse.ShareFetchableTopicResponse
            {
                TopicId = fetchTopic.TopicId,
                Partitions = partitions
            });
        }

        return new ShareFetchResponse
        {
            CorrelationId = request.CorrelationId,
            ApiVersion = request.ApiVersion,
            ErrorCode = ErrorCode.None,
            Responses = responses,
            NodeEndpoints = []
        };
    }

    // ─────────────────────────────────────────────────────────────
    // ShareAcknowledge (API Key 79)
    // ─────────────────────────────────────────────────────────────

    public ShareAcknowledgeResponse HandleShareAcknowledge(ShareAcknowledgeRequest request)
    {
        var responses = new List<ShareAcknowledgeResponse.ShareAcknowledgeTopicResponse>(request.Topics.Count);

        foreach (var ackTopic in request.Topics)
        {
            var topicName = logManager.ResolveTopicId(ackTopic.TopicId);
            var partitions = new List<ShareAcknowledgeResponse.PartitionData>(ackTopic.Partitions.Count);

            foreach (var ackPartition in ackTopic.Partitions)
            {
                if (topicName == null)
                {
                    partitions.Add(new ShareAcknowledgeResponse.PartitionData
                    {
                        PartitionIndex = ackPartition.PartitionIndex,
                        ErrorCode = ErrorCode.UnknownTopicId,
                        CurrentLeader = new ShareAcknowledgeResponse.LeaderIdAndEpoch { LeaderId = -1, LeaderEpoch = -1 }
                    });
                    continue;
                }

                var errorCode = ProcessAcknowledgementBatches(
                    topicName, ackPartition.PartitionIndex, ackPartition.AcknowledgementBatches);

                partitions.Add(new ShareAcknowledgeResponse.PartitionData
                {
                    PartitionIndex = ackPartition.PartitionIndex,
                    ErrorCode = errorCode,
                    CurrentLeader = new ShareAcknowledgeResponse.LeaderIdAndEpoch { LeaderId = 0, LeaderEpoch = 0 }
                });
            }

            responses.Add(new ShareAcknowledgeResponse.ShareAcknowledgeTopicResponse
            {
                TopicId = ackTopic.TopicId,
                Partitions = partitions
            });
        }

        return new ShareAcknowledgeResponse
        {
            CorrelationId = request.CorrelationId,
            ApiVersion = request.ApiVersion,
            ErrorCode = ErrorCode.None,
            Responses = responses,
            NodeEndpoints = []
        };
    }

    // ─────────────────────────────────────────────────────────────
    // DescribeShareGroupOffsets (API Key 90)
    // ─────────────────────────────────────────────────────────────

    public DescribeShareGroupOffsetsResponse HandleDescribeShareGroupOffsets(DescribeShareGroupOffsetsRequest request)
    {
        lock (_groupLock)
        {
            var groups = new List<DescribeShareGroupOffsetsResponse.DescribeGroupResult>(request.Groups.Count);

            foreach (var groupReq in request.Groups)
            {
                if (!_shareGroups.TryGetValue(groupReq.GroupId, out var group))
                {
                    groups.Add(new DescribeShareGroupOffsetsResponse.DescribeGroupResult
                    {
                        GroupId = groupReq.GroupId,
                        Topics = [],
                        ErrorCode = ErrorCode.InvalidGroupId,
                        ErrorMessage = "Share group not found"
                    });
                    continue;
                }

                var topics = new List<DescribeShareGroupOffsetsResponse.DescribeTopicResult>();

                if (groupReq.Topics != null)
                {
                    foreach (var topicReq in groupReq.Topics)
                    {
                        var topicId = logManager.GetTopicId(topicReq.TopicName);
                        var metadata = logManager.GetTopicMetadata(topicReq.TopicName);
                        var partitions = new List<DescribeShareGroupOffsetsResponse.DescribePartitionResult>(topicReq.Partitions.Count);

                        foreach (var partitionIndex in topicReq.Partitions)
                        {
                            var key = string.Concat(topicReq.TopicName, ":", partitionIndex.ToString());
                            var startOffset = group.StartOffsets.GetValueOrDefault(key, 0);

                            // Calculate lag
                            long lag = -1;
                            var tp = new TopicPartition { Topic = topicReq.TopicName, Partition = partitionIndex };
                            var log = logManager.GetLog(tp);
                            if (log != null)
                            {
                                lag = log.HighWatermark - startOffset;
                                if (lag < 0) lag = 0;
                            }

                            partitions.Add(new DescribeShareGroupOffsetsResponse.DescribePartitionResult
                            {
                                PartitionIndex = partitionIndex,
                                StartOffset = startOffset,
                                LeaderEpoch = 0,
                                Lag = lag,
                                ErrorCode = ErrorCode.None
                            });
                        }

                        topics.Add(new DescribeShareGroupOffsetsResponse.DescribeTopicResult
                        {
                            TopicName = topicReq.TopicName,
                            TopicId = topicId,
                            Partitions = partitions
                        });
                    }
                }
                else
                {
                    // Return offsets for all subscribed topics
                    foreach (var topicName in group.SubscribedTopics)
                    {
                        var topicId = logManager.GetTopicId(topicName);
                        var metadata = logManager.GetTopicMetadata(topicName);
                        if (metadata == null) continue;

                        var partitions = new List<DescribeShareGroupOffsetsResponse.DescribePartitionResult>(metadata.PartitionCount);
                        for (int i = 0; i < metadata.PartitionCount; i++)
                        {
                            var key = string.Concat(topicName, ":", i.ToString());
                            var startOffset = group.StartOffsets.GetValueOrDefault(key, 0);

                            long lag = -1;
                            var tp = new TopicPartition { Topic = topicName, Partition = i };
                            var log = logManager.GetLog(tp);
                            if (log != null)
                            {
                                lag = log.HighWatermark - startOffset;
                                if (lag < 0) lag = 0;
                            }

                            partitions.Add(new DescribeShareGroupOffsetsResponse.DescribePartitionResult
                            {
                                PartitionIndex = i,
                                StartOffset = startOffset,
                                LeaderEpoch = 0,
                                Lag = lag,
                                ErrorCode = ErrorCode.None
                            });
                        }

                        topics.Add(new DescribeShareGroupOffsetsResponse.DescribeTopicResult
                        {
                            TopicName = topicName,
                            TopicId = topicId,
                            Partitions = partitions
                        });
                    }
                }

                groups.Add(new DescribeShareGroupOffsetsResponse.DescribeGroupResult
                {
                    GroupId = groupReq.GroupId,
                    Topics = topics,
                    ErrorCode = ErrorCode.None
                });
            }

            return new DescribeShareGroupOffsetsResponse
            {
                CorrelationId = request.CorrelationId,
                ApiVersion = request.ApiVersion,
                Groups = groups
            };
        }
    }

    // ─────────────────────────────────────────────────────────────
    // AlterShareGroupOffsets (API Key 91)
    // ─────────────────────────────────────────────────────────────

    public AlterShareGroupOffsetsResponse HandleAlterShareGroupOffsets(AlterShareGroupOffsetsRequest request)
    {
        lock (_groupLock)
        {
            if (!_shareGroups.TryGetValue(request.GroupId, out var group))
            {
                return new AlterShareGroupOffsetsResponse
                {
                    CorrelationId = request.CorrelationId,
                    ApiVersion = request.ApiVersion,
                    ErrorCode = ErrorCode.InvalidGroupId,
                    ErrorMessage = "Share group not found",
                    Responses = []
                };
            }

            // Can only alter offsets when group is empty
            if (group.Members.Count > 0)
            {
                return new AlterShareGroupOffsetsResponse
                {
                    CorrelationId = request.CorrelationId,
                    ApiVersion = request.ApiVersion,
                    ErrorCode = ErrorCode.NonEmptyGroup,
                    ErrorMessage = "Cannot alter offsets while group has active members",
                    Responses = []
                };
            }

            var responses = new List<AlterShareGroupOffsetsResponse.AlterTopicResult>(request.Topics.Count);

            foreach (var topic in request.Topics)
            {
                var topicId = logManager.GetTopicId(topic.TopicName);
                var partitions = new List<AlterShareGroupOffsetsResponse.AlterPartitionResult>(topic.Partitions.Count);

                foreach (var partition in topic.Partitions)
                {
                    var key = string.Concat(topic.TopicName, ":", partition.PartitionIndex.ToString());
                    group.StartOffsets[key] = partition.StartOffset;

                    partitions.Add(new AlterShareGroupOffsetsResponse.AlterPartitionResult
                    {
                        PartitionIndex = partition.PartitionIndex,
                        ErrorCode = ErrorCode.None
                    });
                }

                responses.Add(new AlterShareGroupOffsetsResponse.AlterTopicResult
                {
                    TopicName = topic.TopicName,
                    TopicId = topicId,
                    Partitions = partitions
                });
            }

            logger.LogInformation("ShareGroup {GroupId}: altered offsets for {TopicCount} topics",
                request.GroupId, request.Topics.Count);

            _persistence?.Save(group.GroupId, group);

            return new AlterShareGroupOffsetsResponse
            {
                CorrelationId = request.CorrelationId,
                ApiVersion = request.ApiVersion,
                ErrorCode = ErrorCode.None,
                Responses = responses
            };
        }
    }

    // ─────────────────────────────────────────────────────────────
    // DeleteShareGroupOffsets (API Key 92)
    // ─────────────────────────────────────────────────────────────

    public DeleteShareGroupOffsetsResponse HandleDeleteShareGroupOffsets(DeleteShareGroupOffsetsRequest request)
    {
        lock (_groupLock)
        {
            if (!_shareGroups.TryGetValue(request.GroupId, out var group))
            {
                return new DeleteShareGroupOffsetsResponse
                {
                    CorrelationId = request.CorrelationId,
                    ApiVersion = request.ApiVersion,
                    ErrorCode = ErrorCode.InvalidGroupId,
                    ErrorMessage = "Share group not found",
                    Responses = []
                };
            }

            // Can only delete offsets when group is empty
            if (group.Members.Count > 0)
            {
                return new DeleteShareGroupOffsetsResponse
                {
                    CorrelationId = request.CorrelationId,
                    ApiVersion = request.ApiVersion,
                    ErrorCode = ErrorCode.NonEmptyGroup,
                    ErrorMessage = "Cannot delete offsets while group has active members",
                    Responses = []
                };
            }

            var responses = new List<DeleteShareGroupOffsetsResponse.DeleteTopicResult>(request.Topics.Count);

            foreach (var topic in request.Topics)
            {
                var topicId = logManager.GetTopicId(topic.TopicName);

                // Remove all start offsets for this topic
                List<string>? keysToRemove = null;
                var prefix = string.Concat(topic.TopicName, ":");
                foreach (var key in group.StartOffsets.Keys)
                {
                    if (key.StartsWith(prefix, StringComparison.Ordinal))
                    {
                        keysToRemove ??= [];
                        keysToRemove.Add(key);
                    }
                }

                if (keysToRemove != null)
                {
                    foreach (var key in keysToRemove)
                    {
                        group.StartOffsets.Remove(key);
                    }
                }

                responses.Add(new DeleteShareGroupOffsetsResponse.DeleteTopicResult
                {
                    TopicName = topic.TopicName,
                    TopicId = topicId,
                    ErrorCode = ErrorCode.None
                });
            }

            logger.LogInformation("ShareGroup {GroupId}: deleted offsets for {TopicCount} topics",
                request.GroupId, request.Topics.Count);

            if (group.Members.Count == 0 && group.StartOffsets.Count == 0)
            {
                _persistence?.Delete(group.GroupId);
            }
            else
            {
                _persistence?.Save(group.GroupId, group);
            }

            return new DeleteShareGroupOffsetsResponse
            {
                CorrelationId = request.CorrelationId,
                ApiVersion = request.ApiVersion,
                ErrorCode = ErrorCode.None,
                Responses = responses
            };
        }
    }

    // ─────────────────────────────────────────────────────────────
    // Internal helpers
    // ─────────────────────────────────────────────────────────────

    /// <summary>
    /// Common surface across <see cref="ShareFetchRequest.AcknowledgementBatch"/> and
    /// <see cref="ShareAcknowledgeRequest.AcknowledgementBatch"/>. Both wire types carry
    /// the same fields; this lets the per-record dispatcher live in one place.
    /// </summary>
    private readonly record struct AckBatch(long FirstOffset, long LastOffset, IReadOnlyList<sbyte> AcknowledgeTypes);

    /// <summary>
    /// Processes acknowledgement batches against the QueueView for a given topic+partition.
    /// AcknowledgeType (KIP-932):
    ///   0=Gap (skip), 1=Accept (Ack), 2=Release (Nack+requeue), 3=Reject (DLQ), 4=Renew (extend lease).
    /// </summary>
    private ErrorCode ProcessAcknowledgementBatches<TBatch>(
        string topicName,
        int partitionIndex,
        List<TBatch> batches,
        Func<TBatch, AckBatch> project)
    {
        var queueView = queueViewManager.Get(topicName);
        if (queueView == null)
        {
            return ErrorCode.UnknownTopicOrPartition;
        }

        try
        {
            foreach (var batch in batches)
            {
                var ack = project(batch);

                for (long offset = ack.FirstOffset; offset <= ack.LastOffset; offset++)
                {
                    var ackTypeIndex = (int)(offset - ack.FirstOffset);
                    var ackType = ackTypeIndex < ack.AcknowledgeTypes.Count
                        ? ack.AcknowledgeTypes[ackTypeIndex]
                        : ack.AcknowledgeTypes[^1];

                    var messageId = string.Concat(topicName, "-", partitionIndex.ToString(), "-", offset.ToString());

                    switch (ackType)
                    {
                        case 0: // Gap - skip, no action needed
                            break;
                        case 1: // Accept - acknowledge
                            queueView.Ack(messageId);
                            break;
                        case 2: // Release - nack with requeue
                            queueView.Nack(messageId, requeue: true);
                            break;
                        case 3: // Reject - route to DLQ
                            _ = queueView.RejectAsync(messageId, CancellationToken.None);
                            break;
                        case 4: // Renew - extend visibility timeout (lease keeps the same delivery count)
                            queueView.ExtendVisibility(messageId);
                            break;
                    }
                }
            }

            return ErrorCode.None;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error processing acknowledgement batches for {Topic}-{Partition}",
                topicName, partitionIndex);
            return ErrorCode.Unknown;
        }
    }

    private ErrorCode ProcessAcknowledgementBatches(
        string topicName,
        int partitionIndex,
        List<ShareFetchRequest.AcknowledgementBatch> batches) =>
        ProcessAcknowledgementBatches(topicName, partitionIndex, batches,
            b => new AckBatch(b.FirstOffset, b.LastOffset, b.AcknowledgeTypes));

    private ErrorCode ProcessAcknowledgementBatches(
        string topicName,
        int partitionIndex,
        List<ShareAcknowledgeRequest.AcknowledgementBatch> batches) =>
        ProcessAcknowledgementBatches(topicName, partitionIndex, batches,
            b => new AckBatch(b.FirstOffset, b.LastOffset, b.AcknowledgeTypes));

    /// <summary>
    /// Removes members whose last heartbeat exceeds the stale timeout.
    /// </summary>
    public void SweepStaleMembers()
    {
        lock (_groupLock)
        {
            foreach (var group in _shareGroups.Values)
            {
                var beforeCount = group.Members.Count;
                CleanStaleMembers(group);
                if (group.Members.Count != beforeCount)
                {
                    if (group.Members.Count == 0 && group.StartOffsets.Count == 0)
                    {
                        _persistence?.Delete(group.GroupId);
                    }
                    else
                    {
                        _persistence?.Save(group.GroupId, group);
                    }
                }
            }
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
    private ShareGroupHeartbeatResponse.AssignmentInfo? BuildAssignment(ShareGroupState group, ShareGroupMember member)
    {
        if (member.SubscribedTopicNames.Count == 0)
        {
            return null;
        }

        var topicPartitions = new List<ShareGroupHeartbeatResponse.TopicPartitions>();

        foreach (var topicName in member.SubscribedTopicNames)
        {
            var metadata = logManager.GetTopicMetadata(topicName);
            if (metadata == null) continue;

            var partitions = new List<int>(metadata.PartitionCount);
            for (int i = 0; i < metadata.PartitionCount; i++)
            {
                partitions.Add(i);
            }

            topicPartitions.Add(new ShareGroupHeartbeatResponse.TopicPartitions
            {
                TopicId = metadata.TopicId,
                Partitions = partitions
            });
        }

        return topicPartitions.Count > 0
            ? new ShareGroupHeartbeatResponse.AssignmentInfo { TopicPartitions = topicPartitions }
            : null;
    }

    /// <summary>
    /// Builds the describe assignment for a member (includes topic names in addition to IDs).
    /// </summary>
    private ShareGroupDescribeResponse.AssignmentInfo BuildDescribeAssignment(ShareGroupState group, ShareGroupMember member)
    {
        var topicPartitions = new List<ShareGroupDescribeResponse.ShareTopicPartitions>();

        foreach (var topicName in member.SubscribedTopicNames)
        {
            var metadata = logManager.GetTopicMetadata(topicName);
            if (metadata == null) continue;

            var partitions = new List<int>(metadata.PartitionCount);
            for (int i = 0; i < metadata.PartitionCount; i++)
            {
                partitions.Add(i);
            }

            topicPartitions.Add(new ShareGroupDescribeResponse.ShareTopicPartitions
            {
                TopicId = metadata.TopicId,
                TopicName = topicName,
                Partitions = partitions
            });
        }

        return new ShareGroupDescribeResponse.AssignmentInfo { TopicPartitions = topicPartitions };
    }

    private static string GetGroupState(ShareGroupState group)
    {
        if (group.Members.Count == 0) return "Empty";
        return "Stable";
    }
}
