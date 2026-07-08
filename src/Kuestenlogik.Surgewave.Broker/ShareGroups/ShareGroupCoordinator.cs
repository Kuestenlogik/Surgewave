using Kuestenlogik.Surgewave.Broker.GroupStatePersistence;
using Kuestenlogik.Surgewave.Coordination.ShareGroups;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Queue;
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
public sealed class ShareGroupCoordinator : IShareGroupCoordinator
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

    public ShareGroupHeartbeatResult Heartbeat(ShareGroupHeartbeatCommand request)
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

            _persistence?.Save(group.GroupId, group);

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
                if (!_shareGroups.TryGetValue(groupId, out var group))
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
                    // ShareGroup-State haelt KEIN separates AssignmentEpoch-Feld (anders als
                    // Java/Kafka). AssignmentEpoch == GroupEpoch ist damit by-construction
                    // garantiert — die KAFKA-20442-Invariante (empty group => epochs gleich) gilt
                    // trivial.
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

    public async Task<ShareFetchResult> ShareFetchAsync(ShareFetchCommand request, CancellationToken cancellationToken)
    {
        var responses = new List<ShareFetchTopicResult>(request.Topics.Count);
        // KIP-1240 — group-level renew gate, looked up once per request.
        var renewEnabled = LookupRenewEnabled(request.GroupId);

        foreach (var fetchTopic in request.Topics)
        {
            var topicName = logManager.ResolveTopicId(fetchTopic.TopicId);
            var partitions = new List<ShareFetchPartitionResult>(fetchTopic.Partitions.Count);

            foreach (var fetchPartition in fetchTopic.Partitions)
            {
                var acknowledgeStatus = ShareGroupErrorStatus.None;

                // Process inline acknowledgement batches first
                if (fetchPartition.AcknowledgementBatches.Count > 0 && topicName != null)
                {
                    acknowledgeStatus = ProcessAcknowledgementBatches(
                        topicName, fetchPartition.PartitionIndex, fetchPartition.AcknowledgementBatches, renewEnabled);
                }

                // KAFKA-20533: Bei Topic-Loeschung waehrend ShareFetch muss der Per-Partition-
                // Error-Code korrekt UnknownTopicId zurueckgegeben werden (Java's Bug war ein
                // NPE in brokerTopicStats.updateBytesOut(null, ...) und Mapping auf
                // UNKNOWN_SERVER_ERROR). Surgewave testet topicName==null hier direkt und
                // emittiert keine Metrics fuer null-Topics, also kein NPE-Pfad.
                if (topicName == null)
                {
                    partitions.Add(new ShareFetchPartitionResult
                    {
                        PartitionIndex = fetchPartition.PartitionIndex,
                        Status = ShareGroupErrorStatus.UnknownTopicId,
                        AcknowledgeStatus = acknowledgeStatus,
                        CurrentLeader = new ShareLeader(-1, -1),
                        AcquiredRecords = []
                    });
                    continue;
                }

                var tp = new TopicPartition { Topic = topicName, Partition = fetchPartition.PartitionIndex };
                var log = logManager.GetLog(tp);
                if (log == null)
                {
                    partitions.Add(new ShareFetchPartitionResult
                    {
                        PartitionIndex = fetchPartition.PartitionIndex,
                        Status = ShareGroupErrorStatus.UnknownTopicOrPartition,
                        AcknowledgeStatus = acknowledgeStatus,
                        CurrentLeader = new ShareLeader(-1, -1),
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
                    var acquiredRecords = new List<ShareAcquiredRecords>();

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
                                acquiredRecords.Add(new ShareAcquiredRecords(rangeStart.Offset, rangeLast.Offset, (short)rangeStart.DeliveryCount));
                                rangeStart = current;
                                rangeLast = current;
                            }
                        }

                        acquiredRecords.Add(new ShareAcquiredRecords(rangeStart.Offset, rangeLast.Offset, (short)rangeStart.DeliveryCount));
                    }

                    partitions.Add(new ShareFetchPartitionResult
                    {
                        PartitionIndex = fetchPartition.PartitionIndex,
                        Status = ShareGroupErrorStatus.None,
                        AcknowledgeStatus = acknowledgeStatus,
                        CurrentLeader = new ShareLeader(0, 0),
                        Records = records,
                        AcquiredRecords = acquiredRecords
                    });
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "ShareFetch error for {Topic}-{Partition}", topicName, fetchPartition.PartitionIndex);
                    partitions.Add(new ShareFetchPartitionResult
                    {
                        PartitionIndex = fetchPartition.PartitionIndex,
                        Status = ShareGroupErrorStatus.Unknown,
                        AcknowledgeStatus = acknowledgeStatus,
                        CurrentLeader = new ShareLeader(-1, -1),
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

    public ShareAcknowledgeResult ShareAcknowledge(ShareAcknowledgeCommand request)
    {
        var responses = new List<ShareAcknowledgeTopicResult>(request.Topics.Count);
        // KIP-1240 — group-level renew gate, looked up once per request.
        var renewEnabled = LookupRenewEnabled(request.GroupId);

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
                        CurrentLeader = new ShareLeader(-1, -1)
                    });
                    continue;
                }

                var status = ProcessAcknowledgementBatches(
                    topicName, ackPartition.PartitionIndex, ackPartition.AcknowledgementBatches, renewEnabled);

                partitions.Add(new ShareAcknowledgePartitionResult
                {
                    PartitionIndex = ackPartition.PartitionIndex,
                    Status = status,
                    CurrentLeader = new ShareLeader(0, 0)
                });
            }

            responses.Add(new ShareAcknowledgeTopicResult(ackTopic.TopicId, partitions));
        }

        return new ShareAcknowledgeResult(responses);
    }

    // ─────────────────────────────────────────────────────────────
    // DescribeShareGroupOffsets (API Key 90)
    // ─────────────────────────────────────────────────────────────

    public DescribeShareOffsetsResult DescribeOffsets(DescribeShareOffsetsCommand request)
    {
        lock (_groupLock)
        {
            var groups = new List<DescribeShareOffsetsGroupResult>(request.Groups.Count);

            foreach (var groupReq in request.Groups)
            {
                if (!_shareGroups.TryGetValue(groupReq.GroupId, out var group))
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

                var topics = new List<DescribeShareOffsetsTopicResult>();

                if (groupReq.Topics != null)
                {
                    foreach (var topicReq in groupReq.Topics)
                    {
                        var topicId = logManager.GetTopicId(topicReq.TopicName);
                        var partitions = new List<DescribeShareOffsetsPartitionResult>(topicReq.Partitions.Count);

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

                            partitions.Add(new DescribeShareOffsetsPartitionResult
                            {
                                PartitionIndex = partitionIndex,
                                StartOffset = startOffset,
                                LeaderEpoch = 0,
                                Lag = lag,
                                Status = ShareGroupErrorStatus.None
                            });
                        }

                        topics.Add(new DescribeShareOffsetsTopicResult
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

                        var partitions = new List<DescribeShareOffsetsPartitionResult>(metadata.PartitionCount);
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

                            partitions.Add(new DescribeShareOffsetsPartitionResult
                            {
                                PartitionIndex = i,
                                StartOffset = startOffset,
                                LeaderEpoch = 0,
                                Lag = lag,
                                Status = ShareGroupErrorStatus.None
                            });
                        }

                        topics.Add(new DescribeShareOffsetsTopicResult
                        {
                            TopicName = topicName,
                            TopicId = topicId,
                            Partitions = partitions
                        });
                    }
                }

                groups.Add(new DescribeShareOffsetsGroupResult
                {
                    GroupId = groupReq.GroupId,
                    Topics = topics,
                    Status = ShareGroupErrorStatus.None
                });
            }

            return new DescribeShareOffsetsResult(groups);
        }
    }

    // ─────────────────────────────────────────────────────────────
    // AlterShareGroupOffsets (API Key 91)
    // ─────────────────────────────────────────────────────────────

    public AlterShareOffsetsResult AlterOffsets(AlterShareOffsetsCommand request)
    {
        lock (_groupLock)
        {
            if (!_shareGroups.TryGetValue(request.GroupId, out var group))
            {
                return new AlterShareOffsetsResult
                {
                    Status = ShareGroupErrorStatus.InvalidGroupId,
                    ErrorMessage = "Share group not found",
                    Responses = []
                };
            }

            // Can only alter offsets when group is empty
            if (group.Members.Count > 0)
            {
                return new AlterShareOffsetsResult
                {
                    Status = ShareGroupErrorStatus.NonEmptyGroup,
                    ErrorMessage = "Cannot alter offsets while group has active members",
                    Responses = []
                };
            }

            var responses = new List<AlterShareOffsetsTopicResult>(request.Topics.Count);

            foreach (var topic in request.Topics)
            {
                var topicId = logManager.GetTopicId(topic.TopicName);
                var partitions = new List<AlterShareOffsetsPartitionResult>(topic.Partitions.Count);

                foreach (var partition in topic.Partitions)
                {
                    var key = string.Concat(topic.TopicName, ":", partition.PartitionIndex.ToString());
                    group.StartOffsets[key] = partition.StartOffset;

                    partitions.Add(new AlterShareOffsetsPartitionResult(partition.PartitionIndex, ShareGroupErrorStatus.None));
                }

                responses.Add(new AlterShareOffsetsTopicResult
                {
                    TopicName = topic.TopicName,
                    TopicId = topicId,
                    Partitions = partitions
                });
            }

            logger.LogInformation("ShareGroup {GroupId}: altered offsets for {TopicCount} topics",
                request.GroupId, request.Topics.Count);

            _persistence?.Save(group.GroupId, group);

            return new AlterShareOffsetsResult
            {
                Status = ShareGroupErrorStatus.None,
                Responses = responses
            };
        }
    }

    // ─────────────────────────────────────────────────────────────
    // DeleteShareGroupOffsets (API Key 92)
    // ─────────────────────────────────────────────────────────────

    public DeleteShareOffsetsResult DeleteOffsets(DeleteShareOffsetsCommand request)
    {
        lock (_groupLock)
        {
            if (!_shareGroups.TryGetValue(request.GroupId, out var group))
            {
                return new DeleteShareOffsetsResult
                {
                    Status = ShareGroupErrorStatus.InvalidGroupId,
                    ErrorMessage = "Share group not found",
                    Responses = []
                };
            }

            // Can only delete offsets when group is empty
            if (group.Members.Count > 0)
            {
                return new DeleteShareOffsetsResult
                {
                    Status = ShareGroupErrorStatus.NonEmptyGroup,
                    ErrorMessage = "Cannot delete offsets while group has active members",
                    Responses = []
                };
            }

            var responses = new List<DeleteShareOffsetsTopicResult>(request.Topics.Count);

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

                responses.Add(new DeleteShareOffsetsTopicResult
                {
                    TopicName = topic.TopicName,
                    TopicId = topicId,
                    Status = ShareGroupErrorStatus.None
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

            return new DeleteShareOffsetsResult
            {
                Status = ShareGroupErrorStatus.None,
                Responses = responses
            };
        }
    }

    // ─────────────────────────────────────────────────────────────
    // Internal helpers
    // ─────────────────────────────────────────────────────────────

    /// <summary>
    /// Processes acknowledgement batches against the QueueView for a given topic+partition.
    /// AcknowledgeType (KIP-932):
    ///   0=Gap (skip), 1=Accept (Ack), 2=Release (Nack+requeue), 3=Reject (DLQ), 4=Renew (extend lease).
    /// </summary>
    private ShareGroupErrorStatus ProcessAcknowledgementBatches(
        string topicName,
        int partitionIndex,
        IReadOnlyList<ShareAcknowledgementBatch> batches,
        bool renewAcknowledgeEnabled)
    {
        // KIP-1240 — RENEW gate is checked up-front so it takes precedence
        // over QueueView absence: the AckType is invalid because the group
        // config says so, regardless of whether the queue happens to exist.
        if (!renewAcknowledgeEnabled)
        {
            foreach (var ack in batches)
            {
                foreach (var ackType in ack.AcknowledgeTypes)
                {
                    if (ackType == 4) return ShareGroupErrorStatus.InvalidRequest;
                }
            }
        }

        var queueView = queueViewManager.Get(topicName);
        if (queueView == null)
        {
            return ShareGroupErrorStatus.UnknownTopicOrPartition;
        }

        try
        {
            foreach (var ack in batches)
            {
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
                        case 4: // Renew (KIP-1222) - extend visibility timeout.
                            // KIP-1240 gate was already enforced above; the
                            // queueView path here is the success branch.
                            queueView.ExtendVisibility(messageId);
                            break;
                    }
                }
            }

            return ShareGroupErrorStatus.None;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error processing acknowledgement batches for {Topic}-{Partition}",
                topicName, partitionIndex);
            return ShareGroupErrorStatus.Unknown;
        }
    }

    /// <summary>
    /// KIP-1240 — look up a group's <see cref="ShareGroupState.RenewAcknowledgeEnabled"/>
    /// once at the entry of an ack-bearing request. If the group hasn't been
    /// established yet we return the broker default (<c>true</c>) so first-time
    /// clients aren't surprised by a Renew rejection.
    /// </summary>
    private bool LookupRenewEnabled(string? groupId)
    {
        if (string.IsNullOrEmpty(groupId)) return true;
        lock (_groupLock)
        {
            return _shareGroups.TryGetValue(groupId, out var group)
                ? group.RenewAcknowledgeEnabled
                : true;
        }
    }

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
    public string? SetShareGroupConfig(string groupId, string name, string? value)
    {
        if (string.IsNullOrEmpty(groupId)) return "Group name must not be empty.";

        lock (_groupLock)
        {
            if (!_shareGroups.TryGetValue(groupId, out var group))
            {
                group = new ShareGroupState { GroupId = groupId };
                _shareGroups[groupId] = group;
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
