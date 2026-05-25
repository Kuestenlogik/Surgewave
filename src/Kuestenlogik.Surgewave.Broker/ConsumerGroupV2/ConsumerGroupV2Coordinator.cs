using Kuestenlogik.Surgewave.Broker.GroupStatePersistence;
using Kuestenlogik.Surgewave.Broker.Native.Assignors;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Broker.ConsumerGroupV2;

/// <summary>
/// Manages consumer group v2 lifecycle, membership, and server-side partition assignment (KIP-848).
/// Unlike classic consumer groups, the SERVER assigns partitions — no SyncGroup needed.
/// The heartbeat carries the full subscription and receives the assignment back.
///
/// The reconciliation flow follows KIP-848: when the membership or subscription changes
/// the server recomputes a target assignment, then advertises a "safe" subset of that
/// target on each heartbeat — partitions still owned by another member are withheld
/// until the previous owner has stopped reporting them in <c>OwnedTopicPartitions</c>.
/// Once every member's <c>OwnedTopicPartitions</c> matches its target the group reaches
/// the Stable state.
/// </summary>
public sealed class ConsumerGroupV2Coordinator
{
    private readonly ILogger<ConsumerGroupV2Coordinator> _logger;
    private readonly Dictionary<string, ConsumerGroupV2State> _groups;
    private readonly Lock _groupLock = new();
    private readonly TargetAssignmentComputer _assignmentComputer;
    private readonly IGroupStateStore<ConsumerGroupV2State>? _persistence;

    private const int DefaultHeartbeatIntervalMs = 5000;
    private static readonly TimeSpan StaleHeartbeatTimeout = TimeSpan.FromSeconds(45);

    public ConsumerGroupV2Coordinator(
        ILogger<ConsumerGroupV2Coordinator> logger,
        LogManager logManager,
        IGroupStateStore<ConsumerGroupV2State>? persistence = null)
    {
        _logger = logger;
        _assignmentComputer = new TargetAssignmentComputer(logManager);
        _persistence = persistence;
        _groups = persistence is null
            ? []
            : persistence.LoadAll().ToDictionary(kv => kv.Key, kv => kv.Value);

        if (_groups.Count > 0)
        {
            _logger.LogInformation("ConsumerGroupV2Coordinator: recovered {Count} group(s) from persistence", _groups.Count);
        }
    }

    // ─────────────────────────────────────────────────────────────
    // ConsumerGroupHeartbeat (API Key 68)
    // ─────────────────────────────────────────────────────────────

    public ConsumerGroupHeartbeatResponse HandleConsumerGroupHeartbeat(ConsumerGroupHeartbeatRequest request)
    {
        lock (_groupLock)
        {
            _logger.LogDebug("ConsumerGroupV2Heartbeat: GroupId={GroupId}, MemberId={MemberId}, MemberEpoch={MemberEpoch}",
                request.GroupId, request.MemberId, request.MemberEpoch);

            // MemberEpoch -1 means leave
            if (request.MemberEpoch == -1)
            {
                return HandleLeave(request);
            }

            if (!_groups.TryGetValue(request.GroupId, out var group))
            {
                group = new ConsumerGroupV2State { GroupId = request.GroupId };
                _groups[request.GroupId] = group;
                _logger.LogInformation("ConsumerGroupV2 created: {GroupId}", request.GroupId);
            }

            // KIP-848 heartbeat-epoch fencing: a heartbeat carrying a non-zero
            // MemberEpoch must match the broker's view exactly. A behind-by-one
            // member gets STALE_MEMBER_EPOCH (113) and re-fetches; a member
            // claiming an epoch the broker never assigned is impossible state
            // and gets FENCED_MEMBER_EPOCH (110); a non-zero epoch with an
            // unknown memberId is UNKNOWN_MEMBER_ID. The check runs before any
            // mutation so a malformed client can never silently advance state.
            if (TryValidateHeartbeatEpoch(request, group, out var fenceResponse))
            {
                return fenceResponse;
            }

            // KIP-955: the founding member's first heartbeat seeds group-level metadata
            // (assignor preference and rebalance timeout). Later heartbeats can adjust
            // the rebalance timeout downward but cannot push it back up — a slow member
            // must not relax the group's tolerance once others are coordinating against
            // the original value.
            ApplyInitMetadata(group, request);

            bool targetNeedsRecompute = false;

            if (TryUpdateAssignor(group, request.ServerAssignor))
            {
                targetNeedsRecompute = true;
            }

            CleanStaleMembers(group, ref targetNeedsRecompute);

            var memberId = request.MemberId;
            if (request.MemberEpoch == 0 && string.IsNullOrEmpty(memberId))
            {
                memberId = $"{request.ClientId}-{Guid.NewGuid()}";
            }

            if (!group.Members.TryGetValue(memberId, out var member))
            {
                member = new ConsumerGroupV2Member
                {
                    MemberId = memberId,
                    InstanceId = request.InstanceId,
                    RackId = request.RackId,
                    ClientId = request.ClientId,
                    ClientHost = "*"
                };
                group.Members[memberId] = member;
                group.GroupEpoch++;
                targetNeedsRecompute = true;
                _logger.LogInformation("ConsumerGroupV2 {GroupId}: member {MemberId} joined (epoch={Epoch})",
                    request.GroupId, memberId, group.GroupEpoch);
            }

            member.LastHeartbeat = DateTime.UtcNow;
            if (request.InstanceId != null) member.InstanceId = request.InstanceId;
            if (request.RackId != null) member.RackId = request.RackId;

            if (request.SubscribedTopicNames != null
                && !AreSubscriptionsEqual(member.SubscribedTopicNames, request.SubscribedTopicNames))
            {
                member.SubscribedTopicNames = request.SubscribedTopicNames;
                group.GroupEpoch++;
                targetNeedsRecompute = true;
            }

            // Update what the member reports owning. KIP-848: this is what the
            // reconciler uses to decide when a partition is safe to hand to its new
            // owner. Treat null as "unchanged since last heartbeat".
            if (request.TopicPartitions != null)
            {
                member.OwnedTopicPartitions = ConvertOwnedFromRequest(request.TopicPartitions);
            }

            if (targetNeedsRecompute)
            {
                _assignmentComputer.Compute(group);
            }

            // Reconcile: advertise only target partitions not still held by another member.
            var safeAssignment = ConsumerGroupV2Reconciler.ComputeSafeAssignment(group, member);
            member.Assignment = safeAssignment;
            member.MemberEpoch = group.GroupEpoch;

            // Persist after every heartbeat that changed authoritative state. The
            // store debounces writes internally so this is cheap.
            _persistence?.Save(group.GroupId, group);

            return new ConsumerGroupHeartbeatResponse
            {
                CorrelationId = request.CorrelationId,
                ApiVersion = request.ApiVersion,
                ErrorCode = ErrorCode.None,
                MemberId = memberId,
                MemberEpoch = member.MemberEpoch,
                HeartbeatIntervalMs = DefaultHeartbeatIntervalMs,
                MemberAssignment = BuildHeartbeatAssignment(safeAssignment),
            };
        }
    }

    private ConsumerGroupHeartbeatResponse HandleLeave(ConsumerGroupHeartbeatRequest request)
    {
        if (_groups.TryGetValue(request.GroupId, out var group))
        {
            if (group.Members.Remove(request.MemberId))
            {
                group.GroupEpoch++;
                _assignmentComputer.Compute(group);
                _logger.LogInformation("ConsumerGroupV2 {GroupId}: member {MemberId} left (epoch={Epoch})",
                    request.GroupId, request.MemberId, group.GroupEpoch);

                if (group.Members.Count == 0)
                {
                    _persistence?.Delete(group.GroupId);
                }
                else
                {
                    _persistence?.Save(group.GroupId, group);
                }
            }
        }

        return new ConsumerGroupHeartbeatResponse
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
    // ConsumerGroupDescribe (API Key 69)
    // ─────────────────────────────────────────────────────────────

    public ConsumerGroupDescribeResponse HandleConsumerGroupDescribe(ConsumerGroupDescribeRequest request)
    {
        lock (_groupLock)
        {
            var groups = new List<ConsumerGroupDescribeResponse.DescribedGroup>(request.GroupIds.Count);

            foreach (var groupId in request.GroupIds)
            {
                if (!_groups.TryGetValue(groupId, out var group))
                {
                    groups.Add(new ConsumerGroupDescribeResponse.DescribedGroup
                    {
                        ErrorCode = ErrorCode.InvalidGroupId,
                        GroupId = groupId,
                        GroupState = "",
                        AssignorName = "range",
                        Members = []
                    });
                    continue;
                }

                var members = new List<ConsumerGroupDescribeResponse.Member>(group.Members.Count);
                foreach (var m in group.Members.Values)
                {
                    members.Add(new ConsumerGroupDescribeResponse.Member
                    {
                        MemberId = m.MemberId,
                        InstanceId = m.InstanceId,
                        RackId = m.RackId,
                        MemberEpoch = m.MemberEpoch,
                        ClientId = m.ClientId ?? "",
                        ClientHost = m.ClientHost ?? "*",
                        SubscribedTopicNames = m.SubscribedTopicNames,
                        SubscribedTopicRegex = m.SubscribedTopicRegex,
                        // KAFKA-20431: MemberAssignment muss assigned + pending-revocation enthalten,
                        // sonst "verschwinden" Partitions waehrend der Reconciliation aus Admin-Sicht.
                        // Pending-Revocation in Surgewave = OwnedTopicPartitions \ Assignment
                        // (was der Member noch besitzt, aber nicht mehr kommuniziert bekommen soll).
                        MemberAssignment = ConvertToDescribeAssignmentWithPendingRevocation(m),
                        TargetAssignment = ConvertToDescribeAssignment(m.TargetAssignment)
                    });
                }

                groups.Add(new ConsumerGroupDescribeResponse.DescribedGroup
                {
                    ErrorCode = ErrorCode.None,
                    GroupId = groupId,
                    GroupState = GetGroupState(group),
                    GroupEpoch = group.GroupEpoch,
                    AssignmentEpoch = group.AssignmentEpoch,
                    AssignorName = group.AssignorName,
                    Members = members
                });
            }

            return new ConsumerGroupDescribeResponse
            {
                CorrelationId = request.CorrelationId,
                ApiVersion = request.ApiVersion,
                Groups = groups
            };
        }
    }

    // ─────────────────────────────────────────────────────────────
    // Internal helpers
    // ─────────────────────────────────────────────────────────────

    /// <summary>
    /// Applies the requested <c>ServerAssignor</c> if the broker recognises it. Unknown
    /// names are ignored so a stale or hostile client cannot stall the coordinator.
    /// Returns true if the assignor changed and the group needs a recomputation.
    /// </summary>
    /// <summary>
    /// KIP-955: seed group metadata on the very first heartbeat, and apply
    /// monotonically-tightening updates on subsequent heartbeats (rebalance timeout
    /// can only shrink). Idempotent — safe to call from every heartbeat.
    /// </summary>
    private static void ApplyInitMetadata(ConsumerGroupV2State group, ConsumerGroupHeartbeatRequest request)
    {
        if (!group.Initialized)
        {
            group.Initialized = true;
            if (request.RebalanceTimeoutMs > 0)
            {
                group.RebalanceTimeoutMs = request.RebalanceTimeoutMs;
            }
            return;
        }

        if (request.RebalanceTimeoutMs > 0
            && (group.RebalanceTimeoutMs <= 0 || request.RebalanceTimeoutMs < group.RebalanceTimeoutMs))
        {
            group.RebalanceTimeoutMs = request.RebalanceTimeoutMs;
        }
    }

    /// <summary>
    /// KAFKA-20434: Aktualisiert <see cref="ConsumerGroupV2State.AssignorName"/>, wenn der vom
    /// Heartbeat gemeldete <paramref name="serverAssignor"/> vom aktuellen Group-Wert abweicht,
    /// und bumpt <c>GroupEpoch</c> entsprechend. In Java/Kafka ist der PreferredServerAssignor
    /// per-Member; Surgewave fuehrt nur einen Group-Level-Wert. Das ist strikter — jeder
    /// Member-Heartbeat mit abweichendem Assignor triggert einen Recompute, also haben wir das
    /// Java-Problem "static members rejoin with different assignor recomputed nicht" by-design
    /// nicht.
    /// </summary>
    private static bool TryUpdateAssignor(ConsumerGroupV2State group, string? serverAssignor)
    {
        if (string.IsNullOrEmpty(serverAssignor)) return false;

        // PartitionAssignorFactory.GetAssignor falls back to "range" for unknown names;
        // re-check the canonical name so we only treat real changes as a recompute.
        var resolved = PartitionAssignorFactory.GetAssignor(serverAssignor).Name;
        if (string.Equals(resolved, group.AssignorName, StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        group.AssignorName = resolved;
        group.GroupEpoch++;
        return true;
    }

    /// <summary>
    /// Validates that a (groupId, memberId, memberEpoch) tuple from a non-heartbeat
    /// request (typically OffsetCommit / OffsetFetch v9+ in the KIP-848 path) refers
    /// to a current member of a known v2 group. Returns:
    /// <list type="bullet">
    ///   <item><see cref="ErrorCode.None"/> — group + member known and epoch matches.</item>
    ///   <item><see cref="ErrorCode.StaleMemberEpoch"/> — group + member known but the supplied epoch is older than the current one.</item>
    ///   <item><see cref="ErrorCode.UnknownMemberId"/> — group exists but the memberId is unknown (or the supplied epoch is from the future, which we treat the same way).</item>
    ///   <item><see cref="ErrorCode.GroupIdNotFound"/> — sentinel-coded as <c>UnknownTopicOrPartition</c>: group itself isn't tracked here, callers may fall back to the classic coordinator. We use a dedicated value so callers can distinguish "fall through" from a real error.</item>
    /// </list>
    /// </summary>
    /// <summary>
    /// Heartbeat-path epoch fence. Returns <c>true</c> with a populated
    /// <paramref name="fenceResponse"/> when the request must be rejected;
    /// <c>false</c> when the heartbeat is structurally valid and the caller
    /// should continue. The semantics mirror KIP-848:
    /// <list type="bullet">
    ///   <item><c>MemberEpoch == 0</c> → join / rejoin path; allowed regardless of memberId.</item>
    ///   <item><c>MemberEpoch &gt; 0</c> + unknown memberId → <see cref="ErrorCode.UnknownMemberId"/>.</item>
    ///   <item><c>MemberEpoch &gt; 0</c> + known memberId + epoch &lt; stored → <see cref="ErrorCode.StaleMemberEpoch"/> (the client is behind).</item>
    ///   <item><c>MemberEpoch &gt; 0</c> + known memberId + epoch &gt; stored → <see cref="ErrorCode.FencedMemberEpoch"/> (impossible — broker never issued this).</item>
    /// </list>
    /// Without this fence a stale or replayed heartbeat silently inherits the
    /// current group epoch on line ~142 and walks back into reconciliation
    /// with state the broker never authorised.
    /// </summary>
    private bool TryValidateHeartbeatEpoch(
        ConsumerGroupHeartbeatRequest request,
        ConsumerGroupV2State group,
        out ConsumerGroupHeartbeatResponse fenceResponse)
    {
        fenceResponse = null!;

        // Join / rejoin paths run with epoch 0 — any memberId behaviour is
        // handled below the fence.
        if (request.MemberEpoch == 0) return false;

        var memberId = request.MemberId;
        if (string.IsNullOrEmpty(memberId) || !group.Members.TryGetValue(memberId, out var member))
        {
            fenceResponse = HeartbeatErrorResponse(request, ErrorCode.UnknownMemberId);
            return true;
        }

        if (request.MemberEpoch < member.MemberEpoch)
        {
            fenceResponse = HeartbeatErrorResponse(request, ErrorCode.StaleMemberEpoch);
            return true;
        }

        if (request.MemberEpoch > member.MemberEpoch)
        {
            fenceResponse = HeartbeatErrorResponse(request, ErrorCode.FencedMemberEpoch);
            return true;
        }

        return false;
    }

    private static ConsumerGroupHeartbeatResponse HeartbeatErrorResponse(
        ConsumerGroupHeartbeatRequest request,
        ErrorCode errorCode) => new()
        {
            CorrelationId = request.CorrelationId,
            ApiVersion = request.ApiVersion,
            ErrorCode = errorCode,
            MemberId = request.MemberId,
            MemberEpoch = request.MemberEpoch,
            HeartbeatIntervalMs = DefaultHeartbeatIntervalMs,
        };

    public ErrorCode ValidateMemberForOffsetOperation(string groupId, string? memberId, int memberEpoch)
    {
        lock (_groupLock)
        {
            if (!_groups.TryGetValue(groupId, out var group))
            {
                // Sentinel: caller should treat this as "not a v2 group" rather than a hard error.
                return ErrorCode.UnknownTopicOrPartition;
            }

            if (string.IsNullOrEmpty(memberId) || !group.Members.TryGetValue(memberId, out var member))
            {
                return ErrorCode.UnknownMemberId;
            }

            if (memberEpoch < member.MemberEpoch)
            {
                return ErrorCode.StaleMemberEpoch;
            }

            if (memberEpoch > member.MemberEpoch)
            {
                // Future epoch — the member is ahead of what we recorded. Treat as fenced
                // so the client re-authenticates against the current state.
                return ErrorCode.FencedMemberEpoch;
            }

            return ErrorCode.None;
        }
    }

    /// <summary>
    /// Background-sweep entry point: runs <see cref="CleanStaleMembers"/> for every
    /// known group and recomputes target assignments where members were evicted. Safe
    /// to call from a hosted timer; intended to be invoked roughly once per
    /// <see cref="StaleHeartbeatTimeout"/>.
    /// </summary>
    public void SweepStaleMembers()
    {
        lock (_groupLock)
        {
            foreach (var group in _groups.Values)
            {
                bool needsRecompute = false;
                CleanStaleMembers(group, ref needsRecompute);
                if (needsRecompute)
                {
                    _assignmentComputer.Compute(group);
                    if (group.Members.Count == 0)
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

    private void CleanStaleMembers(ConsumerGroupV2State group, ref bool needsRecompute)
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

        if (staleMembers == null) return;

        foreach (var staleMember in staleMembers)
        {
            group.Members.Remove(staleMember);
            _logger.LogInformation("ConsumerGroupV2 {GroupId}: removed stale member {MemberId}",
                group.GroupId, staleMember);
        }

        group.GroupEpoch++;
        needsRecompute = true;
    }

    private static bool AreSubscriptionsEqual(List<string> current, List<string> incoming)
    {
        if (current.Count != incoming.Count) return false;
        for (int i = 0; i < current.Count; i++)
        {
            if (current[i] != incoming[i]) return false;
        }
        return true;
    }

    private static List<TopicPartitionAssignment> ConvertOwnedFromRequest(
        List<ConsumerGroupHeartbeatRequest.Assignor> ownedFromRequest)
    {
        var result = new List<TopicPartitionAssignment>(ownedFromRequest.Count);
        foreach (var item in ownedFromRequest)
        {
            result.Add(new TopicPartitionAssignment
            {
                TopicId = item.TopicId,
                Partitions = [.. item.Partitions],
            });
        }
        return result;
    }

    private static ConsumerGroupHeartbeatResponse.Assignment? BuildHeartbeatAssignment(
        List<TopicPartitionAssignment> assignment)
    {
        if (assignment.Count == 0) return null;

        var topicPartitions = new List<ConsumerGroupHeartbeatResponse.TopicPartitions>(assignment.Count);
        foreach (var a in assignment)
        {
            topicPartitions.Add(new ConsumerGroupHeartbeatResponse.TopicPartitions
            {
                TopicId = a.TopicId,
                Partitions = [.. a.Partitions]
            });
        }

        return new ConsumerGroupHeartbeatResponse.Assignment { TopicPartitions = topicPartitions };
    }

    private static ConsumerGroupDescribeResponse.Assignment ConvertToDescribeAssignment(
        List<TopicPartitionAssignment> assignments)
    {
        var topicPartitions = new List<ConsumerGroupDescribeResponse.TopicPartitions>(assignments.Count);
        foreach (var a in assignments)
        {
            topicPartitions.Add(new ConsumerGroupDescribeResponse.TopicPartitions
            {
                TopicId = a.TopicId,
                Partitions = [.. a.Partitions]
            });
        }

        return new ConsumerGroupDescribeResponse.Assignment { TopicPartitions = topicPartitions };
    }

    /// <summary>
    /// KAFKA-20431: Baut die <c>MemberAssignment</c>-Liste fuer
    /// <c>ConsumerGroupDescribeResponse</c> aus der Vereinigung von <see
    /// cref="ConsumerGroupV2Member.Assignment"/> (was der Member kommuniziert bekommt) und
    /// <see cref="ConsumerGroupV2Member.OwnedTopicPartitions"/> (was er aktuell noch besitzt).
    /// Letzteres entspricht semantisch dem Java-Konzept <c>partitionsPendingRevocation</c> —
    /// Partitions, die der Member noch konsumiert, bis die Revocation abgeschlossen ist. Ohne
    /// dieses Merge wuerden sie waehrend der Reconciliation aus der Describe-Response verschwinden.
    /// </summary>
    private static ConsumerGroupDescribeResponse.Assignment ConvertToDescribeAssignmentWithPendingRevocation(
        ConsumerGroupV2Member member)
    {
        var perTopic = new Dictionary<Guid, SortedSet<int>>();

        void Accumulate(List<TopicPartitionAssignment> source)
        {
            foreach (var topicAssignment in source)
            {
                if (!perTopic.TryGetValue(topicAssignment.TopicId, out var partitions))
                {
                    partitions = [];
                    perTopic[topicAssignment.TopicId] = partitions;
                }
                foreach (var p in topicAssignment.Partitions)
                {
                    partitions.Add(p);
                }
            }
        }

        Accumulate(member.Assignment);
        Accumulate(member.OwnedTopicPartitions);

        var topicPartitions = new List<ConsumerGroupDescribeResponse.TopicPartitions>(perTopic.Count);
        foreach (var (topicId, partitions) in perTopic)
        {
            topicPartitions.Add(new ConsumerGroupDescribeResponse.TopicPartitions
            {
                TopicId = topicId,
                Partitions = [.. partitions]
            });
        }

        return new ConsumerGroupDescribeResponse.Assignment { TopicPartitions = topicPartitions };
    }

    private static string GetGroupState(ConsumerGroupV2State group)
    {
        if (group.Members.Count == 0) return "Empty";
        return ConsumerGroupV2Reconciler.IsStable(group) ? "Stable" : "Reconciling";
    }
}
