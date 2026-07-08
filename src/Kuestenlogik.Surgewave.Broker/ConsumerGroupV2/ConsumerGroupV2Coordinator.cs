using Kuestenlogik.Surgewave.Broker.GroupStatePersistence;
using Kuestenlogik.Surgewave.Broker.Native.Assignors;
using Kuestenlogik.Surgewave.Coordination.Consumer;
using Kuestenlogik.Surgewave.Core.Storage;
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
public sealed class ConsumerGroupV2Coordinator : IConsumerGroupV2Coordinator
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

    public ConsumerHeartbeatResult Heartbeat(ConsumerHeartbeatCommand command)
    {
        lock (_groupLock)
        {
            _logger.LogDebug("ConsumerGroupV2Heartbeat: GroupId={GroupId}, MemberId={MemberId}, MemberEpoch={MemberEpoch}",
                command.GroupId, command.MemberId, command.MemberEpoch);

            // MemberEpoch -1 means leave
            if (command.MemberEpoch == -1)
            {
                return HandleLeave(command);
            }

            if (!_groups.TryGetValue(command.GroupId, out var group))
            {
                group = new ConsumerGroupV2State { GroupId = command.GroupId };
                _groups[command.GroupId] = group;
                _logger.LogInformation("ConsumerGroupV2 created: {GroupId}", command.GroupId);
            }

            // KIP-848 heartbeat-epoch fencing: a heartbeat carrying a non-zero
            // MemberEpoch must match the broker's view exactly. A behind-by-one
            // member gets StaleEpoch and re-fetches; a member claiming an epoch the
            // broker never assigned is impossible state and gets FencedEpoch; a
            // non-zero epoch with an unknown memberId is UnknownMember. The check runs
            // before any mutation so a malformed client can never silently advance state.
            if (TryValidateHeartbeatEpoch(command, group, out var fenceResult))
            {
                return fenceResult;
            }

            // KIP-955: the founding member's first heartbeat seeds group-level metadata
            // (assignor preference and rebalance timeout). Later heartbeats can adjust
            // the rebalance timeout downward but cannot push it back up — a slow member
            // must not relax the group's tolerance once others are coordinating against
            // the original value.
            ApplyInitMetadata(group, command);

            bool targetNeedsRecompute = false;

            if (TryUpdateAssignor(group, command.ServerAssignor))
            {
                targetNeedsRecompute = true;
            }

            CleanStaleMembers(group, ref targetNeedsRecompute);

            var memberId = command.MemberId;
            if (command.MemberEpoch == 0 && string.IsNullOrEmpty(memberId))
            {
                memberId = $"{command.ClientId}-{Guid.NewGuid()}";
            }

            if (!group.Members.TryGetValue(memberId, out var member))
            {
                member = new ConsumerGroupV2Member
                {
                    MemberId = memberId,
                    InstanceId = command.InstanceId,
                    RackId = command.RackId,
                    ClientId = command.ClientId,
                    ClientHost = "*"
                };
                group.Members[memberId] = member;
                group.GroupEpoch++;
                targetNeedsRecompute = true;
                _logger.LogInformation("ConsumerGroupV2 {GroupId}: member {MemberId} joined (epoch={Epoch})",
                    command.GroupId, memberId, group.GroupEpoch);
            }

            member.LastHeartbeat = DateTime.UtcNow;
            if (command.InstanceId != null) member.InstanceId = command.InstanceId;
            if (command.RackId != null) member.RackId = command.RackId;

            if (command.SubscribedTopicNames != null
                && !AreSubscriptionsEqual(member.SubscribedTopicNames, command.SubscribedTopicNames))
            {
                member.SubscribedTopicNames = [.. command.SubscribedTopicNames];
                group.GroupEpoch++;
                targetNeedsRecompute = true;
            }

            // Update what the member reports owning. KIP-848: this is what the
            // reconciler uses to decide when a partition is safe to hand to its new
            // owner. Treat null as "unchanged since last heartbeat".
            if (command.OwnedTopicPartitions != null)
            {
                member.OwnedTopicPartitions = ToInternalAssignment(command.OwnedTopicPartitions);
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

            return new ConsumerHeartbeatResult
            {
                Status = ConsumerGroupFenceStatus.Ok,
                MemberId = memberId,
                MemberEpoch = member.MemberEpoch,
                HeartbeatIntervalMs = DefaultHeartbeatIntervalMs,
                Assignment = ToNeutralAssignment(safeAssignment),
            };
        }
    }

    private ConsumerHeartbeatResult HandleLeave(ConsumerHeartbeatCommand command)
    {
        if (_groups.TryGetValue(command.GroupId, out var group))
        {
            if (group.Members.Remove(command.MemberId))
            {
                group.GroupEpoch++;
                _assignmentComputer.Compute(group);
                _logger.LogInformation("ConsumerGroupV2 {GroupId}: member {MemberId} left (epoch={Epoch})",
                    command.GroupId, command.MemberId, group.GroupEpoch);

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

        return new ConsumerHeartbeatResult
        {
            Status = ConsumerGroupFenceStatus.Ok,
            MemberId = command.MemberId,
            MemberEpoch = -1,
            HeartbeatIntervalMs = DefaultHeartbeatIntervalMs,
        };
    }

    // ─────────────────────────────────────────────────────────────
    // ConsumerGroupDescribe (API Key 69)
    // ─────────────────────────────────────────────────────────────

    public IReadOnlyList<ConsumerGroupDescription> Describe(IReadOnlyList<string> groupIds)
    {
        lock (_groupLock)
        {
            var result = new List<ConsumerGroupDescription>(groupIds.Count);

            foreach (var groupId in groupIds)
            {
                if (!_groups.TryGetValue(groupId, out var group))
                {
                    result.Add(new ConsumerGroupDescription
                    {
                        GroupId = groupId,
                        Status = ConsumerGroupDescribeStatus.GroupNotFound,
                    });
                    continue;
                }

                var members = new List<ConsumerGroupMemberDescription>(group.Members.Count);
                foreach (var m in group.Members.Values)
                {
                    members.Add(new ConsumerGroupMemberDescription
                    {
                        MemberId = m.MemberId,
                        InstanceId = m.InstanceId,
                        RackId = m.RackId,
                        MemberEpoch = m.MemberEpoch,
                        ClientId = m.ClientId,
                        ClientHost = m.ClientHost,
                        SubscribedTopicNames = [.. m.SubscribedTopicNames],
                        // KAFKA-20431: MemberAssignment muss assigned + pending-revocation enthalten,
                        // sonst "verschwinden" Partitions waehrend der Reconciliation aus Admin-Sicht.
                        // Pending-Revocation in Surgewave = OwnedTopicPartitions \ Assignment
                        // (was der Member noch besitzt, aber nicht mehr kommuniziert bekommen soll).
                        MemberAssignment = ToNeutralAssignmentWithPendingRevocation(m),
                        TargetAssignment = ToNeutralAssignment(m.TargetAssignment),
                    });
                }

                result.Add(new ConsumerGroupDescription
                {
                    GroupId = groupId,
                    Status = ConsumerGroupDescribeStatus.Ok,
                    Phase = GetGroupPhase(group),
                    GroupEpoch = group.GroupEpoch,
                    AssignmentEpoch = group.AssignmentEpoch,
                    AssignorName = group.AssignorName,
                    Members = members,
                });
            }

            return result;
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
    private static void ApplyInitMetadata(ConsumerGroupV2State group, ConsumerHeartbeatCommand command)
    {
        if (!group.Initialized)
        {
            group.Initialized = true;
            if (command.RebalanceTimeoutMs > 0)
            {
                group.RebalanceTimeoutMs = command.RebalanceTimeoutMs;
            }
            return;
        }

        if (command.RebalanceTimeoutMs > 0
            && (group.RebalanceTimeoutMs <= 0 || command.RebalanceTimeoutMs < group.RebalanceTimeoutMs))
        {
            group.RebalanceTimeoutMs = command.RebalanceTimeoutMs;
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
    /// Heartbeat-path epoch fence. Returns <c>true</c> with a populated
    /// <paramref name="fenceResult"/> when the heartbeat must be rejected; <c>false</c> when
    /// it is structurally valid and the caller should continue. Mirrors KIP-848:
    /// epoch 0 = join/rejoin (allowed); non-zero + unknown member = UnknownMember; behind =
    /// StaleEpoch; ahead of the broker = FencedEpoch. Runs before any mutation so a stale or
    /// replayed heartbeat can't silently inherit the current group epoch.
    /// </summary>
    private bool TryValidateHeartbeatEpoch(
        ConsumerHeartbeatCommand command,
        ConsumerGroupV2State group,
        out ConsumerHeartbeatResult fenceResult)
    {
        fenceResult = null!;

        // Join / rejoin paths run with epoch 0 — any memberId behaviour is
        // handled below the fence.
        if (command.MemberEpoch == 0) return false;

        var memberId = command.MemberId;
        if (string.IsNullOrEmpty(memberId) || !group.Members.TryGetValue(memberId, out var member))
        {
            fenceResult = FenceResult(command, ConsumerGroupFenceStatus.UnknownMember);
            return true;
        }

        if (command.MemberEpoch < member.MemberEpoch)
        {
            fenceResult = FenceResult(command, ConsumerGroupFenceStatus.StaleEpoch);
            return true;
        }

        if (command.MemberEpoch > member.MemberEpoch)
        {
            fenceResult = FenceResult(command, ConsumerGroupFenceStatus.FencedEpoch);
            return true;
        }

        return false;
    }

    private static ConsumerHeartbeatResult FenceResult(
        ConsumerHeartbeatCommand command,
        ConsumerGroupFenceStatus status) => new()
        {
            Status = status,
            MemberId = command.MemberId,
            MemberEpoch = command.MemberEpoch,
            HeartbeatIntervalMs = DefaultHeartbeatIntervalMs,
        };

    /// <summary>
    /// Validates a (groupId, memberId, memberEpoch) tuple from a non-heartbeat request
    /// (KIP-848 OffsetCommit / OffsetFetch path). Returns <see cref="ConsumerGroupFenceStatus.NotAV2Group"/>
    /// (distinct sentinel) when the group isn't a KIP-848 group so the caller can fall through
    /// to the classic coordinator; <see cref="ConsumerGroupFenceStatus.UnknownMember"/> when the
    /// group exists but the member doesn't; <see cref="ConsumerGroupFenceStatus.FencedEpoch"/> for a
    /// future epoch; otherwise <see cref="ConsumerGroupFenceStatus.Ok"/> (KIP-1251: older epochs pass
    /// here — the per-partition check fences).
    /// </summary>
    public ConsumerGroupFenceStatus ValidateMemberForOffsetOperation(string groupId, string? memberId, int memberEpoch)
    {
        lock (_groupLock)
        {
            if (!_groups.TryGetValue(groupId, out var group))
            {
                return ConsumerGroupFenceStatus.NotAV2Group;
            }

            if (string.IsNullOrEmpty(memberId) || !group.Members.TryGetValue(memberId, out var member))
            {
                return ConsumerGroupFenceStatus.UnknownMember;
            }

            if (memberEpoch > member.MemberEpoch)
            {
                // Future epoch — the member is ahead of what we recorded. Treat as fenced.
                return ConsumerGroupFenceStatus.FencedEpoch;
            }

            // KIP-1251 — older epochs are accepted here; IsPartitionAssignmentValid does the
            // fine-grained per-partition fence.
            return ConsumerGroupFenceStatus.Ok;
        }
    }

    /// <summary>
    /// KIP-1251 — per-partition fence-check for offset commits. Returns
    /// <c>true</c> if the member's claimed memberEpoch is at-least the
    /// per-partition assignment epoch (i.e., the partition was assigned
    /// to this member at an epoch the member knows about and hasn't
    /// been re-assigned since). Returns <c>false</c> if the member
    /// doesn't own this partition at all, or if a newer assignment epoch
    /// has bumped past the member's claimed epoch.
    ///
    /// When the group has no per-partition assignment epochs populated
    /// (e.g. legacy persisted state from before KIP-1251 landed), the
    /// check falls back to group-level equality with the member's epoch.
    /// </summary>
    public bool IsPartitionAssignmentValid(string groupId, string? memberId, int memberEpoch, Guid topicId, int partition)
    {
        lock (_groupLock)
        {
            if (!_groups.TryGetValue(groupId, out var group)) return false;
            if (string.IsNullOrEmpty(memberId) || !group.Members.TryGetValue(memberId, out var member)) return false;

            // Walk the member's TARGET assignment — that's the broker's view
            // of "what this member owns at the current group epoch". The
            // committed Assignment may lag behind during reconciliation;
            // commits should be fenced against the target, not the
            // already-reconciled view.
            var topicAssignment = member.TargetAssignment.FirstOrDefault(t => t.TopicId == topicId);
            if (topicAssignment is null) return false;
            var partitionIndex = topicAssignment.Partitions.IndexOf(partition);
            if (partitionIndex < 0) return false;

            // KIP-1251 path: per-partition epoch from the assignor.
            if (topicAssignment.AssignmentEpochs is { Count: > 0 } epochs
                && partitionIndex < epochs.Count)
            {
                var partitionEpoch = epochs[partitionIndex];
                return memberEpoch >= partitionEpoch;
            }

            // Fallback for pre-KIP-1251 persisted state without per-partition epochs.
            return memberEpoch == member.MemberEpoch;
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

    private static bool AreSubscriptionsEqual(List<string> current, IReadOnlyList<string> incoming)
    {
        if (current.Count != incoming.Count) return false;
        for (int i = 0; i < current.Count; i++)
        {
            if (current[i] != incoming[i]) return false;
        }
        return true;
    }

    private static List<TopicPartitionAssignment> ToInternalAssignment(IReadOnlyList<ConsumerTopicPartitions> owned)
    {
        var result = new List<TopicPartitionAssignment>(owned.Count);
        foreach (var item in owned)
        {
            result.Add(new TopicPartitionAssignment
            {
                TopicId = item.TopicId,
                Partitions = [.. item.Partitions],
            });
        }
        return result;
    }

    /// <summary>Projects the internal assignment into neutral records (empty list when none).</summary>
    private static IReadOnlyList<ConsumerTopicPartitions> ToNeutralAssignment(List<TopicPartitionAssignment> assignment)
    {
        if (assignment.Count == 0) return [];

        var result = new List<ConsumerTopicPartitions>(assignment.Count);
        foreach (var a in assignment)
        {
            result.Add(new ConsumerTopicPartitions(a.TopicId, [.. a.Partitions]));
        }
        return result;
    }

    /// <summary>
    /// KAFKA-20431: the describe MemberAssignment is the union of <see cref="ConsumerGroupV2Member.Assignment"/>
    /// (what the member is told to own) and <see cref="ConsumerGroupV2Member.OwnedTopicPartitions"/> (what it
    /// still owns) — semantically Java's <c>partitionsPendingRevocation</c>. Without this merge those
    /// partitions would vanish from the describe view during reconciliation.
    /// </summary>
    private static IReadOnlyList<ConsumerTopicPartitions> ToNeutralAssignmentWithPendingRevocation(ConsumerGroupV2Member member)
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

        var result = new List<ConsumerTopicPartitions>(perTopic.Count);
        foreach (var (topicId, partitions) in perTopic)
        {
            result.Add(new ConsumerTopicPartitions(topicId, [.. partitions]));
        }
        return result;
    }

    private static ConsumerGroupPhase GetGroupPhase(ConsumerGroupV2State group)
    {
        if (group.Members.Count == 0) return ConsumerGroupPhase.Empty;
        return ConsumerGroupV2Reconciler.IsStable(group) ? ConsumerGroupPhase.Stable : ConsumerGroupPhase.Reconciling;
    }
}
