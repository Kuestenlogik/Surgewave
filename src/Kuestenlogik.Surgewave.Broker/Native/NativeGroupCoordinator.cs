using Kuestenlogik.Surgewave.Broker.Coordination;
using Kuestenlogik.Surgewave.Core;
using Kuestenlogik.Surgewave.Broker.Native.Coordination;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Broker.Native;

/// <summary>
/// Consumer group coordinator for Surgewave Native protocol.
/// Manages consumer group lifecycle, membership, and partition assignments.
/// </summary>
public sealed class NativeGroupCoordinator
{
    private readonly ILogger<NativeGroupCoordinator> _logger;
    // The offsets every group commits, in __consumer_offsets (#205); in memory without a store.
    private readonly OffsetStore _offsetStore;
    private readonly SubscriptionManager _subscriptionManager = new();
    private readonly Dictionary<string, NativeConsumerGroup> _groups = new();
    private readonly Lock _lock = new();

    public NativeGroupCoordinator(ILogger<NativeGroupCoordinator> logger, OffsetStore? offsetStore = null)
    {
        _logger = logger;
        _offsetStore = offsetStore ?? OffsetStore.InMemory();
    }

    /// <summary>
    /// Gets the subscription manager for advanced subscription type dispatch.
    /// </summary>
    public SubscriptionManager SubscriptionManager => _subscriptionManager;

    public JoinGroupResult JoinGroup(
        string groupId,
        string? memberId,
        string? groupInstanceId,
        string clientId,
        string protocolType,
        int sessionTimeoutMs,
        int rebalanceTimeoutMs,
        List<GroupProtocol> protocols)
    {
        lock (_lock)
        {
            _logger.LogDebug("JoinGroup request: groupId={GroupId}, memberId={MemberId}, clientId={ClientId}",
                groupId, memberId, clientId);

            if (!_groups.TryGetValue(groupId, out var group))
            {
                group = new NativeConsumerGroup
                {
                    GroupId = groupId,
                    GenerationId = 1,
                    ProtocolType = protocolType,
                    ProtocolName = protocols.FirstOrDefault()?.Name ?? "range",
                    State = GroupState.Empty
                };
                _groups[groupId] = group;
                _logger.LogInformation("Created new consumer group: {GroupId}", groupId);
            }

            // Clean up stale members
            var sessionTimeout = TimeSpan.FromMilliseconds(sessionTimeoutMs > 0 ? sessionTimeoutMs : 10000);
            var now = DateTime.UtcNow;
            var staleMembers = group.Members
                .Where(kvp => now - kvp.Value.LastHeartbeat > sessionTimeout)
                .Select(kvp => kvp.Key)
                .ToList();

            foreach (var staleMember in staleMembers)
            {
                group.Members.Remove(staleMember);
                _logger.LogDebug("Removed stale member {MemberId} from group {GroupId}", staleMember, groupId);
            }

            if (staleMembers.Count > 0)
            {
                group.GenerationId++;
                group.State = group.Members.Count > 0 ? GroupState.PreparingRebalance : GroupState.Empty;
            }

            // Generate member ID if empty
            var actualMemberId = string.IsNullOrEmpty(memberId)
                ? $"{clientId}-{Guid.NewGuid()}"
                : memberId;

            // Add or update member
            if (!group.Members.TryGetValue(actualMemberId, out var member))
            {
                member = new NativeGroupMember
                {
                    MemberId = actualMemberId,
                    GroupInstanceId = groupInstanceId,
                    ClientId = clientId,
                    Metadata = protocols.FirstOrDefault()?.Metadata ?? Array.Empty<byte>()
                };
                group.Members[actualMemberId] = member;
                _logger.LogDebug("Added member {MemberId} to group {GroupId}", actualMemberId, groupId);
            }
            else
            {
                member.Metadata = protocols.FirstOrDefault()?.Metadata ?? Array.Empty<byte>();
            }

            member.LastHeartbeat = DateTime.UtcNow;

            // Update group state
            group.State = GroupState.PreparingRebalance;

            // First member becomes the leader
            var leaderId = group.Members.Keys.First();
            var isLeader = actualMemberId == leaderId;

            // Prepare member list (only for leader)
            var members = isLeader
                ? group.Members.Values.Select(m => new JoinGroupMemberInfo
                {
                    MemberId = m.MemberId,
                    GroupInstanceId = m.GroupInstanceId,
                    Metadata = m.Metadata
                }).ToList()
                : new List<JoinGroupMemberInfo>();

            return new JoinGroupResult
            {
                ErrorCode = 0,
                GenerationId = group.GenerationId,
                ProtocolName = group.ProtocolName,
                LeaderId = leaderId,
                MemberId = actualMemberId,
                Members = members
            };
        }
    }

    /// <summary>
    /// JoinGroup with subscription type support. For non-Standard types, the subscription
    /// manager handles consumer admission (e.g., rejecting second consumer for Exclusive).
    /// </summary>
    public JoinGroupResult JoinGroup(
        string groupId,
        string? memberId,
        string? groupInstanceId,
        string clientId,
        string protocolType,
        int sessionTimeoutMs,
        int rebalanceTimeoutMs,
        List<GroupProtocol> protocols,
        SubscriptionType subscriptionType)
    {
        if (subscriptionType == SubscriptionType.Standard)
        {
            return JoinGroup(groupId, memberId, groupInstanceId, clientId, protocolType,
                sessionTimeoutMs, rebalanceTimeoutMs, protocols);
        }

        // Generate member ID early so we can use it for subscription check
        var actualMemberId = string.IsNullOrEmpty(memberId)
            ? $"{clientId}-{Guid.NewGuid()}"
            : memberId;

        // Check subscription admission before modifying group state
        var subResult = _subscriptionManager.Subscribe(groupId, subscriptionType, actualMemberId);
        if (subResult.ErrorCode != 0)
        {
            _logger.LogDebug(
                "Subscription rejected for {MemberId} on group {GroupId}: {Error}",
                actualMemberId, groupId, subResult.ErrorMessage);

            return new JoinGroupResult
            {
                ErrorCode = (ushort)subResult.ErrorCode,
                MemberId = actualMemberId,
                Members = []
            };
        }

        // Proceed with normal group join using the pre-generated member ID
        return JoinGroup(groupId, actualMemberId, groupInstanceId, clientId, protocolType,
            sessionTimeoutMs, rebalanceTimeoutMs, protocols);
    }

    /// <summary>
    /// Handle consumer disconnect by notifying the subscription manager for failover processing.
    /// </summary>
    public void HandleConsumerDisconnect(string memberId)
    {
        _subscriptionManager.HandleConsumerFailure(memberId);
        _logger.LogDebug("Handled consumer disconnect for subscription failover: {MemberId}", memberId);
    }

    public SyncGroupResult SyncGroup(
        string groupId,
        string memberId,
        int generationId,
        List<MemberAssignment> assignments)
    {
        lock (_lock)
        {
            _logger.LogDebug("SyncGroup request: groupId={GroupId}, memberId={MemberId}, generationId={GenerationId}",
                groupId, memberId, generationId);

            if (!_groups.TryGetValue(groupId, out var group))
            {
                return new SyncGroupResult { ErrorCode = 10 }; // GroupNotFound
            }

            if (!group.Members.ContainsKey(memberId))
            {
                return new SyncGroupResult { ErrorCode = 15 }; // UnknownMemberId
            }

            // Store assignments if this is the leader
            if (assignments.Count > 0)
            {
                foreach (var assignment in assignments)
                {
                    if (group.Members.TryGetValue(assignment.MemberId, out var member))
                    {
                        member.Assignment = assignment.Assignment;
                    }
                }
                group.State = GroupState.Stable;
            }

            // Return the assignment for this member
            var memberAssignment = group.Members.TryGetValue(memberId, out var m)
                ? m.Assignment
                : Array.Empty<byte>();

            return new SyncGroupResult
            {
                ErrorCode = 0,
                Assignment = memberAssignment
            };
        }
    }

    public HeartbeatResult Heartbeat(string groupId, string memberId, int generationId)
    {
        lock (_lock)
        {
            if (!_groups.TryGetValue(groupId, out var group))
            {
                return new HeartbeatResult { ErrorCode = 10 }; // GroupNotFound
            }

            if (!group.Members.TryGetValue(memberId, out var member))
            {
                return new HeartbeatResult { ErrorCode = 15 }; // UnknownMemberId
            }

            if (generationId != group.GenerationId)
            {
                return new HeartbeatResult { ErrorCode = 16 }; // IllegalGeneration
            }

            // Tell members whose generation is still current that the group is
            // rebalancing, so they rejoin. Without this the signal never reaches them:
            // JoinGroup of a new member sets PreparingRebalance but does NOT bump the
            // generation, so every existing member would keep heartbeating happily and
            // never learn that the assignment changed (#116). Matches Kafka, where a
            // member also heartbeats REBALANCE_IN_PROGRESS until SyncGroup completes.
            if (group.State == GroupState.PreparingRebalance)
            {
                member.LastHeartbeat = DateTime.UtcNow;
                return new HeartbeatResult { ErrorCode = 11 }; // RebalanceInProgress
            }

            member.LastHeartbeat = DateTime.UtcNow;
            return new HeartbeatResult { ErrorCode = 0 };
        }
    }

    public LeaveGroupResult LeaveGroup(string groupId, string memberId)
    {
        lock (_lock)
        {
            _logger.LogDebug("LeaveGroup request: groupId={GroupId}, memberId={MemberId}", groupId, memberId);

            if (!_groups.TryGetValue(groupId, out var group))
            {
                return new LeaveGroupResult { ErrorCode = 10 }; // GroupNotFound
            }

            var removed = group.Members.Remove(memberId);
            if (removed)
            {
                group.GenerationId++;
                group.State = group.Members.Count > 0 ? GroupState.PreparingRebalance : GroupState.Empty;
                _logger.LogDebug("Member {MemberId} left group {GroupId}", memberId, groupId);
            }

            return new LeaveGroupResult { ErrorCode = 0 };
        }
    }

    public List<GroupInfo> ListGroups()
    {
        lock (_lock)
        {
            return _groups.Values.Select(g => new GroupInfo
            {
                GroupId = g.GroupId,
                ProtocolType = g.ProtocolType,
                State = GetGroupStateString(g.State)
            }).ToList();
        }
    }

    public DescribeGroupResult DescribeGroup(string groupId)
    {
        lock (_lock)
        {
            if (!_groups.TryGetValue(groupId, out var group))
            {
                return new DescribeGroupResult { ErrorCode = 10 }; // GroupNotFound
            }

            return new DescribeGroupResult
            {
                ErrorCode = 0,
                GroupId = group.GroupId,
                State = GetGroupStateString(group.State),
                ProtocolType = group.ProtocolType,
                ProtocolName = group.ProtocolName,
                GenerationId = group.GenerationId,
                Members = group.Members.Values.Select(m => new GroupMemberInfo
                {
                    MemberId = m.MemberId,
                    GroupInstanceId = m.GroupInstanceId,
                    ClientId = m.ClientId,
                    Metadata = m.Metadata,
                    Assignment = m.Assignment
                }).ToList()
            };
        }
    }

    public DeleteGroupResult DeleteGroup(string groupId)
    {
        lock (_lock)
        {
            _logger.LogDebug("DeleteGroup request: groupId={GroupId}", groupId);

            if (!_groups.TryGetValue(groupId, out var group))
            {
                return new DeleteGroupResult { ErrorCode = 10 }; // GroupNotFound
            }

            if (group.Members.Count > 0)
            {
                return new DeleteGroupResult { ErrorCode = 18 }; // GroupNotEmpty
            }

            _groups.Remove(groupId);
            _logger.LogInformation("Deleted consumer group: {GroupId}", groupId);

            return new DeleteGroupResult { ErrorCode = 0 };
        }
    }

    private Kuestenlogik.Surgewave.Coordination.Coordinators.ICoordinatorLocator? _coordinators;

    /// <summary>
    /// Sets who coordinates which group (#204) — the same answer the Kafka FindCoordinator gives, so a native and a
    /// Kafka client of one group meet at one coordinator.
    /// </summary>
    public void SetCoordinatorLocator(Kuestenlogik.Surgewave.Coordination.Coordinators.ICoordinatorLocator coordinators)
        => _coordinators = coordinators;

    public FindCoordinatorResult FindCoordinator(string key, byte keyType)
    {
        // It answered broker 0 at localhost:9092 whatever the broker's id, host and port were. Without a locator
        // there is no answer to give: COORDINATOR_NOT_AVAILABLE, and the client asks again.
        var location = _coordinators?.Locate(
            keyType == 1
                ? Kuestenlogik.Surgewave.Coordination.Coordinators.CoordinatorKind.Transaction
                : Kuestenlogik.Surgewave.Coordination.Coordinators.CoordinatorKind.Group,
            key);
        if (location is not { Status: Kuestenlogik.Surgewave.Coordination.Coordinators.CoordinatorLookupStatus.Ok } found)
            return new FindCoordinatorResult { ErrorCode = (ushort)Kuestenlogik.Surgewave.Protocol.Native.SurgewaveErrorCode.CoordinatorNotAvailable, CoordinatorId = -1 };

        return new FindCoordinatorResult
        {
            ErrorCode = 0,
            CoordinatorId = found.BrokerId,
            Host = found.Host,
            Port = found.Port,
        };
    }

    /// <summary>Commits the offset; answered once <c>__consumer_offsets</c> has it durably (#205).</summary>
    public async Task<CommitOffsetResult> CommitOffsetAsync(
        string groupId,
        string memberId,
        int generationId,
        string topic,
        int partition,
        long offset,
        string? metadata,
        CancellationToken cancellationToken)
    {
        lock (_lock)
        {
            // A commit for a group nobody joined creates it, as Kafka does for a group without members.
            if (!_groups.ContainsKey(groupId))
            {
                _groups[groupId] = new NativeConsumerGroup
                {
                    GroupId = groupId,
                    GenerationId = 0,
                    ProtocolType = "consumer",
                    ProtocolName = "",
                    State = GroupState.Empty
                };
            }
        }

        var written = await _offsetStore.CommitOffsetAsync(groupId, topic, partition, offset, cancellationToken).ConfigureAwait(false);
        return new CommitOffsetResult { ErrorCode = ToNativeErrorCode(written) };
    }

    // The native protocol has no load-in-progress code; a coordinator still loading is not available yet, and retried.
    private static ushort ToNativeErrorCode(StateWriteStatus status) => status switch
    {
        StateWriteStatus.Ok => 0,
        StateWriteStatus.NotCoordinator => 20, // NotCoordinator
        _ => 21, // CoordinatorNotAvailable
    };

    public FetchOffsetResult FetchOffset(string groupId, string topic, int partition)
    {
        return _offsetStore.OwnershipOf(groupId) switch
        {
            CoordinatorOwnership.Owned => new FetchOffsetResult { ErrorCode = 0, Offset = _offsetStore.GetCommittedOffset(groupId, topic, partition) },
            CoordinatorOwnership.Loading => new FetchOffsetResult { ErrorCode = 21, Offset = -1 }, // CoordinatorNotAvailable
            _ => new FetchOffsetResult { ErrorCode = 20, Offset = -1 }, // NotCoordinator
        };
    }

    private static string GetGroupStateString(GroupState state) => state switch
    {
        GroupState.Empty => "Empty",
        GroupState.PreparingRebalance => "PreparingRebalance",
        GroupState.CompletingRebalance => "CompletingRebalance",
        GroupState.Stable => "Stable",
        GroupState.Dead => "Dead",
        _ => "Unknown"
    };
}

internal sealed class NativeConsumerGroup
{
    public required string GroupId { get; init; }
    public required int GenerationId { get; set; }
    public required string ProtocolType { get; init; }
    public required string ProtocolName { get; set; }
    public required GroupState State { get; set; }
    public Dictionary<string, NativeGroupMember> Members { get; } = new();
}

internal sealed class NativeGroupMember
{
    public required string MemberId { get; init; }
    public string? GroupInstanceId { get; init; }
    public required string ClientId { get; init; }
    public required byte[] Metadata { get; set; }
    public byte[] Assignment { get; set; } = Array.Empty<byte>();
    public DateTime LastHeartbeat { get; set; } = DateTime.UtcNow;
}
