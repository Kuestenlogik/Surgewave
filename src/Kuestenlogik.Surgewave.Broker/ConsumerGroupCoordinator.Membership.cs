using Kuestenlogik.Surgewave.Coordination.Consumer;
using Kuestenlogik.Surgewave.Core.Observability;

namespace Kuestenlogik.Surgewave.Broker;

/// <summary>
/// Membership of the classic group protocol: the join barrier, the hand-off of the leader's assignment, and the
/// session and rebalance deadlines (#216).
/// </summary>
/// <remarks>
/// <para>
/// A rebalance starts when a member joins, leaves or misses its session timeout. The group then answers heartbeats
/// with <c>RebalanceInProgress</c>, and each JoinGroup is held until every known member has rejoined — or until the
/// longest rebalance timeout of the members passes, when those that did not rejoin are dropped. Completing the join
/// round starts a new generation and gives the leader every member's subscription.
/// </para>
/// <para>
/// A follower's SyncGroup is held until the leader's SyncGroup brings the assignment. Requests of an older
/// generation get <c>IllegalGeneration</c>. Every wait is a handshake: a completion source the next step completes,
/// or a timer for a deadline the protocol defines; nothing polls.
/// </para>
/// </remarks>
public sealed partial class ConsumerGroupCoordinator
{
    private readonly TimeProvider _time = timeProvider ?? TimeProvider.System;

    public async Task<JoinGroupResult> JoinGroupAsync(JoinGroupCommand request, CancellationToken cancellationToken)
    {
        // A coordinator still reading the group's partition has no group to join yet (#205).
        if (_offsets.OwnershipOf(request.GroupId) != GroupOwnership.Owned)
            return JoinGroupResult.Failed(ConsumerGroupErrorStatus.CoordinatorLoadInProgress, request.MemberId);

        TaskCompletionSource<JoinGroupResult> joined;
        string memberId;
        ConsumerGroupState group;

        lock (_groupLock)
        {
            var firstProtocol = request.Protocols.Count > 0 ? request.Protocols[0] : null;
            Log.JoinGroupRequest(logger, request.GroupId, request.MemberId,
                request.Protocols.Count, firstProtocol?.Metadata?.Length ?? 0);

            if (!_consumerGroups.TryGetValue(request.GroupId, out group!))
            {
                // A group taken over from another coordinator counts on from the generation it reached there.
                group = new ConsumerGroupState
                {
                    GroupId = request.GroupId,
                    GenerationId = _offsets.GetGeneration(request.GroupId),
                    ProtocolType = request.ProtocolType,
                    ProtocolName = firstProtocol?.Name ?? "range",
                };
                _consumerGroups[request.GroupId] = group;
                Log.JoinGroupCreated(logger, request.GroupId);
            }

            // A member id the group does not know is one it dropped: the client starts over with an empty id.
            if (!string.IsNullOrEmpty(request.MemberId) && !group.Members.ContainsKey(request.MemberId))
            {
                return JoinGroupResult.Failed(ConsumerGroupErrorStatus.UnknownMember, request.MemberId);
            }

            memberId = string.IsNullOrEmpty(request.MemberId) ? $"{request.ClientId}-{Guid.NewGuid()}" : request.MemberId;

            // A static member that rejoins under a new id replaces its old self.
            if (request.GroupInstanceId is not null)
            {
                foreach (var stale in group.Members.Values
                             .Where(m => m.GroupInstanceId == request.GroupInstanceId && m.MemberId != memberId).ToList())
                {
                    RemoveMember(group, stale.MemberId);
                }
            }

            var metadata = firstProtocol?.Metadata ?? [];
            var isNew = !group.Members.TryGetValue(memberId, out var member);
            if (isNew)
            {
                member = new GroupMember
                {
                    MemberId = memberId,
                    GroupInstanceId = request.GroupInstanceId,
                    ClientId = request.ClientId,
                    Metadata = metadata,
                };
                group.Members[memberId] = member;
                Log.JoinGroupMemberAdded(logger, memberId, metadata.Length);
            }
            else
            {
                member!.Metadata = metadata;
                Log.JoinGroupMemberUpdated(logger, memberId, metadata.Length);
            }

            member!.SessionTimeoutMs = request.SessionTimeoutMs > 0 ? request.SessionTimeoutMs : DefaultSessionTimeoutMs;
            member.RebalanceTimeoutMs = request.RebalanceTimeoutMs > 0 ? request.RebalanceTimeoutMs : member.SessionTimeoutMs;
            ArmSession(group, member);

            if (group.Phase != ClassicGroupPhase.PreparingRebalance)
                StartRebalance(group, isNew ? "a member joined" : "a member rejoined");

            // One join per member at a time; a repeated one replaces the earlier, which its connection gave up on.
            if (group.PendingJoins.Remove(memberId, out var earlier))
                earlier.TrySetCanceled(CancellationToken.None);
            joined = new TaskCompletionSource<JoinGroupResult>(TaskCreationOptions.RunContinuationsAsynchronously);
            group.PendingJoins[memberId] = joined;

            TryCompleteJoin(group);
        }

        try
        {
            return await joined.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // The connection went away; the member stays until its session runs out.
            lock (_groupLock)
            {
                if (group.PendingJoins.TryGetValue(memberId, out var pending) && ReferenceEquals(pending, joined))
                    group.PendingJoins.Remove(memberId);
            }
            throw;
        }
    }

    public async Task<SyncGroupResult> SyncGroupAsync(SyncGroupCommand request, CancellationToken cancellationToken)
    {
        if (_offsets.OwnershipOf(request.GroupId) != GroupOwnership.Owned)
            return new SyncGroupResult(ConsumerGroupErrorStatus.CoordinatorLoadInProgress, []);

        TaskCompletionSource<SyncGroupResult> synced;
        ConsumerGroupState? group;

        lock (_groupLock)
        {
            Log.SyncGroupRequest(logger, request.GroupId, request.MemberId, request.GenerationId);
            Log.SyncGroupAssignmentsCount(logger, request.Assignments.Count);

            if (!_consumerGroups.TryGetValue(request.GroupId, out group) || !group.Members.TryGetValue(request.MemberId, out var member))
            {
                Log.SyncGroupNotFound(logger, request.GroupId);
                return new SyncGroupResult(ConsumerGroupErrorStatus.UnknownMember, []);
            }

            if (request.GenerationId != group.GenerationId)
                return new SyncGroupResult(ConsumerGroupErrorStatus.IllegalGeneration, []);
            if (group.Phase == ClassicGroupPhase.PreparingRebalance)
                return new SyncGroupResult(ConsumerGroupErrorStatus.RebalanceInProgress, []);

            ArmSession(group, member);

            if (group.Phase == ClassicGroupPhase.Stable)
            {
                Log.SyncGroupReturningAssignment(logger, request.MemberId, member.Assignment.Length);
                return new SyncGroupResult(ConsumerGroupErrorStatus.None, member.Assignment);
            }

            // CompletingRebalance: the leader brings the assignment, everyone else waits for it.
            if (request.MemberId == group.LeaderId)
            {
                ApplyAssignment(group, request.Assignments);
                return new SyncGroupResult(ConsumerGroupErrorStatus.None, member.Assignment);
            }

            if (group.PendingSyncs.Remove(request.MemberId, out var earlier))
                earlier.TrySetCanceled(CancellationToken.None);
            synced = new TaskCompletionSource<SyncGroupResult>(TaskCreationOptions.RunContinuationsAsynchronously);
            group.PendingSyncs[request.MemberId] = synced;
        }

        try
        {
            return await synced.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            lock (_groupLock)
            {
                if (group.PendingSyncs.TryGetValue(request.MemberId, out var pending) && ReferenceEquals(pending, synced))
                    group.PendingSyncs.Remove(request.MemberId);
            }
            throw;
        }
    }

    public GroupHeartbeatResult Heartbeat(GroupHeartbeatCommand request)
    {
        if (_offsets.OwnershipOf(request.GroupId) != GroupOwnership.Owned)
            return new GroupHeartbeatResult(ConsumerGroupErrorStatus.CoordinatorLoadInProgress);

        lock (_groupLock)
        {
            if (!_consumerGroups.TryGetValue(request.GroupId, out var group) || !group.Members.TryGetValue(request.MemberId, out var member))
                return new GroupHeartbeatResult(ConsumerGroupErrorStatus.UnknownMember);

            if (request.GenerationId != group.GenerationId)
                return new GroupHeartbeatResult(ConsumerGroupErrorStatus.IllegalGeneration);

            ArmSession(group, member);

            return new GroupHeartbeatResult(group.Phase == ClassicGroupPhase.PreparingRebalance
                ? ConsumerGroupErrorStatus.RebalanceInProgress
                : ConsumerGroupErrorStatus.None);
        }
    }

    public LeaveGroupResult LeaveGroup(LeaveGroupCommand request)
    {
        lock (_groupLock)
        {
            if (!_consumerGroups.TryGetValue(request.GroupId, out var group))
                return new LeaveGroupResult(ConsumerGroupErrorStatus.UnknownMember, null);

            // v3+ batch leave, members named by id or by static instance id.
            if (request.Members.Count > 0)
            {
                var results = new List<LeaveGroupMemberResult>(request.Members.Count);
                foreach (var leaving in request.Members)
                {
                    var id = !string.IsNullOrEmpty(leaving.MemberId)
                        ? leaving.MemberId
                        : group.Members.Values.FirstOrDefault(m => m.GroupInstanceId == leaving.GroupInstanceId)?.MemberId;
                    var removed = id is not null && RemoveMember(group, id);
                    results.Add(new LeaveGroupMemberResult(leaving.MemberId, leaving.GroupInstanceId,
                        removed ? ConsumerGroupErrorStatus.None : ConsumerGroupErrorStatus.UnknownMember));
                }
                AfterMembersLeft(group, "members left");
                return new LeaveGroupResult(ConsumerGroupErrorStatus.None, results);
            }

            // v0-2 single leave.
            if (!RemoveMember(group, request.MemberId))
                return new LeaveGroupResult(ConsumerGroupErrorStatus.UnknownMember, null);
            AfterMembersLeft(group, "a member left");
            return new LeaveGroupResult(ConsumerGroupErrorStatus.None, null);
        }
    }

    // Kafka's group.min.session.timeout.ms; a JoinGroup normally names its own.
    private const int DefaultSessionTimeoutMs = 10_000;

    // Under _groupLock. Members that are in the group must rejoin; until they do, heartbeats say so.
    private void StartRebalance(ConsumerGroupState group, string reason)
    {
        group.Phase = ClassicGroupPhase.PreparingRebalance;
        Log.GroupRebalanceStarted(logger, group.GroupId, reason, group.Members.Count);

        // A sync still waiting belongs to the generation that is ending.
        foreach (var pending in group.PendingSyncs.Values)
            pending.TrySetResult(new SyncGroupResult(ConsumerGroupErrorStatus.RebalanceInProgress, []));
        group.PendingSyncs.Clear();

        // The longest rebalance timeout among the members bounds the wait for the ones that do not come back.
        var timeoutMs = group.Members.Values.Select(m => m.RebalanceTimeoutMs).DefaultIfEmpty(DefaultSessionTimeoutMs).Max();
        group.RebalanceDeadline?.Dispose();
        var rebalance = ++group.RebalanceRound;
        group.RebalanceDeadline = _time.CreateTimer(_ => OnRebalanceDeadline(group, rebalance), null,
            TimeSpan.FromMilliseconds(timeoutMs), Timeout.InfiniteTimeSpan);
    }

    // Under _groupLock. The join round completes once every member has rejoined.
    private void TryCompleteJoin(ConsumerGroupState group)
    {
        if (group.Phase == ClassicGroupPhase.PreparingRebalance
            && group.Members.Count > 0
            && group.Members.Keys.All(group.PendingJoins.ContainsKey))
        {
            CompleteJoin(group);
        }
    }

    private void OnRebalanceDeadline(ConsumerGroupState group, int round)
    {
        lock (_groupLock)
        {
            if (group.Phase != ClassicGroupPhase.PreparingRebalance || group.RebalanceRound != round)
                return;

            // Members that did not rejoin in time are out of the group.
            foreach (var missing in group.Members.Keys.Where(id => !group.PendingJoins.ContainsKey(id)).ToList())
            {
                Log.GroupMemberDropped(logger, group.GroupId, missing, "did not rejoin within the rebalance timeout");
                RemoveMember(group, missing);
            }

            if (group.Members.Count == 0)
            {
                BecomeEmpty(group);
                return;
            }
            CompleteJoin(group);
        }
    }

    // Under _groupLock. A new generation; the leader gets every member's subscription, the others none.
    private void CompleteJoin(ConsumerGroupState group)
    {
        group.RebalanceDeadline?.Dispose();
        group.RebalanceDeadline = null;
        group.GenerationId++;
        group.Phase = ClassicGroupPhase.CompletingRebalance;
        if (group.LeaderId is null || !group.Members.ContainsKey(group.LeaderId))
            group.LeaderId = group.Members.Keys.First();

        foreach (var member in group.Members.Values)
            member.Assignment = [];

        var everyone = group.Members.Values
            .Select(m => new JoinGroupMemberInfo(m.MemberId, m.GroupInstanceId, m.Metadata))
            .ToArray();

        Log.GroupRebalanceCompleted(logger, group.GroupId, group.GenerationId, group.Members.Count, group.LeaderId);
        RecordGeneration(group);

        foreach (var (memberId, pending) in group.PendingJoins)
        {
            var isLeader = memberId == group.LeaderId;
            Log.JoinGroupResponse(logger, memberId, isLeader ? everyone.Length : 0, 0);
            pending.TrySetResult(new JoinGroupResult
            {
                GenerationId = group.GenerationId,
                ProtocolName = group.ProtocolName,
                LeaderId = group.LeaderId,
                MemberId = memberId,
                Members = isLeader ? everyone : [],
            });
        }
        group.PendingJoins.Clear();
    }

    // Under _groupLock. The generation goes to __consumer_offsets for the coordinator that takes the group over; the
    // join round does not wait for it — losing the record costs a later coordinator generation numbers, not offsets.
    private void RecordGeneration(ConsumerGroupState group)
    {
        var groupId = group.GroupId;
        var generation = group.GenerationId;
        _offsets.RecordGenerationAsync(groupId, group.ProtocolType, generation, group.ProtocolName, group.LeaderId, CancellationToken.None)
            .ContinueWith(write =>
            {
                if (write.IsFaulted || write.Result != OffsetWriteStatus.Ok)
                    Log.GroupGenerationNotRecorded(logger, groupId, generation, write.Exception?.GetBaseException());
            }, TaskScheduler.Default);
    }

    // Under _groupLock. The leader's assignment: every member gets its part, waiting followers included.
    private void ApplyAssignment(ConsumerGroupState group, IReadOnlyList<SyncGroupAssignmentInput> assignments)
    {
        Log.SyncGroupLeaderAssignments(logger, assignments.Count);
        foreach (var assignment in assignments)
        {
            Log.SyncGroupMemberAssignment(logger, assignment.MemberId, assignment.Assignment.Length);
            if (group.Members.TryGetValue(assignment.MemberId, out var assigned))
                assigned.Assignment = assignment.Assignment;
        }

        group.Phase = ClassicGroupPhase.Stable;
        foreach (var (memberId, pending) in group.PendingSyncs)
        {
            var assignment = group.Members.TryGetValue(memberId, out var m) ? m.Assignment : [];
            Log.SyncGroupReturningAssignment(logger, memberId, assignment.Length);
            pending.TrySetResult(new SyncGroupResult(ConsumerGroupErrorStatus.None, assignment));
        }
        group.PendingSyncs.Clear();

        // One event per rebalance, when the leader distributes. Topic stays empty: the assignments span every
        // topic the group subscribes to, and decoding them to enumerate is more than the tap needs.
        if (observability?.HasSubscribers == true)
        {
            observability.Publish(new SurgewaveBrokerEvent(
                SurgewaveBrokerEventKind.Rebalanced,
                Topic: string.Empty, Partition: -1, Offset: null,
                Principal: null, RejectReason: null,
                Consumers: [group.GroupId],
                Key: null, Value: null,
                Timestamp: DateTimeOffset.UtcNow));
        }
    }

    // Under _groupLock. Restarts the member's session deadline; it runs out unless the member is heard from again.
    private void ArmSession(ConsumerGroupState group, GroupMember member)
    {
        member.LastHeartbeat = DateTime.UtcNow;
        member.SessionDeadline?.Dispose();
        var session = ++member.SessionRound;
        member.SessionDeadline = _time.CreateTimer(_ => OnSessionExpired(group, member, session), null,
            TimeSpan.FromMilliseconds(member.SessionTimeoutMs), Timeout.InfiniteTimeSpan);
    }

    private void OnSessionExpired(ConsumerGroupState group, GroupMember member, int round)
    {
        lock (_groupLock)
        {
            if (member.SessionRound != round || !group.Members.TryGetValue(member.MemberId, out var current) || !ReferenceEquals(current, member))
                return;

            // A member waiting in the join round is not gone; the rebalance timeout decides about it.
            if (group.PendingJoins.ContainsKey(member.MemberId))
                return;

            Log.GroupMemberDropped(logger, group.GroupId, member.MemberId, $"no heartbeat within {member.SessionTimeoutMs} ms");
            RemoveMember(group, member.MemberId);
            AfterMembersLeft(group, "a member's session expired");
        }
    }

    // Under _groupLock. Drops the member and fails what it was waiting for.
    private static bool RemoveMember(ConsumerGroupState group, string memberId)
    {
        if (!group.Members.Remove(memberId, out var member))
            return false;

        member.SessionDeadline?.Dispose();
        member.SessionDeadline = null;
        if (group.PendingJoins.Remove(memberId, out var join))
            join.TrySetResult(JoinGroupResult.Failed(ConsumerGroupErrorStatus.UnknownMember, memberId));
        if (group.PendingSyncs.Remove(memberId, out var sync))
            sync.TrySetResult(new SyncGroupResult(ConsumerGroupErrorStatus.UnknownMember, []));
        if (group.LeaderId == memberId)
            group.LeaderId = null;
        return true;
    }

    // Under _groupLock. The members that remain rebalance among themselves; nobody left empties the group.
    private void AfterMembersLeft(ConsumerGroupState group, string reason)
    {
        if (group.Members.Count == 0)
        {
            BecomeEmpty(group);
            return;
        }

        if (group.Phase != ClassicGroupPhase.PreparingRebalance)
            StartRebalance(group, reason);
        TryCompleteJoin(group);
    }

    // Under _groupLock.
    private static void BecomeEmpty(ConsumerGroupState group)
    {
        group.RebalanceDeadline?.Dispose();
        group.RebalanceDeadline = null;
        group.Phase = ClassicGroupPhase.Empty;
        group.LeaderId = null;
        group.GenerationId++;
    }

    private static string StateName(ClassicGroupPhase phase) => phase switch
    {
        ClassicGroupPhase.PreparingRebalance => "PreparingRebalance",
        ClassicGroupPhase.CompletingRebalance => "CompletingRebalance",
        ClassicGroupPhase.Stable => "Stable",
        _ => "Empty",
    };
}

/// <summary>Where a classic group is in its rebalance cycle, as Kafka names it.</summary>
internal enum ClassicGroupPhase
{
    Empty,
    PreparingRebalance,
    CompletingRebalance,
    Stable,
}
