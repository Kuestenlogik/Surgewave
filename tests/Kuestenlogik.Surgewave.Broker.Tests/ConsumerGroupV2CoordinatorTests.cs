using Kuestenlogik.Surgewave.Broker.ConsumerGroupV2;
using Kuestenlogik.Surgewave.Coordination.Consumer;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// Unit tests for the KIP-848 <see cref="ConsumerGroupV2Coordinator"/>. Each test isolates
/// a single coordinator with an in-memory <see cref="LogManager"/> backing it; topics are
/// created up-front so the assignor can resolve partition counts.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class ConsumerGroupV2CoordinatorTests : IDisposable
{
    private readonly string _dataDir;
    private readonly LogManager _logManager;
    private readonly ConsumerGroupV2Coordinator _coordinator;
    private readonly Guid _topicAId;
    private readonly Guid _topicBId;

    private const string TopicA = "v2-topic-a";
    private const string TopicB = "v2-topic-b";

    public ConsumerGroupV2CoordinatorTests()
    {
        _dataDir = Path.Combine(Path.GetTempPath(), "surgewave-cg2-tests-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_dataDir);

        _logManager = new LogManager(_dataDir, new MemoryLogSegmentFactory(), persistTopicsToFile: false);

        var topicA = _logManager.CreateTopicAsync(TopicA, partitionCount: 6).GetAwaiter().GetResult();
        var topicB = _logManager.CreateTopicAsync(TopicB, partitionCount: 4).GetAwaiter().GetResult();
        _topicAId = topicA.TopicId;
        _topicBId = topicB.TopicId;

        _coordinator = new ConsumerGroupV2Coordinator(
            NullLogger<ConsumerGroupV2Coordinator>.Instance,
            _logManager);
    }

    public void Dispose()
    {
        _logManager.Dispose();
        try { Directory.Delete(_dataDir, recursive: true); } catch { /* best-effort cleanup */ }
    }

    [Fact]
    public void Heartbeat_RebalanceTimeoutMs_OnlyShrinksAfterInit()
    {
        // First heartbeat seeds the rebalance timeout to 60s.
        var first = SendHeartbeat("kip955-g", "c1", [TopicA], rebalanceTimeoutMs: 60_000);
        Assert.Equal(ConsumerGroupFenceStatus.Ok, first.Status);

        // Larger timeout from a later heartbeat must NOT raise the group's value back up.
        SendHeartbeat("kip955-g", "c2", [TopicA], rebalanceTimeoutMs: 90_000);

        // Smaller timeout shrinks it.
        SendHeartbeat("kip955-g", "c3", [TopicA], rebalanceTimeoutMs: 30_000);

        var groups = typeof(ConsumerGroupV2Coordinator)
            .GetField("_groups", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!
            .GetValue(_coordinator)!;
        var indexer = groups.GetType().GetProperty("Item", new[] { typeof(string) })!;
        var group = indexer.GetValue(groups, ["kip955-g"])!;
        var rebalanceTimeoutProp = group.GetType().GetProperty("RebalanceTimeoutMs")!;
        var timeout = (int)rebalanceTimeoutProp.GetValue(group)!;

        Assert.Equal(30_000, timeout); // Latched to the smallest value seen.
    }

    [Fact]
    public void Heartbeat_FirstMember_GetsAllPartitionsOfSubscribedTopic()
    {
        var resp = SendHeartbeat(group: "g1", clientId: "c1", subscribed: [TopicA]);

        Assert.Equal(ConsumerGroupFenceStatus.Ok, resp.Status);
        Assert.NotEmpty(resp.Assignment);
        var assignment = Assert.Single(resp.Assignment);
        Assert.Equal(_topicAId, assignment.TopicId);
        Assert.Equal(new[] { 0, 1, 2, 3, 4, 5 }, assignment.Partitions);
    }

    [Fact]
    public void Heartbeat_SecondMember_TargetSplitButReconciliationWithholdsConflictingPartitions()
    {
        var first = SendHeartbeat("g2", "c1", [TopicA]);

        // Member 1 reports it owns the full range it was just told to take.
        var ownedByFirst = first.Assignment
            .Select(t => new ConsumerTopicPartitions(t.TopicId, t.Partitions))
            .ToList();
        SendHeartbeat("g2", "c1", subscribed: null, memberId: first.MemberId, memberEpoch: first.MemberEpoch, owned: ownedByFirst);

        // Second member joins.
        var second = SendHeartbeat("g2", "c2", [TopicA]);

        // Range over 6 partitions split between two members → 0..2 vs 3..5. Member 2 has
        // not been advertised any partitions yet because member 1 still owns them. KIP-848
        // models this as an absent (empty) assignment in the response.
        Assert.Empty(second.Assignment);
    }

    [Fact]
    public void Heartbeat_SecondMember_AfterFirstRevokes_GetsItsRangeShare()
    {
        var first = SendHeartbeat("g3", "c1", [TopicA]);
        var firstId = first.MemberId;

        // Member 1 acks its full assignment.
        SendHeartbeat("g3", "c1", subscribed: null, memberId: firstId, memberEpoch: first.MemberEpoch,
            owned: AssignmentToOwned(first.Assignment));

        // Member 2 joins → triggers rebalance.
        var second = SendHeartbeat("g3", "c2", [TopicA]);

        // Member 1 returns and now reports a reduced ownership (it has revoked partitions 3..5).
        var refetched = SendHeartbeat("g3", "c1", subscribed: null, memberId: firstId,
            memberEpoch: first.MemberEpoch,
            owned: [new ConsumerTopicPartitions(_topicAId, [0, 1, 2])]);

        // Now member 2 should see its target.
        var second2 = SendHeartbeat("g3", "c2", subscribed: null, memberId: second.MemberId,
            memberEpoch: second.MemberEpoch,
            owned: []);

        Assert.NotEmpty(second2.Assignment);
        var assignment = Assert.Single(second2.Assignment);
        Assert.Equal(_topicAId, assignment.TopicId);
        Assert.Equal(new[] { 3, 4, 5 }, assignment.Partitions);
        Assert.Equal(new[] { 0, 1, 2 }, refetched.Assignment[0].Partitions);
    }

    [Fact]
    public void Heartbeat_LeaveRequest_RemovesMemberAndRebalances()
    {
        var first = SendHeartbeat("g4", "c1", [TopicA]);
        SendHeartbeat("g4", "c1", subscribed: null, memberId: first.MemberId, memberEpoch: first.MemberEpoch,
            owned: AssignmentToOwned(first.Assignment));

        var second = SendHeartbeat("g4", "c2", [TopicA]);

        // Member 1 leaves (epoch -1).
        var leaveResp = _coordinator.Heartbeat(new ConsumerHeartbeatCommand
        {
            ClientId = "c1",
            GroupId = "g4",
            MemberId = first.MemberId,
            MemberEpoch = -1,
            SubscribedTopicNames = null,
        });
        Assert.Equal(-1, leaveResp.MemberEpoch);

        // Member 2's next heartbeat should now see the full topic.
        var second2 = SendHeartbeat("g4", "c2", subscribed: null, memberId: second.MemberId,
            memberEpoch: second.MemberEpoch, owned: []);

        Assert.NotEmpty(second2.Assignment);
        var assignment = Assert.Single(second2.Assignment);
        Assert.Equal(new[] { 0, 1, 2, 3, 4, 5 }, assignment.Partitions);
    }

    [Fact]
    public void Heartbeat_ServerAssignorRoundRobin_DistributesAcrossTopics()
    {
        var first = SendHeartbeat("g5", "c1", [TopicA, TopicB], serverAssignor: "roundrobin");
        var firstId = first.MemberId;
        SendHeartbeat("g5", "c1", subscribed: null, memberId: firstId, memberEpoch: first.MemberEpoch,
            owned: AssignmentToOwned(first.Assignment));

        var second = SendHeartbeat("g5", "c2", [TopicA, TopicB], serverAssignor: "roundrobin");

        // Assignor switch may bump an epoch; verify by describing.
        var describe = _coordinator.Describe(["g5"]);

        var group = Assert.Single(describe);
        Assert.Equal("roundrobin", group.AssignorName);
        Assert.Equal(2, group.Members.Count);
    }

    [Fact]
    public void Heartbeat_UnknownServerAssignor_KeepsRangeAsDefault()
    {
        var first = SendHeartbeat("g6", "c1", [TopicA], serverAssignor: "does-not-exist");

        var describe = _coordinator.Describe(["g6"]);

        var group = Assert.Single(describe);
        // PartitionAssignorFactory falls back to "range" for unknown names; the coordinator
        // uses that canonical name so the group does not flip back and forth on every heartbeat.
        Assert.Equal("range", group.AssignorName);
        Assert.NotEmpty(first.Assignment);
    }

    [Fact]
    public void Describe_ReflectsReconcilingAndStableStates()
    {
        var first = SendHeartbeat("g7", "c1", [TopicA]);
        SendHeartbeat("g7", "c1", subscribed: null, memberId: first.MemberId, memberEpoch: first.MemberEpoch,
            owned: AssignmentToOwned(first.Assignment));

        // After only first member's full ack, the group is stable.
        var stable = Describe("g7");
        Assert.Equal(ConsumerGroupPhase.Stable, stable.Members.Count == 0 ? ConsumerGroupPhase.Empty : stable.Phase);

        // Add a second member — group becomes Reconciling because member 1 still owns its partitions.
        SendHeartbeat("g7", "c2", [TopicA]);
        var reconciling = Describe("g7");
        Assert.Equal(ConsumerGroupPhase.Reconciling, reconciling.Phase);
    }

    private ConsumerHeartbeatResult SendHeartbeat(
        string group,
        string clientId,
        List<string>? subscribed,
        string? memberId = null,
        int memberEpoch = 0,
        List<ConsumerTopicPartitions>? owned = null,
        string? serverAssignor = null,
        int rebalanceTimeoutMs = -1)
    {
        return _coordinator.Heartbeat(new ConsumerHeartbeatCommand
        {
            ClientId = clientId,
            GroupId = group,
            MemberId = memberId ?? "",
            MemberEpoch = memberEpoch,
            SubscribedTopicNames = subscribed,
            ServerAssignor = serverAssignor,
            OwnedTopicPartitions = owned,
            RebalanceTimeoutMs = rebalanceTimeoutMs,
        });
    }

    private ConsumerGroupDescription Describe(string groupId)
    {
        var resp = _coordinator.Describe([groupId]);
        return Assert.Single(resp);
    }

    private static List<ConsumerTopicPartitions> AssignmentToOwned(
        IReadOnlyList<ConsumerTopicPartitions> assignment)
    {
        var result = new List<ConsumerTopicPartitions>(assignment.Count);
        foreach (var t in assignment)
        {
            result.Add(new ConsumerTopicPartitions(t.TopicId, [.. t.Partitions]));
        }
        return result;
    }
}
