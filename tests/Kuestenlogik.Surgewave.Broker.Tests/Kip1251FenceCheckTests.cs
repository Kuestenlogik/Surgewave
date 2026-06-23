using Kuestenlogik.Surgewave.Broker.ConsumerGroupV2;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// KIP-1251 follow-up — the per-partition fence-check on
/// <see cref="ConsumerGroupV2Coordinator.IsPartitionAssignmentValid"/>.
/// The structural piece (per-partition <c>AssignmentEpochs</c> on every
/// <see cref="TopicPartitionAssignment"/>) was pinned by
/// <c>Kip1251PerPartitionEpochTests</c>; this set covers the fence-check
/// that consumes it from <c>CommitOffsetsForV2Group</c>.
///
/// The KIP's whole point: an old member that still owns a partition across
/// a rebalance must be able to commit for it. The pre-flight
/// <see cref="ConsumerGroupV2Coordinator.ValidateMemberForOffsetOperation"/>
/// no longer fences on <c>memberEpoch &lt; group.MemberEpoch</c> — that
/// decision now belongs per-partition.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class Kip1251FenceCheckTests : IDisposable
{
    private readonly string _dataDir;
    private readonly LogManager _logManager;
    private readonly ConsumerGroupV2Coordinator _coordinator;
    private readonly TargetAssignmentComputer _computer;
    private readonly Guid _topicId;

    private const string Topic = "kip1251-fence-topic";

    public Kip1251FenceCheckTests()
    {
        _dataDir = Path.Combine(Path.GetTempPath(), "surgewave-kip1251-fence-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_dataDir);
        _logManager = new LogManager(_dataDir, new MemoryLogSegmentFactory(), persistTopicsToFile: false);
        var topic = _logManager.CreateTopicAsync(Topic, partitionCount: 4).GetAwaiter().GetResult();
        _topicId = topic.TopicId;
        _coordinator = new ConsumerGroupV2Coordinator(NullLogger<ConsumerGroupV2Coordinator>.Instance, _logManager);
        _computer = new TargetAssignmentComputer(_logManager);
    }

    public void Dispose()
    {
        _logManager.Dispose();
        try { Directory.Delete(_dataDir, recursive: true); } catch { /* best-effort */ }
    }

    /// <summary>
    /// Reaches into the coordinator's private <c>_groups</c> dict to set up
    /// a group with a controlled (member, partition) ownership map without
    /// going through the heartbeat path. Lets each test stage the exact
    /// epoch-mismatch scenario it wants to fence-check.
    /// </summary>
    private void SeedGroup(string groupId, int groupEpoch, params (string MemberId, int MemberEpoch, int[] Partitions, int[]? PartitionEpochs)[] members)
    {
        var groupsField = typeof(ConsumerGroupV2Coordinator).GetField("_groups", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!;
        var lockField = typeof(ConsumerGroupV2Coordinator).GetField("_groupLock", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!;
        var lockObj = lockField.GetValue(_coordinator)!;
        var dict = (Dictionary<string, ConsumerGroupV2State>)groupsField.GetValue(_coordinator)!;

        var group = new ConsumerGroupV2State
        {
            GroupId = groupId,
            GroupEpoch = groupEpoch,
            AssignmentEpoch = groupEpoch,
        };
        foreach (var (memberId, memberEpoch, partitions, partitionEpochs) in members)
        {
            group.Members[memberId] = new ConsumerGroupV2Member
            {
                MemberId = memberId,
                MemberEpoch = memberEpoch,
                SubscribedTopicNames = { Topic },
                TargetAssignment =
                [
                    new TopicPartitionAssignment
                    {
                        TopicId = _topicId,
                        Partitions = [.. partitions],
                        AssignmentEpochs = partitionEpochs is null ? null : [.. partitionEpochs],
                    },
                ],
            };
        }
        lock (lockObj) dict[groupId] = group;
    }

    [Fact]
    public void CurrentMember_CurrentEpoch_OwnsPartition_PassesFence()
    {
        SeedGroup("g1", groupEpoch: 5, ("m1", MemberEpoch: 5, [0, 1, 2], [5, 5, 5]));
        Assert.True(_coordinator.IsPartitionAssignmentValid("g1", "m1", memberEpoch: 5, _topicId, partition: 0));
    }

    [Fact]
    public void OldMemberEpoch_PartitionStillOwnedAtOriginalEpoch_PassesFence()
    {
        // The KIP's motivating case: m1 was assigned partition 0 at epoch 5
        // and still holds it after a no-op-for-this-partition rebalance to
        // epoch 7. m1's claim at epoch 5 must still be valid for partition 0.
        SeedGroup("g2", groupEpoch: 7, ("m1", MemberEpoch: 7, [0, 1], [5, 5]));
        Assert.True(_coordinator.IsPartitionAssignmentValid("g2", "m1", memberEpoch: 5, _topicId, partition: 0));
    }

    [Fact]
    public void OldMemberEpoch_PartitionReassignedAtNewerEpoch_RejectsFence()
    {
        // m1's partition 2 was reassigned at epoch 7 (perhaps swept in for
        // load balancing). A claim from m1 at memberEpoch=5 for partition 2
        // is now zombie — the per-partition epoch 7 > 5, so fence.
        SeedGroup("g3", groupEpoch: 7, ("m1", MemberEpoch: 7, [0, 1, 2], [5, 5, 7]));
        Assert.False(_coordinator.IsPartitionAssignmentValid("g3", "m1", memberEpoch: 5, _topicId, partition: 2));
        // Same member at the same older epoch for partitions 0/1 (which
        // didn't move) should still pass.
        Assert.True(_coordinator.IsPartitionAssignmentValid("g3", "m1", memberEpoch: 5, _topicId, partition: 0));
        Assert.True(_coordinator.IsPartitionAssignmentValid("g3", "m1", memberEpoch: 5, _topicId, partition: 1));
    }

    [Fact]
    public void Member_DoesNotOwnPartition_RejectsFence()
    {
        // partition 3 was never assigned to m1.
        SeedGroup("g4", groupEpoch: 5, ("m1", MemberEpoch: 5, [0, 1, 2], [5, 5, 5]));
        Assert.False(_coordinator.IsPartitionAssignmentValid("g4", "m1", memberEpoch: 5, _topicId, partition: 3));
    }

    [Fact]
    public void UnknownGroupOrMember_RejectsFence()
    {
        SeedGroup("g5", groupEpoch: 5, ("m1", MemberEpoch: 5, [0], [5]));
        Assert.False(_coordinator.IsPartitionAssignmentValid("g-nope", "m1", 5, _topicId, 0));
        Assert.False(_coordinator.IsPartitionAssignmentValid("g5", "m-nope", 5, _topicId, 0));
        Assert.False(_coordinator.IsPartitionAssignmentValid("g5", null, 5, _topicId, 0));
    }

    [Fact]
    public void LegacyStateWithoutAssignmentEpochs_FallsBackToGroupLevelEquality()
    {
        // Records persisted before KIP-1251 carry AssignmentEpochs=null.
        // On read-back, the fence check must fall back to the strict
        // group-level equality check — otherwise older epochs would
        // silently pass without the per-partition info.
        SeedGroup("g6", groupEpoch: 5, ("m1", MemberEpoch: 5, [0], null));
        Assert.True(_coordinator.IsPartitionAssignmentValid("g6", "m1", memberEpoch: 5, _topicId, 0));
        Assert.False(_coordinator.IsPartitionAssignmentValid("g6", "m1", memberEpoch: 4, _topicId, 0));
    }

    [Fact]
    public void ValidateMemberForOffsetOperation_OlderEpoch_NoLongerFences()
    {
        // KIP-1251 loosened the pre-flight: memberEpoch < group MemberEpoch
        // is no longer a hard reject. The fine-grained fence happens
        // per-partition. The pre-flight should now return None for the
        // old-epoch case (was StaleMemberEpoch).
        SeedGroup("g7", groupEpoch: 7, ("m1", MemberEpoch: 7, [0], [5]));
        Assert.Equal(ErrorCode.None,
            _coordinator.ValidateMemberForOffsetOperation("g7", "m1", memberEpoch: 5));
    }

    [Fact]
    public void ValidateMemberForOffsetOperation_FutureEpoch_StillFenced()
    {
        // The other half of the loosened pre-flight: a memberEpoch FROM
        // THE FUTURE is impossible (the broker never issued it), so the
        // pre-flight still rejects with FencedMemberEpoch.
        SeedGroup("g8", groupEpoch: 5, ("m1", MemberEpoch: 5, [0], [5]));
        Assert.Equal(ErrorCode.FencedMemberEpoch,
            _coordinator.ValidateMemberForOffsetOperation("g8", "m1", memberEpoch: 99));
    }
}
