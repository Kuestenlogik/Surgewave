using Kuestenlogik.Surgewave.Broker.ConsumerGroupV2;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// KIP-1251 — every partition assigned to a member carries an epoch separate
/// from the group/member epoch. The epoch is bumped to the current group
/// epoch when the partition is newly assigned to the member; if the partition
/// stays with the same member across a rebalance, the per-partition epoch
/// must NOT change (that's the whole point — older commits for unchanged
/// partitions can stay valid).
///
/// These tests target the structural state + assignor wiring. The actual
/// per-partition fence-check on OffsetCommit / TxnOffsetCommit is a
/// documented follow-up; Surgewave today fences group-level which is
/// strictly more conservative, so this test set covers the durable parts.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class Kip1251PerPartitionEpochTests : IDisposable
{
    private readonly string _dataDir;
    private readonly LogManager _logManager;
    private readonly TargetAssignmentComputer _computer;
    private readonly Guid _topicId;

    private const string Topic = "kip1251-topic";

    public Kip1251PerPartitionEpochTests()
    {
        _dataDir = Path.Combine(Path.GetTempPath(), "surgewave-kip1251-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_dataDir);
        _logManager = new LogManager(_dataDir, new MemoryLogSegmentFactory(), persistTopicsToFile: false);
        var topic = _logManager.CreateTopicAsync(Topic, partitionCount: 4).GetAwaiter().GetResult();
        _topicId = topic.TopicId;
        _computer = new TargetAssignmentComputer(_logManager);
    }

    public void Dispose()
    {
        _logManager.Dispose();
        if (Directory.Exists(_dataDir)) Directory.Delete(_dataDir, recursive: true);
    }

    private ConsumerGroupV2State NewGroupWithOneMember(string memberId, int groupEpoch = 5)
    {
        return new ConsumerGroupV2State
        {
            GroupId = "g1",
            GroupEpoch = groupEpoch,
            AssignmentEpoch = groupEpoch - 1,
            Members =
            {
                [memberId] = new ConsumerGroupV2Member
                {
                    MemberId = memberId,
                    SubscribedTopicNames = { Topic },
                    MemberEpoch = groupEpoch,
                },
            },
        };
    }

    [Fact]
    public void FirstCompute_PopulatesAssignmentEpochsWithCurrentGroupEpoch()
    {
        var group = NewGroupWithOneMember("m1", groupEpoch: 7);

        _computer.Compute(group);

        var assignment = group.Members["m1"].TargetAssignment;
        Assert.Single(assignment);
        var topic = assignment[0];
        Assert.Equal(_topicId, topic.TopicId);
        Assert.NotNull(topic.AssignmentEpochs);
        Assert.Equal(topic.Partitions.Count, topic.AssignmentEpochs.Count);
        Assert.All(topic.AssignmentEpochs, e => Assert.Equal(7, e));
    }

    [Fact]
    public void Recompute_SameMembership_KeepsPerPartitionEpochsStable()
    {
        // Member m1 alone — gets all 4 partitions. Compute, bump GroupEpoch,
        // recompute: the partitions stay with m1, so the per-partition
        // epochs MUST remain at the first compute's value (the KIP's whole
        // point — old commits for unchanged partitions stay valid).
        var group = NewGroupWithOneMember("m1", groupEpoch: 7);
        _computer.Compute(group);
        var beforeEpochs = group.Members["m1"].TargetAssignment[0].AssignmentEpochs!.ToList();
        Assert.All(beforeEpochs, e => Assert.Equal(7, e));

        group.GroupEpoch = 9; // bump
        _computer.Compute(group);

        var afterEpochs = group.Members["m1"].TargetAssignment[0].AssignmentEpochs!;
        Assert.Equal(beforeEpochs, afterEpochs); // unchanged — stable per KIP-1251
    }

    [Fact]
    public void NewMemberJoins_TakesPartitions_AtCurrentGroupEpoch()
    {
        // Start with m1 owning all 4 partitions at epoch 7. New member m2
        // joins, GroupEpoch bumps. Partitions that get REASSIGNED to m2 must
        // carry the NEW epoch (9); partitions that stay with m1 keep the
        // original epoch (7).
        var group = NewGroupWithOneMember("m1", groupEpoch: 7);
        _computer.Compute(group);

        group.GroupEpoch = 9;
        group.Members["m2"] = new ConsumerGroupV2Member
        {
            MemberId = "m2",
            SubscribedTopicNames = { Topic },
            MemberEpoch = 9,
        };
        _computer.Compute(group);

        var m1 = group.Members["m1"].TargetAssignment[0];
        var m2 = group.Members["m2"].TargetAssignment[0];

        // m1 kept some partitions → those keep epoch 7
        Assert.All(m1.AssignmentEpochs!, e => Assert.Equal(7, e));
        // m2 newly assigned → epoch 9
        Assert.All(m2.AssignmentEpochs!, e => Assert.Equal(9, e));

        // Sanity: every partition appears exactly once across the two members
        var allPartitions = m1.Partitions.Concat(m2.Partitions).Order().ToList();
        Assert.Equal([0, 1, 2, 3], allPartitions);
    }

    [Fact]
    public void EmptyGroup_DoesNotPopulateAnyEpochs()
    {
        // Edge case: empty group → no TargetAssignment to populate.
        var group = new ConsumerGroupV2State
        {
            GroupId = "g-empty",
            GroupEpoch = 3,
        };
        _computer.Compute(group);
        Assert.Equal(3, group.AssignmentEpoch); // invariant preserved
        Assert.Empty(group.Members);
    }
}
