using Kuestenlogik.Surgewave.Broker.StreamsGroups;
using Kuestenlogik.Surgewave.Coordination.Streams;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// KIP-947: <see cref="StreamsGroupCoordinator"/> assigns tasks stickily across rebalances.
/// Adding or removing a member must move the minimum number of tasks; tasks already
/// owned by a still-present member should remain on that member.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class StreamsGroupStickyTests : IDisposable
{
    private readonly string _dataDir;
    private readonly LogManager _logManager;
    private readonly StreamsGroupCoordinator _coordinator;

    public StreamsGroupStickyTests()
    {
        _dataDir = Path.Combine(Path.GetTempPath(), "surgewave-streams-sticky-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_dataDir);
        _logManager = new LogManager(_dataDir, new MemoryLogSegmentFactory(), persistTopicsToFile: false);
        _logManager.CreateTopicAsync("source-topic", partitionCount: 6).GetAwaiter().GetResult();
        _coordinator = new StreamsGroupCoordinator(NullLogger<StreamsGroupCoordinator>.Instance, _logManager);
    }

    public void Dispose()
    {
        _logManager.Dispose();
        try { Directory.Delete(_dataDir, recursive: true); } catch { /* best-effort */ }
    }

    [Fact]
    public void StickyAssignment_AddingMember_PreservesExistingTaskOwnership()
    {
        // Arrange: two members each get 3 tasks of the 6-partition source topic.
        var topology = BuildSingleSubtopologyTopology();
        var first = SendHeartbeat("sticky-g", "c1", "", 0, topology);
        var second = SendHeartbeat("sticky-g", "c2", "", 0, topology);

        var beforeFirst = ExtractTaskPartitions(first);
        var beforeSecond = ExtractTaskPartitions(second);

        // Act: third member joins, triggering a rebalance.
        var third = SendHeartbeat("sticky-g", "c3", "", 0, topology);

        // Now refetch the assignments for c1 and c2.
        var afterFirst = SendHeartbeat("sticky-g", "c1", first.MemberId!, first.MemberEpoch, topology: null);
        var afterSecond = SendHeartbeat("sticky-g", "c2", second.MemberId!, second.MemberEpoch, topology: null);

        var afterFirstTasks = ExtractTaskPartitions(afterFirst);
        var afterSecondTasks = ExtractTaskPartitions(afterSecond);
        var thirdTasks = ExtractTaskPartitions(third);

        // Assert: each previous owner kept at least one task it had before — sticky.
        Assert.True(beforeFirst.Intersect(afterFirstTasks).Any(),
            $"c1 lost all of its sticky partitions. Before: [{string.Join(",", beforeFirst)}], After: [{string.Join(",", afterFirstTasks)}]");
        Assert.True(beforeSecond.Intersect(afterSecondTasks).Any(),
            $"c2 lost all of its sticky partitions. Before: [{string.Join(",", beforeSecond)}], After: [{string.Join(",", afterSecondTasks)}]");

        // The third member must have received some tasks.
        Assert.NotEmpty(thirdTasks);

        // No task duplication.
        var allAfter = afterFirstTasks.Concat(afterSecondTasks).Concat(thirdTasks).ToList();
        Assert.Equal(allAfter.Count, allAfter.Distinct().Count());
        Assert.Equal(6, allAfter.Count);
    }

    [Fact]
    public void StickyAssignment_RemovingMember_RedistributesOnlyOrphanedTasks()
    {
        var topology = BuildSingleSubtopologyTopology();
        var c1 = SendHeartbeat("rm-g", "c1", "", 0, topology);
        var c2 = SendHeartbeat("rm-g", "c2", "", 0, topology);

        var beforeC1 = ExtractTaskPartitions(c1);

        // c2 leaves.
        SendHeartbeat("rm-g", "c2", c2.MemberId!, -1, topology: null);

        // c1 sees the new assignment.
        var afterC1 = SendHeartbeat("rm-g", "c1", c1.MemberId!, c1.MemberEpoch, topology: null);
        var afterC1Tasks = ExtractTaskPartitions(afterC1);

        // c1 must still hold every task it had before, plus c2's orphans.
        Assert.True(beforeC1.IsSubsetOf(afterC1Tasks),
            $"c1 lost a previously-held task on a remove rebalance. Before: [{string.Join(",", beforeC1)}], After: [{string.Join(",", afterC1Tasks)}]");
        Assert.Equal(6, afterC1Tasks.Count);
    }

    private StreamsHeartbeatResult SendHeartbeat(
        string group,
        string clientId,
        string memberId,
        int memberEpoch,
        StreamsTopology? topology)
    {
        return _coordinator.Heartbeat(new StreamsHeartbeatCommand
        {
            ClientId = clientId,
            GroupId = group,
            MemberId = memberId,
            MemberEpoch = memberEpoch,
            Topology = topology,
        });
    }

    private static StreamsTopology BuildSingleSubtopologyTopology() =>
        new(1, [new StreamsSubtopology("0", ["source-topic"])]);

    private static HashSet<int> ExtractTaskPartitions(StreamsHeartbeatResult result)
    {
        var partitions = new HashSet<int>();
        foreach (var task in result.ActiveTasks)
        {
            foreach (var partition in task.Partitions)
            {
                partitions.Add(partition);
            }
        }
        return partitions;
    }
}
