using Kuestenlogik.Surgewave.Broker;
using Kuestenlogik.Surgewave.Coordination.Consumer;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// KIP-496 OffsetDelete: pin the group / partition error semantics. Without
/// these tests a refactor that "simplifies" the GroupSubscribedToTopic check
/// would silently allow an offset delete on an active group's partition,
/// which could race a future commit and lose progress data.
/// Exercises the protocol-neutral coordinator surface directly (#59); the
/// wire-level status -> ErrorCode mapping is covered by the adapter/e2e tests.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class OffsetDeleteTests : IDisposable
{
    private readonly string _testDir;
    private readonly OffsetStore _offsetStore;
    private readonly ConsumerGroupCoordinator _coordinator;

    public OffsetDeleteTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), "surgewave-offset-delete-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_testDir);
        _offsetStore = new OffsetStore(_testDir, NullLogger<OffsetStore>.Instance);
        _coordinator = new ConsumerGroupCoordinator(NullLogger<ConsumerGroupCoordinator>.Instance, _offsetStore);
    }

    public void Dispose()
    {
        _offsetStore.Dispose();
        try { Directory.Delete(_testDir, recursive: true); } catch { /* best-effort */ }
    }

    [Fact]
    public void UnknownGroup_ReturnsGroupIdNotFoundPerPartition()
    {
        var resp = _coordinator.DeleteOffsets(new OffsetDeleteCommand
        {
            GroupId = "ghost-group",
            Topics =
            [
                new OffsetDeleteTopic { Name = "orders", Partitions = [0, 1] },
            ],
        });

        var topic = Assert.Single(resp.Topics);
        Assert.All(topic.Partitions, p => Assert.Equal(ConsumerGroupErrorStatus.GroupIdNotFound, p.Status));
    }

    [Fact]
    public void EmptyGroup_DeletesOffsetsAndReturnsNone()
    {
        // Seed an offset directly on the store, then create an empty group so
        // the coordinator knows the id (DeleteOffset works against any group).
        _offsetStore.CommitOffset("g-empty", "orders", 0, offset: 42);
        ForceGroupExists("g-empty");

        var resp = _coordinator.DeleteOffsets(MakeCommand("g-empty", ("orders", 0)));

        var partition = Assert.Single(Assert.Single(resp.Topics).Partitions);
        Assert.Equal(ConsumerGroupErrorStatus.None, partition.Status);
        // Offset is gone — re-fetching falls back to -1 (no commit recorded).
        Assert.Equal(-1, _offsetStore.GetCommittedOffset("g-empty", "orders", 0));
    }

    [Fact]
    public void GroupWithActiveMembers_RejectsWithGroupSubscribedToTopic()
    {
        ForceGroupExists("g-active", memberCount: 2);
        _offsetStore.CommitOffset("g-active", "orders", 0, offset: 100);

        var resp = _coordinator.DeleteOffsets(MakeCommand("g-active", ("orders", 0)));

        var partition = Assert.Single(Assert.Single(resp.Topics).Partitions);
        Assert.Equal(ConsumerGroupErrorStatus.GroupSubscribedToTopic, partition.Status);
        // Offset must NOT have been deleted — the rejection is structural.
        Assert.Equal(100, _offsetStore.GetCommittedOffset("g-active", "orders", 0));
    }

    [Fact]
    public void IdempotentDelete_OnNonExistentOffset_ReturnsNone()
    {
        // KIP-496 is idempotent: deleting an offset that was never committed
        // returns None for that partition rather than a "not found" error.
        ForceGroupExists("g-idempotent");

        var resp = _coordinator.DeleteOffsets(MakeCommand("g-idempotent", ("never-seen", 7)));

        var partition = Assert.Single(Assert.Single(resp.Topics).Partitions);
        Assert.Equal(ConsumerGroupErrorStatus.None, partition.Status);
    }

    [Fact]
    public void MultipleTopicsAndPartitions_AllDeletedIndividually()
    {
        ForceGroupExists("g-multi");
        _offsetStore.CommitOffset("g-multi", "orders", 0, offset: 1);
        _offsetStore.CommitOffset("g-multi", "orders", 1, offset: 2);
        _offsetStore.CommitOffset("g-multi", "payments", 0, offset: 3);

        var resp = _coordinator.DeleteOffsets(new OffsetDeleteCommand
        {
            GroupId = "g-multi",
            Topics =
            [
                new OffsetDeleteTopic { Name = "orders", Partitions = [0, 1] },
                new OffsetDeleteTopic { Name = "payments", Partitions = [0] },
            ],
        });

        Assert.Equal(2, resp.Topics.Count);
        Assert.All(resp.Topics, t => Assert.All(t.Partitions, p => Assert.Equal(ConsumerGroupErrorStatus.None, p.Status)));
        Assert.Equal(-1, _offsetStore.GetCommittedOffset("g-multi", "orders", 0));
        Assert.Equal(-1, _offsetStore.GetCommittedOffset("g-multi", "orders", 1));
        Assert.Equal(-1, _offsetStore.GetCommittedOffset("g-multi", "payments", 0));
    }

    private static OffsetDeleteCommand MakeCommand(string groupId, params (string Topic, int Partition)[] tps)
    {
        var topics = tps
            .GroupBy(tp => tp.Topic)
            .Select(g => new OffsetDeleteTopic
            {
                Name = g.Key,
                Partitions = g.Select(tp => tp.Partition).ToList(),
            })
            .ToList();
        return new OffsetDeleteCommand
        {
            GroupId = groupId,
            Topics = topics,
        };
    }

    /// <summary>
    /// Drive the coordinator into having a group with N members by faking a
    /// JoinGroup. Without this we'd need to thread real assignors through the
    /// test, which is out of scope for OffsetDelete contract tests.
    /// </summary>
    private void ForceGroupExists(string groupId, int memberCount = 0)
    {
        var dictField = typeof(ConsumerGroupCoordinator)
            .GetField("_consumerGroups", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!;
        var groups = (System.Collections.IDictionary)dictField.GetValue(_coordinator)!;
        var stateType = typeof(ConsumerGroupCoordinator).Assembly.GetType("Kuestenlogik.Surgewave.Broker.ConsumerGroupState")!;
        var state = Activator.CreateInstance(stateType)!;
        // Set required init-only props via reflection.
        SetProp(stateType, state, "GroupId", groupId);
        SetProp(stateType, state, "ProtocolType", "consumer");
        SetProp(stateType, state, "ProtocolName", "range");

        if (memberCount > 0)
        {
            var membersProp = stateType.GetProperty("Members")!;
            var members = (System.Collections.IDictionary)membersProp.GetValue(state)!;
            var memberType = typeof(ConsumerGroupCoordinator).Assembly.GetType("Kuestenlogik.Surgewave.Broker.GroupMember")!;
            for (var i = 0; i < memberCount; i++)
            {
                var member = Activator.CreateInstance(memberType)!;
                var id = $"m-{i}";
                SetProp(memberType, member, "MemberId", id);
                SetProp(memberType, member, "ClientId", "test-client");
                SetProp(memberType, member, "Metadata", Array.Empty<byte>());
                members[id] = member;
            }
        }

        groups[groupId] = state;
    }

    private static void SetProp(Type t, object instance, string name, object value)
    {
        var prop = t.GetProperty(name)!;
        // init-only setters need the backing-field write — the simplest way is
        // to use the property setter through reflection since it still exists.
        prop.SetValue(instance, value);
    }
}
