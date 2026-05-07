using Kuestenlogik.Surgewave.Broker;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// KIP-496 OffsetDelete: pin the group / partition error semantics. Without
/// these tests a refactor that "simplifies" the GroupSubscribedToTopic check
/// would silently allow an offset delete on an active group's partition,
/// which could race a future commit and lose progress data.
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
        var resp = _coordinator.HandleOffsetDelete(new OffsetDeleteRequest
        {
            ApiKey = ApiKey.OffsetDelete,
            ApiVersion = 0,
            CorrelationId = 1,
            ClientId = "admin",
            GroupId = "ghost-group",
            Topics =
            [
                new OffsetDeleteRequest.OffsetDeleteTopic
                {
                    Name = "orders",
                    Partitions = [
                        new OffsetDeleteRequest.OffsetDeletePartition { PartitionIndex = 0 },
                        new OffsetDeleteRequest.OffsetDeletePartition { PartitionIndex = 1 },
                    ],
                },
            ],
        });

        Assert.Equal(ErrorCode.None, resp.ErrorCode); // top-level OK
        var topic = Assert.Single(resp.Topics);
        Assert.All(topic.Partitions, p => Assert.Equal(ErrorCode.GroupIdNotFound, p.ErrorCode));
    }

    [Fact]
    public void EmptyGroup_DeletesOffsetsAndReturnsNone()
    {
        // Seed an offset directly on the store, then create an empty group so
        // the coordinator knows the id (DeleteOffset works against any group).
        _offsetStore.CommitOffset("g-empty", "orders", 0, offset: 42);
        ForceGroupExists("g-empty");

        var resp = _coordinator.HandleOffsetDelete(MakeRequest("g-empty", ("orders", 0)));

        var partition = Assert.Single(Assert.Single(resp.Topics).Partitions);
        Assert.Equal(ErrorCode.None, partition.ErrorCode);
        // Offset is gone — re-fetching falls back to -1 (no commit recorded).
        Assert.Equal(-1, _offsetStore.GetCommittedOffset("g-empty", "orders", 0));
    }

    [Fact]
    public void GroupWithActiveMembers_RejectsWithGroupSubscribedToTopic()
    {
        ForceGroupExists("g-active", memberCount: 2);
        _offsetStore.CommitOffset("g-active", "orders", 0, offset: 100);

        var resp = _coordinator.HandleOffsetDelete(MakeRequest("g-active", ("orders", 0)));

        var partition = Assert.Single(Assert.Single(resp.Topics).Partitions);
        Assert.Equal(ErrorCode.GroupSubscribedToTopic, partition.ErrorCode);
        // Offset must NOT have been deleted — the rejection is structural.
        Assert.Equal(100, _offsetStore.GetCommittedOffset("g-active", "orders", 0));
    }

    [Fact]
    public void IdempotentDelete_OnNonExistentOffset_ReturnsNone()
    {
        // KIP-496 is idempotent: deleting an offset that was never committed
        // returns None for that partition rather than a "not found" error.
        ForceGroupExists("g-idempotent");

        var resp = _coordinator.HandleOffsetDelete(MakeRequest("g-idempotent", ("never-seen", 7)));

        var partition = Assert.Single(Assert.Single(resp.Topics).Partitions);
        Assert.Equal(ErrorCode.None, partition.ErrorCode);
    }

    [Fact]
    public void MultipleTopicsAndPartitions_AllDeletedIndividually()
    {
        ForceGroupExists("g-multi");
        _offsetStore.CommitOffset("g-multi", "orders", 0, offset: 1);
        _offsetStore.CommitOffset("g-multi", "orders", 1, offset: 2);
        _offsetStore.CommitOffset("g-multi", "payments", 0, offset: 3);

        var resp = _coordinator.HandleOffsetDelete(new OffsetDeleteRequest
        {
            ApiKey = ApiKey.OffsetDelete,
            ApiVersion = 0,
            CorrelationId = 1,
            ClientId = "admin",
            GroupId = "g-multi",
            Topics =
            [
                new OffsetDeleteRequest.OffsetDeleteTopic
                {
                    Name = "orders",
                    Partitions = [
                        new OffsetDeleteRequest.OffsetDeletePartition { PartitionIndex = 0 },
                        new OffsetDeleteRequest.OffsetDeletePartition { PartitionIndex = 1 },
                    ],
                },
                new OffsetDeleteRequest.OffsetDeleteTopic
                {
                    Name = "payments",
                    Partitions = [new OffsetDeleteRequest.OffsetDeletePartition { PartitionIndex = 0 }],
                },
            ],
        });

        Assert.Equal(2, resp.Topics.Count);
        Assert.All(resp.Topics, t => Assert.All(t.Partitions, p => Assert.Equal(ErrorCode.None, p.ErrorCode)));
        Assert.Equal(-1, _offsetStore.GetCommittedOffset("g-multi", "orders", 0));
        Assert.Equal(-1, _offsetStore.GetCommittedOffset("g-multi", "orders", 1));
        Assert.Equal(-1, _offsetStore.GetCommittedOffset("g-multi", "payments", 0));
    }

    private static OffsetDeleteRequest MakeRequest(string groupId, params (string Topic, int Partition)[] tps)
    {
        var topics = tps
            .GroupBy(tp => tp.Topic)
            .Select(g => new OffsetDeleteRequest.OffsetDeleteTopic
            {
                Name = g.Key,
                Partitions = g.Select(tp => new OffsetDeleteRequest.OffsetDeletePartition
                {
                    PartitionIndex = tp.Partition,
                }).ToList(),
            })
            .ToList();
        return new OffsetDeleteRequest
        {
            ApiKey = ApiKey.OffsetDelete,
            ApiVersion = 0,
            CorrelationId = 1,
            ClientId = "admin",
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
