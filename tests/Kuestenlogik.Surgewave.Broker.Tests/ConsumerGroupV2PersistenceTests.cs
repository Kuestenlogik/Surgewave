using Kuestenlogik.Surgewave.Broker.ConsumerGroupV2;
using Kuestenlogik.Surgewave.Broker.Coordination;
using Kuestenlogik.Surgewave.Coordination.Consumer;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// A KIP-848 consumer group's state lives in its partition of <c>__consumer_offsets</c> (#219): the coordinator writes
/// the group's records there before it answers a heartbeat, and a coordinator that takes the partition over — another
/// broker, or this one after a restart — reads the group from them. It used to live in this broker's memory only, so a
/// coordinator that took a group over knew neither its members nor its epochs.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class ConsumerGroupV2PersistenceTests : IDisposable
{
    private const int Partitions = 4;
    private const string TopicA = "v2-persist-topic";

    private readonly string _dataDir = Path.Combine(Path.GetTempPath(), "surgewave-cg2-persist-" + Guid.NewGuid().ToString("N"));
    private readonly LogManager _logManager;
    private readonly RecordBatchSerializer _serializer = new(NullLogger<RecordBatchSerializer>.Instance);
    private readonly CancellationToken _ct = TestContext.Current.CancellationToken;

    public ConsumerGroupV2PersistenceTests()
    {
        Directory.CreateDirectory(_dataDir);
        _logManager = new LogManager(_dataDir, new MemoryLogSegmentFactory(), persistTopicsToFile: false);
        _logManager.CreateTopicAsync(TopicA, partitionCount: 4).GetAwaiter().GetResult();
    }

    public void Dispose()
    {
        _logManager.Dispose();
        try { Directory.Delete(_dataDir, recursive: true); } catch { /* best-effort */ }
    }

    [Fact]
    public async Task ACoordinatorThatTakesThePartitionOverKnowsTheGroup()
    {
        var first = new ConsumerGroupV2Coordinator(NullLogger<ConsumerGroupV2Coordinator>.Instance, _logManager, await StoreAsync());
        var joined = await JoinAsync(first, "persist-g", "c1");
        var second = await JoinAsync(first, "persist-g", "c2");

        // Another store over the same log: a broker that takes the partition over, or this one after a restart.
        var next = new ConsumerGroupV2Coordinator(NullLogger<ConsumerGroupV2Coordinator>.Instance, _logManager, await StoreAsync());

        var group = Assert.Single(next.Describe(["persist-g"]));
        Assert.Equal(ConsumerGroupDescribeStatus.Ok, group.Status);
        Assert.Equal([joined.MemberId, second.MemberId], group.Members.Select(m => m.MemberId).Order(StringComparer.Ordinal));
        Assert.All(group.Members, m => Assert.Contains(TopicA, m.SubscribedTopicNames));
        Assert.Equal(second.MemberEpoch, group.GroupEpoch);

        // A member heartbeats on with the epoch it knew: the takeover kept it, so no fence.
        var again = await next.HeartbeatAsync(new ConsumerHeartbeatCommand
        {
            ClientId = "c1",
            GroupId = "persist-g",
            MemberId = joined.MemberId!,
            MemberEpoch = joined.MemberEpoch,
        }, _ct);
        Assert.NotEqual(ConsumerGroupFenceStatus.UnknownMember, again.Status);
    }

    [Fact]
    public async Task AMemberThatLeftIsGoneForTheNextCoordinatorToo()
    {
        var first = new ConsumerGroupV2Coordinator(NullLogger<ConsumerGroupV2Coordinator>.Instance, _logManager, await StoreAsync());
        var staying = await JoinAsync(first, "leave-g", "c1");
        var leaving = await JoinAsync(first, "leave-g", "c2");
        await first.HeartbeatAsync(new ConsumerHeartbeatCommand
        {
            ClientId = "c2", GroupId = "leave-g", MemberId = leaving.MemberId!, MemberEpoch = -1,
        }, _ct);

        var next = new ConsumerGroupV2Coordinator(NullLogger<ConsumerGroupV2Coordinator>.Instance, _logManager, await StoreAsync());

        Assert.Equal(staying.MemberId, Assert.Single(Assert.Single(next.Describe(["leave-g"])).Members).MemberId);
    }

    [Fact]
    public async Task ABrokerThatDoesNotLeadTheGroupsPartitionIsNotItsCoordinator()
    {
        // A store of a broker in a cluster that leads no partition of __consumer_offsets.
        var store = new OffsetStore(_logManager, _serializer, Partitions, NullLogger<OffsetStore>.Instance);
        var coordinator = new ConsumerGroupV2Coordinator(NullLogger<ConsumerGroupV2Coordinator>.Instance, _logManager, store);

        var response = await JoinAsync(coordinator, "elsewhere-g", "c1");

        Assert.Equal(ConsumerGroupFenceStatus.NotCoordinator, response.Status);
    }

    private Task<ConsumerHeartbeatResult> JoinAsync(ConsumerGroupV2Coordinator coordinator, string group, string clientId) =>
        coordinator.HeartbeatAsync(new ConsumerHeartbeatCommand
        {
            ClientId = clientId,
            GroupId = group,
            MemberId = "",
            MemberEpoch = 0,
            SubscribedTopicNames = [TopicA],
            RebalanceTimeoutMs = 60_000,
        }, _ct);

    // A store that leads every partition of __consumer_offsets in this log, read from it afresh.
    private async Task<OffsetStore> StoreAsync()
    {
        var store = new OffsetStore(_logManager, _serializer, Partitions, NullLogger<OffsetStore>.Instance);
        await store.StartLocalAsync(_ct);
        return store;
    }
}
