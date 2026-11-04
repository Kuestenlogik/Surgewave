using Kuestenlogik.Surgewave.Broker.Queue;
using Kuestenlogik.Surgewave.Broker.ShareGroups;
using Kuestenlogik.Surgewave.Coordination.ShareGroups;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// A share group's state lives in its partition of <c>__consumer_offsets</c> (#219): its members and epochs as Kafka's
/// ShareGroup records; its share partitions' start offsets with their share coordinators in
/// <c>__share_group_state</c> (#228). A coordinator that takes the partitions over —
/// another broker, or this one after a restart — reads the group from them. It lived in the coordinator's memory only.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class ShareGroupPersistenceTests : IAsyncDisposable
{
    private const int Partitions = 4;
    private const string Topic = "share-persist-topic";

    private readonly string _dataDir = Path.Combine(Path.GetTempPath(), "surgewave-share-persist-" + Guid.NewGuid().ToString("N"));
    private readonly LogManager _logManager;
    private readonly QueueViewManager _queues;
    private readonly RecordBatchSerializer _serializer = new(NullLogger<RecordBatchSerializer>.Instance);
    private readonly CancellationToken _ct = TestContext.Current.CancellationToken;

    public ShareGroupPersistenceTests()
    {
        Directory.CreateDirectory(_dataDir);
        _logManager = new LogManager(_dataDir, new MemoryLogSegmentFactory(), persistTopicsToFile: false);
        _logManager.CreateTopicAsync(Topic, partitionCount: 2).GetAwaiter().GetResult();
        _queues = new QueueViewManager(new QueueViewConfig { Enabled = true }, NullLoggerFactory.Instance, _logManager);
    }

    public async ValueTask DisposeAsync()
    {
        await _queues.DisposeAsync();
        _logManager.Dispose();
        try { Directory.Delete(_dataDir, recursive: true); } catch { /* best-effort */ }
    }

    [Fact]
    public async Task ACoordinatorThatTakesThePartitionOverKnowsTheGroupAndItsOffsets()
    {
        var first = await CoordinatorAsync();
        // A group, empty, so its offsets can be altered.
        var left = await first.HeartbeatAsync(Join("c0"), _ct);
        await first.HeartbeatAsync(Join("c0") with { MemberId = left.MemberId, MemberEpoch = -1 }, _ct);
        var altered = await first.AlterOffsetsAsync(new AlterShareOffsetsCommand
        {
            GroupId = "share-g",
            Topics = [new AlterShareOffsetsTopic { TopicName = Topic, Partitions = [new AlterShareOffsetsPartition(1, 42)] }],
        }, _ct);
        Assert.Equal(ShareGroupErrorStatus.None, altered.Status);
        var joined = await first.HeartbeatAsync(Join("c1"), _ct);

        var next = await CoordinatorAsync();

        var group = Assert.Single(next.Describe(["share-g"]));
        Assert.Equal(ShareGroupErrorStatus.None, group.Status);
        Assert.Equal(joined.MemberId, Assert.Single(group.Members).MemberId);
        var offsets = await next.DescribeOffsetsAsync(new DescribeShareOffsetsCommand(
            [new DescribeShareOffsetsGroup { GroupId = "share-g", Topics = [new DescribeShareOffsetsTopic { TopicName = Topic, Partitions = [1] }] }]), _ct);
        Assert.Equal(42, offsets.Groups.Single().Topics.Single().Partitions.Single().StartOffset);
    }

    [Fact]
    public async Task ABrokerThatDoesNotLeadTheGroupsPartitionIsNotItsCoordinator()
    {
        // A store of a broker in a cluster that leads no partition of __consumer_offsets.
        var coordinator = new ShareGroupCoordinator(NullLogger<ShareGroupCoordinator>.Instance, _logManager,
            new OffsetStore(_logManager, _serializer, Partitions, NullLogger<OffsetStore>.Instance));

        var response = await coordinator.HeartbeatAsync(Join("c1"), _ct);

        Assert.Equal(ShareGroupErrorStatus.NotCoordinator, response.Status);
    }

    private static ShareGroupHeartbeatCommand Join(string clientId) => new()
    {
        GroupId = "share-g",
        MemberId = "",
        MemberEpoch = 0,
        ClientId = clientId,
        SubscribedTopicNames = [Topic],
    };

    // A coordinator over stores that lead every partition of __consumer_offsets and __share_group_state in this log,
    // read from it afresh.
    private async Task<ShareGroupCoordinator> CoordinatorAsync()
    {
        var store = new OffsetStore(_logManager, _serializer, Partitions, NullLogger<OffsetStore>.Instance);
        await store.StartLocalAsync(_ct);
        var shareState = new ShareStateStore(_logManager, _serializer, Partitions, NullLogger<ShareStateStore>.Instance);
        await shareState.StartLocalAsync(_ct);
        return new ShareGroupCoordinator(NullLogger<ShareGroupCoordinator>.Instance, _logManager, store, shareState: shareState);
    }
}
