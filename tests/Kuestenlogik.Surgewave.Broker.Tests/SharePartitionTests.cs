using Kuestenlogik.Surgewave.Coordination.Coordinators;
using Kuestenlogik.Surgewave.Broker.Abstractions.Routing;
using Kuestenlogik.Surgewave.Broker.ShareGroups;
using Kuestenlogik.Surgewave.Coordination.ShareGroups;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// A share group's view of a partition, held by the partition's leader (#228). It used to be one queue per topic,
/// shared by every group of the topic — two groups took records from each other — that counted a batch of several
/// records as one record, and every broker served it, a follower from its copy.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class SharePartitionTests : IDisposable
{
    private const string Topic = "share-orders";
    private static readonly TimeSpan LockDuration = TimeSpan.FromSeconds(30);

    private readonly string _dataDir = Path.Combine(Path.GetTempPath(), "surgewave-share-partition-" + Guid.NewGuid().ToString("N"));
    private readonly LogManager _logManager;
    private readonly RecordBatchSerializer _serializer = new(NullLogger<RecordBatchSerializer>.Instance);
    private readonly FakeTimeProvider _time = new();
    private readonly TopicPartition _partition = new() { Topic = Topic, Partition = 0 };
    private readonly Guid _topicId;
    private readonly CancellationToken _ct = TestContext.Current.CancellationToken;

    public SharePartitionTests()
    {
        Directory.CreateDirectory(_dataDir);
        _logManager = new LogManager(_dataDir, new MemoryLogSegmentFactory(), persistTopicsToFile: false);
        _topicId = _logManager.CreateTopicAsync(Topic, partitionCount: 1).GetAwaiter().GetResult().TopicId;
    }

    public void Dispose()
    {
        _logManager.Dispose();
        try { Directory.Delete(_dataDir, recursive: true); } catch { /* best-effort */ }
    }

    [Fact]
    public async Task EachRecordOfABatchIsAnOffsetOfItsOwn()
    {
        await ProduceAsync(3);
        var coordinator = Coordinator();

        var fetched = await FetchAsync(coordinator, "g1", "m1");

        Assert.Equal((0L, 2L, (short)1), Single(fetched));
    }

    [Fact]
    public async Task TwoGroupsOfOneTopicEachGetEveryRecord()
    {
        await ProduceAsync(3);
        var coordinator = Coordinator();

        var first = await FetchAsync(coordinator, "g1", "m1");
        var second = await FetchAsync(coordinator, "g2", "m1");

        Assert.Equal((0L, 2L, (short)1), Single(first));
        Assert.Equal((0L, 2L, (short)1), Single(second));
    }

    [Fact]
    public async Task AReleasedRecordIsDeliveredAgainUntilTheGroupsLimitThenArchived()
    {
        await ProduceAsync(1);
        var coordinator = Coordinator();
        Assert.Null(await coordinator.SetShareGroupConfigAsync("g1", "share.delivery.count.limit", "2", _ct));

        Assert.Equal((0L, 0L, (short)1), Single(await FetchAsync(coordinator, "g1", "m1")));
        Assert.Equal(ShareGroupErrorStatus.None, await AckAsync(coordinator, "g1", "m1", 0, release: true));
        Assert.Equal((0L, 0L, (short)2), Single(await FetchAsync(coordinator, "g1", "m1")));
        Assert.Equal(ShareGroupErrorStatus.None, await AckAsync(coordinator, "g1", "m1", 0, release: true));

        // Delivered twice, the limit: archived, not offered again.
        Assert.Empty((await FetchAsync(coordinator, "g1", "m1")).AcquiredRecords);
    }

    [Fact]
    public async Task ARecordWhoseLockRanOutGoesToTheNextMember()
    {
        await ProduceAsync(1);
        var coordinator = Coordinator();
        Single(await FetchAsync(coordinator, "g1", "m1"));

        Assert.Empty((await FetchAsync(coordinator, "g1", "m2")).AcquiredRecords);
        _time.Advance(LockDuration + TimeSpan.FromSeconds(1));

        Assert.Equal((0L, 0L, (short)2), Single(await FetchAsync(coordinator, "g1", "m2")));
    }

    [Fact]
    public async Task OnlyTheMemberHoldingARecordAcknowledgesIt()
    {
        await ProduceAsync(1);
        var coordinator = Coordinator();
        Single(await FetchAsync(coordinator, "g1", "m1"));

        Assert.Equal(ShareGroupErrorStatus.InvalidRecordState, await AckAsync(coordinator, "g1", "m2", 0, release: false));
        Assert.Equal(ShareGroupErrorStatus.None, await AckAsync(coordinator, "g1", "m1", 0, release: false));
    }

    [Fact]
    public async Task ABrokerThatDoesNotLeadThePartitionNamesItsLeader()
    {
        await ProduceAsync(1);
        var coordinator = Coordinator();
        coordinator.SetPartitionLeadership(new LedBy(new PartitionLeader(3, 7)));

        var fetched = await FetchAsync(coordinator, "g1", "m1");

        Assert.Equal(ShareGroupErrorStatus.NotLeaderOrFollower, fetched.Status);
        Assert.Equal(new ShareLeader(3, 7), fetched.CurrentLeader);
        Assert.Empty(fetched.AcquiredRecords);
    }

    [Fact]
    public async Task ALeaderThatTakesThePartitionOverGoesOnWhereTheLastOneStopped()
    {
        await ProduceAsync(2);
        var first = Coordinator(await ShareStateAsync());
        Assert.Equal((0L, 1L, (short)1), Single(await FetchAsync(first, "g1", "m1")));
        Assert.Equal(ShareGroupErrorStatus.None, await AckAsync(first, "g1", "m1", 0, release: false));

        // Another leader, reading __share_group_state afresh: record 0 is acknowledged, record 1 was held by a member of
        // the last leader and is delivered again, a second time.
        var next = Coordinator(await ShareStateAsync());

        Assert.Equal((1L, 1L, (short)2), Single(await FetchAsync(next, "g1", "m2")));
    }

    [Fact]
    public async Task AlteredStartOffsetsFenceTheLeadersSharePartitionWhichReadsThemAgain()
    {
        await ProduceAsync(3);
        var shareState = ShareStateStore.InMemory();
        var leader = Coordinator(shareState);
        var groupCoordinator = Coordinator(shareState);
        // An empty group, so its offsets can be altered.
        var joined = await groupCoordinator.HeartbeatAsync(new ShareGroupHeartbeatCommand
            { GroupId = "g1", MemberId = "", MemberEpoch = 0, ClientId = "c0", SubscribedTopicNames = [Topic] }, _ct);
        await groupCoordinator.HeartbeatAsync(new ShareGroupHeartbeatCommand
            { GroupId = "g1", MemberId = joined.MemberId, MemberEpoch = -1, ClientId = "c0", SubscribedTopicNames = [Topic] }, _ct);
        Assert.Equal((0L, 2L, (short)1), Single(await FetchAsync(leader, "g1", "m1")));

        var altered = await groupCoordinator.AlterOffsetsAsync(new AlterShareOffsetsCommand
        {
            GroupId = "g1",
            Topics = [new AlterShareOffsetsTopic { TopicName = Topic, Partitions = [new AlterShareOffsetsPartition(0, 2)] }],
        }, _ct);
        Assert.Equal(ShareGroupErrorStatus.None, altered.Status);

        // The leader's share partition is of the old state epoch: its write is refused, and it reads the new start.
        Assert.Equal(ShareGroupErrorStatus.CoordinatorNotAvailable, await AckAsync(leader, "g1", "m1", 0, release: false));
        Assert.Equal((2L, 2L, (short)1), Single(await FetchAsync(leader, "g1", "m1")));
        var described = await groupCoordinator.DescribeOffsetsAsync(new DescribeShareOffsetsCommand(
            [new DescribeShareOffsetsGroup { GroupId = "g1", Topics = [new DescribeShareOffsetsTopic { TopicName = Topic, Partitions = [0] }] }]), _ct);
        Assert.Equal(2, described.Groups.Single().Topics.Single().Partitions.Single().StartOffset);
    }

    [Fact]
    public async Task ALeaderThatDoesNotCoordinateTheGroupRunsItWithItsConfigs()
    {
        await ProduceAsync(1);
        var groupConfigs = new InMemoryGroupConfigStore();
        // The group's coordinator, on a broker in a cluster that leads no partition of __consumer_offsets: a config is
        // the metadata log's, not the group's, and needs no coordinator.
        var elsewhere = new ShareGroupCoordinator(NullLogger<ShareGroupCoordinator>.Instance, _logManager,
            new OffsetStore(_logManager, _serializer, 4, NullLogger<OffsetStore>.Instance), groupConfigs: groupConfigs);
        Assert.Null(await elsewhere.SetShareGroupConfigAsync("g1", "share.delivery.count.limit", "2", _ct));
        var leader = Coordinator(groupConfigs: groupConfigs);

        Assert.Equal((0L, 0L, (short)1), Single(await FetchAsync(leader, "g1", "m1")));
        Assert.Equal(ShareGroupErrorStatus.None, await AckAsync(leader, "g1", "m1", 0, release: true));
        Assert.Equal((0L, 0L, (short)2), Single(await FetchAsync(leader, "g1", "m1")));
        Assert.Equal(ShareGroupErrorStatus.None, await AckAsync(leader, "g1", "m1", 0, release: true));

        // Delivered twice, the group's limit, not Kafka's default of five: archived.
        Assert.Empty((await FetchAsync(leader, "g1", "m1")).AcquiredRecords);
    }

    [Fact]
    public async Task AShareSessionServesItsPartitionsToRequestsThatNoLongerNameThem()
    {
        await ProduceAsync(2);
        var coordinator = Coordinator();

        // Epoch 0 opens the session with the partition; later fetches name only what joins, leaves or is acknowledged.
        var opened = await SessionFetchAsync(coordinator, epoch: 0, partition: true, maxRecords: 1);
        Assert.Equal(ShareGroupErrorStatus.None, opened.Status);
        Assert.Equal((0L, 0L, (short)1), Single(opened.Responses.Single().Partitions.Single()));

        var next = await SessionFetchAsync(coordinator, epoch: 1, partition: false, maxRecords: 1);
        Assert.Equal((1L, 1L, (short)1), Single(next.Responses.Single().Partitions.Single()));
    }

    [Fact]
    public async Task AShareSessionTakesEachEpochOnceAndInOrder()
    {
        await ProduceAsync(1);
        var coordinator = Coordinator();

        Assert.Equal(ShareGroupErrorStatus.ShareSessionNotFound, (await SessionFetchAsync(coordinator, epoch: 1, partition: true)).Status);
        await SessionFetchAsync(coordinator, epoch: 0, partition: true);
        Assert.Equal(ShareGroupErrorStatus.InvalidShareSessionEpoch, (await SessionFetchAsync(coordinator, epoch: 2, partition: false)).Status);

        // An acknowledgement takes epoch 1, so the next fetch is epoch 2; the final epoch closes the session.
        var acked = await coordinator.ShareAcknowledgeAsync(new ShareAcknowledgeCommand
        {
            GroupId = "g1", MemberId = "m1", ShareSessionEpoch = 1,
            Topics = [new ShareAcknowledgeTopic { TopicId = _topicId, Partitions = [new ShareAcknowledgePartition { PartitionIndex = 0, AcknowledgementBatches = [new ShareAcknowledgementBatch(0, 0, [(sbyte)1])] }] }],
        }, _ct);
        Assert.Equal(ShareGroupErrorStatus.None, acked.Status);
        Assert.Equal(ShareGroupErrorStatus.None, (await SessionFetchAsync(coordinator, epoch: 2, partition: false)).Status);
        Assert.Equal(ShareGroupErrorStatus.None, (await SessionFetchAsync(coordinator, epoch: -1, partition: false)).Status);
        Assert.Equal(ShareGroupErrorStatus.ShareSessionNotFound, (await SessionFetchAsync(coordinator, epoch: 3, partition: false)).Status);
    }

    [Fact]
    public async Task AShareCoordinatorThatCannotBeReachedIsNoErrorOfTheFetch()
    {
        await ProduceAsync(1);
        var coordinator = Coordinator(new Unreachable());

        // Nothing this time, and no error: the member fetches again. A coordinator error here stopped Java's consumer.
        var fetched = await FetchAsync(coordinator, "g1", "m1");

        Assert.Equal(ShareGroupErrorStatus.None, fetched.Status);
        Assert.Empty(fetched.AcquiredRecords);
    }

    [Fact]
    public async Task AWriteOfAnOlderStateOrLeaderEpochIsFenced()
    {
        var store = ShareStateStore.InMemory();
        var key = new ShareStateKey("g1", _topicId, 0);
        var (initialized, epoch) = await store.InitializeAsync(key, 0, _ct);
        Assert.Equal(ShareStateStatus.Ok, initialized);

        Assert.Equal(ShareStateStatus.Ok, await store.WriteAsync(key, new ShareStateSnapshot(epoch, 5, 0, []), _ct));
        Assert.Equal(ShareStateStatus.FencedLeaderEpoch, await store.WriteAsync(key, new ShareStateSnapshot(epoch, 4, 0, []), _ct));

        var (_, next) = await store.InitializeAsync(key, 3, _ct);
        Assert.Equal(ShareStateStatus.FencedStateEpoch, await store.WriteAsync(key, new ShareStateSnapshot(epoch, 5, 0, []), _ct));
        var (read, state) = await store.ReadAsync(key, _ct);
        Assert.Equal(ShareStateStatus.Ok, read);
        Assert.Equal((next, 3L), (state!.StateEpoch, state.StartOffset));
    }

    [Fact]
    public async Task AKeyThisBrokerDoesNotCoordinateGoesToTheBrokerThatDoes()
    {
        // A store of a broker in a cluster that leads no partition of __share_group_state.
        var local = new ShareStateStore(_logManager, _serializer, 4, NullLogger<ShareStateStore>.Instance);
        var routing = new RoutingShareStatePersister(local);
        var key = new ShareStateKey("g1", _topicId, 0);

        Assert.Equal(ShareStateStatus.NotCoordinator, (await routing.ReadAsync(key, _ct)).Status);

        var remote = ShareStateStore.InMemory();
        await remote.InitializeAsync(key, 7, _ct);
        routing.SetRemote(remote);

        var (status, state) = await routing.ReadAsync(key, _ct);
        Assert.Equal(ShareStateStatus.Ok, status);
        Assert.Equal(7, state!.StartOffset);
    }

    // A fetch of member m1 of g1 in its share session, naming the topic's partition or not.
    private Task<ShareFetchResult> SessionFetchAsync(ShareGroupCoordinator coordinator, int epoch, bool partition, int maxRecords = 100) =>
        coordinator.ShareFetchAsync(new ShareFetchCommand
        {
            GroupId = "g1",
            MemberId = "m1",
            MaxRecords = maxRecords,
            ShareSessionEpoch = epoch,
            Topics = partition
                ? [new ShareFetchTopic { TopicId = _topicId, Partitions = [new ShareFetchPartition { PartitionIndex = 0, AcknowledgementBatches = [] }] }]
                : [],
        }, _ct);

    private ShareGroupCoordinator Coordinator(IShareStatePersister? shareState = null, IGroupConfigStore? groupConfigs = null) =>
        new(NullLogger<ShareGroupCoordinator>.Instance, _logManager, recordLockDuration: LockDuration, timeProvider: _time,
            shareState: shareState, groupConfigs: groupConfigs);

    // A share coordinator that leads every partition of __share_group_state in this log, read from it afresh.
    private async Task<ShareStateStore> ShareStateAsync()
    {
        var store = new ShareStateStore(_logManager, _serializer, 4, NullLogger<ShareStateStore>.Instance);
        await store.StartLocalAsync(_ct);
        return store;
    }

    // One batch of the given number of records.
    private async Task ProduceAsync(int records)
    {
        var messages = Enumerable.Range(0, records).Select(i => new Message
        {
            Offset = i,
            Timestamp = 1,
            Key = ReadOnlyMemory<byte>.Empty,
            Value = new byte[] { (byte)i },
            Headers = ReadOnlyMemory<byte>.Empty,
        }).ToList();
        await _logManager.AppendBatchAsync(_partition, _serializer.SerializeMessages(messages), _ct);
    }

    private async Task<ShareFetchPartitionResult> FetchAsync(ShareGroupCoordinator coordinator, string group, string member)
    {
        var result = await coordinator.ShareFetchAsync(new ShareFetchCommand
        {
            GroupId = group,
            MemberId = member,
            MaxRecords = 100,
            Topics = [new ShareFetchTopic { TopicId = _topicId, Partitions = [new ShareFetchPartition { PartitionIndex = 0, AcknowledgementBatches = [] }] }],
        }, _ct);
        return result.Responses.Single().Partitions.Single();
    }

    private async Task<ShareGroupErrorStatus> AckAsync(ShareGroupCoordinator coordinator, string group, string member, long offset, bool release) =>
        (await coordinator.ShareAcknowledgeAsync(new ShareAcknowledgeCommand
        {
            GroupId = group,
            MemberId = member,
            Topics =
            [
                new ShareAcknowledgeTopic
                {
                    TopicId = _topicId,
                    Partitions = [new ShareAcknowledgePartition { PartitionIndex = 0, AcknowledgementBatches = [new ShareAcknowledgementBatch(offset, offset, [release ? (sbyte)2 : (sbyte)1])] }],
                },
            ],
        }, _ct)).Responses.Single().Partitions.Single().Status;

    private static (long, long, short) Single(ShareFetchPartitionResult fetched)
    {
        Assert.Equal(ShareGroupErrorStatus.None, fetched.Status);
        var range = Assert.Single(fetched.AcquiredRecords);
        return (range.FirstOffset, range.LastOffset, range.DeliveryCount);
    }

    // A share coordinator that is moving: every request finds none.
    private sealed class Unreachable : IShareStatePersister
    {
        public Task<(ShareStateStatus Status, ShareStateSnapshot? State)> ReadAsync(ShareStateKey key, CancellationToken cancellationToken) =>
            Task.FromResult<(ShareStateStatus, ShareStateSnapshot?)>((ShareStateStatus.CoordinatorNotAvailable, null));

        public Task<ShareStateStatus> WriteAsync(ShareStateKey key, ShareStateSnapshot state, CancellationToken cancellationToken) =>
            Task.FromResult(ShareStateStatus.CoordinatorNotAvailable);

        public Task<(ShareStateStatus Status, int StateEpoch)> InitializeAsync(ShareStateKey key, long startOffset, CancellationToken cancellationToken) =>
            Task.FromResult((ShareStateStatus.CoordinatorNotAvailable, -1));

        public Task<ShareStateStatus> DeleteAsync(ShareStateKey key, CancellationToken cancellationToken) =>
            Task.FromResult(ShareStateStatus.CoordinatorNotAvailable);
    }

    private sealed class LedBy(PartitionLeader leader) : IPartitionLeadership
    {
        public bool IsLedByAnotherBroker(TopicPartition partition) => true;

        public PartitionLeader? LeaderOf(TopicPartition partition) => leader;
    }
}
