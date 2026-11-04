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
        Assert.Equal(ShareGroupErrorStatus.None, Ack(coordinator, "g1", "m1", 0, release: true));
        Assert.Equal((0L, 0L, (short)2), Single(await FetchAsync(coordinator, "g1", "m1")));
        Assert.Equal(ShareGroupErrorStatus.None, Ack(coordinator, "g1", "m1", 0, release: true));

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

        Assert.Equal(ShareGroupErrorStatus.InvalidRecordState, Ack(coordinator, "g1", "m2", 0, release: false));
        Assert.Equal(ShareGroupErrorStatus.None, Ack(coordinator, "g1", "m1", 0, release: false));
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

    private ShareGroupCoordinator Coordinator() =>
        new(NullLogger<ShareGroupCoordinator>.Instance, _logManager, recordLockDuration: LockDuration, timeProvider: _time);

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

    private ShareGroupErrorStatus Ack(ShareGroupCoordinator coordinator, string group, string member, long offset, bool release) =>
        coordinator.ShareAcknowledge(new ShareAcknowledgeCommand
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
        }).Responses.Single().Partitions.Single().Status;

    private static (long, long, short) Single(ShareFetchPartitionResult fetched)
    {
        Assert.Equal(ShareGroupErrorStatus.None, fetched.Status);
        var range = Assert.Single(fetched.AcquiredRecords);
        return (range.FirstOffset, range.LastOffset, range.DeliveryCount);
    }

    private sealed class LedBy(PartitionLeader leader) : IPartitionLeadership
    {
        public bool IsLedByAnotherBroker(TopicPartition partition) => true;

        public PartitionLeader? LeaderOf(TopicPartition partition) => leader;
    }
}
