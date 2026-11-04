using Kuestenlogik.Surgewave.Api.Grpc;
using Kuestenlogik.Surgewave.Api.Grpc.Server;
using Kuestenlogik.Surgewave.Coordination.Consumer;
using Kuestenlogik.Surgewave.Coordination.Coordinators;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Testing;
using Xunit;
using GrpcTopicPartition = Kuestenlogik.Surgewave.Api.Grpc.TopicPartition;
using Group = Kuestenlogik.Surgewave.Coordination.Consumer;

namespace Kuestenlogik.Surgewave.Api.Grpc.Tests;

/// <summary>
/// The gRPC offset commit and fetch go through the group's coordinator (#221), as the Kafka path's OffsetCommit and
/// OffsetFetch do: a broker that does not coordinate the group answers NOT_COORDINATOR and names the one that does, and
/// the coordinator checks the member and generation. They wrote and read the offset store directly, past all of it.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class GroupOffsetsTests : IDisposable
{
    private readonly string _dataDir = Path.Combine(Path.GetTempPath(), $"surgewave-grpc-offsets-{Guid.NewGuid():N}");
    private readonly LogManager _logManager;
    private readonly RecordingCoordinator _coordinator = new();
    private readonly Locator _locator = new();
    private readonly ConsumerServiceImpl _service;

    public GroupOffsetsTests()
    {
        Directory.CreateDirectory(_dataDir);
        _logManager = new LogManager(_dataDir, new MemoryLogSegmentFactory());
        var offsets = new GroupOffsets(_coordinator, _locator);
        _service = new ConsumerServiceImpl(_logManager, _ => [], offsets.CommitAsync, offsets.Fetch, offsets.CoordinatorOf);
    }

    public void Dispose()
    {
        _logManager.Dispose();
        try { Directory.Delete(_dataDir, recursive: true); } catch { /* best-effort cleanup */ }
    }

    [Fact]
    public async Task ACommitOnABrokerThatDoesNotCoordinateTheGroupNamesTheCoordinator()
    {
        _locator.Coordinator = 3;

        var response = await _service.Commit(Commit(), new TestServerCallContext());

        Assert.All(response.Results, r => Assert.Equal(ErrorCode.NotCoordinator, r.Status.ErrorCode));
        Assert.Equal((3, "broker-3", 9092), (response.Coordinator.BrokerId, response.Coordinator.Host, response.Coordinator.Port));
        Assert.Null(_coordinator.Committed);
    }

    [Fact]
    public async Task ACommitIsTheCoordinatorsWithItsMemberAndGeneration()
    {
        _coordinator.Answer = ConsumerGroupErrorStatus.IllegalGeneration;
        var request = Commit();
        request.MemberId = "member-1";
        request.GenerationId = 4;

        var response = await _service.Commit(request, new TestServerCallContext());

        Assert.Equal(("orders-app", "member-1", 4), (_coordinator.Committed!.GroupId, _coordinator.Committed.MemberId, _coordinator.Committed.GenerationIdOrMemberEpoch));
        Assert.All(response.Results, r => Assert.Equal(ErrorCode.IllegalGeneration, r.Status.ErrorCode));
        Assert.Null(response.Coordinator);
    }

    [Fact]
    public async Task ACommitWithoutAGenerationIsASimpleConsumers()
    {
        var response = await _service.Commit(Commit(), new TestServerCallContext());

        Assert.Equal(-1, _coordinator.Committed!.GenerationIdOrMemberEpoch);
        Assert.Null(_coordinator.Committed.MemberId);
        Assert.Equal([0, 1], _coordinator.Committed.Topics.Single().Partitions.Select(p => p.PartitionIndex));
        Assert.All(response.Results, r => Assert.Equal(ErrorCode.None, r.Status.ErrorCode));
    }

    [Fact]
    public async Task AFetchReadsTheCoordinatorsOffsetsOrNamesTheCoordinator()
    {
        var request = new FetchOffsetsRequest { ConsumerGroup = "orders-app", Partitions = { new GrpcTopicPartition { Topic = "orders", Partition = 0 } } };

        var here = await _service.FetchOffsets(request, new TestServerCallContext());
        Assert.Equal((42L, ErrorCode.None), (here.Results.Single().Offset, here.Results.Single().Status.ErrorCode));

        _locator.Coordinator = 3;
        var elsewhere = await _service.FetchOffsets(request, new TestServerCallContext());
        Assert.Equal(ErrorCode.NotCoordinator, elsewhere.Results.Single().Status.ErrorCode);
        Assert.Equal(3, elsewhere.Coordinator.BrokerId);
    }

    private static CommitRequest Commit() => new()
    {
        ConsumerGroup = "orders-app",
        Offsets =
        {
            new OffsetCommit { Topic = "orders", Partition = 0, Offset = 10 },
            new OffsetCommit { Topic = "orders", Partition = 1, Offset = 20 },
        },
    };

    // This broker is 1; the group's coordinator is whichever the test says.
    private sealed class Locator : ICoordinatorLocator
    {
        public int Coordinator { get; set; } = 1;

        public CoordinatorLocation Locate(CoordinatorKind kind, string key) =>
            new(CoordinatorLookupStatus.Ok, Coordinator, $"broker-{Coordinator}", 9092);

        public bool IsLocal(CoordinatorKind kind, string key) => Coordinator == 1;
    }

    // The group coordinator: answers every commit with the given status and serves offset 42.
    private sealed class RecordingCoordinator : IConsumerGroupCoordinator
    {
        public ConsumerGroupErrorStatus Answer { get; set; }

        public Group.OffsetCommitCommand? Committed { get; private set; }

        public Task<Group.OffsetCommitResult> CommitOffsetsAsync(Group.OffsetCommitCommand request, CancellationToken cancellationToken)
        {
            Committed = request;
            return Task.FromResult(new Group.OffsetCommitResult([.. request.Topics.Select(t => new Group.OffsetCommitTopicResult
            {
                Topic = t.Topic,
                TopicId = t.TopicId,
                Partitions = [.. t.Partitions.Select(p => new Group.OffsetCommitPartitionResult(p.PartitionIndex, Answer))],
            })]));
        }

        public Group.OffsetFetchResult FetchOffsets(Group.OffsetFetchCommand request) => new([.. request.Groups.Select(g => new Group.OffsetFetchGroupResult
        {
            GroupId = g.GroupId,
            Topics = [.. g.Topics!.Select(t => new Group.OffsetFetchTopicResult
            {
                Topic = t.Topic,
                TopicId = t.TopicId,
                Partitions = [.. t.PartitionIndexes.Select(p => new Group.OffsetFetchPartitionResult(p, 42, ConsumerGroupErrorStatus.None))],
            })],
        })]);

        public Task<Group.JoinGroupResult> JoinGroupAsync(Group.JoinGroupCommand request, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task<Group.SyncGroupResult> SyncGroupAsync(Group.SyncGroupCommand request, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Group.GroupHeartbeatResult Heartbeat(Group.GroupHeartbeatCommand request) => throw new NotSupportedException();
        public Group.LeaveGroupResult LeaveGroup(Group.LeaveGroupCommand request) => throw new NotSupportedException();
        public IReadOnlyList<Group.GroupDescription> DescribeGroups(IReadOnlyList<string> groupIds) => throw new NotSupportedException();
        public IReadOnlyList<Group.GroupListing> ListGroups(IReadOnlyList<string>? statesFilter) => throw new NotSupportedException();
        public Task<IReadOnlyList<Group.DeleteGroupResult>> DeleteGroupsAsync(IReadOnlyList<string> groupIds, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task<Group.OffsetDeleteResult> DeleteOffsetsAsync(Group.OffsetDeleteCommand request, CancellationToken cancellationToken) => throw new NotSupportedException();
    }
}
