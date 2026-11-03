using Kuestenlogik.Surgewave.Coordination.Consumer;
using Kuestenlogik.Surgewave.Coordination.Coordinators;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Kafka.Tests;

/// <summary>
/// A broker answers group requests only for the groups it coordinates (#204); another broker's group gets
/// NOT_COORDINATOR, and the client asks FindCoordinator again. Before, every broker served every group, so the members
/// of one group could join different brokers' copies of it.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class GroupCoordinatorOwnershipTests
{
    private const string Mine = "group-here";
    private const string Theirs = "group-elsewhere";

    private static readonly RequestContext Context = new()
    {
        ConnectionState = new ConnectionState("127.0.0.1"),
        ClientId = "client",
    };

    private readonly IConsumerGroupCoordinator _classic = Substitute.For<IConsumerGroupCoordinator>();
    private readonly IConsumerGroupV2Coordinator _consumerGroups = Substitute.For<IConsumerGroupV2Coordinator>();
    private readonly ConsumerGroupApiHandler _handler;

    public GroupCoordinatorOwnershipTests()
    {
        var locator = Substitute.For<ICoordinatorLocator>();
        locator.IsLocal(CoordinatorKind.Group, Mine).Returns(true);
        locator.IsLocal(CoordinatorKind.Group, Theirs).Returns(false);

        _handler = new ConsumerGroupApiHandler(_classic, _consumerGroups, NullLogger<ConsumerGroupApiHandler>.Instance);
        _handler.SetCoordinatorLocator(locator);
    }

    [Fact]
    public async Task AJoinForAnotherBrokersGroupIsNotCoordinatorAndNeverReachesTheCoordinator()
    {
        var response = Assert.IsType<JoinGroupResponse>(await _handler.HandleAsync(new JoinGroupRequest
        {
            ApiKey = ApiKey.JoinGroup,
            ApiVersion = 9,
            CorrelationId = 1,
            ClientId = "client",
            GroupId = Theirs,
            SessionTimeoutMs = 45_000,
            RebalanceTimeoutMs = 300_000,
            MemberId = string.Empty,
            ProtocolType = "consumer",
            Protocols = [],
        }, Context, CancellationToken.None));

        Assert.Equal(ErrorCode.NotCoordinator, response.ErrorCode);
        _classic.DidNotReceiveWithAnyArgs().JoinGroup(default!);
    }

    [Fact]
    public async Task AnOffsetCommitForAnotherBrokersGroupIsNotCoordinatorOnEveryPartition()
    {
        var response = Assert.IsType<OffsetCommitResponse>(await _handler.HandleAsync(new OffsetCommitRequest
        {
            ApiKey = ApiKey.OffsetCommit,
            ApiVersion = 8,
            CorrelationId = 2,
            ClientId = "client",
            GroupId = Theirs,
            Topics =
            [
                new TopicPartitionCommit
                {
                    Topic = "orders",
                    Partitions = [new PartitionCommit { PartitionIndex = 0, CommittedOffset = 5 }, new PartitionCommit { PartitionIndex = 1, CommittedOffset = 7 }],
                },
            ],
        }, Context, CancellationToken.None));

        var partitions = Assert.Single(response.Topics).Partitions;
        Assert.Equal(2, partitions.Count);
        Assert.All(partitions, p => Assert.Equal(ErrorCode.NotCoordinator, p.ErrorCode));
        _classic.DidNotReceiveWithAnyArgs().CommitOffsets(default!);
    }

    [Fact]
    public async Task DescribeGroupsAnswersItsOwnGroupsAndNotCoordinatorForTheRestInTheOrderAsked()
    {
        _classic.DescribeGroups(Arg.Any<IReadOnlyList<string>>()).Returns(
        [
            new GroupDescription
            {
                Status = ConsumerGroupErrorStatus.None,
                GroupId = Mine,
                GroupState = "Stable",
                ProtocolType = "consumer",
                ProtocolData = "range",
                Members = [],
            },
        ]);

        var response = Assert.IsType<DescribeGroupsResponse>(await _handler.HandleAsync(new DescribeGroupsRequest
        {
            ApiKey = ApiKey.DescribeGroups,
            ApiVersion = 5,
            CorrelationId = 3,
            ClientId = "client",
            GroupIds = [Theirs, Mine],
        }, Context, CancellationToken.None));

        Assert.Collection(response.Groups,
            g => { Assert.Equal(Theirs, g.GroupId); Assert.Equal(ErrorCode.NotCoordinator, g.ErrorCode); },
            g => { Assert.Equal(Mine, g.GroupId); Assert.Equal(ErrorCode.None, g.ErrorCode); Assert.Equal("Stable", g.GroupState); });
        _classic.Received(1).DescribeGroups(Arg.Is<IReadOnlyList<string>>(ids => ids.Count == 1 && ids[0] == Mine));
    }

    [Fact]
    public async Task AKip848HeartbeatForAnotherBrokersGroupIsNotCoordinator()
    {
        var locator = Substitute.For<ICoordinatorLocator>();
        locator.IsLocal(CoordinatorKind.Group, Theirs).Returns(false);
        var handler = new ConsumerGroupV2ApiHandler(_consumerGroups, _ => null, NullLogger<ConsumerGroupV2ApiHandler>.Instance);
        handler.SetCoordinatorLocator(locator);

        var response = Assert.IsType<ConsumerGroupHeartbeatResponse>(await handler.HandleAsync(new ConsumerGroupHeartbeatRequest
        {
            ApiKey = ApiKey.ConsumerGroupHeartbeat,
            ApiVersion = 1,
            CorrelationId = 4,
            ClientId = "client",
            GroupId = Theirs,
            MemberId = "m1",
            MemberEpoch = 0,
        }, Context, CancellationToken.None));

        Assert.Equal(ErrorCode.NotCoordinator, response.ErrorCode);
        _consumerGroups.DidNotReceiveWithAnyArgs().Heartbeat(default!);
    }
}
