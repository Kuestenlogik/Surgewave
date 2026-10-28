using Kuestenlogik.Surgewave.Coordination.Consumer;
using Kuestenlogik.Surgewave.Protocol.Kafka.Handlers;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Kafka.Tests;

/// <summary>
/// The group admin calls as a Java 4.x admin client makes them (#195): `kafka-consumer-groups --list` lists
/// classic and KIP-848 groups in one ListGroups, and `--describe` of a classic group first asks
/// ConsumerGroupDescribe and falls back to DescribeGroups only on GROUP_ID_NOT_FOUND.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class GroupAdminForJavaClientTests
{
    private static readonly RequestContext Context = new()
    {
        ConnectionState = new ConnectionState("127.0.0.1"),
        ClientId = "adminclient-1",
    };

    [Fact]
    public async Task ListGroupsListsClassicAndConsumerGroupsWithTheirType()
    {
        var response = await ListGroups(typesFilter: []);

        Assert.Collection(response.Groups,
            g => { Assert.Equal("g-classic", g.GroupId); Assert.Equal("classic", g.GroupType); },
            g => { Assert.Equal("g-consumer", g.GroupId); Assert.Equal("consumer", g.GroupType); Assert.Equal("consumer", g.ProtocolType); });
    }

    [Fact]
    public async Task ListGroupsTypeFilterNarrowsToTheNamedTypes()
    {
        var response = await ListGroups(typesFilter: ["consumer"]);

        var group = Assert.Single(response.Groups);
        Assert.Equal("g-consumer", group.GroupId);
    }

    [Fact]
    public async Task ConsumerGroupDescribeOfAGroupItDoesNotHoldIsGroupIdNotFound()
    {
        var consumerGroups = Substitute.For<IConsumerGroupV2Coordinator>();
        consumerGroups.Describe(Arg.Any<IReadOnlyList<string>>()).Returns(
            [new ConsumerGroupDescription { GroupId = "g-classic", Status = ConsumerGroupDescribeStatus.GroupNotFound }]);
        var handler = new ConsumerGroupV2ApiHandler(consumerGroups, _ => null, NullLogger<ConsumerGroupV2ApiHandler>.Instance);

        var response = Assert.IsType<ConsumerGroupDescribeResponse>(await handler.HandleAsync(new ConsumerGroupDescribeRequest
        {
            ApiKey = ApiKey.ConsumerGroupDescribe,
            ApiVersion = 1,
            CorrelationId = 7,
            ClientId = "adminclient-1",
            GroupIds = ["g-classic"],
        }, Context, CancellationToken.None));

        var group = Assert.Single(response.Groups);
        Assert.Equal(ErrorCode.GroupIdNotFound, group.ErrorCode);
    }

    private static async Task<ListGroupsResponse> ListGroups(List<string> typesFilter)
    {
        var classic = Substitute.For<IConsumerGroupCoordinator>();
        classic.ListGroups(Arg.Any<IReadOnlyList<string>?>()).Returns(
            [new GroupListing("g-classic", "consumer", "Empty", "classic")]);
        var consumerGroups = Substitute.For<IConsumerGroupV2Coordinator>();
        consumerGroups.ListGroups(Arg.Any<IReadOnlyList<string>?>()).Returns(
            [new GroupListing("g-consumer", "consumer", "Stable", "consumer")]);
        var handler = new ConsumerGroupApiHandler(classic, consumerGroups, NullLogger<ConsumerGroupApiHandler>.Instance);

        return Assert.IsType<ListGroupsResponse>(await handler.HandleAsync(new ListGroupsRequest
        {
            ApiKey = ApiKey.ListGroups,
            ApiVersion = 5,
            CorrelationId = 3,
            ClientId = "adminclient-1",
            TypesFilter = typesFilter,
        }, Context, CancellationToken.None));
    }
}
