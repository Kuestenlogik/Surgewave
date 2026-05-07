using Kuestenlogik.Surgewave.Api.GraphQL.Types;
using Kuestenlogik.Surgewave.Api.GraphQL.Services;
using Kuestenlogik.Surgewave.Api.GraphQL.Query;
using Kuestenlogik.Surgewave.Api.GraphQL.Mutation;
using NSubstitute;
using Xunit;

namespace Kuestenlogik.Surgewave.Api.GraphQL.Tests;

public sealed class SurgewaveQueryTests
{
    private readonly IGraphQLBrokerService _service = Substitute.For<IGraphQLBrokerService>();

    [Fact]
    public async Task GetTopics_ReturnsTopics()
    {
        // Arrange
        var expected = new List<TopicType>
        {
            new() { Name = "orders", PartitionCount = 3, ReplicationFactor = 1, MessageCount = 100 },
            new() { Name = "events", PartitionCount = 1, ReplicationFactor = 1, MessageCount = 50 },
        };
        _service.GetTopicsAsync(Arg.Any<CancellationToken>()).Returns(expected);

        var query = new SurgewaveQuery();

        // Act
        var result = await query.GetTopics(_service, CancellationToken.None);

        // Assert
        Assert.Equal(2, result.Count);
        Assert.Equal("orders", result[0].Name);
        Assert.Equal(3, result[0].PartitionCount);
        Assert.Equal(100, result[0].MessageCount);
        Assert.Equal("events", result[1].Name);
    }

    [Fact]
    public async Task GetMessages_ReturnsMessages()
    {
        // Arrange
        var expected = new List<MessageType>
        {
            new()
            {
                Topic = "test-topic",
                Partition = 0,
                Offset = 0,
                Timestamp = DateTimeOffset.UtcNow,
                Key = "key1",
                Value = "value1",
            },
            new()
            {
                Topic = "test-topic",
                Partition = 0,
                Offset = 1,
                Timestamp = DateTimeOffset.UtcNow,
                Key = null,
                Value = "value2",
            },
        };
        _service.GetMessagesAsync("test-topic", null, null, 10, Arg.Any<CancellationToken>())
            .Returns(expected);

        var query = new SurgewaveQuery();

        // Act
        var result = await query.GetMessages(_service, "test-topic", cancellationToken: CancellationToken.None);

        // Assert
        Assert.Equal(2, result.Count);
        Assert.Equal("key1", result[0].Key);
        Assert.Equal("value2", result[1].Value);
    }

    [Fact]
    public async Task GetConsumerGroups_ReturnsGroups()
    {
        // Arrange
        var expected = new List<ConsumerGroupType>
        {
            new() { GroupId = "my-group", State = "Stable", MemberCount = 3 },
        };
        _service.GetConsumerGroupsAsync(Arg.Any<CancellationToken>()).Returns(expected);

        var query = new SurgewaveQuery();

        // Act
        var result = await query.GetConsumerGroups(_service, CancellationToken.None);

        // Assert
        Assert.Single(result);
        Assert.Equal("my-group", result[0].GroupId);
        Assert.Equal("Stable", result[0].State);
        Assert.Equal(3, result[0].MemberCount);
    }

    [Fact]
    public async Task GetCluster_ReturnsClusterInfo()
    {
        // Arrange
        var expected = new ClusterInfoType
        {
            BrokerId = 0,
            Host = "localhost",
            Port = 9092,
            TopicCount = 5,
            PartitionCount = 15,
        };
        _service.GetClusterInfoAsync(Arg.Any<CancellationToken>()).Returns(expected);

        var query = new SurgewaveQuery();

        // Act
        var result = await query.GetCluster(_service, CancellationToken.None);

        // Assert
        Assert.Equal(0, result.BrokerId);
        Assert.Equal("localhost", result.Host);
        Assert.Equal(5, result.TopicCount);
        Assert.Equal(15, result.PartitionCount);
    }
}

public sealed class SurgewaveMutationTests
{
    private readonly IGraphQLBrokerService _service = Substitute.For<IGraphQLBrokerService>();

    [Fact]
    public async Task ProduceMessage_Succeeds()
    {
        // Arrange
        var expected = new MessageType
        {
            Topic = "test-topic",
            Partition = 0,
            Offset = 42,
            Timestamp = DateTimeOffset.UtcNow,
            Key = "key1",
            Value = "hello",
        };
        _service.ProduceMessageAsync("test-topic", "key1", "hello", 0, Arg.Any<CancellationToken>())
            .Returns(expected);

        var eventSender = Substitute.For<HotChocolate.Subscriptions.ITopicEventSender>();
        var mutation = new SurgewaveMutation();

        // Act
        var result = await mutation.ProduceMessage(
            _service, eventSender, "test-topic", "key1", "hello", cancellationToken: CancellationToken.None);

        // Assert
        Assert.Equal("test-topic", result.Topic);
        Assert.Equal(42, result.Offset);
        Assert.Equal("hello", result.Value);

        // Verify subscription event was sent
        await eventSender.Received(1).SendAsync(
            "OnMessage_test-topic",
            Arg.Any<MessageType>(),
            Arg.Any<CancellationToken>());
    }
}

public sealed class TypeSerializationTests
{
    [Fact]
    public void MessageType_Serialization()
    {
        var msg = new MessageType
        {
            Topic = "test",
            Partition = 0,
            Offset = 5,
            Timestamp = DateTimeOffset.Parse("2026-01-01T00:00:00Z"),
            Key = "k",
            Value = "v",
            Headers = new Dictionary<string, string> { ["h1"] = "v1" },
        };

        Assert.Equal("test", msg.Topic);
        Assert.Equal(0, msg.Partition);
        Assert.Equal(5, msg.Offset);
        Assert.Equal("k", msg.Key);
        Assert.Equal("v", msg.Value);
        Assert.Single(msg.Headers);
        Assert.Equal("v1", msg.Headers["h1"]);
    }

    [Fact]
    public void TopicType_Serialization()
    {
        var topic = new TopicType
        {
            Name = "my-topic",
            PartitionCount = 4,
            ReplicationFactor = 3,
            MessageCount = 1000,
            IsMirror = false,
            IsReadOnly = false,
            CreatedAt = DateTimeOffset.Parse("2026-03-01T12:00:00Z"),
        };

        Assert.Equal("my-topic", topic.Name);
        Assert.Equal(4, topic.PartitionCount);
        Assert.Equal(3, topic.ReplicationFactor);
        Assert.Equal(1000, topic.MessageCount);
        Assert.False(topic.IsMirror);
    }
}

public sealed class GraphQLConfigTests
{
    [Fact]
    public void GraphQLConfig_Defaults()
    {
        var config = new GraphQLConfig();

        Assert.False(config.Enabled);
        Assert.Equal("/graphql", config.Path);
        Assert.Equal("Surgewave:GraphQL", GraphQLConfig.SectionName);
    }

    [Fact]
    public void GraphQLConfig_CustomValues()
    {
        var config = new GraphQLConfig
        {
            Enabled = true,
            Path = "/api/graphql",
        };

        Assert.True(config.Enabled);
        Assert.Equal("/api/graphql", config.Path);
    }
}
