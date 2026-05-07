using HotChocolate.Subscriptions;
using Kuestenlogik.Surgewave.Api.GraphQL.Mutation;
using Kuestenlogik.Surgewave.Api.GraphQL.Query;
using Kuestenlogik.Surgewave.Api.GraphQL.Services;
using Kuestenlogik.Surgewave.Api.GraphQL.Subscription;
using Kuestenlogik.Surgewave.Api.GraphQL.Types;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Xunit;

namespace Kuestenlogik.Surgewave.Api.GraphQL.Tests;

public sealed class SurgewaveQueryExtendedTests
{
    private readonly IGraphQLBrokerService _service = Substitute.For<IGraphQLBrokerService>();
    private readonly SurgewaveQuery _query = new();

    [Fact]
    public async Task GetTopics_ReturnsEmptyList_WhenNoTopics()
    {
        _service.GetTopicsAsync(Arg.Any<CancellationToken>())
            .Returns(new List<TopicType>());

        var result = await _query.GetTopics(_service, CancellationToken.None);

        Assert.Empty(result);
    }

    [Fact]
    public async Task GetTopics_ReturnsAllTopicFields()
    {
        var created = DateTimeOffset.Parse("2026-01-01T00:00:00Z");
        var expected = new List<TopicType>
        {
            new()
            {
                Name = "events",
                PartitionCount = 6,
                ReplicationFactor = 3,
                MessageCount = 9999,
                IsMirror = true,
                IsReadOnly = true,
                CreatedAt = created,
            },
        };
        _service.GetTopicsAsync(Arg.Any<CancellationToken>()).Returns(expected);

        var result = await _query.GetTopics(_service, CancellationToken.None);

        Assert.Single(result);
        var topic = result[0];
        Assert.Equal("events", topic.Name);
        Assert.Equal(6, topic.PartitionCount);
        Assert.Equal(3, topic.ReplicationFactor);
        Assert.Equal(9999, topic.MessageCount);
        Assert.True(topic.IsMirror);
        Assert.True(topic.IsReadOnly);
        Assert.Equal(created, topic.CreatedAt);
    }

    [Fact]
    public async Task GetMessages_WithPartitionAndOffset_PassesParametersToService()
    {
        var expected = new List<MessageType>
        {
            new() { Topic = "orders", Partition = 2, Offset = 100, Timestamp = DateTimeOffset.UtcNow, Key = null, Value = "data" },
        };
        _service.GetMessagesAsync("orders", 2, 100L, 5, Arg.Any<CancellationToken>())
            .Returns(expected);

        var result = await _query.GetMessages(_service, "orders", partition: 2, offset: 100L, limit: 5, cancellationToken: CancellationToken.None);

        Assert.Single(result);
        Assert.Equal(2, result[0].Partition);
        Assert.Equal(100, result[0].Offset);
    }

    [Fact]
    public async Task GetMessages_DefaultLimit_IsPassedCorrectly()
    {
        _service.GetMessagesAsync(Arg.Any<string>(), Arg.Any<int?>(), Arg.Any<long?>(), 10, Arg.Any<CancellationToken>())
            .Returns(new List<MessageType>());

        await _query.GetMessages(_service, "test-topic", cancellationToken: CancellationToken.None);

        await _service.Received(1).GetMessagesAsync("test-topic", null, null, 10, Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task GetMessages_ReturnsEmptyList_WhenNoMessages()
    {
        _service.GetMessagesAsync(Arg.Any<string>(), Arg.Any<int?>(), Arg.Any<long?>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(new List<MessageType>());

        var result = await _query.GetMessages(_service, "empty-topic", cancellationToken: CancellationToken.None);

        Assert.Empty(result);
    }

    [Fact]
    public async Task GetConsumerGroups_ReturnsEmptyList_WhenNoGroups()
    {
        _service.GetConsumerGroupsAsync(Arg.Any<CancellationToken>())
            .Returns(new List<ConsumerGroupType>());

        var result = await _query.GetConsumerGroups(_service, CancellationToken.None);

        Assert.Empty(result);
    }

    [Fact]
    public async Task GetConsumerGroups_ReturnsAllFields()
    {
        var expected = new List<ConsumerGroupType>
        {
            new() { GroupId = "analytics-group", State = "Rebalancing", MemberCount = 5, ProtocolType = "consumer" },
        };
        _service.GetConsumerGroupsAsync(Arg.Any<CancellationToken>()).Returns(expected);

        var result = await _query.GetConsumerGroups(_service, CancellationToken.None);

        Assert.Single(result);
        Assert.Equal("analytics-group", result[0].GroupId);
        Assert.Equal("Rebalancing", result[0].State);
        Assert.Equal(5, result[0].MemberCount);
        Assert.Equal("consumer", result[0].ProtocolType);
    }

    [Fact]
    public async Task GetConsumerGroups_MultipleGroups_ReturnsAll()
    {
        var expected = new List<ConsumerGroupType>
        {
            new() { GroupId = "group-a", State = "Stable", MemberCount = 2 },
            new() { GroupId = "group-b", State = "Empty", MemberCount = 0 },
            new() { GroupId = "group-c", State = "Dead", MemberCount = 0 },
        };
        _service.GetConsumerGroupsAsync(Arg.Any<CancellationToken>()).Returns(expected);

        var result = await _query.GetConsumerGroups(_service, CancellationToken.None);

        Assert.Equal(3, result.Count);
    }

    [Fact]
    public async Task GetCluster_ReturnsAllFields()
    {
        var expected = new ClusterInfoType
        {
            BrokerId = 7,
            Host = "broker.surgewave.local",
            Port = 9093,
            TopicCount = 42,
            PartitionCount = 126,
        };
        _service.GetClusterInfoAsync(Arg.Any<CancellationToken>()).Returns(expected);

        var result = await _query.GetCluster(_service, CancellationToken.None);

        Assert.Equal(7, result.BrokerId);
        Assert.Equal("broker.surgewave.local", result.Host);
        Assert.Equal(9093, result.Port);
        Assert.Equal(42, result.TopicCount);
        Assert.Equal(126, result.PartitionCount);
    }

    [Fact]
    public async Task GetCluster_CancellationToken_IsForwarded()
    {
        using var cts = new CancellationTokenSource();
        var token = cts.Token;

        _service.GetClusterInfoAsync(token).Returns(new ClusterInfoType { Host = "h", BrokerId = 0, Port = 0, TopicCount = 0, PartitionCount = 0 });

        await _query.GetCluster(_service, token);

        await _service.Received(1).GetClusterInfoAsync(token);
    }
}

public sealed class SurgewaveMutationExtendedTests
{
    private readonly IGraphQLBrokerService _service = Substitute.For<IGraphQLBrokerService>();
    private readonly ITopicEventSender _eventSender = Substitute.For<ITopicEventSender>();
    private readonly SurgewaveMutation _mutation = new();

    [Fact]
    public async Task ProduceMessage_WithNullKey_Succeeds()
    {
        var expected = new MessageType
        {
            Topic = "events",
            Partition = 0,
            Offset = 0,
            Timestamp = DateTimeOffset.UtcNow,
            Key = null,
            Value = "payload",
        };
        _service.ProduceMessageAsync("events", null, "payload", 0, Arg.Any<CancellationToken>())
            .Returns(expected);

        var result = await _mutation.ProduceMessage(
            _service, _eventSender, "events", null, "payload", cancellationToken: CancellationToken.None);

        Assert.Null(result.Key);
        Assert.Equal("payload", result.Value);
    }

    [Fact]
    public async Task ProduceMessage_DefaultPartition_IsZero()
    {
        var expected = new MessageType
        {
            Topic = "test", Partition = 0, Offset = 1,
            Timestamp = DateTimeOffset.UtcNow, Key = "k", Value = "v",
        };
        _service.ProduceMessageAsync("test", "k", "v", 0, Arg.Any<CancellationToken>())
            .Returns(expected);

        await _mutation.ProduceMessage(_service, _eventSender, "test", "k", "v", cancellationToken: CancellationToken.None);

        await _service.Received(1).ProduceMessageAsync("test", "k", "v", 0, Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ProduceMessage_CustomPartition_IsForwarded()
    {
        var expected = new MessageType
        {
            Topic = "orders", Partition = 3, Offset = 50,
            Timestamp = DateTimeOffset.UtcNow, Key = "o1", Value = "{}",
        };
        _service.ProduceMessageAsync("orders", "o1", "{}", 3, Arg.Any<CancellationToken>())
            .Returns(expected);

        var result = await _mutation.ProduceMessage(
            _service, _eventSender, "orders", "o1", "{}", partition: 3, cancellationToken: CancellationToken.None);

        Assert.Equal(3, result.Partition);
        Assert.Equal(50, result.Offset);
    }

    [Fact]
    public async Task ProduceMessage_SendsEventToCorrectTopic()
    {
        var topic = "my-special-topic";
        var msg = new MessageType
        {
            Topic = topic, Partition = 0, Offset = 10,
            Timestamp = DateTimeOffset.UtcNow, Key = null, Value = "v",
        };
        _service.ProduceMessageAsync(topic, null, "v", 0, Arg.Any<CancellationToken>()).Returns(msg);

        await _mutation.ProduceMessage(_service, _eventSender, topic, null, "v", cancellationToken: CancellationToken.None);

        await _eventSender.Received(1).SendAsync(
            $"OnMessage_{topic}",
            Arg.Any<MessageType>(),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ProduceMessage_EventSenderReceivesCorrectMessage()
    {
        var msg = new MessageType
        {
            Topic = "signals", Partition = 1, Offset = 999,
            Timestamp = DateTimeOffset.UtcNow, Key = "sensor1", Value = "reading",
        };
        _service.ProduceMessageAsync("signals", "sensor1", "reading", 1, Arg.Any<CancellationToken>()).Returns(msg);

        var result = await _mutation.ProduceMessage(
            _service, _eventSender, "signals", "sensor1", "reading", partition: 1, cancellationToken: CancellationToken.None);

        Assert.Equal("signals", result.Topic);
        Assert.Equal(999, result.Offset);
        Assert.Equal("sensor1", result.Key);

        await _eventSender.Received(1).SendAsync(
            "OnMessage_signals",
            Arg.Is<MessageType>(m => m.Topic == "signals" && m.Offset == 999),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task CreateTopic_DefaultValues_AreCorrect()
    {
        var expected = new TopicType
        {
            Name = "new-topic", PartitionCount = 1, ReplicationFactor = 1, MessageCount = 0,
        };
        _service.CreateTopicAsync("new-topic", 1, 1, Arg.Any<CancellationToken>()).Returns(expected);

        var result = await _mutation.CreateTopic(_service, "new-topic", cancellationToken: CancellationToken.None);

        Assert.Equal("new-topic", result.Name);
        Assert.Equal(1, result.PartitionCount);
        Assert.Equal(1, result.ReplicationFactor);
        Assert.Equal(0, result.MessageCount);
    }

    [Fact]
    public async Task CreateTopic_CustomPartitions_IsForwarded()
    {
        var expected = new TopicType
        {
            Name = "high-throughput", PartitionCount = 32, ReplicationFactor = 3, MessageCount = 0,
        };
        _service.CreateTopicAsync("high-throughput", 32, 3, Arg.Any<CancellationToken>()).Returns(expected);

        var result = await _mutation.CreateTopic(
            _service, "high-throughput", partitions: 32, replicationFactor: 3, cancellationToken: CancellationToken.None);

        Assert.Equal("high-throughput", result.Name);
        Assert.Equal(32, result.PartitionCount);
        Assert.Equal(3, result.ReplicationFactor);

        await _service.Received(1).CreateTopicAsync("high-throughput", 32, 3, Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task CreateTopic_ReturnsTopicWithMetadata()
    {
        var createdAt = DateTimeOffset.Parse("2026-01-15T10:00:00Z");
        var expected = new TopicType
        {
            Name = "events-v2",
            PartitionCount = 8,
            ReplicationFactor = 2,
            MessageCount = 0,
            IsMirror = false,
            IsReadOnly = false,
            CreatedAt = createdAt,
        };
        _service.CreateTopicAsync("events-v2", 8, 2, Arg.Any<CancellationToken>()).Returns(expected);

        var result = await _mutation.CreateTopic(
            _service, "events-v2", partitions: 8, replicationFactor: 2, cancellationToken: CancellationToken.None);

        Assert.Equal("events-v2", result.Name);
        Assert.False(result.IsMirror);
        Assert.False(result.IsReadOnly);
        Assert.Equal(createdAt, result.CreatedAt);
    }
}

public sealed class SurgewaveSubscriptionTests
{
    [Fact]
    public void OnMessage_ReturnsEventMessage()
    {
        var subscription = new SurgewaveSubscription();
        var message = new MessageType
        {
            Topic = "stream-topic",
            Partition = 0,
            Offset = 42,
            Timestamp = DateTimeOffset.UtcNow,
            Key = "k1",
            Value = "v1",
        };

        var result = subscription.OnMessage(message, "stream-topic");

        Assert.Same(message, result);
    }

    [Fact]
    public void OnMessage_PassesThroughAllFields()
    {
        var subscription = new SurgewaveSubscription();
        var timestamp = DateTimeOffset.Parse("2026-03-21T12:00:00Z");
        var headers = new Dictionary<string, string> { ["x-trace"] = "abc123" };
        var message = new MessageType
        {
            Topic = "telemetry",
            Partition = 3,
            Offset = 1000,
            Timestamp = timestamp,
            Key = "device-7",
            Value = """{"temp":22.5}""",
            Headers = headers,
        };

        var result = subscription.OnMessage(message, "telemetry");

        Assert.Equal("telemetry", result.Topic);
        Assert.Equal(3, result.Partition);
        Assert.Equal(1000, result.Offset);
        Assert.Equal(timestamp, result.Timestamp);
        Assert.Equal("device-7", result.Key);
        Assert.Equal("""{"temp":22.5}""", result.Value);
        Assert.NotNull(result.Headers);
        Assert.Equal("abc123", result.Headers["x-trace"]);
    }

    [Fact]
    public void OnMessage_WithNullKey_StillReturns()
    {
        var subscription = new SurgewaveSubscription();
        var message = new MessageType
        {
            Topic = "log-events",
            Partition = 0,
            Offset = 5,
            Timestamp = DateTimeOffset.UtcNow,
            Key = null,
            Value = "log line",
        };

        var result = subscription.OnMessage(message, "log-events");

        Assert.Null(result.Key);
        Assert.Equal("log line", result.Value);
    }
}

public sealed class GraphQLTypeTests
{
    [Fact]
    public void ConsumerGroupType_DefaultProtocolType_IsNull()
    {
        var group = new ConsumerGroupType { GroupId = "g1", State = "Empty", MemberCount = 0 };

        Assert.Null(group.ProtocolType);
    }

    [Fact]
    public void ConsumerGroupType_AllStates()
    {
        var states = new[] { "Stable", "Empty", "Rebalancing", "Dead", "PreparingRebalance" };

        foreach (var state in states)
        {
            var group = new ConsumerGroupType { GroupId = "g", State = state, MemberCount = 0 };
            Assert.Equal(state, group.State);
        }
    }

    [Fact]
    public void MessageType_EmptyHeaders_IsNull()
    {
        var msg = new MessageType
        {
            Topic = "t", Partition = 0, Offset = 0,
            Timestamp = DateTimeOffset.UtcNow, Key = null, Value = null,
        };

        Assert.Null(msg.Headers);
    }

    [Fact]
    public void MessageType_MultipleHeaders()
    {
        var headers = new Dictionary<string, string>
        {
            ["content-type"] = "application/json",
            ["x-trace-id"] = "trace-001",
            ["x-correlation-id"] = "corr-999",
        };
        var msg = new MessageType
        {
            Topic = "t", Partition = 0, Offset = 0,
            Timestamp = DateTimeOffset.UtcNow, Key = null, Value = "v",
            Headers = headers,
        };

        Assert.Equal(3, msg.Headers!.Count);
        Assert.Equal("application/json", msg.Headers["content-type"]);
        Assert.Equal("trace-001", msg.Headers["x-trace-id"]);
    }

    [Fact]
    public void TopicType_ZeroMessageCount()
    {
        var topic = new TopicType
        {
            Name = "empty-topic", PartitionCount = 1, ReplicationFactor = 1, MessageCount = 0,
        };

        Assert.Equal(0, topic.MessageCount);
        Assert.False(topic.IsMirror);
        Assert.False(topic.IsReadOnly);
    }

    [Fact]
    public void ClusterInfoType_BrokerIdCanBeNonZero()
    {
        var info = new ClusterInfoType
        {
            BrokerId = 5, Host = "node5.cluster", Port = 9092, TopicCount = 10, PartitionCount = 30,
        };

        Assert.Equal(5, info.BrokerId);
        Assert.Equal("node5.cluster", info.Host);
    }

    [Fact]
    public void ClusterInfoType_HighPartitionCount()
    {
        var info = new ClusterInfoType
        {
            BrokerId = 0, Host = "localhost", Port = 9092,
            TopicCount = 1000, PartitionCount = 50000,
        };

        Assert.Equal(50000, info.PartitionCount);
        Assert.Equal(1000, info.TopicCount);
    }
}

public sealed class GraphQLBrokerServiceHolderTests
{
    [Fact]
    public void Instance_CanBeSetAndRead()
    {
        var original = GraphQLBrokerServiceHolder.Instance;
        try
        {
            var mock = Substitute.For<IGraphQLBrokerService>();
            GraphQLBrokerServiceHolder.Instance = mock;

            Assert.Same(mock, GraphQLBrokerServiceHolder.Instance);
        }
        finally
        {
            GraphQLBrokerServiceHolder.Instance = original;
        }
    }

    [Fact]
    public void Instance_CanBeSetToNull()
    {
        var original = GraphQLBrokerServiceHolder.Instance;
        try
        {
            GraphQLBrokerServiceHolder.Instance = null;

            Assert.Null(GraphQLBrokerServiceHolder.Instance);
        }
        finally
        {
            GraphQLBrokerServiceHolder.Instance = original;
        }
    }
}

public sealed class GroupInfoDtoTests
{
    [Fact]
    public void GroupInfoDto_AllProperties()
    {
        var dto = new GroupInfoDto("my-group", "Stable", 4, "consumer");

        Assert.Equal("my-group", dto.GroupId);
        Assert.Equal("Stable", dto.State);
        Assert.Equal(4, dto.MemberCount);
        Assert.Equal("consumer", dto.ProtocolType);
    }

    [Fact]
    public void GroupInfoDto_NullProtocolType_IsDefault()
    {
        var dto = new GroupInfoDto("g", "Empty", 0);

        Assert.Null(dto.ProtocolType);
    }
}

public sealed class ClusterInfoDtoTests
{
    [Fact]
    public void ClusterInfoDto_AllProperties()
    {
        var dto = new ClusterInfoDto(1, "my-host", 9092, 10, 30);

        Assert.Equal(1, dto.BrokerId);
        Assert.Equal("my-host", dto.Host);
        Assert.Equal(9092, dto.Port);
        Assert.Equal(10, dto.TopicCount);
        Assert.Equal(30, dto.PartitionCount);
    }

    [Fact]
    public void ClusterInfoDto_RecordEquality()
    {
        var dto1 = new ClusterInfoDto(0, "localhost", 9092, 5, 15);
        var dto2 = new ClusterInfoDto(0, "localhost", 9092, 5, 15);

        Assert.Equal(dto1, dto2);
    }
}
