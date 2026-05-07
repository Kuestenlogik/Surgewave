using Kuestenlogik.Surgewave.Gateway.WebSocket.Protocol;
using Xunit;

namespace Kuestenlogik.Surgewave.Gateway.Tests.WebSocket;

public class WebSocketMessageTests
{
    [Fact]
    public void WebSocketMessage_Type_IsRequired()
    {
        // Arrange & Act
        var message = new WebSocketMessage { Type = "test" };

        // Assert
        Assert.Equal("test", message.Type);
    }

    [Fact]
    public void WebSocketMessage_Id_DefaultsToNull()
    {
        // Arrange & Act
        var message = new WebSocketMessage { Type = "test" };

        // Assert
        Assert.Null(message.Id);
    }

    [Fact]
    public void WebSocketMessage_ClusterId_DefaultsToNull()
    {
        // Arrange & Act
        var message = new WebSocketMessage { Type = "test" };

        // Assert
        Assert.Null(message.ClusterId);
    }

    [Fact]
    public void WebSocketMessage_CanSetAllProperties()
    {
        // Arrange & Act
        var message = new WebSocketMessage
        {
            Type = "subscribe",
            Id = "req-001",
            ClusterId = "surgewave-cluster"
        };

        // Assert
        Assert.Equal("subscribe", message.Type);
        Assert.Equal("req-001", message.Id);
        Assert.Equal("surgewave-cluster", message.ClusterId);
    }

    [Fact]
    public void WebSocketMessageGeneric_Payload_CanBeSet()
    {
        // Arrange & Act
        var message = new WebSocketMessage<SubscribePayload>
        {
            Type = "subscribe",
            Id = "req-001",
            Payload = new SubscribePayload { Topic = "my-topic" }
        };

        // Assert
        Assert.Equal("subscribe", message.Type);
        Assert.NotNull(message.Payload);
        Assert.Equal("my-topic", message.Payload.Topic);
    }

    [Fact]
    public void WebSocketMessageGeneric_Payload_DefaultsToNull()
    {
        // Arrange & Act
        var message = new WebSocketMessage<SubscribePayload>
        {
            Type = "subscribe"
        };

        // Assert
        Assert.Null(message.Payload);
    }
}

public class WebSocketMessageTypeTests
{
    [Theory]
    [InlineData("subscribe", WebSocketMessageType.Subscribe)]
    [InlineData("unsubscribe", WebSocketMessageType.Unsubscribe)]
    [InlineData("produce", WebSocketMessageType.Produce)]
    [InlineData("produce_batch", WebSocketMessageType.ProduceBatch)]
    [InlineData("commit", WebSocketMessageType.Commit)]
    [InlineData("admin", WebSocketMessageType.Admin)]
    [InlineData("ping", WebSocketMessageType.Ping)]
    public void ClientMessageTypes_HaveCorrectValues(string expected, string actual)
    {
        Assert.Equal(expected, actual);
    }

    [Theory]
    [InlineData("subscribe_response", WebSocketMessageType.SubscribeResponse)]
    [InlineData("unsubscribe_response", WebSocketMessageType.UnsubscribeResponse)]
    [InlineData("message", WebSocketMessageType.Message)]
    [InlineData("message_batch", WebSocketMessageType.MessageBatch)]
    [InlineData("produce_response", WebSocketMessageType.ProduceResponse)]
    [InlineData("produce_batch_response", WebSocketMessageType.ProduceBatchResponse)]
    [InlineData("commit_response", WebSocketMessageType.CommitResponse)]
    [InlineData("admin_response", WebSocketMessageType.AdminResponse)]
    [InlineData("admin_event", WebSocketMessageType.AdminEvent)]
    [InlineData("error", WebSocketMessageType.Error)]
    [InlineData("pong", WebSocketMessageType.Pong)]
    public void ServerMessageTypes_HaveCorrectValues(string expected, string actual)
    {
        Assert.Equal(expected, actual);
    }
}

public class AdminActionTypeTests
{
    [Theory]
    [InlineData("list_topics", AdminActionType.ListTopics)]
    [InlineData("describe_topic", AdminActionType.DescribeTopic)]
    [InlineData("list_consumer_groups", AdminActionType.ListConsumerGroups)]
    [InlineData("describe_consumer_group", AdminActionType.DescribeConsumerGroup)]
    [InlineData("get_cluster_info", AdminActionType.GetClusterInfo)]
    public void AdminActionTypes_HaveCorrectValues(string expected, string actual)
    {
        Assert.Equal(expected, actual);
    }
}

public class AdminEventTypeTests
{
    [Theory]
    [InlineData("topic_created", AdminEventType.TopicCreated)]
    [InlineData("topic_deleted", AdminEventType.TopicDeleted)]
    [InlineData("partition_added", AdminEventType.PartitionAdded)]
    [InlineData("consumer_group_rebalanced", AdminEventType.ConsumerGroupRebalanced)]
    [InlineData("connection_established", AdminEventType.ConnectionEstablished)]
    [InlineData("connection_closed", AdminEventType.ConnectionClosed)]
    public void AdminEventTypes_HaveCorrectValues(string expected, string actual)
    {
        Assert.Equal(expected, actual);
    }
}

public class WebSocketErrorCodeTests
{
    [Theory]
    [InlineData("INVALID_MESSAGE", WebSocketErrorCode.InvalidMessage)]
    [InlineData("UNKNOWN_CLUSTER", WebSocketErrorCode.UnknownCluster)]
    [InlineData("UNKNOWN_TOPIC", WebSocketErrorCode.UnknownTopic)]
    [InlineData("SUBSCRIBE_ERROR", WebSocketErrorCode.SubscribeError)]
    [InlineData("SUBSCRIPTION_NOT_FOUND", WebSocketErrorCode.SubscriptionNotFound)]
    [InlineData("MAX_SUBSCRIPTIONS_EXCEEDED", WebSocketErrorCode.MaxSubscriptionsExceeded)]
    [InlineData("PRODUCE_FAILED", WebSocketErrorCode.ProduceFailed)]
    [InlineData("COMMIT_FAILED", WebSocketErrorCode.CommitFailed)]
    [InlineData("UNAUTHORIZED", WebSocketErrorCode.Unauthorized)]
    [InlineData("INTERNAL_ERROR", WebSocketErrorCode.InternalError)]
    public void ErrorCodes_HaveCorrectValues(string expected, string actual)
    {
        Assert.Equal(expected, actual);
    }
}
