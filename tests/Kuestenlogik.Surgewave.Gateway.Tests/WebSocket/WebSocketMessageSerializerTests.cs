using System.Text;
using Kuestenlogik.Surgewave.Gateway.WebSocket.Protocol;
using Xunit;

namespace Kuestenlogik.Surgewave.Gateway.Tests.WebSocket;

public class WebSocketMessageSerializerTests
{
    [Fact]
    public void DeserializeBase_ValidJson_ReturnsMessage()
    {
        // Arrange
        var json = """{"type":"subscribe","id":"req-001"}""";
        var bytes = Encoding.UTF8.GetBytes(json);

        // Act
        var message = WebSocketMessageSerializer.DeserializeBase(bytes);

        // Assert
        Assert.NotNull(message);
        Assert.Equal("subscribe", message.Type);
        Assert.Equal("req-001", message.Id);
    }

    [Fact]
    public void DeserializeBase_WithClusterId_ReturnsClusterId()
    {
        // Arrange
        var json = """{"type":"subscribe","id":"req-001","cluster_id":"surgewave-cluster"}""";
        var bytes = Encoding.UTF8.GetBytes(json);

        // Act
        var message = WebSocketMessageSerializer.DeserializeBase(bytes);

        // Assert
        Assert.NotNull(message);
        Assert.Equal("surgewave-cluster", message.ClusterId);
    }

    [Fact]
    public void Deserialize_SubscribePayload_ReturnsTypedMessage()
    {
        // Arrange
        var json = """
        {
            "type": "subscribe",
            "id": "req-001",
            "payload": {
                "topic": "my-topic",
                "partitions": [0, 1, 2],
                "from_offset": "latest",
                "consumer_group": "my-group"
            }
        }
        """;
        var bytes = Encoding.UTF8.GetBytes(json);

        // Act
        var message = WebSocketMessageSerializer.Deserialize<SubscribePayload>(bytes);

        // Assert
        Assert.NotNull(message);
        Assert.Equal("subscribe", message.Type);
        Assert.NotNull(message.Payload);
        Assert.Equal("my-topic", message.Payload.Topic);
        Assert.Equal([0, 1, 2], message.Payload.Partitions!);
        Assert.Equal("latest", message.Payload.FromOffset);
        Assert.Equal("my-group", message.Payload.ConsumerGroup);
    }

    [Fact]
    public void Deserialize_ProducePayload_ReturnsTypedMessage()
    {
        // Arrange
        var json = """
        {
            "type": "produce",
            "id": "req-002",
            "payload": {
                "topic": "my-topic",
                "partition": 0,
                "key": "bXkta2V5",
                "value": "bXktdmFsdWU="
            }
        }
        """;
        var bytes = Encoding.UTF8.GetBytes(json);

        // Act
        var message = WebSocketMessageSerializer.Deserialize<ProducePayload>(bytes);

        // Assert
        Assert.NotNull(message);
        Assert.Equal("produce", message.Type);
        Assert.NotNull(message.Payload);
        Assert.Equal("my-topic", message.Payload.Topic);
        Assert.Equal(0, message.Payload.Partition);
        Assert.Equal("bXkta2V5", message.Payload.Key);
        Assert.Equal("bXktdmFsdWU=", message.Payload.Value);
    }

    [Fact]
    public void Deserialize_AdminPayload_ReturnsTypedMessage()
    {
        // Arrange
        var json = """
        {
            "type": "admin",
            "id": "req-003",
            "payload": {
                "action": "list_topics",
                "include_internal": true
            }
        }
        """;
        var bytes = Encoding.UTF8.GetBytes(json);

        // Act
        var message = WebSocketMessageSerializer.Deserialize<AdminPayload>(bytes);

        // Assert
        Assert.NotNull(message);
        Assert.Equal("admin", message.Type);
        Assert.NotNull(message.Payload);
        Assert.Equal("list_topics", message.Payload.Action);
        Assert.True(message.Payload.IncludeInternal);
    }

    [Fact]
    public void Serialize_SubscribeResponse_ReturnsValidJson()
    {
        // Arrange
        var message = WebSocketMessageSerializer.CreateSubscribeResponse(
            requestId: "req-001",
            success: true,
            subscriptionId: "sub-abc123",
            topic: "my-topic",
            partitions: [0, 1]);

        // Act
        var bytes = WebSocketMessageSerializer.Serialize(message);
        var json = Encoding.UTF8.GetString(bytes);

        // Assert
        Assert.Contains("\"type\":\"subscribe_response\"", json);
        Assert.Contains("\"id\":\"req-001\"", json);
        Assert.Contains("\"success\":true", json);
        Assert.Contains("\"subscription_id\":\"sub-abc123\"", json);
        Assert.Contains("\"topic\":\"my-topic\"", json);
    }

    [Fact]
    public void Serialize_Message_ReturnsValidJson()
    {
        // Arrange
        var message = WebSocketMessageSerializer.CreateMessage(
            subscriptionId: "sub-abc",
            topic: "my-topic",
            partition: 0,
            offset: 1234,
            timestamp: 1700000000000,
            key: "my-key",
            value: "my-value");

        // Act
        var bytes = WebSocketMessageSerializer.Serialize(message);
        var json = Encoding.UTF8.GetString(bytes);

        // Assert
        Assert.Contains("\"type\":\"message\"", json);
        Assert.Contains("\"subscription_id\":\"sub-abc\"", json);
        Assert.Contains("\"topic\":\"my-topic\"", json);
        Assert.Contains("\"partition\":0", json);
        Assert.Contains("\"offset\":1234", json);
        Assert.Contains("\"key\":\"my-key\"", json);
        Assert.Contains("\"value\":\"my-value\"", json);
    }

    [Fact]
    public void Serialize_MessageBatch_ReturnsValidJson()
    {
        // Arrange
        var records = new[]
        {
            new MessageBatchRecord { Offset = 100, Timestamp = 1700000000000, Key = "key1", Value = "value1" },
            new MessageBatchRecord { Offset = 101, Timestamp = 1700000000001, Key = "key2", Value = "value2" }
        };

        var message = WebSocketMessageSerializer.CreateMessageBatch(
            subscriptionId: "sub-abc",
            topic: "my-topic",
            partition: 0,
            highWatermark: 200,
            records: records);

        // Act
        var bytes = WebSocketMessageSerializer.Serialize(message);
        var json = Encoding.UTF8.GetString(bytes);

        // Assert
        Assert.Contains("\"type\":\"message_batch\"", json);
        Assert.Contains("\"subscription_id\":\"sub-abc\"", json);
        Assert.Contains("\"high_watermark\":200", json);
        Assert.Contains("\"records\":", json);
    }

    [Fact]
    public void Serialize_ProduceResponse_ReturnsValidJson()
    {
        // Arrange
        var message = WebSocketMessageSerializer.CreateProduceResponse(
            requestId: "req-002",
            success: true,
            topic: "my-topic",
            partition: 0,
            offset: 5678,
            timestamp: 1700000000000);

        // Act
        var bytes = WebSocketMessageSerializer.Serialize(message);
        var json = Encoding.UTF8.GetString(bytes);

        // Assert
        Assert.Contains("\"type\":\"produce_response\"", json);
        Assert.Contains("\"id\":\"req-002\"", json);
        Assert.Contains("\"success\":true", json);
        Assert.Contains("\"offset\":5678", json);
    }

    [Fact]
    public void Serialize_Error_ReturnsValidJson()
    {
        // Arrange
        var message = WebSocketMessageSerializer.CreateError(
            requestId: "req-003",
            code: "UNKNOWN_TOPIC",
            message: "Topic does not exist");

        // Act
        var bytes = WebSocketMessageSerializer.Serialize(message);
        var json = Encoding.UTF8.GetString(bytes);

        // Assert
        Assert.Contains("\"type\":\"error\"", json);
        Assert.Contains("\"id\":\"req-003\"", json);
        Assert.Contains("\"code\":\"UNKNOWN_TOPIC\"", json);
        Assert.Contains("\"message\":\"Topic does not exist\"", json);
    }

    [Fact]
    public void Serialize_Pong_ReturnsValidJson()
    {
        // Arrange
        var message = WebSocketMessageSerializer.CreatePong("req-004");

        // Act
        var bytes = WebSocketMessageSerializer.Serialize(message);
        var json = Encoding.UTF8.GetString(bytes);

        // Assert
        Assert.Contains("\"type\":\"pong\"", json);
        Assert.Contains("\"id\":\"req-004\"", json);
        Assert.Contains("\"timestamp\":", json);
    }

    [Fact]
    public void Serialize_AdminEvent_ReturnsValidJson()
    {
        // Arrange
        var message = WebSocketMessageSerializer.CreateAdminEvent(
            eventType: "topic_created",
            clusterId: "surgewave-cluster",
            data: new { topic = "new-topic", partitions = 3 });

        // Act
        var bytes = WebSocketMessageSerializer.Serialize(message);
        var json = Encoding.UTF8.GetString(bytes);

        // Assert
        Assert.Contains("\"type\":\"admin_event\"", json);
        Assert.Contains("\"event_type\":\"topic_created\"", json);
        Assert.Contains("\"cluster_id\":\"surgewave-cluster\"", json);
    }

    [Fact]
    public void SerializeToString_ReturnsJsonString()
    {
        // Arrange
        var message = WebSocketMessageSerializer.CreatePong("req-005");

        // Act
        var json = WebSocketMessageSerializer.SerializeToString(message);

        // Assert
        Assert.Contains("\"type\":\"pong\"", json);
        Assert.Contains("\"id\":\"req-005\"", json);
    }

    [Fact]
    public void Deserialize_WithSnakeCaseNaming_ParsesCorrectly()
    {
        // Arrange - using snake_case property names
        var json = """
        {
            "type": "subscribe",
            "id": "req-001",
            "payload": {
                "topic": "test",
                "from_offset": "earliest",
                "consumer_group": "test-group",
                "max_batch_size": 50,
                "max_wait_ms": 500
            }
        }
        """;
        var bytes = Encoding.UTF8.GetBytes(json);

        // Act
        var message = WebSocketMessageSerializer.Deserialize<SubscribePayload>(bytes);

        // Assert
        Assert.NotNull(message?.Payload);
        Assert.Equal("earliest", message.Payload.FromOffset);
        Assert.Equal("test-group", message.Payload.ConsumerGroup);
        Assert.Equal(50, message.Payload.MaxBatchSize);
        Assert.Equal(500, message.Payload.MaxWaitMs);
    }

    [Fact]
    public void Deserialize_CaseInsensitive_ParsesCorrectly()
    {
        // Arrange - using mixed case property names
        var json = """{"Type":"ping","ID":"req-001"}""";
        var bytes = Encoding.UTF8.GetBytes(json);

        // Act
        var message = WebSocketMessageSerializer.DeserializeBase(bytes);

        // Assert
        Assert.NotNull(message);
        Assert.Equal("ping", message.Type);
        Assert.Equal("req-001", message.Id);
    }

    [Fact]
    public void Serialize_OmitsNullProperties()
    {
        // Arrange
        var message = WebSocketMessageSerializer.CreateSubscribeResponse(
            requestId: "req-001",
            success: true,
            subscriptionId: "sub-abc",
            topic: "my-topic",
            partitions: null,  // null should be omitted
            error: null);      // null should be omitted

        // Act
        var bytes = WebSocketMessageSerializer.Serialize(message);
        var json = Encoding.UTF8.GetString(bytes);

        // Assert
        Assert.DoesNotContain("\"partitions\":", json);
        Assert.DoesNotContain("\"error\":", json);
    }

    [Fact]
    public void Deserialize_ProduceBatchPayload_ReturnsTypedMessage()
    {
        // Arrange
        var json = """
        {
            "type": "produce_batch",
            "id": "req-005",
            "payload": {
                "topic": "my-topic",
                "records": [
                    { "partition": 0, "key": "key1", "value": "value1" },
                    { "partition": 0, "key": "key2", "value": "value2" }
                ]
            }
        }
        """;
        var bytes = Encoding.UTF8.GetBytes(json);

        // Act
        var message = WebSocketMessageSerializer.Deserialize<ProduceBatchPayload>(bytes);

        // Assert
        Assert.NotNull(message);
        Assert.Equal("produce_batch", message.Type);
        Assert.NotNull(message.Payload);
        Assert.Equal("my-topic", message.Payload.Topic);
        Assert.Equal(2, message.Payload.Records.Length);
        Assert.Equal("key1", message.Payload.Records[0].Key);
        Assert.Equal("value1", message.Payload.Records[0].Value);
    }

    [Fact]
    public void Deserialize_UnsubscribePayload_ReturnsTypedMessage()
    {
        // Arrange
        var json = """
        {
            "type": "unsubscribe",
            "id": "req-006",
            "payload": {
                "subscription_id": "sub-abc123"
            }
        }
        """;
        var bytes = Encoding.UTF8.GetBytes(json);

        // Act
        var message = WebSocketMessageSerializer.Deserialize<UnsubscribePayload>(bytes);

        // Assert
        Assert.NotNull(message);
        Assert.Equal("unsubscribe", message.Type);
        Assert.NotNull(message.Payload);
        Assert.Equal("sub-abc123", message.Payload.SubscriptionId);
    }

    [Fact]
    public void Deserialize_CommitPayload_ReturnsTypedMessage()
    {
        // Arrange
        var json = """
        {
            "type": "commit",
            "id": "req-007",
            "payload": {
                "consumer_group": "my-group",
                "offsets": [
                    { "topic": "topic-a", "partition": 0, "offset": 100 },
                    { "topic": "topic-a", "partition": 1, "offset": 200 }
                ]
            }
        }
        """;
        var bytes = Encoding.UTF8.GetBytes(json);

        // Act
        var message = WebSocketMessageSerializer.Deserialize<CommitPayload>(bytes);

        // Assert
        Assert.NotNull(message);
        Assert.Equal("commit", message.Type);
        Assert.NotNull(message.Payload);
        Assert.Equal("my-group", message.Payload.ConsumerGroup);
        Assert.Equal(2, message.Payload.Offsets.Length);
        Assert.Equal("topic-a", message.Payload.Offsets[0].Topic);
        Assert.Equal(0, message.Payload.Offsets[0].Partition);
        Assert.Equal(100, message.Payload.Offsets[0].Offset);
    }
}
