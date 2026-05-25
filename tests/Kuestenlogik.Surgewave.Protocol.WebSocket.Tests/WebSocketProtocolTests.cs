using System.Text;
using System.Text.Json;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.WebSocket.Tests;

public sealed class WebSocketProtocolTests
{
    [Fact]
    public void ParseProduceMessage_ValidJson_ReturnsCorrectFields()
    {
        // Arrange
        var json = """{"key":"sensor-1","value":{"temperature":23.5},"headers":{"source":"iot"}}"""u8;

        // Act
        var message = WebSocketMessageSerializer.DeserializeProduceMessage(json);

        // Assert
        Assert.NotNull(message);
        Assert.Equal("sensor-1", message.Key);
        Assert.NotNull(message.Value);
        Assert.NotNull(message.Headers);
        Assert.Equal("iot", message.Headers["source"]);
    }

    [Fact]
    public void ParseProduceMessage_MinimalJson_ReturnsNullsForOptionalFields()
    {
        // Arrange
        var json = """{"value":"hello world"}"""u8;

        // Act
        var message = WebSocketMessageSerializer.DeserializeProduceMessage(json);

        // Assert
        Assert.NotNull(message);
        Assert.Null(message.Key);
        Assert.NotNull(message.Value);
        Assert.Null(message.Headers);
    }

    [Fact]
    public void ParseSubscribeMessage_ValidJson_ReturnsCorrectFields()
    {
        // Arrange
        var json = """{"action":"subscribe","topics":["orders","payments"],"offsets":{"orders":-2,"payments":-1}}"""u8;

        // Act
        var message = WebSocketMessageSerializer.DeserializeSubscribeMessage(json);

        // Assert
        Assert.NotNull(message);
        Assert.Equal("subscribe", message.Action);
        Assert.Equal(2, message.Topics.Count);
        Assert.Contains("orders", message.Topics);
        Assert.Contains("payments", message.Topics);
        Assert.NotNull(message.Offsets);
        Assert.Equal(-2, message.Offsets["orders"]);
        Assert.Equal(-1, message.Offsets["payments"]);
    }

    [Fact]
    public void ParseSubscribeMessage_UnsubscribeAction_ParsesCorrectly()
    {
        // Arrange
        var json = """{"action":"unsubscribe","topics":["orders"]}"""u8;

        // Act
        var message = WebSocketMessageSerializer.DeserializeSubscribeMessage(json);

        // Assert
        Assert.NotNull(message);
        Assert.Equal("unsubscribe", message.Action);
        Assert.Single(message.Topics);
        Assert.Null(message.Offsets);
    }

    [Fact]
    public void SerializeConsumeMessage_ProducesValidJson()
    {
        // Arrange
        var message = new WebSocketConsumeMessage
        {
            Topic = "orders",
            Partition = 0,
            Offset = 42,
            Key = "order-123",
            Value = "test payload",
            Timestamp = 1700000000000,
        };

        // Act
        var json = WebSocketMessageSerializer.SerializeConsumeMessage(message);
        var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        // Assert
        Assert.Equal("orders", root.GetProperty("topic").GetString());
        Assert.Equal(0, root.GetProperty("partition").GetInt32());
        Assert.Equal(42, root.GetProperty("offset").GetInt64());
        Assert.Equal("order-123", root.GetProperty("key").GetString());
        Assert.Equal("test payload", root.GetProperty("value").GetString());
        Assert.Equal(1700000000000, root.GetProperty("timestamp").GetInt64());
    }

    [Fact]
    public void SerializeConsumeMessage_NullKey_OmitsKeyField()
    {
        // Arrange
        var message = new WebSocketConsumeMessage
        {
            Topic = "events",
            Partition = 1,
            Offset = 0,
            Value = "data",
            Timestamp = 1700000000000,
        };

        // Act
        var json = WebSocketMessageSerializer.SerializeConsumeMessage(message);
        var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        // Assert
        Assert.False(root.TryGetProperty("key", out _)); // null key omitted
        Assert.Equal("events", root.GetProperty("topic").GetString());
    }

    [Fact]
    public void SerializeErrorMessage_ProducesValidJson()
    {
        // Arrange
        var error = new WebSocketErrorMessage
        {
            Error = "topic_not_found",
            Message = "Topic 'unknown' does not exist",
            CorrelationId = "req-123",
        };

        // Act
        var json = WebSocketMessageSerializer.SerializeErrorMessage(error);
        var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        // Assert
        Assert.Equal("topic_not_found", root.GetProperty("error").GetString());
        Assert.Equal("Topic 'unknown' does not exist", root.GetProperty("message").GetString());
        Assert.Equal("req-123", root.GetProperty("correlationId").GetString());
    }

    [Fact]
    public void SerializeErrorMessage_NoCorrelationId_OmitsField()
    {
        // Arrange
        var error = new WebSocketErrorMessage
        {
            Error = "internal_error",
            Message = "Something went wrong",
        };

        // Act
        var json = WebSocketMessageSerializer.SerializeErrorMessage(error);
        var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        // Assert
        Assert.Equal("internal_error", root.GetProperty("error").GetString());
        Assert.False(root.TryGetProperty("correlationId", out _));
    }
}
