using System.Text;
using System.Text.Json;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.WebSocket.Tests;

/// <summary>
/// Tests for WebSocket message model serialization and deserialization edge cases
/// </summary>
public sealed class WebSocketMessageModelTests
{
    [Fact]
    public void ProduceMessage_Deserialize_WithAllFields()
    {
        var json = """{"key":"my-key","value":{"nested":true},"headers":{"x-src":"service-a","x-trace":"abc"}}"""u8;
        var msg = WebSocketMessageSerializer.DeserializeProduceMessage(json);

        Assert.NotNull(msg);
        Assert.Equal("my-key", msg.Key);
        Assert.NotNull(msg.Value);
        Assert.NotNull(msg.Headers);
        Assert.Equal(2, msg.Headers.Count);
        Assert.Equal("service-a", msg.Headers["x-src"]);
        Assert.Equal("abc", msg.Headers["x-trace"]);
    }

    [Fact]
    public void ProduceMessage_Deserialize_StringValue()
    {
        var json = """{"value":"plain-string-payload"}"""u8;
        var msg = WebSocketMessageSerializer.DeserializeProduceMessage(json);

        Assert.NotNull(msg);
        Assert.NotNull(msg.Value);
        Assert.Null(msg.Key);
        Assert.Null(msg.Headers);
    }

    [Fact]
    public void ProduceMessage_Deserialize_EmptyHeaders()
    {
        var json = """{"value":"data","headers":{}}"""u8;
        var msg = WebSocketMessageSerializer.DeserializeProduceMessage(json);

        Assert.NotNull(msg);
        Assert.NotNull(msg.Headers);
        Assert.Empty(msg.Headers);
    }

    [Fact]
    public void SubscribeMessage_Deserialize_WithEarliestOffset()
    {
        var json = """{"action":"subscribe","topics":["events"],"offsets":{"events":-2}}"""u8;
        var msg = WebSocketMessageSerializer.DeserializeSubscribeMessage(json);

        Assert.NotNull(msg);
        Assert.Equal("subscribe", msg.Action);
        Assert.Single(msg.Topics);
        Assert.Equal("events", msg.Topics[0]);
        Assert.NotNull(msg.Offsets);
        Assert.Equal(-2L, msg.Offsets["events"]);
    }

    [Fact]
    public void SubscribeMessage_Deserialize_MultipleTopicsNoOffsets()
    {
        var json = """{"action":"subscribe","topics":["t1","t2","t3"]}"""u8;
        var msg = WebSocketMessageSerializer.DeserializeSubscribeMessage(json);

        Assert.NotNull(msg);
        Assert.Equal(3, msg.Topics.Count);
        Assert.Null(msg.Offsets);
    }

    [Fact]
    public void ConsumeMessage_Serialize_IncludesAllFields()
    {
        var msg = new WebSocketConsumeMessage
        {
            Topic = "orders",
            Partition = 2,
            Offset = 100L,
            Key = "order-456",
            Value = "order payload",
            Timestamp = 1700000000000L
        };

        var json = WebSocketMessageSerializer.SerializeConsumeMessage(msg);
        var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        Assert.Equal("orders", root.GetProperty("topic").GetString());
        Assert.Equal(2, root.GetProperty("partition").GetInt32());
        Assert.Equal(100L, root.GetProperty("offset").GetInt64());
        Assert.Equal("order-456", root.GetProperty("key").GetString());
        Assert.Equal(1700000000000L, root.GetProperty("timestamp").GetInt64());
    }

    [Fact]
    public void ConsumeMessage_Serialize_ZeroOffset()
    {
        var msg = new WebSocketConsumeMessage
        {
            Topic = "t",
            Partition = 0,
            Offset = 0L,
            Value = "v",
            Timestamp = 0L
        };

        var json = WebSocketMessageSerializer.SerializeConsumeMessage(msg);
        var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        Assert.Equal(0L, root.GetProperty("offset").GetInt64());
        Assert.Equal(0, root.GetProperty("partition").GetInt32());
    }

    [Fact]
    public void ConsumeMessage_Serialize_ProducesUtf8Bytes()
    {
        var msg = new WebSocketConsumeMessage
        {
            Topic = "t",
            Partition = 0,
            Offset = 1L,
            Value = "v",
            Timestamp = 1L
        };

        var bytes = WebSocketMessageSerializer.SerializeConsumeMessage(msg);
        Assert.NotEmpty(bytes);

        // Verify it's valid UTF-8 JSON
        var str = Encoding.UTF8.GetString(bytes);
        Assert.StartsWith("{", str);
        Assert.EndsWith("}", str);
    }

    [Fact]
    public void ErrorMessage_Serialize_AllFields()
    {
        var error = new WebSocketErrorMessage
        {
            Error = "unauthorized",
            Message = "Access denied to topic",
            CorrelationId = "corr-789"
        };

        var json = WebSocketMessageSerializer.SerializeErrorMessage(error);
        var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        Assert.Equal("unauthorized", root.GetProperty("error").GetString());
        Assert.Equal("Access denied to topic", root.GetProperty("message").GetString());
        Assert.Equal("corr-789", root.GetProperty("correlationId").GetString());
    }

    [Fact]
    public void ErrorMessage_Serialize_NullCorrelationId_OmitsField()
    {
        var error = new WebSocketErrorMessage
        {
            Error = "topic_not_found",
            Message = "No such topic",
            CorrelationId = null
        };

        var json = WebSocketMessageSerializer.SerializeErrorMessage(error);
        var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        Assert.False(root.TryGetProperty("correlationId", out _));
        Assert.Equal("topic_not_found", root.GetProperty("error").GetString());
    }

    [Fact]
    public void SubscribeMessage_Deserialize_UnsubscribeWithMultipleTopics()
    {
        var json = """{"action":"unsubscribe","topics":["orders","payments","events"]}"""u8;
        var msg = WebSocketMessageSerializer.DeserializeSubscribeMessage(json);

        Assert.NotNull(msg);
        Assert.Equal("unsubscribe", msg.Action);
        Assert.Equal(3, msg.Topics.Count);
        Assert.Contains("orders", msg.Topics);
        Assert.Contains("payments", msg.Topics);
        Assert.Contains("events", msg.Topics);
    }

    [Fact]
    public void ConsumeMessage_Serialize_LargeOffset()
    {
        var msg = new WebSocketConsumeMessage
        {
            Topic = "high-volume",
            Partition = 15,
            Offset = long.MaxValue,
            Value = "v",
            Timestamp = long.MaxValue
        };

        var json = WebSocketMessageSerializer.SerializeConsumeMessage(msg);
        var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        Assert.Equal(long.MaxValue, root.GetProperty("offset").GetInt64());
        Assert.Equal(long.MaxValue, root.GetProperty("timestamp").GetInt64());
    }
}
