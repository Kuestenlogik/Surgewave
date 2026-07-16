using System.Text.Json;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.WebSocket.Tests;

/// <summary>
/// Pins how <see cref="WebSocketMessageSerializer"/> handles malformed and edge-case payloads:
/// syntactically broken JSON throws, missing required subscribe fields throw, unknown fields are
/// ignored, and null-valued optional fields are omitted when serializing.
/// </summary>
public sealed class WebSocketMessageSerializerMalformedInputTests
{
    [Fact]
    public void DeserializeProduceMessage_MalformedJson_ThrowsJsonException()
    {
        Assert.Throws<JsonException>(() => WebSocketMessageSerializer.DeserializeProduceMessage("{ this is not json"u8));
    }

    [Fact]
    public void DeserializeProduceMessage_NullLiteral_ReturnsNull()
    {
        Assert.Null(WebSocketMessageSerializer.DeserializeProduceMessage("null"u8));
    }

    [Fact]
    public void DeserializeProduceMessage_HeadersWithWrongType_ThrowsJsonException()
    {
        Assert.Throws<JsonException>(() => WebSocketMessageSerializer.DeserializeProduceMessage("""{"value":"x","headers":42}"""u8));
    }

    [Fact]
    public void DeserializeProduceMessage_UnknownExtraFields_AreIgnored()
    {
        var message = WebSocketMessageSerializer.DeserializeProduceMessage("""{"value":"x","unknown":123,"more":{"a":1}}"""u8);

        Assert.NotNull(message);
        Assert.NotNull(message.Value);
    }

    [Fact]
    public void DeserializeProduceMessage_ObjectValue_IsExposedAsJsonElement()
    {
        var message = WebSocketMessageSerializer.DeserializeProduceMessage("""{"value":{"nested":[1,2,3]}}"""u8);

        Assert.NotNull(message);
        var element = Assert.IsType<JsonElement>(message.Value);
        Assert.Equal(JsonValueKind.Object, element.ValueKind);
    }

    [Fact]
    public void DeserializeProduceMessage_NumberValue_IsExposedAsJsonElement()
    {
        var message = WebSocketMessageSerializer.DeserializeProduceMessage("""{"value":23.5}"""u8);

        Assert.NotNull(message);
        var element = Assert.IsType<JsonElement>(message.Value);
        Assert.Equal(JsonValueKind.Number, element.ValueKind);
    }

    [Fact]
    public void DeserializeSubscribeMessage_MalformedJson_ThrowsJsonException()
    {
        Assert.Throws<JsonException>(() => WebSocketMessageSerializer.DeserializeSubscribeMessage("[not-an-object"u8));
    }

    [Fact]
    public void DeserializeSubscribeMessage_MissingAction_ThrowsJsonException()
    {
        Assert.Throws<JsonException>(() => WebSocketMessageSerializer.DeserializeSubscribeMessage("""{"topics":["orders"]}"""u8));
    }

    [Fact]
    public void DeserializeSubscribeMessage_MissingTopics_ThrowsJsonException()
    {
        Assert.Throws<JsonException>(() => WebSocketMessageSerializer.DeserializeSubscribeMessage("""{"action":"subscribe"}"""u8));
    }

    [Fact]
    public void DeserializeSubscribeMessage_NullLiteral_ReturnsNull()
    {
        Assert.Null(WebSocketMessageSerializer.DeserializeSubscribeMessage("null"u8));
    }

    [Fact]
    public void SerializeConsumeMessage_NullValue_OmitsValueField()
    {
        var message = new WebSocketConsumeMessage
        {
            Topic = "t",
            Partition = 0,
            Offset = 3,
            Value = null,
            Timestamp = 1,
        };

        var json = WebSocketMessageSerializer.SerializeConsumeMessage(message);
        var root = JsonDocument.Parse(json).RootElement;

        Assert.False(root.TryGetProperty("value", out _));
        Assert.Equal(3L, root.GetProperty("offset").GetInt64());
    }
}
