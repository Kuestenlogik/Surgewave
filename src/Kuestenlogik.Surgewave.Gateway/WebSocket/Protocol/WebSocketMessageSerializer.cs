using System.Buffers;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Kuestenlogik.Surgewave.Gateway.WebSocket.Protocol;

/// <summary>
/// JSON serializer for WebSocket messages.
/// </summary>
public static class WebSocketMessageSerializer
{
    private static readonly JsonSerializerOptions Options = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        PropertyNameCaseInsensitive = true
    };

    /// <summary>
    /// Deserializes a JSON message to extract the type.
    /// </summary>
    public static WebSocketMessage? DeserializeBase(ReadOnlySpan<byte> json)
    {
        return JsonSerializer.Deserialize<WebSocketMessage>(json, Options);
    }

    /// <summary>
    /// Deserializes a JSON message with typed payload.
    /// </summary>
    public static WebSocketMessage<T>? Deserialize<T>(ReadOnlySpan<byte> json) where T : class
    {
        return JsonSerializer.Deserialize<WebSocketMessage<T>>(json, Options);
    }

    /// <summary>
    /// Deserializes a JSON message with typed payload from string.
    /// </summary>
    public static WebSocketMessage<T>? Deserialize<T>(string json) where T : class
    {
        return JsonSerializer.Deserialize<WebSocketMessage<T>>(json, Options);
    }

    /// <summary>
    /// Serializes a message to JSON bytes.
    /// </summary>
    public static byte[] Serialize<T>(WebSocketMessage<T> message) where T : class
    {
        return JsonSerializer.SerializeToUtf8Bytes(message, Options);
    }

    /// <summary>
    /// Serializes a message to JSON string.
    /// </summary>
    public static string SerializeToString<T>(WebSocketMessage<T> message) where T : class
    {
        return JsonSerializer.Serialize(message, Options);
    }

    /// <summary>
    /// Serializes a message to a pooled buffer.
    /// </summary>
    public static ArraySegment<byte> SerializeToPooledBuffer<T>(WebSocketMessage<T> message) where T : class
    {
        var bufferWriter = new ArrayBufferWriter<byte>();
        using var writer = new Utf8JsonWriter(bufferWriter);
        JsonSerializer.Serialize(writer, message, Options);
        return new ArraySegment<byte>(bufferWriter.WrittenSpan.ToArray());
    }

    // Factory methods for common server messages

    /// <summary>
    /// Creates a subscribe response message.
    /// </summary>
    public static WebSocketMessage<SubscribeResponsePayload> CreateSubscribeResponse(
        string? requestId,
        bool success,
        string? subscriptionId = null,
        string? topic = null,
        int[]? partitions = null,
        string? error = null)
    {
        return new WebSocketMessage<SubscribeResponsePayload>
        {
            Type = WebSocketMessageType.SubscribeResponse,
            Id = requestId,
            Payload = new SubscribeResponsePayload
            {
                Success = success,
                SubscriptionId = subscriptionId,
                Topic = topic,
                Partitions = partitions,
                Error = error
            }
        };
    }

    /// <summary>
    /// Creates a message delivery message.
    /// </summary>
    public static WebSocketMessage<MessagePayload> CreateMessage(
        string subscriptionId,
        string topic,
        int partition,
        long offset,
        long timestamp,
        string? key,
        string value,
        Dictionary<string, string>? headers = null)
    {
        return new WebSocketMessage<MessagePayload>
        {
            Type = WebSocketMessageType.Message,
            Payload = new MessagePayload
            {
                SubscriptionId = subscriptionId,
                Topic = topic,
                Partition = partition,
                Offset = offset,
                Timestamp = timestamp,
                Key = key,
                Value = value,
                Headers = headers
            }
        };
    }

    /// <summary>
    /// Creates a message batch delivery message.
    /// </summary>
    public static WebSocketMessage<MessageBatchPayload> CreateMessageBatch(
        string subscriptionId,
        string topic,
        int partition,
        long highWatermark,
        MessageBatchRecord[] records)
    {
        return new WebSocketMessage<MessageBatchPayload>
        {
            Type = WebSocketMessageType.MessageBatch,
            Payload = new MessageBatchPayload
            {
                SubscriptionId = subscriptionId,
                Topic = topic,
                Partition = partition,
                HighWatermark = highWatermark,
                Records = records
            }
        };
    }

    /// <summary>
    /// Creates a produce response message.
    /// </summary>
    public static WebSocketMessage<ProduceResponsePayload> CreateProduceResponse(
        string? requestId,
        bool success,
        string? topic = null,
        int partition = 0,
        long offset = 0,
        long timestamp = 0,
        string? error = null)
    {
        return new WebSocketMessage<ProduceResponsePayload>
        {
            Type = WebSocketMessageType.ProduceResponse,
            Id = requestId,
            Payload = new ProduceResponsePayload
            {
                Success = success,
                Topic = topic,
                Partition = partition,
                Offset = offset,
                Timestamp = timestamp,
                Error = error
            }
        };
    }

    /// <summary>
    /// Creates an admin event message.
    /// </summary>
    public static WebSocketMessage<AdminEventPayload> CreateAdminEvent(
        string eventType,
        string? clusterId = null,
        object? data = null)
    {
        return new WebSocketMessage<AdminEventPayload>
        {
            Type = WebSocketMessageType.AdminEvent,
            Payload = new AdminEventPayload
            {
                EventType = eventType,
                ClusterId = clusterId,
                Data = data
            }
        };
    }

    /// <summary>
    /// Creates an error message.
    /// </summary>
    public static WebSocketMessage<ErrorPayload> CreateError(
        string? requestId,
        string code,
        string message,
        object? details = null)
    {
        return new WebSocketMessage<ErrorPayload>
        {
            Type = WebSocketMessageType.Error,
            Id = requestId,
            Payload = new ErrorPayload
            {
                Code = code,
                Message = message,
                Details = details
            }
        };
    }

    /// <summary>
    /// Creates a pong message.
    /// </summary>
    public static WebSocketMessage<PongPayload> CreatePong(string? requestId)
    {
        return new WebSocketMessage<PongPayload>
        {
            Type = WebSocketMessageType.Pong,
            Id = requestId,
            Payload = new PongPayload
            {
                Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
            }
        };
    }

    /// <summary>
    /// Creates a commit response message.
    /// </summary>
    public static WebSocketMessage<CommitResponsePayload> CreateCommitResponse(
        string? requestId,
        bool success,
        string? error = null)
    {
        return new WebSocketMessage<CommitResponsePayload>
        {
            Type = WebSocketMessageType.CommitResponse,
            Id = requestId,
            Payload = new CommitResponsePayload
            {
                Success = success,
                Error = error
            }
        };
    }
}
