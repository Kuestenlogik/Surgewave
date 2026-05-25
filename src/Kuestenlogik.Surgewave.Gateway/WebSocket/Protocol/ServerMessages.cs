using System.Text.Json.Serialization;

namespace Kuestenlogik.Surgewave.Gateway.WebSocket.Protocol;

/// <summary>
/// Subscribe response payload.
/// </summary>
public sealed class SubscribeResponsePayload
{
    [JsonPropertyName("success")]
    public bool Success { get; set; }

    [JsonPropertyName("subscription_id")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? SubscriptionId { get; set; }

    [JsonPropertyName("topic")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? Topic { get; set; }

    [JsonPropertyName("partitions")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public int[]? Partitions { get; set; }

    [JsonPropertyName("error")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? Error { get; set; }
}

/// <summary>
/// Unsubscribe response payload.
/// </summary>
public sealed class UnsubscribeResponsePayload
{
    [JsonPropertyName("success")]
    public bool Success { get; set; }

    [JsonPropertyName("subscription_id")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? SubscriptionId { get; set; }
}

/// <summary>
/// Single message delivery payload.
/// </summary>
public sealed class MessagePayload
{
    [JsonPropertyName("subscription_id")]
    public required string SubscriptionId { get; set; }

    [JsonPropertyName("topic")]
    public required string Topic { get; set; }

    [JsonPropertyName("partition")]
    public int Partition { get; set; }

    [JsonPropertyName("offset")]
    public long Offset { get; set; }

    [JsonPropertyName("timestamp")]
    public long Timestamp { get; set; }

    [JsonPropertyName("key")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? Key { get; set; }

    [JsonPropertyName("value")]
    public required string Value { get; set; }

    [JsonPropertyName("headers")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public Dictionary<string, string>? Headers { get; set; }
}

/// <summary>
/// Single record in a message batch.
/// </summary>
public sealed class MessageBatchRecord
{
    [JsonPropertyName("offset")]
    public long Offset { get; set; }

    [JsonPropertyName("timestamp")]
    public long Timestamp { get; set; }

    [JsonPropertyName("key")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? Key { get; set; }

    [JsonPropertyName("value")]
    public required string Value { get; set; }

    [JsonPropertyName("headers")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public Dictionary<string, string>? Headers { get; set; }
}

/// <summary>
/// Batch message delivery payload.
/// </summary>
public sealed class MessageBatchPayload
{
    [JsonPropertyName("subscription_id")]
    public required string SubscriptionId { get; set; }

    [JsonPropertyName("topic")]
    public required string Topic { get; set; }

    [JsonPropertyName("partition")]
    public int Partition { get; set; }

    [JsonPropertyName("high_watermark")]
    public long HighWatermark { get; set; }

    [JsonPropertyName("records")]
    public required MessageBatchRecord[] Records { get; set; }
}

/// <summary>
/// Produce response payload.
/// </summary>
public sealed class ProduceResponsePayload
{
    [JsonPropertyName("success")]
    public bool Success { get; set; }

    [JsonPropertyName("topic")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? Topic { get; set; }

    [JsonPropertyName("partition")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
    public int Partition { get; set; }

    [JsonPropertyName("offset")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
    public long Offset { get; set; }

    [JsonPropertyName("timestamp")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
    public long Timestamp { get; set; }

    [JsonPropertyName("error")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? Error { get; set; }
}

/// <summary>
/// Single result in a produce batch response.
/// </summary>
public sealed class ProduceBatchResult
{
    [JsonPropertyName("partition")]
    public int Partition { get; set; }

    [JsonPropertyName("offset")]
    public long Offset { get; set; }

    [JsonPropertyName("timestamp")]
    public long Timestamp { get; set; }

    [JsonPropertyName("error")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? Error { get; set; }
}

/// <summary>
/// Produce batch response payload.
/// </summary>
public sealed class ProduceBatchResponsePayload
{
    [JsonPropertyName("success")]
    public bool Success { get; set; }

    [JsonPropertyName("topic")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? Topic { get; set; }

    [JsonPropertyName("results")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public ProduceBatchResult[]? Results { get; set; }
}

/// <summary>
/// Commit response payload.
/// </summary>
public sealed class CommitResponsePayload
{
    [JsonPropertyName("success")]
    public bool Success { get; set; }

    [JsonPropertyName("error")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? Error { get; set; }
}

/// <summary>
/// Admin response payload.
/// </summary>
public sealed class AdminResponsePayload
{
    [JsonPropertyName("success")]
    public bool Success { get; set; }

    [JsonPropertyName("action")]
    public required string Action { get; set; }

    [JsonPropertyName("data")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public object? Data { get; set; }

    [JsonPropertyName("error")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? Error { get; set; }
}

/// <summary>
/// Admin event payload.
/// </summary>
public sealed class AdminEventPayload
{
    [JsonPropertyName("event_type")]
    public required string EventType { get; set; }

    [JsonPropertyName("cluster_id")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? ClusterId { get; set; }

    [JsonPropertyName("data")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public object? Data { get; set; }
}

/// <summary>
/// Error response payload.
/// </summary>
public sealed class ErrorPayload
{
    [JsonPropertyName("code")]
    public required string Code { get; set; }

    [JsonPropertyName("message")]
    public required string Message { get; set; }

    [JsonPropertyName("details")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public object? Details { get; set; }
}

/// <summary>
/// Pong response payload.
/// </summary>
public sealed class PongPayload
{
    [JsonPropertyName("timestamp")]
    public long Timestamp { get; set; }
}
