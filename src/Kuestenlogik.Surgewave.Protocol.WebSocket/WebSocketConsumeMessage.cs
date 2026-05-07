using System.Text.Json.Serialization;

namespace Kuestenlogik.Surgewave.Protocol.WebSocket;

/// <summary>
/// JSON message sent to clients representing a consumed message from a Surgewave topic.
/// </summary>
public sealed class WebSocketConsumeMessage
{
    /// <summary>
    /// Topic the message was consumed from.
    /// </summary>
    [JsonPropertyName("topic")]
    public required string Topic { get; set; }

    /// <summary>
    /// Partition number.
    /// </summary>
    [JsonPropertyName("partition")]
    public required int Partition { get; set; }

    /// <summary>
    /// Message offset within the partition.
    /// </summary>
    [JsonPropertyName("offset")]
    public required long Offset { get; set; }

    /// <summary>
    /// Optional message key.
    /// </summary>
    [JsonPropertyName("key")]
    public string? Key { get; set; }

    /// <summary>
    /// Message value (payload).
    /// </summary>
    [JsonPropertyName("value")]
    public object? Value { get; set; }

    /// <summary>
    /// Timestamp of the message in milliseconds since epoch.
    /// </summary>
    [JsonPropertyName("timestamp")]
    public long Timestamp { get; set; }
}
